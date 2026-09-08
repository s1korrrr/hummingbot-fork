"""Real SQLite boundaries; order transport remains inert."""
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor
from test.hummingbot.strategy_v2.executors.test_held_inventory_accounting import make_orchestrator
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from scripts.v2_with_controllers import V2WithControllers


@pytest.fixture
def recorder(tmp_path):
    import asyncio
    owned_loop = None
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        owned_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(owned_loop)
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                   db_path=str(tmp_path / 'lifecycle.sqlite'))
    result = MarketsRecorder(manager, [], 'safety.yml', 'v2_with_controllers',
                             market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    yield result
    manager.engine.dispose()
    if owned_loop is not None:
        owned_loop.close()
        asyncio.set_event_loop(None)


def test_risk_state_roundtrip_uses_real_sqlite_and_rejects_invalid_write(recorder):
    state = {'version': 1, 'max_global_pnl': '100.25', 'operator_paused': {'controller': True}}
    assert recorder.load_strategy_risk_state() is None
    recorder.store_strategy_risk_state(state)
    assert recorder.load_strategy_risk_state() == state
    with pytest.raises(ValueError):
        recorder.store_strategy_risk_state({'invalid': float('nan')})
    assert recorder.load_strategy_risk_state() == state


def test_persisted_unfinished_executor_blocks_restart(recorder):
    executor = make_executor()
    executor.config.controller_id = 'controller'
    recorder.store_or_update_executor(executor)
    strategy = executor._strategy
    strategy.controllers = {'controller': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.recovery_required == {'controller': [executor.config.id]}
        with pytest.raises(RuntimeError, match='startup reconciliation'):
            orchestrator.create_executor(CreateExecutorAction(controller_id='controller', executor_config=executor.config))
    strategy.buy.assert_not_called()


def test_persistence_failure_never_starts_executor():
    orchestrator = make_orchestrator()
    executor = make_executor()
    orchestrator.strategy.controllers = {}
    orchestrator.strategy.connectors['okx'].in_flight_orders = {}
    orchestrator._validate_okx_wallet_reservation = MagicMock()
    created = MagicMock()
    orchestrator._resolve_executor_class = MagicMock(return_value=MagicMock(return_value=created))
    recorder = MagicMock()
    recorder.store_or_update_executor.side_effect = IOError('disk full')
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), pytest.raises(IOError):
        orchestrator.create_executor(CreateExecutorAction(controller_id='controller', executor_config=executor.config))
    created.start.assert_not_called()
    assert orchestrator.active_executors['controller'] == []


def test_intent_exists_in_sqlite_before_start(recorder):
    orchestrator = make_orchestrator()
    executor = make_executor()
    orchestrator.strategy.controllers = {}
    orchestrator.strategy.connectors['okx'].in_flight_orders = {}
    orchestrator._validate_okx_wallet_reservation = MagicMock()
    executor.start = MagicMock(side_effect=lambda: pytest.fail('intent missing') if not recorder.get_all_executors() else None)
    orchestrator._resolve_executor_class = MagicMock(return_value=MagicMock(return_value=executor))
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        orchestrator.create_executor(CreateExecutorAction(controller_id='controller', executor_config=executor.config))
    executor.start.assert_called_once()
    assert recorder.get_all_executors()[0].id == executor.config.id


def test_cross_controller_unsubmitted_orders_reserve_shared_quote_wallet():
    orchestrator = make_orchestrator()
    first = make_executor()
    first.config.amount = D('8')
    first.config.entry_price = D('100')
    orchestrator.active_executors = {'first': [first], 'second': []}
    orchestrator.strategy.connectors['okx'].get_available_balance.return_value = D('1000')
    second = make_executor().config
    second.amount = D('3')
    second.entry_price = D('100')
    with pytest.raises(RuntimeError, match='Insufficient unreserved'):
        orchestrator._validate_okx_wallet_reservation(second)


def test_queued_create_rechecks_drawdown_and_operator_events():
    strategy = MagicMock(spec=V2WithControllers)
    strategy._is_stop_triggered = False
    strategy.drawdown_exited_controllers = []
    strategy.control_max_drawdown.side_effect = lambda: setattr(strategy, '_is_stop_triggered', True)
    action = SimpleNamespace(controller_id='controller')
    assert not V2WithControllers.validate_create_action(strategy, action)
    strategy.process_operator_events.assert_called_once()
    strategy.control_max_drawdown.assert_called_once()
    strategy._persist_risk_state.assert_called_once()


@pytest.mark.asyncio
async def test_real_strategy_constructor_restores_pauses_and_drawdown_peaks(recorder):
    import asyncio
    from collections import deque

    from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    from scripts.v2_with_controllers import V2WithControllersConfig

    recorder.store_strategy_risk_state({
        'version': 1, 'controllers': ['controller'], 'max_global_pnl': '100',
        'max_pnl_by_controller': {'controller': '75'}, 'drawdown_exited_controllers': [],
        'stopped': False, 'operator_paused': {'controller': True}})
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDT'])

    def initialize(strategy):
        config = OKRSIControllerConfig(id='controller', connector_name='okx', trading_pair='ETH-USDT')
        strategy.controllers = {'controller': OKRSIController(config, strategy.market_data_provider, strategy.actions_queue)}

    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig())
        try:
            assert not strategy._is_stop_triggered
            assert strategy.max_global_pnl == D('100')
            assert strategy.max_pnl_by_controller == {'controller': D('75')}
            assert strategy.controllers['controller']._operator_entry_paused
            strategy.apply_initial_setting()
            assert strategy.max_pnl_by_controller['controller'] == D('75')
            assert not connector.in_flight_orders
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)


def test_recovery_diagnostic_includes_shutting_down_and_excludes_terminated(recorder):
    from pathlib import Path

    from scripts.diagnostics.ok_rsi_recovery_status import inspect_database
    executor = make_executor()
    executor._status = RunnableStatus.SHUTTING_DOWN
    recorder.store_or_update_executor(executor)
    report = inspect_database(Path(recorder.sql_manager.db_path), 'safety.yml')
    assert report['unfinished_executors'][0]['executor_id'] == executor.config.id
    assert report['requires_reconciliation']
    executor._status = RunnableStatus.TERMINATED
    recorder.store_or_update_executor(executor)
    report = inspect_database(Path(recorder.sql_manager.db_path), 'safety.yml')
    assert report['unfinished_executors'] == []
    assert report['requires_reconciliation']  # Existing history still needs its risk state.


def test_unowned_native_order_blocks_new_execution_before_persistence():
    orchestrator = make_orchestrator()
    orchestrator.strategy.controllers = {}
    orchestrator.strategy.connectors['okx'].in_flight_orders = {'orphan': SimpleNamespace(trading_pair='ETH-USDT')}
    with pytest.raises(RuntimeError, match='Unowned OKX orders'):
        orchestrator.create_executor(CreateExecutorAction(controller_id='controller', executor_config=make_executor().config))
    assert orchestrator.active_executors['controller'] == []


@pytest.mark.asyncio
@pytest.mark.parametrize('invalid_state', [None, {'version': 999}])
async def test_failed_risk_restore_preserves_original_evidence_after_tick(recorder, invalid_state):
    import asyncio
    from collections import deque

    from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
    from hummingbot.connector.exchange.okx.okx_exchange import OkxExchange
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    from scripts.v2_with_controllers import V2WithControllersConfig

    executor = make_executor()
    executor._status = RunnableStatus.TERMINATED
    recorder.store_or_update_executor(executor)
    if invalid_state is not None:
        recorder.store_strategy_risk_state(invalid_state)
    connector = OkxExchange(okx_api_key='', okx_secret_key='', okx_passphrase='', trading_pairs=['ETH-USDT'])

    def initialize(strategy):
        config = OKRSIControllerConfig(id='controller', connector_name='okx', trading_pair='ETH-USDT')
        strategy.controllers = {'controller': OKRSIController(config, strategy.market_data_provider, strategy.actions_queue)}

    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(StrategyV2Base, 'initialize_controllers', initialize), \
            patch.object(StrategyV2Base, 'initialize_candles'), \
            patch('hummingbot.data_feed.market_data_provider.MarketDataProvider.initialize_rate_sources'), \
            patch('scripts.v2_with_controllers.ExternalEventFactory.create_queue', return_value=deque()):
        strategy = V2WithControllers({'okx': connector}, V2WithControllersConfig())
        try:
            assert strategy._is_stop_triggered
            with patch.object(StrategyV2Base, 'on_tick'), \
                    patch.object(strategy, 'process_operator_events'), \
                    patch.object(strategy, 'control_max_drawdown'):
                strategy.on_tick()
                assert not strategy.validate_create_action(SimpleNamespace(controller_id='controller'))
            assert recorder.load_strategy_risk_state() == invalid_state
            assert 'EXECUTION BLOCKED' in strategy.format_status()
            assert 'reconciliation' in strategy.format_status()
            assert not connector.in_flight_orders
        finally:
            strategy.listen_to_executor_actions_task.cancel()
            await asyncio.gather(strategy.listen_to_executor_actions_task, return_exceptions=True)
