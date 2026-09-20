"""Native recovery holds. Planned resume is refused until the owner path selects ModularPositionExecutor."""
import asyncio
import importlib.util
import json
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch

import pytest

if importlib.util.find_spec('hummingbot') is None:
    pytest.skip('native Hummingbot package/compiled runtime is unavailable', allow_module_level=True)

from test.modular_spot.test_native_lifecycle import (
    config,
    disposable_recorder,
    make_modular_executor,
    plan_inputs,
    release_recorder,
    tracked,
    view_of,
)
from test.modular_spot.test_shared_data import snapshot as candle_snapshot

from controllers.generic.modular_spot import ModularSpotController
from controllers.generic.modular_spot_core.candle_data import Replica
from controllers.generic.modular_spot_core.data_reader import dataset_for
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor, native_config
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from controllers.generic.modular_spot_core.state import DecisionState, DecisionStore, reconcile_claims
from hummingbot.client.settings import AllConnectorSettings, ConnectorType
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


@pytest.fixture
def recorder(tmp_path):
    value = disposable_recorder(tmp_path)
    try:
        yield value
    finally:
        release_recorder(value)


def test_retained_dust_blocks_a_new_campaign():
    executor = make_modular_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    close = tracked('close', TradeType.SELL, '.995', OrderState.FILLED)
    executor._close_order = close
    executor.close_type = CloseType.POSITION_HOLD
    executor._status = RunnableStatus.TERMINATED
    owned = view_of(executor)
    assert owned.remaining_base == D('.005')
    assert owned.remaining_base < D('.01')
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=(executor.config.id,), committed_quote=D('100.5'))
    inventory = {executor.config.id: owned}
    revised, reason = reconcile_claims(state, inventory)
    assert revised == state and reason == 'held_inventory_requires_reconciliation'
    _, proposal, plan_reason = plan_inputs(state, inventory)
    assert proposal is None and plan_reason == 'held_inventory_requires_reconciliation'


def test_config_change_with_open_position_holds_without_resetting_trailing(recorder):
    original = config(execution_enabled=True)
    provider = NS(ready=True, time=lambda: 1200, connectors={}, get_candles_df=lambda *a, **k: None)
    controller = ModularSpotController(original, provider, asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    executor = make_modular_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    executor._trailing_stop_trigger_pct = D('.018')
    executor._trailing_stop_move_count = 2
    trailing = executor._trailing_stop_trigger_pct
    moves = executor._trailing_stop_move_count
    cost = view_of(executor).entry_quote
    store = DecisionStore(recorder, original.id, 'okx', 'BTC-USDC')
    store.compare_and_swap(None, DecisionState(
        fingerprint=controller.runtime.fingerprint, candle=1080,
        claims=(executor.config.id,), committed_quote=D('100.5')))
    controller.config.total_amount_quote = D('200')
    assert controller.config.fingerprint() != controller.runtime.fingerprint
    assert controller.determine_executor_actions() == []
    assert 'implementation/configuration changed' in controller.runtime.fault
    assert executor._trailing_stop_trigger_pct == trailing
    assert executor._trailing_stop_move_count == moves
    assert view_of(executor).entry_quote == cost
    loaded, _ = store.load()
    assert loaded.fingerprint == controller.runtime.fingerprint
    assert loaded.claims == (executor.config.id,)


def test_generic_planned_resume_keeps_base_executor_without_requesting_new_entry_grant():
    # This generic recipe remains unsupported by its native binding guard.
    # A direct native manifest does not gain a controller entry-factory grant;
    # genuine down-only specialization has separate native resume coverage.

    cfg = native_config(config(), identifier='resume-1', timestamp=100, price=D('100'), amount=D('1'),
                        cycle=0, stage=0)
    order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED).order
    order.current_state = OrderState.FILLED
    position = {
        'controller_id': 'modular_spot_test',
        'config': json.loads(cfg.model_dump_json()),
        'entry': order.to_json(),
        'trailing': {'trailing_stop_trigger_pct': '.03', 'trailing_move_count': 4,
                     'trailing_armed_timestamp': 200, 'trailing_last_update_timestamp': 220},
        'net_base': '1',
    }
    manifest = {'id': 'manifest-1', 'positions': [position]}
    connector = MagicMock()
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = D('100')
    connector.get_available_balance.return_value = D('2')
    connector.trading_rules = {'BTC-USDC': TradingRule(
        'BTC-USDC', min_order_size=D('.01'), min_price_increment=D('.01'),
        min_base_amount_increment=D('.001'), min_notional_size=D('1'))}
    strategy = MagicMock(spec=StrategyV2Base)
    controller = MagicMock()
    controller.config.connector_name = 'okx'
    controller.config.trading_pair = 'BTC-USDC'
    controller.get_executor_class_for_config.return_value = ModularPositionExecutor
    strategy.controllers = {'modular_spot_test': controller}
    strategy.connectors = {'okx': connector}
    strategy.markets = {'okx': {'BTC-USDC'}}
    strategy.market_data_provider = NS(ready=True)
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder') as markets:
        markets.get_instance.return_value = NS(sql_manager=NS(db_path=None), get_all_executors=lambda: [],
                                               get_all_positions=lambda: [],
                                               get_all_position_hold_ledger_entries=lambda: [],
                                               get_trades_for_config=lambda *a, **k: [],
                                               config_file_path='modular.yml')
        orchestrator = ExecutorOrchestrator(strategy=strategy)
    recorder = NS(sql_manager=NS(db_path='/tmp/m2-resume.sqlite'))
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest', return_value=manifest), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim') as claimed:
        resumed = orchestrator._claim_planned_positions(recorder)
    claimed.assert_called_once()
    created = orchestrator.active_executors['modular_spot_test'][0]
    assert resumed == {cfg.id}
    assert type(created) is PositionExecutor
    assert type(created) is not ModularPositionExecutor
    controller.get_executor_class_for_config.assert_not_called()
    assert created._trailing_stop_trigger_pct == D('.03')
    assert created._trailing_stop_move_count == 4
    assert created._resume_entry_sealed is True


def test_pending_planned_resume_is_an_explicit_unsupported_hold(recorder):
    controller = ModularSpotController(config(), NS(ready=False), asyncio.Queue())
    manifest = {'positions': [{'controller_id': controller.config.id}]}
    with patch('hummingbot.strategy_v2.executors.planned_position_resume.pending_manifest',
               return_value=manifest):
        with pytest.raises(PolicyError, match='bypasses local executor selection'):
            NativeBindings.guard_resume(controller, recorder)
    with patch('hummingbot.strategy_v2.executors.planned_position_resume.pending_manifest',
               side_effect=ValueError('database binding changed')):
        with pytest.raises(PolicyError, match='bypasses local executor selection'):
            NativeBindings.guard_resume(controller, recorder)
    with patch.object(NativeBindings, 'guard_resume',
                      side_effect=PolicyError(NativeBindings.UNSUPPORTED_PLANNED_RESUME)):
        with pytest.raises(PolicyError, match='bypasses local executor selection'):
            ModularSpotController(config(), NS(ready=False), asyncio.Queue())


def test_shared_candle_failure_blocks_acquisitions_without_a_loss_sale(recorder, monkeypatch):
    shared = config(execution_enabled=True, data={'mode': 'shared', 'origin': 'http://127.0.0.1:8100',
                                                  'token_file': '/run/secrets/modular_candle_reader'})
    spec = dataset_for(shared)
    replica = Replica((spec,))
    replica.begin_session()
    replica.accept(candle_snapshot(stream=spec), now=6000)
    assert not replica.entry_ready(spec.key)
    pool = NS(entry_ready=lambda dataset: replica.entry_ready(dataset.key), client=NS(replica=replica))
    setting = NS(type=ConnectorType.Exchange)
    monkeypatch.setattr(AllConnectorSettings, 'get_connector_settings', classmethod(lambda cls: {'okx': setting}))
    rules = TradingRule('BTC-USDC', min_order_size=D('.01'), min_price_increment=D('.01'),
                        min_base_amount_increment=D('.001'), min_notional_size=D('1'))
    connector = NS(ready=True, supports_terminal_fill_reconciliation=True, trading_rules={'BTC-USDC': rules},
                   get_available_balance=lambda quote: D('100'), get_fee=lambda **k: AddedToCostTradeFee(percent=D('0.001')),
                   in_flight_orders={}, get_price_by_type=lambda *a, **k: D('110'))
    provider = NS(ready=True, connectors={'okx': connector}, time=lambda: 1200,
                  get_price_by_type=lambda *a, **k: D('110'))
    with patch('controllers.generic.modular_spot_core.runtime.attach_pool', return_value=pool):
        controller = ModularSpotController(shared, provider, asyncio.Queue())
    controller.runtime.pool = pool
    controller._status = RunnableStatus.RUNNING
    controller.performance_report = NS(pnl_available=True, global_pnl_quote=D('0'))
    executor = make_modular_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    executor._trailing_stop_trigger_pct = D('.02')
    store = controller.runtime.ensure_store()
    store.compare_and_swap(None, DecisionState(
        fingerprint=controller.runtime.fingerprint, candle=1080,
        claims=(executor.config.id,), committed_quote=D('90')))
    controller.executors_info = [executor.executor_info]
    controller.positions_held = []
    market = MarketFeatures(1140, D('100'), D('29'), D('31'), False, False)

    def observe():
        controller.runtime.market = market
        return market

    controller.runtime.observe = observe
    assert controller.runtime.acquisition_permitted() is False
    assert controller.runtime.reason == 'shared_data_resynchronization'
    assert controller.determine_executor_actions() == []
    assert controller.runtime.reason == 'shared_data_resynchronization'
    executor.place_order.reset_mock()
    executor.get_price.return_value = D('110')
    executor.control_barriers()
    assert executor._should_fallback_trailing_limit_close() is False
    assert executor._close_order_type_for(CloseType.TRAILING_STOP) == OrderType.LIMIT
    market_calls = [c for c in executor.place_order.call_args_list
                    if c.kwargs.get('order_type') == OrderType.MARKET]
    assert market_calls == []
    assert executor.close_type is None
    assert view_of(executor).entry_quote == D('100')
    assert controller.runtime.fault is None


def test_partial_fill_cancels_remainder_buy_when_shared_candles_are_not_entry_ready(recorder, monkeypatch):
    shared = config(execution_enabled=True, data={'mode': 'shared', 'origin': 'http://127.0.0.1:8100',
                                                  'token_file': '/run/secrets/modular_candle_reader'})
    spec = dataset_for(shared)
    replica = Replica((spec,))
    replica.begin_session()
    replica.accept(candle_snapshot(stream=spec), now=6000)
    assert not replica.entry_ready(spec.key)
    pool = NS(entry_ready=lambda dataset: replica.entry_ready(dataset.key), client=NS(replica=replica))
    setting = NS(type=ConnectorType.Exchange)
    monkeypatch.setattr(AllConnectorSettings, 'get_connector_settings', classmethod(lambda cls: {'okx': setting}))
    rules = TradingRule('BTC-USDC', min_order_size=D('.01'), min_price_increment=D('.01'),
                        min_base_amount_increment=D('.001'), min_notional_size=D('1'))
    connector = NS(ready=True, supports_terminal_fill_reconciliation=True, trading_rules={'BTC-USDC': rules},
                   get_available_balance=lambda quote: D('100'), get_fee=lambda **k: AddedToCostTradeFee(percent=D('0.001')),
                   in_flight_orders={}, get_price_by_type=lambda *a, **k: D('110'))
    provider = NS(ready=True, connectors={'okx': connector}, time=lambda: 1200,
                  get_price_by_type=lambda *a, **k: D('110'))
    with patch('controllers.generic.modular_spot_core.runtime.attach_pool', return_value=pool):
        controller = ModularSpotController(shared, provider, asyncio.Queue())
    controller.runtime.pool = pool
    controller._status = RunnableStatus.RUNNING
    assert controller.runtime.acquisition_permitted() is False
    assert controller.runtime.reason == 'shared_data_resynchronization'

    executor = make_modular_executor()
    remainder = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    remainder.order.creation_timestamp = executor._strategy.current_timestamp
    executor._open_order = remainder
    executor._entry_submitted = True
    executor._trailing_stop_trigger_pct = D('.02')
    executor._strategy.controllers[controller.config.id] = controller
    seen = []

    def permission(config, *, check_funds=True):
        allowed = ModularSpotController.entry_permission(controller, config, check_funds=check_funds)
        seen.append(allowed)
        return allowed

    controller.entry_permission = permission
    assert executor.open_filled_amount == D('.4')
    assert remainder.order.is_open
    executor.place_order.reset_mock()
    executor.control_open_order()
    assert seen == [False]
    executor._strategy.cancel.assert_called_once_with(
        connector_name='okx', trading_pair='BTC-USDC', order_id='entry')
    assert executor._entry_stop_reason == 'modular_entry_permission_revoked'
    assert executor._entry_lifecycle['state'] == 'cancel_pending'
    executor.place_order.assert_not_called()
    executor.control_barriers()
    market_calls = [c for c in executor.place_order.call_args_list
                    if c.kwargs.get('order_type') == OrderType.MARKET]
    assert market_calls == []
    assert view_of(executor).remaining_base == D('.4')
    assert view_of(executor).entry_quote == D('40')
    assert executor.close_type is None
    assert executor._trailing_stop_trigger_pct == D('.02')
    executor.control_open_order()
    executor.place_order.assert_not_called()
    buy_calls = [c for c in executor.place_order.call_args_list
                 if c.kwargs.get('trade_type') == TradeType.BUY]
    assert buy_calls == []
    assert executor._open_order is remainder
    assert remainder.order.current_state == OrderState.PARTIALLY_FILLED
