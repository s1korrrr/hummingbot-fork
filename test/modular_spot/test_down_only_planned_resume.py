"""Exact native down-only receipts survive serialization and supported planned resume."""
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors import test_planned_position_resume as legacy
from test.test_ok_rsi_down_only_entry_execution import make_config
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from test_native_lifecycle import recorder as _recorder_fixture

from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import DownOnlyMakerPositionExecutor
from controllers.generic.modular_spot_core.native_funding import _config_identity
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors import planned_position_resume as resume
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus

recorder = _recorder_fixture


def down_only_executor():
    old = legacy.make_executor()
    barriers = old.config.triple_barrier_config.model_copy(update={'open_order_limit_max_age_seconds': 0})
    config = make_config(id=old.config.id, timestamp=old.config.timestamp,
                         controller_id='owner', trading_pair=old.config.trading_pair,
                         triple_barrier_config=barriers)
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules):
        result = DownOnlyMakerPositionExecutor(old._strategy, config)
    result._status = RunnableStatus.RUNNING
    result._sleep = AsyncMock()
    result.get_price = MagicMock(return_value=D('100'))
    result.get_in_flight_order = MagicMock(return_value=None)
    return result


@pytest.mark.asyncio
async def test_actual_recorder_preserves_down_only_config_and_funding_identity(recorder):
    executor = down_only_executor()
    recorder.store_or_update_executor(executor)
    restored = recorder.get_all_executors()[0]
    assert type(restored.config) is type(executor.config)
    assert restored.config == executor.config
    assert _config_identity(restored.config, 'owner') == _config_identity(executor.config, 'owner')
    assert restored.model_dump(mode='json')['config'] == executor.config.model_dump(mode='json')


@pytest.mark.asyncio
@pytest.mark.parametrize('owner_kind', ['public', 'spoof'])
async def test_supported_native_resume_retains_class_config_trailing_and_no_buy(tmp_path, owner_kind):
    from types import SimpleNamespace

    from controllers.generic.rsi_modular import get_config_class
    original_factory = legacy.make_executor
    # Use native legacy fixture ownership/SQLite/venue proof; replace only the
    # executor constructor with the genuine down-only native implementation.

    def make_down():
        with patch.object(legacy, 'make_executor', original_factory):
            return down_only_executor()
    with patch.object(legacy, 'make_executor', make_down):
        db, before, root, recorder, old, evidence = await legacy.stopped_fixture(tmp_path, True, '0', trading_pair='ETH-USDC', controller_id='modular_spot_resume')
    manifest = resume.prepare(db, before, evidence, now=1000, root=root)
    strategy = old._strategy
    controller_config = get_config_class('ok_rsi')(profile='ok_rsi', id='modular_spot_resume', trading_pair='ETH-USDC',
                                                   entry_acquisition_policy='down_only_maker_v1')
    if owner_kind == 'spoof':
        controller_config = SimpleNamespace(controller_name='ok_rsi', connector_name='okx', trading_pair='ETH-USDC',
                                            entry_acquisition_policy='down_only_maker_v1')
    strategy.controllers = {'modular_spot_resume': SimpleNamespace(config=controller_config)}
    strategy.markets = {'okx': {'ETH-USDC'}}
    strategy.ready_to_trade = True
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.ready = True
    strategy.market_data_provider.get_price_by_type.return_value = D('100')
    connector = strategy.connectors['okx']
    connector.in_flight_orders = {}
    connector.get_price_by_type.return_value = D('100')
    connector._order_tracker.fetch_order.return_value = None
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.pending_manifest',
                  side_effect=lambda path: resume.pending_manifest(path, now=1001, root=root)), \
            patch('hummingbot.strategy_v2.executors.executor_orchestrator.claim',
                  side_effect=lambda path, value: resume.claim(path, value, now=1001)), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=old.trading_rules), \
            patch.object(PositionExecutor, 'start') as start:
        orchestrator = ExecutorOrchestrator(strategy)
        if owner_kind == 'spoof':
            assert not orchestrator.resume_planned_positions_if_ready()
            assert 'native owner policy differs' in orchestrator._planned_resume_failure
            start.assert_not_called()
            assert resume.inspect_manifest(db)[0]['state'] == 'prepared'
            return
        assert orchestrator.resume_planned_positions_if_ready()
        recovered = orchestrator.active_executors['modular_spot_resume'][0]
        assert type(recovered) is DownOnlyMakerPositionExecutor
        assert recovered.config == old.config
        assert _config_identity(recovered.config, 'modular_spot_resume') == _config_identity(old.config, 'modular_spot_resume')
        assert recovered._planned_resume_id == manifest['id']
        assert recovered._trailing_stop_trigger_pct == old._trailing_stop_trigger_pct
        assert recovered._trailing_stop_move_count == old._trailing_stop_move_count
        recovered._status = RunnableStatus.RUNNING
        recovered.place_order = MagicMock(return_value='protective-exit')
        recovered._sleep = AsyncMock()
        recovered.place_open_order()
        recovered.control_open_order()
        recovered.place_order.assert_not_called()
        connector.get_price_by_type.return_value = D('103')
        await recovered.control_task()
        await recovered.control_task()
        recovered.place_order.assert_called_once()
        assert recovered.place_order.call_args.kwargs['side'] is TradeType.SELL
        assert recovered.place_order.call_args.kwargs['amount'] == D('1')


@pytest.mark.asyncio
@pytest.mark.parametrize('damage,reason', [
    ('reprice', 'unreplaced singleton'),
    ('partial', 'completely filled'),
    ('missing_policy', 'missing native down-only'),
    ('unknown_policy', 'Unsupported persisted'),
])
async def test_resume_rejects_unsupported_down_only_history(tmp_path, damage, reason):
    from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
    original_factory = legacy.make_executor

    def make_down():
        with patch.object(legacy, 'make_executor', original_factory):
            return down_only_executor()
    with patch.object(legacy, 'make_executor', make_down):
        db, before, _, _, _, _ = await legacy.stopped_fixture(tmp_path, True, '0')
    with resume.connect(before) as connection:
        preflight = resume.decode_row(connection.execute('SELECT * FROM Executors').fetchone())
    with resume.connect(db) as connection:
        stopped = resume.decode_row(connection.execute('SELECT * FROM Executors').fetchone())
    for row in (preflight, stopped):
        if damage == 'reprice':
            row['custom_info']['entry_acquisition']['reprice_count'] = 1
        elif damage == 'partial':
            row['custom_info']['entry_acquisition']['cumulative_filled_base'] = '.5'
        elif damage == 'missing_policy':
            row['config'] = {key: value for key, value in row['config'].items() if key in PositionExecutorConfig.model_fields}
        else:
            row['config']['entry_acquisition_policy'] = 'unknown_policy'
    with pytest.raises(ValueError, match=reason):
        resume.validate_position(preflight, stopped)
