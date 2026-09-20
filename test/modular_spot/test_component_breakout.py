"""Actual native paper breakout boundary; synthetic engineering evidence only."""
import asyncio
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import PAIR, paper
from unittest.mock import patch

import pandas as pd
import pytest

from controllers.generic.component_breakout import (
    ComponentBreakoutConfig,
    ComponentBreakoutController,
    breakout_features,
)
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.paper_session import NativePaperSession


def candles(now=600000):
    frame = pd.DataFrame([dict(timestamp=now - (60 - i) * 60, open=99, high=100, low=98, close=99,
                               volume=10, confirmed=True) for i in range(60)])
    frame.loc[59, ['open', 'high', 'low', 'close']] = [99, 102, 98, 101]
    return frame


def config(**kwargs):
    return ComponentBreakoutConfig(id='breakout_reference', **kwargs)


@pytest.fixture(autouse=True)
def isolated_no_network():
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')), patch('socket.getaddrinfo', side_effect=AssertionError('network forbidden')):
        yield
    MarketsRecorder._shared_instance = previous


def test_reference_schema_and_causal_prior_high():
    assert ComponentBreakoutController.__bases__ == (ControllerBase,)
    assert not any('rsi' in name.lower() for name in ComponentBreakoutConfig.model_fields)
    assert config().execution_enabled is False
    for kwargs in ({'execution_mode': 'live'}, {'connector_name': 'okx'}, {'trade_quote': 51}, {'rsi_length': 14}, {'risk_fraction': 0}, {'execution_enabled': 1}, {'trade_quote': float('nan')}):
        with pytest.raises(ValueError):
            config(**kwargs)
    data = breakout_features(candles(), 20, 14)
    assert data['signal'] == 1 and data['prior_high'] == 100
    frame = candles()
    frame.loc[59, 'close'] = 100
    assert breakout_features(frame, 20, 14)['signal'] == 0


def inputs(now=600000, sequence=2):
    return dict(books=[dict(trading_pair=PAIR, sequence=sequence, timestamp=now, bids=[(100, 10)], asks=[(102, 10)])],
                candles=[dict(connector_name='okx', trading_pair=PAIR, interval='1m', frame=candles(now), available_at=now)])


@pytest.mark.asyncio
async def test_actual_controller_queue_lifecycle_and_pending_blocks(tmp_path):
    session = NativePaperSession(paper(), [config(execution_enabled=True)], tmp_path / 'queue.sqlite')
    try:
        provider = session.market_data_provider
        session._connector.tick(600000)
        session._connector.set_order_book(**inputs()['books'][0])
        provider.set_candles(**inputs()['candles'][0])
        controller = session.controllers['breakout_reference']
        controller.executors_update_event.set()
        await controller.control_task()
        actions = await asyncio.wait_for(controller.actions_queue.get(), .5)
        assert len(actions) == 1
        assert controller.validate_executor_config(actions[0].executor_config)
        assert controller.determine_executor_actions() == []
        assert not controller.executors_update_event.is_set()
        controller.config.manual_kill_switch = True
        assert not controller.validate_executor_config(actions[0].executor_config)
        controller.config.manual_kill_switch = False
        assert controller.validate_executor_config(actions[0].executor_config)
        controller.processed_data['data_health']['ready'] = False
        assert not controller.validate_executor_config(actions[0].executor_config)
        controller.processed_data['data_health']['ready'] = True
        original_amount = actions[0].executor_config.amount
        actions[0].executor_config.amount *= 2
        assert not controller.validate_executor_config(actions[0].executor_config)
        actions[0].executor_config.amount = original_amount
        session._connector.tick(600011)
        assert not controller.validate_executor_config(actions[0].executor_config)
        controller.config.manual_kill_switch = True
        assert not controller.validate_executor_config(actions[0].executor_config)
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_native_partial_fill_fees_and_restart_hold(tmp_path):
    db = tmp_path / 'paper.sqlite'
    session = NativePaperSession(paper(), [config(execution_enabled=True)], db)
    try:
        receipt = await session.step(600000, **inputs())
        assert len(receipt['actions']) == 1
        executor = session.executor_orchestrator.active_executors['breakout_reference'][0]
        assert type(executor) is PositionExecutor
        assert executor.config.triple_barrier_config.stop_loss > 0
        assert executor.config.amount * executor.config.entry_price <= 10
        price = executor._open_order.order.price
        receipt = await session.step(600001, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600001,
                                                          side=TradeType.SELL, price=price - D('.01'), amount=D('.01'))])
        assert receipt['actions'] == []
        assert executor.open_filled_amount == D('.01')
        assert session._connector.get_balance('BTC') == D('.01')
        assert session._connector.get_balance('USDC') == D('1000') - D('.01') * price * D('1.001')
        assert len(session.recorder.get_trades_for_config('native-paper-session')) == 1
    finally:
        await session.close()
    restored = NativePaperSession(paper(), [config(execution_enabled=True)], db, restore=True)
    try:
        receipt = await restored.step(600060, **inputs(600060, 3))
        assert receipt['actions'] == []
        assert restored._connector.get_balance('BTC') == D('.01')
        assert not receipt['readiness']['ready']
        assert any('reference_restart' in reason for reason in receipt['readiness']['reasons'])
    finally:
        await restored.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('mutation', ['stale', 'future', 'gap', 'unconfirmed', 'nan', 'disabled', 'foreign', 'held'])
async def test_fail_closed_data_and_ownership(tmp_path, mutation):
    session = NativePaperSession(paper(), [config(execution_enabled=mutation != 'disabled')], tmp_path / 'closed.sqlite')
    try:
        controller = session.controllers['breakout_reference']
        session._connector.tick(600000)
        frame = candles()
        if mutation == 'stale':
            frame.timestamp -= 60
        if mutation == 'future':
            frame.timestamp += 60
        if mutation == 'gap':
            frame.loc[30, 'timestamp'] -= 60
        if mutation == 'unconfirmed':
            frame.loc[59, 'confirmed'] = False
        if mutation == 'nan':
            frame.loc[59, 'close'] = float('nan')
        with patch.object(session.market_data_provider, 'get_candles_df', return_value=frame):
            await controller.update_processed_data()
        if mutation == 'held':
            controller.positions_held = [object()]
        if mutation == 'foreign':
            session.market_data_provider.connectors = {'okx_paper_trade': object()}
        assert controller.determine_executor_actions() == []
        if mutation in {'stale', 'future', 'gap', 'unconfirmed', 'nan'}:
            assert not controller.processed_data['data_health']['ready']
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_native_protective_stop_exits_filled_inventory_when_entries_paused(tmp_path):
    from hummingbot.strategy_v2.models.executors import CloseType
    session = NativePaperSession(paper(), [config(execution_enabled=True)], tmp_path / 'stop.sqlite')
    try:
        await session.step(600000, **inputs())
        controller = session.controllers['breakout_reference']
        executor = session.executor_orchestrator.active_executors['breakout_reference'][0]
        amount = executor.config.amount
        await session.step(600001, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600001,
                                                side=TradeType.SELL, price=executor.config.entry_price - D('.01'), amount=amount)])
        assert executor.open_filled_amount == amount
        controller.config.manual_kill_switch = True
        for offset in range(2, 6):
            receipt = await session.step(600000 + offset, books=[dict(trading_pair=PAIR, sequence=offset + 1,
                                                                      timestamp=600000 + offset, bids=[(80, 10)], asks=[(81, 10)])])
            assert receipt['actions'] == []
        assert executor.close_type == CloseType.STOP_LOSS
        assert executor.close_filled_amount == amount
        assert session._connector.get_balance('BTC') == 0
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_actual_native_observation_preserves_bounded_reference_diagnostics(tmp_path):
    from hummingbot.strategy.native_observability import controller_observation
    session = NativePaperSession(paper(), [config()], tmp_path / 'observation.sqlite')
    try:
        await session.step(600000, **inputs())
        controller = session.controllers['breakout_reference']
        entry = {'controller': controller, 'controller_id': controller.config.id,
                 'summary': controller.get_status_summary()}
        observed = controller_observation(entry)
        assert observed['controller_name'] == 'component_breakout'
        assert observed['profile'] is None
        diagnostics = observed['custom_info']['strategy_components']
        assert diagnostics['data_ready'] and diagnostics['signal'] == 1
        assert diagnostics['decision_time'] == 600000
        assert diagnostics['execution_mode'] == 'paper'
        assert not diagnostics['execution_enabled']
        assert 'pnl' not in str(diagnostics).lower()
        custom = controller.get_custom_info()
        custom['strategy_components']['controller_name'] = 'spoof'
        with patch.object(controller, 'get_custom_info', return_value=custom):
            assert controller_observation(entry)['controller_name'] == 'component_breakout'
        for invalid in ([str(i) for i in range(17)], ['a' * 65], ['ok', 'ok']):
            custom['strategy_components']['component_ids'] = invalid
            with patch.object(controller, 'get_custom_info', return_value=custom):
                assert controller_observation(entry)['observation_status'] == 'unavailable'
        assert not controller.allow_implementation_hot_reload
        assert Path(__import__('controllers.generic.component_breakout', fromlist=['x']).__file__) in controller.implementation_source_files()
        assert all('modular_spot_core' not in str(path) for path in controller.implementation_source_files())
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_allocation_reserves_actual_native_fee_even_if_config_buffer_is_zero(tmp_path):
    session = NativePaperSession(paper(), [config(execution_enabled=True, total_amount_quote=50,
                                                  trade_quote=50, risk_fraction=D('.05'), atr_stop_multiple=D('.1'), fee_buffer=0)], tmp_path / 'fee.sqlite')
    try:
        receipt = await session.step(600000, **inputs())
        executor = receipt['actions'][0].executor_config
        assert executor.amount * executor.entry_price * D('1.001') <= 50
        assert executor.amount * executor.entry_price > 49
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_missing_or_stale_native_book_cannot_emit_reference_create(tmp_path):
    session = NativePaperSession(paper(), [config(execution_enabled=True)], tmp_path / 'book.sqlite')
    try:
        controller = session.controllers['breakout_reference']
        session._connector.tick(600000)
        session.market_data_provider.set_candles(**inputs()['candles'][0])
        await controller.update_processed_data()
        assert controller.processed_data['data_health']['ready']
        assert not session._connector.ready
        assert controller.determine_executor_actions() == []
        session._connector.set_order_book(**inputs()['books'][0])
        assert session._connector.ready
        actions = controller.determine_executor_actions()
        assert len(actions) == 1
        session._connector.tick(600060)
        # Supply current candles to isolate book freshness from candle freshness.
        session.market_data_provider.set_candles(**inputs(600060)['candles'][0])
        await controller.update_processed_data()
        assert controller.processed_data['data_health']['ready']
        assert not session._connector.ready
        assert not controller.validate_executor_config(actions[0].executor_config)
        assert controller.determine_executor_actions() == []
    finally:
        await session.close()


def test_programmatic_config_bypass_cannot_increase_reference_capital(tmp_path):
    invalid = config().model_copy(update={'total_amount_quote': D('500'), 'trade_quote': D('500'),
                                          'execution_enabled': True})
    with pytest.raises(ValueError):
        NativePaperSession(paper(), [invalid], tmp_path / 'invalid.sqlite')
    assert not (tmp_path / 'invalid.sqlite').exists()
