"""2026-10-04 V2 BNB-USDC: a restart-restored down-only executor closes with native fee dust.

Production (rsibot-v2, controller modular_spot_v2_bnb): executor 7G22vD23... bought 0.078688 BNB
(OKX charged the 0.0000629504 BNB maker fee in base), was suspended and restored by restart
continuity three times, then its trailing stop sold the quantized 0.078625. The 4.96e-8 BNB
remainder is below the 0.000001 lot. From that fill until the next restart the BUY gate read
``exposure_unavailable`` (``$inf/$500``). These tests drive the real executor classes, the real
SQLite MarketsRecorder and the real orchestrator restore path, then read exposure through the
real OK RSI controller from both the live executor report and the persisted native history.

Every case fails with Infinity on the deployed V2 engine (1eb132b63, without 9fdd5d8eb..d1cabb397):
there ``_executor_reported_owned_base`` saw remaining 0 against fill-minus-close 4.96e-8 and the
closed executor counted toward BUY capital as unprovable. The restart restore is incidental: the
never-restarted cases fail identically, and the live report equals the persisted terminal row.
"""
import asyncio
import importlib
import json
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, DeductedFromReturnsTradeFee, TokenAmount
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy_v2.executors import planned_position_resume as resume
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop, TripleBarrierConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

PAIR = 'BNB-USDC'
CONTROLLER = 'modular_spot_v2_bnb'
RULE = TradingRule(PAIR, min_order_size=D('.001'), min_price_increment=D('.1'),
                   min_base_amount_increment=D('.000001'), min_notional_size=D('1'))
AUTHORIZED = D('0.078688')
BUY_FEE_BASE = D('0.0000629504')
SOLD = D('0.078625')
DUST = AUTHORIZED - BUY_FEE_BASE - SOLD  # 4.96E-8, below the 0.000001 lot
MID = D('789.05')


def _native(module, name):
    """Other native tests purge and re-import ``controllers``; resolve classes at call time so
    identities match the orchestrator's runtime imports (see test_strategy_v2_base)."""
    return getattr(importlib.import_module(module), name)


def _ok_rsi_config(**values):
    return _native('controllers.directional_trading.ok_rsi_core.config', 'OKRSIControllerConfig')(**values)


def _down_only_class():
    return _native('controllers.directional_trading.ok_rsi_core.down_only_entry_execution',
                   'DownOnlyMakerPositionExecutor')


@pytest.fixture
def recorder(tmp_path):
    owned_loop = None
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        owned_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(owned_loop)
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                   db_path=str(tmp_path / 'v2_bnb.sqlite'))
    result = MarketsRecorder(manager, [], 'v2_bnb.yml', 'v2_with_controllers',
                             market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    yield result
    manager.engine.dispose()
    if owned_loop is not None:
        owned_loop.close()
        asyncio.set_event_loop(None)


def _order(oid, side, amount, price, fill, fee, state, order_type=OrderType.LIMIT_MAKER):
    order = InFlightOrder(client_order_id=oid, exchange_order_id=f'venue-{oid}', trading_pair=PAIR,
                          order_type=order_type, trade_type=side, amount=amount, price=price,
                          creation_timestamp=1, initial_state=state)
    if fill:
        order.update_with_trade_update(TradeUpdate(
            trade_id=f'trade-{oid}', client_order_id=oid, exchange_order_id=f'venue-{oid}', trading_pair=PAIR,
            fill_price=price, fill_base_amount=fill, fill_quote_amount=fill * price, fee=fee, fill_timestamp=2))
    result = TrackedOrder(order_id=oid)
    result.order = order
    return result


def _strategy():
    strategy = make_executor()._strategy
    connector = strategy.connectors['okx']
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: (amount // D('.000001')) * D('.000001')
    connector.get_balance.return_value = D('8.776')
    connector.get_available_balance.return_value = D('8.776')
    connector._order_tracker.fetch_order.return_value = None
    connector.in_flight_orders = {}
    strategy.markets = {'okx': {PAIR}}
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.get_price_by_type.return_value = MID
    strategy.market_data_provider.ready = True
    strategy.ready_to_trade = True
    strategy.controllers = {CONTROLLER: SimpleNamespace(
        config=_ok_rsi_config(id=CONTROLLER, connector_name='okx', trading_pair=PAIR,
                              entry_acquisition_policy='down_only_maker_v1'),
        get_executor_class_for_config=lambda config: _down_only_class())}
    return strategy


def _filled_down_only_executor(strategy, *, predecessor, chain):
    barriers = TripleBarrierConfig(
        open_order_type=OrderType.LIMIT_MAKER, open_order_limit_max_age_seconds=0,
        trailing_stop=TrailingStop(activation_price=D('.0075'), trailing_delta=D('.0022')),
        trailing_stop_order_type=OrderType.LIMIT, trailing_stop_limit_max_age_seconds=3)
    # The production row's config (executor 7G22..., modular_spot_v2_bnb), at the test's own id.
    config = _native('controllers.directional_trading.ok_rsi_core.down_only_entry_execution',
                     'DownOnlyMakerPositionExecutorConfig')(
        id='7G22-restored-dust', timestamp=1.0, controller_id=CONTROLLER, connector_name='okx',
        trading_pair=PAIR, side=TradeType.BUY, amount=AUTHORIZED, entry_price=D('788.4'), leverage=1,
        triple_barrier_config=barriers, activation_bounds=[D('.0005')], entry_order_policy='single_attempt',
        decision_best_bid=D('788.5'), decision_best_ask=D('788.6'), decision_atr_1m=D('.289'),
        decision_price_step=D('.1'), maximum_acceptable_price=D('788.4'), entry_total_max_age_seconds=300,
        entry_max_downward_reprices=3, entry_reprice_cooldown_seconds=5, entry_reprice_min_ticks=3,
        entry_reprice_atr_fraction=D('.05'),
        entry_chain_contract=resume.ACQUISITION_CHAIN_V1 if chain else None)
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE):
        executor = _down_only_class()(strategy, config)
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(return_value=MID)
    executor.get_in_flight_order = MagicMock(return_value=None)
    if predecessor:
        # The post-only rejection of the first child and its zero-fill cancel, as in production.
        executor._open_order = _order('entry-old', TradeType.BUY, AUTHORIZED, D('788.4'), D('0'),
                                      AddedToCostTradeFee(), OrderState.CANCELED)
        executor._completed_open_orders.append(executor._open_order)
        executor._entry_reprice_count = 1
    executor._open_order = _order('entry', TradeType.BUY, AUTHORIZED, D('788.3'), AUTHORIZED,
                                  AddedToCostTradeFee(flat_fees=[TokenAmount('BNB', BUY_FEE_BASE)]),
                                  OrderState.FILLED)
    executor._entry_submitted = True
    executor._entry_lifecycle = {'state': 'managing_fill', 'stop_reason': None}
    return executor


def _boot(recorder, strategy):
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder), \
            patch.object(PositionExecutor, 'get_trading_rules', return_value=RULE), \
            patch.object(PositionExecutor, 'start'):
        orchestrator = ExecutorOrchestrator(strategy)
        assert orchestrator.resume_planned_positions_if_ready() is True
    executors = orchestrator.active_executors[CONTROLLER]
    assert len(executors) == 1
    return orchestrator, executors[0]


def _close_by_trailing_stop(executor, recorder):
    """Real trailing close path: quantized LIMIT sell, venue fill, then the native shutdown loop."""
    executor._status = RunnableStatus.RUNNING
    executor._strategy.current_timestamp = 1100
    executor.get_price = MagicMock(return_value=D('793.1'))
    executor.get_in_flight_order = MagicMock(return_value=None)
    executor.place_order = MagicMock(return_value='exit')
    executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    executor.place_order.assert_called_once()
    assert executor.place_order.call_args.kwargs['amount'] == SOLD
    executor._close_order.order = _order(
        'exit', TradeType.SELL, SOLD, D('793.1'), SOLD,
        DeductedFromReturnsTradeFee(flat_fees=[TokenAmount('USDC', SOLD * D('793.1') * D('.001'))]),
        OrderState.FILLED, order_type=OrderType.LIMIT).order
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        asyncio.get_event_loop().run_until_complete(executor.control_task())
    assert executor.status == RunnableStatus.TERMINATED, executor.status
    assert executor.close_type == CloseType.TRAILING_STOP


def _controller():
    provider = MagicMock()
    provider.time.return_value = 1200.0
    provider.get_available_balance.return_value = D('1000')
    provider.get_price_by_type.return_value = MID
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    provider.get_trading_rules.return_value = RULE
    controller_class = _native('controllers.directional_trading.ok_rsi', 'OKRSIController')
    bot = controller_class(config=_ok_rsi_config(
        id=CONTROLLER, connector_name='okx', trading_pair=PAIR, strict_hmm_mode=False,
        entry_acquisition_policy='down_only_maker_v1', max_total_position_usd=D('500'),
    ), market_data_provider=provider, actions_queue=asyncio.Queue())
    bot.positions_held = []
    bot.executors_info = []
    return bot


PROOF_FIELDS = ('order_ids', 'open_order_filled_amount_base', 'close_order_filled_amount_base',
                'remaining_position_amount_base', 'execution_receipt', 'execution_order_snapshots',
                'close_quantity', 'entry_lifecycle', 'held_position_orders')


def _closed_executor(recorder, *, restored, predecessor, chain):
    strategy = _strategy()
    executor = _filled_down_only_executor(strategy, predecessor=predecessor, chain=chain)
    orchestrator = None
    if restored:
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
            assert executor.suspend_for_restart() is True
            executor._advance_restart_suspend()
        orchestrator, executor = _boot(recorder, strategy)
        assert isinstance(executor, _down_only_class())
        assert executor._restart_restored['suspended'] is True
    else:
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
            recorder.store_or_update_executor(executor)  # running checkpoint
    _close_by_trailing_stop(executor, recorder)
    live = (orchestrator.get_executors_report()[CONTROLLER] if orchestrator is not None
            else [executor.executor_info])
    return executor, live


def _buy_gate(bot):
    bot.processed_data = {'signal': 1, 'timestamp': 1200.0, 'signal_state': {
        'condition_ok': True, 'timestamp': 1200.0, 'signal_score': 3, 'buy_entry_role': 'full'}}
    bot.can_create_executor(1)
    return bot._last_gate_reason.get(TradeType.BUY)


@pytest.mark.parametrize('chain', [False, True], ids=['single_attempt', 'v2_chain'])
@pytest.mark.parametrize('predecessor', [False, True], ids=['one_child', 'cancelled_predecessor'])
@pytest.mark.parametrize('restored', [True, False], ids=['restart_restored', 'never_restarted'])
def test_closed_down_only_exit_with_base_fee_dust_keeps_exposure_finite(recorder, restored, predecessor, chain):
    executor, live = _closed_executor(recorder, restored=restored, predecessor=predecessor, chain=chain)
    assert [info.id for info in live] == [executor.config.id]
    assert live[0].side == TradeType.BUY  # the live report carries the enum side
    custom = live[0].custom_info
    assert custom['execution_receipt']['residual_dust_base'] == DUST
    assert custom['remaining_position_amount_base'] == 0
    assert custom['open_order_filled_amount_base'] - custom['close_order_filled_amount_base'] == DUST
    assert custom['order_ids'] == (['entry-old'] if predecessor else []) + ['entry', 'exit']

    bot = _controller()
    bot.executors_info = live
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        # Native history still holds the pre-exit RUNNING checkpoint: production state from the fill
        # until the next restart. Deployed V2 (1eb132b63) returned Infinity here.
        assert bot._get_total_position_value_usd() == DUST * MID
        assert _buy_gate(bot) != 'exposure_unavailable'

        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
            recorder.store_or_update_executor(executor)  # orchestrator.store_executor writes the terminal row
        persisted = [row for row in recorder.get_executors_by_controller(CONTROLLER)
                     if row.id == executor.config.id]
        assert len(persisted) == 1
        # H2 falsified: the live report and the persisted terminal row agree on every proof field.
        assert {key: persisted[0].custom_info.get(key) for key in PROOF_FIELDS} == \
            {key: _json(custom.get(key)) for key in PROOF_FIELDS}
        assert bot._get_total_position_value_usd() == DUST * MID

        bot.executors_info = []  # after a restart only native history remains
        assert bot._get_total_position_value_usd() == DUST * MID
        assert _buy_gate(bot) != 'exposure_unavailable'


def _json(value):
    """Native persistence stores custom_info through JSON."""
    return json.loads(json.dumps(value, default=str))
