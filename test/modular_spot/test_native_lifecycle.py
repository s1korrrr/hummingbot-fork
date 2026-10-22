"""Native admission and accounting. Disposable recorder; no network or account calls."""
import asyncio
import importlib.util
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch

import pytest

if importlib.util.find_spec('hummingbot') is None:
    pytest.skip('native Hummingbot package/compiled runtime is unavailable', allow_module_level=True)

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor, native_config
from controllers.generic.modular_spot_core.inventory import inventory_view
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.modular_spot_core.planner import plan
from controllers.generic.modular_spot_core.state import DecisionState, DecisionStore
from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import OrderCancelledEvent, OrderFilledEvent
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


def config(**overrides):
    return ModularSpotControllerConfig(id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC', **overrides)


def disposable_recorder(tmp_path):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    previous = MarketsRecorder._shared_instance
    manager = SQLConnectionManager(ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS,
                                   db_path=str(tmp_path / 'm2-native.sqlite'))
    recorder = MarketsRecorder(manager, [], 'modular.yml', 'v2_with_controllers',
                               market_data_collection=MarketDataCollectionConfigMap(market_data_collection_enabled=False))
    recorder._previous_shared = previous
    return recorder


def release_recorder(recorder):
    recorder.stop()
    MarketsRecorder._shared_instance = getattr(recorder, '_previous_shared', None)


@pytest.fixture
def recorder(tmp_path):
    value = disposable_recorder(tmp_path)
    try:
        yield value
    finally:
        release_recorder(value)


def make_modular_executor(*, identifier='tranche-0', amount=D('1'), price=D('100')):
    strategy = MagicMock(spec=StrategyV2Base)
    strategy.current_timestamp = 1000
    strategy.controllers = {}
    connector = MagicMock()
    connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
    connector.get_available_balance.return_value = D('10')
    connector.get_fee.return_value = AddedToCostTradeFee(percent=D('0.001'))
    connector.in_flight_orders = {}
    strategy.connectors = {'okx': connector}
    cfg = native_config(config(), identifier=identifier, timestamp=120, price=price, amount=amount, cycle=0, stage=0)
    rule = TradingRule('BTC-USDC', min_order_size=D('.01'), min_price_increment=D('.01'),
                       min_base_amount_increment=D('.001'), min_notional_size=D('1'))
    with patch.object(PositionExecutor, 'get_trading_rules', return_value=rule):
        executor = ModularPositionExecutor(strategy, cfg)
    executor._status = RunnableStatus.RUNNING
    executor.get_price = MagicMock(return_value=price)
    executor.get_in_flight_order = MagicMock(return_value=None)
    executor.place_order = MagicMock(return_value='unexpected-order')
    return executor


def tracked(oid, side, fill, state, *, price=D('100'), amount=D('1'), fee=None, trade_id=None):
    order = InFlightOrder(client_order_id=oid, exchange_order_id=oid, trading_pair='BTC-USDC',
                          order_type=OrderType.LIMIT, trade_type=side, amount=amount, price=price,
                          creation_timestamp=1, initial_state=state)
    if D(fill):
        order.update_with_trade_update(TradeUpdate(
            trade_id=trade_id or oid, client_order_id=oid, exchange_order_id=oid, trading_pair='BTC-USDC',
            fill_price=price, fill_base_amount=D(fill), fill_quote_amount=D(fill) * price,
            fee=fee or AddedToCostTradeFee(), fill_timestamp=2))
    result = TrackedOrder(order_id=oid)
    result.order = order
    return result


def apply_fill(order, *, trade_id, fill, price=D('100'), fee=None):
    order.update_with_trade_update(TradeUpdate(
        trade_id=trade_id, client_order_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
        trading_pair='BTC-USDC', fill_price=price, fill_base_amount=D(fill), fill_quote_amount=D(fill) * price,
        fee=fee or AddedToCostTradeFee(), fill_timestamp=3))


def cancel(executor, order):
    order.order.current_state = OrderState.CANCELED
    executor.process_order_canceled_event(0, None, OrderCancelledEvent(1000, order.order_id))


def filled_event(tracked_order, *, trade_id, fill, price=D('100'), fee=None):
    return OrderFilledEvent(1000, tracked_order.order_id, 'BTC-USDC', TradeType.BUY, OrderType.LIMIT,
                            price, D(fill), fee or AddedToCostTradeFee(), trade_id,
                            tracked_order.order.exchange_order_id)


def view_of(executor):
    info = executor.executor_info
    return inventory_view([info], [], controller_id=info.controller_id,
                          connector=info.config.connector_name, pair=info.config.trading_pair)[info.id]


def plan_inputs(state, inventory, **changes):
    values = dict(fingerprint='a' * 64, budget=D('100'), now=1200,
                  market=MarketFeatures(1140, D('100'), D('29'), D('31'), False, False),
                  bid=D('100'), ask=D('100.1'), available_quote=D('100'),
                  amount_step=D('.001'), minimum_amount=D('.001'), minimum_notional=D('1'),
                  pnl=D('0'), signal=SignalPolicy(), acquisition=AcquisitionPolicy(), risk=RiskPolicy())
    values.update(changes)
    return plan(state, inventory, **values)


def test_partial_entry_cancel_keeps_fill_and_late_fill_is_owned():
    executor = make_modular_executor()
    entry = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    executor._open_order = entry
    cancel(executor, entry)
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert view_of(executor).remaining_base == D('.4')
    apply_fill(entry.order, trade_id='late', fill='.6')
    entry.order.current_state = OrderState.FILLED
    executor.process_order_filled_event(0, None, filled_event(entry, trade_id='late', fill='.6'))
    held = view_of(executor)
    assert held.remaining_base == D('1')
    assert held.entry_quote == D('100')
    assert executor.place_order.call_count == 0
    assert executor.close_type is None


def test_fee_correction_updates_owned_base_and_missing_receipt_holds_signal():
    executor = make_modular_executor()
    fee = AddedToCostTradeFee(flat_fees=[TokenAmount('USDC', D('0.08'))])
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED, fee=fee)
    before = view_of(executor)
    assert before.remaining_base == D('1')
    assert before.entry_quote == D('100')
    fill = executor._open_order.order.order_fills['entry']
    executor._open_order.order.order_fills['entry'] = fill._replace(
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount('BTC', D('0.001')), TokenAmount('USDC', D('0.08'))]))
    after = view_of(executor)
    assert after.remaining_base == D('.999')
    assert after.entry_quote == D('100')
    receipt = executor.executor_info.custom_info['execution_receipt']
    assert D(str(receipt['fees_quote'])) >= D('0.08')
    assert base_fee_amount(executor._open_order.order.to_json(), 'BTC') == D('0.001')
    assert executor._fees_complete()

    missing = executor.executor_info
    missing.custom_info = {**missing.custom_info}
    missing.custom_info.pop('execution_receipt')
    with pytest.raises(PolicyError):
        inventory_view([missing], [], controller_id='modular_spot_test', connector='okx', pair='BTC-USDC')
    state = DecisionState(fingerprint='a' * 64, candle=1080, claims=(executor.config.id,), committed_quote=D('100.5'))
    revised, proposal, reason = plan_inputs(state, {}, market=MarketFeatures(1140, D('100'), D('29'), D('31'), False, False))
    assert proposal is None and reason == 'execution_receipt_missing'


def test_incomplete_fees_cannot_be_waived_by_a_trailing_close():
    executor = make_modular_executor()
    entry = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    entry.order.order_fills.clear()
    executor._open_order = entry
    executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    executor.place_order.assert_not_called()
    assert executor.close_type is None
    executor.place_close_order_and_cancel_open_orders(CloseType.STOP_LOSS)
    assert executor.close_type == CloseType.POSITION_HOLD


def test_repeated_start_stop_preserves_checkpoint_and_trailing(recorder):
    provider = NS(ready=False, time=lambda: 1200, initialize_candles_feed=MagicMock())
    controller = ModularSpotController(config(), provider, asyncio.Queue())
    store = DecisionStore(recorder, controller.config.id, 'okx', 'BTC-USDC')
    state = DecisionState(fingerprint=controller.runtime.fingerprint, candle=120,
                          claims=('tranche-0',), committed_quote=D('100.5'))
    assert store.compare_and_swap(None, state)
    executor = make_modular_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    executor._trailing_stop_trigger_pct = D('.02')
    executor._trailing_stop_move_count = 3
    recorder.store_or_update_executor(executor)

    async def restart_twice():
        for _ in range(2):
            controller.start()
            await asyncio.sleep(0)
            controller.stop()
            tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            await asyncio.gather(*tasks)

    asyncio.run(restart_twice())
    loaded, _ = store.load()
    assert loaded == state
    restored = recorder.get_executors_by_controller(controller.config.id)[0]
    assert D(str(restored.custom_info['trailing_stop_trigger_pct'])) == D('.02')
    assert restored.custom_info['trailing_move_count'] == 3
    assert D(str(restored.custom_info['execution_receipt']['entry_gross_quote'])) == D('100')


def test_partial_exit_counts_once_and_never_market_dumps():
    executor = make_modular_executor()
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    close = tracked('close', TradeType.SELL, '.4', OrderState.PARTIALLY_FILLED)
    executor._close_order = close
    cancel(executor, close)
    assert view_of(executor).remaining_base == D('.6')
    assert view_of(executor).sold_base == D('.4')
    assert executor._close_order_type_for(CloseType.TRAILING_STOP) == OrderType.LIMIT
    assert executor._should_fallback_trailing_limit_close() is False
    executor.place_close_order_and_cancel_open_orders(CloseType.TIME_LIMIT)
    executor.place_order.assert_not_called()
    assert executor.close_type == CloseType.POSITION_HOLD


def test_per_tranche_quantity_cost_fees_and_tick_floor():
    executor = make_modular_executor(amount=D('1'), price=D('100'))
    fee = AddedToCostTradeFee(flat_fees=[TokenAmount('BTC', D('0.001')), TokenAmount('USDC', D('0.08'))])
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED, fee=fee)
    receipt = executor.executor_info.custom_info['execution_receipt']
    owned = view_of(executor)
    assert owned.remaining_base == D(str(receipt['remaining_owned_base'])) == D('.999')
    assert owned.entry_quote == D(str(receipt['entry_gross_quote'])) == D('100')
    assert D(str(receipt['entry_gross_base'])) == D('1')
    assert D(str(receipt['fees_quote'])) >= D('0.08')
    quote_only = make_modular_executor()
    quote_only._open_order = tracked(
        'entry', TradeType.BUY, '1', OrderState.FILLED,
        fee=AddedToCostTradeFee(flat_fees=[TokenAmount('USDC', D('0.08'))]))
    floor = quote_only._min_no_loss_sell_price()
    assert floor is not None
    assert floor == (floor / D('.01')).to_integral_value() * D('.01')
    assert floor >= D('100')


def test_permission_revoked_before_submission_ends_the_unsent_entry():
    """A revoked entry that never reached the venue ends; it must not wait forever for reconciliation."""
    executor = make_modular_executor()
    executor._strategy.controllers[executor.config.controller_id] = NS(
        entry_permission=lambda config, check_funds=True: False)
    assert executor._open_order is None and executor._entry_submitted is False
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor._entry_stop_reason == 'modular_entry_permission_revoked'
    assert executor._entry_lifecycle['state'] == 'ended_no_fill'
    assert executor.close_type == CloseType.EXPIRED
    assert executor.is_closed
