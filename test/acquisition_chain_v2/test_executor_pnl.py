"""Realized PnL of a closed V2 chain whose exit leaves only sub-lot base-fee dust.

Real OKX BTC-USDC fills recorded by rsibot-stack-v2 on 2026-09-24. Each chain
bought with base-denominated maker fees, so the net inventory was finer than the
1E-8 BTC lot; the trailing stop sold the largest valid lot and the remainder
(3.12E-10 BTC and 8.672E-9 BTC) can never be ordered. The executor used to move
such an exit into POSITION_HOLD, which zeroed the trade PnL and recorded the
profitable round trip as minus the fees (-0.0721 and -0.0760 USDC).

Every scenario drives public entry points only; nothing here sends orders or
touches a live stack.
"""
from decimal import Decimal
from test.acquisition_chain_v2.test_child_state import proof_for
from test.test_ok_rsi_down_only_entry_execution import make_config, make_executor
from unittest.mock import AsyncMock, MagicMock

import pytest

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, DeductedFromReturnsTradeFee, TokenAmount
from hummingbot.core.event.events import OrderFilledEvent, SellOrderCompletedEvent
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1
from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop, TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

D = Decimal
PAIR = 'BTC-USDC'
# OKX BTC-USDC spot rules at the time of the fills.
RULE = TradingRule(PAIR, min_order_size=D('0.00001'), min_price_increment=D('0.1'),
                   min_base_amount_increment=D('0.00000001'), min_notional_size=D('1'))
TRAILING = TrailingStop(activation_price=D('0.0060455894704488601'), trailing_delta=D('0.0020455894704488601'))

# Entry fills are (trade id, base amount, BTC fee); dust is the sub-lot remainder after the exit.
LIVE_EXITS = {
    'AUDGievR92oRcNY37tKdEzi5TD5ty43Z9bfiPvvQqdeN': dict(
        entry_price=D('84105.6'),
        entry_fills=(('5541826', D('0.00011'), D('8.8E-8')), ('5541828', D('0.00036461'), D('2.91688E-7'))),
        exit_amount=D('0.00047423'), exit_price=D('84719.3'), exit_fee=D('0.040176433639'), dust=D('3.12E-10')),
    '3LgGBsLxHzKHkaFmUbbVirk9C8XSfnZf2487GwKZ931f': dict(
        entry_price=D('83769'),
        entry_fills=(('5557819', D('0.00050166'), D('4.01328E-7')),),
        exit_amount=D('0.00050125'), exit_price=D('84561.5'), exit_fee=D('0.042386451875'), dust=D('8.672E-9')),
}


def buy_order(order_id, price, fills):
    amount = sum((base for _, base, _ in fills), D('0'))
    order = InFlightOrder(client_order_id=order_id, exchange_order_id=f'venue-{order_id}', trading_pair=PAIR,
                          order_type=OrderType.LIMIT_MAKER, trade_type=TradeType.BUY, amount=amount, price=price,
                          creation_timestamp=1, initial_state=OrderState.OPEN)
    for trade_id, base, fee in fills:
        order.update_with_trade_update(TradeUpdate(
            trade_id=trade_id, client_order_id=order_id, exchange_order_id=order.exchange_order_id,
            trading_pair=PAIR, fill_price=price, fill_base_amount=base, fill_quote_amount=base * price,
            fee=AddedToCostTradeFee(percent_token='BTC', flat_fees=[TokenAmount('BTC', fee)]), fill_timestamp=2))
    order.current_state = OrderState.FILLED
    tracked = TrackedOrder(order_id=order_id)
    tracked.order = order
    return tracked


def sell_order(order_id, amount, price, fee, filled):
    order = InFlightOrder(client_order_id=order_id, exchange_order_id=f'venue-{order_id}', trading_pair=PAIR,
                          order_type=OrderType.LIMIT, trade_type=TradeType.SELL, amount=amount,
                          price=price - D('10.2'), creation_timestamp=100, initial_state=OrderState.OPEN)
    trade_fee = DeductedFromReturnsTradeFee(percent_token='USDC', flat_fees=[TokenAmount('USDC', fee)])
    order.update_with_trade_update(TradeUpdate(
        trade_id=f'{order_id}-fill', client_order_id=order_id, exchange_order_id=order.exchange_order_id,
        trading_pair=PAIR, fill_price=price, fill_base_amount=filled, fill_quote_amount=filled * price,
        fee=trade_fee, fill_timestamp=101))
    order.current_state = OrderState.FILLED if filled == amount else OrderState.PARTIALLY_FILLED
    return order, trade_fee


def closed_chain_executor(entry_price, entry_fills):
    amount = sum((base for _, base, _ in entry_fills), D('0'))
    config = make_config(
        entry_chain_contract=ACQUISITION_CHAIN_V1, trading_pair=PAIR, amount=amount, entry_price=entry_price,
        decision_best_bid=entry_price, decision_best_ask=entry_price + D('0.1'), maximum_acceptable_price=entry_price,
        decision_price_step=D('0.1'), decision_atr_1m=D('40'), require_terminal_entry_reconciliation=True,
        triple_barrier_config=TripleBarrierConfig(
            open_order_type=OrderType.LIMIT_MAKER, open_order_limit_max_age_seconds=0, trailing_stop=TRAILING,
            trailing_stop_order_type=OrderType.LIMIT, trailing_stop_limit_max_age_seconds=3))
    executor, connector, prices = make_executor(config=config, bid=entry_price, ask=entry_price + D('0.1'))
    executor.trading_rules = RULE
    executor._sleep = AsyncMock()
    connector.get_fee = MagicMock(return_value=AddedToCostTradeFee(percent=D('0.001')))
    executor.get_available_balance = MagicMock(return_value=D('0.0029364056'))
    executor.control_open_order()
    filled = buy_order('parent', entry_price, entry_fills)
    executor._open_order = filled
    executor.control_open_order()
    executor._store_entry_reconciliation([filled], [proof_for(filled)])
    assert executor.get_custom_info()['acquisition_outcome'] == 'fully_acquired'
    return executor, connector, prices


def trailing_exit(executor, connector, prices, exit_price, exit_fee, filled):
    prices[PriceType.BestBid] = exit_price
    prices[PriceType.BestAsk] = exit_price + D('0.1')
    executor._strategy.current_timestamp = 100
    executor.place_order = MagicMock(return_value='exit')
    executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
    kwargs = executor.place_order.call_args.kwargs
    assert kwargs['side'] is TradeType.SELL
    order, trade_fee = sell_order('exit', kwargs['amount'], exit_price, exit_fee, filled)
    executor.get_in_flight_order = MagicMock(side_effect=lambda connector_name, order_id: {'exit': order}.get(order_id))
    executor.process_order_filled_event(0, connector, OrderFilledEvent(
        101, 'exit', PAIR, TradeType.SELL, OrderType.LIMIT, exit_price, filled, trade_fee,
        exchange_trade_id='exit-fill', exchange_order_id=order.exchange_order_id))
    if filled == kwargs['amount']:
        executor.process_order_completed_event(0, connector, SellOrderCompletedEvent(
            101, 'exit', 'BTC', 'USDC', filled, filled * exit_price, OrderType.LIMIT, order.exchange_order_id))
    return kwargs['amount']


@pytest.mark.asyncio
@pytest.mark.parametrize('executor_id', sorted(LIVE_EXITS))
async def test_full_lot_exit_with_sub_lot_fee_dust_realizes_cash_pnl(executor_id):
    case = LIVE_EXITS[executor_id]
    executor, connector, prices = closed_chain_executor(case['entry_price'], case['entry_fills'])
    gross_base = sum((base for _, base, _ in case['entry_fills']), D('0'))
    base_fee = sum((fee for _, _, fee in case['entry_fills']), D('0'))
    assert executor.open_filled_amount == gross_base - base_fee
    assert executor.entry_price == case['entry_price']

    sold = trailing_exit(executor, connector, prices, case['exit_price'], case['exit_fee'], case['exit_amount'])
    assert sold == case['exit_amount']
    assert executor._close_quantity['residual_base'] == case['dust']
    assert case['dust'] < RULE.min_base_amount_increment

    for _ in range(3):
        await executor.control_task()

    assert executor.status == RunnableStatus.TERMINATED
    assert executor.close_type == CloseType.TRAILING_STOP
    # Cash truth: quote received net of the taker fee, minus quote paid for the gross lot.
    # The base-denominated maker fee is already the difference between the lot bought
    # and the lot sold; it is reported in cum_fees_quote, not subtracted a second time.
    proceeds = case['exit_amount'] * case['exit_price']
    expected_net = proceeds - case['exit_fee'] - gross_base * case['entry_price']
    assert expected_net > 0
    assert executor.cum_fees_quote == base_fee * case['entry_price'] + case['exit_fee']
    assert abs(executor.net_pnl_quote - expected_net) < D('1e-12')
    assert abs(executor.trade_pnl_quote - (expected_net + executor.cum_fees_quote)) < D('1e-12')
    assert executor.net_pnl_pct == executor.net_pnl_quote / executor.open_filled_amount_quote

    custom = executor.get_custom_info()
    assert custom['exit_trigger_reason'] == 'TRAILING_STOP'
    assert custom['held_position_orders'] == []
    assert custom['remaining_position_amount_base'] == case['dust']
    receipt = custom['execution_receipt']
    assert receipt['accounting_owner'] == 'executor'
    assert receipt['residual_dust_base'] == case['dust']
    assert receipt['exit_gross_quote'] == proceeds


@pytest.mark.asyncio
async def test_residual_of_a_whole_lot_below_minimum_still_holds_inventory():
    """The write-off is only for residue finer than one lot; a real lot is held, as before."""
    case = LIVE_EXITS['AUDGievR92oRcNY37tKdEzi5TD5ty43Z9bfiPvvQqdeN']
    executor, connector, prices = closed_chain_executor(case['entry_price'], case['entry_fills'])
    partial = case['exit_amount'] - D('0.00000500')
    trailing_exit(executor, connector, prices, case['exit_price'], case['exit_fee'], partial)
    executor._close_order.order.current_state = OrderState.CANCELED

    for _ in range(3):
        await executor.control_task()

    assert executor.status == RunnableStatus.TERMINATED
    assert executor.close_type == CloseType.POSITION_HOLD
    custom = executor.get_custom_info()
    assert custom['remaining_position_amount_base'] == D('0.00000500') + case['dust']
    assert custom['execution_receipt']['accounting_owner'] == 'held_position'
    assert custom['execution_receipt']['residual_dust_base'] == D('0')
    assert [order['trade_type'] for order in custom['held_position_orders']] == ['BUY', 'SELL']
    assert executor.net_pnl_quote == -executor.cum_fees_quote
