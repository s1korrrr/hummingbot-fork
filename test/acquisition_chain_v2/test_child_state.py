import asyncio
from decimal import Decimal
from test.test_ok_rsi_down_only_entry_execution import make_config, make_executor, tracked_order
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import OrderCancelledEvent
from hummingbot.strategy_v2.executors.acquisition_chain import validate_acquisition_chain


@pytest.mark.asyncio
async def test_predecessor_cancel_does_not_stop_live_replacement():
    executor, connector, _ = make_executor(config=make_config(entry_chain_contract='v2_bounded_acquisition_v1'))
    executor._completed_open_orders = [tracked_order('parent', state=OrderState.CANCELED)]
    executor._open_order = tracked_order('child-1', price='99', fill='0.2', state=OrderState.PARTIALLY_FILLED)
    executor._entry_submitted = True
    executor._entry_reprice_count = 1
    executor._entry_lifecycle.update(order_id='child-1', cancel_event_timestamp=1, cancel_event_order_id='parent')
    await executor._reconcile_expired_entry()
    assert executor._entry_stop_reason is None
    assert not executor._open_order_entry_expired
    executor._cancel_entry_expiry_reconciliation()


def proof_for(tracked):
    order = tracked.order
    return dict(schema=1, source='okx_terminal_status_v1', order_id=tracked.order_id,
                exchange_order_id=order.exchange_order_id, trading_pair=order.trading_pair,
                side='BUY', amount=str(order.amount), filled_base=str(order.executed_amount_base),
                state=order.current_state.name, trade_ids=sorted(order.order_fills))


def terminal_response(connector, tracked):
    connector._request_order_status = AsyncMock(return_value=OrderUpdate(
        trading_pair='ETH-USDC', update_timestamp=12, new_state=tracked.order.current_state,
        client_order_id=tracked.order_id, exchange_order_id=tracked.order.exchange_order_id,
        misc_updates={'terminal_reconciliation': proof_for(tracked)}))


@pytest.mark.asyncio
async def test_real_submission_cancel_repair_and_full_fill_emit_closed_valid_chain():
    executor, connector, _ = make_executor(config=make_config(entry_chain_contract='v2_bounded_acquisition_v1'))
    executor.control_open_order()
    first = tracked_order('parent', state=OrderState.CANCELED)
    first.order.cancel_source = '31'
    executor._open_order = first
    terminal_response(connector, first)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, 'parent'))
    await executor._reconcile_expired_entry()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await executor._reconcile_expired_entry()
    assert executor._entry_child_submission_authorized
    assert not executor._chain_membership_closed()
    executor._strategy.current_timestamp = 10
    executor.control_open_order()
    assert executor.place_order.call_count == 2
    assert executor._open_order.order_id == 'child-1'
    assert 'cancel_event_timestamp' not in executor._entry_lifecycle
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(13, 'parent'))
    assert 'cancel_event_timestamp' not in executor._entry_lifecycle
    child = tracked_order('child-1', fill='.2', state=OrderState.PARTIALLY_FILLED,
                          fee=AddedToCostTradeFee(flat_fees=[TokenAmount('ETH', Decimal('.0002'))]))
    executor._open_order = child
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(14, 'parent'))
    await executor._reconcile_expired_entry()
    assert executor._entry_stop_reason is None
    child.order.update_with_trade_update(TradeUpdate(
        'second-fill', 'child-1', 'exchange-child-1', 'ETH-USDC', 15,
        Decimal('100'), Decimal('.8'), Decimal('80'),
        AddedToCostTradeFee(flat_fees=[TokenAmount('ETH', Decimal('.0008'))])))
    child.order.current_state = OrderState.FILLED
    assert executor.open_filled_amount == Decimal('.999')
    executor.control_open_order()
    executor._store_entry_reconciliation([executor._open_order], [proof_for(executor._open_order)])
    assert executor._chain_membership_closed()
    assert len(executor._entry_reconciliation['orders']) == 2
    custom = executor.get_custom_info()
    validate_acquisition_chain(executor.config, custom, executor._entry_reconciliation)
    executor.control_open_order()
    assert executor.place_order.call_count == 2
    assert custom['further_acquisition_permission'] == 'closed'


@pytest.mark.asyncio
async def test_old_async_completion_cannot_be_consumed_as_current_child():
    executor, _, _ = make_executor(config=make_config(entry_chain_contract='v2_bounded_acquisition_v1'))
    executor._open_order = tracked_order('child-1', fill='.2', state=OrderState.PARTIALLY_FILLED)
    old = asyncio.create_task(asyncio.sleep(0))
    await old
    executor._entry_expiry_reconciliation_task = old
    executor._entry_reconciliation_order_id = 'parent'
    await executor._reconcile_expired_entry()
    assert executor._entry_expiry_reconciliation_task is None
    assert executor._entry_stop_reason is None
    assert not executor._open_order_expiry_reconciled


def test_ambiguous_submission_holds_and_never_retries():
    executor, _, _ = make_executor(config=make_config(entry_chain_contract='v2_bounded_acquisition_v1'))
    executor.place_order.side_effect = RuntimeError('connection lost after submit')
    with pytest.raises(RuntimeError):
        executor.control_open_order()
    executor.control_open_order()
    assert executor.place_order.call_count == 1
    assert executor._entry_submission_uncertain
    assert not executor._chain_membership_closed()
    assert executor.get_custom_info()['further_acquisition_permission'] == 'held'


def test_late_predecessor_fill_requires_hold_without_selling():
    executor, _, _ = make_executor(config=make_config(entry_chain_contract='v2_bounded_acquisition_v1'))
    executor._completed_open_orders = [tracked_order('parent', state=OrderState.CANCELED)]
    executor._open_order = tracked_order('child-1', fill='.2', state=OrderState.PARTIALLY_FILLED)
    executor.process_order_filled_event(0, None, SimpleNamespace(order_id='parent'))
    assert executor._resume_conflicting_fill is not None
    assert executor._entry_stop_reason == 'predecessor_fill_conflict'
    assert executor.get_custom_info()['further_acquisition_permission'] == 'held'
    assert not executor._chain_membership_closed()
    executor.control_open_order()
    executor.control_trailing_stop()
    executor.place_order.assert_not_called()


def test_partial_fill_after_terminal_cancel_never_authorizes_successor():
    executor, _, _ = make_executor(config=make_config(entry_chain_contract='v2_bounded_acquisition_v1'))
    executor._open_order = tracked_order('parent', fill='.2', state=OrderState.CANCELED)
    executor._entry_submitted = True
    executor.control_open_order()
    assert not executor._entry_child_submission_authorized
    executor.place_order.assert_not_called()


def test_changed_terminal_proof_holds_without_overwriting_original():
    executor, _, _ = make_executor(config=make_config(entry_chain_contract='v2_bounded_acquisition_v1'))
    executor.control_open_order()
    first = tracked_order('parent', state=OrderState.CANCELED)
    executor._open_order = first
    proof = proof_for(first)
    executor._store_entry_reconciliation([first], [proof])
    changed = dict(proof, trade_ids=['unknown-fill'])
    with pytest.raises(ValueError, match='proof changed'):
        executor._store_entry_reconciliation([first], [changed])
    assert executor._entry_reconciliation == proof
    assert executor._resume_conflicting_fill is not None
    before = executor.place_order.call_count
    executor.control_open_order()
    assert executor.place_order.call_count == before


def test_legacy_single_attempt_remains_without_chain_opt_in():
    executor, _, _ = make_executor()
    executor.control_open_order()
    assert 'acquisition_chain' not in executor.get_custom_info()
    executor._entry_lifecycle.update(cancel_event_timestamp=1)
    assert executor._entry_cancel_observed()
