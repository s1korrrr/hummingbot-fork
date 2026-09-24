"""Validator and executor child-state edges for one bounded V2 acquisition chain.

Every executor scenario drives public entry points (control_open_order, the
order event handlers, _reconcile_expired_entry, _store_entry_reconciliation and
get_custom_info); nothing here sends orders or touches a live stack.
"""
import asyncio
import copy
import json
from decimal import Decimal
from test.acquisition_chain_v2.test_child_state import proof_for, terminal_response
from test.acquisition_chain_v2.test_funding_chain import chain_case
from test.test_ok_rsi_down_only_entry_execution import make_config, make_executor, tracked_order

import pytest

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import OrderCancelledEvent, OrderFilledEvent
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1, validate_acquisition_chain
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

V2 = ACQUISITION_CHAIN_V1
# Keys the chain contract adds or relies on; the recorder persists them through
# ExecutorInfo.model_dump_json, so they must already be JSON-native values.
CHAIN_KEYS = ('acquisition_chain', 'entry_child_lifecycles', 'acquisition_outcome',
              'further_acquisition_permission', 'entry_acquisition', 'entry_lifecycle',
              'entry_reconciliation', 'execution_order_snapshots', 'resume_conflicting_fill')


def v2_executor(**config_changes):
    return make_executor(config=make_config(entry_chain_contract=V2, **config_changes))


async def reconcile_zero_fill_cancel(executor, connector, tracked, now):
    """Deliver the venue cancel for the live child and obtain its terminal proof."""
    tracked.order.current_state = OrderState.CANCELED
    terminal_response(connector, tracked)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(now, tracked.order_id))
    await executor._reconcile_expired_entry()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await executor._reconcile_expired_entry()


async def downward_reprice(executor, connector, prices, *, bid, now, next_price):
    """Cancel the resting child for a lower target, prove zero fill, submit the successor."""
    predecessor = executor._open_order
    executor._strategy.current_timestamp = now
    prices[PriceType.BestBid] = Decimal(bid)
    executor.control_open_order()
    assert executor._entry_replacement_purpose == 'downward_reprice'
    assert executor._entry_replacement_of_order_id == predecessor.order_id
    await reconcile_zero_fill_cancel(executor, connector, predecessor, now)
    assert executor._entry_child_submission_authorized
    assert executor._open_order is None
    executor.control_open_order()
    child = executor._open_order
    assert child is not None and child.order_id != predecessor.order_id
    assert executor.place_order.call_args.kwargs['price'] == Decimal(next_price)
    executor._open_order = tracked_order(child.order_id, price=next_price)
    return executor._open_order


def fill_completely(executor, tracked, price):
    """Replace the live child handle with its fully filled native order and close the chain."""
    executor._open_order = tracked_order(tracked.order_id, price=price, fill='1', state=OrderState.FILLED)
    executor.control_open_order()
    executor._store_entry_reconciliation([executor._open_order], [proof_for(executor._open_order)])
    return executor._open_order


async def three_child_chain(executor, connector, prices):
    executor.control_open_order()
    executor._open_order = tracked_order('parent')
    first = await downward_reprice(executor, connector, prices, bid='99.90', now=10, next_price='99.90')
    second = await downward_reprice(executor, connector, prices, bid='99.80', now=20, next_price='99.80')
    assert (first.order_id, second.order_id) == ('child-1', 'child-2')
    fill_completely(executor, second, '99.80')
    return executor.get_custom_info()


@pytest.mark.asyncio
async def test_three_child_downward_chain_closes_with_decreasing_prices_and_generations():
    executor, connector, prices = v2_executor()
    custom = await three_child_chain(executor, connector, prices)
    chain = validate_acquisition_chain(executor.config, custom, executor._entry_reconciliation)
    assert chain['membership_closed'] and chain['successor_pending'] is None
    assert [child['generation'] for child in chain['children']] == [0, 1, 2]
    assert [child['purpose'] for child in chain['children']] == ['initial', 'downward_reprice', 'downward_reprice']
    assert [child['predecessor_id'] for child in chain['children']] == [None, 'parent', 'child-1']
    limit_prices = [Decimal(child['limit_price']) for child in chain['children']]
    assert limit_prices == [Decimal('100'), Decimal('99.90'), Decimal('99.80')]
    assert all(later < earlier for earlier, later in zip(limit_prices, limit_prices[1:]))
    assert len(chain['children']) <= executor.config.entry_max_downward_reprices + 1
    assert chain['gross_filled_base'] == '1'
    assert executor.place_order.call_count == 3
    assert custom['acquisition_outcome'] == 'fully_acquired'
    assert custom['further_acquisition_permission'] == 'closed'
    assert len(executor._entry_reconciliation['orders']) == 3
    executor.control_open_order()
    assert executor.place_order.call_count == 3


@pytest.mark.asyncio
async def test_reprice_budget_exhaustion_never_submits_beyond_the_bound():
    executor, connector, prices = v2_executor(entry_max_downward_reprices=1)
    executor.control_open_order()
    executor._open_order = tracked_order('parent')
    child = await downward_reprice(executor, connector, prices, bid='99.90', now=10, next_price='99.90')
    assert executor._entry_reprice_count == 1
    cancels_before = executor._strategy.cancel.call_count
    # A further qualifying decline holds the resting child instead of cancelling it.
    executor._strategy.current_timestamp = 20
    prices[PriceType.BestBid] = Decimal('99.80')
    executor.control_open_order()
    assert executor.get_custom_info()['entry_acquisition']['state'] == 'reprice_budget_exhausted'
    assert executor._strategy.cancel.call_count == cancels_before
    assert executor._entry_replacement_purpose is None
    # The venue then post-only cancels the child: repair is authorized but the
    # budget forbids another submission, so the chain closes terminally.
    child.order.cancel_source = '31'
    await reconcile_zero_fill_cancel(executor, connector, child, 21)
    assert executor._entry_child_submission_authorized
    executor._strategy.current_timestamp = 30
    executor.control_open_order()
    assert executor.place_order.call_count == 2
    assert executor._entry_stop_reason == 'reprice_budget_exhausted'
    assert not executor._entry_child_submission_authorized
    custom = executor.get_custom_info()
    assert custom['further_acquisition_permission'] == 'closed'
    chain = validate_acquisition_chain(executor.config, custom, executor._entry_reconciliation)
    assert chain['membership_closed']
    assert len(chain['children']) == executor.config.entry_max_downward_reprices + 1
    executor.control_open_order()
    assert executor.place_order.call_count == 2


@pytest.mark.asyncio
async def test_single_child_partial_fill_then_cancel_closes_as_partially_acquired():
    executor, connector, _ = v2_executor()
    executor.control_open_order()
    parent = tracked_order('parent', fill='0.4', state=OrderState.CANCELED)
    executor._open_order = parent
    await reconcile_zero_fill_cancel(executor, connector, parent, 12)
    assert executor._open_order is parent
    assert executor._entry_stop_reason == 'exchange_canceled'
    assert executor.place_order.call_count == 1
    custom = executor.get_custom_info()
    chain = validate_acquisition_chain(executor.config, custom, executor._entry_reconciliation, require_closed=True)
    assert chain['membership_closed'] and chain['successor_pending'] is None
    assert len(chain['children']) == 1
    assert chain['gross_filled_base'] == '0.4'
    assert custom['acquisition_outcome'] == 'partially_acquired'
    assert custom['further_acquisition_permission'] == 'closed'
    assert custom['entry_acquisition']['state'] == 'managing_fill'
    assert executor.status is RunnableStatus.RUNNING
    executor.control_open_order()
    assert executor.place_order.call_count == 1


def test_duplicate_fill_event_on_the_current_child_counts_once():
    executor, connector, _ = v2_executor()
    executor.control_open_order()
    child = tracked_order('parent', fill='0.2', state=OrderState.PARTIALLY_FILLED)
    executor._open_order = child
    executor.get_in_flight_order.return_value = child.order
    update = child.order.order_fills['fill-parent']
    event = OrderFilledEvent(1, 'parent', 'ETH-USDC', TradeType.BUY, OrderType.LIMIT_MAKER,
                             update.fill_price, update.fill_base_amount, update.fee,
                             exchange_trade_id='fill-parent', exchange_order_id='exchange-parent')
    executor.process_order_filled_event(0, connector, event)
    before = executor.get_custom_info()
    assert child.order.update_with_trade_update(update) is False
    executor.process_order_filled_event(0, connector, event)
    after = executor.get_custom_info()
    assert child.order.executed_amount_base == Decimal('0.2')
    assert len(child.order.order_fills) == 1
    assert before['acquisition_chain']['gross_filled_base'] == after['acquisition_chain']['gross_filled_base'] == '0.2'
    assert after['entry_acquisition']['cumulative_filled_base'] == '0.2'
    assert after['execution_receipt']['entry_gross_base'] == Decimal('0.2')
    assert executor._resume_conflicting_fill is None
    assert executor._entry_stop_reason is None


def test_cancel_event_for_a_foreign_order_is_ignored():
    executor, connector, _ = v2_executor()
    executor.control_open_order()
    executor._open_order = tracked_order('parent')
    executor.control_open_order()
    lifecycle_before = copy.deepcopy(executor._entry_lifecycle)
    children_before = copy.deepcopy(executor._entry_child_lifecycles)
    chain_before = executor.get_custom_info()['acquisition_chain']
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(12, 'foreign-order'))
    assert executor._entry_lifecycle == lifecycle_before
    assert executor._entry_child_lifecycles == children_before
    assert executor.get_custom_info()['acquisition_chain'] == chain_before
    assert executor._entry_stop_reason is None
    assert executor._resume_conflicting_fill is None
    assert executor._entry_replacement_purpose is None
    assert executor.status is RunnableStatus.RUNNING
    assert executor._open_order.order_id == 'parent'
    assert executor._open_order.order.current_state is OrderState.OPEN
    executor._strategy.cancel.assert_not_called()
    executor.control_open_order()
    assert executor.place_order.call_count == 1
    assert executor.get_custom_info()['further_acquisition_permission'] == 'open'


@pytest.mark.asyncio
async def test_late_predecessor_cancel_after_closed_chain_changes_nothing():
    executor, connector, prices = v2_executor()
    custom_before = await three_child_chain(executor, connector, prices)
    validate_acquisition_chain(executor.config, custom_before, executor._entry_reconciliation)
    lifecycle_before = copy.deepcopy(executor._entry_lifecycle)
    stop_before = executor._entry_stop_reason
    status_before = executor.status
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(99, 'parent'))
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(100, 'child-1'))
    custom_after = executor.get_custom_info()
    assert executor._entry_lifecycle == lifecycle_before
    assert executor._entry_stop_reason == stop_before
    assert executor.status is status_before
    assert executor._open_order.order_id == 'child-2'
    assert executor._open_order.order.current_state is OrderState.FILLED
    assert custom_after['acquisition_chain'] == custom_before['acquisition_chain']
    assert custom_after['acquisition_chain']['membership_closed']
    assert custom_after['further_acquisition_permission'] == 'closed'
    assert custom_after['entry_child_lifecycles']['parent']['cancel_event_timestamp'] == 99
    assert custom_after['entry_child_lifecycles']['parent']['cancel_event_order_id'] == 'parent'
    assert custom_after['entry_child_lifecycles']['child-1']['cancel_event_timestamp'] == 100
    validate_acquisition_chain(executor.config, custom_after, executor._entry_reconciliation)
    executor.control_open_order()
    assert executor.place_order.call_count == 3
    executor._strategy.sell.assert_not_called()


def _restore(custom, index):
    return InFlightOrder.from_json(custom['execution_order_snapshots'][index])


def _buy_fill(order, trade_id, base, price, timestamp):
    order.update_with_trade_update(TradeUpdate(trade_id, order.client_order_id, order.exchange_order_id,
                                               'BTC-USDC', timestamp, Decimal(price), Decimal(base),
                                               Decimal(price) * Decimal(base), AddedToCostTradeFee()))


def test_validator_rejects_a_trade_id_shared_by_two_children():
    config, custom, proofs = chain_case()
    first = _restore(custom, 0)
    _buy_fill(first, 'fill-1', '.1', '100', 3)
    first.current_state = OrderState.CANCELED
    custom['execution_order_snapshots'][0] = first.to_json()
    custom['acquisition_chain']['gross_filled_base'] = '1.1'
    proofs[0].update(filled_base='.1', trade_ids=['fill-1'])
    with pytest.raises(ValueError, match='trade identity is duplicated'):
        validate_acquisition_chain(config, custom, proofs)


def test_validator_rejects_a_predecessor_proof_with_nonzero_fill():
    config, custom, proofs = chain_case()
    first = _restore(custom, 0)
    _buy_fill(first, 'old-fill', '.1', '100', 3)
    first.current_state = OrderState.CANCELED
    custom['execution_order_snapshots'][0] = first.to_json()
    second = InFlightOrder('second', 'BTC-USDC', OrderType.LIMIT_MAKER, TradeType.BUY,
                           Decimal('.9'), 2, Decimal('99'), exchange_order_id='venue-second')
    _buy_fill(second, 'fill-2', '.9', '99', 4)
    second.current_state = OrderState.FILLED
    custom['execution_order_snapshots'][1] = second.to_json()
    custom['acquisition_chain']['children'][1]['submitted_base'] = '.9'
    proofs[0].update(filled_base='.1', trade_ids=['old-fill'])
    proofs[1].update(amount='.9', filled_base='.9', trade_ids=['fill-2'])
    with pytest.raises(ValueError, match='predecessor lacks zero-fill terminal proof'):
        validate_acquisition_chain(config, custom, proofs)


def test_validator_rejects_a_closed_chain_missing_one_terminal_proof():
    config, custom, proofs = chain_case()
    with pytest.raises(ValueError, match='terminal proof coverage differs'):
        validate_acquisition_chain(config, custom, proofs[1:], require_closed=True)
    with pytest.raises(ValueError, match='terminal proof coverage differs'):
        validate_acquisition_chain(config, custom, proofs[:1], require_closed=True)


def test_validator_rejects_a_child_priced_above_the_maximum():
    config, custom, proofs = chain_case()
    custom['acquisition_chain']['children'][0]['limit_price'] = '100.01'
    first = _restore(custom, 0)
    first.price = Decimal('100.01')
    custom['execution_order_snapshots'][0] = first.to_json()
    with pytest.raises(ValueError, match='child exceeds authorization'):
        validate_acquisition_chain(config, custom, proofs)


def test_validator_rejects_a_downward_reprice_at_the_predecessor_price():
    config, custom, proofs = chain_case()
    custom['acquisition_chain']['children'][1]['limit_price'] = '100'
    second = _restore(custom, 1)
    second.price = Decimal('100')
    custom['execution_order_snapshots'][1] = second.to_json()
    with pytest.raises(ValueError, match='child exceeds authorization'):
        validate_acquisition_chain(config, custom, proofs)


def test_validator_rejects_a_child_larger_than_the_remaining_authorization():
    config, custom, proofs = chain_case()
    first = _restore(custom, 0)
    _buy_fill(first, 'old-fill', '.1', '100', 3)
    first.current_state = OrderState.CANCELED
    custom['execution_order_snapshots'][0] = first.to_json()
    second = InFlightOrder('second', 'BTC-USDC', OrderType.LIMIT_MAKER, TradeType.BUY,
                           Decimal('.95'), 2, Decimal('99'), exchange_order_id='venue-second',
                           initial_state=OrderState.CANCELED)
    _buy_fill(second, 'fill-2', '.5', '99', 4)
    second.current_state = OrderState.CANCELED
    custom['execution_order_snapshots'][1] = second.to_json()
    custom['acquisition_chain']['children'][1]['submitted_base'] = '.95'
    custom['acquisition_chain']['gross_filled_base'] = '.6'
    proofs[0].update(filled_base='.1', trade_ids=['old-fill'])
    proofs[1].update(amount='.95', filled_base='.5', state='CANCELED', trade_ids=['fill-2'])
    with pytest.raises(ValueError, match='submitted order exceeds authorization'):
        validate_acquisition_chain(config, custom, proofs)


def test_validator_rejects_gross_fill_that_differs_from_snapshots():
    config, custom, proofs = chain_case()
    custom['acquisition_chain']['gross_filled_base'] = '0.5'
    with pytest.raises(ValueError, match='gross fill snapshot differs'):
        validate_acquisition_chain(config, custom, proofs)


def test_validator_rejects_a_chain_owned_by_another_parent():
    config, custom, proofs = chain_case()
    custom['acquisition_chain']['parent_id'] = 'parent-eth'
    with pytest.raises(ValueError, match='identity or membership is invalid'):
        validate_acquisition_chain(config, custom, proofs)


def test_validator_rejects_closed_membership_with_a_pending_successor():
    config, custom, proofs = chain_case()
    custom['acquisition_chain']['successor_pending'] = {'predecessor_id': 'second', 'purpose': 'downward_reprice'}
    assert custom['acquisition_chain']['membership_closed'] is True
    with pytest.raises(ValueError, match='still authorizes a successor'):
        validate_acquisition_chain(config, custom, proofs, require_closed=False)


def test_validator_rejects_a_snapshot_filled_beyond_its_submitted_amount():
    config, custom, proofs = chain_case()
    second = _restore(custom, 1)
    _buy_fill(second, 'fill-3', '.5', '99', 5)
    assert second.executed_amount_base > second.amount
    custom['execution_order_snapshots'][1] = second.to_json()
    custom['acquisition_chain']['gross_filled_base'] = '1.5'
    proofs[1].update(filled_base='1.5', trade_ids=['fill-1', 'fill-2', 'fill-3'])
    with pytest.raises(ValueError, match='native child scope or bound differs'):
        validate_acquisition_chain(config, custom, proofs)


def assert_json_native(executor, phase):
    custom = executor.get_custom_info()
    chain_view = {key: custom[key] for key in CHAIN_KEYS}
    encoded = json.dumps(chain_view)
    assert json.loads(encoded) == chain_view, phase
    persisted = ExecutorInfo.model_validate_json(executor.executor_info.model_dump_json())
    assert persisted.custom_info['acquisition_chain'] == custom['acquisition_chain'], phase
    assert persisted.config.entry_chain_contract == V2, phase
    return custom


@pytest.mark.asyncio
async def test_custom_info_is_json_native_at_every_chain_phase():
    executor, connector, prices = v2_executor()
    assert_json_native(executor, 'fresh')
    executor.control_open_order()
    executor._open_order = tracked_order('parent')
    assert_json_native(executor, 'after first submission')
    executor._strategy.current_timestamp = 10
    prices[PriceType.BestBid] = Decimal('99.90')
    executor.control_open_order()
    parent = executor._open_order
    parent.order.current_state = OrderState.CANCELED
    terminal_response(connector, parent)
    executor.process_order_canceled_event(0, connector, OrderCancelledEvent(10, 'parent'))
    custom = assert_json_native(executor, 'after predecessor cancel')
    assert custom['acquisition_chain']['successor_pending'] == {'predecessor_id': 'parent', 'purpose': 'downward_reprice'}
    await executor._reconcile_expired_entry()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await executor._reconcile_expired_entry()
    assert executor._entry_child_submission_authorized
    custom = assert_json_native(executor, 'after child authorized')
    assert custom['entry_lifecycle']['state'] == 'child_authorized'
    executor.control_open_order()
    executor._open_order = tracked_order('child-1', price='99.90')
    custom = assert_json_native(executor, 'after replacement submitted')
    assert custom['acquisition_chain']['successor_pending'] is None
    executor._open_order = tracked_order('child-1', price='99.90', fill='0.3', state=OrderState.PARTIALLY_FILLED)
    custom = assert_json_native(executor, 'after partial fill')
    assert custom['acquisition_outcome'] == 'partially_acquired'
    fill_completely(executor, executor._open_order, '99.90')
    custom = assert_json_native(executor, 'after full fill and closed chain')
    assert custom['acquisition_chain']['membership_closed']
    validate_acquisition_chain(executor.config, custom, executor._entry_reconciliation)


@pytest.fixture
def recorder(tmp_path):
    from test.modular_spot.test_native_lifecycle import disposable_recorder, release_recorder
    value = disposable_recorder(tmp_path)
    try:
        yield value
    finally:
        release_recorder(value)


def assert_recorder_round_trip(recorder, executor):
    recorder.store_or_update_executor(executor)
    rows = recorder.get_all_executors()
    assert len(rows) == 1
    restored = rows[0]
    expected = executor.get_custom_info()
    assert restored.custom_info['acquisition_chain'] == expected['acquisition_chain']
    assert restored.custom_info['entry_child_lifecycles'] == expected['entry_child_lifecycles']
    assert restored.custom_info['further_acquisition_permission'] == expected['further_acquisition_permission']
    assert restored.custom_info['entry_reconciliation'] == expected['entry_reconciliation']
    assert type(restored.config) is type(executor.config)
    assert restored.config.entry_chain_contract == V2
    assert restored.config == executor.config
    return restored


@pytest.mark.asyncio
async def test_recorder_round_trips_mid_chain_and_closed_chain_states(recorder):
    executor, connector, prices = v2_executor(id='chain-round-trip', controller_id='owner')
    executor.control_open_order()
    executor._open_order = tracked_order('parent')
    executor._strategy.current_timestamp = 10
    prices[PriceType.BestBid] = Decimal('99.90')
    executor.control_open_order()
    parent = executor._open_order
    await reconcile_zero_fill_cancel(executor, connector, parent, 10)
    assert executor._entry_child_submission_authorized
    mid = assert_recorder_round_trip(recorder, executor)
    pending = {'predecessor_id': 'parent', 'purpose': 'downward_reprice'}
    assert mid.custom_info['acquisition_chain']['successor_pending'] == pending
    assert not mid.custom_info['acquisition_chain']['membership_closed']
    assert validate_acquisition_chain(mid.config, mid.custom_info, mid.custom_info['entry_reconciliation'],
                                      require_closed=False) == mid.custom_info['acquisition_chain']
    executor.control_open_order()
    fill_completely(executor, executor._open_order, '99.90')
    closed = assert_recorder_round_trip(recorder, executor)
    assert closed.custom_info['acquisition_chain']['membership_closed']
    assert [child['order_id'] for child in closed.custom_info['acquisition_chain']['children']] == ['parent', 'child-1']
    sealed = validate_acquisition_chain(closed.config, closed.custom_info, closed.custom_info['entry_reconciliation'])
    assert sealed == closed.custom_info['acquisition_chain']


@pytest.mark.asyncio
async def test_legacy_single_attempt_latches_cancel_executor_wide_and_still_stops():
    executor, _, _ = make_executor()
    assert executor.config.entry_chain_contract is None
    executor._completed_open_orders = [tracked_order('parent', state=OrderState.CANCELED)]
    executor._open_order = tracked_order('child-1', price='99', fill='0.2', state=OrderState.PARTIALLY_FILLED)
    executor._entry_submitted = True
    executor._entry_reprice_count = 1
    # The same predecessor-scoped cancel that leaves a V2 replacement alive
    # (test_child_state.test_predecessor_cancel_does_not_stop_live_replacement)
    # is an executor-wide latch for the legacy contract.
    executor._entry_lifecycle.update(order_id='child-1', cancel_event_timestamp=1, cancel_event_order_id='parent')
    assert executor._entry_cancel_observed()
    await executor._reconcile_expired_entry()
    assert executor._entry_stop_reason == 'exchange_canceled'
    assert executor._open_order_entry_expired
    assert executor._entry_lifecycle['stop_reason'] == 'exchange_canceled'
    custom = executor.get_custom_info()
    assert 'acquisition_chain' not in custom
    assert 'further_acquisition_permission' not in custom
    assert custom['entry_acquisition']['terminal_reason'] == 'exchange_canceled'
    executor.control_open_order()
    executor.place_order.assert_not_called()
    executor._cancel_entry_expiry_reconciliation()
