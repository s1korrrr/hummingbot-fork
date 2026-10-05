"""A replacement consumes one original acquisition authorization."""

from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pytest

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1, validate_acquisition_chain


def chain_case():
    config = SimpleNamespace(id='parent-btc', side=TradeType.BUY, leverage=1, amount=Decimal('1'),
                             entry_price=Decimal('100'), maximum_acceptable_price=Decimal('100'),
                             connector_name='okx', trading_pair='BTC-USDC', entry_max_downward_reprices=2,
                             entry_acquisition_policy='down_only_maker_v1',
                             entry_chain_contract=ACQUISITION_CHAIN_V1,
                             triple_barrier_config=SimpleNamespace(open_order_type=OrderType.LIMIT_MAKER))
    first = InFlightOrder('first', 'BTC-USDC', OrderType.LIMIT_MAKER, TradeType.BUY,
                          Decimal('1'), 1, Decimal('100'), exchange_order_id='venue-first',
                          initial_state=OrderState.CANCELED)
    second = InFlightOrder('second', 'BTC-USDC', OrderType.LIMIT_MAKER, TradeType.BUY,
                           Decimal('1'), 2, Decimal('99'), exchange_order_id='venue-second')
    second.update_with_trade_update(TradeUpdate('fill-1', 'second', 'venue-second', 'BTC-USDC', 3,
                                                Decimal('99'), Decimal('.25'), Decimal('24.75'), AddedToCostTradeFee()))
    second.update_with_trade_update(TradeUpdate('fill-2', 'second', 'venue-second', 'BTC-USDC', 4,
                                                Decimal('99'), Decimal('.75'), Decimal('74.25'), AddedToCostTradeFee()))
    second.current_state = OrderState.FILLED
    chain = {'schema': 1, 'contract': ACQUISITION_CHAIN_V1, 'parent_id': config.id,
             'authorized_base': '1', 'maximum_price': '100',
             'children': [
                 {'generation': 0, 'order_id': 'first', 'predecessor_id': None, 'purpose': 'initial',
                  'submitted_base': '1', 'limit_price': '100'},
                 {'generation': 1, 'order_id': 'second', 'predecessor_id': 'first',
                  'purpose': 'downward_reprice', 'submitted_base': '1', 'limit_price': '99'},
             ], 'successor_pending': None, 'membership_closed': True, 'gross_filled_base': '1'}
    custom = {'acquisition_chain': chain, 'execution_order_snapshots': [first.to_json(), second.to_json()]}
    proofs = [
        {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': 'first',
         'exchange_order_id': 'venue-first', 'trading_pair': 'BTC-USDC', 'side': 'BUY',
         'amount': '1', 'filled_base': '0', 'state': 'CANCELED', 'trade_ids': []},
        {'schema': 1, 'source': 'okx_terminal_status_v1', 'order_id': 'second',
         'exchange_order_id': 'venue-second', 'trading_pair': 'BTC-USDC', 'side': 'BUY',
         'amount': '1', 'filled_base': '1', 'state': 'FILLED', 'trade_ids': ['fill-1', 'fill-2']},
    ]
    return config, custom, proofs


def test_terminal_zero_fill_predecessor_and_two_fill_child_consume_one_parent():
    config, custom, proofs = chain_case()
    assert validate_acquisition_chain(config, custom, proofs) == custom['acquisition_chain']


def test_authorized_unsubmitted_successor_keeps_chain_open():
    config, custom, proofs = chain_case()
    custom = deepcopy(custom)
    custom['acquisition_chain']['children'] = custom['acquisition_chain']['children'][:1]
    custom['execution_order_snapshots'] = custom['execution_order_snapshots'][:1]
    custom['acquisition_chain']['gross_filled_base'] = '0'
    custom['acquisition_chain']['membership_closed'] = False
    custom['acquisition_chain']['successor_pending'] = {'predecessor_id': 'first', 'purpose': 'downward_reprice'}
    assert validate_acquisition_chain(config, custom, proofs[:1], require_closed=False) == custom['acquisition_chain']
    with pytest.raises(ValueError, match='successor'):
        validate_acquisition_chain(config, custom, proofs[:1])
    # Cancel request may precede authoritative REST proof; no capacity is
    # released while it is pending, and other parents can still be assessed.
    assert validate_acquisition_chain(config, custom, [], require_closed=False) == custom['acquisition_chain']


def test_partial_predecessor_cannot_authorize_replacement():
    config, custom, proofs = chain_case()
    first = InFlightOrder.from_json(custom['execution_order_snapshots'][0])
    first.update_with_trade_update(TradeUpdate('old-fill', 'first', 'venue-first', 'BTC-USDC', 3,
                                               Decimal('100'), Decimal('.1'), Decimal('10'), AddedToCostTradeFee()))
    first.current_state = OrderState.CANCELED
    custom['execution_order_snapshots'][0] = first.to_json()
    # Keep aggregate below the parent limit and make every other receipt agree:
    # a zero-fill predecessor is the substantive condition being tested.
    second = InFlightOrder('second', 'BTC-USDC', OrderType.LIMIT_MAKER, TradeType.BUY,
                           Decimal('.9'), 2, Decimal('99'), exchange_order_id='venue-second')
    second.update_with_trade_update(TradeUpdate('fill-2', 'second', 'venue-second', 'BTC-USDC', 4,
                                                Decimal('99'), Decimal('.9'), Decimal('89.1'), AddedToCostTradeFee()))
    second.current_state = OrderState.FILLED
    custom['execution_order_snapshots'][1] = second.to_json()
    custom['acquisition_chain']['children'][1]['submitted_base'] = '.9'
    proofs[0].update(filled_base='.1', trade_ids=['old-fill'])
    proofs[1].update(amount='.9', filled_base='.9', trade_ids=['fill-2'])
    with pytest.raises(ValueError, match='zero-fill'):
        validate_acquisition_chain(config, custom, proofs)


def test_unowned_extra_native_buy_snapshot_cannot_be_dropped_from_chain():
    config, custom, proofs = chain_case()
    extra = InFlightOrder('third', 'BTC-USDC', OrderType.LIMIT_MAKER, TradeType.BUY,
                          Decimal('1'), 5, Decimal('98'), exchange_order_id='venue-third')
    custom['execution_order_snapshots'].append(extra.to_json())
    with pytest.raises(ValueError, match='coverage'):
        validate_acquisition_chain(config, custom, proofs)


@pytest.mark.parametrize('mutation', [
    lambda c, p: p[0].update(filled_base='0.1'),
    lambda c, p: c['acquisition_chain']['children'][1].update(predecessor_id='other'),
    lambda c, p: c['acquisition_chain']['children'][1].update(limit_price='101'),
    lambda c, p: c['acquisition_chain']['children'][1].update(submitted_base='1.01'),
    lambda c, p: p[1].update(trade_ids=['fill-1', 'fill-1']),
    lambda c, p: p[1].update(source='unknown'),
    lambda c, p: c['acquisition_chain'].update(gross_filled_base='0'),
])
def test_contradictory_chain_cannot_seal(mutation):
    config, custom, proofs = chain_case()
    mutation(custom, proofs)
    with pytest.raises(ValueError, match='Acquisition chain'):
        validate_acquisition_chain(config, custom, proofs)


def test_legacy_configuration_cannot_implicitly_adopt_v2_chain():
    config, custom, proofs = chain_case()
    config.entry_chain_contract = None
    with pytest.raises(ValueError, match='unsupported'):
        validate_acquisition_chain(config, custom, proofs)


def test_late_predecessor_fill_flag_blocks_closed_chain_release_and_new_funding():
    config, custom, proofs = chain_case()
    custom['resume_conflicting_fill'] = {'order_id': 'first', 'reason': 'late native fill'}
    with pytest.raises(ValueError, match='contradictory predecessor fill'):
        validate_acquisition_chain(config, custom, proofs, require_closed=False)


def test_cumulative_quote_must_match_exact_child_fill_receipts():
    config, custom, proofs = chain_case()
    custom['execution_order_snapshots'][1]['executed_amount_quote'] = '98'
    with pytest.raises(ValueError, match='native quote total differs'):
        validate_acquisition_chain(config, custom, proofs)


def in_flight_case():
    """A parent whose newest child was submitted but is not yet tracked by the connector.

    `place_order` returns the client order id synchronously; the connector starts
    tracking it only when its create task first runs, so the chain records the
    child one loop turn before any native snapshot exists.
    """
    config, custom, proofs = chain_case()
    custom = deepcopy(custom)
    custom['acquisition_chain']['membership_closed'] = False
    custom['acquisition_chain']['gross_filled_base'] = '0'
    custom['execution_order_snapshots'] = custom['execution_order_snapshots'][:1]
    return config, custom, proofs[:1]


def test_open_chain_child_in_submission_keeps_the_chain_open_for_peer_funding():
    config, custom, proofs = in_flight_case()
    assert validate_acquisition_chain(config, custom, proofs, require_closed=False, allow_unacknowledged_tail=True) == custom['acquisition_chain']
    custom['acquisition_chain']['children'] = custom['acquisition_chain']['children'][:1]
    custom['execution_order_snapshots'] = []
    assert validate_acquisition_chain(config, custom, [], require_closed=False, allow_unacknowledged_tail=True) == custom['acquisition_chain']


@pytest.mark.parametrize('mutation, match', [
    # Only the newest child can be in submission; an older one without a snapshot is lost history.
    (lambda c, p: c['execution_order_snapshots'].__setitem__(slice(None), []), 'coverage'),
    # A closed chain claims every child terminal, so each needs its native order.
    (lambda c, p: c['acquisition_chain'].update(membership_closed=True), 'coverage|successor|closed'),
    # A proof cannot cover an order the connector never tracked.
    (lambda c, p: p.append({**p[0], 'order_id': 'second', 'exchange_order_id': 'venue-second'}), 'proof'),
])
def test_in_flight_tolerance_is_limited_to_the_newest_open_child(mutation, match):
    config, custom, proofs = in_flight_case()
    mutation(custom, proofs)
    with pytest.raises(ValueError, match=match):
        validate_acquisition_chain(config, custom, proofs, require_closed=False, allow_unacknowledged_tail=True)


def test_closed_validation_still_requires_every_child_snapshot():
    config, custom, proofs = in_flight_case()
    with pytest.raises(ValueError, match='coverage|successor'):
        validate_acquisition_chain(config, custom, proofs, allow_unacknowledged_tail=True)


def test_executor_own_reconciliation_stays_strict_about_the_tail():
    # down_only_entry_execution._store_entry_reconciliation validates its own open chain
    # without the projection allowance; an untracked child there is still a contradiction.
    config, custom, proofs = in_flight_case()
    with pytest.raises(ValueError, match='coverage'):
        validate_acquisition_chain(config, custom, proofs, require_closed=False)


def test_unacknowledged_tail_keeps_its_authorization_and_predecessor_proof_bounds():
    """Only the native comparison waits for the ack: a tail after a filled predecessor, or
    beyond the remaining authorization, is refused before its order object exists."""
    config, custom, proofs = in_flight_case()
    first = InFlightOrder.from_json(custom['execution_order_snapshots'][0])
    first.update_with_trade_update(TradeUpdate('old-fill', 'first', 'venue-first', 'BTC-USDC', 3,
                                               Decimal('100'), Decimal('.4'), Decimal('40'), AddedToCostTradeFee()))
    first.current_state = OrderState.CANCELED
    custom['execution_order_snapshots'][0] = first.to_json()
    custom['acquisition_chain']['gross_filled_base'] = '.4'
    proofs[0].update(filled_base='.4', trade_ids=['old-fill'])
    with pytest.raises(ValueError, match='exceeds authorization'):  # 1.0 tail > 0.6 remaining
        validate_acquisition_chain(config, custom, proofs, require_closed=False, allow_unacknowledged_tail=True)
    custom['acquisition_chain']['children'][1]['submitted_base'] = '.6'
    with pytest.raises(ValueError, match='zero-fill'):  # a filled predecessor never authorizes a successor
        validate_acquisition_chain(config, custom, proofs, require_closed=False, allow_unacknowledged_tail=True)
