"""A venue-terminal child must agree with its native recorder status before release."""

from test.acquisition_chain_v2.test_funding_chain import chain_case
from test.test_ok_rsi_down_only_entry_execution import make_config
from types import SimpleNamespace

import pytest

from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1


def _release_case(contract, last_status, status_history=()):
    _, custom, proofs = chain_case()
    config = make_config(id='parent-btc', controller_id='owner', trading_pair='BTC-USDC',
                         entry_chain_contract=contract)
    info = SimpleNamespace(config=config, connector_name='okx', trading_pair='BTC-USDC',
                           custom_info={'order_ids': ['first', 'second'],
                                        'execution_order_snapshots': custom['execution_order_snapshots']})
    connector = SimpleNamespace(_order_tracker=SimpleNamespace(all_orders={}))
    owner = SimpleNamespace(config=SimpleNamespace(execution_mode='live', connector_name='okx'),
                            market_data_provider=SimpleNamespace(connectors={'okx': connector}))
    store = object.__new__(NativeFundingStore)
    store.owner = owner
    store.recorder = SimpleNamespace(get_order_status_history=lambda order_id: list(status_history))
    recorded = SimpleNamespace(id='first', exchange_order_id='venue-first', symbol='BTC-USDC',
                               market='okx', last_status=last_status)
    return store, info, proofs[0], [recorded]


def test_chain_release_requires_canceled_recorder_predecessor():
    store, info, proof, orders = _release_case(ACQUISITION_CHAIN_V1, 'BuyOrderCreated')
    with pytest.raises(PolicyError, match='recorded child status'):
        store._validate_order_release(info, proof, 0, [], orders)
    orders[0].last_status = 'OrderCancelled'
    store._validate_order_release(info, proof, 0, [], orders)


def test_legacy_single_attempt_record_status_contract_is_unchanged():
    store, info, proof, orders = _release_case(None, 'BuyOrderCreated')
    store._validate_order_release(info, proof, 0, [], orders)


def test_rejected_chain_child_cannot_have_a_created_recorder_order():
    store, info, proof, orders = _release_case(ACQUISITION_CHAIN_V1, 'BuyOrderCreated')
    native = InFlightOrder.from_json(info.custom_info['execution_order_snapshots'][0])
    native.current_state = OrderState.FAILED
    native.exchange_order_id = None
    info.custom_info['execution_order_snapshots'][0] = native.to_json()
    proof.update(source='okx_submission_rejected_v1', state='REJECTED', exchange_order_id=None)
    orders[0].exchange_order_id = None
    with pytest.raises(PolicyError, match='recorded child status'):
        store._validate_order_release(info, proof, 0, [], orders)
    store._validate_order_release(info, proof, 0, [], [])


def test_chain_release_rejects_conflicting_terminal_recorder_history():
    store, info, proof, orders = _release_case(
        ACQUISITION_CHAIN_V1, 'OrderCancelled', ('BuyOrderCreated', 'OrderCancelled', 'BuyOrderCompleted'))
    with pytest.raises(PolicyError, match='conflicting terminal order receipts'):
        store._validate_order_release(info, proof, 0, [], orders)
