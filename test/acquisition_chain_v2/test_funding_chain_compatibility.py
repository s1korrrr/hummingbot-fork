"""Old single-attempt BTC is accepted only by an explicitly claimed manifest."""

import json
from copy import deepcopy
from test.acquisition_chain_v2.test_funding_chain import chain_case
from test.test_ok_rsi_down_only_entry_execution import make_config
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from controllers.generic.modular_spot_core.domain import identity
from controllers.generic.modular_spot_core.native_funding import NativeFundingStore, _config_identity
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.strategy_v2.executors.acquisition_chain import ACQUISITION_CHAIN_V1


def compatibility_case():
    _, custom, proofs = chain_case()
    custom['execution_order_snapshots'][0]['cancel_source'] = '31'
    custom['acquisition_chain']['children'][1]['purpose'] = 'post_only_repair'
    original = make_config(id='parent-btc', controller_id='owner', trading_pair='BTC-USDC')
    resume = make_config(id='parent-btc', controller_id='owner', trading_pair='BTC-USDC',
                         entry_chain_contract=ACQUISITION_CHAIN_V1)
    old_raw = original.model_dump(mode='json')
    old_raw.pop('entry_chain_contract')  # Exact shape persisted before V2 opt-in existed.
    resume_raw = resume.model_dump(mode='json')
    reconciliation = {'schema': 1, 'source': 'native_terminal_entry_set_v1', 'orders': proofs}
    position = {'executor_id': original.id, 'config': old_raw, 'resume_config': resume_raw,
                'acquisition_chain': custom['acquisition_chain'],
                'entry_orders': custom['execution_order_snapshots'],
                'entry_reconciliation': reconciliation}
    manifest = {'id': 'reviewed-claim', 'positions': [position]}
    marker = {'schema': 1, 'kind': 'legacy_single_post_only_repair_v1',
              'manifest_id': manifest['id'], 'executor_id': original.id,
              'original_config_sha256': identity(old_raw), 'resume_config_sha256': identity(resume_raw),
              'acquisition_chain_sha256': identity(custom['acquisition_chain']),
              'entry_order_ids': ['first', 'second']}
    info = SimpleNamespace(id=original.id, controller_id='owner', config=original,
                           custom_info={'planned_position_resume': manifest['id'],
                                        'planned_position_resume_compatibility_v1': marker,
                                        'entry_reconciliation': reconciliation,
                                        'execution_order_snapshots': custom['execution_order_snapshots']})
    engine = create_engine('sqlite:///:memory:')
    with engine.begin() as connection:
        connection.exec_driver_sql('CREATE TABLE PlannedPositionResume '
                                   '(id TEXT PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL, claimed_at REAL)')
        connection.exec_driver_sql('INSERT INTO PlannedPositionResume VALUES (?, ?, ?, ?)',
                                   (manifest['id'], 'claimed', json.dumps(manifest), 1.0))
    store = object.__new__(NativeFundingStore)
    store.recorder = SimpleNamespace(sql_manager=SimpleNamespace(get_new_session=lambda: Session(engine)))
    claim = {'config': _config_identity(original, 'owner')}
    assert claim['config'] == identity(old_raw)
    return store, info, claim, engine


def test_claimed_exact_old_to_new_chain_is_accepted():
    store, info, claim, engine = compatibility_case()
    try:
        assert store._legacy_chain_compatibility(info, claim)['entry_order_ids'] == ['first', 'second']
        info.config = make_config(id='parent-btc', controller_id='owner', trading_pair='BTC-USDC',
                                  entry_chain_contract=ACQUISITION_CHAIN_V1)
        assert store._legacy_chain_compatibility(info, claim)['manifest_id'] == 'reviewed-claim'
    finally:
        engine.dispose()


def test_post_resume_sell_snapshot_preserves_bound_buy_history():
    store, info, claim, engine = compatibility_case()
    try:
        sell = InFlightOrder('exit', 'BTC-USDC', OrderType.LIMIT, TradeType.SELL,
                             1, 5, 110, exchange_order_id='venue-exit', initial_state=OrderState.FILLED)
        info.custom_info['execution_order_snapshots'].append(sell.to_json())
        assert store._legacy_chain_compatibility(info, claim)['manifest_id'] == 'reviewed-claim'
        altered = deepcopy(info.custom_info['execution_order_snapshots'][1])
        altered['order_fills']['fill-1']['fee']['flat_fees'] = [{'token': 'USDC', 'amount': '1'}]
        info.custom_info['execution_order_snapshots'][1] = altered
        with pytest.raises(ValueError, match='Legacy acquisition compatibility'):
            store._legacy_chain_compatibility(info, claim)
    finally:
        engine.dispose()


def make_manifest_unclaimed(_info, engine):
    with engine.begin() as connection:
        connection.exec_driver_sql("UPDATE PlannedPositionResume SET state='prepared' WHERE id='reviewed-claim'")


@pytest.mark.parametrize('mutation', [
    lambda info, engine: info.custom_info['planned_position_resume_compatibility_v1'].update(
        acquisition_chain_sha256='0' * 64),
    lambda info, engine: info.custom_info['planned_position_resume_compatibility_v1'].update(
        entry_order_ids=['first', 'foreign']),
    lambda info, engine: info.custom_info.update(planned_position_resume='other'),
    make_manifest_unclaimed,
])
def test_unbound_or_unclaimed_legacy_receipt_is_rejected(mutation):
    store, info, claim, engine = compatibility_case()
    try:
        mutation(info, engine)
        with pytest.raises(ValueError, match='Legacy acquisition compatibility'):
            store._legacy_chain_compatibility(info, claim)
    finally:
        engine.dispose()


def test_later_claimed_handovers_preserve_original_funding_bridge():
    store, info, claim, engine = compatibility_case()
    try:
        with engine.begin() as connection:
            original = json.loads(connection.exec_driver_sql(
                "SELECT payload FROM PlannedPositionResume WHERE id='reviewed-claim'").scalar())
        marker = deepcopy(info.custom_info['planned_position_resume_compatibility_v1'])
        for reference in ('second-handover', 'third-handover'):
            position = deepcopy(original['positions'][0])
            position['config'] = position.pop('resume_config')
            position['planned_position_resume_compatibility_v1'] = marker
            current = {'id': reference, 'positions': [position]}
            with engine.begin() as connection:
                connection.exec_driver_sql('INSERT INTO PlannedPositionResume VALUES (?, ?, ?, ?)',
                                           (reference, 'claimed', json.dumps(current), 2.0))
            info.config = make_config(id='parent-btc', controller_id='owner', trading_pair='BTC-USDC',
                                      entry_chain_contract=ACQUISITION_CHAIN_V1)
            info.custom_info['planned_position_resume'] = reference
            assert store._legacy_chain_compatibility(info, claim)['manifest_id'] == 'reviewed-claim'
        with engine.begin() as connection:
            current['positions'][0]['entry_reconciliation']['orders'][1]['filled_base'] = '0.5'
            connection.exec_driver_sql('UPDATE PlannedPositionResume SET payload=? WHERE id=?',
                                       (json.dumps(current), 'third-handover'))
        with pytest.raises(ValueError, match='compatibility manifest differs'):
            store._legacy_chain_compatibility(info, claim)
    finally:
        engine.dispose()
