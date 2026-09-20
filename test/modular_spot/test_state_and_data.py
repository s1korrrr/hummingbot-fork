from decimal import Decimal as D
from types import SimpleNamespace as NS

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from controllers.generic.modular_spot_core.config import SignalPolicy
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.inventory import inventory_view
from controllers.generic.modular_spot_core.market import features
from controllers.generic.modular_spot_core.state import DecisionState, DecisionStore, reconcile_claims


def candles():
    close = 100 + np.sin(np.arange(100) / 4)
    return pd.DataFrame(dict(timestamp=np.arange(100) * 60, open=close, high=close + 1,
                             low=close - 1, close=close, volume=np.ones(100)))


def trend(frame, **kwargs):
    return NS(is_valid=True, direction=NS(value='up'))


def test_unclosed_bar_is_not_available_to_features():
    frame = candles()
    left = features(frame, now=5970, interval=60, max_age=120, policy=SignalPolicy(), trend_analyzer=trend)
    frame.loc[99, ['open', 'high', 'low', 'close']] = [900, 901, 899, 900]
    right = features(frame, now=5970, interval=60, max_age=120, policy=SignalPolicy(), trend_analyzer=trend)
    assert left == right and left.candle_open == 5880


def test_feature_calculation_does_not_mutate_shared_feed():
    frame = candles()
    before = frame.copy(deep=True)
    features(frame, now=6000, interval=60, max_age=120, policy=SignalPolicy(), trend_analyzer=trend)
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize('corruption', ['nan', 'duplicate', 'gap', 'geometry', 'future', 'stale', 'descending'])
def test_unusable_candles_hold(corruption):
    frame = candles()
    now = 6000
    if corruption == 'nan':
        frame.loc[97, 'close'] = np.nan
    elif corruption == 'duplicate':
        frame.loc[97, 'timestamp'] = frame.loc[96, 'timestamp']
    elif corruption == 'gap':
        frame = frame.drop(96)
    elif corruption == 'geometry':
        frame.loc[97, 'low'] = 300
    elif corruption == 'future':
        frame.loc[99, 'timestamp'] = 7000
    elif corruption == 'stale':
        now = 9000
    else:
        frame = frame.iloc[::-1]
    with pytest.raises(PolicyError):
        features(frame, now=now, interval=60, max_age=120, policy=SignalPolicy(), trend_analyzer=trend)


def native_info(*, identifier='one', remaining='1', terminal=False, lifecycle='managing_fill', owner='new'):
    return NS(id=identifier, controller_id=owner, config=NS(controller_id=owner, connector_name='okx',
              trading_pair='BTC-USDC', side=NS(name='BUY'), level_id='modular_spot:v1:abc'),
              is_done=terminal, is_active=not terminal, close_type=None,
              custom_info={'entry_price': D('100'), 'entry_lifecycle': {'state': lifecycle},
                           'execution_receipt': {'entry_gross_quote': '100', 'remaining_owned_base': remaining,
                                                 'exit_gross_base': '0', 'accounting_owner': 'executor'}})


def test_inventory_is_a_projection_and_rejects_unknown_ownership():
    info = native_info()
    view = inventory_view([info], [], controller_id='new', connector='okx', pair='BTC-USDC')
    assert view['one'].remaining_base == D('1')
    assert info.custom_info['execution_receipt']['remaining_owned_base'] == '1'
    with pytest.raises(PolicyError):
        inventory_view([native_info(owner='other')], [], controller_id='new', connector='okx', pair='BTC-USDC')


def test_missing_or_contradictory_receipt_is_not_zero_inventory():
    info = native_info()
    del info.custom_info['execution_receipt']
    with pytest.raises(PolicyError):
        inventory_view([info], [], controller_id='new', connector='okx', pair='BTC-USDC')
    with pytest.raises(PolicyError):
        inventory_view([native_info(remaining='NaN')], [], controller_id='new', connector='okx', pair='BTC-USDC')


def test_unseen_claim_is_retained_not_retried():
    state = DecisionState(fingerprint='a' * 64, candle=60, claims=('missing',), committed_quote=D('10'))
    revised, reason = reconcile_claims(state, {})
    assert revised == state and reason == 'execution_receipt_missing'


def test_campaign_can_reset_only_when_all_native_positions_are_flat():
    state = DecisionState(fingerprint='a' * 64, candle=60, claims=('one',), committed_quote=D('10'))
    view = inventory_view([native_info(remaining='0', terminal=True)], [],
                          controller_id='new', connector='okx', pair='BTC-USDC')
    revised, reason = reconcile_claims(state, view)
    assert revised.claims == () and revised.committed_quote == 0 and revised.cycle == 1
    held = native_info(remaining='.01', terminal=True)
    held.custom_info['execution_receipt']['accounting_owner'] = 'held_position'
    view = inventory_view([held], [], controller_id='new', connector='okx', pair='BTC-USDC')
    assert reconcile_claims(state, view)[1] == 'held_inventory_requires_reconciliation'


@pytest.fixture
def store_pair(tmp_path):
    base = declarative_base()

    class MetadataFixture(base):
        __tablename__ = 'Metadata'
        key = Column(Text, primary_key=True, nullable=False)
        value = Column(Text, nullable=False)

    engine = create_engine('sqlite:///' + str(tmp_path / 'native-metadata.db'))
    base.metadata.create_all(engine)
    recorder = NS(config_file_path='isolated.yml', sql_manager=NS(get_new_session=sessionmaker(bind=engine)))
    a = DecisionStore(recorder, 'new', 'okx', 'BTC-USDC', metadata_model=MetadataFixture)
    b = DecisionStore(recorder, 'new', 'okx', 'BTC-USDC', metadata_model=MetadataFixture)
    yield a, b, recorder, MetadataFixture
    engine.dispose()


def test_real_sqlite_cas_allows_one_admission_and_survives_restart(store_pair):
    a, b, _, _ = store_pair
    initial = DecisionState(fingerprint='a' * 64, candle=60)
    assert a.compare_and_swap(None, initial)
    assert not b.compare_and_swap(None, initial)
    loaded, raw = b.load()
    assert loaded == initial
    one = loaded.model_copy(update={'candle': 120, 'claims': ('one',), 'committed_quote': D('10')})
    two = loaded.model_copy(update={'candle': 120, 'claims': ('two',), 'committed_quote': D('10')})
    assert a.compare_and_swap(raw, one)
    assert not b.compare_and_swap(raw, two)
    assert b.load()[0].claims == ('one',)


def test_corrupt_state_cannot_silently_reinitialize(store_pair):
    a, _, recorder, model = store_pair
    with recorder.sql_manager.get_new_session() as session:
        session.add(model(key=a.key, value='{}'))
        session.commit()
    with pytest.raises(PolicyError):
        a.load()


def test_state_rejects_time_regression_and_config_replacement(store_pair):
    a, _, _, _ = store_pair
    initial = DecisionState(fingerprint='a' * 64, candle=120)
    assert a.compare_and_swap(None, initial)
    _, raw = a.load()
    for update in ({'candle': 60}, {'fingerprint': 'b' * 64}):
        with pytest.raises(PolicyError):
            a.compare_and_swap(raw, initial.model_copy(update=update))


def test_checkpoint_cannot_release_claims_without_a_new_cycle(store_pair):
    a, _, _, _ = store_pair
    initial = DecisionState(fingerprint='a' * 64, candle=120, claims=('one',), committed_quote=D('10'))
    assert a.compare_and_swap(None, initial)
    _, raw = a.load()
    forged = initial.model_copy(update={'claims': (), 'committed_quote': D('0')})
    with pytest.raises(PolicyError):
        a.compare_and_swap(raw, forged)


def test_checkpoint_cannot_clear_risk_latch(store_pair):
    a, _, _, _ = store_pair
    initial = DecisionState(fingerprint='a' * 64, candle=120, risk_paused=True)
    assert a.compare_and_swap(None, initial)
    _, raw = a.load()
    with pytest.raises(PolicyError):
        a.compare_and_swap(raw, initial.model_copy(update={'risk_paused': False}))
