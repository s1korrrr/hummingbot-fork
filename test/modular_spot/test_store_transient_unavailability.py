"""A transient recorder failure is a retryable data hold, never a permanent controller fault.

`DataUnavailable` holds one decision and the next cycle re-reads authoritative state;
any other `PolicyError` latches the runtime/recovery fault until a restart. A busy or
briefly unavailable SQLite recorder must therefore surface as `DataUnavailable`, while a
compare-and-swap conflict (a real concurrent writer) keeps refusing as before.
"""
from types import SimpleNamespace as NS

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from controllers.generic.modular_spot_core.bag import BagTrailStore
from controllers.generic.modular_spot_core.native_recovery import NativeRecoveryStore
from controllers.generic.modular_spot_core.sell_only import SellOnlyStore
from controllers.generic.modular_spot_core.state import DecisionState, DecisionStore
from hummingbot.model import get_declarative_base
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.components.domain import DataUnavailable, PolicyError


def locked(*_args, **_kwargs):
    raise OperationalError('SELECT', {}, Exception('database is locked'))


def recorder(sessions):
    return NS(config_file_path='owner.yml', sql_manager=NS(get_new_session=sessions))


@pytest.fixture
def native(tmp_path):
    get_declarative_base()
    engine = create_engine('sqlite:///' + str(tmp_path / 'native.sqlite'))
    Metadata.__table__.create(engine)
    yield sessionmaker(bind=engine)
    engine.dispose()


STORES = {
    'decision': lambda r: DecisionStore(r, 'owner', 'okx', 'BTC-USDC'),
    'bag': lambda r: BagTrailStore(r, 'owner', 'okx', 'BTC-USDC'),
    'sell_only': lambda r: SellOnlyStore(r, 'owner', 'okx', 'BTC-USDC'),
}


@pytest.mark.parametrize('name', sorted(STORES))
def test_unavailable_recorder_read_is_a_retryable_hold(name):
    store = STORES[name](recorder(locked))
    with pytest.raises(DataUnavailable, match='unavailable'):
        store.load()


def test_decision_write_unavailable_is_a_hold_and_cas_conflict_is_not(native):
    store = DecisionStore(recorder(native), 'owner', 'okx', 'BTC-USDC')
    first, second = DecisionState(fingerprint='a' * 64, candle=60), DecisionState(fingerprint='a' * 64, candle=120)
    assert store.compare_and_swap(None, first)
    assert not store.compare_and_swap(None, second)  # A concurrent writer: refused, not held.
    store.recorder = recorder(locked)
    with pytest.raises(DataUnavailable, match='decision persistence'):
        store.compare_and_swap(store_raw(native, store), second)


def store_raw(native, store):
    with native() as session:
        return session.get(Metadata, store.key).value


def test_bag_write_unavailable_is_a_hold_and_cas_conflict_is_not(native):
    store = BagTrailStore(recorder(native), 'owner', 'okx', 'BTC-USDC')
    store.encode = lambda trail, candle: f'{{"candle":{candle}}}'
    assert store.compare_and_swap(None, None, 1)
    assert not store.compare_and_swap('{"candle":0}', None, 2)  # A concurrent writer: refused, not held.
    store.recorder = recorder(locked)
    with pytest.raises(DataUnavailable, match='bag persistence'):
        store.compare_and_swap('{"candle":1}', None, 3)


def test_native_recovery_checkpoint_unavailable_is_a_retryable_hold():
    owner = NS(config=NS(id='owner', connector_name='okx', trading_pair='BTC-USDC'))
    store = NativeRecoveryStore(owner, recorder(locked), 'source')
    with pytest.raises(DataUnavailable, match='unavailable'):
        store._load()
    assert issubclass(DataUnavailable, PolicyError)
