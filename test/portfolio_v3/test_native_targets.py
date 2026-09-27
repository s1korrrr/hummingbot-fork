"""Real native Metadata/SQLite transactions, not execution or account admission."""
import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from test.portfolio_v3.test_portfolio import H, mandate, policy, proposal, snapshot

import pytest

from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore, ProposalWriter
from hummingbot.strategy_v2.portfolio_v3.allocation import propose_target


def writer(**changes):
    return ProposalWriter(**(dict(portfolio_id='meridian_v3', account_id='paper_only', writer_id='writer1',
                                  epoch='e1', fence=1, mandate_sha256=mandate().identity, recipe_sha256=H) | changes))


def target(cut=1000, sequence=1):
    return propose_target(mandate(), snapshot(cutoff_ms=cut, sequence=sequence), (proposal(),), policy(),
                          recipe_sha256=H, epoch='e1', fence=1)


def test_native_restore_idempotence_cas_and_new_forecast_cut(native):
    store = NativeTargetStore(native, writer())
    t = target()
    first = store.publish(t, previous_sha256=None, now_ms=1000)
    restored = NativeTargetStore(native, writer())
    assert restored.read() == first
    assert restored.publish(t, previous_sha256=None, now_ms=1001) == first
    newer = target(cut=1001)  # Market/decision cut can advance without a fill/account event.
    with pytest.raises(ValueError):
        restored.publish(newer, previous_sha256=None, now_ms=1001)
    second = restored.publish(newer, previous_sha256=t.identity, now_ms=1001)
    assert second['previous_sha256'] == t.identity
    assert not second['execution_authorized']
    with native() as session:
        assert session.query(Metadata).count() == 3  # head + immutable two-target history
    with pytest.raises(ValueError):
        restored.publish(t, previous_sha256=newer.identity, now_ms=1002)


def test_two_writers_race_one_recorder_slot(native):
    def attempt(name):
        try:
            NativeTargetStore(native, writer(writer_id=name)).publish(target(), previous_sha256=None, now_ms=1000)
            return 'accepted'
        except ValueError:
            return 'held'
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, ('w1', 'w2'))) == ['accepted', 'held']


@pytest.mark.parametrize('changes', [{'epoch': 'other'}, {'fence': 2}, {'recipe_sha256': 'b' * 64},
                                     {'execution_authorized': True}])
def test_foreign_or_executable_targets_rejected(native, changes):
    store = NativeTargetStore(native, writer())
    with pytest.raises(ValueError):
        store.publish(target().model_copy(update=changes), previous_sha256=None, now_ms=1000)
    assert store.read() is None


@pytest.mark.parametrize('now', [999, 1250])
def test_future_or_expired_target_rejected(native, now):
    with pytest.raises(ValueError):
        NativeTargetStore(native, writer()).publish(target(), previous_sha256=None, now_ms=now)


@pytest.mark.parametrize('field,value', [('target_sha256', 'b' * 64), ('execution_authorized', True), ('scope', 'ACCOUNT_WIDE')])
def test_corrupt_receipts_fail_closed_on_read_and_write(native, field, value):
    store = NativeTargetStore(native, writer())
    t = target()
    store.publish(t, previous_sha256=None, now_ms=1000)
    with native() as session, session.begin():
        row = session.get(Metadata, store.key)
        data = json.loads(row.value)
        data[field] = value
        row.value = json.dumps(data)
    with pytest.raises(ValueError):
        store.read()
    with pytest.raises(ValueError):
        store.publish(target(cut=1001), previous_sha256=t.identity, now_ms=1001)


def test_unbounded_or_inconsistent_targets_cannot_enter_recorder(native):
    t = target()
    changes = ({'quantities': t.quantities * 65}, {'requested_acquisition_quote': Decimal(0)},
               {'reasons': ('x',) * 129}, {'execution_authorized': 0},
               {'quantities': (t.quantities[0].model_copy(update={'symbol': 'BNB-PERP'}),)},
               {'quantities': (t.quantities[0].model_copy(update={'quantity': Decimal(0)}),)})
    store = NativeTargetStore(native, writer())
    for update in changes:
        with pytest.raises(ValueError):
            store.publish(t.model_copy(update=update), previous_sha256=None, now_ms=1000)
    assert store.read() is None
