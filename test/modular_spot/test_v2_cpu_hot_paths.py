"""Per-tick hot paths stay cheap without changing their results."""
import ast
import gc
from test.modular_spot.test_shared_data import rows, snapshot
from unittest.mock import patch

import pytest

from controllers.generic.modular_spot_core import native_composition
from controllers.generic.modular_spot_core.candle_data import _SNAPSHOT_FRAMES
from controllers.generic.modular_spot_core.domain import PolicyError


def _uncached_source_files(family, *, public_module=None):
    """The closure walk as it was before import references were cached."""
    root = native_composition.source_root()
    pending = [root / 'generic' / ((public_module or native_composition.FAMILIES[family]) + '.py'),
               root / 'directional_trading' / (family + '.py'),
               *sorted((root / 'generic' / 'modular_spot_core').rglob('*.py'))]
    seen = set()

    def enqueue(module):
        if module and module[0] == 'controllers':
            path = root.joinpath(*module[1:])
            for candidate in (path.with_suffix('.py'), path / '__init__.py'):
                if candidate.is_file() and candidate not in seen:
                    pending.append(candidate)

    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        package = ('controllers', *path.relative_to(root).parts[:-1])
        for count in range(1, len(package) + 1):
            initializer = root.joinpath(*package[1:count]) / '__init__.py'
            if initializer.is_file() and initializer not in seen:
                pending.append(initializer)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    enqueue(tuple(alias.name.split('.')))
            elif isinstance(node, ast.ImportFrom):
                suffix = tuple(node.module.split('.')) if node.module else ()
                module = (*package[:len(package) - node.level + 1], *suffix) if node.level else suffix
                enqueue(module)
                for alias in node.names:
                    enqueue((*module, alias.name))
            elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                  and node.func.attr == 'import_module' and node.args
                  and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
                enqueue(tuple(node.args[0].value.split('.')))
    return sorted(seen)


@pytest.mark.parametrize('family,public', [('ok_rsi', None), ('rsi_v5', None), ('ok_rsi', 'rsi_modular')])
def test_cached_source_closure_equals_the_uncached_walk(family, public):
    assert native_composition.source_files(family, public_module=public) == \
        _uncached_source_files(family, public_module=public)


def test_unchanged_sources_are_not_reparsed_on_repeated_reloads():
    native_composition.source_files('ok_rsi')
    with patch.object(native_composition.ast, 'parse', side_effect=AssertionError('reparsed')):
        first = native_composition.source_files('ok_rsi')
        assert native_composition.source_files('ok_rsi') == first


def test_changed_source_text_is_parsed_again():
    before = native_composition.import_references('import controllers.a\n')
    after = native_composition.import_references('import controllers.b\nfrom . import c\n')
    assert before == (('import', ('controllers', 'a')),)
    assert after == (('import', ('controllers', 'b')), ('from', 1, (), ('c',)))


def test_snapshot_frame_is_converted_once_and_still_returns_private_copies():
    snap = snapshot()
    first = snap.frame(now=6000, max_age=120)
    with patch('hummingbot.strategy_v2.components.candle_data.pd.DataFrame',
               side_effect=AssertionError('reconverted')):
        second = snap.frame(now=6000, max_age=120)
    first.loc[0, 'close'] = 1
    assert second.close.iloc[0] == 100
    assert snap.frame(now=6000, max_age=120).close.iloc[0] == 100


def test_cached_snapshot_frame_still_enforces_age_on_every_call():
    snap = snapshot()
    snap.frame(now=6000, max_age=120)
    from controllers.generic.modular_spot_core.domain import DataUnavailable
    with pytest.raises(DataUnavailable):
        snap.frame(now=6000 + 10_000, max_age=120)


def test_distinct_snapshots_never_share_a_cached_frame():
    low, high = snapshot(), snapshot(candles=rows(price='200'))
    assert low.frame(now=6000, max_age=120).close.iloc[-1] == 100
    assert high.frame(now=6000, max_age=120).close.iloc[-1] == 200


def test_cached_frame_is_released_with_its_snapshot():
    snap = snapshot()
    snap.frame(now=6000, max_age=120)
    key = id(snap)
    assert key in _SNAPSHOT_FRAMES
    del snap
    gc.collect()
    assert key not in _SNAPSHOT_FRAMES


def test_non_finite_conversion_is_never_cached():
    snap = snapshot()
    with patch('hummingbot.strategy_v2.components.candle_data.math.isfinite', return_value=False):
        for _ in range(2):
            with pytest.raises(PolicyError):
                snap.frame(now=6000, max_age=120)
    assert id(snap) not in _SNAPSHOT_FRAMES


def _pool_on(replica):
    from types import SimpleNamespace

    from controllers.generic.modular_spot_core.data_reader import DataPool
    pool = DataPool(None)
    pool.client = SimpleNamespace(replica=replica)
    return pool


def test_pool_cuts_each_replica_snapshot_once_and_recuts_when_it_is_replaced():
    from test.modular_spot.test_shared_data import dataset

    from controllers.generic.modular_spot_core.candle_data import Replica
    spec = dataset()
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    pool = _pool_on(replica)

    first = pool.snapshot(spec, now=6000, max_age=120)
    assert pool.snapshot(spec, now=6000, max_age=120) is first
    assert len(first.rows) == 60 and first.dataset == spec

    replica.accept(snapshot(seq=2, at=6060, candles=rows(end=6000)), now=6060)
    second = pool.snapshot(spec, now=6060, max_age=120)
    assert second is not first and second.rows[-1].open_time == 6000


def test_pool_snapshot_reuse_never_skips_the_freshness_check():
    from test.modular_spot.test_shared_data import dataset

    from controllers.generic.modular_spot_core.candle_data import Replica
    from controllers.generic.modular_spot_core.domain import DataUnavailable
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    pool = _pool_on(replica)
    pool.snapshot(dataset(), now=6000, max_age=120)
    with pytest.raises(DataUnavailable):
        pool.snapshot(dataset(), now=6000 + 10_000, max_age=120)
    replica.disconnect()
    with pytest.raises(PolicyError):
        pool.snapshot(dataset(), now=6000, max_age=120)


def test_check_admits_and_refuses_exactly_like_frame():
    from test.modular_spot.test_shared_data import dataset

    from controllers.generic.modular_spot_core.candle_data import Replica
    from controllers.generic.modular_spot_core.domain import DataUnavailable
    replica = Replica((dataset(),))
    replica.begin_session()
    replica.accept(snapshot(), now=6000)
    assert replica.check(dataset().key, now=6000, max_age=120) is replica.snapshots[dataset().key]
    assert replica.frame(dataset().key, now=6000, max_age=120).equals(
        snapshot().frame(now=6000, max_age=120).iloc[-dataset().history:].reset_index(drop=True))
    for call in (replica.check, replica.frame):
        with pytest.raises(DataUnavailable):
            call(dataset().key, now=6000 + 10_000, max_age=120)
