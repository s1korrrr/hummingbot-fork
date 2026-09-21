import asyncio
from hashlib import sha256
from test.modular_spot.test_native_lifecycle import release_recorder

import pytest

from hummingbot.strategy_v2.components.recipe_contract import source_digest

from .test_composition import api
from .test_native_admission import host_binding, host_evaluate

pytestmark = pytest.mark.filterwarnings('ignore::ResourceWarning')


def finish_recorder(recorder):
    release_recorder(recorder)
    recorder.sql_manager.engine.dispose()
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
    except RuntimeError:
        return
    if not loop.is_closed():
        loop.close()
    asyncio.set_event_loop(None)


def recover_under_host():
    state = None
    result = None
    for prices in ((), (90,), (90, 90), (90, 90, 91), (90, 90, 91, 94)):
        result = host_evaluate(state, prices)
        state = result.state
    return result


def test_host_binding_round_trips_composition_state_through_the_existing_codec():
    from controllers.generic.crash_recovery_v2 import source_files, source_root
    _, rt = api()
    x = recover_under_host()
    raw = rt.encode_checkpoint(x.state)
    binding, source = host_binding()
    assert binding.source_sha256 == source == source_digest(source_root(), source_files())
    restored = rt.decode_checkpoint(raw, expected_sha256=sha256(raw.encode()).hexdigest(), binding=binding)
    assert restored == x.state


def test_checkpoint_persists_in_recorder_owned_metadata(tmp_path):
    try:
        from test.modular_spot.test_native_lifecycle import disposable_recorder

        from hummingbot.model.metadata import Metadata
    except ModuleNotFoundError as exc:
        pytest.skip(f'{exc}. ./compile in this worktree is the next action')
    from controllers.generic.crash_recovery_v2 import CheckpointStore
    _, rt = api()
    x = recover_under_host()
    raw = rt.encode_checkpoint(x.state)
    binding, _ = host_binding()
    recorder = disposable_recorder(tmp_path)
    try:
        store = CheckpointStore(recorder, 'crash_recovery_v2_lot1')
        assert store.compare_and_swap(None, raw) is True
        assert store.compare_and_swap(None, raw) is False
        stored = store.load()
        assert stored == raw
        restored = rt.decode_checkpoint(stored, expected_sha256=sha256(raw.encode()).hexdigest(), binding=binding)
        assert restored == x.state
        with recorder.sql_manager.get_new_session() as session:
            keys = [row.key for row in session.query(Metadata).all()]
        assert 'crash-recovery-v2:crash_recovery_v2_lot1' in keys
        assert not any(key.startswith('modular-') or 'funding' in key for key in keys)
    finally:
        finish_recorder(recorder)


def test_checkpoint_cas_rejects_a_stale_write(tmp_path):
    try:
        from test.modular_spot.test_native_lifecycle import disposable_recorder
    except ModuleNotFoundError as exc:
        pytest.skip(f'{exc}. ./compile in this worktree is the next action')
    from controllers.generic.crash_recovery_v2 import CheckpointStore
    _, rt = api()
    seeded = host_evaluate(acquire=False)
    shocked = host_evaluate(seeded.state, (90,), acquire=False)
    first = rt.encode_checkpoint(seeded.state)
    later = rt.encode_checkpoint(shocked.state)
    recorder = disposable_recorder(tmp_path)
    try:
        store = CheckpointStore(recorder, 'crash_recovery_v2_lot1')
        assert store.compare_and_swap(None, first) is True
        assert store.compare_and_swap('not-the-previous-bytes', later) is False
        assert store.load() == first
    finally:
        finish_recorder(recorder)
