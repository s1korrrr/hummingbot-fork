from hashlib import sha256

import pytest

from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import source_digest

from .test_composition import api, recipe
from .test_evidence import packet
from .test_native_admission import host_binding, host_evaluate
from .test_persistence import finish_recorder

pytestmark = pytest.mark.filterwarnings('ignore::ResourceWarning')


def test_missing_checkpoint_with_obligations_rejects_under_host_binding():
    from controllers.generic.crash_recovery_v2 import source_files, source_root
    config = recipe()
    binding, source = host_binding()
    assert source == source_digest(source_root(), source_files())
    _, rt = api()
    observed = packet(())
    with pytest.raises(PolicyError, match='missing checkpoint with native obligations'):
        rt.evaluate(None, observed, config, binding, expected_source_sha256=source,
                    now_ms=observed.received_at_ms, has_obligations=True)


def test_restored_shock_cannot_skip_to_recovery_or_clear_hold(tmp_path):
    try:
        from test.modular_spot.test_native_lifecycle import disposable_recorder
    except ModuleNotFoundError as exc:
        pytest.skip(f'{exc}. ./compile in this worktree is the next action')
    from controllers.generic.crash_recovery_v2 import CheckpointStore
    binding, source = host_binding()
    shocked = host_evaluate(acquire=False)
    shocked = host_evaluate(shocked.state, (90,), acquire=False)
    assert shocked.recovery.state.phase == 'SHOCK_HOLD'
    _, rt = api()
    raw = rt.encode_checkpoint(shocked.state)
    recorder = disposable_recorder(tmp_path)
    try:
        store = CheckpointStore(recorder, 'crash_recovery_v2_lot1')
        assert store.compare_and_swap(None, raw) is True
        restored = rt.decode_checkpoint(store.load(), expected_sha256=sha256(raw.encode()).hexdigest(),
                                        binding=binding)
        resumed = host_evaluate(restored, (90, 90), acquire=False)
        assert resumed.recovery.state.phase == 'STABILIZING'
        assert resumed.recovery.state.phase != 'RECOVERY_ELIGIBLE'
        assert resumed.execution_authorized is False
        assert source == binding.source_sha256
    finally:
        finish_recorder(recorder)
