import pytest
from pydantic import ValidationError

from hummingbot.strategy_v2.components.program_budget import Reservation

from .test_native_admission import host_config, host_controller

pytestmark = pytest.mark.filterwarnings('ignore::ResourceWarning')


def test_crash_host_cannot_occupy_modular_spot_funding_or_budget_slots():
    cfg = host_config()
    assert not hasattr(cfg, 'funding')
    claim = dict(executor_id='lot_1', trading_pair='BNB-USDC', reserved_quote='1', reserved_base='1',
                 account='main', connector='okx', program='v2', slot='BNB', expires_at='0')
    with pytest.raises(ValidationError):
        Reservation(controller_id=cfg.id, **claim)


def test_checkpoint_store_does_not_write_native_funding_keys(tmp_path):
    try:
        from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
        from hummingbot.model.metadata import Metadata
    except ModuleNotFoundError as exc:
        pytest.skip(f'{exc}. ./compile in this worktree is the next action')
    from test.modular_spot.test_native_lifecycle import disposable_recorder

    from controllers.generic.crash_recovery_v2 import CheckpointStore

    from .test_composition import api, recover
    from .test_persistence import finish_recorder
    _, rt = api()
    recorder = disposable_recorder(tmp_path)
    try:
        store = CheckpointStore(recorder, 'crash_recovery_v2_lot1')
        assert store.compare_and_swap(None, rt.encode_checkpoint(recover().state)) is True
        with recorder.sql_manager.get_new_session() as session:
            keys = [row.key for row in session.query(Metadata).all()]
        assert 'crash-recovery-v2:crash_recovery_v2_lot1' in keys
        assert not any(key.startswith('modular-') or 'funding' in key for key in keys)
        with pytest.raises(AttributeError):
            NativeFundingStore(host_controller(), recorder)
    finally:
        finish_recorder(recorder)
