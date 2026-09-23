import importlib.util
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from controllers.generic.mean_reversion_v2_core.exit_a.admission import (
    ExitAAdmissionStore,
    admit,
    recover_late_fill,
    request_cancel,
)
from controllers.generic.mean_reversion_v2_core.exit_a.recipe import ExitARecipeV2, bind_exit_a_v2
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.exit_policy import NativeProfitFloor
from hummingbot.strategy_v2.components.recipe_contract import verify_binding

_POLICY_SPEC = importlib.util.spec_from_file_location(
    'exit_a_policy_helpers', Path(__file__).with_name('test_exit_a_policy.py'))
_POLICY = importlib.util.module_from_spec(_POLICY_SPEC)
_POLICY_SPEC.loader.exec_module(_POLICY)
H, context, evaluate = _POLICY.H, _POLICY.context, _POLICY.evaluate


def recipe(**changes):
    values = dict(program_id='mean_reversion_v2_exit_a', owner_id='mean_reversion_v2_exit_a',
                  trading_pair='BNB-USDC', baseline_recipe_sha256=H, model_sha256=H,
                  training_manifest_sha256=H)
    values.update(changes)
    return ExitARecipeV2(**values)


def test_bind_rejects_legacy_and_compatibility_identities():
    for field, value in (('program_id', 'ok_rsi'), ('owner_id', 'rsi_modular_v2'),
                         ('program_id', 'okx_spot_dca'), ('owner_id', 'rsi_v5_host'),
                         ('program_id', 'r11')):
        with pytest.raises(ValueError):
            recipe(**{field: value})


def test_bind_seals_isolated_source_and_full_controls():
    bound = bind_exit_a_v2(recipe())
    local = [path for path, _ in bound.source_records if not path.startswith('@')]
    assert local and all(path.startswith('generic/mean_reversion_v2_core/') for path in local)
    assert not any('modular_spot_core' in path or 'rsi_modular' in path or 'ok_rsi' in path for path in local)
    assert any(path.startswith('@strategy_components/') for path, _ in bound.source_records)
    assert bound.binding.recipe_id == 'exit_a_v2'
    assert bound.full_configuration_sha256 != bound.binding.configuration_sha256
    assert bound.recipe.execution_enabled is False
    verify_binding(bound.binding, bound.recipe.model_dump(mode='json'),
                   source_sha256=bound.binding.source_sha256)
    with pytest.raises(PolicyError):
        verify_binding(bound.binding, bound.recipe.model_dump(mode='json') | {'trading_pair': 'ETH-USDC'},
                       source_sha256=bound.binding.source_sha256)


def test_exit_a_is_outside_modular_spot_core():
    generic = Path(__file__).resolve().parents[2] / 'controllers' / 'generic'
    modular = generic / 'modular_spot_core'
    isolated = generic / 'mean_reversion_v2_core'
    assert isolated.is_dir() and modular.is_dir()
    for path in modular.rglob('*.py'):
        assert 'mean_reversion_v2' not in path.read_text()


def metadata_store(tmp_path, owner='mean_reversion_v2_exit_a'):
    base = declarative_base()

    class Metadata(base):
        __tablename__ = 'Metadata'
        key = Column(Text, primary_key=True)
        value = Column(Text, nullable=False)

    engine = create_engine('sqlite:///' + str(tmp_path / 'exit-a.db'), connect_args={'timeout': 5})
    base.metadata.create_all(engine)
    recorder = NS(config_file_path='mean_reversion_v2.yml',
                  sql_manager=NS(get_new_session=sessionmaker(bind=engine)))
    return engine, ExitAAdmissionStore(recorder, owner_id=owner, metadata_model=Metadata)


def reduction():
    first = evaluate(context())
    current = context(101)
    result = evaluate(current, first.cursor)
    floor = NativeProfitFloor(owner_id='v2_owner', executor_id='position_1', receipt_sha256=H,
                              quantity_base='1', price='100', observed_at_ms=101)
    return result, current, floor


def test_store_rejects_compatibility_owner():
    recorder = NS(config_file_path='x.yml', sql_manager=NS(get_new_session=lambda: None))
    with pytest.raises(PolicyError):
        ExitAAdmissionStore(recorder, owner_id='rsi_modular_host')


def test_admit_persists_before_place_and_does_not_forward_zero_price(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    assert result.proposal.minimum_price == 0
    seen = []

    def place(claim):
        loaded = store.load(claim.claim_id)
        seen.append((loaded is not None, str(claim.assessment.limit_price)))

    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50, place=place)
    engine.dispose()
    assert claim is not None and claim.state == 'ADMITTED'
    assert claim.inventory_released is False
    assert claim.assessment.execution_authorized is False
    assert claim.assessment.limit_price == floor.price
    assert seen == [(True, '100')]


def test_cancel_and_late_fill_keep_inventory(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    claim = admit(result, current.native, floor, store, now_ms=101, max_age_ms=50)
    cancelled = request_cancel(claim, store)
    assert cancelled.state == 'CANCEL_PENDING' and cancelled.inventory_released is False
    recovered = recover_late_fill(cancelled, 'c' * 64, store)
    engine.dispose()
    assert recovered.state == 'LATE_FILL_RECOVERED'
    assert recovered.inventory_released is False
    assert recovered.fill_receipt_sha256 == 'c' * 64
    assert store.load(claim.claim_id) == recovered
    assert recover_late_fill(recovered, 'c' * 64, store) == recovered
    with pytest.raises(PolicyError):
        recover_late_fill(recovered, 'd' * 64, store)


def test_hold_assessment_does_not_persist_a_claim(tmp_path):
    engine, store = metadata_store(tmp_path)
    result, current, floor = reduction()
    assert admit(result, current.native, None, store, now_ms=101, max_age_ms=50) is None
    assert store._parse(store._load_raw()) == {}
    engine.dispose()
