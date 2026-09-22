import os
import subprocess
import sys
from dataclasses import replace
from decimal import Decimal as D

import pytest
from pydantic import ValidationError

from controllers.generic.spot_recipe_v2 import SpotRecipeV2Controller, SpotRecipeV2ControllerConfig
from controllers.generic.spot_recipe_v2_core.acquisition import evaluate_acquisition
from controllers.generic.spot_recipe_v2_core.recipe import (
    PolicyDependency,
    SpotRecipeV2,
    bind_recipe,
    compose_universes,
    decide_spot_recipe,
    render_recipe,
    source_files,
    source_root,
)
from hummingbot.strategy_v2.components.config import AcquisitionPolicy, RiskPolicy, TrailingPolicy
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.program_budget import Reservation
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, source_records
from hummingbot.strategy_v2.components.source_identity import NAMESPACE
from hummingbot.strategy_v2.components.universe import UniversePolicy

from .test_acquisition import opportunity
from .test_capital import LIMITS, capital, evidence, inputs, ledger
from .test_lifecycle import pending, scope, snapshot
from .test_lifecycle_coordination import trailing
from .test_native_views import exit_inputs
from .test_seal_blast_radius import modular_spot_records

FROZEN = ('modular_spot', 'rsi_modular', 'modular_ok_rsi', 'modular_rsi_v5', 'component_breakout')
PAYLOAD = {'total_amount_quote': '100', 'execution_enabled': False, 'initial_positions': []}


def recipe(**updates):
    values = dict(recipe_id='v2_baseline', mandate='profit_only', connector='okx_paper_trade',
                  execution_mode='paper', interval='1m', observation_epoch='fixture', data_revision=1,
                  model_identity='none', quote_budget=D('100'),
                  universe=UniversePolicy(mode='fixed', symbols=('BTC-USDC',)),
                  acquisition=AcquisitionPolicy(), trailing=TrailingPolicy(), risk=RiskPolicy(),
                  policies=(PolicyDependency(policy_id='trail', action='reduce'),))
    return SpotRecipeV2(**(values | updates))


def bound_snapshot(item=None, **updates):
    item = recipe() if item is None else item
    binding = RecipeBinding(recipe_id=item.recipe_id, recipe_sha256=item.recipe_sha256,
                            source_sha256='b' * 64, configuration_sha256='c' * 64)
    return snapshot(scope=scope(recipe_binding=binding), **updates)


def decide(item=None, snap=None, **kwargs):
    item = recipe() if item is None else item
    snap = bound_snapshot(item) if snap is None else snap
    _, view, proposals, floor = exit_inputs(snap, minimum_price=D('109'))
    kwargs.setdefault('previous_trailing', None)
    return decide_spot_recipe(item, snap, (trailing(snap),), view, proposals, floor,
                              now_ms=1002, max_age_ms=10, **kwargs)


def config(**updates):
    values = dict(id='spot_recipe_v2_lot1', connector_name='okx_paper_trade',
                  trading_pair='BTC-USDC', recipe=recipe())
    return SpotRecipeV2ControllerConfig(**(values | updates))


def test_v2_host_owns_source_files_disjoint_from_the_frozen_consumer():
    owned = {name for name, _ in source_records(source_root(), source_files())
             if not name.startswith(NAMESPACE)}
    assert 'spot_recipe_v2.py' in owned
    assert owned and not any(name.startswith(FROZEN) for name in owned)


def test_v2_and_modular_spot_share_only_the_identical_component_closure():
    ours, theirs = dict(source_records(source_root(), source_files())), dict(modular_spot_records())
    owned_by_us = {name for name in ours if not name.startswith(NAMESPACE)}
    owned_by_them = {name for name in theirs if not name.startswith(NAMESPACE)}
    assert not owned_by_us & owned_by_them
    shared = set(ours) & set(theirs)
    assert shared and all(name.startswith(NAMESPACE) for name in shared)
    assert all(ours[name] == theirs[name] for name in shared)


def test_v2_controller_identity_can_never_occupy_a_frozen_program_budget_slot():
    claim = dict(executor_id='lot_1', trading_pair='BTC-USDC', reserved_quote=D('1'), reserved_base=D('1'),
                 account='main', connector='okx_paper_trade', program='v2', slot='BTC', expires_at=D('0'))
    with pytest.raises(ValidationError):
        Reservation(controller_id=config().id, **claim)
    assert Reservation(controller_id='modular_spot_lot1', **claim).controller_id == 'modular_spot_lot1'


def test_recipe_rejects_unknown_fields_and_duplicate_policy_identity():
    with pytest.raises(ValidationError):
        recipe(unknown_field=1)
    with pytest.raises(ValidationError, match='duplicate policy identity'):
        recipe(policies=(PolicyDependency(policy_id='trail', action='reduce'),
                         PolicyDependency(policy_id='trail', action='acquire')))


def test_profit_only_mandate_rejects_a_hard_loss_liquidation_policy():
    hard_loss = PolicyDependency(policy_id='momentum', action='reduce', requires_loss_liquidation=True)
    with pytest.raises(ValidationError, match='hard-loss'):
        recipe(policies=(hard_loss,))
    assert recipe(mandate='loss_permitted', policies=(hard_loss,)).mandate == 'loss_permitted'


def test_recipe_requires_a_reduction_policy_and_a_consistent_paper_venue():
    with pytest.raises(ValidationError, match='reduction policy'):
        recipe(policies=(PolicyDependency(policy_id='entry', action='acquire'),))
    with pytest.raises(ValidationError, match='paper'):
        recipe(connector='okx')


@pytest.mark.parametrize('updates', [
    {'connector': 'okx', 'execution_mode': 'live'}, {'interval': '5m'}, {'observation_epoch': 'other'},
    {'data_revision': 2}, {'model_identity': 'kronos_v1'}, {'mandate': 'loss_permitted'},
])
def test_any_changed_identity_field_produces_a_different_recipe_revision(updates):
    assert recipe(**updates).recipe_sha256 != recipe().recipe_sha256


def test_native_scope_bound_to_another_recipe_revision_is_rejected():
    with pytest.raises(PolicyError, match='another recipe revision'):
        decide(recipe(recipe_id='other_v2'), snap=bound_snapshot())


def test_native_scope_with_a_different_venue_or_execution_mode_is_rejected():
    live = recipe(connector='okx', execution_mode='live')
    paper = bound_snapshot(live)
    assert paper.scope.connector == 'okx_paper_trade'
    with pytest.raises(PolicyError, match='venue or execution mode'):
        decide(live, snap=paper)


def test_binding_requires_disabled_execution_because_the_digest_excludes_control_fields():
    binding = bind_recipe(recipe(), PAYLOAD)
    assert binding.recipe_sha256 == recipe().recipe_sha256
    with pytest.raises(PolicyError, match='disabled execution'):
        bind_recipe(recipe(), {**PAYLOAD, 'execution_enabled': True})
    with pytest.raises(PolicyError, match='adopted inventory'):
        bind_recipe(recipe(), {**PAYLOAD, 'initial_positions': [{'any': 'thing'}]})


def test_rendered_bundle_carries_the_honesty_fields():
    bundle = render_recipe(recipe(), PAYLOAD)
    assert bundle['capital_authority'] == 'NOT_PROVEN'
    assert bundle['native_validation'] == 'NOT_RUN'
    assert bundle['native_runtime_validation'] == 'NOT_RUN'
    assert bundle['execution_enabled'] is False
    assert bundle['economic_backtest'] is False


def test_overlapping_exit_requests_reach_one_bounded_native_proposal():
    result = decide()
    assert result.exit_assessment.amount_base == D('1.2')
    assert result.exit_assessment.execution_authorized is False
    assert result.execution_authorized is False


def test_acquisition_pause_never_suppresses_native_management_or_exit():
    paused = recipe(universe=UniversePolicy(mode='sell_only', symbols=('BTC-USDC',)))
    result = decide(paused)
    assert result.acquisition_reason == 'acquisition_paused'
    assert result.acquisition is None
    assert result.exit_assessment.amount_base == D('1.2')
    assert result.lifecycle.management_required is True


def test_missing_acquisition_evidence_is_named_not_silently_treated_as_eligible():
    result = decide()
    assert result.acquisition_reason == 'acquisition_inputs_unavailable'
    assert result.acquisition is None


def test_partial_acquisition_evidence_is_rejected_rather_than_defaulted():
    with pytest.raises(PolicyError, match='exact acquisition evidence set'):
        decide(acquisition={'opportunity': None})


def acquisition_evidence(item, snap, **updates):
    chance = opportunity(999, recipe_sha256=item.recipe_sha256)
    cursor = evaluate_acquisition(snap, None, chance, purpose='initial_entry',
                                  now_ms=1002, max_age_ms=300, prerequisites=()).cursor
    values = dict(opportunity=opportunity(recipe_sha256=item.recipe_sha256), cursor=cursor, prerequisites=(),
                  purpose='initial_entry', ledger=ledger(), predecessor_ledger=None, stage_inputs=inputs(), recorder_evidence=evidence(),
                  capital_snapshot=capital(), capital_limits=LIMITS)
    return values | updates


def ready_snapshot(item):
    return bound_snapshot(item, remaining_base=D('0'), entry_gross_quote=D('0'), entry_terminal=False)


def decide_ready(item, snap, evidence_for):
    view = exit_inputs(snap, remaining_base=D('0'))[1]
    return decide_spot_recipe(item, snap, (), view, (), None, now_ms=1002, max_age_ms=10,
                              previous_trailing=None, acquisition=acquisition_evidence(evidence_for, snap))


def ready_decision(item=None):
    item = recipe() if item is None else item
    return decide_ready(item, ready_snapshot(item), item)


def test_a_flat_scope_reports_no_reduction_instead_of_inventing_trailing_evidence():
    result = ready_decision()
    assert result.lifecycle.management_required is False
    assert result.exit_assessment is None
    assert result.exit_reason == 'no_native_position_to_reduce'


def test_a_managed_position_can_never_be_reported_without_an_exit_assessment():
    managed = decide()
    assert managed.lifecycle.management_required is True
    assert managed.exit_assessment is not None
    with pytest.raises(ValidationError, match='requires an exit assessment'):
        managed.__class__.model_validate(
            managed.model_dump(mode='python') | {'exit_assessment': None, 'exit_reason': 'looks_fine'})


def test_one_pass_composes_lifecycle_exit_eligibility_sizing_and_capital_admission():
    result = ready_decision()
    assert result.lifecycle.phase == 'READY'
    assert result.acquisition is not None
    assert result.acquisition.admitted_quote > 0
    assert result.acquisition.execution_authorized is False
    assert result.acquisition_reason == result.acquisition.reason
    assert result.execution_authorized is False


@pytest.mark.parametrize('updates', [{'epoch': 'other'}, {'revision': 2}])
def test_an_opportunity_from_another_observation_epoch_or_revision_is_rejected(updates):
    """The recipe is unchanged; the observation feed is not the one the recipe is pinned to."""
    item = recipe()
    snap = ready_snapshot(item)
    given = acquisition_evidence(item, snap)
    stale = given['opportunity']
    given['opportunity'] = stale.model_copy(update={'primary': stale.primary.model_copy(update=updates)})
    view = exit_inputs(snap, remaining_base=D('0'))[1]
    with pytest.raises(PolicyError, match='observation epoch or data revision'):
        decide_spot_recipe(item, snap, (), view, (), None, now_ms=1002, max_age_ms=10,
                           previous_trailing=None, acquisition=given)


def test_deselected_instrument_with_inventory_stays_in_the_management_universe():
    selection = compose_universes(recipe(), (bound_snapshot(),), available=('ETH-USDC',), tradable=('ETH-USDC',),
                                  scores={}, blocked=(), previous=(), now_ms=1002, max_age_ms=10)
    assert 'BTC-USDC' in selection.management
    assert 'BTC-USDC' not in selection.entries
    assert 'BTC-USDC' in selection.observation


def test_we_refuse_the_selection_if_the_shared_universe_component_ever_drops_held_inventory(monkeypatch):
    """select_universe lives outside this package's seal, so its contract is checked, not assumed."""
    import controllers.generic.spot_recipe_v2_core.recipe as module
    original = module.select_universe
    monkeypatch.setattr(module, 'select_universe',
                        lambda *a, **k: replace(original(*a, **k), management=()))
    with pytest.raises(PolicyError, match='dropped attributed inventory'):
        compose_universes(recipe(), (bound_snapshot(),), available=('BTC-USDC',), tradable=('BTC-USDC',),
                          scores={}, blocked=(), previous=(), now_ms=1002, max_age_ms=10)


def test_pending_order_keeps_its_instrument_managed_while_entry_is_paused():
    held = bound_snapshot(commitments=(pending(status='PENDING'),))
    paused = recipe(universe=UniversePolicy(mode='sell_only', symbols=('BTC-USDC',)))
    selection = compose_universes(paused, (held,), available=('BTC-USDC',), tradable=('BTC-USDC',),
                                  scores={}, blocked=(), previous=(), now_ms=1002, max_age_ms=10)
    assert selection.entries == ()
    assert 'BTC-USDC' in selection.management


def test_host_config_cannot_be_enabled_for_execution_or_take_a_frozen_identity():
    assert config().execution_enabled is False
    with pytest.raises(ValidationError):
        config(execution_enabled=True)
    with pytest.raises(ValidationError):
        config(id='modular_spot_lot1')


def test_host_never_emits_an_executor_action_even_when_the_decision_is_eligible():
    assert decide().exit_assessment.status == 'PROPOSED_REDUCTION'
    controller = SpotRecipeV2Controller.__new__(SpotRecipeV2Controller)
    controller.config = config()
    assert controller.determine_executor_actions() == []
    assert controller.validate_executor_config(object()) is False


def test_importing_the_v2_host_loads_no_frozen_controller_family():
    script = ('import sys\n'
              'import controllers.generic.spot_recipe_v2\n'
              f'frozen = {FROZEN!r}\n'
              'loaded = [n for n in sys.modules if n.startswith("controllers.generic.")]\n'
              'assert not [n for n in loaded if n.split(".")[2].startswith(frozen)], loaded\n')
    result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                            env={**os.environ, 'PYTHONPATH': '.'}, timeout=120)
    assert result.returncode == 0, result.stderr
