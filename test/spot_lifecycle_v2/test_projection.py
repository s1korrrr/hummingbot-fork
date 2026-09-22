from decimal import Decimal as D

import pytest
from pydantic import ValidationError

from controllers.generic.spot_recipe_v2_core.projection import OperatorProjectionV2, project_operator_view
from controllers.generic.spot_recipe_v2_core.recipe import source_files, source_root
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import source_digest

from .test_recipe import bound_snapshot, decide, recipe


def project(item=None, snap=None, result=None, **kwargs):
    item = recipe() if item is None else item
    snap = bound_snapshot(item) if snap is None else snap
    result = decide(item, snap=snap) if result is None else result
    values = dict(bid=D('110'), floor_price=D('100'), now_ms=1002, max_age_ms=10)
    return project_operator_view(item, result, snap, **(values | kwargs))


def test_available_projection_carries_native_identity_and_the_reason_chain():
    view = project()
    assert view.available is True
    assert view.unavailable_reason is None
    assert view.native_phase == 'MANAGING'
    assert view.evidence_age_ms == 2
    assert view.retained_base == D('2')
    assert view.requested_base == D('1.2')
    assert view.capital_capability == 'RECORDER_SCOPED'
    assert view.execution_authorized is False
    assert 'native_price_bounded_reduction' in view.reason_chain


def test_unknown_pnl_is_unavailable_and_is_never_coerced_to_zero():
    view = project()
    assert view.realized_pnl_quote is None
    assert view.unrealized_pnl_quote is None
    for field in ('realized_pnl_quote', 'unrealized_pnl_quote'):
        with pytest.raises(ValidationError):
            OperatorProjectionV2.model_validate(view.model_dump(mode='python') | {field: D('0')})


def test_admitted_quote_is_unavailable_when_no_capital_was_admitted():
    view = project()
    assert view.admitted_quote is None
    assert view.reason_chain[-1] == 'acquisition_inputs_unavailable'


def test_stale_native_evidence_projects_unavailable_rather_than_healthy():
    view = project(now_ms=99999)
    assert view.available is False
    assert view.unavailable_reason == 'native_evidence_stale'


def test_evidence_from_the_future_projects_unavailable():
    view = project(now_ms=500)
    assert view.available is False
    assert view.unavailable_reason == 'native_evidence_stale'


def test_a_decision_for_another_snapshot_or_recipe_projects_unavailable():
    other = recipe(recipe_id='other_v2')
    view = project(item=other, snap=bound_snapshot(other), result=decide())
    assert view.available is False
    assert view.unavailable_reason == 'decision_snapshot_mismatch'


def test_a_recipe_retuned_to_another_venue_can_no_longer_match_its_own_decision():
    """Why the projection needs no venue check: changing the venue breaks the recipe digest first."""
    item = recipe()
    snap = bound_snapshot(item)
    result = decide(item, snap=snap)
    live = item.model_copy(update={'connector': 'okx', 'execution_mode': 'live'})
    assert live.recipe_sha256 != result.recipe_sha256
    view = project_operator_view(live, result, snap, bid=D('110'),
                                 floor_price=D('100'), now_ms=1002, max_age_ms=10)
    assert view.available is False
    assert view.unavailable_reason == 'recipe_mismatch'


def test_a_managed_position_cannot_be_published_without_an_exit_assessment():
    """model_copy skips model validators, so the projection has to revalidate what it publishes."""
    result = decide()
    assert result.lifecycle.management_required is True
    stripped = result.model_copy(update={'exit_assessment': None})
    assert stripped.exit_assessment is None
    with pytest.raises(ValidationError, match='requires an exit assessment'):
        project(result=stripped)


def test_the_published_source_seal_is_computed_here_and_cannot_be_asserted_by_the_caller():
    with pytest.raises(TypeError):
        project(source_sha256='e' * 64)
    assert project().source_sha256 == source_digest(source_root(), source_files())


def test_an_unavailable_projection_cannot_publish_native_quantities():
    with pytest.raises(ValidationError, match='cannot publish native quantities'):
        OperatorProjectionV2(available=False, unavailable_reason='native_evidence_stale', owner_id='o',
                             executor_id='e', trading_pair='BTC-USDC', execution_mode='paper',
                             retained_base=D('2'))
    with pytest.raises(ValidationError, match='exactly one reason'):
        OperatorProjectionV2(available=True, unavailable_reason='native_evidence_stale', owner_id='o',
                             executor_id='e', trading_pair='BTC-USDC', execution_mode='paper')


def test_a_malformed_phase_or_mode_is_rejected_rather_than_coerced():
    with pytest.raises(ValidationError):
        OperatorProjectionV2(available=True, owner_id='o', executor_id='e', trading_pair='BTC-USDC',
                             execution_mode='paper', native_phase='LOOKS_FINE')
    with pytest.raises(ValidationError):
        OperatorProjectionV2(available=True, owner_id='o', executor_id='e', trading_pair='BTC-USDC',
                             execution_mode='margin')


def test_projection_requires_canonical_inputs_and_a_bounded_clock():
    with pytest.raises(PolicyError, match='exact isolated v2 recipe'):
        project_operator_view(object(), decide(), bound_snapshot(),
                              now_ms=1002, max_age_ms=10)
    with pytest.raises(PolicyError, match='projection clock'):
        project(max_age_ms=0)


def test_the_projection_round_trips_as_bounded_json():
    view = project()
    assert OperatorProjectionV2.model_validate_json(view.model_dump_json()) == view
