import importlib
from decimal import Decimal, localcontext

import pytest

from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, ObservationRef, Opportunity, Prerequisite

def snapshot_api():
    return importlib.import_module('controllers.generic.mr_context_v2_core.snapshot')


def payload(**changes):
    data = dict(source='okx', market_type='spot', quote_asset='USDC', interval='1m',
                dataset_sha256='a' * 64, epoch='epoch1', revision=1,
                implementation_sha256='b' * 64, parameters_sha256='c' * 64,
                initialization_sha256='d' * 64, universe_sha256='e' * 64,
                universe_revision=1, source_content_sha256='f' * 64,
                bar_open_ms=60000, bar_close_ms=120000, confirmed=True,
                available_time_ms=121000, received_at_ms=121050, expires_at_ms=150000,
                expected_members=10, ready_members=10, fresh_members=10,
                history_bars=30, missing_required=(), evidence_kind='observed',
                components=dict(return_1m_fraction='-0.002', return_5m_fraction='-0.02',
                                breadth_fraction='0.2', quote_volume_5m='1000000',
                                realized_vol_15m_fraction='0.01'))
    data.update(changes)
    return data


def requirements(**changes):
    data = dict(source='okx', dataset_sha256='a' * 64, implementation_sha256='b' * 64,
                parameters_sha256='c' * 64, initialization_sha256='d' * 64,
                universe_sha256='e' * 64, universe_revision=1,
                min_coverage_fraction='0.9', min_members=5, min_history_bars=20,
                max_source_age_ms=60000, max_transport_age_ms=10000)
    data.update(changes)
    return snapshot_api().ContextRequirementsV2(**data)


def api():
    return importlib.import_module('controllers.generic.mr_context_v2_core.policy')


def recipes():
    return importlib.import_module('controllers.generic.mr_context_v2_core.recipe')


def make_recipe(mode='restrict', rules=None):
    rules = rules if rules is not None else [dict(name='selloff', return_5m_at_most='-0.01',
                                                 breadth_at_most='0.3', multiplier='0.5')]
    return recipes().ContextRecipeV2(recipe_id='mr_context_test', revision=1,
        baseline_recipe_sha256='1' * 64, mode=mode, requirements=requirements(), rules=rules)


def packet(mode='restrict', rules=None, snapshot_changes=None, proposal_changes=None, capacity_changes=None):
    recipe = make_recipe(mode, rules)
    snapshot = snapshot_api().ContextSnapshotV2(**payload(**(snapshot_changes or {})))
    scope = DecisionScope(program_id='mr_v2_test', owner_id='isolated', connector='okx',
                          trading_pair='BTC-USDC', recipe_sha256=recipe.recipe_sha256)
    primary = ObservationRef(dataset_sha256='2' * 64, epoch='primary', revision=1,
                             content_sha256='3' * 64, event_time_ms=120000, available_time_ms=121000)
    opportunity = Opportunity(scope=scope, setup='dip', action='acquire', primary=primary,
                              dependencies=(snapshot.observation,), created_at_ms=122000, lifetime_ms=60000)
    common = dict(scope=scope, account_id='test_account', state_sha256='4' * 64,
                  state_version=3, observed_at_ms=122000, expires_at_ms=149000)
    proposal_data = dict(**common, opportunity_id=opportunity.opportunity_id,
                         baseline_recipe_sha256=recipe.baseline_recipe_sha256,
                         price='100', quantity='1', amount_step='0.001', minimum_quantity='0.001',
                         minimum_notional='5', fee_fraction_bound='0.001',
                         max_quote_commitment='100.1', fee_currency='USDC')
    proposal_data.update(proposal_changes or {})
    proposal = api().AcquisitionProposalV2(**proposal_data)
    capacity_data = dict(**common, remaining_quote='100.1', liquidity_quote_cap='100.1')
    capacity_data.update(capacity_changes or {})
    capacity = api().CapacityEvidenceV2(**capacity_data)
    return dict(recipe=recipe, binding=recipes().bind_policy(recipe, source_sha256='9' * 64),
                source_sha256='9' * 64, opportunity=opportunity, proposal=proposal,
                capacity=capacity, snapshot=snapshot, now_ms=123000)


def test_joint_selloff_rule_reduces_only_the_baseline_proposal():
    args = packet()
    result = api().evaluate_context(**args)
    assert result.status == 'PASS'
    assert result.multiplier == Decimal('0.5')
    assert result.quantity == Decimal('0.5')
    assert result.quote_commitment == Decimal('50.05')
    assert result.expires_at_ms == 149000
    assert result.execution_authorized is False
    prerequisite = api().admission_prerequisite(result, opportunity=args['opportunity'], now_ms=args['now_ms'])
    assert type(prerequisite) is Prerequisite
    assert prerequisite.status == 'PASS'
    assert prerequisite.evidence_sha256 == result.assessment_sha256


def test_rule_predicates_are_conjunctions_not_confidence_averages():
    args = packet(snapshot_changes={'components': dict(return_5m_fraction='-0.02', breadth_fraction='0.8')})
    result = api().evaluate_context(**args)
    assert result.quantity == Decimal('1')
    assert result.multiplier == Decimal('1')


def test_most_restrictive_cap_wins_without_multiplying_votes():
    rules = [dict(name='one', breadth_at_most='0.3', multiplier='0.5'),
             dict(name='two', breadth_at_most='0.3', multiplier='0.4')]
    result = api().evaluate_context(**packet(rules=rules))
    assert result.multiplier == Decimal('0.4')
    assert result.quantity == Decimal('0.4')
    rules.append(dict(name='veto', return_5m_at_most='0', multiplier='0'))
    result = api().evaluate_context(**packet(rules=rules))
    assert result.status == 'HOLD' and result.quantity == 0 and result.quote_commitment == 0


def test_capacity_and_liquidity_cannot_be_created_by_context():
    args = packet(capacity_changes={'remaining_quote': '30.02', 'liquidity_quote_cap': '20.02'})
    result = api().evaluate_context(**args)
    assert result.quantity == Decimal('0.2')
    assert result.quote_commitment <= Decimal('20.02')


def test_below_minimum_is_withheld_not_rounded_up():
    result = api().evaluate_context(**packet(capacity_changes={'remaining_quote': '4.9'}))
    assert result.status == 'HOLD' and result.quantity == 0
    assert 'minimum_order' in result.reasons


@pytest.mark.parametrize('mode', ['off', 'shadow', 'restrict'])
def test_modes_are_hashed_and_never_authorize_execution(mode):
    args = packet(mode)
    result = api().evaluate_context(**args)
    if mode == 'off':
        assert result is None
    else:
        assert result.mode == mode and result.execution_authorized is False
    assert len({make_recipe(m).recipe_sha256 for m in ['off', 'shadow', 'restrict']}) == 3


def test_off_does_not_require_context_or_capacity():
    args = packet('off')
    args.update(snapshot=None, proposal=None, capacity=None)
    assert api().evaluate_context(**args) is None


def test_reductions_bypass_all_acquisition_only_dependencies():
    args = packet()
    args['opportunity'] = args['opportunity'].model_copy(update={'action': 'reduce'})
    args.update(recipe=None, binding=None, source_sha256=None, proposal=None, capacity=None, snapshot=None)
    assert api().evaluate_context(**args) is None


def test_shadow_cannot_be_used_as_an_admission_prerequisite():
    args = packet('shadow')
    result = api().evaluate_context(**args)
    with pytest.raises(ValueError, match='shadow'):
        api().admission_prerequisite(result, opportunity=args['opportunity'], now_ms=args['now_ms'])


def test_shadow_uses_canonical_cursor_and_does_not_rearm_same_event():
    args = packet('shadow')
    result = api().evaluate_context(**args)
    cursor, first = api().project_shadow(None, args['opportunity'], result, prerequisites=())
    original = cursor.model_dump_json()
    _, second = api().project_shadow(cursor, args['opportunity'], result, prerequisites=())
    assert first.reason == 'seeded' and second.reason == 'already_observed'
    assert cursor.model_dump_json() == original
    assert not first.execution_authorized and not second.execution_authorized


def test_held_new_primary_event_is_consumed_when_context_recovers():
    from hummingbot.strategy_v2.components.decision_protocol import decide
    args = packet('shadow')
    result = api().evaluate_context(**args)
    cursor, _ = api().project_shadow(None, args['opportunity'], result, prerequisites=())
    later = args['opportunity'].model_dump(mode='python')
    later['primary'].update(event_time_ms=121000, available_time_ms=122000, revision=2)
    later['created_at_ms'] = 123000
    opportunity = Opportunity.model_validate(later)
    held, trace = decide(cursor, opportunity, now_ms=123000,
        prerequisites=(Prerequisite(name='mr_context', status='HOLD'),))
    _, repeated = decide(held, opportunity, now_ms=124000,
        prerequisites=(Prerequisite(name='mr_context', status='PASS', evidence_sha256='5' * 64),))
    assert trace.reason == 'prerequisite_hold'
    assert repeated.reason == 'already_observed'


@pytest.mark.parametrize('reason,changes', [
    ('availability_unobserved', {'evidence_kind': 'reconstructed'}),
    ('coverage_insufficient', {'fresh_members': 8}),
    ('dependency_missing', {'missing_required': ('ETH-USDC',)}),
    ('warmup_incomplete', {'history_bars': 1}),
    ('component_unavailable', {'components': {'breadth_fraction': '0.2'}}),
])
def test_unavailable_context_zeroes_candidate_not_native_state(reason, changes):
    args = packet(snapshot_changes=changes)
    proposal_before = args['proposal'].model_dump_json()
    result = api().evaluate_context(**args)
    assert result.status != 'PASS' and result.quantity == 0
    assert reason in result.reasons
    assert args['proposal'].model_dump_json() == proposal_before


def test_context_snapshot_must_match_bound_opportunity_dependency():
    args = packet()
    args['snapshot'] = snapshot_api().ContextSnapshotV2(**payload(revision=2))
    result = api().evaluate_context(**args)
    assert result.status == 'HOLD' and 'context_not_bound' in result.reasons


def test_receipt_after_setup_is_not_backdated_into_the_opportunity():
    args = packet(snapshot_changes={'received_at_ms': 122001})
    result = api().evaluate_context(**args)
    assert 'context_unavailable_at_setup' in result.reasons and result.quantity == 0


def test_missing_context_and_expiry_never_renew_or_release():
    args = packet()
    args['snapshot'] = None
    assert 'context_unavailable' in api().evaluate_context(**args).reasons
    args = packet()
    args['now_ms'] = 150000
    result = api().evaluate_context(**args)
    assert result.status == 'STALE' and result.quantity == 0
    assert result.expires_at_ms == 149000


@pytest.mark.parametrize('target,field,value', [
    ('capacity', 'state_version', 4), ('capacity', 'state_sha256', '5' * 64),
    ('capacity', 'account_id', 'other'), ('proposal', 'opportunity_id', '5' * 64),
    ('proposal', 'baseline_recipe_sha256', '5' * 64),
])
def test_native_identity_mismatch_never_receives_candidate_cap(target, field, value):
    args = packet()
    args[target] = args[target].model_copy(update={field: value})
    with pytest.raises(ValueError):
        api().evaluate_context(**args)


def test_native_future_receipts_and_expired_receipts_hold():
    args = packet(capacity_changes={'observed_at_ms': 124000})
    result = api().evaluate_context(**args)
    assert 'capacity_not_available' in result.reasons and result.quantity == 0
    args = packet(capacity_changes={'expires_at_ms': 123000})
    assert 'capacity_expired' in api().evaluate_context(**args).reasons


@pytest.mark.parametrize('change', [
    {'recipe_sha256': '0' * 64}, {'source_sha256': '0' * 64},
    {'configuration_sha256': '0' * 64}, {'recipe_id': 'other'}
])
def test_binding_checks_all_recipe_source_and_config_fields(change):
    args = packet()
    args['binding'] = args['binding'].model_copy(update=change)
    with pytest.raises(ValueError):
        api().evaluate_context(**args)


@pytest.mark.parametrize('value', ['-0.1', '1.1', 'NaN', 'Infinity', True, 0.5])
def test_invalid_rule_multiplier_rejected(value):
    with pytest.raises(ValueError):
        make_recipe(rules=[dict(name='bad', breadth_at_most='0.2', multiplier=value)])


def test_rules_are_explicit_bounded_unique_and_entry_only():
    with pytest.raises(ValueError):
        make_recipe(rules=[])
    with pytest.raises(ValueError):
        make_recipe(rules=[dict(name='bad', multiplier='0.5')])
    rule = dict(name='dup', breadth_at_most='0.2', multiplier='0.5')
    with pytest.raises(ValueError):
        make_recipe(rules=[rule, rule])
    with pytest.raises(ValueError):
        make_recipe(rules=[dict(rule, name=f'r{i}') for i in range(9)])
    data = make_recipe().model_dump(mode='python')
    data['acquisition_kind'] = 'dca'
    with pytest.raises(ValueError):
        recipes().ContextRecipeV2.model_validate(data)


def test_unknown_or_unvalidated_fields_do_not_disappear_at_policy_boundary():
    args = packet()
    args['recipe'] = args['recipe'].model_copy(update={'silently_ignored': True})
    with pytest.raises(ValueError):
        api().evaluate_context(**args)
    args = packet()
    args['proposal'] = args['proposal'].model_copy(update={'quantity': True})
    with pytest.raises(ValueError):
        api().evaluate_context(**args)


def test_no_downward_rounding_or_fee_conservation_depends_on_decimal_context():
    args = packet(capacity_changes={'remaining_quote': '17.999'})
    results = []
    for precision in (6, 28, 70):
        with localcontext() as ctx:
            ctx.prec = precision
            result = api().evaluate_context(**args)
            results.append((result.quantity, result.quote_commitment, result.assessment_sha256))
    assert len(set(results)) == 1


def test_deterministic_budget_sweep_never_exceeds_any_cap():
    for cents in range(0, 10001, 37):
        budget = Decimal(cents) / 100
        result = api().evaluate_context(**packet(capacity_changes={'remaining_quote': budget}))
        assert result.quantity <= Decimal('0.5')
        assert result.quote_commitment <= min(budget, Decimal('50.05'))
        assert result.quantity % Decimal('0.001') == 0
        if result.quantity:
            assert result.quantity * 100 >= 5
            assert result.quote_commitment == result.quantity * Decimal('100.1')


def test_assessment_binds_source_identity_not_only_parameters():
    first_args = packet()
    first = api().evaluate_context(**first_args)
    second_args = packet()
    second_args['source_sha256'] = '8' * 64
    second_args['binding'] = recipes().bind_policy(second_args['recipe'], source_sha256='8' * 64)
    second = api().evaluate_context(**second_args)
    assert first.assessment_sha256 != second.assessment_sha256
    assert first.source_sha256 == '9' * 64
    assert first.state_sha256 == first_args['capacity'].state_sha256
    assert first.state_version == 3


def test_shadow_retains_existing_hard_vetoes():
    args = packet('shadow')
    result = api().evaluate_context(**args)
    _, trace = api().project_shadow(None, args['opportunity'], result,
        prerequisites=(Prerequisite(name='native_risk', status='HOLD'),))
    assert 'native_risk:HOLD' in trace.blockers


def test_cached_prerequisite_expires_before_primary_opportunity():
    args = packet()
    result = api().evaluate_context(**args)
    prerequisite = api().admission_prerequisite(result, opportunity=args['opportunity'], now_ms=149000)
    assert prerequisite.status == 'STALE'
    assert args['opportunity'].expires_at_ms > 149000


def test_prerequisite_cannot_be_attached_to_a_reduction_or_different_opportunity():
    args = packet()
    result = api().evaluate_context(**args)
    reduction = args['opportunity'].model_copy(update={'action': 'reduce'})
    with pytest.raises(ValueError):
        api().admission_prerequisite(result, opportunity=reduction, now_ms=123000)
