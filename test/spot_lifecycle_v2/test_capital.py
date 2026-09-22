from decimal import Decimal as D

import pytest

from controllers.generic.spot_recipe_v2_core.acquisition import evaluate_acquisition
from controllers.generic.spot_recipe_v2_core.capital import (
    CAPABILITIES,
    AcquisitionLedgerV2,
    AdmittedStage,
    RecorderOwner,
    RecorderScopeEvidence,
    StageInputs,
    admit_acquisition,
    declared_capability,
    reconciled_ledger,
    size_stage,
)
from hummingbot.strategy_v2.components.capital_policy import CapitalLimits, CapitalSnapshot, NativeExposure
from hummingbot.strategy_v2.components.config import AcquisitionPolicy, RiskPolicy
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.program_budget import BudgetPolicy

from .test_acquisition import opportunity, ready
from .test_lifecycle import pending, snapshot

RISK = RiskPolicy(fee_reserve_fraction=D('.005'), cash_reserve_quote=D('0'))


def managing(filled_quote, **updates):
    values = dict(remaining_base=D('.15'), entry_gross_quote=filled_quote, entry_terminal=True)
    return snapshot(**(values | updates))


def assess(view, purpose='initial_entry', now_ms=1002, max_age_ms=10):
    cursor = evaluate_acquisition(view, None, opportunity(999), purpose=purpose,
                                  now_ms=1002, max_age_ms=300, prerequisites=()).cursor
    return evaluate_acquisition(view, cursor, opportunity(), purpose=purpose,
                                now_ms=now_ms, max_age_ms=max_age_ms, prerequisites=())


def raw_ledger(*stages, grant=D('100')):
    return AcquisitionLedgerV2(owner_id='spot_v2_owner', executor_id='lot_1', grant_quote=grant, stages=stages)


PREDECESSORS: dict[str, AcquisitionLedgerV2] = {}


def ledger(*stages, grant=D('100')):
    """A ledger holding fills only exists by reconciling its admitted predecessor, so build it that way."""
    current = raw_ledger(*stages, grant=grant)
    if current.filled_quote == 0:
        return current
    empty = (item.model_copy(update={'filled_quote': D('0'), 'filled_fees_quote': D('0'),
                                     'filled_base': D('0')}) for item in stages)
    before = raw_ledger(*empty, grant=grant)
    after = reconciled_ledger(before, current)
    PREDECESSORS[after.ledger_sha256] = before
    return after


def sized(assessment, snapshot, book, *args, **kwargs):
    """size_stage with the predecessor the helper actually reconciled from, or None for a fresh ledger."""
    return size_stage(assessment, snapshot, book, *args,
                      predecessor=PREDECESSORS.get(book.ledger_sha256), **kwargs)


def stage(request_id='request_1', **updates):
    values = dict(request_id=request_id, admitted_quote=D('40'), filled_quote=D('15'),
                  filled_fees_quote=D('.075'), filled_base=D('.15'), terminal=False)
    return AdmittedStage(**(values | updates))


def inputs(**updates):
    values = dict(bid=D('100'), available_quote=D('500'), liquidity_cap_quote=D('500'),
                  amount_step=D('.01'), minimum_amount=D('.01'), minimum_notional=D('1'))
    return StageInputs(**(values | updates))


def evidence(owners=None, **updates):
    values = dict(account_id='okx_main', recorder_sha256='e' * 64,
                  budget_policy=BudgetPolicy(group='fixture_program_v2', max_quote=D('500'), max_assets=4),
                  owners=owners or (RecorderOwner(owner_id='spot_v2_owner', recorder_sha256='e' * 64, represented=True),),
                  unrepresented_commitments=False)
    return RecorderScopeEvidence(**(values | updates))


def exposure(**updates):
    values = dict(executor_id='lot_1', program_id='fixture_program_v2', trading_pair='BTC-USDC',
                  marked_inventory_quote=D('0'), reserved_acquisition_quote=D('0'))
    return NativeExposure(**(values | updates))


def capital(**updates):
    values = dict(account_id='okx_main', valuation_currency='USDC', observed_at_ms=1000,
                  source_sha256='1' * 64, valuation_sha256='2' * 64, complete=True, reconciled=True,
                  risk_paused=False, available_quote=D('1000'), exposures=())
    return CapitalSnapshot(**(values | updates))


LIMITS = CapitalLimits(account_quote=D('1000'), program_quote=D('500'), base_quote=D('400'),
                       order_quote=D('200'), max_age_ms=10)


def one_shot(**updates):
    return sized(assess(ready()), ready(), ledger(), AcquisitionPolicy(fractions=(D('1'),)), RISK,
                 inputs(**updates))


def test_one_shot_entry_reserves_the_whole_grant_including_fees():
    result = one_shot()
    assert result.status == 'SIZED'
    assert result.reason == 'eligible'
    assert result.stage_index == 0
    assert result.base_amount == D('.99')
    assert result.quote_amount == D('.99') * D('100') * D('1.005')
    assert result.quote_amount <= D('100')
    assert result.residual_quote == D('100')
    assert result.execution_authorized is False
    assert result.economic_backtest is False
    assert len(result.input_sha256) == 64


def test_staged_accumulation_uses_the_real_minimum_drop_rule():
    policy = AcquisitionPolicy(fractions=(D('.4'), D('.6')))
    filled = stage(filled_quote=D('25'), filled_fees_quote=D('.125'), filled_base=D('.25'), terminal=True)
    view = managing(D('25'), remaining_base=D('.25'))
    blocked = sized(assess(view, 'dca'), view, ledger(filled), policy, RISK, inputs(bid=D('99')))
    assert blocked.status == 'HOLD'
    assert blocked.reason == 'minimum_stage_drop_unmet'
    allowed = sized(assess(view, 'dca'), view, ledger(filled), policy, RISK, inputs(bid=D('95')))
    assert allowed.status == 'SIZED'
    assert allowed.stage_index == 1
    assert allowed.residual_quote == D('74.875')


def test_exhausted_schedule_is_not_silent_pyramiding():
    filled = stage(admitted_quote=D('100'), filled_quote=D('25'), filled_fees_quote=D('.125'),
                   filled_base=D('.25'), terminal=True)
    view = managing(D('25'), remaining_base=D('.25'))
    result = sized(assess(view, 'dca'), view, ledger(filled), AcquisitionPolicy(fractions=(D('1'),)),
                   RISK, inputs(bid=D('95')))
    assert result.reason == 'acquisition_schedule_exhausted'
    assert result.quote_amount is None


def test_unsupported_pyramiding_purpose_is_rejected_by_the_canonical_gate():
    with pytest.raises(ValueError):
        assess(ready(), 'pyramiding')


def test_short_or_reduction_sizing_is_refused():
    assessment = assess(ready())
    reduction = assessment.model_copy(update={'cursor': assessment.cursor.model_copy(update={'action': 'reduce'})})
    with pytest.raises(PolicyError, match='reduction'):
        sized(reduction, ready(), ledger(), AcquisitionPolicy(fractions=(D('1'),)), RISK, inputs())


@pytest.mark.parametrize('updates,reason', [
    ({'available_quote': D('100')}, 'insufficient_fee_reserve'),
    ({'minimum_amount': D('2')}, 'below_minimum_amount'),
    ({'minimum_notional': D('200')}, 'below_minimum_notional'),
    ({'liquidity_cap_quote': D('.5'), 'amount_step': D('1'), 'minimum_amount': D('1')}, 'stage_quote_below_precision'),
])
def test_sizing_vetoes_are_named_precisely(updates, reason):
    result = one_shot(**updates)
    assert result.status == 'HOLD'
    assert result.reason == reason
    assert result.base_amount is None


def test_cash_reserve_counts_against_the_fee_reserve():
    risk = RiskPolicy(fee_reserve_fraction=D('.005'), cash_reserve_quote=D('450'))
    result = sized(assess(ready()), ready(), ledger(), AcquisitionPolicy(fractions=(D('1'),)), risk, inputs())
    assert result.reason == 'insufficient_fee_reserve'


@pytest.mark.parametrize('view,book,reason', [
    (ready(), ledger(), 'dca_requires_managing_inventory'),
    (managing(D('15'), fees_complete=False), ledger(stage(terminal=True)), 'native_fees_incomplete'),
    (managing(D('15'), reconciled=False), ledger(stage(terminal=True)), 'native_reconciliation_required'),
    (managing(D('15'), terminal=True), ledger(stage(terminal=True)), 'dca_requires_managing_inventory'),
])
def test_dca_cannot_bypass_inventory_or_management_constraints(view, book, reason):
    result = sized(assess(view, 'dca'), view, book,
                   AcquisitionPolicy(fractions=(D('.4'), D('.6'))), RISK, inputs(bid=D('95')))
    assert result.status == 'HOLD'
    assert result.reason == reason


def test_stale_snapshot_and_expired_opportunity_hold_separately():
    view = managing(D('15'))
    policy = AcquisitionPolicy(fractions=(D('.4'), D('.6')))
    book = ledger(stage(terminal=True))
    stale = sized(assess(view, 'dca', now_ms=2000, max_age_ms=10), view, book, policy, RISK, inputs(bid=D('95')))
    assert stale.reason == 'native_snapshot_stale'
    expired = sized(assess(view, 'dca', now_ms=1200, max_age_ms=300), view, book, policy, RISK, inputs(bid=D('95')))
    assert expired.reason == 'opportunity_expired'


def test_partial_fill_with_unacknowledged_cancel_keeps_capacity_reserved():
    view = managing(D('15'))
    policy = AcquisitionPolicy(fractions=(D('.4'), D('.6')))
    outstanding = ledger(stage())
    held = sized(assess(view, 'dca'), view, outstanding, policy, RISK, inputs(bid=D('95')))
    assert held.status == 'HOLD'
    assert held.reason == 'unresolved_acquisition_outstanding'
    assert held.residual_quote == D('60')

    late = ledger(stage(filled_quote=D('25'), filled_fees_quote=D('.125'), filled_base=D('.25'), terminal=True))
    reconciled = reconciled_ledger(outstanding, late)
    settled = managing(D('25'), remaining_base=D('.25'))
    result = size_stage(assess(settled, 'dca'), settled, reconciled, policy, RISK, inputs(bid=D('95')),
                        predecessor=outstanding)
    assert result.status == 'SIZED'
    assert result.residual_quote == D('74.875')
    assert result.quote_amount == D('.62') * D('95') * D('1.005')
    assert result.quote_amount != D('40')


def test_a_rebuilt_ledger_cannot_launder_away_outstanding_capacity():
    """The audit counterexample: rewrite admitted down to the fills and 85 of live capacity vanishes."""
    view = managing(D('15'))
    rebuilt = raw_ledger(stage(admitted_quote=D('15.075'), terminal=True), grant=D('100'))
    assert rebuilt.residual_quote == D('84.925')
    with pytest.raises(PolicyError, match='descend from its predecessor'):
        sized(assess(view, 'dca'), view, rebuilt, AcquisitionPolicy(fractions=(D('.5'), D('.5'))),
              RISK, inputs(bid=D('95')))


def test_a_quoted_lineage_digest_is_not_evidence_without_the_predecessor_itself():
    """A digest is cheap to write down. The check is against the predecessor object, not the string.

    This bounds an honest caller that rebuilds its ledger, which is the realistic in-process
    mistake. It does not bound a caller that fabricates the root of its own chain; nothing inside
    a pure function can, and closing that needs the authenticated persistence seam gated in M9.
    """
    view = managing(D('15'))
    stages = (stage(admitted_quote=D('15.075'), terminal=True),)
    forged = raw_ledger(*stages, grant=D('100')).model_copy(update={'lineage_sha256': 'a' * 64})
    with pytest.raises(PolicyError, match='checked against its predecessor'):
        sized(assess(view, 'dca'), view, forged, AcquisitionPolicy(fractions=(D('.5'), D('.5'))),
              RISK, inputs(bid=D('95')))
    with pytest.raises(PolicyError, match='does not match the supplied predecessor'):
        size_stage(assess(view, 'dca'), view, forged, AcquisitionPolicy(fractions=(D('.5'), D('.5'))),
                   RISK, inputs(bid=D('95')), predecessor=raw_ledger(*stages, grant=D('100')))


def test_an_unacknowledged_predecessor_request_survives_a_rebuild_the_venue_cannot_confirm():
    """`snapshot.commitments` never saw this request, so only the predecessor object retains it."""
    view = managing(D('0'), remaining_base=D('0'))
    submitted = raw_ledger(stage(request_id='request_9', admitted_quote=D('40'), filled_quote=D('0'),
                                 filled_fees_quote=D('0'), filled_base=D('0')), grant=D('100'))
    rebuilt = raw_ledger(grant=D('100')).model_copy(update={'lineage_sha256': submitted.ledger_sha256})
    assert rebuilt.residual_quote == D('100')
    with pytest.raises(PolicyError, match='drops an unresolved predecessor acquisition'):
        size_stage(assess(view), view, rebuilt, AcquisitionPolicy(fractions=(D('1'),)), RISK,
                   inputs(bid=D('95')), predecessor=submitted)


def test_only_reconciliation_confers_lineage_and_it_names_the_exact_predecessor():
    outstanding = ledger(stage(admitted_quote=D('100')), grant=D('100'))
    assert outstanding.lineage_sha256 is not None
    assert raw_ledger(stage(admitted_quote=D('100')), grant=D('100')).lineage_sha256 is None
    settled = reconciled_ledger(outstanding, raw_ledger(stage(admitted_quote=D('100'), terminal=True),
                                                        grant=D('100')))
    assert settled.lineage_sha256 == outstanding.ledger_sha256


@pytest.mark.parametrize('stages', [
    (dict(request_id='request_2', terminal=True),),
    (dict(request_id='request_1', terminal=True),),
])
def test_a_ledger_that_forgets_a_live_native_request_is_refused(stages):
    """Dropping the request, or calling it resolved, both lose capacity the exchange may still fill."""
    view = managing(D('15'), commitments=(pending(status='PENDING'),))
    forgetful = ledger(*(stage(**item) for item in stages), grant=D('100'))
    with pytest.raises(PolicyError, match='omits an outstanding native acquisition request'):
        sized(assess(view, 'dca'), view, forgetful, AcquisitionPolicy(fractions=(D('.5'), D('.5'))),
              RISK, inputs(bid=D('95')))


def test_a_late_fill_cannot_create_a_second_request_or_shrink_evidence():
    outstanding = ledger(stage())
    with pytest.raises(PolicyError, match='terminal'):
        reconciled_ledger(outstanding, ledger(stage('request_2')))
    with pytest.raises(PolicyError, match='regress'):
        reconciled_ledger(outstanding, ledger(stage(filled_quote=D('5'), filled_base=D('.05'))))
    with pytest.raises(PolicyError, match='grant'):
        reconciled_ledger(outstanding, ledger(stage(admitted_quote=D('80'))))


def test_two_unresolved_requests_in_one_scope_are_invalid_evidence():
    with pytest.raises(ValueError, match='unresolved'):
        ledger(stage('request_1'), stage('request_2'))


def test_a_shadow_scope_keeps_separate_admitted_state():
    live = ledger(stage(terminal=True, filled_quote=D('25'), filled_fees_quote=D('.125'), filled_base=D('.25')))
    shadow = AcquisitionLedgerV2(owner_id='spot_v2_owner', executor_id='shadow_lot_1',
                                 grant_quote=D('100'), stages=())
    with pytest.raises(PolicyError, match='scope'):
        reconciled_ledger(live, shadow)
    assert shadow.residual_quote == D('100')
    assert live.residual_quote == D('74.875')


def test_duplicate_event_delivery_cannot_multiply_exposure():
    view = managing(D('15'))
    first = assess(view, 'dca')
    repeat = evaluate_acquisition(view, first.cursor, opportunity(), purpose='dca',
                                  now_ms=1003, max_age_ms=10, prerequisites=())
    result = sized(repeat, view, ledger(stage(terminal=True)),
                   AcquisitionPolicy(fractions=(D('.4'), D('.6'))), RISK, inputs(bid=D('95')))
    assert result.status == 'HOLD'
    assert result.reason == 'already_observed'


def test_ledger_cannot_disagree_with_authoritative_native_fills():
    view = managing(D('15'))
    with pytest.raises(PolicyError, match='disagree'):
        sized(assess(view, 'dca'), view, ledger(stage(terminal=True, filled_quote=D('9'))),
              AcquisitionPolicy(fractions=(D('.4'), D('.6'))), RISK, inputs(bid=D('95')))


def test_a_terminal_fill_without_fee_evidence_is_not_a_free_budget():
    with pytest.raises(ValueError, match='fee'):
        stage(terminal=True, filled_fees_quote=D('0'))
    with pytest.raises(ValueError, match='grant'):
        stage(filled_quote=D('45'), filled_base=D('.45'))


def test_residual_dust_at_apparent_completion_stays_qualified():
    view = managing(D('25'), remaining_base=D('.00000001'), terminal=True)
    result = sized(assess(view, 'dca'), view,
                   ledger(stage(filled_quote=D('25'), filled_fees_quote=D('.125'),
                                filled_base=D('.25'), terminal=True)),
                   AcquisitionPolicy(fractions=(D('.4'), D('.6'))), RISK, inputs(bid=D('95')))
    assert result.status == 'HOLD'
    assert result.reason == 'dca_requires_managing_inventory'


def test_capability_matrix_declares_account_atomic_unsupported():
    assert {item.name: item.supported for item in CAPABILITIES} == {'RECORDER_SCOPED': True, 'ACCOUNT_ATOMIC': False}
    assert declared_capability('ACCOUNT_ATOMIC').supported is False
    with pytest.raises(PolicyError):
        declared_capability('ACCOUNT_WIDE')


def admit(**updates):
    args = dict(stage=one_shot(), evidence=evidence(), snapshot=capital(), limits=LIMITS, risk=RISK, now_ms=1002)
    args.update(updates)
    return admit_acquisition(args.pop('stage'), args.pop('evidence'), args.pop('snapshot'),
                             args.pop('limits'), args.pop('risk'), **args)


def test_admission_delegates_to_the_canonical_capital_assessor():
    result = admit()
    assert result.status == 'ELIGIBLE_FOR_NATIVE_ADMISSION'
    assert result.reason == 'eligible'
    assert result.capability == 'RECORDER_SCOPED'
    assert result.admitted_quote == one_shot().quote_amount
    assert result.assessment.projected_account_quote == result.admitted_quote
    assert result.execution_authorized is False
    assert result.assessment.execution_authorized is False


def test_account_atomic_admission_is_refused():
    result = admit(capability='ACCOUNT_ATOMIC')
    assert result.status == 'HOLD'
    assert result.reason == 'account_atomic_unsupported'
    assert result.assessment is None
    assert result.request is None


def test_two_independent_recorder_owners_sharing_cash_admit_nothing():
    owners = (RecorderOwner(owner_id='spot_v2_owner', recorder_sha256='e' * 64, represented=True),
              RecorderOwner(owner_id='ok_rsi', recorder_sha256='f' * 64, represented=False))
    result = admit(evidence=evidence(owners=owners))
    assert result.status == 'HOLD'
    assert result.reason == 'native_account_segregation_unproven'
    assert result.assessment is None
    assert result.admitted_quote is None


@pytest.mark.parametrize('asserted', [
    BudgetPolicy(group='fixture_program_v2', max_quote=D('1'), max_assets=1),
    BudgetPolicy(group='totally_unrelated', max_quote=D('999999'), max_assets=1000),
])
def test_an_asserted_per_bot_cap_is_not_segregation_evidence(asserted):
    """Recorder identity decides segregation. A restrictive cap does not prove it and a
    generous one does not waive it, so the asserted policy cannot change the outcome."""
    owners = (RecorderOwner(owner_id='spot_v2_owner', recorder_sha256='e' * 64, represented=True),
              RecorderOwner(owner_id='ok_rsi', recorder_sha256='f' * 64, represented=True))
    result = admit(evidence=evidence(owners=owners, budget_policy=asserted))
    assert result.reason == 'native_account_segregation_unproven'


def test_unrepresented_commitments_block_admission():
    result = admit(evidence=evidence(unrepresented_commitments=True))
    assert result.reason == 'native_commitments_unrepresented'
    assert result.assessment is None


def test_an_already_reserved_commitment_is_not_counted_twice():
    result = admit(snapshot=capital(exposures=(exposure(reserved_acquisition_quote=D('40')),)))
    assert result.status == 'HOLD'
    assert result.reason == 'unresolved_acquisition_outstanding'
    assert result.assessment is None


def dca_stage(**updates):
    policy = AcquisitionPolicy(fractions=(D('.5'), D('.5')))
    filled = AdmittedStage(request_id='request_1', admitted_quote=D('101'), filled_quote=D('100'),
                           filled_fees_quote=D('.5'), filled_base=D('1'), terminal=True)
    view = managing(D('100'), remaining_base=D('1'))
    return sized(assess(view, 'dca'), view, ledger(filled, grant=D('200')), policy, RISK,
                 inputs(bid=D('95'), **updates))


def test_marked_loss_beyond_the_risk_envelope_vetoes_further_acquisition():
    held = capital(exposures=(exposure(marked_inventory_quote=D('80')),))
    result = admit(stage=dca_stage(), snapshot=held)
    assert result.status == 'HOLD'
    assert result.reason == 'marked_loss_limit'
    assert result.assessment is None


def test_missing_native_mark_for_existing_inventory_holds():
    result = admit(stage=dca_stage(), snapshot=capital())
    assert result.reason == 'native_marked_inventory_missing'


@pytest.mark.parametrize('updates,reason', [
    ({'risk_paused': True}, 'native_risk_paused'),
    ({'reconciled': False}, 'native_reconciliation_required'),
    ({'observed_at_ms': 900}, 'native_snapshot_stale'),
    ({'available_quote': D('50')}, 'available_quote_limit'),
])
def test_canonical_account_vetoes_are_not_reimplemented(updates, reason):
    result = admit(snapshot=capital(**updates))
    assert result.status == 'HOLD'
    assert result.reason == reason


def test_order_limit_veto_comes_from_the_canonical_limits():
    result = admit(limits=LIMITS.model_copy(update={'order_quote': D('10')}))
    assert result.reason == 'order_limit'


def test_a_held_sizing_result_cannot_be_admitted():
    result = admit(stage=one_shot(minimum_amount=D('2')))
    assert result.status == 'HOLD'
    assert result.reason == 'below_minimum_amount'
    assert result.request is None


def test_admission_account_scope_must_match_the_recorder_evidence():
    with pytest.raises(PolicyError, match='account'):
        admit(evidence=evidence(account_id='other_account'))
