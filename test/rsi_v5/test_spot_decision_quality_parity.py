from decimal import Decimal

import pytest

from controllers.directional_trading.rsi_v5_core.spot_decision_quality import (
    D1ResolutionStatus,
    DeferredScaleIn,
    OneShotHarvestState,
    apply_x1_fill,
    evaluate_d1,
    evaluate_x1,
)


def _deferred() -> DeferredScaleIn:
    return DeferredScaleIn(
        program_id="p",
        original_quantity=Decimal("1"),
        requested_event_id="e1",
        requested_bar_close_ms=60_000,
        bar_interval_ms=60_000,
    )


def _evaluate(deferred: DeferredScaleIn, **overrides):
    values = {
        "deferred": deferred,
        "evaluation_bar_close_ms": 120_000,
        "evaluation_bar_closed": True,
        "fresh_reference_quantity": Decimal("0.4"),
        "fresh_reference_intent": True,
        "previous_close": Decimal("100"),
        "latest_close": Decimal("101"),
    }
    values.update(overrides)
    return evaluate_d1(**values)


def test_d1_resolves_only_on_immediate_successor_closed_bar():
    before_close = _evaluate(
        _deferred(),
        evaluation_bar_closed=False,
    )
    assert before_close.status is D1ResolutionStatus.PENDING
    assert not before_close.deferred.resolved

    accepted = _evaluate(_deferred())
    assert accepted.status is D1ResolutionStatus.PROPOSED
    assert accepted.deferred.resolved
    assert accepted.proposal is not None
    assert accepted.proposal.quantity == Decimal("0.4")
    assert accepted.proposal.side == "BUY"

    repeated = _evaluate(accepted.deferred)
    assert repeated.status is D1ResolutionStatus.ALREADY_RESOLVED
    assert repeated.proposal is None


@pytest.mark.parametrize(
    ("overrides", "status"),
    [
        (
            {"fresh_reference_intent": False},
            D1ResolutionStatus.DISCARDED_NO_FRESH_INTENT,
        ),
        (
            {"latest_close": Decimal("99")},
            D1ResolutionStatus.DISCARDED_DECLINING_CLOSE,
        ),
        (
            {"protective_exit_pending": True},
            D1ResolutionStatus.DISCARDED_PROTECTIVE_EXIT,
        ),
    ],
)
def test_d1_next_bar_rejection_is_terminal(overrides, status):
    resolved = _evaluate(_deferred(), **overrides)
    assert resolved.status is status
    assert resolved.deferred.resolved
    assert resolved.proposal is None
    assert _evaluate(resolved.deferred).status is D1ResolutionStatus.ALREADY_RESOLVED


def test_d1_discards_late_evaluation_without_accumulating():
    stale = _evaluate(_deferred(), evaluation_bar_close_ms=180_000)
    assert stale.status is D1ResolutionStatus.DISCARDED_STALE
    assert stale.deferred.resolved
    assert stale.proposal is None


def test_d1_rejects_non_successor_or_unclosed_future_bar():
    with pytest.raises(ValueError, match="precedes"):
        _evaluate(_deferred(), evaluation_bar_close_ms=60_000)
    with pytest.raises(ValueError, match="future"):
        _evaluate(
            _deferred(),
            evaluation_bar_close_ms=180_000,
            evaluation_bar_closed=False,
        )


def test_x1_is_one_shot_quantized_and_restart_safe():
    state = OneShotHarvestState("p")
    proposal = evaluate_x1(state=state, eligible_unreserved_inventory=Decimal("2.03"), activation_reached=True, net_profitable=True, lot_size=Decimal("0.01"))
    assert proposal.quantity == Decimal("0.50")
    persisted = apply_x1_fill(state, Decimal("0.20"))
    assert persisted.consumed and persisted.filled_quantity == Decimal("0.20")
    assert evaluate_x1(state=persisted, eligible_unreserved_inventory=Decimal("1.83"), activation_reached=True, net_profitable=True, lot_size=Decimal("0.01")) is None


def test_x1_yields_to_protective_exit():
    assert evaluate_x1(
        state=OneShotHarvestState("p"),
        eligible_unreserved_inventory=Decimal("2"),
        activation_reached=True,
        net_profitable=True,
        lot_size=Decimal("0.01"),
        protective_exit_pending=True,
    ) is None


def test_x1_zero_fill_does_not_consume_marker():
    state = apply_x1_fill(OneShotHarvestState("p"), Decimal("0"))
    assert not state.consumed


def test_non_finite_inputs_fail_closed():
    with pytest.raises(ValueError):
        evaluate_x1(state=OneShotHarvestState("p"), eligible_unreserved_inventory=Decimal("NaN"), activation_reached=True, net_profitable=True, lot_size=Decimal("0.01"))
