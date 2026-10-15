import os
import subprocess
import sys
from decimal import Decimal
from fractions import Fraction

import pytest

from hummingbot.strategy_v2.market_context.market_picture.diagnostics import (
    capped_weights,
    matched_pressure_trajectory,
    normalized_breadth,
    price_breadth_divergence,
    top_five_absolute_contribution_share,
    weighted_cohort_return,
)


def _returns(advances: int, declines: int, unchanged: int) -> dict[str, Decimal]:
    values = [Decimal("0.001")] * advances + [Decimal("-0.001")] * declines + [Decimal(0)] * unchanged
    return {f"asset-{index:02d}": value for index, value in enumerate(values)}


def test_t01_one_basis_point_normalized_breadth_preserves_missing_coverage():
    values = _returns(5, 2, 1)
    values.update({"missing-1": None, "missing-2": None})
    result = normalized_breadth(values, expected_count=10)
    assert (result.advances, result.declines, result.unchanged) == (5, 2, 1)
    assert (result.valid_count, result.expected_count) == (8, 10)
    assert result.pressure == Decimal("0.375")


def test_one_basis_point_band_is_distinct_from_legacy_zero_sign_classification():
    result = normalized_breadth(
        {"above": "0.00010001", "below": "-0.00010001", "edge": "0.0001", "zero": "-0.0001"},
        expected_count=4,
    )
    assert (result.advances, result.declines, result.unchanged, result.pressure) == (
        1,
        1,
        2,
        Decimal(0),
    )


def test_t04_missing_start_cohort_member_does_not_become_survivor_index_return():
    ids = ("asset-a", "asset-b")
    weights = {"asset-a": Decimal("0.5"), "asset-b": Decimal("0.5")}
    assert weighted_cohort_return({"asset-a": Decimal("0.10"), "asset-b": None}, weights, expected_ids=ids) is None
    assert weighted_cohort_return({"asset-a": Decimal("0.10")}, {"asset-a": Decimal(1)}, expected_ids=ids) is None


def test_t29_top_five_absolute_contribution_survives_signed_cancellation():
    contributions = {
        "a": "0.04",
        "b": "-0.04",
        "c": "0.01",
        "d": "-0.01",
        "e": "0.005",
        "f": "-0.005",
    }
    actual = top_five_absolute_contribution_share(contributions, expected_ids=tuple(sorted(contributions)))
    assert actual == Decimal("0.105") / Decimal("0.11")
    assert top_five_absolute_contribution_share({"a": "0.01", "b": "-0.01"}, expected_ids=("a", "b")) == Decimal(1)


def test_t29_missing_member_and_near_zero_gross_contribution_are_unavailable():
    assert top_five_absolute_contribution_share({"a": "0.04", "b": None}, expected_ids=("a", "b")) is None


def test_t29_five_member_full_cohort_concentration_is_exactly_one_under_decimal_rounding():
    contributions = {
        "okx:spot:BNB-USDC": "0.001106136594774550331085303567",
        "okx:spot:BTC-USDC": "0.004134408941867593316062080394",
        "okx:spot:C000-USDC": "0.003017589170140663518087565270",
        "okx:spot:ETH-USDC": "-0.0003583049416539065794604584970",
        "okx:spot:SOL-USDC": "-0.003272704361774138407082364014",
    }

    actual = top_five_absolute_contribution_share(contributions, expected_ids=tuple(sorted(contributions)))

    assert actual == Decimal(1)
    assert (
        top_five_absolute_contribution_share({"a": "0.0000000000001", "b": "-0.0000000000001"}, expected_ids=("a", "b"))
        is None
    )


def test_t33_pressure_trajectory_uses_one_matched_cohort_and_declared_tau_units():
    current = _returns(7, 2, 1)  # pressure 0.5
    previous = _returns(6, 2, 2)  # pressure 0.4
    oldest = _returns(4, 2, 4)  # pressure 0.2
    cutoff = 1_800_000_000_000
    tau = 15 * 60_000
    result = matched_pressure_trajectory(
        {cutoff: current, cutoff - tau: previous, cutoff - 2 * tau: oldest},
        cutoff_ms=cutoff,
        horizon_minutes=15,
        interval_minutes=15,
        expected_count=10,
    )
    assert result.level == Decimal("0.5")
    assert result.change == Decimal("0.1")
    assert result.velocity == Decimal("0.1") / 15
    assert result.acceleration == Decimal("-0.1") / 225
    assert result.share_change_percentage_points == Decimal(10)
    assert result.matched_count == 10 and len(result.matched_membership_hash) == 64


def test_trajectory_advance_share_change_uses_the_normalized_one_basis_point_boundary():
    cutoff = 1_800_000_000_000
    tau = 5 * 60_000
    current = {"a": "0.0002", "b": "0.00005", "c": "-0.0002", "d": "0"}
    previous = {"a": "0.00005", "b": "0.00005", "c": "-0.0002", "d": "0"}
    oldest = dict(previous)
    result = matched_pressure_trajectory(
        {cutoff: current, cutoff - tau: previous, cutoff - 2 * tau: oldest},
        cutoff_ms=cutoff,
        horizon_minutes=5,
        interval_minutes=5,
        expected_count=4,
    )
    assert result.share_change_percentage_points == Decimal(25)


def test_trajectory_is_unavailable_when_any_required_timepoint_or_member_is_missing():
    cutoff = 1_800_000_000_000
    tau = 5 * 60_000
    complete = _returns(2, 1, 1)
    incomplete = dict(complete)
    incomplete.pop("asset-00")
    result = matched_pressure_trajectory(
        {cutoff: complete, cutoff - tau: incomplete},
        cutoff_ms=cutoff,
        horizon_minutes=5,
        interval_minutes=5,
        expected_count=4,
    )
    assert result.level is None and result.change is None and result.acceleration is None


def test_t34_divergence_is_causal_and_requires_prior_range_and_matched_change():
    assert price_breadth_divergence(current_close=101, prior_closes=(99, 100), pressure_change="-0.1") is True
    assert price_breadth_divergence(current_close=101, prior_closes=(99, 100), pressure_change="0.1") is False
    assert price_breadth_divergence(current_close=99, prior_closes=(100, 101), pressure_change="0.1") is True
    assert price_breadth_divergence(current_close=101, prior_closes=(99, 100), pressure_change=None) is None


def test_t40_capped_liquidity_weights_require_a_feasible_complete_prior_cohort():
    assert capped_weights({f"a{i}": 1 for i in range(9)}, expected_ids=tuple(f"a{i}" for i in range(9))) is None
    ids = tuple(f"a{i}" for i in range(10))
    weights = capped_weights({key: (100 if key == "a0" else 1) for key in ids}, expected_ids=ids)
    assert weights is not None
    assert weights["a0"] == Decimal("0.10")
    assert max(weights.values()) <= Decimal("0.10")
    assert sum(weights.values()) == Decimal(1)
    assert capped_weights({key: 1 for key in ids[:-1]}, expected_ids=ids) is None
    assert capped_weights({key: (0 if key == "a0" else 1) for key in ids}, expected_ids=ids) is None
    assert capped_weights({key: ("NaN" if key == "a0" else 1) for key in ids}, expected_ids=ids) is None
    with pytest.raises(ValueError, match="finite positive numbers"):
        capped_weights({key: ("not-a-number" if key == "a0" else 1) for key in ids}, expected_ids=ids)
    thirds = capped_weights(
        {key: 1 for key in ("a", "b", "c")},
        expected_ids=("a", "b", "c"),
        cap=Decimal("0.33333333333333333333333333334"),
    )
    assert thirds is not None
    assert sum((Fraction(value) for value in thirds.values()), Fraction(0)) == 1
    assert max(thirds.values()) <= Decimal("0.33333333333333333333333333334")
    assert sum(value == Decimal("0.33333333333333333333333333334") for value in thirds.values()) == 2


def test_capped_weights_are_hashseed_stable_for_300_positive_members():
    script = """
import json
from decimal import Decimal, localcontext
from hummingbot.strategy_v2.market_context.market_picture.diagnostics import capped_weights

ids = tuple(f"asset-{index:03d}" for index in range(300))
raw = {identity: Decimal(index + 1) for index, identity in enumerate(ids)}
weights = capped_weights(raw, expected_ids=ids)
with localcontext() as context:
    context.prec = 9
    reversed_weights = capped_weights(dict(reversed(tuple(raw.items()))), expected_ids=ids)
assert weights is not None
assert reversed_weights == weights
assert tuple(weights) == ids
assert all(Decimal(0) < weight <= Decimal("0.10") for weight in weights.values())
assert sum(weights.values(), Decimal(0)) == Decimal(1)
print(json.dumps([str(weights[identity]) for identity in ids]))
"""
    outputs = []
    for seed in ("20", "56", "59"):
        environment = dict(os.environ, PYTHONHASHSEED=seed)
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        outputs.append(completed.stdout)
    assert outputs[0] == outputs[1] == outputs[2]
