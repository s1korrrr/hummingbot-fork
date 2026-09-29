"""Pure transition predicates and stable occurrence identity for descriptive events."""

import math
from dataclasses import dataclass
from hashlib import sha256
from statistics import fmean
from typing import Mapping, Sequence

from hummingbot.strategy_v2.components.recipe_contract import canonical_json


@dataclass(frozen=True)
class ThresholdTransition:
    direction: str
    prior_value: float
    current_value: float


@dataclass(frozen=True)
class ValidObservation:
    instrument_id: str | None
    source_schema: str
    sequence: int
    cutoff_ms: int
    value: float | None
    status: str = "VALID"


def _valid_pair(previous: float | None, current: float | None) -> bool:
    return previous is not None and current is not None and math.isfinite(previous) and math.isfinite(current)


def threshold_crossing(
    previous: float | None, current: float | None, threshold: float, *, above: bool
) -> ThresholdTransition | None:
    """Return only a true crossing; invalid/missing values never bridge a gap."""
    if not math.isfinite(threshold):
        raise ValueError("threshold must be finite")
    if not _valid_pair(previous, current):
        return None
    before, after = bool(previous > threshold), bool(current > threshold)
    if above and not before and after:
        return ThresholdTransition("above", float(previous), float(current))
    if not above and previous >= threshold and current < threshold:
        return ThresholdTransition("below", float(previous), float(current))
    return None


def regime_transition(
    previous: ValidObservation,
    current: ValidObservation,
    *,
    previous_label: str | None,
    current_label: str | None,
    expected_instrument_id: str,
    expected_schema: str,
    interval_ms: int = 60_000,
) -> bool:
    """Only consecutive, valid, same-instrument/schema observations can create a regime shift."""
    if type(interval_ms) is not int or interval_ms < 1:
        raise ValueError("regime cadence must be a positive integer number of milliseconds")
    if previous_label is None or current_label is None or previous_label == current_label:
        return False
    return bool(
        previous.status == current.status == "VALID"
        and previous.instrument_id == current.instrument_id == expected_instrument_id
        and previous.source_schema == current.source_schema == expected_schema
        and current.sequence == previous.sequence + 1
        and current.cutoff_ms - previous.cutoff_ms == interval_ms
    )


def trend_continuation(
    observations: Sequence[tuple[int, str | None, float | None, str]],
    *,
    interval_ms: int = 60_000,
    threshold: float = 25.0,
) -> str | None:
    """Direction-stable, strictly rising ADX over three consecutive valid technical bars."""
    if type(interval_ms) is not int or interval_ms < 1 or not math.isfinite(threshold):
        raise ValueError("invalid trend-event parameters")
    if len(observations) != 3:
        return None
    times, directions, adx_values, statuses = zip(*observations)
    if (
        any(status != "VALID" for status in statuses)
        or len(set(directions)) != 1
        or directions[0] not in ("up", "down")
    ):
        return None
    if (
        any(type(t) is not int for t in times)
        or times[1] - times[0] != interval_ms
        or times[2] - times[1] != interval_ms
    ):
        return None
    if any(v is None or not math.isfinite(v) or v <= threshold for v in adx_values):
        return None
    if not adx_values[0] < adx_values[1] < adx_values[2]:
        return None
    return directions[0]


def breadth_threshold_transition(
    previous_share: float | None,
    current_share: float | None,
    *,
    previous_cohort_hash: str,
    current_cohort_hash: str,
    previous_coverage: float,
    current_coverage: float,
) -> ThresholdTransition | None:
    """Same-cohort 60/40 crossing with at least the specified 95% valid coverage at both times."""
    if previous_cohort_hash != current_cohort_hash:
        return None
    if any(not math.isfinite(v) or not 0 <= v <= 1 for v in (previous_coverage, current_coverage)):
        raise ValueError("coverage must be a share in [0,1]")
    if min(previous_coverage, current_coverage) < 0.95 or not _valid_pair(previous_share, current_share):
        return None
    if not 0 <= previous_share <= 1 or not 0 <= current_share <= 1:
        raise ValueError("breadth share must be in [0,1]")
    if previous_share <= 0.60 < current_share:
        return ThresholdTransition("above_60", previous_share, current_share)
    if previous_share >= 0.40 > current_share:
        return ThresholdTransition("below_40", previous_share, current_share)
    return None


def quality_transition(previous: str, current: str) -> tuple[str, str] | None:
    """Emit only a genuine provider-quality state change."""
    allowed = {"fresh", "partial", "lagging", "stale", "warming", "missing", "unavailable"}
    if previous not in allowed or current not in allowed:
        raise ValueError("unknown market-picture quality state")
    return (previous, current) if previous != current else None


def qualified_flow_spike(
    *,
    current_buy_quote: float,
    current_sell_quote: float,
    prior_buy_quote: Sequence[float],
    prior_sell_quote: Sequence[float],
    complete_window: bool,
    current_total_quote: float,
) -> str | None:
    """Return BUY/SELL only from complete, explicit taker-side quote-volume aggregates."""
    if not complete_window:
        return None
    values = (current_buy_quote, current_sell_quote, current_total_quote)
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0
        for value in values
    ):
        raise ValueError("current flow totals must be finite nonnegative quote amounts")
    if len(prior_buy_quote) != 20 or len(prior_sell_quote) != 20:
        return None
    buy = tuple(prior_buy_quote)
    sell = tuple(prior_sell_quote)
    if any(
        isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in (*buy, *sell)
    ):
        raise ValueError("prior flow baselines must be finite nonnegative quote amounts")
    if current_total_quote <= 0 or current_buy_quote + current_sell_quote > current_total_quote:
        return None
    buy_baseline, sell_baseline = fmean(buy), fmean(sell)
    buy_share = current_buy_quote / current_total_quote
    sell_share = current_sell_quote / current_total_quote
    if buy_baseline > 0 and current_buy_quote > 2.0 * buy_baseline and buy_share >= 0.65:
        return "BUY"
    if sell_baseline > 0 and current_sell_quote > 2.0 * sell_baseline and sell_share >= 0.65:
        return "SELL"
    return None


def event_occurrence_key(
    *,
    definition_id: str,
    definition_version: str,
    instrument_id: str | None,
    universe_hash: str,
    window_start_ms: int,
    window_end_ms: int,
    transition: Mapping[str, object],
) -> str:
    """Stable identity deliberately excludes transport epoch/sequence so reconnects do not replay events."""
    if window_start_ms < 0 or window_end_ms < window_start_ms:
        raise ValueError("event window is invalid")
    payload = {
        "definition_id": definition_id,
        "definition_version": definition_version,
        "instrument_id": instrument_id,
        "universe_hash": universe_hash,
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "transition": dict(transition),
    }
    return sha256(canonical_json(payload).encode()).hexdigest()


def regime_transition_event(
    previous: ValidObservation,
    current: ValidObservation,
    *,
    previous_label: str | None,
    current_label: str | None,
    expected_instrument_id: str,
    expected_schema: str,
    interval_ms: int = 60_000,
) -> dict | None:
    if not regime_transition(
        previous,
        current,
        previous_label=previous_label,
        current_label=current_label,
        expected_instrument_id=expected_instrument_id,
        expected_schema=expected_schema,
        interval_ms=interval_ms,
    ):
        return None
    return {
        "before": previous_label,
        "value": current_label,
        "instrument_id": expected_instrument_id,
        "source_schema": expected_schema,
        "source_sequence": current.sequence,
    }
