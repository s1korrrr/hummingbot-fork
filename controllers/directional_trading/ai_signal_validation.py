"""Pure prediction validation; this module does not grant execution authority."""

import math
from dataclasses import dataclass


def _number(value, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    try:
        result = float(value)
    except OverflowError as error:
        raise ValueError(f"{label} must be a finite number") from error
    if not math.isfinite(result):
        raise ValueError(f"{label} must be a finite number")
    return result


@dataclass(frozen=True)
class ValidatedPrediction:
    timestamp: float
    received_at: float
    valid_until: float
    probabilities: tuple[float, float, float]
    target_pct: float

    def is_fresh(self, now: float) -> bool:
        try:
            instant = _number(now, "clock")
        except ValueError:
            return False
        return self.received_at <= instant < self.valid_until


def validate_prediction(
    payload: dict, *, now: float, max_age: float, max_future_skew: float,
    max_target_pct: float, last_timestamp: float | None,
) -> ValidatedPrediction:
    now = _number(now, "clock")
    max_age = _number(max_age, "maximum age")
    max_future_skew = _number(max_future_skew, "clock skew")
    max_target_pct = _number(max_target_pct, "maximum target")
    if max_age <= 0 or max_future_skew < 0 or not 0 < max_target_pct <= 1:
        raise ValueError("invalid prediction validation policy")
    if not isinstance(payload, dict):
        raise ValueError("prediction must be an object")
    timestamp = _number(payload.get("timestamp"), "timestamp")
    if timestamp <= 0 or now - timestamp >= max_age or timestamp > now + max_future_skew:
        raise ValueError("prediction timestamp is stale, future, or invalid")
    if last_timestamp is not None and timestamp <= _number(last_timestamp, "last timestamp"):
        raise ValueError("prediction timestamp was replayed or regressed")
    raw = payload.get("probabilities")
    if not isinstance(raw, (list, tuple)) or len(raw) != 3:
        raise ValueError("probabilities must contain short, neutral, and long")
    probabilities = tuple(_number(value, "probability") for value in raw)
    if any(not 0 <= value <= 1 for value in probabilities) or not math.isclose(
        math.fsum(probabilities), 1.0, rel_tol=0.0, abs_tol=1e-6
    ):
        raise ValueError("probabilities must lie in [0, 1] and sum to one")
    target = _number(payload.get("target_pct", 0.01), "target_pct")
    if not 0 < target <= max_target_pct:
        raise ValueError("target_pct exceeds configured bounds")
    return ValidatedPrediction(
        timestamp=timestamp, received_at=now, valid_until=min(timestamp + max_age, now + max_age),
        probabilities=probabilities, target_pct=target,
    )
