import math

import pytest

from controllers.directional_trading.ai_signal_validation import validate_prediction


def valid(**changes):
    payload = {"timestamp": 1_000.0, "probabilities": [0.1, 0.1, 0.8], "target_pct": 0.01}
    payload.update(changes)
    return payload


def validate(payload, **changes):
    args = dict(now=1_001.0, max_age=30.0, max_future_skew=1.0,
                max_target_pct=0.1, last_timestamp=None)
    args.update(changes)
    return validate_prediction(payload, **args)


@pytest.mark.parametrize("timestamp", [0, -1, 900, 1003, float("nan"), float("inf"), True, "1000"])
def test_bad_timestamps_are_rejected(timestamp):
    with pytest.raises(ValueError):
        validate(valid(timestamp=timestamp))


@pytest.mark.parametrize("probabilities", [
    [], [0.2, 0.8], [0.0, 0.0, 2.0], [-0.1, 0.2, 0.9],
    [0.2, 0.2, 0.2], [float("nan"), 0, 1], [False, 0, 1], ["0", 0, 1],
])
def test_bad_probabilities_are_rejected(probabilities):
    with pytest.raises(ValueError):
        validate(valid(probabilities=probabilities))


@pytest.mark.parametrize("target", [0, -0.1, 1, math.inf, math.nan, True, "0.01"])
def test_target_is_bounded(target):
    with pytest.raises(ValueError):
        validate(valid(target_pct=target))


def test_expiry_is_half_open_and_replay_is_rejected():
    result = validate(valid())
    assert result.valid_until == 1_030
    assert not result.is_fresh(1_030)
    assert result.is_fresh(1_029.999)
    with pytest.raises(ValueError):
        validate(valid(), last_timestamp=1_000)
    with pytest.raises(ValueError):
        validate(valid(), now=1_030)


def test_snapshot_does_not_alias_message_and_future_skew_does_not_extend_lifetime():
    source = valid(timestamp=1_002)
    result = validate(source)
    source["probabilities"][2] = 2
    assert result.probabilities == (0.1, 0.1, 0.8)
    assert result.valid_until == 1_031
    assert not result.is_fresh(1_000)


@pytest.mark.parametrize("changes", [
    {"max_age": 0}, {"max_age": math.nan}, {"max_future_skew": -1},
    {"now": math.inf}, {"max_target_pct": math.nan},
])
def test_bad_clock_or_policy_is_rejected(changes):
    with pytest.raises(ValueError):
        validate(valid(), **changes)
