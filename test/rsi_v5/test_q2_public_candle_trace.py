from __future__ import annotations

import math
from test.rsi_v5.q2_public_candle_trace import observe_q2_public_candle_trace


def test_public_candle_trace_is_deterministic_and_normalized() -> None:
    first = observe_q2_public_candle_trace()
    second = observe_q2_public_candle_trace()

    assert first == second
    assert [scenario["scenario_id"] for scenario in first] == [
        "healthy_two_update_rebound",
        "strict_hmm_219_fail_close",
        "atr_veto_after_rebound",
    ]
    for scenario in first:
        assert set(scenario) == {"scenario_id", "steps", "parameters", "evidence_scope"}
        assert scenario["evidence_scope"] == [
            "generated_ohlcv",
            "public_update_processed_data",
            "public_determine_executor_actions",
        ]


def test_healthy_public_candle_path_fits_hmm_observes_trend_and_proposes_action() -> None:
    healthy = observe_q2_public_candle_trace()[0]
    arm, rebound = healthy["steps"]

    assert [arm["step_id"], rebound["step_id"]] == ["arm", "rebound"]
    assert arm["data_health"] == {"ready": True, "reason": "ok", "row_count": 240}
    assert arm["strict_hmm"]["fit"] is True
    assert arm["strict_hmm"]["detected"] is True
    assert arm["strict_hmm"]["required"] is True
    assert arm["strict_hmm"]["backend"] == "GaussianHMM"
    assert arm["strict_hmm"]["error"] is None
    assert arm["trend"]["observed"] is True
    assert arm["trend"]["valid_timeframes"] == ["long", "medium", "short"]
    assert all(math.isfinite(value) for value in arm["indicators"].values())
    assert arm["signal_state"]["raw_buy_reversal"] is True
    assert arm["signal_state"]["confirmation_active"] is True
    assert arm["signal_state"]["decision"] == "armed"
    assert arm["actions"] == []

    assert rebound["data_health"] == {"ready": True, "reason": "ok", "row_count": 241}
    assert rebound["strict_hmm"]["fit"] is True
    assert rebound["strict_hmm"]["detected"] is True
    assert rebound["trend"]["observed"] is True
    assert rebound["signal"] == 1
    assert rebound["signal_state"]["confirmation_active"] is False
    assert rebound["signal_state"]["decision"] == "ready"
    assert rebound["signal_state"]["reason"] == "signal-generated"
    assert [action["action_type"] for action in rebound["actions"]] == ["CreateExecutorAction"]
    assert rebound["actions"][0]["side"] == "BUY"


def test_219_rows_strict_hmm_fails_closed_without_action() -> None:
    scenario = observe_q2_public_candle_trace()[1]
    step = scenario["steps"][0]

    assert step["data_health"] == {"ready": True, "reason": "ok", "row_count": 219}
    assert step["strict_hmm"]["fit"] is False
    assert step["strict_hmm"]["detected"] is False
    assert "fitted GaussianHMM" in step["strict_hmm"]["error"]
    assert step["signal"] == 0
    assert step["signal_state"]["condition_ok"] is False
    assert step["signal_state"]["condition_reason"] == "strict_hmm"
    assert step["actions"] == []


def test_atr_veto_preserves_no_action_after_real_rebound_confirmation() -> None:
    scenario = observe_q2_public_candle_trace()[2]
    arm, veto = scenario["steps"]

    assert arm["signal_state"]["raw_buy_reversal"] is True
    assert arm["signal_state"]["confirmation_active"] is True
    assert veto["indicators"]["atr_pct"] > scenario["parameters"]["max_atr_pct_to_trade"]
    assert veto["signal"] == 0
    assert veto["signal_state"]["condition_ok"] is False
    assert veto["signal_state"]["condition_reason"] == "high_vol"
    assert veto["signal_state"]["confirmation_active"] is True
    assert veto["actions"] == []
