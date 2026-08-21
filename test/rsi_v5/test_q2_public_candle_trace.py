from __future__ import annotations

import copy
import json
import math
from pathlib import Path
from test.rsi_v5.q2_public_candle_trace import (
    ContractCandleProvider,
    observe_q2_public_candle_scenarios,
    observe_q2_public_candle_trace,
)

import pytest

from controllers.directional_trading.rsi_v5 import RSIv5Controller

REQUEST = Path(__file__).resolve().parents[3] / "docs/integration/q2-rsi-v5-public-candle-request-v1.json"


def _request() -> dict:
    return json.loads(REQUEST.read_text(encoding="utf-8"))


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


def test_request_driven_observer_returns_exact_root_raw_schema_without_mutating_input() -> None:
    request = _request()
    original = copy.deepcopy(request)

    first = observe_q2_public_candle_scenarios(request)
    second = observe_q2_public_candle_scenarios(request)

    assert first == second
    assert request == original
    assert [scenario["scenario_id"] for scenario in first] == [
        "healthy_two_update_rebound",
        "atr_veto_after_rebound",
        "strict_hmm_219_fail_close",
    ]
    for scenario in first:
        assert set(scenario) == {"scenario_id", "steps"}
        for step in scenario["steps"]:
            assert set(step) == {
                "step_id",
                "data_health",
                "indicators",
                "regime_consumption",
                "signal",
                "signal_state",
                "actions",
            }
            assert all(math.isfinite(value) for value in step["indicators"].values())


def test_request_driven_observer_consumes_controlled_regime_and_exact_branch_contract() -> None:
    scenarios = observe_q2_public_candle_scenarios(_request())
    healthy, atr_veto, strict = scenarios

    arm, rebound = healthy["steps"]
    assert arm["regime_consumption"] == _request()["scenarios"][0]["steps"][0]["controlled_regime"]
    assert arm["signal_state"] == {
        "raw_buy_reversal": True,
        "confirmation_active": True,
        "decision": "armed",
        "reason": "rebound-confirmation-armed",
        "condition_ok": True,
        "condition_reason": "ok",
    }
    assert arm["actions"] == []
    assert rebound["signal"] == 1
    assert rebound["signal_state"]["decision"] == "ready"
    assert rebound["signal_state"]["reason"] == "signal-generated"
    assert rebound["actions"] == [
        {
            "action_type": "CreateExecutorAction",
            "side": "BUY",
            "executor_type": "position_executor",
            "entry_price": "99.04",
            "amount": "0.1009",
        }
    ]

    _, veto = atr_veto["steps"]
    assert veto["signal"] == 0
    assert veto["signal_state"]["decision"] == "armed"
    assert veto["signal_state"]["reason"] == "rebound-confirmation-armed"
    assert veto["signal_state"]["condition_reason"] == "high_vol"
    assert veto["actions"] == []

    strict_step = strict["steps"][0]
    assert strict_step["data_health"]["row_count"] == 219
    assert strict_step["regime_consumption"] == {
        "required": True,
        "backend": "CONTROLLED_GAUSSIAN_HMM",
        "fit": False,
        "detected": False,
        "label": None,
        "error_code": "HMM_NOT_FIT",
    }
    assert strict_step["signal"] == 0
    assert strict_step["signal_state"] == {
        "raw_buy_reversal": False,
        "confirmation_active": False,
        "decision": "idle",
        "reason": "none",
        "condition_ok": False,
        "condition_reason": "strict_hmm",
    }
    assert strict_step["actions"] == []


def test_request_driven_observer_invokes_public_controller_methods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts = {"update": 0, "actions": 0}
    intervals = []
    original_update = RSIv5Controller.update_processed_data
    original_actions = RSIv5Controller.determine_executor_actions
    original_candles = ContractCandleProvider.get_candles_df

    async def observed_update(controller: RSIv5Controller) -> None:
        counts["update"] += 1
        await original_update(controller)

    def observed_actions(controller: RSIv5Controller):
        counts["actions"] += 1
        return original_actions(controller)

    def observed_candles(provider: ContractCandleProvider, **kwargs):
        intervals.append(kwargs["interval"])
        return original_candles(provider, **kwargs)

    monkeypatch.setattr(RSIv5Controller, "update_processed_data", observed_update)
    monkeypatch.setattr(RSIv5Controller, "determine_executor_actions", observed_actions)
    monkeypatch.setattr(ContractCandleProvider, "get_candles_df", observed_candles)

    observe_q2_public_candle_scenarios(_request())

    assert counts == {"update": 5, "actions": 5}
    assert intervals == ["1m"] * 5


def test_request_driven_observer_rejects_unvalidated_or_trend_enabled_request() -> None:
    invalid_hash = _request()
    invalid_hash["provider"]["best_bid_offset"] = "0.06"
    with pytest.raises(ValueError, match="request_hash"):
        observe_q2_public_candle_scenarios(invalid_hash)

    trend_enabled = _request()
    trend_enabled["profiles"][0]["config"]["trend_confirmation_enabled"] = True
    with pytest.raises(ValueError, match="trend_confirmation_enabled"):
        observe_q2_public_candle_scenarios(trend_enabled)
