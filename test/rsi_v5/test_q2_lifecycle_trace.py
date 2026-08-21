from __future__ import annotations

import copy
import json
from pathlib import Path
from test.rsi_v5.q2_stateful_trace import observe_q2_scenarios
from types import SimpleNamespace

import pytest

from controllers.directional_trading.rsi_v5 import RSIv5Controller

REQUEST = Path(__file__).resolve().parents[2].parent / "docs/integration/q2-rsi-v5-parity-scenario-request-v2.json"
STEP_IDS = {
    "entry": ["derive_signal", "executor_config"],
    "dca": ["admission", "gate", "executor_config"],
    "cooldown": ["base_before", "base_at", "adaptive_before", "adaptive_at"],
    "partial_harvest": ["arm_below", "arm_at", "new_peak", "pullback_before", "pullback_at"],
    "runner": ["confirm", "admission", "gate", "executor_config"],
    "trailing": ["executor_config"],
    "exit": ["hard_stop_before", "hard_stop_at", "signal", "owned_exit"],
}
ADMISSIONS = {
    "dca": {"allowed": True, "rejection_code": None},
    "runner": {"allowed": True, "rejection_code": None},
}


def _by_action(observation: list[dict]) -> dict[str, dict]:
    return {scenario["action"]: scenario for scenario in observation}


def _step(scenario: dict, step_id: str) -> dict:
    return next(step for step in scenario["steps"] if step["step_id"] == step_id)


def test_q2_producer_consumes_path_or_dict_and_emits_exact_stateful_topology() -> None:
    request_dict = json.loads(REQUEST.read_text(encoding="utf-8"))

    from_path = observe_q2_scenarios(REQUEST, admission_outcomes=ADMISSIONS)
    from_dict = observe_q2_scenarios(request_dict, admission_outcomes=ADMISSIONS)

    assert from_path == from_dict
    assert [(item["scenario_id"], item["action"]) for item in from_path] == [
        (f"rsi_v5_{action}_v2", action) for action in STEP_IDS
    ]
    assert {
        scenario["action"]: [step["step_id"] for step in scenario["steps"]]
        for scenario in from_path
    } == STEP_IDS
    assert all(
        set(step) == {"step_id", "decision", "actions", "post_state"}
        for scenario in from_path
        for step in scenario["steps"]
    )


def test_q2_decimal_normalization_preserves_integer_trailing_zeroes() -> None:
    scenarios = _by_action(observe_q2_scenarios(REQUEST, admission_outcomes=ADMISSIONS))

    assert _step(scenarios["entry"], "derive_signal")["post_state"]["mid_price"] == "100"
    assert _step(scenarios["dca"], "gate")["post_state"]["inventory"][0]["breakeven_price"] == "100"


def test_q2_producer_invokes_actual_controller_methods(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"signal": 0, "gate": 0, "config": 0, "stop": 0}

    for method_name, key in (
        ("_determine_signal", "signal"),
        ("can_create_executor", "gate"),
        ("get_executor_config", "config"),
        ("stop_actions_proposal", "stop"),
    ):
        original = getattr(RSIv5Controller, method_name)

        def tracking(self, *args, _original=original, _key=key, **kwargs):
            called[_key] += 1
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(RSIv5Controller, method_name, tracking)

    observe_q2_scenarios(REQUEST, admission_outcomes=ADMISSIONS)

    assert called == {"signal": 3, "gate": 6, "config": 4, "stop": 8}


def test_q2_entry_and_runner_derive_roles_from_confirmation_state() -> None:
    scenarios = _by_action(observe_q2_scenarios(REQUEST, admission_outcomes=ADMISSIONS))

    entry_signal = _step(scenarios["entry"], "derive_signal")
    entry_config = _step(scenarios["entry"], "executor_config")
    runner_confirm = _step(scenarios["runner"], "confirm")
    runner_gate = _step(scenarios["runner"], "gate")
    runner_config = _step(scenarios["runner"], "executor_config")

    assert entry_signal["decision"] == {"kind": "SIGNAL", "signal": 1, "role": "scout"}
    assert entry_signal["post_state"]["pending_buy_confirmation"]["trough_rsi"] == "29.2"
    assert entry_config["actions"][0]["level_id"] == "scout"
    assert entry_config["actions"][0]["amount_base"] == "0.0875"
    assert runner_confirm["decision"] == {"kind": "SIGNAL", "signal": 1, "role": "runner"}
    assert runner_confirm["post_state"]["pending_buy_confirmation"] is None
    assert runner_gate["decision"] == {"kind": "CONTROLLER_GATE", "allowed": True, "reason": "ready"}
    assert runner_config["actions"][0]["level_id"] == "runner"
    assert runner_config["actions"][0]["entry_price"] == "101.49"
    assert runner_config["actions"][0]["amount_base"] == "0.15931373"


def test_q2_external_admission_is_recorded_and_missing_admission_fails_closed() -> None:
    scenarios = _by_action(observe_q2_scenarios(REQUEST, admission_outcomes=ADMISSIONS))

    for action in ("dca", "runner"):
        admission = _step(scenarios[action], "admission")
        assert admission["decision"] == {
            "kind": "EXTERNAL_ADMISSION",
            "allowed": True,
            "rejection_code": None,
            "enforced_by": "root",
        }

    with pytest.raises(ValueError, match="missing evaluated admission outcome for dca"):
        observe_q2_scenarios(REQUEST, admission_outcomes={"runner": ADMISSIONS["runner"]})


def test_q2_attr_admission_rejection_blocks_dca_gate_and_config() -> None:
    rejected = {
        "dca": SimpleNamespace(allowed=False, rejection_code="RISK_HOLD"),
        "runner": SimpleNamespace(allowed=True, rejection_code=None),
    }

    scenarios = _by_action(observe_q2_scenarios(REQUEST, admission_outcomes=rejected))
    dca = scenarios["dca"]

    assert _step(dca, "admission")["decision"]["rejection_code"] == "RISK_HOLD"
    assert _step(dca, "gate")["decision"] == {
        "kind": "EXTERNAL_ADMISSION_BLOCKED",
        "allowed": False,
        "rejection_code": "RISK_HOLD",
    }
    assert _step(dca, "executor_config")["actions"] == []


def test_q2_dca_and_cooldown_cover_score_and_exact_time_boundaries_without_gap_patch() -> None:
    request = json.loads(REQUEST.read_text(encoding="utf-8"))
    scenarios = _by_action(observe_q2_scenarios(request, admission_outcomes=ADMISSIONS))

    dca_gate = _step(scenarios["dca"], "gate")
    dca_config = _step(scenarios["dca"], "executor_config")
    cooldown = scenarios["cooldown"]

    assert dca_gate["decision"] == {"kind": "CONTROLLER_GATE", "allowed": True, "reason": "ready"}
    assert dca_gate["post_state"]["effective_price_gap_fraction"] == "0.01"
    assert dca_config["actions"][0]["requested_fraction"] == "0.25"
    assert [_step(cooldown, step)["decision"]["allowed"] for step in STEP_IDS["cooldown"]] == [False, True, False, True]
    assert [_step(cooldown, step)["post_state"]["current_cooldown_seconds"] for step in STEP_IDS["cooldown"]] == [90, 90, 45, 45]
    assert _step(cooldown, "adaptive_before")["post_state"]["effective_price_gap_fraction"] == "0.0135"
    assert _step(cooldown, "adaptive_at")["post_state"]["effective_price_gap_fraction"] == "0.0135"

    low_score = copy.deepcopy(request)
    dca = next(item for item in low_score["scenarios"] if item["action"] == "dca")
    dca["initial_state"]["processed_signal_state"]["signal_score"] = 2
    dca["steps"][1]["inputs"]["event"]["signal_score"] = 2
    low = _by_action(observe_q2_scenarios(low_score, admission_outcomes=ADMISSIONS))
    assert _step(low["dca"], "gate")["decision"] == {
        "kind": "CONTROLLER_GATE",
        "allowed": False,
        "reason": "dca_score_low",
    }


def test_q2_partial_harvest_is_fee_aware_stateful_and_does_not_invent_fill() -> None:
    scenarios = _by_action(observe_q2_scenarios(REQUEST, admission_outcomes=ADMISSIONS))
    harvest = scenarios["partial_harvest"]

    assert _step(harvest, "arm_below")["post_state"]["recovery_trails"] == []
    assert _step(harvest, "arm_at")["post_state"]["recovery_trails"][0]["last_reason"] == "armed"
    assert _step(harvest, "new_peak")["post_state"]["recovery_trails"][0]["peak_price"] == "102"
    assert _step(harvest, "pullback_before")["actions"] == []
    at = _step(harvest, "pullback_at")
    assert at["actions"][0]["amount_base"] == "0.5"
    assert at["actions"][0]["execution_strategy"] == "LIMIT_CHASER"
    assert at["post_state"]["inventory"][0]["amount_base"] == "1"


def test_q2_trailing_is_config_only_and_exit_stops_without_fictional_fill() -> None:
    scenarios = _by_action(observe_q2_scenarios(REQUEST, admission_outcomes=ADMISSIONS))
    trailing = _step(scenarios["trailing"], "executor_config")
    exit_scenario = scenarios["exit"]

    assert trailing["decision"] == {"kind": "TRAILING_CONFIG_ONLY"}
    assert trailing["actions"][0]["trailing_stop"] == {
        "activation_fraction": "0.047",
        "distance_fraction": "0.034",
        "order_type": "LIMIT",
        "limit_max_age_seconds": 3,
    }
    assert _step(exit_scenario, "hard_stop_before")["actions"] == []
    at = _step(exit_scenario, "hard_stop_at")
    assert at["actions"] == [
        {
            "action_type": "STOP_EXECUTOR",
            "executor_id": "exit-existing-buy",
            "keep_position": True,
        }
    ]
    assert at["post_state"]["inventory"][0]["amount_base"] == "1"
    assert _step(exit_scenario, "signal")["decision"] == {"kind": "SIGNAL", "signal": -1, "role": None}
    assert _step(exit_scenario, "owned_exit")["decision"] == {
        "kind": "EXIT_OWNERSHIP",
        "owner": "active_signal_sell_executor",
        "recovery_action_suppressed": True,
    }
