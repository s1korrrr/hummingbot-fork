from __future__ import annotations

import json
from pathlib import Path
from test.rsi_v5.q2_exit_lifecycle_trace import observe_q2_exit_lifecycle_scenarios

import pytest


def _request() -> dict:
    return {
        "profile": {
            "connector": "hyperliquid_perpetual",
            "trading_pair": "BNB-USDC",
            "entry_price": "100",
            "position_amount": "1",
            "trailing_activation": "0.047",
            "trailing_delta": "0.034",
            "trailing_order_type": "LIMIT",
            "hard_stop_fraction": "0.08",
            "early_stop_keep_position": False,
            "recovery_partial_fraction": "0.5",
            "signal_sell_inventory_slice_multiplier": "0",
            "usd_per_entry": "53",
            "sell_price": "106",
        },
        "scenarios": [
            {
                "scenario_id": "trailing_full_close",
                "steps": [
                    {"step_id": "activation_equal", "input": {"net_pnl_pct": "0.047"}},
                    {"step_id": "activation_cross", "input": {"net_pnl_pct": "0.047001"}},
                    {"step_id": "new_peak", "input": {"net_pnl_pct": "0.08"}},
                    {"step_id": "trigger_equal", "input": {"net_pnl_pct": "0.046"}},
                    {"step_id": "trigger_cross", "input": {"net_pnl_pct": "0.045999"}},
                ],
            },
            {
                "scenario_id": "hard_stop_no_preserve",
                "steps": [
                    {"step_id": "above_stop", "input": {"market_price": "92.00000001"}},
                    {"step_id": "at_stop", "input": {"market_price": "92"}},
                ],
            },
            {
                "scenario_id": "signal_recovery_full",
                "steps": [
                    {"step_id": "sell_signal", "input": {"signal": -1, "held_base": "1", "available_base": "1"}}
                ],
            },
            {
                "scenario_id": "signal_recovery_capped",
                "steps": [
                    {
                        "step_id": "sell_signal",
                        "input": {"signal": -1, "held_base": "1", "available_base": "0.75"},
                    }
                ],
            },
            {
                "scenario_id": "recovery_partial",
                "steps": [
                    {"step_id": "pullback", "input": {"signal": 0, "held_base": "1", "available_base": "1"}}
                ],
            },
            {
                "scenario_id": "signal_quote_slice",
                "profile_overrides": {"signal_sell_inventory_slice_multiplier": "1"},
                "steps": [
                    {"step_id": "sell_signal", "input": {"signal": -1, "held_base": "1", "available_base": "1"}}
                ],
            },
        ],
    }


def _by_id(scenarios: list[dict]) -> dict[str, dict]:
    return {scenario["scenario_id"]: scenario for scenario in scenarios}


def test_q2_exit_lifecycle_producer_consumes_mapping_or_path_and_is_deterministic(tmp_path: Path) -> None:
    request = _request()
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")

    from_mapping = observe_q2_exit_lifecycle_scenarios(request)
    from_path = observe_q2_exit_lifecycle_scenarios(request_path)

    assert from_mapping == from_path
    assert [scenario["scenario_id"] for scenario in from_mapping] == [
        "trailing_full_close",
        "hard_stop_no_preserve",
        "signal_recovery_full",
        "signal_recovery_capped",
        "recovery_partial",
        "signal_quote_slice",
    ]
    assert all(set(scenario) == {"scenario_id", "steps"} for scenario in from_mapping)
    assert all(
        set(step) == {"step_id", "decision", "actions", "post_state"}
        for scenario in from_mapping
        for step in scenario["steps"]
    )
    assert all(
        set(action) == {"kind", "side", "amount", "order_type", "keep_position"}
        for scenario in from_mapping
        for step in scenario["steps"]
        for action in step["actions"]
    )


def test_q2_exit_lifecycle_producer_observes_exact_runtime_boundaries() -> None:
    scenarios = _by_id(observe_q2_exit_lifecycle_scenarios(_request()))

    trailing = scenarios["trailing_full_close"]["steps"]
    assert [(step["decision"]["owner"], step["decision"]["reason"]) for step in trailing] == [
        ("POSITION_EXECUTOR", "activation_not_exceeded"),
        ("POSITION_EXECUTOR", "trailing_armed"),
        ("POSITION_EXECUTOR", "new_peak"),
        ("POSITION_EXECUTOR", "trigger_not_crossed"),
        ("POSITION_EXECUTOR", "trailing_close"),
    ]
    assert [step["post_state"]["trigger_net_pnl_pct"] for step in trailing] == [
        None,
        "0.013001",
        "0.046",
        "0.046",
        "0.046",
    ]
    assert trailing[-1]["actions"] == [
        {
            "kind": "CLOSE_POSITION",
            "side": "SELL",
            "amount": "1",
            "order_type": "LIMIT",
            "keep_position": None,
        }
    ]
    assert all(step["post_state"]["remaining_amount"] == "1" for step in trailing)

    hard_stop = scenarios["hard_stop_no_preserve"]["steps"]
    assert hard_stop[0]["decision"] == {"owner": "RSI_V5_CONTROLLER", "reason": "threshold_not_reached"}
    assert hard_stop[0]["actions"] == []
    assert hard_stop[1]["decision"] == {"owner": "RSI_V5_CONTROLLER", "reason": "hard_stop"}
    assert hard_stop[1]["actions"] == [
        {
            "kind": "STOP_EXECUTOR",
            "side": None,
            "amount": None,
            "order_type": None,
            "keep_position": False,
        }
    ]


def test_q2_exit_lifecycle_producer_preserves_recovery_ownership_and_sizing() -> None:
    scenarios = _by_id(observe_q2_exit_lifecycle_scenarios(_request()))

    expected = {
        "signal_recovery_full": ("signal_full_exit", "1", "LIMIT_CHASER"),
        "signal_recovery_capped": ("signal_capped_exit", "0.75", "LIMIT_CHASER"),
        "recovery_partial": ("recovery_partial_exit", "0.5", "LIMIT_CHASER"),
        "signal_quote_slice": ("signal_quote_slice", "0.5", "LIMIT_CHASER"),
    }
    for scenario_id, (reason, amount, order_type) in expected.items():
        step = scenarios[scenario_id]["steps"][0]
        assert step["decision"] == {"owner": "HELD_BAG_RECOVERY", "reason": reason}
        assert step["actions"] == [
            {
                "kind": "CREATE_RECOVERY_EXIT",
                "side": "SELL",
                "amount": amount,
                "order_type": order_type,
                "keep_position": None,
            }
        ]
        assert step["post_state"]["remaining_amount"] == "1"


def test_q2_exit_lifecycle_producer_rejects_wrong_scenario_topology() -> None:
    request = _request()
    request["scenarios"].pop()

    with pytest.raises(ValueError, match="ordered scenarios"):
        observe_q2_exit_lifecycle_scenarios(request)
