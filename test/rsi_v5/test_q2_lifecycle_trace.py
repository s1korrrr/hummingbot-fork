import json
from pathlib import Path

from controllers.directional_trading.rsi_v5_core.q2_trace import capture_q2_lifecycle_trace

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/rsi_v5/q2_lifecycle_trace.json"


def test_q2_lifecycle_fixture_is_exact_actual_controller_output() -> None:
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    actual = capture_q2_lifecycle_trace()

    assert actual == expected
    assert actual["schema_version"] == "rsi-v5-q2-lifecycle-trace/1.0"
    assert actual["authority"] == {
        "paper_shadow_only": True,
        "live_orders_allowed": False,
        "execution_authority": False,
    }
    assert [event["action"] for event in actual["events"]] == [
        "entry",
        "dca",
        "cooldown",
        "partial_harvest",
        "runner",
        "trailing",
        "exit",
    ]
    assert len(actual["trace_hash"]) == 64
