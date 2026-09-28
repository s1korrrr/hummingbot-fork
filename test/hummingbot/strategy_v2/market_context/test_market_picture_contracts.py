import json
from pathlib import Path

import pytest

from hummingbot.strategy_v2.market_context.market_picture.contracts import (
    MarketEventV1,
    MetricSeriesV1,
    parse_metric_series,
    resolve_metric_series,
    validate_frame,
)

FIXTURE = Path(__file__).parents[3] / "fixtures" / "market_picture_metric_series.v1.json"


def test_compact_metric_series_round_trips_every_semantic_field():
    raw = FIXTURE.read_bytes()
    series = parse_metric_series(raw)
    assert series.metric_id == "return_pct_h1"
    assert [cell.instrument_id for cell in series.cells] == ["okx:spot:BTC-USDC", "okx:spot:ETH-USDC"]
    expanded = resolve_metric_series(series)
    assert expanded["okx:spot:BTC-USDC"]["value"] == "1.25"
    assert expanded["okx:spot:BTC-USDC"]["unit"] == "return_percent"
    assert expanded["okx:spot:BTC-USDC"]["definition"]["definition_id"] == "return_pct"
    assert expanded["okx:spot:BTC-USDC"]["source_ref"]["sequence"] == 7
    assert expanded["okx:spot:BTC-USDC"]["sample_count"] == 2
    assert expanded["okx:spot:BTC-USDC"]["required_samples"] == 2
    assert expanded["okx:spot:BTC-USDC"]["expected_denominator"] == 2
    assert expanded["okx:spot:ETH-USDC"]["value"] is None
    assert expanded["okx:spot:ETH-USDC"]["reason_codes"] == ["INSUFFICIENT_HISTORY"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row.update(encoding="metric-series.v2"),
        lambda row: row["cells"][0].update(value="NaN"),
        lambda row: row["cells"][0].update(instrument_id="BTC-USDC"),
        lambda row: row["cells"][0].update(reason_codes=["UNKNOWN_REASON"]),
        lambda row: row["cells"][0].update(sample_count=-1),
        lambda row: row["cells"].reverse(),
    ],
)
def test_rejects_malformed_metric_series(mutate):
    data = json.loads(FIXTURE.read_text())
    mutate(data)
    with pytest.raises(ValueError):
        MetricSeriesV1.model_validate(data)


def test_parse_rejects_duplicate_keys_and_digest_mismatch():
    with pytest.raises(ValueError):
        parse_metric_series(b'{"encoding":"metric-series.v1","encoding":"metric-series.v1"}')
    data = json.loads(FIXTURE.read_text())
    data["cells"][0]["value"] = "2"
    with pytest.raises(ValueError):
        parse_metric_series(json.dumps(data).encode())


def test_valid_zero_is_distinct_from_missing():
    data = json.loads(FIXTURE.read_text())
    data["cells"][0]["value"] = "0"
    # Re-seal the fixture with the owner canonical serializer.
    from hummingbot.strategy_v2.market_context.market_picture.contracts import metric_series_digest

    data["payload_digest"] = metric_series_digest(data)
    series = MetricSeriesV1.model_validate_json(json.dumps(data))
    assert resolve_metric_series(series)["okx:spot:BTC-USDC"]["value"] == "0"


def test_validate_frame_accepts_json_array_values_from_the_wire():
    series = json.loads(FIXTURE.read_text())
    frame = {"encoding": "metric-series.v1", "metric_series": [series]}
    validate_frame(frame)


def test_compact_model_dump_round_trips_null_values_and_inherited_defaults():
    series = parse_metric_series(FIXTURE.read_bytes())
    compact = series.model_dump(mode="json", exclude_none=True, exclude_defaults=True)
    reparsed = MetricSeriesV1.model_validate_json(json.dumps(compact))
    assert reparsed.payload_digest == series.payload_digest
    expanded = resolve_metric_series(reparsed)
    assert expanded["okx:spot:ETH-USDC"]["value"] is None
    assert expanded["okx:spot:ETH-USDC"]["available_at_ms"] == series.available_at_ms


def test_explicit_default_fixture_marker_is_canonicalized_before_digest_check():
    data = json.loads(FIXTURE.read_text())
    data["fixture_only"] = False
    from hummingbot.strategy_v2.market_context.market_picture.contracts import metric_series_digest

    data["payload_digest"] = metric_series_digest(data)
    series = MetricSeriesV1.model_validate_json(json.dumps(data))
    assert not series.fixture_only


def test_series_validation_enforces_units_even_before_expansion():
    data = json.loads(FIXTURE.read_text())
    data["definition_ref"]["unit"] = "share_fraction"
    data["series_unit"] = "share_fraction"
    data["cells"][0]["value"] = "1.01"
    from hummingbot.strategy_v2.market_context.market_picture.contracts import metric_series_digest

    data["payload_digest"] = metric_series_digest(data)
    with pytest.raises(ValueError):
        MetricSeriesV1.model_validate_json(json.dumps(data))


def _event_payload(**updates):
    digest = "a" * 64
    observed = 1_780_000_000_000
    payload = {
        "event_id": digest,
        "occurrence_key": "b" * 64,
        "event_type": "new_24h_high",
        "severity": "notice",
        "instrument_id": "okx:spot:BTC-USDC",
        "definition_ref": {
            "definition_id": "high_break_24h",
            "definition_version": "1.0.0",
            "parameter_hash": digest,
            "code_hash": digest,
            "unit": "count",
        },
        "provider_ref": {
            "provider_id": "candles",
            "stream_id": "okx_spot",
            "epoch": "epoch-1",
            "sequence": 3,
            "source_snapshot_id": digest,
            "source_digest": digest,
            "source_kind": "observed",
            "observed_at_ms": observed,
            "available_at_ms": observed + 1,
            "expires_at_ms": observed + 60_001,
            "coverage": {"expected_count": 1, "valid_count": 1, "coverage_fraction": "1"},
            "capabilities": ["confirmed_1m"],
        },
        "source_window_start_ms": observed - 60_000,
        "source_window_end_ms": observed,
        "observed_at_ms": observed,
        "available_at_ms": observed + 1,
        "persisted_at_ms": observed + 2,
        "before": {"prior_high": "10"},
        "value": {"new_high": "11"},
        "status": "original",
        "supersedes_event_id": None,
        "snapshot_id": digest,
        "source_epoch": "epoch-1",
        "source_sequence": 3,
        "is_historical_reconstruction": False,
    }
    payload.update(updates)
    return payload


def test_event_contract_preserves_source_lineage_and_revision_rules():
    event = MarketEventV1.model_validate_json(json.dumps(_event_payload()))
    assert event.event_type == "new_24h_high" and event.source_sequence == 3
    with pytest.raises(ValueError):
        MarketEventV1.model_validate_json(json.dumps(_event_payload(source_sequence=4)))
    with pytest.raises(ValueError):
        MarketEventV1.model_validate_json(json.dumps(_event_payload(status="corrected")))
    corrected = MarketEventV1.model_validate_json(
        json.dumps(_event_payload(status="corrected", supersedes_event_id="c" * 64))
    )
    assert corrected.supersedes_event_id == "c" * 64
