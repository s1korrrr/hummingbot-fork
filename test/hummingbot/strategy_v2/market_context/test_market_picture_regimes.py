import json

import pytest

from hummingbot.strategy_v2.market_context.market_picture.regimes import RegimeObservationV1, validate_regime

H = "a" * 64


def regime(**updates):
    payload = {
        "instrument_id": "okx:spot:BTC-USDC",
        "source_bar_close_ms": 120_000,
        "origin": "controller_recorded",
        "producer_id": "rsi_modular",
        "controller_id": "controller-1",
        "regime_label": "trend_up",
        "regime_definition_id": "controller-regime.v1",
        "confidence_value": "0.92",
        "confidence_unit": "fraction",
        "confidence_kind": "reported_unknown",
        "calibration_status": "unknown",
        "model_id": None,
        "model_version": None,
        "fit_id": None,
        "feature_schema_hash": None,
        "trend_direction": "down",
        "trend_confidence_value": None,
        "trend_confidence_unit": None,
        "context_bias": "buy_context_bias",
        "provider_ref": {
            "provider_id": "recorded_controller",
            "stream_id": "screener",
            "epoch": "epoch-1",
            "sequence": 1,
            "source_snapshot_id": H,
            "source_digest": H,
            "source_kind": "observed",
            "observed_at_ms": 120_000,
            "available_at_ms": 120_001,
            "expires_at_ms": 180_000,
            "coverage": {"expected_count": 1, "valid_count": 1, "coverage_fraction": "1"},
            "capabilities": [],
        },
        "available_at_ms": 120_001,
        "expires_at_ms": 180_000,
        "source_epoch": "epoch-1",
        "source_sequence": 1,
    }
    payload.update(updates)
    return RegimeObservationV1.model_validate_json(json.dumps(payload))


def test_preserves_recorded_confidence_unknown_calibration_and_independent_trend():
    row = regime()
    assert row.confidence_value == "0.92" and row.calibration_status == "unknown"
    assert row.trend_direction == "down" and row.context_bias == "buy_context_bias"
    assert (
        validate_regime(
            row, expected_instrument_id=row.instrument_id, expected_source_bar_close_ms=row.source_bar_close_ms
        )
        is row
    )


def test_rejects_confidence_unit_rescaling_and_identity_leakage():
    percent_report = regime(confidence_unit="percent")
    assert percent_report.confidence_value == "0.92"  # retain the provider value; do not silently multiply by 100
    with pytest.raises(ValueError):
        validate_regime(regime(), expected_instrument_id="okx:spot:ETH-USDC", expected_source_bar_close_ms=120_000)
    with pytest.raises(ValueError):
        validate_regime(regime(), expected_instrument_id="okx:spot:BTC-USDC", expected_source_bar_close_ms=180_000)


def test_market_observation_cannot_claim_controller_and_missing_confidence_stays_null():
    row = regime(
        origin="market_observation",
        controller_id=None,
        confidence_value=None,
        confidence_unit=None,
        confidence_kind=None,
        calibration_status="not_applicable",
    )
    assert row.confidence_value is None and row.controller_id is None
    with pytest.raises(ValueError):
        regime(origin="market_observation")
