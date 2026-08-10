from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from controllers.directional_trading.rsi_v5_core.canonical import canonical_decimal_string, canonical_json_bytes
from controllers.directional_trading.rsi_v5_core.domain import RSIEntryIntent

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "schemas/rsi-v5/entry-intent-v1.canonical-fixtures.json"


def fixture_contract() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_decimal_vectors_match_normative_contract() -> None:
    for vector in fixture_contract()["decimal_vectors"]:
        outputs = {canonical_decimal_string(Decimal(raw)) for raw in vector["inputs"]}
        assert outputs == {vector["canonical_decimal"]}


def test_nonfinite_decimals_are_rejected() -> None:
    for raw in fixture_contract()["invalid_decimal_vectors"]:
        with pytest.raises(ValueError, match="non-finite"):
            canonical_decimal_string(Decimal(raw))


def test_entry_intent_producer_matches_golden_bytes_and_identity() -> None:
    vector = fixture_contract()["entry_intent_equivalence"]
    for decimals in vector["equivalent_decimal_inputs"]:
        intent = RSIEntryIntent.build(**vector["base_unsigned_payload"], **decimals)
        payload_bytes = canonical_json_bytes(intent.canonical_payload())
        assert intent.intent_id == vector["intent_id"]
        assert payload_bytes.hex() == vector["canonical_payload_utf8_hex"]
        assert hashlib.sha256(payload_bytes).hexdigest() == vector["canonical_payload_sha256"]


def test_legacy_schema_1_0_decimal_bytes_remain_unchanged() -> None:
    payload = dict(fixture_contract()["entry_intent_equivalence"]["base_unsigned_payload"])
    payload.update(
        schema_version="1.0",
        planned_notional="100.00",
        planned_price="100.00",
        amount_base="1.00",
    )
    first = RSIEntryIntent.build(**payload)
    assert first.canonical_payload()["planned_notional"] == "100.00"
