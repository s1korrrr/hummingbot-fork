import ast
import hashlib
import inspect
import json
import subprocess
import sys
import unittest
from decimal import Decimal

from pydantic import BaseModel, TypeAdapter, ValidationError

from hummingbot.strategy_v2.components.events_v2.contracts import (
    CoverageScope,
    EventEvidenceV2,
    InstrumentScopeV2,
    QualifiedEventSnapshotV2,
    SnapshotBindingV2,
)
from hummingbot.strategy_v2.components.events_v2.validation import (
    validate_freshness,
    validate_generation,
    validate_preexisting_market,
    validate_snapshot_sequence,
    validate_strict_payload,
)

T = 1_780_000_000_000
H = 'a' * 64
C = 'b' * 64
D = 'c' * 64


def evidence(**changes):
    values = dict(
        source_id='okx_announcements', occurrence_id='notice-1', cluster_id='cluster-1',
        asset_id='eip155:1/erc20:0xabc', kind='SPOT_LISTING_ANNOUNCED', stage='INITIAL',
        original_sha256=H, content_sha256=C, source_host='www.okx.com',
        monotonic_clock_domain='collector-session-1', source_qualification='UNVERIFIED',
        instrument_pair='ABC-USDC', listed_ms=T - 5_000, published_ms=T - 1_000,
        first_public_ms=T - 1_000, received_ms=T, parsed_ms=T, processed_ms=T,
        label_available_ms=T, mapping_available_ms=T, mapping_receipt_sha256=D,
        revision=1, monotonic_ns=1, novel=True)
    values.update(changes)
    return EventEvidenceV2(**values)


def snapshot(events=None, **changes):
    events = tuple(events if events is not None else (evidence(),))
    values = dict(
        asof_ms=T, sequence=1, generation=1, dataset_sha256=D, events=events,
        completeness=(CoverageScope(source_id='okx_announcements', coverage_state='UNVERIFIED',
                                    asof_ms=T, reason='fixture_unverified'),),
        observation_universe=tuple(item.asset_id for item in events),
        entry_universe=(), management_universe=(),
        binding=SnapshotBindingV2(source_session='fixture', mapping_sha256=H))
    values.update(changes)
    return QualifiedEventSnapshotV2(**values)


class ContractTests(unittest.TestCase):
    def test_snapshot_schema_keeps_canonical_wire_and_instance_contract(self):
        item = snapshot()
        payload = item.model_dump(mode='json')
        encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False)
        self.assertEqual(item.schema, 'cross_venue_event.snapshot.v2')
        self.assertEqual(payload['schema'], item.schema)
        self.assertNotIn('schema_id', payload)
        self.assertEqual(item.model_dump()['schema'], item.schema)
        self.assertEqual(json.loads(item.model_dump_json())['schema'], item.schema)
        self.assertEqual(item.model_dump(by_alias=False)['schema'], item.schema)
        self.assertEqual(json.loads(item.model_dump_json(by_alias=False))['schema'], item.schema)
        self.assertEqual(TypeAdapter(QualifiedEventSnapshotV2).dump_python(item, mode='json'), payload)

        class Envelope(BaseModel):
            snapshot: QualifiedEventSnapshotV2

        self.assertEqual(Envelope(snapshot=item).model_dump(mode='json')['snapshot'], payload)
        self.assertEqual(QualifiedEventSnapshotV2.model_validate(payload).model_dump(mode='json'), payload)
        self.assertEqual(hashlib.sha256(encoded.encode()).hexdigest(),
                         'f1cdc069655a4e00fb2cfc407d201e0540264641a06ef2a3b6436875cd3ecc09')  # noqa: mock
        with self.assertRaises(ValidationError):
            snapshot(schema='cross_venue_event.snapshot.v3')

    def test_snapshot_contract_cold_import_is_warning_free(self):
        result = subprocess.run(
            [sys.executable, '-W', 'error', '-c',
             'from hummingbot.strategy_v2.components.events_v2.contracts import QualifiedEventSnapshotV2'],
            capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_late_receipt_does_not_make_market_preexisting(self):
        with self.assertRaisesRegex(ValueError, 'OKX_MARKET_NOT_PREEXISTING'):
            validate_preexisting_market(first_public_ms=1780000000000, listed_ms=1780000000500)

    def test_preexisting_market_requires_strict_utc_ms(self):
        with self.assertRaisesRegex(ValueError, 'UTC milliseconds'):
            validate_preexisting_market(first_public_ms=0, listed_ms=1)

    def test_event_and_snapshot_clocks_reject_coerced_numbers(self):
        for value in (float(T), str(T), True):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                evidence(received_ms=value)
            with self.subTest(value=value), self.assertRaises(ValidationError):
                evidence(listed_ms=value)
            with self.subTest(value=value), self.assertRaises(ValidationError):
                snapshot(asof_ms=value)

    def test_mapping_and_qualification_cannot_claim_missing_receipts(self):
        with self.assertRaisesRegex(ValidationError, 'mapping availability'):
            evidence(mapping_receipt_sha256=None)
        with self.assertRaisesRegex(ValidationError, 'source qualification'):
            evidence(source_qualification='COMPLETE')

    def test_parsing_time_is_part_of_causal_availability(self):
        item = evidence(parsed_ms=T + 2, processed_ms=T + 2)
        self.assertEqual(item.event_available_ms, T + 2)
        with self.assertRaises(ValidationError):
            snapshot(events=(item,), asof_ms=T)

    def test_instrument_scope_requires_finite_native_units(self):
        values = dict(venue='okx', pair='ABC-USDC', instrument_type='SPOT',
                      region='global', quote_asset='eip155:1/erc20:0xdef',
                      fee_asset='eip155:1/erc20:0xdef')
        InstrumentScopeV2(**values, lot_size_base=Decimal('0.01'))
        for lot in (Decimal('0'), Decimal('NaN'), Decimal('Infinity')):
            with self.subTest(lot=lot), self.assertRaises(ValidationError):
                InstrumentScopeV2(**values, lot_size_base=lot)

    def test_ticker_is_not_a_canonical_asset_id(self):
        with self.assertRaises(ValidationError):
            evidence(asset_id='BTC')

    def test_duplicate_json_fields_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate JSON key'):
            validate_strict_payload('{"a":1,"a":2}')

    def test_nonfinite_json_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'nonfinite'):
            validate_strict_payload('{"x":Infinity}')

    def test_source_clock_fault_when_publication_follows_receipt(self):
        with self.assertRaisesRegex(ValidationError, 'SOURCE_CLOCK_FAULT'):
            evidence(published_ms=T + 1)

    def test_knowledge_time_cannot_precede_receipt(self):
        with self.assertRaises(ValidationError):
            evidence(label_available_ms=T - 1)

    def test_revision_requires_predecessor_hash(self):
        with self.assertRaisesRegex(ValidationError, 'predecessor'):
            evidence(revision=2)

    def test_future_generation_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'FUTURE_GENERATION'):
            validate_generation(generation=3, admitted_generation=2)

    def test_freshness_rejects_unknown_future_labels(self):
        item = evidence()
        with self.assertRaisesRegex(ValueError, 'not yet known'):
            validate_freshness(item, asof_ms=item.event_available_ms - 1, max_age_ms=60_000)

    def test_snapshot_excludes_future_knowledge(self):
        with self.assertRaisesRegex(ValidationError, 'not available'):
            snapshot(events=(evidence(label_available_ms=T + 5),), asof_ms=T)

    def test_snapshot_sequence_requires_predecessor(self):
        first = snapshot()
        second = snapshot(sequence=2, predecessor_sha256=first.dataset_sha256, asof_ms=T + 1)
        validate_snapshot_sequence(first, second)
        with self.assertRaisesRegex(ValueError, 'predecessor'):
            validate_snapshot_sequence(first, snapshot(sequence=2, predecessor_sha256='d' * 64, asof_ms=T + 1))

    def test_unverified_coverage_cannot_claim_completeness(self):
        with self.assertRaises(ValidationError):
            CoverageScope(source_id='okx_announcements', coverage_state='UNVERIFIED', asof_ms=T,
                          complete_for_entry=True, reason='bad')

    def test_import_starts_no_services_and_loads_no_controller_families(self):
        import hummingbot.strategy_v2.components.events_v2 as package
        import hummingbot.strategy_v2.components.events_v2.contracts as contracts
        import hummingbot.strategy_v2.components.events_v2.validation as validation
        for module in (package, contracts, validation):
            tree = ast.parse(inspect.getsource(module))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module]
                self.assertFalse(any(name.startswith('controllers') for name in names))

    def test_negative_monotonic_clock_is_rejected(self):
        with self.assertRaises(ValidationError):
            evidence(monotonic_ns=-1)
