"""T1 contract tests for the neutral shared market-context package (observation only)."""
import ast
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from hummingbot.strategy_v2.market_context import contracts as c

PACKAGE = Path(c.__file__).parent
SHA = 'a' * 64


def feature(value=0.25, status='VALID', reasons=(), unit='share', horizon=5):
    return c.NamedFeature(name='breadth_positive_eq_h5', value=value, unit=unit, horizon_minutes=horizon,
                          status=status, reasons=reasons, valid_count=5, expected_count=5,
                          valid_weight_fraction=1.0, model_id=None, input_available_at_ms=1_800_000_001_000)


def content(**changes):
    values = dict(
        venue='okx', numeraire='USDC', cutoff_ms=1_800_000_000_000 - 1_800_000_000_000 % 60_000,
        max_input_available_at_ms=1_799_999_990_000,
        provenance=c.Provenance(code_digest=SHA, config_digest=SHA, universe_hash=SHA, input_manifest_digest=SHA,
                                model_digest=SHA, serializer_version=c.SERIALIZER_VERSION, artifact_refs=()),
        coverage=c.Coverage(expected_count=5, valid_count=5, valid_weight_fraction=1.0, missing=()),
        status='READY', reasons=(), market=(feature(),), assets=())
    values.update(changes)
    return c.SnapshotContent(**values)


def sealed(source_kind='observed', **changes):
    body = content(**changes)
    return c.seal_snapshot(body, stream_id='okx-usdc-basket5', epoch='e1', sequence=1, source_kind=source_kind,
                           available_at_ms=body.cutoff_ms + 3_000, expires_at_ms=body.cutoff_ms + 65_000,
                           supersedes=None)


def requirement(**changes):
    values = dict(consumer='execution', schema_major=1, stream_id='okx-usdc-basket5', venue='okx', numeraire='USDC',
                  config_digest=SHA, universe_hash=SHA, max_age_ms=120_000, min_count_coverage='0.95',
                  min_weight_coverage='0.95', mandatory_features=('breadth_positive_eq_h5',),
                  allowed_source_kinds=('observed',))
    values.update(changes)
    return c.ContextRequirement(**values)


def test_valid_snapshot_round_trip_is_deterministic():
    snapshot = sealed()
    raw = c.serialize_snapshot(snapshot)
    parsed = c.parse_snapshot(raw)
    assert parsed == snapshot and parsed.snapshot_id == snapshot.snapshot_id
    assert c.serialize_snapshot(parsed) == raw
    assert snapshot.snapshot_id == hashlib.sha256(c.canonical_bytes(snapshot)).hexdigest()
    assert b'snapshot_id' not in c.canonical_bytes(snapshot)
    selection = c.validate_context(snapshot, requirement(), c.ClockView(decision_ms=snapshot.available_at_ms,
                                                                        receipt_ms=None))
    assert selection.accepted and selection.snapshot == snapshot and selection.reasons == ()


def test_future_or_expired_snapshot_rejected():
    snapshot = sealed()
    early = c.validate_context(snapshot, requirement(), c.ClockView(decision_ms=snapshot.available_at_ms - 1,
                                                                    receipt_ms=None))
    assert not early.accepted and early.snapshot is None and early.reasons == ('CONTEXT_FUTURE',)
    expired = c.validate_context(snapshot, requirement(), c.ClockView(decision_ms=snapshot.expires_at_ms,
                                                                      receipt_ms=None))
    assert not expired.accepted and expired.reasons == ('CONTEXT_EXPIRED',)
    too_old = c.validate_context(snapshot, requirement(max_age_ms=1_000),
                                 c.ClockView(decision_ms=snapshot.cutoff_ms + 3_500, receipt_ms=None))
    assert too_old.reasons == ('CONTEXT_EXPIRED',)
    # A consumer that received the payload after producer availability cannot use it earlier.
    received_late = c.validate_context(snapshot, requirement(),
                                       c.ClockView(decision_ms=snapshot.available_at_ms + 10,
                                                   receipt_ms=snapshot.available_at_ms + 20))
    assert received_late.reasons == ('CONTEXT_FUTURE',)
    with pytest.raises(ValueError):
        c.seal_snapshot(content(), stream_id='s', epoch='e', sequence=1, source_kind='observed',
                        available_at_ms=content().cutoff_ms + 10, expires_at_ms=content().cutoff_ms + 10,
                        supersedes=None)
    with pytest.raises(ValueError):  # availability cannot precede included inputs
        c.seal_snapshot(content(), stream_id='s', epoch='e', sequence=1, source_kind='observed',
                        available_at_ms=content().max_input_available_at_ms - 1,
                        expires_at_ms=content().cutoff_ms + 65_000, supersedes=None)


def test_nonfinite_and_duplicate_keys_rejected():
    snapshot = sealed()
    raw = c.serialize_snapshot(snapshot).decode()
    duplicate = raw.replace('"venue":"okx"', '"venue":"okx","venue":"okx"', 1)
    with pytest.raises(c.ContextError) as error:
        c.parse_snapshot(duplicate.encode())
    assert error.value.reasons == ('PAYLOAD_INVALID',)
    for bad in ('NaN', 'Infinity', '-Infinity'):
        tampered = raw.replace('"value":0.25', f'"value":{bad}', 1)
        assert tampered != raw
        with pytest.raises(c.ContextError):
            c.parse_snapshot(tampered.encode())
    numeric_string = raw.replace('"value":0.25', '"value":"0.25"', 1)
    with pytest.raises(c.ContextError):
        c.parse_snapshot(numeric_string.encode())
    with pytest.raises(ValueError):
        feature(value=float('nan'))
    with pytest.raises(ValueError):  # a valid feature needs a value; a missing value is not zero
        feature(value=None)
    with pytest.raises(ValueError):  # an invalid feature needs a reason and no value
        feature(value=0.0, status='INPUT_MISSING', reasons=('INPUT_MISSING',))
    with pytest.raises(ValueError):
        feature(value=None, status='INPUT_MISSING', reasons=())
    tampered = json.loads(raw)
    tampered['market'][0]['value'] = 0.5
    changed = json.dumps(tampered, sort_keys=True, separators=(',', ':')).encode()
    with pytest.raises(c.ContextError) as error:
        c.parse_snapshot(changed)
    assert error.value.reasons == ('SNAPSHOT_HASH_MISMATCH',)
    forged = snapshot.model_copy(update={'venue': 'binance'})
    result = c.validate_context(forged, requirement(), c.ClockView(decision_ms=snapshot.available_at_ms,
                                                                   receipt_ms=None))
    assert result.reasons == ('SNAPSHOT_HASH_MISMATCH',)
    with pytest.raises(c.ContextError) as error:
        c.parse_snapshot(b'{' + b' ' * c.MAX_SNAPSHOT_BYTES + b'}')
    assert error.value.reasons == ('PAYLOAD_TOO_LARGE',)


def test_synthetic_source_rejected_for_execution():
    snapshot = sealed(source_kind='synthetic')
    clock = c.ClockView(decision_ms=snapshot.available_at_ms, receipt_ms=None)
    assert c.validate_context(snapshot, requirement(), clock).reasons == ('SOURCE_KIND_REJECTED',)
    with pytest.raises(ValueError):
        requirement(allowed_source_kinds=('observed', 'synthetic'))
    research = requirement(consumer='research', allowed_source_kinds=('synthetic',))
    assert c.validate_context(snapshot, research, clock).accepted
    modeled = sealed(source_kind='modeled_availability')
    assert c.validate_context(modeled, requirement(), clock).reasons == ('SOURCE_KIND_REJECTED',)
    with pytest.raises(ValueError):
        sealed(source_kind='replayed')


def test_schema_and_numeraire_mismatch_rejected():
    snapshot = sealed()
    clock = c.ClockView(decision_ms=snapshot.available_at_ms, receipt_ms=None)
    assert c.validate_context(snapshot, requirement(numeraire='USDT'), clock).reasons == ('QUOTE_MISMATCH',)
    assert c.validate_context(snapshot, requirement(venue='binance'), clock).reasons == ('VENUE_MISMATCH',)
    assert c.validate_context(snapshot, requirement(stream_id='other'), clock).reasons == ('STREAM_MISMATCH',)
    assert c.validate_context(snapshot, requirement(config_digest='b' * 64), clock).reasons == ('PROVENANCE_MISMATCH',)
    with pytest.raises(ValueError):
        requirement(schema_major=2)
    raw = json.loads(c.serialize_snapshot(snapshot))
    raw['schema_version'] = '2.0'
    with pytest.raises(c.ContextError) as error:
        c.parse_snapshot(json.dumps(raw).encode())
    assert error.value.reasons == ('VERSION_MISMATCH',)
    missing = requirement(mandatory_features=('breadth_positive_eq_h5', 'market_return_eq_h60'))
    assert c.validate_context(snapshot, missing, clock).reasons == ('FEATURE_UNAVAILABLE',)
    low = sealed(coverage=c.Coverage(expected_count=5, valid_count=4, valid_weight_fraction=0.8,
                                     missing=(c.MissingAsset(asset_id='SOL', reasons=('INPUT_MISSING',)),)))
    assert c.validate_context(low, requirement(), clock).reasons == ('COVERAGE_LOW',)
    invalid = sealed(market=(feature(value=None, status='WARMUP_INCOMPLETE', reasons=('WARMUP_INCOMPLETE',)),),
                     status='WARMING', reasons=('WARMUP_INCOMPLETE',))
    assert c.validate_context(invalid, requirement(), clock).reasons == ('FEATURE_UNAVAILABLE',)


def test_selection_never_carries_a_fallback_snapshot():
    with pytest.raises(ValueError):
        c.ContextSelection(accepted=False, snapshot=sealed(), reasons=('CONTEXT_EXPIRED',), snapshot_id=None,
                           effective_available_at_ms=None, decision_ms=1)
    with pytest.raises(ValueError):
        c.ContextSelection(accepted=True, snapshot=None, reasons=(), snapshot_id=None,
                           effective_available_at_ms=None, decision_ms=1)


def test_contract_fields_grant_no_authority():
    forbidden = {'order', 'orders', 'amount', 'budget', 'capital', 'quote_amount', 'side', 'credential', 'api_key',
                 'target_position', 'spend'}
    for model in (c.ContextSnapshot, c.SnapshotContent, c.ContextSelection, c.NotificationEnvelope,
                  c.ContextStreamStatus):
        assert not forbidden & set(model.model_fields)


def _imports(path):
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add(node.module or '')
    return names


def test_import_has_no_io_or_controller_dependency(tmp_path):
    forbidden = ('controllers', 'hummingbot.connector', 'hummingbot.strategy_v2.controllers', 'hummingbot.data_feed',
                 'hummingbot.client', 'hummingbot.core', 'aiohttp', 'asyncio', 'socket', 'sqlite3', 'requests',
                 'os', 'time', 'datetime', 'random', 'uuid', 'subprocess', 'threading')
    for path in sorted(PACKAGE.glob('*.py')):
        for module in _imports(path):
            assert not any(module == f or module.startswith(f + '.') for f in forbidden), (path.name, module)
        clock_reads = {'now', 'utcnow', 'today', 'monotonic', 'monotonic_ns', 'perf_counter', 'time_ns'}
        assert not [node.attr for node in ast.walk(ast.parse(path.read_text()))
                    if isinstance(node, ast.Attribute) and node.attr in clock_reads], path.name
    modules = sorted(path.stem for path in PACKAGE.glob('*.py') if path.stem != '__init__')
    assert 'contracts' in modules
    probe = (
        'import sys\n'
        'events = []\n'
        'def hook(name, args):\n'
        '    if name.startswith(("socket.", "subprocess.", "os.system")) or (name == "open" and args[0]\n'
        '            and not str(args[0]).endswith((".py", ".pyc", ".so")) and "__pycache__" not in str(args[0])\n'
        '            and not str(args[0]).endswith(("/", "site-packages")) and "dist-info" not in str(args[0])):\n'
        '        events.append((name, str(args[0]) if args else ""))\n'
        'import numpy, pydantic, hummingbot.strategy_v2.components.recipe_contract\n'
        'import hummingbot.strategy_v2.components.strict_json\n'
        'before = set(sys.modules)\n'
        'sys.addaudithook(hook)\n'
        'import importlib\n'
        f'for name in {modules!r}:\n'
        '    importlib.import_module("hummingbot.strategy_v2.market_context." + name)\n'
        'loaded = set(sys.modules) - before\n'
        'bad = sorted(m for m in loaded if m.startswith(("controllers", "hummingbot.connector", "aiohttp",\n'
        '    "hummingbot.strategy_v2.controllers", "hummingbot.data_feed", "sqlite3", "asyncio")))\n'
        'files = [e for e in events if e[0] == "open" and not e[1].startswith(tuple(sys.path[1:]))]\n'
        'print(repr((bad, [e for e in events if e[0] != "open"], files)))\n')
    # Audit package effects, excluding the interpreter's own bytecode-cache writes.
    # A cold cache keeps this boundary independent of earlier test/import order.
    output = subprocess.run([sys.executable, '-B', '-c', probe], check=True, capture_output=True, text=True,
                            env={**os.environ, 'PYTHONPYCACHEPREFIX': str(tmp_path / 'bytecode')},
                            cwd=str(PACKAGE.parents[2])).stdout.strip().splitlines()[-1]
    assert output == '([], [], [])'
