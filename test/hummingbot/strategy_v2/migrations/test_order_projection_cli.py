"""The native maintenance CLI binds proof and never supplies an order transport."""
import importlib.util
from contextlib import contextmanager
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[4] / 'scripts/repair_order_projection.py'


def load():
    spec = importlib.util.spec_from_file_location('order_projection_cli', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_apply_consumes_exact_plan_without_network_and_reports_pending_checkpoint(monkeypatch, tmp_path):
    module = load()
    events = []

    @contextmanager
    def lock(path):
        events.append('lock')
        yield

    @contextmanager
    def connection(path):
        events.append('connect')
        yield object()
    native = {'order_id': 'one', 'plan_sha256': 'a' * 64}
    envelope = {'status': 'ORDER_PROJECTION_PLANNED', 'order_id': 'one', 'native_plan': native,
                'configuration_sha256': {'config': 'b' * 64}}
    envelope['plan_sha256'] = module._seal_envelope(envelope)
    monkeypatch.setattr(module, '_configuration_hashes', lambda: {'config': 'b' * 64})
    monkeypatch.setattr(module, '_owner_lock', lock)
    monkeypatch.setattr(module, '_checked_stopped', lambda path: events.append('stopped'))
    monkeypatch.setattr(module, '_rw_connection', connection)
    monkeypatch.setattr(module, '_postcommit_checkpoint', lambda path: 'pending')

    def apply(con, *, plan, expected_plan_sha256):
        assert plan == native and expected_plan_sha256 == 'a' * 64
        events.append('apply')
        return {'status': 'committed'}
    monkeypatch.setattr(module, '_native_api', lambda: (None, apply))
    result = module.apply_plan(tmp_path / 'existing.sqlite', 'one', tmp_path, envelope, envelope['plan_sha256'])
    assert events == ['lock', 'stopped', 'connect', 'apply']
    assert result['checkpoint_status'] == 'pending'
    assert result['status'] == 'ORDER_PROJECTION_REPAIRED_CHECKPOINT_PENDING'


@pytest.mark.parametrize('change', [{'order_id': 'foreign'}, {'plan_sha256': 'b' * 64}, {'status': 'forged'}])
def test_apply_rejects_changed_envelope_before_writable_connection(tmp_path, monkeypatch, change):
    module = load()
    calls = []
    monkeypatch.setattr(module, '_owner_lock', lambda path: calls.append(path))
    envelope = {'status': 'ORDER_PROJECTION_PLANNED', 'order_id': 'one', 'plan_sha256': 'a' * 64,
                'native_plan': {'order_id': 'one', 'plan_sha256': 'a' * 64}, **change}
    with pytest.raises(ValueError):
        module.apply_plan(tmp_path / 'db', 'one', tmp_path, envelope, 'a' * 64)
    assert calls == []


def test_configuration_change_refuses_apply_before_opening_writer(tmp_path, monkeypatch):
    module = load()

    @contextmanager
    def lock(path):
        yield
    monkeypatch.setattr(module, '_owner_lock', lock)
    monkeypatch.setattr(module, '_configuration_hashes', lambda: {'config': 'c' * 64})
    monkeypatch.setattr(module, '_rw_connection', lambda path: pytest.fail('changed config opened writer'))
    envelope = {'status': 'ORDER_PROJECTION_PLANNED', 'order_id': 'one',
                'native_plan': {'order_id': 'one', 'plan_sha256': 'a' * 64},
                'configuration_sha256': {'config': 'b' * 64}}
    envelope['plan_sha256'] = module._seal_envelope(envelope)
    with pytest.raises(ValueError, match='configuration'):
        module.apply_plan(tmp_path / 'db', 'one', tmp_path, envelope, envelope['plan_sha256'])
    with pytest.raises(ValueError, match='configuration'):
        module.verify_configuration(tmp_path, envelope, 'one', envelope['plan_sha256'])
