"""The production smoke cannot hide lost streaming delivery behind recovered history."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

SCRIPT = Path(__file__).resolve().parents[2] / 'docs/controllers/modular_spot/rust_candle_smoke.py'
spec = importlib.util.spec_from_file_location('rust_candle_smoke', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def smoke(tmp_path):
    universe = tmp_path / 'universe.json'
    universe.write_text(json.dumps({'pairs': ['BTC-USDC']}))
    result = module.Smoke(SimpleNamespace(output=tmp_path / 'output', universe=universe, history=90, seconds=900))
    result.started = result.started_mono = 0
    result.steady = result.steady_mono = 60
    result.finished = result.finished_mono = 960
    rows = {('BTC-USDC', minute) for minute in module.completed_minutes(60, 960)}
    result.seen = [set(rows), set(rows)]
    result.timely = [set(rows), set(rows)]
    result.delays = [[.001] * 100, [.002] * 100]
    result.frames_checked = 100
    result.consumer_restarted = result.consumer_recovered = True
    result.producer_restarted = result.recovered_after_restart = True
    result.final_ready = [1, 1]
    return result


def test_complete_streaming_fixture_can_pass(tmp_path):
    assert smoke(tmp_path).finish()['passed']


def test_backfilled_history_cannot_mask_missing_live_updates(tmp_path):
    candidate = smoke(tmp_path)
    candidate.timely[0].remove(('BTC-USDC', 240))
    report = candidate.finish()
    assert report['coverage_passed']
    assert not report['incremental_coverage_passed']
    assert not report['passed']


def test_consumer_restart_exempts_only_affected_reader(tmp_path):
    candidate = smoke(tmp_path)
    candidate.outages[0] = [[300, 305]]
    candidate.timely[0].remove(('BTC-USDC', 240))
    assert candidate.finish()['passed']
    candidate.timely[1].remove(('BTC-USDC', 240))
    assert not candidate.finish()['passed']


def test_unexpected_failure_rejects_otherwise_complete_capture(tmp_path):
    candidate = smoke(tmp_path)
    candidate.errors.append({'type': 'unexpected_reader_failure', 'error': 'PolicyError'})
    assert not candidate.finish()['passed']


def test_clock_step_invalidates_duration_evidence(tmp_path):
    candidate = smoke(tmp_path)
    candidate.finished = 961
    report = candidate.finish()
    assert report['observed_seconds'] == 900
    assert any(error['type'] == 'local_clock_step' for error in report['errors'])
    assert not report['passed']


def test_latency_target_and_final_readiness_are_required(tmp_path):
    candidate = smoke(tmp_path)
    candidate.delays[0] = [.101] * 100
    assert not candidate.finish()['passed']
    candidate.delays[0] = [.001] * 100
    candidate.final_ready[0] = 0
    assert not candidate.finish()['passed']


def test_provenance_catalog_is_rendered_as_strict_producer_manifest(tmp_path):
    universe = tmp_path / 'catalog.json'
    universe.write_text(json.dumps({'pairs': ['BTC-USDC'], 'url': 'https://public.example/catalog',
                                    'raw_sha256': 'a' * 64, 'received_at': 1}))
    candidate = module.Smoke(SimpleNamespace(output=tmp_path / 'output', universe=universe,
                                             history=90, seconds=900))
    assert json.loads(candidate.producer_universe.read_text()) == {'pairs': ['BTC-USDC']}
    assert json.loads(universe.read_text())['raw_sha256'] == 'a' * 64
