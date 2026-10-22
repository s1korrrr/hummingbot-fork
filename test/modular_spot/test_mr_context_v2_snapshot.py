import importlib
from decimal import Decimal

import pytest


def api():
    return importlib.import_module('controllers.generic.mr_context_v2_core.snapshot')


def payload(**changes):
    data = dict(source='okx', market_type='spot', quote_asset='USDC', interval='1m',
                dataset_sha256='a' * 64, epoch='epoch1', revision=1,
                implementation_sha256='b' * 64, parameters_sha256='c' * 64,
                initialization_sha256='d' * 64, universe_sha256='e' * 64,
                universe_revision=1, source_content_sha256='f' * 64,
                bar_open_ms=60000, bar_close_ms=120000, confirmed=True,
                available_time_ms=121000, received_at_ms=121050, expires_at_ms=150000,
                expected_members=10, ready_members=10, fresh_members=10,
                history_bars=30, missing_required=(), evidence_kind='observed',
                components=dict(return_1m_fraction='-0.002', return_5m_fraction='-0.02',
                                breadth_fraction='0.2', quote_volume_5m='1000000',
                                realized_vol_15m_fraction='0.01'))
    data.update(changes)
    return data


def requirements(**changes):
    data = dict(source='okx', dataset_sha256='a' * 64, implementation_sha256='b' * 64,
                parameters_sha256='c' * 64, initialization_sha256='d' * 64,
                universe_sha256='e' * 64, universe_revision=1,
                min_coverage_fraction='0.9', min_members=5, min_history_bars=20,
                max_source_age_ms=60000, max_transport_age_ms=10000)
    data.update(changes)
    return api().ContextRequirementsV2(**data)


def test_context_snapshot_rejects_unconfirmed():
    cls = api().ContextSnapshotV2
    with pytest.raises(ValueError):
        cls(**payload(confirmed=False))


def test_snapshot_uses_canonical_reference_and_roundtrips():
    from hummingbot.strategy_v2.components.decision_protocol import ObservationRef
    cls = api().ContextSnapshotV2
    item = cls(**payload())
    assert isinstance(item.observation, ObservationRef)
    assert item.observation.event_time_ms == 120000
    assert item.observation.available_time_ms == 121000
    assert item.observation.content_sha256 == item.content_sha256
    assert cls.model_validate_json(item.model_dump_json()) == item
    assert item.components.return_5m_fraction == Decimal('-0.02')
    with pytest.raises(ValueError):
        item.revision = 2


@pytest.mark.parametrize('field,value', [
    ('confirmed', 1), ('revision', True), ('universe_revision', 0),
    ('bar_open_ms', '60000'), ('bar_close_ms', 119999), ('bar_open_ms', 60001),
    ('available_time_ms', 119999), ('received_at_ms', 120999),
    ('expires_at_ms', 121000), ('expires_at_ms', 100000000),
    ('expected_members', 0), ('fresh_members', 11), ('ready_members', 11),
    ('fresh_members', True), ('ready_members', -1), ('history_bars', 0),
    ('universe_sha256', 'xyz'), ('interval', '5m'), ('market_type', 'perpetual'),
    ('missing_required', ('a', 'a')), ('missing_required', tuple(str(i) for i in range(65))),
])
def test_invalid_snapshot_inputs_reject(field, value):
    with pytest.raises(ValueError):
        api().ContextSnapshotV2(**payload(**{field: value}))


@pytest.mark.parametrize('value', [True, float('nan'), 'NaN', 'Infinity', 0.2, '-0.1', '1.1'])
def test_breadth_requires_finite_fraction_without_float_coercion(value):
    data = payload()
    data['components']['breadth_fraction'] = value
    with pytest.raises(ValueError):
        api().ContextSnapshotV2(**data)


def test_unknown_scalar_and_units_cannot_be_silently_accepted():
    data = payload()
    data['composite_score'] = 95
    with pytest.raises(ValueError):
        api().ContextSnapshotV2(**data)
    data = payload()
    data['components']['units'] = 'percent'
    with pytest.raises(ValueError):
        api().ContextSnapshotV2(**data)


def test_read_does_not_renew_observation_or_expiry():
    cls = api().ContextSnapshotV2
    first, reread = cls(**payload()), cls(**payload(received_at_ms=122000))
    assert first.observation == reread.observation
    assert first.expires_at_ms == reread.expires_at_ms
    assert 'context_expired' in api().context_blockers(reread, requirements(), now_ms=150000)


def test_future_data_prefix_invariance_and_permutation():
    cls = api().ContextSnapshotV2
    current = cls(**payload())
    future = cls(**payload(revision=2, bar_open_ms=120000, bar_close_ms=180000,
                           available_time_ms=181000, received_at_ms=181050, expires_at_ms=210000))
    for records in ((current,), (current, future), (future, current)):
        assert api().select_as_of(records, now_ms=122000) == current
    assert api().select_as_of((current,), now_ms=121049) is None


def test_correction_creates_distinct_content_without_rewriting_past():
    cls = api().ContextSnapshotV2
    first = cls(**payload())
    corrected = cls(**payload(revision=2, available_time_ms=123000, received_at_ms=123050))
    assert first.observation.content_sha256 != corrected.observation.content_sha256
    assert api().select_as_of((first, corrected), now_ms=122000) == first
    assert api().select_as_of((first, corrected), now_ms=124000) == corrected


def test_same_revision_conflict_rejected_and_identical_duplicates_deduplicated():
    cls = api().ContextSnapshotV2
    first = cls(**payload())
    changed = cls(**payload(source_content_sha256='0' * 64))
    with pytest.raises(ValueError, match='conflict'):
        api().select_as_of((first, changed), now_ms=122000)
    assert api().select_as_of((first, first), now_ms=122000) == first


def test_snapshot_selection_is_bounded_and_single_dataset():
    cls = api().ContextSnapshotV2
    first = cls(**payload())
    with pytest.raises(ValueError):
        api().select_as_of((first,) * 257, now_ms=122000)
    with pytest.raises(ValueError):
        api().select_as_of((first, cls(**payload(dataset_sha256='0' * 64))), now_ms=122000)


def test_latest_bad_membership_is_not_replaced_with_older_favorable_context():
    cls = api().ContextSnapshotV2
    first = cls(**payload())
    changed = cls(**payload(
        revision=2, universe_sha256='0' * 64,
        available_time_ms=123000, received_at_ms=123010))
    latest = api().select_as_of((first, changed), now_ms=124000)
    assert latest == changed
    assert 'membership_changed' in api().context_blockers(latest, requirements(), now_ms=124000)


@pytest.mark.parametrize('changes,reason', [
    ({'evidence_kind': 'reconstructed'}, 'availability_unobserved'),
    ({'fresh_members': 8}, 'coverage_insufficient'),
    ({'ready_members': 8}, 'coverage_insufficient'),
    ({'history_bars': 19}, 'warmup_incomplete'),
    ({'missing_required': ('BTC-USDC',)}, 'dependency_missing'),
    ({'source': 'binance'}, 'source_mismatch'),
    ({'dataset_sha256': '0' * 64}, 'dataset_mismatch'),
    ({'parameters_sha256': '0' * 64}, 'implementation_mismatch'),
])
def test_context_validity_is_not_a_bullish_score(changes, reason):
    item = api().ContextSnapshotV2(**payload(**changes))
    assert reason in api().context_blockers(item, requirements(), now_ms=122000)


def test_source_age_and_transport_age_are_separate():
    item = api().ContextSnapshotV2(**payload(received_at_ms=190000, expires_at_ms=210000))
    assert 'source_stale' in api().context_blockers(item, requirements(), now_ms=190001)
    item = api().ContextSnapshotV2(**payload())
    assert 'transport_stale' in api().context_blockers(item, requirements(), now_ms=140000)
    assert 'context_unavailable' in api().context_blockers(None, requirements(), now_ms=122000)


def test_validity_revalidates_constructed_or_copied_models():
    item = api().ContextSnapshotV2(**payload()).model_copy(update={'confirmed': False})
    with pytest.raises(ValueError):
        api().context_blockers(item, requirements(), now_ms=122000)
    req = requirements().model_copy(update={'max_source_age_ms': True})
    with pytest.raises(ValueError):
        api().context_blockers(api().ContextSnapshotV2(**payload()), req, now_ms=122000)


@pytest.mark.parametrize('now', [True, '122000', -1])
def test_invalid_clock_rejected(now):
    with pytest.raises(ValueError):
        api().select_as_of((), now_ms=now)


def test_independent_ready_and_fresh_counts_do_not_prove_their_intersection():
    item = api().ContextSnapshotV2(**payload(ready_members=9, fresh_members=9))
    assert 'coverage_insufficient' in api().context_blockers(item, requirements(), now_ms=122000)


def test_validity_threshold_is_independent_of_decimal_precision():
    from decimal import localcontext
    item = api().ContextSnapshotV2(**payload(ready_members=9))
    req = requirements(min_coverage_fraction='0.900000000000000001')
    with localcontext() as ctx:
        ctx.prec = 6
        assert 'coverage_insufficient' in api().context_blockers(item, req, now_ms=122000)


def test_wire_decoder_reuses_strict_json_and_enforces_payload_bound():
    import json
    raw = json.dumps(payload()).encode()
    assert api().decode_snapshot(raw) == api().ContextSnapshotV2(**payload())
    with pytest.raises(ValueError, match='duplicate'):
        api().decode_snapshot(b'{"revision":1,"revision":2}')
    for invalid in (b' ' * 65537, b'\xff', b'{"x":NaN}', b'[]', b'[' * 1500):
        with pytest.raises(ValueError):
            api().decode_snapshot(invalid)


def test_market_pulse_scalar_payload_is_not_decoded_as_context():
    import json
    body = dict(payload())
    body['composite_score'] = 70
    with pytest.raises(ValueError, match='Market Pulse'):
        api().decode_snapshot(json.dumps(body).encode())
