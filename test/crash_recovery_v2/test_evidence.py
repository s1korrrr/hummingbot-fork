import importlib
from decimal import Decimal, localcontext

import pytest

from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot, Dataset

BASE = 1_777_593_600
DATASET = Dataset(connector='okx', trading_pair='BNB-USDC', history=60)


def modules():
    prefix = 'controllers.generic.crash_recovery_v2_core.'
    for name in ('market_stress_v1', 'recovery_evidence_v1'):
        if importlib.util.find_spec(prefix + name) is None:
            pytest.fail('missing specified component: ' + name)
    return (importlib.import_module(prefix + 'market_stress_v1'),
            importlib.import_module(prefix + 'recovery_evidence_v1'))


def policies(**changes):
    s, r = modules()
    values = dict(method='closed_drawdown_v1', lookback_bars=1, downside_fraction='0.05',
                  max_age_ms=120000, max_transport_age_ms=10000, max_history_rows=60)
    values.update(changes)
    return s.StressParameters(**values), r.RecoveryParameters(
        method='closed_reclaim_v1', stabilization_ms=120000, reclaim_fraction='0.03',
        new_low_fraction='0.001', eligibility_ttl_ms=120000)


def packet(prices=(), *, epoch='producer1', sequence=None, received_delay=10):
    s, _ = modules()
    tape = [Decimal('100')] * 60 + [Decimal(str(p)) for p in prices]
    rows = tuple(Candle(open_time=BASE + j * 60, open=p, high=p, low=p, close=p, volume='1')
                 for j, p in enumerate(tape) if j >= len(tape) - 60)
    event = (rows[-1].open_time + 60) * 1000
    snap = CandleSnapshot(dataset=DATASET, epoch=epoch, sequence=sequence or len(prices) + 1,
                          published_at=event / 1000, rows=rows)
    return s.ReceivedCandles(snapshot=snap, received_at_ms=event + received_delay)


def step(previous, prices, **kwargs):
    s, r = modules()
    sp, rp = policies()
    observed = packet(prices, **kwargs)
    old_s, old_r = (None, None) if previous is None else (previous[0].state, previous[1].state)
    se = s.evaluate_stress(old_s, observed, sp, observed.received_at_ms)
    re = r.evaluate_recovery(old_r, se, rp, observed.received_at_ms)
    return se, re


def recovered():
    state = None
    for tape in ((), (90,), (90, 90), (90, 90, 91), (90, 90, 91, 94)):
        state = step(state, tape)
    return state


def test_seed_never_admits_entry():
    se, re = step(None, ())
    assert se.reason == 'seeded' and se.status == 'HOLD' and not se.fresh
    assert re.status == 'HOLD' and re.state.phase == 'NORMAL'


def test_shock_stabilization_and_causal_reclaim():
    x = step(None, ())
    x = step(x, (90,))
    assert x[0].shocked and x[1].state.phase == 'SHOCK_HOLD'
    x = step(x, (90, 90))
    assert x[1].state.phase == 'STABILIZING' and x[1].status == 'HOLD'
    x = step(x, (90, 90, 94))
    assert x[1].reason == 'recovery_wait_time'
    x = step(x, (90, 90, 94, 94))
    assert x[1].status == 'PASS' and x[1].state.phase == 'RECOVERY_ELIGIBLE'
    assert x[1].state.running_low == Decimal('90')


def test_exact_threshold_is_shock_and_previous_closes_only():
    x = step(None, ())
    x = step(x, (95,))
    assert x[0].shocked
    x = step(None, ())
    x = step(x, (Decimal('95.0000000001'),))
    assert not x[0].shocked


def test_reclaim_is_not_enough_before_stabilization():
    x = step(step(None, ()), (90,))
    x = step(x, (90, 94))
    assert x[1].status == 'HOLD'


def test_new_low_resets_stabilization_and_rebound_is_not_budget():
    x = step(step(step(None, ()), (90,)), (90, 90))
    x = step(x, (90, 90, 89))
    assert x[1].reason == 'recovery_new_low'
    stamp = x[1].state.stable_since_ms
    x = step(x, (90, 90, 89, 94))
    assert x[1].status == 'HOLD' and x[1].state.stable_since_ms == stamp
    assert 'budget' not in type(x[1].state).model_fields


def test_duplicate_does_not_advance_time_or_renew_evidence():
    s, r = modules()
    sp, rp = policies()
    x = recovered()
    original = x[1].state
    p = packet((90, 90, 91, 94), received_delay=1000)
    se = s.evaluate_stress(x[0].state, p, sp, p.received_at_ms)
    re = r.evaluate_recovery(original, se, rp, p.received_at_ms)
    assert not se.fresh and re.status == 'HOLD'
    assert re.state.eligible_until_ms == original.eligible_until_ms
    assert se.observation.available_time_ms == x[0].observation.available_time_ms


def test_expiry_cannot_rearm_the_same_event():
    s, r = modules()
    sp, rp = policies()
    x = recovered()
    p = packet((90, 90, 91, 94))
    now = x[1].state.eligible_until_ms
    se = s.evaluate_stress(x[0].state, p, sp, now)
    re = r.evaluate_recovery(x[1].state, se, rp, now)
    assert re.status != 'PASS' and re.state.phase != 'RECOVERY_ELIGIBLE'


def test_epoch_change_requires_new_evidence_and_keeps_low():
    x = recovered()
    x = step(x, (90, 90, 91, 94, 94), epoch='producer2')
    assert x[0].reason == 'continuity_reseed'
    assert x[1].status == 'HOLD' and x[1].state.running_low == Decimal('90')
    assert x[1].state.phase == 'STABILIZING'


def test_event_gap_reseeds_instead_of_counting_missing_bars():
    x = step(None, ())
    x = step(x, (100, 100, 100))
    assert x[0].status == 'HOLD' and x[0].reason == 'continuity_reseed'


def test_same_event_correction_invalidates_recovery():
    x = recovered()
    x = step(x, (90, 90, 91, 93), sequence=6)
    assert x[0].reason == 'revised_observation'
    assert x[1].status == 'HOLD' and x[1].state.phase == 'STABILIZING'


def test_historical_overlap_correction_on_new_event_is_not_silent():
    x = recovered()
    x = step(x, (90, 90, 92, 94, 94))
    assert x[0].reason == 'revised_history'
    assert x[1].status == 'HOLD'


def test_conflicting_same_revision_and_regressing_clock_reject():
    s, _ = modules()
    sp, _ = policies()
    x = step(None, ())
    p = packet((99,), sequence=1)
    with pytest.raises(ValueError):
        s.evaluate_stress(x[0].state, p, sp, p.received_at_ms)
    p = packet(())
    with pytest.raises(ValueError):
        s.evaluate_stress(x[0].state, p, sp, x[0].state.clock_ms - 1)


def test_missing_data_preserves_shock_without_granting_eligibility():
    s, r = modules()
    sp, rp = policies()
    x = step(step(None, ()), (90,))
    now = x[0].state.clock_ms + 1
    se = s.evaluate_stress(x[0].state, None, sp, now)
    re = r.evaluate_recovery(x[1].state, se, rp, now)
    assert se.status == 'UNAVAILABLE'
    assert re.status != 'PASS' and re.state.phase == 'SHOCK_HOLD'
    assert re.state.running_low == x[1].state.running_low


def test_stale_candle_cannot_be_refreshed_by_recent_receipt():
    s, _ = modules()
    sp, _ = policies()
    p = packet((), received_delay=120001)
    se = s.evaluate_stress(None, p, sp, p.received_at_ms)
    assert se.status == 'STALE'


def test_transport_age_is_independent_of_candle_age():
    s, _ = modules()
    sp, _ = policies()
    p = packet(())
    se = s.evaluate_stress(None, p, sp, p.received_at_ms + 10001)
    assert se.status == 'STALE'


@pytest.mark.parametrize('field,value', [('lookback_bars', True), ('lookback_bars', 0),
                                         ('lookback_bars', 10001), ('downside_fraction', 'NaN'), ('downside_fraction', 'Infinity'),
                                         ('downside_fraction', True), ('downside_fraction', 0), ('downside_fraction', 1),
                                         ('max_age_ms', False), ('max_history_rows', 59), ('method', 'normalized_magic')])
def test_invalid_parameters_reject(field, value):
    with pytest.raises(ValueError):
        policies(**{field: value})


def test_missing_parameters_are_not_filled_with_trading_defaults():
    s, r = modules()
    with pytest.raises(ValueError):
        s.StressParameters()
    with pytest.raises(ValueError):
        r.RecoveryParameters()


@pytest.mark.parametrize('mutate', ['unconfirmed', 'gap', 'future', 'nonfinite', 'wrong_interval', 'huge_price'])
def test_forged_canonical_snapshot_is_revalidated(mutate):
    s, _ = modules()
    sp, _ = policies()
    p = packet(())
    snap = p.snapshot
    if mutate == 'unconfirmed':
        snap = snap.model_copy(update={'rows': (*snap.rows[:-1], snap.rows[-1].model_copy(update={'confirmed': False}))})
    elif mutate == 'gap':
        snap = snap.model_copy(update={'rows': (*snap.rows[:-2], snap.rows[-1])})
    elif mutate == 'future':
        snap = snap.model_copy(update={'published_at': snap.published_at + 100})
    elif mutate == 'nonfinite':
        snap = snap.model_copy(update={'published_at': float('nan')})
    elif mutate == 'wrong_interval':
        snap = snap.model_copy(update={'dataset': snap.dataset.model_copy(update={'interval': '5m'})})
    else:
        huge = Decimal('1e99999')
        last = snap.rows[-1].model_copy(update=dict(open=huge, high=huge, low=huge, close=huge))
        snap = snap.model_copy(update={'rows': (*snap.rows[:-1], last)})
    forged = p.model_copy(update={'snapshot': snap})
    with pytest.raises((ValueError, TypeError)):
        s.evaluate_stress(None, forged, sp, p.received_at_ms)


def test_future_receipt_and_boolean_clock_reject():
    s, _ = modules()
    sp, _ = policies()
    p = packet(())
    with pytest.raises(ValueError):
        s.evaluate_stress(None, p, sp, p.received_at_ms - 1)
    with pytest.raises(ValueError):
        s.evaluate_stress(None, p, sp, True)


def test_warmup_is_unavailable_not_normal_market():
    s, _ = modules()
    sp, _ = policies()
    p = packet(())
    p = p.model_copy(update={'snapshot': p.snapshot.model_copy(update={'rows': p.snapshot.rows[-2:]})})
    assert s.evaluate_stress(None, p, sp, p.received_at_ms).status == 'UNAVAILABLE'


def test_parameter_and_dataset_change_do_not_reset_checkpoint():
    s, _ = modules()
    sp, _ = policies()
    x = step(None, ())
    p = packet((99,))
    changed, _ = policies(downside_fraction='0.04')
    with pytest.raises(ValueError):
        s.evaluate_stress(x[0].state, p, changed, p.received_at_ms)
    ds = p.snapshot.dataset.model_copy(update={'trading_pair': 'BTC-USDC'})
    p = p.model_copy(update={'snapshot': p.snapshot.model_copy(update={'dataset': ds})})
    with pytest.raises(ValueError):
        s.evaluate_stress(x[0].state, p, sp, p.received_at_ms)


def test_checkpoint_round_trip_and_decimal_context_are_deterministic():
    s, r = modules()
    sp, rp = policies()
    x = recovered()
    st = s.StressState.model_validate_json(x[0].state.model_dump_json())
    rt = r.RecoveryState.model_validate_json(x[1].state.model_dump_json())
    p = packet((90, 90, 91, 94, 94))
    a = s.evaluate_stress(st, p, sp, p.received_at_ms)
    b = r.evaluate_recovery(rt, a, rp, p.received_at_ms)
    with localcontext() as ctx:
        ctx.prec = 2
        aa = s.evaluate_stress(st, p, sp, p.received_at_ms)
        bb = r.evaluate_recovery(rt, aa, rp, p.received_at_ms)
    assert aa == a and bb == b


def test_forged_state_and_mismatched_recovery_clock_reject():
    s, r = modules()
    sp, rp = policies()
    x = recovered()
    p = packet((90, 90, 91, 94, 94))
    with pytest.raises(ValueError):
        s.evaluate_stress(x[0].state.model_copy(update={'clock_ms': True}), p, sp, p.received_at_ms)
    with pytest.raises(ValueError):
        r.evaluate_recovery(x[1].state, x[0], rp, x[0].evaluated_at_ms + 1)


def test_delayed_first_receipt_does_not_hide_old_publication():
    s, _ = modules()
    sp, _ = policies()
    p = packet((), received_delay=20000)
    assert s.evaluate_stress(None, p, sp, p.received_at_ms).status == 'STALE'


def test_repeated_small_lows_eventually_reset_qualification_anchor():
    x = step(step(step(None, ()), (90,)), (90, 90))
    x = step(x, (90, 90, Decimal('89.95')))
    assert x[1].state.qualifying_low == Decimal('90')
    x = step(x, (90, 90, Decimal('89.95'), Decimal('89.89')))
    assert x[1].reason == 'recovery_new_low'
    assert x[1].state.qualifying_low == Decimal('89.89')


def test_new_shock_invalidates_eligibility():
    x = step(recovered(), (90, 90, 91, 94, 80))
    assert x[1].state.phase == 'SHOCK_HOLD' and x[1].state.eligible_until_ms is None


def test_bad_quality_invalidates_but_does_not_erase_low():
    s, r = modules()
    sp, rp = policies()
    x = recovered()
    now = x[0].state.clock_ms + 1
    se = s.evaluate_stress(x[0].state, None, sp, now)
    re = r.evaluate_recovery(x[1].state, se, rp, now)
    assert re.state.phase == 'STABILIZING' and re.state.eligible_until_ms is None
    assert re.state.running_low == x[1].state.running_low


def test_passing_stress_without_price_evidence_is_rejected():
    _, r = modules()
    _, rp = policies()
    x = step(step(None, ()), (100,))
    broken = x[0].model_copy(update={'close': None, 'low': None})
    with pytest.raises(ValueError):
        r.evaluate_recovery(x[1].state, broken, rp, broken.evaluated_at_ms)


def test_missing_recovery_checkpoint_cannot_clear_existing_stress_history():
    _, r = modules()
    _, rp = policies()
    x = recovered()
    with pytest.raises(ValueError):
        r.evaluate_recovery(None, x[0], rp, x[0].evaluated_at_ms)


def test_price_failing_reclaim_stays_held_and_underperformance_cannot_sell():
    x = step(step(step(None, ()), (90,)), (90, 90))
    x = step(x, (90, 90, 91))
    x = step(x, (90, 90, 91, 92))
    assert x[1].reason == 'recovery_wait_reclaim'
    assert x[1].status == 'HOLD'


def test_recovery_policy_revision_and_partial_checkpoint_reject():
    _, r = modules()
    _, rp = policies()
    x = recovered()
    bad = rp.model_copy(update={'reclaim_fraction': Decimal('0.04')})
    with pytest.raises(ValueError):
        r.evaluate_recovery(x[1].state, x[0], bad, x[0].evaluated_at_ms)
    for changes in ({'running_low': None}, {'qualifying_low': Decimal('1')},
                    {'stable_since_ms': None}, {'eligible_until_ms': None}, {'phase': 'NORMAL'}):
        with pytest.raises(ValueError):
            r.evaluate_recovery(x[1].state.model_copy(update=changes), x[0], rp, x[0].evaluated_at_ms)


def test_new_shock_after_recovery_does_not_reuse_an_ancient_low():
    x = step(recovered(), (90, 90, 91, 94, 110))
    x = step(x, (90, 90, 91, 94, 110, 100))
    assert x[1].state.phase == 'SHOCK_HOLD'
    assert x[1].state.running_low == Decimal('100')
    x = step(x, (90, 90, 91, 94, 110, 100, 90))
    assert x[1].state.running_low == Decimal('90')
