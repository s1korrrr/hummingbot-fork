from decimal import Decimal as D

from controllers.generic.momentum_fusion_v2_core import recipe, setups


def test_b0_confirms_a_prior_channel_break_once():
    state = setups.idle('b0_breakout')
    state = setups.step_b0(state, close=102, prior_high=101, event_ms=1000, now_ms=1000,
                           lifetime_ms=10_000, cooldown_ms=1000)
    assert state.phase == 'CONFIRMED' and state.reason == 'channel_break'
    state = setups.step_b0(state, close=102, prior_high=101, event_ms=2000, now_ms=2000,
                           lifetime_ms=10_000, cooldown_ms=1000)
    assert state.phase == 'PROPOSED'
    state = setups.step_b0(state, close=102, prior_high=101, event_ms=3000, now_ms=3000,
                           lifetime_ms=10_000, cooldown_ms=1000)
    assert state.phase == 'COOLDOWN'


def test_c1_valid_shelf_then_break():
    state = setups.idle('c1_shelf')
    state = setups.step_c1(state, close=110, high=111, low=108, impulse=1, bandwidth='0.4',
                           event_ms=1000, now_ms=1000, max_pullback='0.02', max_bandwidth='0.55',
                           min_shelf_bars=2, lifetime_ms=10_000, cooldown_ms=1000)
    assert state.phase == 'ARMED' and state.reason == 'impulse_armed'
    for i in range(2):
        state = setups.step_c1(state, close=110.2, high=110.4, low=110.0, impulse=0, bandwidth='0.3',
                               event_ms=2000 + i * 1000, now_ms=2000 + i * 1000, max_pullback='0.02',
                               max_bandwidth='0.55', min_shelf_bars=2, lifetime_ms=10_000, cooldown_ms=1000)
        assert state.reason == 'shelf_forming'
    state = setups.step_c1(state, close=111.5, high=111.6, low=110.8, impulse=0, bandwidth='0.3',
                           event_ms=5000, now_ms=5000, max_pullback='0.02', max_bandwidth='0.55',
                           min_shelf_bars=2, lifetime_ms=10_000, cooldown_ms=1000)
    assert state.phase == 'CONFIRMED' and state.reason == 'shelf_break'


def test_c1_false_shelf_and_overextended_break_and_expiry():
    armed = setups.step_c1(setups.idle('c1_shelf'), close=110, high=111, low=108, impulse=1, bandwidth='0.4',
                           event_ms=1000, now_ms=1000, max_pullback='0.005', max_bandwidth='0.55',
                           min_shelf_bars=2, lifetime_ms=5000, cooldown_ms=1000)
    deep = setups.step_c1(armed, close=109, high=109.2, low=108.9, impulse=0, bandwidth='0.3',
                          event_ms=2000, now_ms=2000, max_pullback='0.005', max_bandwidth='0.55',
                          min_shelf_bars=2, lifetime_ms=5000, cooldown_ms=1000)
    assert deep.phase == 'INVALIDATED' and deep.reason == 'false_shelf'
    chase = setups.step_c1(armed, close=112, high=112.2, low=111.5, impulse=0, bandwidth='0.3',
                           event_ms=2000, now_ms=2000, max_pullback='0.005', max_bandwidth='0.55',
                           min_shelf_bars=2, lifetime_ms=5000, cooldown_ms=1000)
    assert chase.phase == 'INVALIDATED' and chase.reason == 'overextended_break'
    expired = setups.step_c1(armed, close=110.2, high=110.3, low=110.1, impulse=0, bandwidth='0.3',
                             event_ms=8000, now_ms=8000, max_pullback='0.005', max_bandwidth='0.55',
                             min_shelf_bars=2, lifetime_ms=5000, cooldown_ms=1000)
    assert expired.phase == 'EXPIRED' and expired.reason == 'candidate_expired'


def test_c2_and_c3_positive_and_negative():
    compressed = setups.step_c2(setups.idle('c2_compression'), close=100, prior_high=101, bandwidth='0.2',
                                expansion_ratio='1.0', event_ms=1000, now_ms=1000, compression_max='0.35',
                                expand_min='1.6', lifetime_ms=10_000, cooldown_ms=1000)
    assert compressed.phase == 'ARMED'
    quiet = setups.step_c2(setups.idle('c2_compression'), close=100, prior_high=101, bandwidth='0.9',
                           expansion_ratio='1.0', event_ms=1000, now_ms=1000, compression_max='0.35',
                           expand_min='1.6', lifetime_ms=10_000, cooldown_ms=1000)
    assert quiet.phase == 'IDLE'
    broke = setups.step_c2(compressed, close=102, prior_high=101, bandwidth='0.2', expansion_ratio='2.0',
                           event_ms=2000, now_ms=2000, compression_max='0.35', expand_min='1.6',
                           lifetime_ms=10_000, cooldown_ms=1000)
    assert broke.phase == 'CONFIRMED'
    trend = setups.step_c3(setups.idle('c3_reclaim'), close=100, high=101, low=99, uptrend=1,
                           event_ms=1000, now_ms=1000, pullback_max='0.02', lifetime_ms=10_000, cooldown_ms=1000)
    pulled = setups.step_c3(trend, close=99, high=99.2, low=98.8, uptrend=1, event_ms=2000, now_ms=2000,
                            pullback_max='0.02', lifetime_ms=10_000, cooldown_ms=1000)
    assert pulled.reason == 'pullback'
    reclaim = setups.step_c3(pulled, close=100.2, high=100.4, low=99.8, uptrend=1, event_ms=3000, now_ms=3000,
                             pullback_max='0.02', lifetime_ms=10_000, cooldown_ms=1000)
    assert reclaim.phase == 'CONFIRMED' and reclaim.reason == 'reclaim'
    dumped = setups.step_c3(trend, close=90, high=91, low=89, uptrend=1, event_ms=2000, now_ms=2000,
                            pullback_max='0.02', lifetime_ms=10_000, cooldown_ms=1000)
    assert dumped.phase == 'INVALIDATED'


def test_first_slice_enables_b0_and_c1_only():
    spec = recipe.RecipeSpec()
    assert spec.modules() == recipe.FIRST_SLICE
    assert spec.execution_enabled is False
