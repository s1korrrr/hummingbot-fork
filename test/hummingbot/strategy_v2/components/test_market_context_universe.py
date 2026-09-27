"""T2 point-in-time reference universe and weights (observation only; no entry authority)."""
import random
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.market_context import universe as u
from hummingbot.strategy_v2.market_context.contracts import ClockView, ContextError

DAY = u.DAY_MS
D = 20_000 * DAY  # a UTC midnight
CLOCK = ClockView(decision_ms=D + 5 * 60_000, receipt_ms=None)


def meta(asset, *, instrument=None, quote='USDC', cls='spot_asset', listed=D - 400 * DAY, venue='okx'):
    return u.InstrumentMetadata(instrument_id=instrument or f'{asset}-{quote}', asset_id=asset, venue=venue,
                                numeraire=quote, classification=cls, listed_at_ms=listed)


def metadata(*rows, available=D - DAY):
    return u.MetadataSnapshot(revision='meta-1', classification_revision='cls-1', available_at_ms=available,
                              instruments=tuple(rows))


def days(instrument, amount, *, count=7, available_delay=60_000, base=D):
    return [u.TurnoverDay(instrument_id=instrument, day_start_ms=base - k * DAY, quote_turnover=Decimal(amount),
                          available_at_ms=base - k * DAY + DAY + available_delay)
            for k in range(1, count + 1)]


def panel(*groups):
    return u.TurnoverPanel(numeraire='USDC', rows=tuple(row for group in groups for row in group))


def spec(**changes):
    values = dict(universe_id='okx-usdc-top3', mode='liquid_top_n', count=3, venue='okx', numeraire='USDC',
                  basket_assets=(), benchmarks=(), lookback_days=7, min_listing_age_days=30,
                  hysteresis=Decimal('1.2'), eligible_classifications=('spot_asset',), cap_floor=Decimal('0.05'),
                  algorithm_version='mc-universe-1')
    values.update(changes)
    return u.UniverseSpec(**values)


ASSETS = {'BTC': 900, 'ETH': 800, 'SOL': 700, 'XRP': 600, 'DOGE': 500, 'ADA': 400}


def base_inputs():
    return (metadata(*(meta(a) for a in ASSETS)),
            panel(*(days(f'{a}-USDC', v) for a, v in ASSETS.items())))


def test_membership_uses_only_available_prior_volume():
    md, tp = base_inputs()
    late = days('ADA-USDC', 10_000, count=1, available_delay=10 * 60_000)  # D-1 published after the decision
    # The current day completes after the decision; it cannot vote even if replayed from a later cache.
    current_day = [u.TurnoverDay(instrument_id='ADA-USDC', day_start_ms=D, quote_turnover=Decimal(10 ** 9),
                                 available_at_ms=D + DAY + 60_000)]
    with pytest.raises(ValueError):  # an in-progress day is not a complete turnover observation
        u.TurnoverDay(instrument_id='ADA-USDC', day_start_ms=D, quote_turnover=Decimal(1), available_at_ms=D + 1)
    rows = [r for r in tp.rows if not (r.instrument_id == 'ADA-USDC' and r.day_start_ms == D - DAY)]
    binding = u.select_reference(spec(), md, u.TurnoverPanel(numeraire='USDC', rows=(*rows, *late, *current_day)),
                                 None, CLOCK)
    assert binding.member_asset_ids == ('BTC', 'ETH', 'SOL')
    assert ('ADA-USDC', ('TURNOVER_UNAVAILABLE',)) in {(e.instrument_id, e.reasons) for e in binding.excluded}
    # A membership computed late is effective at its computation, not retroactively at midnight.
    assert binding.selection_as_of_ms == D and binding.effective_from_ms == CLOCK.decision_ms
    assert binding.available_at_ms == CLOCK.decision_ms
    with pytest.raises(ContextError) as error:  # metadata published after the decision cannot be used
        u.select_reference(spec(), metadata(*md.instruments, available=CLOCK.decision_ms + 1), tp, None, CLOCK)
    assert error.value.reasons == ('MEMBERSHIP_UNAVAILABLE',)
    # Removing future information entirely yields the same binding.
    again = u.select_reference(spec(), md, u.TurnoverPanel(numeraire='USDC', rows=tuple(rows)), None, CLOCK)
    assert again.member_asset_ids == binding.member_asset_ids


def test_duplicate_underlying_has_one_vote():
    md, tp = base_inputs()
    extra = meta('BTC', instrument='XBT-USDC')
    binding = u.select_reference(spec(count=4), metadata(*md.instruments, extra),
                                 panel(tp.rows, days('XBT-USDC', 5_000)), None, CLOCK)
    assert binding.member_asset_ids == ('BTC', 'ETH', 'SOL', 'XRP')
    assert dict(zip(binding.member_asset_ids, binding.member_instrument_ids))['BTC'] == 'XBT-USDC'
    assert ('BTC-USDC', ('DUPLICATE_UNDERLYING',)) in {(e.instrument_id, e.reasons) for e in binding.excluded}
    # The duplicate instrument cannot increase the underlying's vote: turnover is not summed.
    assert dict(zip(binding.member_asset_ids, binding.trailing_turnover))['BTC'] == Decimal(5_000 * 7)


def test_quote_or_classification_unknown_excludes():
    md, tp = base_inputs()
    rows = (meta('USDT', cls='stablecoin'), meta('WIF', cls=None), meta('NEW', listed=D - 5 * DAY),
            meta('OLD', listed=None), meta('EUR', quote='EUR'))
    extra = panel(*(days(f'{a}-USDC', 10 ** 6) for a in ('USDT', 'WIF', 'NEW', 'OLD')), days('EUR-EUR', 10 ** 6))
    binding = u.select_reference(spec(), metadata(*md.instruments, *rows),
                                 u.TurnoverPanel(numeraire='USDC', rows=(*tp.rows, *extra.rows)), None, CLOCK)
    assert binding.member_asset_ids == ('BTC', 'ETH', 'SOL')
    reasons = {e.instrument_id: e.reasons for e in binding.excluded}
    assert reasons['USDT-USDC'] == ('CLASSIFICATION_EXCLUDED',)
    assert reasons['WIF-USDC'] == ('CLASSIFICATION_UNKNOWN',)
    assert reasons['NEW-USDC'] == ('LISTING_TOO_RECENT',)
    assert reasons['OLD-USDC'] == ('LISTING_UNKNOWN',)
    assert reasons['EUR-EUR'] == ('QUOTE_MISMATCH',)
    with pytest.raises(ContextError) as error:  # exactly N valid members are required; fewer never relabel
        u.select_reference(spec(count=7), md, tp, None, CLOCK)
    assert error.value.reasons == ('MEMBERSHIP_UNAVAILABLE',)
    with pytest.raises(ContextError) as error:
        u.select_reference(spec(benchmarks=('ADA',)), md, tp, None, CLOCK)
    assert error.value.reasons == ('BENCHMARK_MISSING',)
    with pytest.raises(ContextError) as error:  # broad mode rejects rather than silently truncating
        u.select_reference(spec(mode='broad_eligible', count=5), md, tp, None, CLOCK)
    assert error.value.reasons == ('CAPACITY_EXCEEDED',)


def test_hysteresis_and_ties_deterministic():
    md, tp = base_inputs()
    first = u.select_reference(spec(), md, tp, None, CLOCK)
    assert first.member_asset_ids == ('BTC', 'ETH', 'SOL')
    # XRP overtakes SOL; SOL is rank 4 <= ceil(1.2*3)=4 and stays as an incumbent.
    changed = panel(*(days(f'{a}-USDC', v) for a, v in {**ASSETS, 'XRP': 750}.items()))
    tomorrow = ClockView(decision_ms=CLOCK.decision_ms + DAY, receipt_ms=None)
    shifted = panel(*(days(f'{a}-USDC', v, base=D + DAY) for a, v in {**ASSETS, 'XRP': 750}.items()))
    kept = u.select_reference(spec(), md, shifted, first, tomorrow)
    assert kept.member_asset_ids == ('BTC', 'ETH', 'SOL')
    fresh = u.select_reference(spec(), md, changed, None, CLOCK)
    assert fresh.member_asset_ids == ('BTC', 'ETH', 'XRP')
    # SOL falls to rank 5 > 4 and is replaced by rank order.
    fallen = panel(*(days(f'{a}-USDC', v, base=D + DAY) for a, v in {**ASSETS, 'XRP': 750, 'DOGE': 720}.items()))
    replaced = u.select_reference(spec(), md, fallen, first, tomorrow)
    assert replaced.member_asset_ids == ('BTC', 'ETH', 'XRP')
    # Ties break by canonical asset id; input order never matters.
    tied = panel(*(days(f'{a}-USDC', 100) for a in ASSETS))
    expected = u.select_reference(spec(), md, tied, None, CLOCK)
    assert expected.member_asset_ids == ('ADA', 'BTC', 'DOGE')
    for seed in range(5):
        rng = random.Random(seed)
        rows, instruments = list(tied.rows), list(md.instruments)
        rng.shuffle(rows)
        rng.shuffle(instruments)
        again = u.select_reference(spec(), metadata(*instruments), u.TurnoverPanel(numeraire='USDC', rows=tuple(rows)),
                                   None, CLOCK)
        assert again == expected
    with pytest.raises(ValueError):  # a changed rule versions the universe instead of reusing incumbents
        u.select_reference(spec(universe_id='okx-usdc-top3-v2'), md, tp, first, tomorrow)


def test_management_survives_reference_removal():
    md, tp = base_inputs()
    first = u.select_reference(spec(), md, tp, None, CLOCK)
    fallen = panel(*(days(f'{a}-USDC', v, base=D + DAY) for a, v in {**ASSETS, 'SOL': 1}.items()))
    later = u.select_reference(spec(), md, fallen, first, ClockView(decision_ms=CLOCK.decision_ms + DAY,
                                                                    receipt_ms=None))
    assert 'SOL' not in later.member_asset_ids
    bound = u.bind_consumer_universes(later, entry=('BTC',), management={'SOL': ('inventory', 'working_order')})
    assert 'SOL' in bound.management and 'SOL' in bound.observation
    assert bound.reference == later.member_asset_ids and bound.entry == ('BTC',)
    assert dict(bound.management_obligations)['SOL'] == ('inventory', 'working_order')
    with pytest.raises(ValueError):
        u.bind_consumer_universes(later, entry=('BTC',), management={'SOL': ()})


def test_cap_weights_feasible_for_five():
    md, tp = base_inputs()
    basket = spec(universe_id='okx-usdc-basket5', mode='basket_5', count=5,
                  basket_assets=('ADA', 'BTC', 'DOGE', 'ETH', 'SOL'), benchmarks=('BTC', 'ETH'))
    skewed = panel(*(days(f'{a}-USDC', v) for a, v in {**ASSETS, 'BTC': 10 ** 9}.items()))
    binding = u.select_reference(basket, md, skewed, None, CLOCK)
    assert binding.member_asset_ids == ('ADA', 'BTC', 'DOGE', 'ETH', 'SOL')
    assert binding.equal_weights == (Decimal('0.2'),) * 5
    assert binding.capped_weights == (Decimal('0.2'),) * 5 and sum(binding.capped_weights) == 1
    weights = u.capped_weights((Decimal(50), Decimal(30), Decimal(10), Decimal(10)) + (Decimal(1),) * 16,
                               Decimal('0.2'))
    assert max(weights) == Decimal('0.2') and abs(sum(weights) - 1) < Decimal('1e-20')
    assert weights[1] == Decimal('0.2') and weights[2] < Decimal('0.2')
    with pytest.raises(ContextError):
        u.capped_weights((Decimal(0),) * 5, Decimal('0.2'))
    with pytest.raises(ContextError):  # an infeasible cap is rejected, never silently relaxed
        u.capped_weights((Decimal(1),) * 3, Decimal('0.2'))
    with pytest.raises(ContextError) as error:  # a missing basket member makes the basket unavailable
        u.select_reference(basket.model_copy(update={'basket_assets': ('ADA', 'BTC', 'DOGE', 'ETH', 'WIF')}),
                           md, tp, None, CLOCK)
    assert error.value.reasons == ('MEMBERSHIP_UNAVAILABLE',)
    assert len(binding.membership_hash) == 64
    assert u.select_reference(basket, md, skewed, None, CLOCK).membership_hash == binding.membership_hash
