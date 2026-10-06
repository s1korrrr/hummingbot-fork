"""Tidewater V4.1 spacing: hourly ATR from closed 1m bars (M2) and price-zone memory (M4)."""
from decimal import Decimal as D

import numpy as np
import pytest

from controllers.generic.modular_spot_core.domain import DataUnavailable
from controllers.generic.tidewater_v4_core.spacing import (
    SpacingConfig,
    hourly_atr_fraction,
    spacing_decision,
    spacing_zones,
)

T0 = 1_777_593_600  # 2026-05-01T00:00Z, a UTC hour boundary


def minutes(hours, *, start=T0, extra=0, high_step=None):
    """`hours` full UTC hours of 1m bars plus `extra` minutes; hour h trades flat at 100 + h with a
    high of +1 and a low of -1 inside the hour (true range 2 + gap to the previous close)."""
    n = hours * 60 + extra
    t = start + 60 * np.arange(n)
    hour = (t - start) // 3600
    close = 100.0 + hour
    high, low = close.copy(), close.copy()
    for h in range(hours + 1):
        rows = np.flatnonzero(hour == h)
        if len(rows):
            high[rows[len(rows) // 2]] = 100.0 + h + 1
            low[rows[len(rows) // 3]] = 100.0 + h - 1
    return t, high, low, close


def test_atr_is_the_simple_mean_of_the_last_14_completed_hour_true_ranges():
    t, h, lo, c = minutes(16)
    # Each hour: high = c + 1, low = c - 1, previous close = c - 1 -> TR = max(2, 2, 0) = 2.
    atr = hourly_atr_fraction(t, h, lo, c, cutoff_open=int(t[-1]))
    assert atr == D(2) / D('115')  # last completed hour (h = 15) closes at 115


def test_only_hours_ended_by_the_cutoff_close_count():
    t, h, lo, c = minutes(16, extra=30)  # 30 minutes of an unfinished 17th hour
    full = hourly_atr_fraction(t[:16 * 60], h[:16 * 60], lo[:16 * 60], c[:16 * 60], cutoff_open=int(t[16 * 60 - 1]))
    assert hourly_atr_fraction(t, h, lo, c, cutoff_open=int(t[-1])) == full


def test_bars_after_the_native_cutoff_are_ignored():
    t, h, lo, c = minutes(17)
    cut = int(t[16 * 60 - 1])  # the last closed minute of hour 15
    h2 = h.copy()
    h2[16 * 60:] = 10_000.0  # a later hour that must not be seen
    assert hourly_atr_fraction(t, h2, lo, c, cutoff_open=cut) == hourly_atr_fraction(
        t[:16 * 60], h[:16 * 60], lo[:16 * 60], c[:16 * 60], cutoff_open=cut)


def test_the_frame_must_contain_the_native_last_closed_minute():
    t, h, lo, c = minutes(16)
    with pytest.raises(DataUnavailable, match='last closed minute'):
        hourly_atr_fraction(t[:-1], h[:-1], lo[:-1], c[:-1], cutoff_open=int(t[-1]))


def test_fifteen_completed_hours_are_required():
    t, h, lo, c = minutes(14, extra=59)
    with pytest.raises(DataUnavailable, match='15 completed hours'):
        hourly_atr_fraction(t, h, lo, c, cutoff_open=int(t[-1]))


def test_an_hour_the_frame_starts_inside_is_not_complete():
    t, h, lo, c = minutes(16)
    # Drop the first 10 minutes: hour 0 is partial, so only 15 complete hours remain.
    atr = hourly_atr_fraction(t[10:], h[10:], lo[10:], c[10:], cutoff_open=int(t[-1]))
    assert atr == D(2) / D('115')
    with pytest.raises(DataUnavailable, match='15 completed hours'):
        hourly_atr_fraction(t[70:], h[70:], lo[70:], c[70:], cutoff_open=int(t[-1]))


def test_a_missing_hour_breaks_the_true_range_chain():
    t, h, lo, c = minutes(16)
    keep = (t - T0) // 3600 != 10
    with pytest.raises(DataUnavailable, match='consecutive'):
        hourly_atr_fraction(t[keep], h[keep], lo[keep], c[keep], cutoff_open=int(t[-1]))


def test_gaps_between_hours_enter_the_true_range():
    t, h, lo, c = minutes(16)
    jump = (t - T0) // 3600 == 15
    h, lo, c = h + jump * 10, lo + jump * 10, c + jump * 10  # hour 15 opens 10 above hour 14's close
    atr = hourly_atr_fraction(t, h, lo, c, cutoff_open=int(t[-1]))
    # Hour 15: high 126, low 124, previous close 114 -> TR 12; the other 13 hours have TR 2.
    assert atr == (D(13 * 2) + D(12)) / D(14) / D('125')


def test_the_gap_is_the_configured_multiple_of_the_hourly_atr():
    cfg = SpacingConfig(atr_multiplier='2', zones='all')
    assert cfg.gap(D('0.005')) == D('0.010')


def test_the_first_tranche_is_never_spaced():
    cfg = SpacingConfig(atr_multiplier='2', zones='all')
    decision = spacing_decision(cfg, bid=D('100'), zones=(), gap=D('0.01'))
    assert decision.allowed and decision.reason == 'spacing_first'


def test_a_buy_must_be_a_full_gap_from_every_open_zone_in_either_direction():
    cfg = SpacingConfig(atr_multiplier='2', zones='all')
    zones = (D('100'), D('95'))
    near = spacing_decision(cfg, bid=D('95.5'), zones=zones, gap=D('0.01'))  # 0.53% above the 95 zone
    assert not near.allowed and near.reason == 'spacing_gap' and near.nearest == D('95')
    assert spacing_decision(cfg, bid=D('97.5'), zones=zones, gap=D('0.01')).allowed
    assert spacing_decision(cfg, bid=D('94.05'), zones=zones, gap=D('0.01')).allowed  # exactly 1% below 95
    assert not spacing_decision(cfg, bid=D('100.5'), zones=zones, gap=D('0.01')).allowed  # upward too


def test_latest_mode_spaces_from_the_latest_zone_only():
    latest = SpacingConfig(atr_multiplier='2', zones='latest')
    every = SpacingConfig(atr_multiplier='2', zones='all')
    tranches, bag = (D('100'), D('95')), None  # 95 is the latest acquired tranche
    assert spacing_zones(latest, tranches=tranches, bag=bag) == (D('95'),)
    assert spacing_zones(every, tranches=tranches, bag=bag) == (D('100'), D('95'))
    # Without active tranches the non-dust bag breakeven is the latest zone.
    assert spacing_zones(latest, tranches=(), bag=D('120')) == (D('120'),)
    assert spacing_zones(every, tranches=tranches, bag=D('120')) == (D('100'), D('95'), D('120'))
    # The bid at 100.6 is a gap from 95 but not from 100: only the all-zones rule refuses it.
    assert spacing_decision(latest, bid=D('100.6'), zones=spacing_zones(latest, tranches=tranches, bag=bag),
                            gap=D('0.01')).allowed
    assert not spacing_decision(every, bid=D('100.6'), zones=spacing_zones(every, tranches=tranches, bag=bag),
                                gap=D('0.01')).allowed


@pytest.mark.parametrize('change', [{'atr_multiplier': '0'}, {'zones': 'nearest'}, {'atr_hours': 0}])
def test_invalid_spacing_configurations_are_rejected(change):
    with pytest.raises(ValueError):
        SpacingConfig(**{'atr_multiplier': '2', 'zones': 'all', **change})
