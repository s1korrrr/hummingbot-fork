"""Tidewater protected runner policy: parity with the research reference, hummingbot a42150506.

`tidewater_v4_core/runner.py` is Tidewater's single source of the policy. Each definition's source
text is pinned to a42150506 (`modular_spot_core/bag.py`, computed from `git show a42150506`), and the
reference commit's own test vectors (`test/modular_spot/test_runner.py`) are replayed here unchanged
against Tidewater's module. A divergent copy fails both.
"""
import ast
import hashlib
from decimal import Decimal as D
from pathlib import Path

import numpy as np
import pytest

from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.tidewater_v4_core import runner as module
from controllers.generic.tidewater_v4_core.runner import (
    RunnerLot,
    runner_atr_pct,
    runner_bar,
    runner_config,
    runner_cost_floor,
    runner_daily_bars,
    runner_exit_fill,
    runner_floor,
    runner_limit_fill,
    runner_split,
)

# sha256 of each definition's source text (decorators included) in a42150506's modular_spot_core/bag.py.
A42150506 = {  # noqa: documentation
    'RunnerConfig': '12add9708a265a885b1303f39a7293e98176fab341c52d75b98df675a5cee1db',  # noqa: documentation
    'RUNNER_FIELDS': '3a833b2727c433a5d607de9bf8cff5719efb6892ae225c4882d1c3e9ba29bff9',  # noqa: documentation
    'runner_config': '8daefd166d4b80e09fcd499fdd575f0a291b9872939865a32937028f2f330336',  # noqa: documentation
    'RunnerSplit': 'bb0f38eab1cc4797cc8a93121d588cfa0c8c3386fbd661b2a47dd2e0e8edbe39',  # noqa: documentation
    'runner_split': '781460f29b6e9150be70ce94adea6046e12a92d7430fd6d98c3889e06885837a',  # noqa: documentation
    'runner_cost_floor': '7d67fa3e71cdf5f4b18f375f4e03e78600117e1a8439375be49c3edf4448f72d',  # noqa: documentation
    'runner_floor': '5e5aa8aaf0cbcd40b8b45e2cf1d144164512347cdf88eb3c15deb4c776101c69',  # noqa: documentation
    'DailyBar': 'ba63fc64e23b6a1f7439dc2e310a9336bf01de2053318f25ff76bef8e7cb1ed0',  # noqa: documentation
    'DAY_SECONDS': '5ebd3443fdad2c73f7bdf47b9ccfcd597dccb2004fef406e583dd9a165a8016e',  # noqa: documentation
    'runner_daily_bars': '397fa8dec07225f4f453c4b79764a1000b070ac642d507fdaee6bf1988d46316',  # noqa: documentation
    'runner_atr_pct': '77e29c4bf5d5298601b035f929a2f9ec76217b081135f071657c14c3d84e367c',  # noqa: documentation
    'runner_exit_fill': '091aeffd6276f4ac15ff1fe8faf8415a524c10d312cacdc0d51759ffd7ed6be8',  # noqa: documentation
    'runner_limit_fill': '7e252d514460f6864b5364a2a91ff9f55a0f96067bf7a0c806a22a3ff57e2603',  # noqa: documentation
    'RunnerLot': '075b55e2180aaf501c8f9611d834691090b649d6c15d45477dd7fec1047b90e6',  # noqa: documentation
    'RunnerStep': 'cc19ed87c75bacdce6d393d02a98e026600547f3d907dfbde2726e7346983135',  # noqa: documentation
    'runner_bar': '8e9661c48bd13d7895aabcfeff45d2286c77ba8c7d1eaa0842bec41d7c8bef42',  # noqa: documentation
}


def _definitions(path):
    source = Path(path).read_text()
    found = {}
    for node in ast.parse(source).body:
        name = getattr(node, 'name', None)
        if name is None and isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
        if name is None:
            continue
        parts = [ast.get_source_segment(source, d) for d in getattr(node, 'decorator_list', [])]
        parts.append(ast.get_source_segment(source, node))
        found[name] = hashlib.sha256('\n'.join(parts).encode()).hexdigest()
    return found


def test_every_policy_definition_is_the_reference_text():
    found = _definitions(module.__file__)
    assert {name: found.get(name) for name in A42150506} == A42150506


def test_the_module_defines_nothing_beyond_the_reference_policy():
    found = _definitions(module.__file__)
    assert set(found) == set(A42150506)


# ---------------------------------------------------------------------- a42150506 test vectors, unchanged
PRESET = {"runner_fraction": "0.5", "floor_margin": "0.01", "atr_multiplier": "2", "atr_days": "14"}


def test_runner_config_accepts_the_preset_and_refuses_out_of_range_settings():
    cfg = runner_config(PRESET)
    assert (cfg.runner_fraction, cfg.floor_margin, cfg.atr_multiplier, cfg.atr_days) == (D("0.5"), D("0.01"), D("2"), 14)
    assert runner_config({**PRESET, "floor_margin": "0", "atr_days": "2"}).atr_days == 2
    for field, value in (("runner_fraction", "0"), ("runner_fraction", "1"), ("floor_margin", "-0.001"),
                         ("atr_multiplier", "0"), ("atr_days", "1"), ("atr_days", "14.5"), ("atr_days", "x"),
                         ("runner_fraction", "nan")):
        with pytest.raises((ValueError, PolicyError)):
            runner_config({**PRESET, field: value})
    for bad in ({k: v for k, v in PRESET.items() if k != "atr_days"}, {**PRESET, "extra": "1"}):
        with pytest.raises((ValueError, PolicyError)):
            runner_config(bad)


def test_split_quantizes_the_runner_down_and_sells_the_rest():
    cfg = runner_config(PRESET)
    split = runner_split(cfg, amount=D("1.393"), price=D("98"), amount_step=D(".001"), minimum_notional=D("1"))
    assert (split.runner_base, split.sold_base, split.skipped) == (D("0.696"), D("0.697"), None)
    assert split.runner_base + split.sold_base == D("1.393")


def test_split_is_skipped_when_either_part_is_below_the_minimum_notional():
    cfg = runner_config(PRESET)
    tiny = runner_config({**PRESET, "runner_fraction": "0.001"})
    low = runner_split(tiny, amount=D("1.393"), price=D("98"), amount_step=D(".001"), minimum_notional=D("1"))
    assert (low.runner_base, low.sold_base, low.skipped) == (D("0"), D("1.393"), "runner_below_min_notional")
    zero = runner_split(tiny, amount=D("0.5"), price=D("98"), amount_step=D(".001"), minimum_notional=D("1"))
    assert zero.skipped == "runner_below_min_notional" and zero.sold_base == D("0.5")  # quantized to nothing
    heavy = runner_config({**PRESET, "runner_fraction": "0.6"})
    sold = runner_split(heavy, amount=D("0.02"), price=D("100"), amount_step=D(".001"), minimum_notional=D("1"))
    assert (sold.runner_base, sold.sold_base, sold.skipped) == (D("0"), D("0.02"), "sold_below_min_notional")
    exact = runner_split(cfg, amount=D("0.02"), price=D("100"), amount_step=D(".001"), minimum_notional=D("1"))
    assert (exact.runner_base, exact.sold_base, exact.skipped) == (D("0.010"), D("0.010"), None)  # 1.0 >= 1


def test_cost_floor_covers_both_sides_and_the_margin():
    cfg = runner_config(PRESET)
    assert runner_cost_floor(cfg, cost_basis=D("100"), fee_fraction=D("0.003")) == D("101.3")
    with pytest.raises(PolicyError):
        runner_cost_floor(cfg, cost_basis=D("0"), fee_fraction=D("0.003"))


def test_floor_is_monotone_and_never_below_the_cost_floor():
    cfg = runner_config(PRESET)
    cost = D("101.3")
    assert runner_floor(cfg, previous_floor=cost, cost_floor=cost, high_water=D("110"), atr_pct=None) == cost
    assert runner_floor(cfg, previous_floor=cost, cost_floor=cost, high_water=D("110"), atr_pct=D("0.02")) == D("105.6")
    # A wider ATR or a lower high never lowers a floor already reached.
    assert runner_floor(cfg, previous_floor=D("105.6"), cost_floor=cost, high_water=D("110"), atr_pct=D("0.05")) == D("105.6")
    # A trail below cost (huge ATR: negative trail) is lifted to the cost floor.
    assert runner_floor(cfg, previous_floor=cost, cost_floor=cost, high_water=D("110"), atr_pct=D("0.6")) == cost


def _minutes(days, start=0, drift=0.0):
    stamps = np.arange(start, start + days * 1440 * 60, 60)
    i = np.arange(len(stamps))
    close = 100 + drift * i / 1440 + np.sin(i / 7.0)
    return stamps, close + 0.5, close - 0.5, close


def test_daily_bars_are_whole_utc_days_only():
    stamps, highs, lows, closes = _minutes(3, start=86400 - 600)  # ten minutes of a partial first day
    days = runner_daily_bars(stamps, highs, lows, closes)
    assert [d.start for d in days] == [86400, 2 * 86400]  # the partial first and last days are not bars
    first = days[0]
    inside = (stamps >= 86400) & (stamps < 2 * 86400)
    assert first.high == D(str(float(highs[inside].max()))) and first.low == D(str(float(lows[inside].min())))
    assert first.close == D(str(float(closes[inside][-1])))
    assert first.previous_close == D(str(float(closes[stamps == 86400 - 60][0])))  # the prior minute's close
    lone = runner_daily_bars(*_minutes(1, start=5 * 86400))
    assert len(lone) == 1 and lone[0].previous_close is None


def test_daily_atr_is_causal_and_needs_atr_days_completed_days():
    stamps, highs, lows, closes = _minutes(4, start=86400, drift=3.0)
    days = runner_daily_bars(stamps, highs, lows, closes)
    assert runner_atr_pct(days, now=3 * 86400 - 60, atr_days=2) is None  # day 2 (86400*2..) still forming
    at_day3 = runner_atr_pct(days, now=3 * 86400, atr_days=2)
    true_range = [days[0].high - days[0].low,
                  max(days[1].high - days[1].low, abs(days[1].high - days[0].close), abs(days[1].low - days[0].close))]
    assert at_day3 == (true_range[0] + true_range[1]) / 2 / days[1].close
    # No current-day leakage: rewriting the forming day's bars changes no value seen inside that day.
    spiked = highs.copy()
    spiked[(stamps >= 3 * 86400) & (stamps < 4 * 86400)] += 50
    leaked = runner_daily_bars(stamps, spiked, lows, closes)
    for now in (3 * 86400, 3 * 86400 + 60, 4 * 86400 - 60):
        assert runner_atr_pct(leaked, now=now, atr_days=2) == at_day3
    assert runner_atr_pct(leaked, now=4 * 86400, atr_days=2) != runner_atr_pct(days, now=4 * 86400, atr_days=2)


def test_exit_fill_is_the_next_close_unless_that_would_realise_a_loss():
    assert runner_exit_fill(next_close=D("103"), cost_floor=D("101.3")) == ("MARKET", D("103"))
    assert runner_exit_fill(next_close=D("101.3"), cost_floor=D("101.3")) == ("MARKET", D("101.3"))
    assert runner_exit_fill(next_close=D("100.9"), cost_floor=D("101.3")) == ("LIMIT", D("101.3"))


def test_resting_limit_fills_at_its_price_on_the_first_touching_path_point():
    assert runner_limit_fill(D("101.3"), (D("100"), D("99"), D("101.2"), D("101"))) is None
    assert runner_limit_fill(D("101.3"), (D("100"), D("101.3"), D("103"), D("102"))) == (1, D("101.3"))
    assert runner_limit_fill(D("101.3"), (D("104"), D("99"), D("105"), D("100"))) == (0, D("101.3"))  # gap up: limit price


def test_bar_step_checks_the_floor_before_updating_the_high_water():
    cfg = runner_config(PRESET)
    lot = RunnerLot(base=D("1"), cost_basis=D("100"), cost_floor=D("101.3"), high_water=D("110"), floor=D("101.3"))
    step = runner_bar(cfg, lot, high=D("130"), close=D("105.5"), atr_pct=D("0.02"))
    # floor from the high water before this bar: 110 x 0.96 = 105.6 >= close 105.5, so it exits; the bar's 130 high
    # (reached after the check) cannot lift the floor first.
    assert step.exit and step.lot.floor == D("105.6") and step.lot.high_water == D("110") and step.floor_changed
    hold = runner_bar(cfg, lot, high=D("130"), close=D("106"), atr_pct=D("0.02"))
    assert not hold.exit and hold.lot.floor == D("105.6") and hold.lot.high_water == D("130")
    later = runner_bar(cfg, hold.lot, high=D("125"), close=D("124.9"), atr_pct=D("0.02"))
    assert later.lot.floor == D("124.8") and not later.exit and later.lot.high_water == D("130")
    same = runner_bar(cfg, later.lot, high=D("120"), close=D("125"), atr_pct=D("0.02"))
    assert not same.floor_changed and same.lot.floor == D("124.8")
