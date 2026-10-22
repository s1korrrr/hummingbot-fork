import json
import os
import subprocess
import sys
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

from hummingbot.strategy_v2.utils import regime as R

ROOT = str(Path(__file__).resolve().parents[4])
START = 1_777_593_600  # 2026-05-01 00:00 UTC


def synthetic(n=4 * 1440, seed=7, start=START):
    """Deterministic 1m bars whose volatility alternates every 6 hours (quiet / volatile)."""
    rng = np.random.default_rng(seed)
    vol = np.where((np.arange(n) // 360) % 2 == 0, 0.0004, 0.0016)
    close = 600 * np.exp(np.cumsum(rng.normal(0, vol)))
    open_ = np.r_[close[0], close[:-1]]
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, vol / 2)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, vol / 2)))
    volume = rng.gamma(2.0, 50.0, n) * (vol / 0.0004)
    return pd.DataFrame({"timestamp": start + 60 * np.arange(n), "open": open_, "high": high, "low": low,
                         "close": close, "volume": volume})


def bars_of(frame):
    return R.Bars.from_frame(frame)


HTF = R.HtfVolSpec(vol_interval="15m", vol_window=8, vol_edges=(0.004, 0.007), trend_interval="1h", trend_window=6)
HMM = R.HmmSpec(n_states=3, fit_bars=720, refit_seconds=21600, warm_bars=120, seeds=(0, 1), n_iter=100, tol=1e-2,
                anchors=((0.0, -1.0, 0.0, -0.5), (0.0, 1.0, 0.0, 0.5), (0.0, 0.0, 0.0, 0.0)))


class SpecTest(unittest.TestCase):
    def test_specs_validate_and_round_trip(self):
        for spec in (HTF, R.HmmSpec(), HMM):
            again = R.spec_from_dict(json.loads(json.dumps(spec.to_dict())))
            self.assertEqual(again, spec)
            self.assertEqual(again.spec_hash, spec.spec_hash)
        self.assertNotEqual(HTF.spec_hash, replace(HTF, vol_edges=(0.004, 0.008)).spec_hash)
        with self.assertRaises(ValueError):
            R.HtfVolSpec(vol_edges=(0.006, 0.004))
        with self.assertRaisesRegex(ValueError, 'vol_edges are required'):
            R.HtfVolSpec()  # edges are data-derived choices: no calibrated default
        with self.assertRaises(ValueError):
            R.HtfVolSpec(vol_interval="3m")
        with self.assertRaises(ValueError):
            R.HmmSpec(n_states=3)  # six default anchors
        with self.assertRaises(ValueError):
            R.HmmSpec(refit_seconds=90)
        with self.assertRaises(ValueError):
            R.Unavailable("something_else")

    def test_bars_reject_unsorted_or_fractional_minutes(self):
        frame = synthetic(10)
        with self.assertRaises(ValueError):
            bars_of(frame.iloc[::-1])
        frame.loc[3, "timestamp"] += 1
        with self.assertRaises(ValueError):
            bars_of(frame)

    def test_import_does_not_load_hmmlearn_or_controllers(self):
        code = ("import sys; import hummingbot.strategy_v2.utils.regime; "
                "print(any(m.startswith(('hmmlearn', 'controllers')) for m in sys.modules))")
        env = {**os.environ, "PYTHONPATH": ROOT}
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=ROOT, check=True)
        self.assertEqual(out.stdout.strip(), "False")


class HtfVolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frame = synthetic()
        cls.bars = bars_of(cls.frame)
        cls.replay = R.replay(HTF, cls.bars)

    def test_labels_follow_the_volatility_phases(self):
        labels = self.replay.labels()
        self.assertEqual(set(labels[600:]) - {None}, {"vol_low", "vol_mid", "vol_high"})
        # a quiet phase ends at 06:00; its last rows see only quiet 15m bars
        self.assertEqual(labels[359], "vol_low")
        self.assertEqual(labels[719], "vol_high")
        self.assertEqual(self.replay.reasons()[0], "insufficient_history")

    def test_prefix_invariance_no_lookahead(self):
        for k in (500, 1441, 3000):
            prefix = R.replay(HTF, bars_of(self.frame.iloc[:k]))
            self.assertEqual(prefix.results, self.replay.results[:k])
        shocked = self.frame.copy()
        shocked.loc[2000:, ["open", "high", "low", "close"]] *= 3.0  # future bars change drastically
        again = R.replay(HTF, bars_of(shocked))
        self.assertEqual(again.results[:2001], self.replay.results[:2001])
        self.assertNotEqual(again.results[2100:], self.replay.results[2100:])

    def test_evaluate_equals_replay_and_is_restart_independent(self):
        for i in (130, 900, 1441, 2222, 3777, len(self.frame) - 1):
            now = int(self.frame.timestamp[i])
            live = R.evaluate(HTF, self.bars, now)
            self.assertEqual(live, self.replay.results[i])
            # a process that started 9 hours before ``now`` holds only that history
            recent = bars_of(self.frame[(self.frame.timestamp >= now - 9 * 3600) & (self.frame.timestamp < now)])
            self.assertEqual(R.evaluate(HTF, recent, now), live)
            # an in-progress bar opening at ``now`` is ignored, whatever it contains
            partial = self.frame[self.frame.timestamp <= now].copy()
            partial.loc[partial.index[-1], ["open", "high", "low", "close"]] = [1.0, 1e6, 0.5, 2.0]
            self.assertEqual(R.evaluate(HTF, bars_of(partial), now), live)

    def test_mid_bucket_evaluation_uses_the_last_completed_htf_bar(self):
        for offset in range(0, 900, 60):
            now = START + 1000 * 60 + offset
            state = R.evaluate(HTF, self.bars, now)
            self.assertEqual(state.source_bar, (now // 900) * 900 - 900)

    def test_aggregation_matches_canonical_htf_candles(self):
        from hummingbot.strategy_v2.components.candle_aggregation import derive_higher_timeframe_candles

        now = int(self.frame.timestamp[2500])
        state = R.evaluate(HTF, self.bars, now)
        htf = derive_higher_timeframe_candles(self.frame, base_interval="1m", target_interval="15m",
                                              current_time=now, max_records=HTF.vol_window + 1)
        self.assertEqual(int(htf.timestamp.iloc[-1]), state.source_bar)
        previous = htf.close.to_numpy()[:-1]
        high, low = htf.high.to_numpy()[1:], htf.low.to_numpy()[1:]
        true_range = np.maximum(high - low, np.maximum(np.abs(high - previous), np.abs(low - previous)))
        self.assertEqual(true_range.mean() / htf.close.iloc[-1], state.vol_atr_fraction)

    def test_bucket_edges_are_right_closed(self):
        state = R.evaluate(HTF, self.bars, int(self.frame.timestamp[2500]))
        exact = replace(HTF, vol_edges=(state.vol_atr_fraction, state.vol_atr_fraction * 2))
        self.assertEqual(R.evaluate(exact, self.bars, state.timestamp).label, "vol_mid")

    def test_trend_joins_the_label_only_when_configured(self):
        spec = replace(HTF, trend_in_label=True)
        state = R.evaluate(spec, self.bars, int(self.frame.timestamp[3000]))
        self.assertRegex(state.label, r"^vol_(low|mid|high)/trend_(down|flat|up)$")
        self.assertIn(R.evaluate(HTF, self.bars, int(self.frame.timestamp[3000])).label, R.VOL_LABELS)

    def test_failure_modes_are_explicit_and_bounded(self):
        now = int(self.frame.timestamp[3000])
        broken = self.frame.copy()
        broken.loc[2990, "close"] = np.nan
        self.assertEqual(R.evaluate(HTF, bars_of(broken), now).reason, "invalid_bar")
        # the bad bar leaves the 9-bar 15m window after about two and a quarter hours
        self.assertIsInstance(R.evaluate(HTF, bars_of(broken), now + 150 * 60), R.RegimeState)
        missing = bars_of(self.frame.drop(index=2980))
        self.assertEqual(R.evaluate(HTF, missing, now).reason, "gap")
        self.assertEqual(R.evaluate(HTF, bars_of(self.frame.iloc[:100]), int(self.frame.timestamp[100])).reason,
                         "insufficient_history")
        flat = self.frame.copy()
        flat[["open", "high", "low", "close"]] = 600.0
        self.assertEqual(R.evaluate(HTF, bars_of(flat), now).reason, "flat_range")
        inconsistent = self.frame.copy()
        inconsistent.loc[2995, "high"] = inconsistent.loc[2995, "low"] * 0.5
        self.assertEqual(R.evaluate(HTF, bars_of(inconsistent), now).reason, "invalid_bar")

    def test_replay_is_deterministic(self):
        self.assertEqual(R.replay(HTF, self.bars).digest(), self.replay.digest())


class HmmTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
        cls.frame = synthetic(n=2 * 1440)
        cls.bars = bars_of(cls.frame)
        cls.boundary = START + 64800  # first grid boundary with 720 fit bars plus the feature lookback
        cls.model = R.fit(HMM, cls.bars, cls.boundary)

    def test_fit_is_deterministic_and_serializable(self):
        self.assertIsInstance(self.model, R.RegimeModel)
        again = R.fit(HMM, self.bars, self.boundary)
        self.assertEqual(again, self.model)
        self.assertEqual(again.model_hash, self.model.model_hash)
        restored = R.RegimeModel.from_json(self.model.to_json())
        self.assertEqual(restored, self.model)
        self.assertEqual(restored.model_hash, self.model.model_hash)
        with self.assertRaises(ValueError):
            R.RegimeModel.from_json(self.model.to_json().replace('"seed"', '"seed_"'))

    def test_fit_uses_only_bars_before_the_boundary(self):
        shocked = self.frame.copy()
        after = shocked.timestamp >= self.boundary
        shocked.loc[after, ["open", "high", "low", "close"]] *= 2.0
        shocked.loc[after, "volume"] = 0.0
        self.assertEqual(R.fit(HMM, bars_of(shocked), self.boundary), self.model)

    def test_labels_are_anchored_not_permutation_dependent(self):
        means = np.asarray(self.model.means)
        order = R.canonical_order(means, HMM.anchors)
        np.testing.assert_array_equal(order, np.arange(HMM.n_states))  # fitted model is already canonical
        for permutation in ([1, 2, 0], [2, 0, 1], [0, 2, 1]):
            shuffled = means[permutation]
            reordered = shuffled[R.canonical_order(shuffled, HMM.anchors)]
            np.testing.assert_array_equal(reordered, means)

    def test_label_anchoring_is_stable_across_refits(self):
        # Slot identity comes from fixed spec anchors: the quiet-range anchor keeps the quiet
        # state and the volatile-range anchor the volatile one on every refit of the series.
        for boundary in range(self.boundary, START + 2 * 86400, HMM.refit_seconds):
            model = R.fit(HMM, self.bars, boundary)
            self.assertIsInstance(model, R.RegimeModel)
            self.assertTrue(model.labels[0].startswith("LV_"), (boundary, model.labels))
            self.assertTrue(model.labels[1].startswith("HV_"), (boundary, model.labels))
            atr = np.asarray(model.means)[:, 1]
            self.assertLess(atr[0], 0.0)
            self.assertGreater(atr[1], 0.0)

    def test_evaluate_equals_replay_and_is_restart_independent(self):
        replayed = R.replay(HMM, self.bars)
        stamps = self.frame.timestamp.to_numpy()
        for i in (1080, 1081, 1300, 1439, 1440, 2000, len(stamps) - 1):
            now = int(stamps[i])
            boundary = R.refit_boundary(HMM, now)
            cold = R.evaluate(HMM, self.bars, now)  # restart: refits the same window deterministically
            model = dict(replayed.models)[boundary]
            warm = R.evaluate(HMM, self.bars, now, model=R.RegimeModel.from_json(model.to_json()))
            self.assertEqual(cold, warm)
            self.assertEqual(cold, replayed.results[i])
            history = boundary - (HMM.fit_bars + HMM.lookback) * 60
            trimmed = bars_of(self.frame[(self.frame.timestamp >= history) & (self.frame.timestamp < now)])
            self.assertEqual(R.evaluate(HMM, trimmed, now), cold)

    def test_prefix_invariance_no_lookahead(self):
        full = R.replay(HMM, self.bars)
        for k in (1200, 1700):
            prefix = R.replay(HMM, bars_of(self.frame.iloc[:k]))
            self.assertEqual(prefix.results, full.results[:k])
        self.assertTrue(all(isinstance(r, R.Unavailable) and r.reason == "insufficient_history"
                            for r in full.results[:1080]))
        self.assertIsInstance(full.results[1080], R.RegimeState)

    def test_model_for_another_boundary_is_rejected(self):
        now = self.boundary + 21600 + 600
        result = R.evaluate(HMM, self.bars, now, model=self.model)
        self.assertEqual(result.reason, "model_mismatch")

    def test_nan_bar_is_unavailable_only_while_it_is_in_the_feature_window(self):
        broken = self.frame.copy()
        row = int(np.searchsorted(broken.timestamp, self.boundary + 3600))
        broken.loc[row, "close"] = np.nan
        bars = bars_of(broken)
        after = int(broken.timestamp[row]) + 60
        self.assertEqual(R.evaluate(HMM, bars, after, model=self.model).reason, "invalid_bar")
        self.assertEqual(R.evaluate(HMM, bars, after + HMM.lookback * 60, model=self.model).reason, "invalid_bar")
        self.assertIsInstance(R.evaluate(HMM, bars, after + (HMM.lookback + 1) * 60, model=self.model),
                              R.RegimeState)

    def test_fit_failure_modes(self):
        short = bars_of(self.frame.iloc[:300])
        self.assertEqual(R.fit(HMM, short, self.boundary).reason, "insufficient_history")
        flat = self.frame.copy()
        flat["volume"] = 0.0
        self.assertEqual(R.fit(HMM, bars_of(flat), self.boundary).reason, "flat_volume")
        holes = self.frame.copy()
        holes.loc[500:530, "close"] = np.nan  # inside [boundary - 720 min, boundary)
        self.assertEqual(R.fit(HMM, bars_of(holes), self.boundary).reason, "invalid_bar")
        capped = replace(HMM, n_iter=3, tol=0.0)
        self.assertEqual(R.fit(capped, self.bars, self.boundary).reason, "nonconverged_fit")
        gated = replace(HMM, min_occupancy=0.99)
        self.assertEqual(R.fit(gated, self.bars, self.boundary).reason, "label_gate")
        with self.assertRaises(ValueError):
            R.fit(HMM, self.bars, self.boundary + 60)

    def test_filtered_posterior_is_normalized(self):
        state = R.evaluate(HMM, self.bars, self.boundary + 1800, model=self.model)
        self.assertAlmostEqual(sum(mass for _, mass in state.probabilities), 1.0, places=12)
        self.assertEqual(state.confidence, dict(state.probabilities)[state.label])


if __name__ == "__main__":
    unittest.main()
