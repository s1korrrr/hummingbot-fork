"""Synthetic path semantics only; no economics or executor lifecycle claims."""
import dataclasses
import importlib
import math
import unittest
from pathlib import Path


class RecoveryPathTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[3]
        if not (root / 'controllers/generic/nre_v2_core/recovery_path.py').is_file():
            self.fail('isolated v2 recovery-path implementation is absent')
        self.m = importlib.import_module('controllers.generic.nre_v2_core.recovery_path')
        self.policy = self.m.Policy(2, 1.0, 0.5, 20)

    def bar(self, i=0, close=100.0, low=None, high=None, delay=7):
        return self.m.Candle((i + 1) * 60000, (i + 2) * 60000 + delay,
                             close, close if high is None else high, close if low is None else low, close)

    def seed(self, bar=None, policy=None, scale=2.0):
        b = bar or self.bar()
        return self.m.start('setup-1', 'okx:ETH-USDC', scale, b, b.available_at_ms, policy or self.policy)

    def step(self, s, b):
        return self.m.step(s, b, b.available_at_ms)

    def test_warmup_and_extension_are_not_imputed(self):
        s = self.seed()
        self.assertIsNone(self.m.view(s, s.last.available_at_ms)['extension_atr'])
        s = self.step(self.step(s, self.bar(1, 98)), self.bar(2, 99))
        v = self.m.view(s, s.last.available_at_ms)
        self.assertEqual(v['extension_atr'], 1)
        self.assertEqual(v['rebound_atr'], 0.5)
        self.assertIsNone(v['retention'])

    def test_millisecond_confirmation_and_close_only_peak(self):
        s = self.step(self.seed(), self.bar(1, 102, low=99, high=150, delay=7))
        v = self.m.view(s, 180007)
        self.assertEqual(v['confirmed_at_ms'], 180007)
        self.assertEqual(v['bar_close_ms'], 180000)
        self.assertEqual(v['peak_close'], 102)
        s = self.step(s, self.bar(2, 101, low=100, high=200))
        self.assertAlmostEqual(self.m.view(s, 240007)['retention'], 2 / 3)

    def test_signed_retest_and_terminal_failure_do_not_rearm(self):
        s = self.step(self.seed(), self.bar(1, 102))
        s = self.step(s, self.bar(2, 99.5))
        self.assertEqual(self.m.view(s, 240007)['retention'], -0.25)
        s = self.step(s, self.bar(3, 110, low=99))
        self.assertEqual(s.status, 'FAILED')
        self.assertFalse(self.m.view(s, 300007)['feature_valid'])
        with self.assertRaisesRegex(ValueError, 'terminal'):
            self.step(s, self.bar(4, 120))

    def test_failure_precedes_expiry_and_expiry_does_not_confirm(self):
        p = dataclasses.replace(self.policy, max_bars=3)
        s = self.step(self.seed(policy=p), self.bar(1, 102))
        s = self.step(s, self.bar(2, 110, low=99))
        self.assertEqual(s.status, 'FAILED')
        s = self.step(self.step(self.seed(policy=p), self.bar(1)), self.bar(2, 105))
        self.assertEqual(s.status, 'EXPIRED')
        self.assertIsNone(s.confirmed_at_ms)

    def test_duplicate_revision_gap_and_clock_boundaries(self):
        s = self.seed()
        self.assertIs(self.step(s, s.last), s)
        for b in (self.bar(0, 101), self.bar(2)):
            with self.assertRaises(ValueError):
                self.step(s, b)
        for now in (120006, 180000, True, 120007.0):
            with self.assertRaises(ValueError):
                self.m.view(s, now)
        self.assertEqual(s.count, 1)

    def test_prices_policy_and_unrepresentable_thresholds_reject(self):
        for scale in (0, -1, True, math.inf, math.nan, '2', 1e-320):
            with self.subTest(scale=scale), self.assertRaises(ValueError):
                self.seed(scale=scale)
        for changes in ({'timestamp_ms': 61000}, {'available_at_ms': True}, {'high': math.inf}, {'open': True}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.seed(dataclasses.replace(self.bar(), **changes))
        with self.assertRaises(ValueError):
            self.m.Policy(True, 1, 0.5, 20)
        with self.assertRaises(ValueError):
            self.seed(self.bar(close=1e308, low=1), scale=1e-100)

    def test_state_immutable_prefix_replay_and_bounded_history(self):
        p = dataclasses.replace(self.policy, max_bars=200, rebound_atr=100)
        bars = [self.bar(i, 100 + i % 3 / 10) for i in range(80)]
        def replay(tape, scale=2):
            s = self.seed(tape[0], p, scale)
            rows = [self.m.view(s, s.last.available_at_ms)]
            for b in tape[1:]:
                s = self.step(s, b)
                self.assertLessEqual(len(s.lows), 3)
                rows.append(self.m.view(s, s.last.available_at_ms))
            return rows
        all_rows = replay(bars)
        self.assertEqual(all_rows[:13], replay(bars[:13]))
        scaled = [dataclasses.replace(b, **{k: getattr(b, k) * 10 for k in ('open', 'high', 'low', 'close')}) for b in bars]
        for a, b in zip(all_rows, replay(scaled, 20)):
            self.assertAlmostEqual(a['rebound_atr'], b['rebound_atr'])
        with self.assertRaises(dataclasses.FrozenInstanceError):
            self.seed().scale = 3


if __name__ == '__main__':
    unittest.main()
