import unittest

import ab_digest_check as ab

NOW = 1_790_000_000.0


def pair(head=1_789_999_980, digest='d1', ready=True, age=20.0, rejected=None, window=60):
    return {'ready': ready, 'reason': '' if ready else 'starting', 'last_close_age_s': age if ready else None,
            'window': window, 'window_digest': digest if ready else None, 'window_head_open_time': head if ready else None,
            'rejected': rejected or {}}


def view(**pairs):
    return {'published_at': NOW - 3, 'pairs': pairs}


class Compare(unittest.TestCase):
    def codes(self, a, b):
        return sorted(f['code'] for f in ab.compare(a, b, NOW))

    def test_identical_instances_are_quiet(self):
        self.assertEqual(self.codes(view(BTC=pair()), view(BTC=pair())), [])

    def test_same_head_different_digest_is_critical(self):
        self.assertEqual(self.codes(view(BTC=pair()), view(BTC=pair(digest='d2'))), ['candle_pair_digest_mismatch'])

    def test_one_minute_rollover_lag_is_tolerated_but_two_bars_is_not(self):
        self.assertEqual(self.codes(view(BTC=pair()), view(BTC=pair(head=1_789_999_920, digest='x'))), [])
        self.assertEqual(self.codes(view(BTC=pair()), view(BTC=pair(head=1_789_999_860, digest='x'))), ['candle_pair_head_lag'])

    def test_degraded_unready_and_stale(self):
        self.assertEqual(self.codes(view(BTC=pair()), view(BTC=pair(ready=False))), ['candle_instance_degraded'])
        self.assertEqual(self.codes(view(BTC=pair(ready=False)), view(BTC=pair(ready=False))), ['candle_pair_unready'])
        self.assertEqual(self.codes(view(BTC=pair(age=300.0)), view(BTC=pair(age=300.0))), ['candle_pair_stale', 'candle_pair_stale'])

    def test_missing_or_old_status_file(self):
        self.assertEqual(self.codes(None, view(BTC=pair())), ['candle_instance_status_stale'])
        old = view(BTC=pair())
        old['published_at'] = NOW - 120
        self.assertEqual(self.codes(old, view(BTC=pair())), ['candle_instance_status_stale'])

    def test_rejections_are_a_warning(self):
        findings = ab.compare(view(BTC=pair(rejected={'forming_future': 9})), view(BTC=pair()), NOW)
        self.assertEqual([(f['code'], f['severity']) for f in findings], [('candle_rejections', 'warning')])


if __name__ == '__main__':
    unittest.main()
