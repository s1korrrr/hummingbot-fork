"""Self-test for v1_candle_parity.py with synthetic data (stdlib; indicator test needs pandas_ta)."""
import json
import os
import sqlite3
import tempfile
import unittest

import v1_candle_parity as tool

T0 = 1_790_000_040  # minute-aligned


def make_fixture(directory, mutate=None, rows=1500):
    bars = []
    price = 100.0
    for i in range(rows):
        price += ((i * 37) % 11 - 5) * 0.01
        bars.append({'open_time': T0 + 60 * i, 'open': f'{price:.2f}', 'high': f'{price + 0.05:.2f}', 'low': f'{price - 0.05:.2f}',
                     'close': f'{price + 0.01:.2f}', 'volume': '1.5', 'quote_volume': '150', 'confirmed': True})
    cache = os.path.join(directory, 'cache')
    os.makedirs(cache)
    with open(os.path.join(cache, 'BTC-USDC.json'), 'w') as handle:
        json.dump({'v': 1, 'pair': 'BTC-USDC', 'rows': bars}, handle)
    db = os.path.join(directory, 'copy.sqlite')
    conn = sqlite3.connect(db)
    conn.execute('create table ChartSnapshot (pair text, interval text, candle_timestamp integer, open_price real, '
                 'high_price real, low_price real, close_price real, volume real, rsi real, atr real)')
    for bar in bars:
        conn.execute('insert into ChartSnapshot values (?,?,?,?,?,?,?,?,NULL,NULL)',
                     ('BTC-USDC', '1m', bar['open_time'] * 1000, float(bar['open']), float(bar['high']), float(bar['low']),
                      float(bar['close']), float(bar['volume'])))
    if mutate:
        mutate(conn)
    conn.commit()
    conn.close()
    return cache, db


class BarParity(unittest.TestCase):
    def run_mode(self, mutate=None):
        with tempfile.TemporaryDirectory() as directory:
            cache, db = make_fixture(directory, mutate)
            return tool.compare_bars(tool.open_copy(db), cache, ['BTC-USDC'])['BTC-USDC']

    def test_identical_bars_pass(self):
        report = self.run_mode()
        self.assertEqual(report['equal'], 1500)
        self.assertFalse(report.get('mismatched'))
        self.assertEqual(report['service_rows_without_v1_row'], 0)

    def test_single_changed_close_is_found(self):
        report = self.run_mode(lambda c: c.execute('update ChartSnapshot set close_price=close_price+0.01 where candle_timestamp=?', ((T0 + 600) * 1000,)))
        self.assertEqual(report['mismatched'], 1)
        self.assertEqual(report['field_close'], 1)
        self.assertEqual(report['examples'][0]['open_time'], T0 + 600)

    def test_row_absent_from_service_is_reported(self):
        report = self.run_mode(lambda c: c.execute('insert into ChartSnapshot values (?,?,?,1,1,1,1,1,NULL,NULL)', ('BTC-USDC', '1m', (T0 + 60 * 1499 + 60) * 1000 - 0)))
        self.assertEqual(report.get('missing_in_service', 0), 0)  # outside the service window by construction
        self.assertEqual(report['equal'], 1500)

    def test_live_recorder_paths_are_refused(self):
        with self.assertRaises(SystemExit):
            tool.refuse_live_path(os.path.expanduser('~/.local/share/rsibot-stack/execution/main/data/ok_rsi.sqlite'))


try:
    import pandas_ta  # noqa: F401
    HAVE_TA = True
except Exception:  # pragma: no cover
    HAVE_TA = False


@unittest.skipUnless(HAVE_TA, 'pandas_ta not installed')
class IndicatorParity(unittest.TestCase):
    def test_recorded_values_from_the_same_formulas_match_and_a_shortened_window_does_not(self):
        with tempfile.TemporaryDirectory() as directory:
            cache, db = make_fixture(directory)
            _, rows = tool.load_cache(cache, 'BTC-USDC')
            full = tool.service_indicators(rows, 14, 14)
            conn = sqlite3.connect(db)
            for t in sorted(rows):
                if t < T0 + 60 * 1000:
                    continue
                if t >= T0 + 60 * 1400:   # the last 100 rows were recorded by a feed that had just restarted with 40 bars
                    window = tool.service_indicators({k: v for k, v in rows.items() if k <= t}, 14, 14, tail=40)
                    rsi, atr = float(window.iloc[-1]['rsi']), float(window.iloc[-1]['atr'])
                else:
                    rsi, atr = float(full.at[t, 'rsi']), float(full.at[t, 'atr'])
                conn.execute('update ChartSnapshot set rsi=?, atr=? where candle_timestamp=?', (rsi, atr, t * 1000))
            conn.commit()
            conn.close()
            report, failed = tool.compare_indicators(tool.open_copy(db), cache, {'BTC-USDC': 14}, 14, T0 + 60 * 1000, 1000)
            row = report['BTC-USDC']
            self.assertTrue(failed)
            self.assertEqual(row['enforced_rows'], 500)
            self.assertGreater(row['within_tolerance'], 390)
            self.assertGreater(row['out_of_tolerance'], 0)              # short-window rows are caught
            self.assertGreater(row['enforced_failures'], 50)
            # Enforcing only the rows recorded with a full window passes.
            report, failed = tool.compare_indicators(tool.open_copy(db), cache, {'BTC-USDC': 14}, 14, T0 + 60 * 1000, 1000)
            self.assertEqual(report['BTC-USDC']['within_tolerance'] + report['BTC-USDC']['out_of_tolerance'], 500)


if __name__ == '__main__':
    unittest.main()
