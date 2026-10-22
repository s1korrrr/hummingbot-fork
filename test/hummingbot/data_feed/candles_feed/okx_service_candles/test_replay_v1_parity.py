import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("replay_v1_parity", Path(__file__).with_name("replay_v1_parity.py"))
replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay)


class ReplayHarnessGuards(unittest.TestCase):
    def test_live_recorder_paths_are_refused(self):
        for path in ("/Users/x/.local/share/rsibot-stack/execution/main/data/ok_rsi.sqlite",
                     "/home/u/.local/share/rsibot-stack-v2/execution/rsi/data/ok_rsi.sqlite"):
            with self.assertRaises(SystemExit):
                replay.refuse_live_path(path)

    def test_copy_is_opened_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ok_rsi.sqlite"
            sqlite3.connect(path).close()
            conn = replay.open_copy(path)
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("create table x (a)")
            conn.close()

    def test_a_one_cent_difference_is_caught_and_float_equality_is_not(self):
        self.assertTrue(replay.close_enough(775.1, 775.1))
        self.assertFalse(replay.close_enough(775.1, 775.11))
        self.assertTrue(replay.close_enough(25.2942410, 25.2942415, absolute=replay.RSI_ABS_TOL, relative=0.0))
        self.assertFalse(replay.close_enough(25.29, 25.30, absolute=replay.RSI_ABS_TOL, relative=0.0))
        self.assertTrue(replay.close_enough(None, None))
        self.assertFalse(replay.close_enough(None, 1.0))

    def test_indicator_comparison_names_the_differing_columns(self):
        recorded = {key: 1.0 for key in replay.NUMERIC}
        replayed = dict(recorded)
        self.assertEqual(replay.indicator_differences(recorded, replayed), [])
        replayed["atr"] = 1.0 + 1e-6
        replayed["rsi"] = 1.0 + 5e-7
        self.assertEqual(replay.indicator_differences(recorded, replayed), ["atr"])
        replayed["rsi"] = 1.0 + 5e-3
        self.assertEqual(replay.indicator_differences(recorded, replayed), ["rsi", "atr"])
        self.assertEqual(replay.indicator_differences(recorded, {}), list(replay.NUMERIC))

    def test_bar_comparison_requires_the_same_open_time_and_prices(self):
        import pandas as pd
        frame = pd.DataFrame([dict(timestamp=60.0, open=1.0, high=2.0, low=0.5, close=1.5, volume=3.0)])
        rec = dict(open=1.0, high=2.0, low=0.5, close=1.5, volume=3.0)
        self.assertTrue(replay.bar_matches(rec, frame, 60))
        self.assertFalse(replay.bar_matches(rec, frame, 120))
        self.assertFalse(replay.bar_matches({**rec, "close": 1.51}, frame, 60))

    def test_okx_array_matches_the_exchange_shape(self):
        row = dict(open_time=1790933520, open="775.1", high="775.1", low="775.1", close="775.1", volume="0",
                   quote_volume="0", confirmed=True)
        array = replay.okx_array(row)
        self.assertEqual(len(array), 9)
        self.assertEqual((array[0], array[8]), ("1790933520000", "1"))
