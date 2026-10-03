#!/usr/bin/env python3
"""Replay V1's recorded decision-time rows through the OK RSI controller fed by the service candle feed.

Offline and read-only. Run it inside the V1 engine image with `--network none`; mount the inputs read-only:

* ``--recorder-copy``  a COPY of V1's ``ok_rsi.sqlite`` (copy the .sqlite, -wal and -shm together first). Paths under
  ``~/.local/share/rsibot-*`` are refused: opening a live WAL recorder with a host SQLite client unlinks its
  -wal/-shm and breaks every reader.
* ``--cache-dir``      a copy of the service cache (``<PAIR>.json``): the bars the service publishes.
* ``--conf-dir``       a copy of V1's ``conf/controllers/ok_rsi_*.yml``.

Two checks per pair, both enforced (exit status is non-zero when either fails):

* projection: V1 stores each row of its newest feature window as the newest evaluation projects it, so one
  service-fed evaluation at the newest recorded minute must reproduce every stored indicator row inside that window
  (RSI 1e-6 absolute, ATR, ATR %, EMA, Bollinger, MACD histogram 1e-9 relative), and the frame it received must equal,
  exactly, the frame the native OKX feed holds for the same confirmed rows;
* decision time: for the freshest ``--fresh-minutes`` of recorded rows, one evaluation per minute, oldest first, on
  the 3660 service bars ending at that minute (through `OKXServiceCandles` and `MarketDataProvider.get_candles_df`),
  compared with the row V1 recorded at decision time. Older stored rows were overwritten by later projections whose
  window position has less history, so they differ from decision-time values by up to about 5e-3 RSI points and are
  not an oracle for this check. Decision columns (signal, regime, thresholds) are reported but not enforced: they
  depend on position and executor state the replay does not have.
"""
import argparse
import asyncio
import json
import math
import os
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

DATA = {"mode": "shared", "transport": "stream", "origin": "http://127.0.0.1:8093", "token_file": "/run/candles/token"}
HISTORY = 3660
NUMERIC = ("rsi", "atr", "atr_pct", "ema_fast", "ema_slow", "bb_lower", "bb_mid", "bb_upper", "macd_hist")
DECISION_NUMERIC = ("signal_score", "regime_confidence", "trend_confidence", "rsi_buy_threshold",
                    "rsi_sell_threshold")
DECISION_TEXT = ("regime_label", "trend_direction", "buy_decision", "sell_decision", "buy_entry_role",
                 "buy_context_bias")
RSI_ABS_TOL = 1e-6
REL_TOL = 1e-9


def refuse_live_path(path):
    resolved = os.path.realpath(path)
    if "/.local/share/rsibot-" in resolved:
        raise SystemExit(f"refusing live recorder path {resolved}: copy the sqlite trio first")
    return resolved


def open_copy(path):
    return sqlite3.connect(f"file:{refuse_live_path(path)}?mode=ro", uri=True)


def load_cache(cache_dir, pair):
    record = json.loads((Path(cache_dir) / f"{pair}.json").read_text())
    return record["rows"]


def okx_array(row):
    """The OKX history-candles array a cache row came from (index 6 is not carried; set to the base volume)."""
    return [str(row["open_time"] * 1000), row["open"], row["high"], row["low"], row["close"], row["volume"],
            row["volume"], row["quote_volume"], "1"]


def native_frame(rows, max_records):
    from hummingbot.data_feed.candles_feed.okx_spot_candles import OKXSpotCandles
    feed = OKXSpotCandles("BNB-USDC", "1m", max_records)
    for parsed in feed._parse_rest_candles({"data": [okx_array(r) for r in reversed(rows)]}):
        feed._candles.append(np.array(parsed).astype(float))
    return feed.candles_df


def close_enough(recorded, replayed, *, absolute=0.0, relative=REL_TOL):
    if recorded is None or replayed is None:
        return recorded is None and replayed is None
    return math.isclose(float(recorded), float(replayed), rel_tol=relative, abs_tol=absolute)


class Replay:
    def __init__(self, pair, conf_dir, cache_rows):
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
        from hummingbot.data_feed.candles_feed.okx_service_candles import service_hub
        from hummingbot.data_feed.market_data_provider import MarketDataProvider
        from hummingbot.strategy_v2.components.candle_stream import CandleStreamClient

        class OfflineClient(CandleStreamClient):
            def start(self):
                self.begin_session()

            async def close(self):
                self.replica.disconnect()

        self.pair, self.rows = pair, cache_rows
        self.now = [0.0]
        service_hub.reset_service_candles()
        raw = yaml.safe_load((Path(conf_dir) / f"ok_rsi_{pair.split('-')[0].lower()}.yml").read_text())
        raw.update(candles_connector="okx_service", data=DATA)
        self.provider = MarketDataProvider({})
        self.provider.initialize_rate_sources = lambda *a, **k: None
        self.provider.time = lambda: self.now[0]
        # Patch once from the pristine constructor: a second pair must not chain onto the first pair's clock.
        original = getattr(service_hub.ServiceCandleHub, "_replay_original_init", service_hub.ServiceCandleHub.__init__)
        service_hub.ServiceCandleHub._replay_original_init = original

        def init(hub, policy, **kwargs):
            original(hub, policy, **{**kwargs, "clock": lambda: self.now[0], "client_factory": OfflineClient,
                                     "coalesce_seconds": 0})
        service_hub.ServiceCandleHub.__init__ = init
        self.controller = OKRSIController(config=OKRSIControllerConfig(**raw), market_data_provider=self.provider,
                                          actions_queue=asyncio.Queue())
        self.config = self.controller.config
        self.service_hub = service_hub
        self.index = {int(r["open_time"]): i for i, r in enumerate(cache_rows)}
        self.feed = None

    async def start(self):
        self.controller.initialize_candles()
        for _ in range(10):
            await asyncio.sleep(0)
        self.feed = self.provider.candles_feeds[f"okx_{self.pair}_1m"]

    def serve(self, open_time):
        """Publish the 3660 bars ending at open_time as a fresh service session."""
        end = self.index[open_time] + 1
        window = self.rows[end - HISTORY:end]
        self.now[0] = float(open_time + 61)
        hub = self.service_hub.get_service_hub()
        hub.client.begin_session()
        hub.client.apply(dict(v=1, type="snapshot", epoch="replay", published_at=self.now[0], pair=self.pair,
                              sequence=1, received_at=self.now[0], rows=window))
        return window

    async def decide(self, open_time):
        window = self.serve(open_time)
        frame = self.provider.get_candles_df("okx", self.pair, "1m", self.controller._derived_base_max_records())
        shape_equal = True
        try:
            pd.testing.assert_frame_equal(
                frame, native_frame(window, self.feed.max_records).iloc[-len(frame):], check_exact=True)
        except AssertionError:
            shape_equal = False
        await self.controller.update_processed_data()
        return frame, shape_equal

    def computed(self, open_time):
        from controllers.directional_trading.ok_rsi_core.chart_snapshots import _indicator_columns, chart_observation
        processed = self.controller.processed_data
        features = processed["features"]
        row = features.loc[features["timestamp"] == open_time]
        columns = _indicator_columns(self.config)
        values = {key: (None if row.empty else float(row[column].iloc[0])) for key, column in columns.items()}
        close = None if row.empty else float(row["close"].iloc[0])
        values["atr_pct"] = None if values["atr"] is None or not close else values["atr"] / close
        _, observation = chart_observation(self.config, "1m", processed, self.now[0], 60)
        return values, observation


def load_recorded(conn, pair, since):
    names = ["ts", "open", "high", "low", "close", "volume", *NUMERIC, *DECISION_NUMERIC, *DECISION_TEXT, "signal"]
    query = ("select candle_timestamp, open_price, high_price, low_price, close_price, volume, "
             + ", ".join(NUMERIC + DECISION_NUMERIC + DECISION_TEXT) + ", signal "
             "from ChartSnapshot where pair=? and interval='1m' and candle_timestamp>=? and rsi is not null "
             "order by candle_timestamp")
    return [dict(zip(names, row)) for row in conn.execute(query, (pair, since * 1000))]


def indicator_differences(rec, values):
    bad = []
    for key in NUMERIC:
        tolerance = {"absolute": RSI_ABS_TOL, "relative": 0.0} if key == "rsi" else {}
        if not close_enough(rec[key], values.get(key), **tolerance):
            bad.append(key)
    return bad


def bar_matches(rec, frame, t):
    last = frame.iloc[-1]
    return bool(frame["timestamp"].iloc[-1] == t and all(
        close_enough(rec[key], last[key]) for key in ("open", "high", "low", "close", "volume")))


def decision_differences(rec, observation):
    bad = [key for key in DECISION_NUMERIC if rec[key] is not None and not close_enough(rec[key], observation.get(key))]
    bad += [key for key in DECISION_TEXT if rec[key] is not None and rec[key] != observation.get(key)]
    if rec["signal"] is not None and observation.get("signal") is not None \
            and float(rec["signal"]) != float(observation["signal"]):
        bad.append("signal")
    return bad


async def projection_check(pair, args, cache_rows, recorded, counts, examples):
    """V1 stores every row of its newest feature window as that evaluation projects it, so one service-fed
    evaluation at the newest recorded minute must reproduce all stored rows inside the window."""
    replay = Replay(pair, args.conf_dir, cache_rows)
    await replay.start()
    newest = recorded[-1]["ts"] // 1000
    frame, shape_equal = await replay.decide(newest)
    window = replay.controller.max_records
    counts["projection_frame_equal_native"] += shape_equal
    for rec in recorded:
        t = rec["ts"] // 1000
        if (newest - t) // 60 >= window:
            counts["projection_outside_feature_window"] += 1
            continue
        values, _ = replay.computed(t)
        bad = indicator_differences(rec, values)
        counts["projection_rows"] += 1
        counts["projection_indicators_equal"] += not bad
        for key in bad:
            counts["projection_differs_" + key] += 1
        if bad and len(examples) < 5:
            examples.append({"projection_open_time": t, "indicator_diff": bad})


async def decision_check(pair, args, cache_rows, recorded, counts, examples):
    """Replay the minutes whose recorded values are decision-time: one evaluation per minute, oldest first."""
    replay = Replay(pair, args.conf_dir, cache_rows)
    await replay.start()
    newest = recorded[-1]["ts"] // 1000
    fresh = [rec for rec in recorded if (newest - rec["ts"] // 1000) <= args.fresh_minutes * 60]
    fresh = fresh[::args.step]
    for rec in fresh:
        t = rec["ts"] // 1000
        if t not in replay.index or replay.index[t] + 1 < HISTORY:
            counts["no_service_window"] += 1
            continue
        frame, shape_equal = await replay.decide(t)
        values, observation = replay.computed(t)
        bad = indicator_differences(rec, values)
        decision_bad = decision_differences(rec, observation)
        ok_bar = bar_matches(rec, frame, t)
        counts["decision_rows"] += 1
        counts["decision_frame_equal_native"] += shape_equal
        counts["decision_bar_equal_recorded"] += ok_bar
        counts["decision_indicators_equal"] += not bad
        counts["decision_columns_equal_informational"] += not decision_bad
        for key in bad:
            counts["decision_differs_" + key] += 1
        for key in decision_bad:
            counts["informational_differs_" + key] += 1
        if (not shape_equal or not ok_bar or bad) and len(examples) < 5:
            examples.append({"decision_open_time": t, "shape_equal": shape_equal, "bar_ok": ok_bar,
                             "indicator_diff": bad})


async def run(args):
    conn = open_copy(args.recorder_copy)
    report, failed = {}, False
    for pair in args.pairs:
        cache_rows = load_cache(args.cache_dir, pair)
        recorded = load_recorded(conn, pair, args.since)
        if not recorded:
            report[pair] = {"error": "no recorded rows since --since"}
            failed = True
            continue
        counts, examples, started = Counter(), [], time.time()
        await projection_check(pair, args, cache_rows, recorded, counts, examples)
        await decision_check(pair, args, cache_rows, recorded, counts, examples)
        report[pair] = {**{k: int(v) for k, v in counts.items()}, "seconds": round(time.time() - started, 1),
                        "examples": examples}
        enforced = (counts["projection_rows"] == counts["projection_indicators_equal"]
                    and counts["projection_frame_equal_native"] == 1
                    and counts["decision_rows"] == counts["decision_frame_equal_native"]
                    == counts["decision_bar_equal_recorded"] == counts["decision_indicators_equal"]
                    and counts["projection_rows"] > 0 and counts["decision_rows"] > 0)
        failed = failed or not enforced
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--recorder-copy", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--conf-dir", required=True)
    parser.add_argument("--pairs", nargs="+", default=["BNB-USDC", "BTC-USDC", "ETH-USDC", "SOL-USDC", "XRP-USDC"])
    parser.add_argument("--since", type=int, required=True, help="first candle open time (epoch s) to compare")
    parser.add_argument("--fresh-minutes", type=int, default=120,
                        help="replay decision-time evaluations only for recorded rows this young (see the module note)")
    parser.add_argument("--step", type=int, default=1, help="replay every Nth fresh row")
    sys.exit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
