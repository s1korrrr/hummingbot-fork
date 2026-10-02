#!/usr/bin/env python3
"""Replay V1's recorded decision-time rows through the OK RSI controller fed by the service candle feed.

Offline and read-only. Run it inside the V1 engine image with `--network none`; mount the inputs read-only:

* ``--recorder-copy``  a COPY of V1's ``ok_rsi.sqlite`` (copy the .sqlite, -wal and -shm together first). Paths under
  ``~/.local/share/rsibot-*`` are refused: opening a live WAL recorder with a host SQLite client unlinks its
  -wal/-shm and breaks every reader.
* ``--cache-dir``      a copy of the service cache (``<PAIR>.json``): the bars the service publishes.
* ``--conf-dir``       a copy of V1's ``conf/controllers/ok_rsi_*.yml``.

For every sampled closed minute ``t`` it (1) builds the frame the controller receives from the service feed (3660 bars
ending at ``t``, through `OKXServiceCandles` and `MarketDataProvider.get_candles_df`), (2) checks it equals, exactly,
the frame the native OKX feed holds for the same confirmed rows, (3) runs the real controller's
``update_processed_data`` on it and compares the indicator and decision columns with the row V1 recorded for ``t``.
Exit status is non-zero when an enforced comparison fails.
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


async def run(args):
    conn = open_copy(args.recorder_copy)
    report, failed = {}, False
    for pair in args.pairs:
        cache_rows = load_cache(args.cache_dir, pair)
        replay = Replay(pair, args.conf_dir, cache_rows)
        await replay.start()
        counts, examples = Counter(), []
        query = ("select candle_timestamp, open_price, high_price, low_price, close_price, volume, "
                 + ", ".join(NUMERIC + DECISION_NUMERIC + DECISION_TEXT) + ", signal "
                 "from ChartSnapshot where pair=? and interval='1m' and candle_timestamp>=? and rsi is not null "
                 "order by candle_timestamp")
        names = ["ts", "open", "high", "low", "close", "volume", *NUMERIC, *DECISION_NUMERIC, *DECISION_TEXT,
                 "signal"]
        recorded = [dict(zip(names, r)) for r in conn.execute(query, (pair, args.since * 1000))]
        recorded = recorded[::args.step][-args.limit:] if args.limit else recorded[::args.step]
        started = time.time()
        for rec in recorded:
            t = rec["ts"] // 1000
            if t not in replay.index or replay.index[t] + 1 < HISTORY:
                counts["no_service_window"] += 1
                continue
            frame, shape_equal = await replay.decide(t)
            counts["rows"] += 1
            counts["frame_equal_native"] += shape_equal
            last = frame.iloc[-1]
            bar_ok = bool(frame["timestamp"].iloc[-1] == t and all(
                close_enough(rec[key], last[key]) for key in ("open", "high", "low", "close", "volume")))
            counts["bar_equal_recorded"] += bool(bar_ok)
            values, observation = replay.computed(t)
            bad = []
            for key in NUMERIC:
                tolerance = {"absolute": RSI_ABS_TOL, "relative": 0.0} if key == "rsi" else {}
                if not close_enough(rec[key], values.get(key), **tolerance):
                    bad.append(key)
            counts["indicators_equal"] += not bad
            decision_bad = [key for key in DECISION_NUMERIC if rec[key] is not None and not close_enough(
                rec[key], observation.get(key))]
            decision_bad += [key for key in DECISION_TEXT if rec[key] is not None and rec[key] != observation.get(key)]
            if rec["signal"] is not None and observation.get("signal") is not None \
                    and float(rec["signal"]) != float(observation["signal"]):
                decision_bad.append("signal")
            counts["decision_rows"] += 1
            counts["decisions_equal"] += not decision_bad
            for key in bad + decision_bad:
                counts["differs_" + key] += 1
            if (not shape_equal or not bar_ok or bad or decision_bad) and len(examples) < 5:
                examples.append({"open_time": t, "shape_equal": shape_equal, "bar_ok": bar_ok,
                                 "indicator_diff": bad, "decision_diff": decision_bad})
        report[pair] = {**{k: int(v) for k, v in counts.items()}, "seconds": round(time.time() - started, 1), "examples": examples}
        enforced = counts["rows"] and not (counts["rows"] == counts["frame_equal_native"] == counts["bar_equal_recorded"]
                                           == counts["indicators_equal"])
        failed = failed or bool(enforced)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--recorder-copy", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--conf-dir", required=True)
    parser.add_argument("--pairs", nargs="+", default=["BNB-USDC", "BTC-USDC", "ETH-USDC", "SOL-USDC", "XRP-USDC"])
    parser.add_argument("--since", type=int, required=True, help="first candle open time (epoch s) to replay")
    parser.add_argument("--step", type=int, default=1, help="replay every Nth recorded row")
    parser.add_argument("--limit", type=int, default=0, help="replay at most the last N sampled rows per pair")
    sys.exit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
