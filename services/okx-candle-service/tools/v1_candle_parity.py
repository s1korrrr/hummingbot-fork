#!/usr/bin/env python3
"""Parity proof between V1's in-process OKX candle feed and the okx-candle-service cache.

Read-only. It never talks to Docker, a live recorder or the network:

* ``--recorder-copy`` must be a COPY of V1's ``ok_rsi.sqlite`` (copy the .sqlite, -wal and -shm
  together first). Paths under ``~/.local/share/rsibot-*`` are refused: opening a live WAL recorder
  with a host SQLite client unlinks its -wal/-shm and breaks every reader.
* ``--cache-dir`` is a service cache directory (``<PAIR>.json`` files, or a copy of them).

Modes
  bars        every V1 ChartSnapshot 1m row must equal the service bar of the same open time
              (open, high, low, close, volume).
  indicators  recompute RSI (pandas_ta, same call as hl_rsi.update_processed_data) and ATR (the
              controller's ewm(alpha=1/n, adjust=False) formula) from the SERVICE bars and compare
              with the values V1 recorded, from ``--enforce-since`` onward.
Exit status is non-zero when any enforced comparison fails.

Caveat measured on 2026-10-02 (see docs/architecture/rsibot-candle-service.md): V1's recorded
ChartSnapshot indicator columns are decision-time values only for the freshest rows (about the last
four hours). Older rows were re-projected from a rolling window of rsi_length+1 bars (that window
reproduces 12 of 12 sampled old rows with error 0.0), so they are NOT a parity oracle. Pass
``--enforce-since`` at the first fresh row; the rest is reported informationally. The decision-time
proof for the cutover is the live shadow comparison described in the architecture document.
"""
import argparse
import json
import math
import os
import sqlite3
import sys
from collections import Counter

BAR_REL_TOL = 1e-12        # float64 of the same decimal string: equality, with rounding slack
RSI_ABS_TOL = 1e-6        # RSI points; observed worst case on matched windows 9.3e-7
ATR_REL_TOL = 1e-6        # observed worst case 2.7e-8
DEFAULT_LENGTHS = {'BNB-USDC': 17, 'BTC-USDC': 14, 'ETH-USDC': 17, 'SOL-USDC': 17, 'XRP-USDC': 16}


def refuse_live_path(path):
    resolved = os.path.realpath(path)
    if '/.local/share/rsibot-' in resolved:
        raise SystemExit(f'refusing live recorder path {resolved}: copy the sqlite trio first')
    return resolved


def open_copy(path):
    resolved = refuse_live_path(path)
    return sqlite3.connect(f'file:{resolved}?mode=ro', uri=True)


def load_cache(cache_dir, pair):
    with open(os.path.join(cache_dir, f'{pair}.json'), encoding='utf-8') as handle:
        record = json.load(handle)
    rows = {int(r['open_time']): r for r in record['rows']}
    if len(rows) != len(record['rows']):
        raise SystemExit(f'{pair}: duplicate open_time in cache')
    return record, rows


def close_enough(recorded, service, rel=BAR_REL_TOL):
    if recorded is None:
        return False
    a, b = float(recorded), float(service)
    return math.isclose(a, b, rel_tol=rel, abs_tol=0.0)


def compare_bars(conn, cache_dir, pairs):
    report = {}
    for pair in pairs:
        _, service = load_cache(cache_dir, pair)
        lo, hi = min(service), max(service)
        counts = Counter()
        examples = []
        query = ("select candle_timestamp, open_price, high_price, low_price, close_price, volume "
                 "from ChartSnapshot where pair=? and interval='1m' and candle_timestamp between ? and ? "
                 "order by candle_timestamp")
        for ts, o, h, low, c, v in conn.execute(query, (pair, lo * 1000, hi * 1000)):
            counts['v1_rows'] += 1
            bar = service.get(ts // 1000)
            if bar is None:
                counts['missing_in_service'] += 1
                continue
            pairs_to_check = (('open', o, bar['open']), ('high', h, bar['high']), ('low', low, bar['low']),
                              ('close', c, bar['close']), ('volume', v, bar['volume']))
            bad = [name for name, rec, svc in pairs_to_check if not close_enough(rec, svc)]
            if bad:
                counts['mismatched'] += 1
                for name in bad:
                    counts['field_' + name] += 1
                if len(examples) < 5:
                    examples.append({'open_time': ts // 1000, 'fields': bad})
            else:
                counts['equal'] += 1
        counts['service_rows_without_v1_row'] = len(service) - (counts['v1_rows'] - counts['missing_in_service'])
        report[pair] = {'window': [lo, hi], **counts, 'examples': examples}
    return report


def service_indicators(rows, rsi_length, atr_length, tail=None):
    """RSI and ATR series over service bars, mirroring hl_rsi's computation exactly."""
    import pandas as pd
    import pandas_ta  # noqa: F401  (registers the .ta accessor, as in the controller)
    ordered = [rows[t] for t in sorted(rows)]
    frame = pd.DataFrame({'timestamp': [int(r['open_time']) for r in ordered],
                          'open': [float(r['open']) for r in ordered],
                          'high': [float(r['high']) for r in ordered],
                          'low': [float(r['low']) for r in ordered],
                          'close': [float(r['close']) for r in ordered]})
    if tail:
        frame = frame.tail(tail).reset_index(drop=True)
    frame.ta.rsi(length=rsi_length, append=True)
    prev = frame['close'].shift(1)
    tr = pd.concat([frame['high'] - frame['low'], (frame['high'] - prev).abs(), (frame['low'] - prev).abs()], axis=1).max(axis=1)
    frame['atr'] = tr.ewm(alpha=1.0 / atr_length, min_periods=atr_length, adjust=False).mean()
    return frame.set_index('timestamp')[[f'RSI_{rsi_length}', 'atr']].rename(columns={f'RSI_{rsi_length}': 'rsi'})


def compare_indicators(conn, cache_dir, lengths, atr_length, enforce_since, warmup_bars):
    report, failed = {}, False
    for pair, rsi_length in lengths.items():
        _, service = load_cache(cache_dir, pair)
        series = service_indicators(service, rsi_length, atr_length)
        first = min(service) + warmup_bars * 60            # skip the recomputation's own warm-up
        counts = Counter()
        worst_rsi = worst_atr = 0.0
        last_bad = None
        query = ("select candle_timestamp, rsi, atr from ChartSnapshot where pair=? and interval='1m' "
                 "and candle_timestamp>=? and rsi is not null and atr is not null order by candle_timestamp")
        for ts, rsi, atr in conn.execute(query, (pair, first * 1000)):
            t = ts // 1000
            if t not in series.index:
                continue
            d_rsi = abs(series.at[t, 'rsi'] - rsi)
            d_atr = abs(series.at[t, 'atr'] - atr) / atr if atr else math.inf
            ok = d_rsi <= RSI_ABS_TOL and d_atr <= ATR_REL_TOL
            counts['rows'] += 1
            counts['within_tolerance' if ok else 'out_of_tolerance'] += 1
            if not ok:
                last_bad = t
            if t >= enforce_since:
                counts['enforced_rows'] += 1
                if not ok:
                    counts['enforced_failures'] += 1
                worst_rsi, worst_atr = max(worst_rsi, d_rsi), max(worst_atr, d_atr)
        failed |= counts['enforced_failures'] > 0 or counts['enforced_rows'] == 0
        report[pair] = {'rsi_length': rsi_length, 'atr_length': atr_length, **counts, 'last_out_of_tolerance': last_bad,
                        'enforced_max_abs_rsi': worst_rsi, 'enforced_max_rel_atr': worst_atr}
    return report, failed


def parse_lengths(text):
    if not text:
        return dict(DEFAULT_LENGTHS)
    return {k: int(v) for k, v in (item.split('=') for item in text.split(','))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('mode', choices=['bars', 'indicators'])
    parser.add_argument('--recorder-copy', required=True)
    parser.add_argument('--cache-dir', required=True)
    parser.add_argument('--pairs', default=','.join(DEFAULT_LENGTHS))
    parser.add_argument('--rsi-lengths', help='PAIR=N,... (default: the V1 controller YAML values)')
    parser.add_argument('--atr-length', type=int, default=14)
    parser.add_argument('--enforce-since', type=int, default=0, help='unix seconds; indicator rows from here must match')
    parser.add_argument('--warmup-bars', type=int, default=1000)
    args = parser.parse_args(argv)
    conn = open_copy(args.recorder_copy)
    pairs = [p for p in args.pairs.split(',') if p]
    if args.mode == 'bars':
        report = compare_bars(conn, args.cache_dir, pairs)
        failed = any(r.get('mismatched') or r.get('missing_in_service') for r in report.values())
    else:
        lengths = {p: n for p, n in parse_lengths(args.rsi_lengths).items() if p in pairs}
        report, failed = compare_indicators(conn, args.cache_dir, lengths, args.atr_length, args.enforce_since, args.warmup_bars)
    json.dump({'mode': args.mode, 'failed': failed, 'pairs': report}, sys.stdout, indent=2, default=str)
    sys.stdout.write('\n')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
