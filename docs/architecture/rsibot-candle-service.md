# OKX candle service: hardening, shared active/active pair, V1 move

This is the historical pre-rollout hardening study. Its live identities, authority and
rollout proposal below describe that inspection window. A later reviewed candle-only
rollout on 2026-10-02 deployed image `sha256:3a4faad584501f33f7f01a6be0ec679c17a0e23d46cbbf94e3a3dba873ad6bfd`.
All five 1m projection routes passed ready/history/provenance checks; the other thirteen
container identities were unchanged at the rollout check. The current dated receipt is
[the root unified-finish rollout evidence](https://github.com/s1korrrr/rsibot/blob/11b419cd664c9eae2e80cd8bf7a9b40004ac29b6/docs/evidence/2026-10-02-unified-finish/candle-final-audit-rollout.json).
This does not establish engine recovery or trading readiness.

Status 2026-10-02. Branch `feat/andrzej_candles_hardening_20261002`, based on engine commit `39a24e226ebd`
(the commit stamped on both live images, label `io.rsitech.v3.engine-commit`). Nothing here was deployed:
no container was stopped, recreated or restarted, nothing was written under `~/.local/share/rsibot-*`,
no token was read or printed.

Authority ceiling: repository-process-only. The service is read-only public market data; it has no
credential, order or administrative endpoint. Redeploying any of it is a separate, approved action.

## 1. Source and provenance

| Item | Value |
|---|---|
| Source | `services/okx-candle-service/` in the hummingbot repo (Rust, axum, tokio-tungstenite, Cargo.lock pinned) |
| Live image | `rsibot-v2-candles:v3-limited-39a24e` = `sha256:b9dbb6dc960d`, entrypoint `/usr/local/bin/okx-candle-service`; **the same image runs V2 and V3** |
| Identity check | `git diff 39a24e 15c0ba1d0 0a3b7538b -- services/okx-candle-service` is empty, so every current engine branch carries the deployed service source unchanged |
| V2 args | `--universe universe-rsi.json --listen 127.0.0.1:8092 --history 7514 --cache-dir /cache --market-picture` |
| V3 args | `--universe universe-meridian.json --listen 127.0.0.1:8093 --history 2000 --cache-dir /cache` |
| Engine client | `hummingbot/strategy_v2/components/candle_stream.py` (`CandleStreamClient`, WebSocket), `data_reader.py` (`DataPool`, `attach_pool`), `candle_data.py` (`Replica`). `candle_transport.py` is the HTTP snapshot transport and the token/grant helpers, not used for `transport: stream` |
| Universe | BNB, BTC, ETH, SOL, XRP `-USDC` in V1 (`ok_rsi_*.yml`), V2 (`universe-rsi.json`) and V3 (`universe-meridian.json`): the union is the same five pairs |

## 2. Root causes, quantified

Window: container logs 2026-09-29T14:29Z to 2026-10-02 (V2 service), 2026-10-01T11:19Z onward (V3 service was
recreated then), plus the V2 engine file logs. Reproduce with `docker logs -t <candles container>`.

| Event class | V2 | V3 | Where the line comes from |
|---|---|---|---|
| `future candle` (WS 52, REST seed 9) | 61 | 0 | `model.rs` `parse_row`: `open_time <= received_at + 5` applied to every row, forming rows included |
| `confirmed candle before close` | 9 | 0 | `parse_row`: `open_time + 60 <= received_at` with zero tolerance |
| `initial REST page is stale or future` | 25 | 5 | `history.rs` `HistoryPager::accept` first-page check |
| WS `text pong timeout` | 3 | 1 | `collector.rs` 20 s ping / 10 s pong |
| WS reset without close handshake / reset by peer | 9 + 5 | 5 | upstream socket |
| `local stream closed: deadline has elapsed` | 44 (8 groups of 5, 4 singles) | 0 | `server.rs` (two different timers, below) |
| `local stream closed: unexpected client message` | 3 | 0 | `server.rs` catch-all arm |

### 2.1 Clock-class rejections (61 + 9, in two windows)

66 of the 70 lines fall at 2026-10-01 10:04:31-10:04:52Z (all five pairs; WS lines end 10:04:44, REST-seed lines
10:04:52); the other 4 `before close` lines are 2026-09-30 01:33:58-59. A forming bar is rejected only when
`open_time > received_at + 5`, which for the current minute's forming bar needs the container clock to be
behind OKX by more than `s + 5` seconds (`s` = true second of the minute); rejections across :31-:44 point to
a lag of roughly a minute. A confirmed bar arrives about 0.3 s after its close, so any lag above that tripped
`before close` (the check had **zero** tolerance). The engine's logs show the same incident: at 10:07:22 the
V2 strategy logged `[clock=10:07:03]` (19 s tick lag), OKX price-refresh timeouts at 10:03:08 and 10:06:07,
and `Network status has changed` at 10:07:52.

Conclusion (INFERRED, not measured at the time): the Docker VM clock ran about a minute behind OKX and was
stepped forward to true time at about 10:04:45. Today the clock is fine (container minus host -11 ms; OKX
minus host +36 ms median over 8 samples, range 25-120 ms, measured 2026-10-02). macOS sleep is disabled
(`pmset`: `sleep 0`, caffeinate held), so this is a VM stall or step, not a host sleep. Why it happened is
UNCONFIRMED.

What made it expensive is the service, not the clock. One bad row anywhere in a frame took the whole frame,
and `State::reject_pair_data` then bumped the pair's generation, cleared its rows **and its warm baseline**
(`seed_base`) and invalidated it. Recovery was therefore a cold REST seed of 7514 rows per pair (about 26 pages
at 210 ms) instead of a one-page suffix fill, and a REST page containing the forming bar failed the same
parser (`public REST seed ...: future candle`, 9 lines).

### 2.2 Stale first REST page (25 + 5)

Bursts: V2 at 10:38:30 and :42, 10:48:30 and :42, 11:49:30; V3 at 11:49:32 (five pairs each). They begin
at second :30-:32 and recur 12 s later. Mechanism: `repair_loop` invalidates a ready pair when its newest
confirmed bar closed more than 90 s ago, which first becomes true at second :30 when the previous minute's
confirmed bar has not arrived. The reseed's first REST page must show a bar no older than 90 s; if OKX's
candle store is also behind, `HistoryPager::accept` fails it, and the retry comes `10 s` later (observed
12 s: ten-second timer plus a two-second tick). The 11:49:30 burst hit **both independent instances in the
same second** and the engine logged `Price refresh incomplete` at 11:49:58, so the cause is common to the VM
and OKX path, not an instance bug. Normal REST confirm latency measured today is +2.2 s to +3.7 s after the
minute boundary (3 samples, BTC-USDC), so a 30 s lag is an upstream or path stall. The service's behaviour is
correct to fail closed; the cost was the slow, fixed 10 s retry and an error that did not say "stale".

### 2.3 Public WebSocket flaps

19 upstream disconnect events in about 2.5 days (V2 13, V3 6; V2's five `reset by peer` lines are one 15 s
reconnect storm at 10:06:23 with backoff 1, 2, 4, 8 s and no jitter). Two of them hit both independent
instances within seconds (19:56:16 V3 and 19:56:23 V2 `text pong timeout`), so some flaps are network-wide. Each loss called
`State::disconnected`, which invalidated the pair and discarded the in-flight seed. With a single socket per
100 pairs every flap is a hole. Reconnect backoff was deterministic 1..30 s.

### 2.4 `deadline has elapsed` (whose deadline?)

Two server-side timers produce that identical text and the log could not tell them apart:
the 2 s write deadline in `send()` (every frame, including 1.3 MB snapshots), and the 5 s wait for the
client's subscribe message in `serve()`. `unexpected client message` was a catch-all that also swallowed real
transport errors (`Some(Err(_))`). It is the **server's** deadline against a slow or frozen **reader**; the
engine's own 10 s heartbeat deadline would be reported by the client, not here.

Pattern: 8 groups of exactly 5 within one second (09-30 20:17:25; 10-01 01:11:33, 11:41:42, 12:52:50,
13:34:51, 13:47:45, 13:56:27, 20:03:24), which is the five per-controller V2 streams stalling together.
Corroboration: V2 engine boots (`okx is not ready` clusters) at 09-30 20:20, 10-01 14:20; the 09-30 20:17:25
group is three minutes before the 20:20 boot, and the 10-01 20:03:24 group has the engine logging
`Price refresh recovered` at 20:03:58 (an event-loop or VM stall). A full-history snapshot is 7514 rows,
about 1.3 MB per pair per stream, and each V2 stream receives five of them on every (re)connect; a Python
reader parsing them with per-row model validation can be unresponsive for seconds. Which of {engine stop,
engine or VM stall, big-snapshot parse} caused each group is UNCONFIRMED; the new counters
(`client_write_deadline`, `client_subscribe_timeout`, `client_transport_error`, `client_unexpected_message`,
`client_lagged`, `client_scope_refused`) separate them from the next occurrence.

### 2.5 History, cache and recovery (what already works)

`cache.rs` persists `/cache/<PAIR>.json` (exact rows, sha256, mode 0600, atomic write) every 5 s and on
shutdown; load rejects checksum/identity/continuity/age (>24 h) failures. A loaded cache never enables
readiness: it becomes a baseline (`seed_base`), REST fills only the missing suffix (`limit` = minutes since
anchor + 2), and a live acknowledged socket plus a matching forming or confirmed head proves liveness
(`try_warm_restore`). After a disconnect the same baseline is kept. So start-up and reconnect gap-fill are
deterministic already; the hardening below stops bad rows and single-socket flaps from throwing the baseline
away.

## 3. Hardening changes (all in `services/okx-candle-service/`)

| Change | Where | Behaviour |
|---|---|---|
| Forming rows never fail | `model.rs` `parse_row_tolerant` -> `Parsed::{Confirmed,Deferred,Forming,Ignored}` | An unconfirmed row is liveness evidence or ignored (`forming_future`, `forming_malformed`), in WS frames and REST pages. Confirmed rows keep strict structure/decimal/OHLC validation |
| Confirmed-close availability | `model.rs` `parse_row_tolerant`, `state.rs` `release_deferred` | Confirmed rows are not admitted before `open+60 <= received_at`, regardless of the former 2 s skew allowance. Earlier rows are bounded in a per-pair deferred queue (max 8), retain their original upstream receipt timestamp, and release on the 1 s repair tick only at/after close. The candle event's `published_at` is the release tick time; rows more than 180 s ahead are dropped with `clock_skew_unreconciled`. REST seeds and cache revalidation use the same strict close boundary. |
| No baseline wipe on timing | `state.rs` `defer`, `note_rejected` | Timing rejections never bump the generation or clear `seed_base`. Only malformed confirmed data still does |
| Typed stale page, fast jittered retry | `history.rs` `StaleRestPage`; `collector.rs` `seed_retry_delay` | Stale first page retries after 3 s (then 6, 12...), other failures start at 10 s, cap 60 s, +-25 % jitter, reset on success |
| Reconnect backoff with jitter | `collector.rs` `reconnect_delay`, `unit_random` | `min(30, 2^n)` s scaled into `[0.5, 1)` |
| Two staggered public sockets | `collector.rs` `LANES = 2`, `session_lane`; `state.rs` `lanes` bitmask, `lane_disconnected` | Lane 1 starts 15 s after lane 0, ping phase offset. Rows are merged by `State::ingest` (identical duplicates counted in `duplicates_ignored`). A pair stays online and ready while any lane is acknowledged; only the last lane lost takes the old disconnect path |
| Honest disconnect reasons | `server.rs` `Disconnect` | Distinct log text and counters per cause; transport errors no longer reported as "unexpected client message" |
| Size-proportional write deadline | `server.rs` `write_deadline` | 2 s + 1 s per 512 KiB, cap 20 s (a 1.3 MB snapshot gets about 4.5 s; heartbeats keep 2 s) |
| Consumer-scoped tokens | `auth.rs`, `config.rs` `--consumers-file` | Per consumer: pair scope, history ceiling, stream cap; legacy `--token-file` still works as the full-scope consumer. Constant-time compare across all tokens; secret-file checks (0600, no symlink) reused |
| Freshness and digest metrics | `state.rs` `metrics`, `server.rs` `GET /v1/metrics?window=N` | Authenticated, read-only, scoped to the caller. Per pair: ready/online/lanes, `last_close_age_s`, `last_confirmed_receipt_age_s`, `reconnects`, `rejected{reason}`, `deferred`, `window_digest`, `window_head_open_time`. The compatibility key `skew_tolerance_s` reports the active tolerance (`0.0`) |
| Host-readable status file | `status.rs` `--status-file` | The metrics view (60-bar window) rewritten atomically every 5 s, mode 0600. Needed because the service listens on a namespace-local loopback that host monitors cannot reach |

`window_digest` is SHA-256 over the same canonical JSON array as the cache file digest (tested), so two
instances' windows, or a window and a cache file, are directly comparable.

Wire protocol v1, `/health` and the cache format are unchanged (additive counters only).

Market Picture restart loading checks the original digest and every row. A single otherwise-valid minute row in the former 2-second pre-close envelope is excluded in memory; validated neighboring history is retained. The minute remains a gap until a fresh source snapshot replaces it with new observed provenance. Original bytes and old timestamps are never rewritten by loading. Multiple bad rows, larger skews, hourly/daily early rows, malformed values and checksum failures reject the record. Hour/day readiness retains independent coverage/freshness checks. For deferred live candles, projection availability and freshness use `max(received_at, published_at)`; the receipt remains unchanged and the actual-close publication establishes availability. Future-dated source observations are rejected, and a >90 s early receipt followed by a later delta is covered by a regression.

### Tests and commands

Baseline before changes: 50 passed, 3 ignored (`cargo test --locked`). The current offline suite reports 73 passed, 4 ignored (`cargo test --offline --locked`).

```
cd services/okx-candle-service
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
python3 -m unittest discover -s tools          # parity + A/B tools, stdlib
```

`tests/hardening.rs` covers close-boundary parsing, early REST/seed rejection, close-time one-shot release,
same-open correction and supersession, next-minute continuity, typed stale pages, backoff and jitter bounds,
lane accounting, baseline preservation, digest parity, scoped socket consumers, write deadline scaling,
consumers-file validation and status file permissions. `tests/market_picture_history.rs` passes an emitted
deferred candle event through the source projection and writes a sanitized producer wire fixture on request.
Session-level tests in `src/collector_tests.rs` cover two lanes deduplicating without opening gaps and
confirmed/forming timing classification without fatal socket errors.

Not covered by tests, flagged: the production `run()` lane/backoff loop and the real OKX endpoints (no network
in tests, by the service's own rule); fault injection must stay fixture-only, no paper lane is running.

## 4. One shared pair, active/active

### 4.1 Topology (reconciled with `docs/architecture/rsibot-unified-stack.md` on branch `feat/andrzej_unified_stack_20261002`)

The unified stack puts all engines and the candle service in one namespace holder (`rsibot-netns`, compat
mode), loopback only. In that holder:

```
candles-a  (primary)    --listen 127.0.0.1:8093  --cache-dir /cache-a  --status-file /status/a.json
candles-b  (secondary)  --listen 127.0.0.1:8094  --cache-dir /cache-b  --status-file /status/b.json
common:  --universe universe.json  --history 7514  --market-picture  --consumers-file /candle/consumers.json
```

* 8093 stays V3's sealed origin, so V3 needs no change. V2's five controller files move 8092 to 8093 as the
  unified plan already says.
* Universe: the union of V1, V2 and V3 pairs is the five USDC pairs, so `universe-rsi.json` is the superset.
* History 7514 is the superset: V2 7514 (500 15-minute bars plus alignment), V1 3660 (`HISTORY_RECORDS`,
  owner reconstruction; 1275 for derived 5m/15m), V3 at most 2000. `server.rs` rejects a subscription above
  capacity, so the 7514 instance serves every consumer and a 2000 instance serves only V3. Retention is the
  rolling 7514-row window per pair (5 x 7514 = 37,570 rows) plus the market-picture projection on `--market-picture`. Measured resident
  memory today: V2 instance 231 MiB (7514 rows + projection), V3 instance 18 MiB (2000 rows, no projection),
  so budget about 250 MiB per instance.
* Separate cache directories and ports so two services never write one directory (the unified doc's own rule).
* Both instances run identical args except `--listen`, cache dir and status path, so digests are comparable.
* Public egress per instance: 2 sockets, so 4 sockets, paced by each process's own 1050 ms connect pace;
  REST seeding stays 4 concurrent at 210 ms per process. Stagger the first start of `candles-b` by about 60 s
  so the two cold seeds do not overlap (a warm cache makes this moot).

Consumer scopes and client limits (`--consumers-file`, 32 global stream slots, sized for three engines plus
shadow; V2 holds five streams today, one per controller `DataPool`):

| Consumer | pairs | max_history | max_streams |
|---|---|---|---|
| `v2` | five | 7514 | 8 |
| `v3` | five | 2000 | 4 |
| `v1` | five | 3660 | 4 (the adapter below uses one) |
| `api` (observation worker, market picture) | five | 2 to 2000 | 2 |
| `shadow` (research, parity, A/B probe) | five | 7514 | 4 |

Total 22 of 32. The old single token keeps working as consumer `default` during the migration.

Sizing risk: five V2 streams x five 1.3 MB snapshots is 33 MB of JSON on every V2 reconnect. The size-scaled
write deadline absorbs most of it; the durable fix is one `DataPool` per engine process rather than per
controller (see 4.4).

### 4.2 How engines reach the healthy instance

`CandleStreamClient` takes exactly one origin (`validate_origin`, pool key `(origin, token_file,
transport)`), reconnects with 0.5..30 s backoff, clears its snapshots on reconnect and holds entries until a
newer closed bar (1-2 minutes). It has no multi-origin support. Options, in order of preference:

1. **Server-side first (done above).** Each instance now survives one public-socket loss without invalidating
   anything, and clock or forming-bar noise no longer wipes warm state. Single-origin clients, including V3's
   sealed controller, benefit with no client change. This removes most of the observed outages.
2. **Smallest client change (about 30-50 lines, not made here because it changes sealed config):**
   `DataPolicy.origin` accepts an ordered list; `CandleStreamClient.run` tries them in order on each
   reconnect, sticky on the last good one with a hold-down (for example 5 minutes) before failing back, and
   only fails back at a candle boundary; per-origin token file; pool key includes the tuple. Needs a hummingbot
   image rebuild and, for V3, a new sealed release.
3. **L4 proxy in the holder** (HAProxy TCP mode owning 8093, backends on 18093/18094 with an authenticated
   `/health` check). WebSocket and bearer pass through untouched, so no engine change at all. Costs one new
   component and a health-check token in its config. Use only if the client change is blocked.

The V1 adapter (section 5) is a new component, so it takes the multi-origin list from day one.

### 4.3 A/B digest alarm and stale-feed hook

* Each instance publishes the metrics view to its status file. `tools/ab_digest_check.py` compares two files:
  `candle_pair_digest_mismatch` (same head, same window, different digest: critical), `candle_pair_head_lag`
  (heads differ by more than one bar), `candle_pair_stale`, `candle_pair_unready`, `candle_instance_degraded`
  (redundancy lost), `candle_instance_status_stale` (status older than 30 s), `candle_rejections` (warning).
  Covered by 6 unit tests.
* Push pipeline hook: `scripts/stack_alert_observation.py` already builds rows with
  `check(service, code, active, title, detail, severity, delay)` and has `v2_checks(state, now)`. Add a
  `candle_service_checks(state, now)` that reads the two status files from the state directory, calls
  `compare` and emits one `check` per code with `delay=OWNER_STALL_SECONDS` for the stale codes. Today
  `candles_stale` there reads `operations.pairs[].candle.source_at`, which is null for V2 (observed), and
  `stack_v2_health.py` rates `candles-rsi` by container state only. The status file closes both gaps without a
  container probe. I did not edit `scripts/` (other worktree, read-only inspection only).
* Cross-instance cache check today: V2 and V3 caches were byte-identical over 2000 common rows x 5 pairs
  (coordinator finding), so digests from independent instances do agree.

### 4.4 What redundancy does not cover

* One Docker VM clock and one egress for both instances. A VM stall or clock step (section 2.1) hits both at
  once; the 11:49:30 incident hit V2 and V3 in the same second. Deferred confirmed rows are not published
  until their actual close.
* An OKX-side candle delay or outage. Both instances see the same upstream. The fail-closed 90 s freshness
  contract stays, and consumers hold entries; that is intended.
* The engine side: a candle outage or restart revokes unfilled resting entry orders (`runtime.py:609`); open
  positions and trailing exits are candle-independent. A stalled engine reader (section 2.4) is not fixed by a
  second service; the one-pool-per-process change is.
* Holder single point: `rsibot-netns` is the unified plan's own single point.

## 5. V1 on the shared service

### 5.1 Seam

V1 (`ok_rsi_{bnb,btc,eth,sol,xrp}.yml`, `candles_config: []`) gets bars from Hummingbot's own
`CandlesFactory` -> `OKXSpotCandles` through `MarketDataProvider.get_candles_feed(config)`; the controller
calls `market_data_provider.get_candles_df(connector_name='okx', trading_pair, interval='1m', max_records)`
(`hl_rsi.py:3739-3748`, `ok_rsi.py:1086-1160`; max_records 500 base, 1275 for derived 5m/15m, up to 3660
for owner reconstruction). It is V1's worst error source (376 `Unexpected error occurred when listening to
public klines`, 178 WS closed, 45 network status errors in 8 days).

### 5.2 Minimal change

No controller change. Add `ServiceCandlesFeed(CandlesBase)` and register it in `CandlesFactory` for connector
`okx` when the service origin is configured. It implements the surface `MarketDataProvider` uses: `start()`,
`stop()`, `ready` (stream synchronized and `len >= max_records`), `candles_df` (the confirmed bars of the
process-wide `CandleStreamClient` replica, columns `timestamp, open, high, low, close, volume,
quote_asset_volume`, plus zero `n_trades/taker_*` as `chart_snapshots.py` treats them as optional),
`interval_in_seconds`, `max_records`, `rate_limits=[]`, `set_throttler` no-op. One shared client per process
(one stream, not five), multi-origin from day one. History request = max of the controllers' needs, 3660.
Differences to confirm in shadow before cutover: V1's feed also contains the forming bar and the controller
filters it with `timestamp + seconds <= now` (`_select_signal_candles`), so confirmed-only input is a subset
by construction; the service has no forming bar for `candles_df.iloc[-1]` consumers, and `rg` found no use of
a forming bar in `hl_rsi.py`, `ok_rsi.py` or `ok_rsi_core/` (only a comment in `chart_snapshots.py`).

Shipping: new engine image (`engine_source_sha256` changes), then `scripts/owner_restart.py v1 --image
sha256:<id>`. The tool writes a fresh cutover receipt that hashes the image and every `conf/` and `data/`
file, valid 14 minutes, and the entrypoint re-verifies it; adding the origin and token mount to the V1 compose
and any `conf` key is covered automatically by that receipt. The monitor's audited source pin
(`operations_execution_sources`, keyed by bot name) must be updated to the new image. Do not change V1 live;
rollback is restarting with the previous image id, which restores the in-process feed.

### 5.3 Parity proof

Evidence gathered now (read-only; recorder copied with its WAL and shm files to the scratchpad and opened
`mode=ro`; service cache read from the V2 cache directory):

| Check | Result |
|---|---|
| Bars: every V1 `ChartSnapshot` 1m row (open, high, low, close, volume) vs the service bar of the same open time, five pairs | **37,469 of 37,469 equal** (relative tolerance 1e-12, windows 2026-09-27T03:20Z to 2026-10-02T08:33Z), 0 missing in service. Reproduce: `python3 tools/v1_candle_parity.py bars --recorder-copy <copy> --cache-dir <cache>` |
| Indicators on the freshest rows: RSI via `pandas_ta` and ATR via the controller's `ewm(alpha=1/n, adjust=False)`, recomputed from the **service** bars vs V1's recorded values, rows since 2026-10-02 04:30Z (223-224 per pair) | max abs RSI diff 4.0e-7, max relative ATR diff 3.3e-10; all within the stated tolerances (RSI 1e-6 points, ATR 1e-6 relative) |
| Indicators on older rows | **Not a parity oracle.** 83-88 % of rows older than about four hours differ from a full-history recompute by 1 RSI point or more. A rolling window of `rsi_length + 1` bars reproduces 12 of 12 sampled old rows with error 0.0, so V1's chart store re-projects old rows from a short window; they are not decision-time values. Why the changeover is near 04:30Z is UNCONFIRMED |

Window dependence is not a concern for the decision values: Wilder smoothing decays as
`(1 - 1/n)^k`, below 1e-12 after 500 bars for n <= 17, so V1's 500/1275 window and the service's longer history give
the same last-bar RSI; the adapter must still return `tail(max_records)` exactly as the provider does.

Method for the cutover gate (decision-time proof, because recorded old chart rows cannot serve):

1. Shadow adapter: built into the image but disabled for decisions. It records per closed minute and pair a
   JSONL row: open time, SHA-256 of the exact frame the controller would receive, and RSI/ATR/EMA/BB/MACD from
   the controller's own code path via the same `market_data_provider.get_candles_df` seam.
2. Compare against V1's native feed for the same minutes (the controller writes both the live indicators and,
   for the freshest rows, the chart row). Pass: >= 24 h and >= 1,400 minutes per pair, bars equal
   100 % (OHLCV exact decimal), RSI within 1e-6, ATR within 1e-6 relative, EMA/BB/MACD within 1e-9 relative,
   and every minute where the adapter withheld (hold-down) explained by a service `status` event.
3. Replay harness: `tools/v1_candle_parity.py` (bars and indicators modes, refuses live recorder paths, uses
   `file:...?mode=ro`) plus `tools/test_v1_candle_parity.py` (synthetic fixtures, shows that a one-cent change
   and a restarted 40-bar window are both caught). Run inside the V1 image against a throwaway
   `OKRSIController` instance for the full signal diff (`signal`, `signal_score`, `regime`) with
   service-sourced frames versus recorded V1 frames; that run needs a container and was not done here.
4. Fault drills, fixture-only (no paper lane is running): inject a skewed clock, a forming bar in a REST page,
   a single-socket reset and a snapshot-parse stall into the Rust integration harness; none may produce a
   pair invalidation except the documented ones.

## 6. Quick fixes outside the candle service (not applied)

| Item | File / key | Evidence | Fix and risk |
|---|---|---|---|
| Rate oracle source unreachable | `conf/conf_client.yml` `rate_oracle_source: {name: binance}` (V1, V2; `global_token: USDT`) | 22 rate-oracle errors from Binance; advisory only (`stack_alert_observation.ADVISORY_LOGGERS`) | Set a reachable source from the fork's list (`kucoin`, `gate_io`, `mexc`, `coin_gecko`, `coinbase_advanced_trade`). There is no OKX source in this fork. Read at engine start, so it rides the next sanctioned restart and is covered by the V1 receipt hash. Risk low (conversion rates only) but unverified: confirm what consumes `RateOracle` rates before choosing |
| REST price-refresh timeouts | `market_data_provider.py:31` `_rates_request_timeout = 5.0`, `:920-990` `_safe_get_last_traded_prices(timeout=5)`; interval `rates_update_interval` (`:50`) | 611+ `Price refresh incomplete` in V2, 83 in V1; **all five pairs time out together** and recover in about 60 s | Do not raise timeouts: the pairs fail as a group, which points to the VM or network stalls of section 2.1, not slow tickers. Cheaper robust option: fall back to the already-connected order book mid price when the REST ticker times out, and keep the existing `OWNER_STALL_SECONDS` alert tolerance. Risk: a changed price source needs a parity test |
| `okx is not ready` | `strategy_v2_base.py:392` | 207 lines in clusters of about 6 at each engine boot (clusters at 09-29 12:31, 09-30 20:20, 10-01 14:20) | Boot chatter, not a runtime fault. Log once per boot (or at debug) to stop it inflating counts. Risk none |
| Candle health observed nowhere | `stack_v2_health.py` (container state only), `operations.pairs[].candle` null | status file and `v1`/`v2` alarm codes in 4.3 | Wire the status file; also a compose healthcheck is possible, but candles-rsi is on the auto-recovery allowlist and must be held first |
| Five streams per V2 engine | `data_reader.py` `attach_pool` keys pools by provider | 5 simultaneous disconnect groups | One `DataPool` per engine process cuts streams 5 to 1 and snapshot traffic 5x. Engine change, hash-bound for V3 |
| Host stalls | Docker Desktop VM | price refresh groups, clock events, `deadline` groups | Check VM CPU/memory pressure at the incident times (10:04Z, 11:49Z on 10-01; 20:03Z); heavy builds and simulators share the VM. UNCONFIRMED |

## 7. Rollout, rollback, risks

Order: (1) merge the hardening and build one image; (2) shadow: `candles-b` plus the status files and the A/B
check with no live change; (3) swap `candles-a` onto the new image (primary), watch `rejected`, `reconnects`
and digests for a day; (4) move V2 controllers to the shared origin per the unified plan; (5) V1 adapter in
shadow, then cutover with `owner_restart.py v1 --image`. Every live step is a separate approved action.

Rollback: previous image id `sha256:b9dbb6dc960d` for the service; the consumers file is optional and the old
`--token-file` keeps working. The status file and `/v1/metrics` are additive.

Risks:
* A confirmed flag is not an availability timestamp. A row received before its close is held until the
  local close; its original receive time remains visible and publication time records release.
* Two lanes double public sockets and subscriptions; connect pacing is per process, so four sockets from one IP
  is still far below OKX limits, but the REST budget is per process too. Stagger starts.
* Both lanes share fate on a network-wide flap (section 4.4; the 19:56Z event hit V2 and V3 within 7 s). They
  protect against per-connection resets and pong timeouts; the logs cannot say how many of the 19 events were
  per-connection, so the benefit is UNCONFIRMED until the new `reconnects` counters accumulate.
* `--consumers-file` holds tokens: keep it 0600 outside any repository and never commit it; validation errors
  deliberately omit token text.
* `reject_pair_data` still wipes the baseline for malformed confirmed rows. Intentional (distrust everything
  from that pair) but it means one genuinely corrupt OKX row costs a cold seed.

## 8. Unconfirmed

* Cause of the 10-01 10:04Z clock lag and of the deadline groups (section 2.4).
* Whether V3 clients run correctly against a 7514-capacity service (the unified doc also flags this).
* Why V1's chart rows switch from window-projected to decision values near 2026-10-02 04:30Z.
* V1 adapter parity of `signal`/`regime` outputs (needs a container run).
* Real-network behaviour of two lanes under flaps; tests are offline by design.
