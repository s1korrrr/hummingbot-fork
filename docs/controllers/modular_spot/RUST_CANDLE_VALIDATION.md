# Rust candle service: implementation, audit and public smoke

> Historical receipt for the first 90-row implementation. The subsequent native
> integration extends admitted history to 10,000 rows, adds validated persistent
> cache recovery and Linux packaging, and supplies full native controller wrappers.
> Its source identities and fresh evidence are recorded in
> `rsibot/docs/reports/2026-09-19-modular-native-completion.md` and
> [CONTROLLER_PARITY.md](CONTROLLER_PARITY.md). The earlier binary, 90-row ceiling,
> test counts and unvalidated-Linux statements below apply only to this original run.

Date: 2026-09-19. Owner: Hummingbot, isolated `feat/andrzej_modular_stack_validation` worktree. Authority: repository-process-only with public market-data reads and disposable local processes. No account API, orders, deployments, live-stack changes, component pins, commits or pushes.

## Original 90-row result

**PASS for the bounded Rust/native data integration smoke.** Implementation and independent source audit completed before the final run. The frozen candidate then ran for **900.014 seconds**, from 2026-09-19 19:25:49 to 19:40:49 UTC, after both actual Hummingbot DataPool consumers had all 294 requested 90-candle histories. The producer and readers exited cleanly after capture.

| Observed measurement | Reader 1 | Reader 2 |
|---|---:|---:|
| Incremental candle events | 4,287 | 4,287 |
| Rust receipt → adapter p50 | 1.37 ms | 1.43 ms |
| Rust receipt → adapter p95 | 17.80 ms | 18.06 ms |
| Rust receipt → adapter p99 | 28.51 ms | 28.23 ms |
| Rust receipt → adapter maximum | 29.83 ms | 31.20 ms |
| Rust receipt → native frame read p99 | 82.94 ms | 60.46 ms |
| Candle close → adapter p99 | 1.449 s | 1.449 s |
| Candle close → adapter maximum | 1.934 s | 1.935 s |

Timing distributions cover delivered incremental events, including those delivered during recovery; they **exclude snapshot/backfill delivery delays**. The summary's field `close_to_adapter_seconds_including_recovery` must be read with this qualification. The producer outage is separate evidence below. Clock offset used was +4.9 ms with approximately ±142 ms half-RTT uncertainty; the final clock sample was +40.6 ms with 355 ms RTT. Local wall-versus-monotonic movement was 5.5 ms. Venue-close timing is approximate, while the local latency target compares timestamps on this host.

All 14 eligible closed-minute intervals were complete by capture end: **4,116 pair-intervals per reader**, with no unresolved gaps. Thirteen intervals delivered all 294 pairs incrementally. During the deliberately interrupted interval, 171 arrived incrementally and 123 were recovered from history. The unaffected reader received no exemption for the other reader's restart.

- Reader restart: **0.311 seconds** to full readiness.
- Producer cold restart: **66.428 seconds** to full readiness, including the deliberate two-second stop. Initial readiness took 65.733 seconds from harness start. A producer restart therefore interrupts availability for roughly a minute; this is not a high-availability implementation.
- Final readiness: **294/294 on both readers**. Unexpected errors: **zero**. Startup and planned-restart connection errors are retained as expected events.
- Both producer epochs made exactly three upstream connection attempts. No spontaneous reconnect or parser-failure counter appeared. Pre-acknowledgement buffering was exercised 98 and 17 times. Last sampled ping/pong totals were 75/75 and 66/65; a sampled outstanding ping is not a timeout. No pong timeout was observed.
- Producer peak sampled RSS: **50.5 MiB**. Native frame reads checked: **8,574**.

Primary evidence: [summary](evidence/2026-09-19-rust-candles/smoke-15m-3/summary.json), [source identity](evidence/2026-09-19-rust-candles/smoke-15m-3/source.json), [health samples](evidence/2026-09-19-rust-candles/smoke-15m-3/health.json.xz), and [completion receipt](evidence/2026-09-19-rust-candles/completion-receipt.json). All 44 recorded source files and the tested binary remained byte-identical through the run.

## Implementation and supported contract

`services/okx-candle-service` is a standalone Rust/Tokio process. It maintains a bounded history of confirmed 1m OKX USDC spot candles. It uses at most 100 pairs per upstream business WebSocket, with shared connection-attempt pacing and four paced public REST seed/backfill requests in flight. These limits coordinate this process, not every application sharing the public IP.

It exposes an authenticated loopback WebSocket and health endpoint. Each consumer receives a seed snapshot, incremental confirmed-candle updates, explicit per-pair unavailable status and heartbeats. Sequence/epoch validation, bounded queues, slow-consumer disconnection and recovery snapshots prevent silent event loss. Forming candles are excluded. Disconnection, missing acknowledgement/pong, stale history, malformed rows and gaps have explicit failure/recovery paths. Known-pair malformed rows invalidate that pair; unbound protocol failures reconnect the affected socket group.

The Hummingbot `CandleStreamClient` is selected by `DataPolicy.transport=stream` in shared mode. It supplies the existing native Replica/DataPool boundary. Recipes select `data_transport=stream` before native export. Existing native/snapshot defaults remain unchanged. Sealed configurations reject post-export changes.

This supported native stream configuration uses **61–90 candles, subject to the configured signal's warmup requirement**, with 90 in the public test. The minimum is `max(60, 2 × rsi_length + 2, breakout_lookback + 2) + 1`; configurations requiring more than 90 are unsupported by this stream delivery. A longer strategy warmup must not be silently truncated. The service itself supports smaller diagnostic histories, but the native controller enforces its feature warmup minimum. The reference [rust-stream.json](recipe-examples/rust-stream.json) remains disabled and uses an unassigned account. It is not an economic recommendation or activation configuration.

PositionExecutor, OrderExecutor, connectors, order management, native fills/fees, recorder and execution owners remain unchanged. This is a data-service implementation, not proof of complete modular OK RSI/RSI V5 strategy parity.

## Verification and source identities

- Native combined modular/executor/orchestrator/candle suite: **605 passed +10 subtests**, zero skips or warnings, followed by **7 smoke-contract regression tests** after a harness-only manifest fix. Counts overlap and must not be added.
- Rust suite: **16 passed**: six actual upstream-session duplex tests, nine contract/state tests, one authenticated local stream ordering/shutdown test.
- `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, release build, scoped Python hooks and diff checks passed.
- `cargo audit`: zero known vulnerabilities and no warnings against advisory database commit `d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16`, updated 2026-09-19. This is a known-advisory check, not a proof of absence of vulnerabilities.
- Genuine native stream-90 recipe export, API schema helper roundtrip, and repaired PR #188 source/configuration handoff passed. These are local contract checks, not a deployed Hummingbot API test.
- Native Python source seal: `98b82026fecf5542ba8ae3992b973b6e9944b529cfe747d1871df966ec9379dc`.
- Final Rust release binary: `f7b509249bf3e02b83d76f4d4fd20d323a978176c368fb44a93f8c80fd72e61e`.

The native source seal covers Python modular code. Rust source, Cargo.lock, binary and universe have separate exact hashes in each smoke's `source.json`. Frozen source bytes and final binary are archived under root `.audit/rust-candles-20260919`. Build observed on macOS with Rust/Cargo 1.97.1 and the genuine Hummingbot conda runtime. Linux/container runtime has not been validated in this task.

## Audit findings and retained failed attempts

Independent source review covered Rust collection/state/distribution, the Python adapter, native configuration wiring and smoke acceptance. The following material issues were repaired before the final run:

1. Strict native confirmed-close timestamp parity, instead of permitting a future closed candle.
2. Per-pair invalidation/generation protection for malformed rows and stale REST results.
3. Smoke acceptance requires streaming delivery outside declared interruption windows; recovered snapshots cannot hide missed live updates.
4. Monotonic duration/deadlines, final clock evidence, explicit consumer recovery deadline and precise outage bookkeeping.
5. Genuine recipe selection of stream transport and early rejection of unsupported history.
6. Real Rust upstream-session tests for busy traffic without pong, valid repeated pongs, missing acknowledgements and EOF/reseed.

The two-pair 150-second preflight passed its scope (under 3ms local delivery, no errors), on an earlier binary. Its generic full-smoke `passed` field is false because it did not attempt 900 seconds or planned restarts; its separate preflight receipt explains this.

All-pair attempt 1 aborted before collection: the harness passed a provenance-rich catalogue to Rust's strict pairs-only manifest parser. The harness now creates and hashes a separate producer manifest; the strict service schema remains unchanged.

All-pair attempt 2 exposed a real exchange ordering case: requested candle data can arrive before its subscription acknowledgement. The collector previously disconnected. It now buffers only requested, validated rows within the existing capacity, while remaining offline/unready until explicit acknowledgement and valid seed. ACK deadlines and foreign-pair rejection remain enforced. Two actual duplex regressions cover this, and the health counter `buffered_before_ack` records occurrences. The run was deliberately stopped; it is not completed-duration evidence. Independent review approved the repair before attempt 3.

## Measurement and acceptance

The public universe is frozen from live OKX SPOT instruments quoted in USDC: **294 pairs**. Catalogue membership does not prove account-specific trading eligibility. Only exchange-confirmed rows enter the native data boundary.

Three timings remain distinct:

- Candle-close → adapter: venue confirmation/publication plus network plus local work, corrected with measured public-clock offset and its RTT uncertainty.
- Rust upstream receipt → Python adapter acceptance: local transport/processing delay. Proposed target p99 below100ms for each consumer.
- Rust receipt → actual DataPool snapshot/frame read: observed native boundary usability, including harness scheduling and frame conversion; not strategy decision or order latency.

Coverage includes every complete interval opened after all consumers are ready and closed at least ten seconds before capture ends. Every pair must be available by the end, including any backfilled interruption bars. Outside explicitly recorded planned outage windows, every expected bar must also have arrived incrementally. The unaffected consumer receives no exemption for the other consumer's restart. Unexpected reader or native-frame failures fail the run. The surviving final state must have all pairs ready.

## Reproduction

From `services/okx-candle-service`:

```sh
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --release --locked
cargo audit
```

From this Hummingbot worktree, using an exclusive new output directory:

```sh
PYTHONPATH="$PWD" /Users/s1kor/anaconda3/envs/hummingbot/bin/python \
  docs/controllers/modular_spot/rust_candle_smoke.py \
  --universe docs/controllers/modular_spot/evidence/2026-09-19-rust-candles/universe.json \
  --output /private/tmp/new-rust-candle-smoke --seconds 900
```

The exact source-bound executed command and final receipt are retained with the evidence. The harness owns only its child producer, two data pools, temporary token and loopback port. It does not create an execution connector or interact with live owners.

## Operational boundary and rollback

The tested topology is local Unix processes on authenticated loopback. A container deployment requires explicit network-namespace or TLS-proxy binding, immutable source/binary identity, operator configuration and its own deployment validation. The existing live stack has not been enrolled. A 15-minute smoke is bounded evidence, not a permanent-availability promise or long-duration soak.

Rollback requires stopping only the named disposable Rust process/readers and retaining the prior default transport. No live service restart is required for these uninstalled changes. The preserved pre-task candidate and unrelated root/nested work remain protected by the task archive. All 13 live-stack container IDs, image IDs and start timestamps match the pre-task capture. Root and nested Hummingbot HEADs and short Git statuses also match their pre-task captures. These checks prove no observed restart or checkout-state change; they do not claim byte-level equality of mutable live databases. All implementation changes remain local and uncommitted.

## Next integration work

The next gate is a Linux/container build and isolated Hummingbot API topology test using the exact packaged engine, followed by a longer read-only soak. This does not require modifying the current live stack. Cold-restart history recovery and the 90-candle ceiling must be evaluated against the required strategy warmups and outage budget before deployment. Full OK RSI/RSI V5 signal, sizing, exit and recovery parity remains separate required work before either existing owner can adopt modular recipes. No new executor/accounting implementation is proposed.
