# All-USDC candle and modular integration validation

Date: 2026-09-19. Authority: repository-process-only, isolated public data reads and local validation. No credentials, exchange account calls, orders, activation, deployment, live-stack writes/restarts, component pins, commits, pushes or merges.

## Result and source identity

The first requested all-symbol test passed on **294 currently live OKX USDC spot pairs**, using three public business WebSocket connections and native Hummingbot WSAssistant/OKXSpotCandles parsing. This is a bounded multiplexed transport diagnostic, not a production publisher deployment. The combined repaired native candidate passes **563 tests and 10 subtests, zero skips or warnings**.

The integrated Hummingbot checkout is `.worktrees/hummingbot-modular-integration-pr57`, branch `feat/andrzej_modular_stack_validation`, based on PR #57 commit `068702141f1bba09a70fe14dee85beb8516f09aa`, with the preserved/repaired uncommitted PR #55 implementation reconciled on top. The original PR #55 worktree remains preserved. Its corrected specification, plan and prior validation receipt are carried forward here; PR #57 desk documentation remains alongside them. Counts from earlier receipts are historical, not additive evidence.

API changes are isolated at `.worktrees/hummingbot-api-modular-validation`, base `1d53cf58b39a2fe9cc38d189966fead1072ccbd3`. Root PR #188 validation uses exact head `acd1f83c458dfaf8a7d6e3eb60ba93f95ad513ff` (implementation checkpoint `aafdf5e98a68512855df024b6d77bec33714fe3f`). All changes remain local and uncommitted.

## Public WebSocket evidence

Frozen public response and sorted universe: [universe.json](evidence/2026-09-19-universe/universe.json) and [instruments-response.json](evidence/2026-09-19-rust-candles/instruments-response.json.gz) (gzip of the byte-identical response, SHA-256 `401ecddf47cb4829a0cf281c47b0cbf1e4f054da33af2cea1e9c10431ed75279` uncompressed). Filter: SPOT, quoteCcy USDC, state live; catalog availability is not account trading eligibility.

| Measurement | Observed result |
|---|---:|
| Acknowledged and updating pairs | 294 / 294 |
| Steady observation after all acknowledgements | 300.202 seconds |
| Complete eligible 1m intervals | 4 |
| Confirmed bars in those intervals | 1,176 / 1,176 |
| Candle-close-to-receipt median / p95 / p99 / maximum | 0.943 / 1.419 / 1.449 / 1.471 seconds |
| Ping RTT median / p95 / maximum | 0.267 / 0.522 / 0.618 seconds |
| Maximum event-loop delay | 5.98 ms |
| Initial update for every pair, including setup/clock samples | 6.41 seconds |
| Planned socket disconnect → complete re-subscription | 2.422 seconds |
| Reconnected group with subsequent confirmed candles | 100 / 100 |
| Unexpected errors, corrections, duplicate confirmations | 0 / 0 / 0 |
| Process CPU / peak RSS | 2.34 seconds / 140.6 MB |

[Run 2 summary](evidence/2026-09-19-universe/run-2/summary.json) and [raw confirmed receipts](evidence/2026-09-19-universe/run-2/confirmed-bars.jsonl) are the evidence. Run 1 failed the planned disconnect: a native close frame raised TypeError outside the diagnostic retry handler. Its [failed summary](evidence/2026-09-19-universe/run-1/summary.json) is retained. The handler now retries that failure; regression tests cover recovery/reconfirmation, busy traffic with missing pong, bounded attempts, and exclusion of post-capture data. Run 2 includes exactly one expected injected error, not a claim of zero errors of every kind.

The candle timestamp denotes candle **open**, not venue send time. Delay is local receipt plus measured clock offset minus open+60 seconds. The chosen offset is +13.2 ms with approximately ±131 ms midpoint uncertainty from the minimum-RTT clock sample. These values measure closed-candle availability, not one-way network latency. Only exchange-confirmed rows count; complete intervals start after full subscription and finish with 10 seconds of observation grace. Provisional updates and initial replay do not establish closed-bar coverage.

The test retains TLS validation, uses no credentials, paces connection attempts at least 1.05 seconds apart, bounds subscription payloads and retries, checks subscription identities, sends application ping, and enforces pong deadlines even on busy streams. These follow the official [OKX WebSocket limits](https://www.okx.com/docs-v5/en/#overview-websocket-connect) and [candlestick channel contract](https://app.okx.com/docs-v5/en/#order-book-trading-market-data-ws-candlesticks-channel). A five-minute test with one injected interruption cannot establish permanent availability, an SLA, or behavior through every venue/network failure.

## Actual publisher and reader capacity

A separate local probe uses real native OKXSpotCandles objects, SnapshotHub and two CandleClients with 294 synthetic 90-bar histories. It does not open public connections. This isolates application overhead from exchange/network behavior.

| Measurement | Before | After |
|---|---:|---:|
| Both readers ready for all datasets | 2.137 s | 1.529 s |
| Median complete polling cycle | 1.441 s | 1.123–1.150 s |
| Maximum event-loop stall | 553 ms | 94 ms |
| Event-loop lag p95 | 2.68 ms | 15.89 ms |
| CPU in approximately 25.4 seconds | 19.27 s | 18.14 s |
| Peak RSS | 304 MB | 308 MB |
| Complete cycles per reader | 11 | 12 |
| Errors/timeouts | 0 | 0 |

Capture now yields between datasets; polling uses at most eight concurrent requests. A protocol failure cancels sibling requests, clears cached state and disconnects; per-symbol HTTP503 still isolates that symbol. Whole-universe async capture takes median 1.133 s, maximum 1.219 s including scheduling. Yielding reduces the longest stall but raises p95 scheduling delay. Poll cycles additionally sleep one second; this is not a subsecond end-to-end guarantee.

**Remaining data-path gap:** the production collector still starts native per-pair feeds. It lacks shared IP-wide connection-attempt pacing and the diagnostic's OKX text-heartbeat/reconnect management. We deliberately did not open 294 production feed sockets. The modular consumer currently polls `GET /v1/snapshot/{key}` with ETags; it does not consume `/v1/stream`. Exchange candles are WebSocket-sourced, but the final all-pair publisher→consumer path still needs an integrated proof. Existing earlier two-pair native public pipeline evidence remains valid only at that scale.

## Compatibility fixes and limits

- PR #57 recipe configuration now forwards account, native-owner mapping, benchmark datasets and program/policy revisions into the native profile. Genuine native validation stays mandatory; there is no native-free export claim.
- The native report projects bounded modular status and recipe identity through `get_custom_info`, tested through the real native V2 controller report. Execution and acquisition remain disabled by default.
- Hummingbot API preserves nested schema and original union/null alternatives, materializes JSON-safe factory defaults, and protects modular native source from managed-source replacement. Focused API suite: **112 passed**, eight pre-existing dependency warnings. Independent review caught and verified the nullable-schema repair.
- PR #188's handoff now preserves valid explicit native owner IDs as well as generated IDs, retaining source/config/binding seal verification. Its suite passes **19 tests**; genuine default and custom-owner exports independently pass the repaired helper.
- Stack integration remains a packaging gap: the generic read/write controllers mount can hide the image's native bundle. A sealed immutable controller bundle and an isolated API/bot-management smoke are required before deployment. Nothing was installed into the live stack.
- PR #57 feature, decision, capital and exit libraries are tested foundations, but are not wired into the controller's action path. The overall 22-block desk program is incomplete.

## Native reuse and next implementation order

Keep Hummingbot's native PositionExecutor, OrderExecutor, connector, recorder, fills, fee accounting and orchestrator. ModularSpotController remains native composition. Sell-only uses a native OrderExecutor LIMIT SELL CLOSE specialization because it reduces held spot inventory; a SELL PositionExecutor is not an interchangeable representation. No replacement execution engine or duplicate fill/PnL ledger is proposed.

BudgetStore coordinates programs sharing one recorder/configuration. It is not an account-wide lock across the existing owners' separate databases. This does not justify building a distributed allocator now: first prove one explicitly bounded owner/program. Sharing the live wallet with other independently owned programs requires a separate allocation/ownership decision.

| Capability to preserve | Existing implementation to reuse/compare | Required proof |
|---|---|---|
| Confirmed candles and higher timeframe derivation | `directional_trading/ok_rsi_core/candles.py`, `ClosedCandleProvider` | Same closed-bar inputs, warmup, freshness and gaps |
| Entry decisions and confirmation | `ok_rsi_core/entry_decisions.py`, `rsi_v5_core/entry.py` | Same intents and rejection reasons for frozen inputs |
| Maker pricing, down-only acquisition and rest lifecycle | `ok_rsi_core/entry_pricing.py`, `down_only_entry.py`, existing RSI V5 entry path | Same native order intent, partial fills, cancel/reject/retry behavior |
| Profit floor, trailing and exits | `ok_rsi_core/trailing.py`, `rsi_v5_core/exit.py` | Same fee-aware floor and retained high-water; exits continue during entry pause |
| Exposure, slots, cooldown and loss rails | `rsi_v5_core/risk.py`, controller runtime risk gates | Same held+filled+pending accounting and bounded admission |
| Recovery, flash-crash/bag protection and resume | `EpisodeRuntime`, `rsi_v5_core/state_machine.py`, native runtime store | No historical re-entry, cost reset, lost trailing state or unowned reduction |

Recommended sequence:

1. Finish the production all-pair data path using a bounded shared OKX subscription lifecycle while retaining native parsing and candle semantics. Prove reconnect/backfill/gaps, stale isolation and time-to-controller with two real consumers. Choose HTTP versus the existing stream transport based on measured delay, then run a longer isolated soak. Do not infer this from the diagnostic.
2. Complete a capability/parameter matrix for OK RSI and RSI V5, then port one existing policy slice at a time. Start with one fixed-pair OK RSI recipe. Use pure existing policies and differential tests; the current baseline RSI/breakout policies are not semantic parity.
3. Bind one immutable native bundle through Hummingbot API and an isolated disposable stack, proving config roundtrip, native lifecycle, reporting and recovery without live owners or orders.
4. After parity, use the fixed cached May–August 2026 research window with the correct owner adapter and baseline. Economic testing and operational/activation decisions are distinct work; this pass runs no backtests or trading.

## Reproduction, validation and rollback

Run in the genuine Hummingbot environment, with this checkout on PYTHONPATH:

```bash
PYTHONPATH="$PWD" /Users/s1kor/anaconda3/envs/hummingbot/bin/python docs/controllers/modular_spot/public_universe_benchmark.py --universe docs/controllers/modular_spot/evidence/2026-09-19-universe/universe.json --output /tmp/new-exclusive-universe-run --seconds 300
PYTHONPATH="$PWD" /Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest -q test/modular_spot test/hummingbot/strategy_v2/executors/position_executor test/hummingbot/strategy_v2/executors/order_executor test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/hummingbot/data_feed/candles_feed/okx_spot_candles test/hummingbot/data_feed/candles_feed/test_candles_base.py
```

The combined command passed **563 tests +10 subtests**. All applicable scoped pre-commit hooks passed; checks for unrelated file types reported no applicable files. Independent reviews covered transport changes, recipe/native-report integration and API schema/source management. Final source seal: `db029530c1990f8caa1892656e758fccec4e4489461378b45ebfcbf2096cda37`. The [compatibility receipt](evidence/2026-09-19-universe/compatibility/receipt.json) binds actual exports, API results and handoff packets. Root-helper suite: 19 passed. No unrun live test or economic result is implied.

Rollback is local: stop only diagnostic-owned processes, then revert this task's intentional diff in the isolated worktrees. Preserve the pre-existing PR #55 candidate and root/nested dirt. Preservation archives are under root `.audit/pr55-validation-20260919` and `.audit/pr55-universe-20260919`. No service restart is required for these uninstalled changes. Final read-only inspection confirms all 13 live containers retain identical IDs, images and start times. Nested Hummingbot dirty status is unchanged. Root status changed only for the unrelated quants-lab gitlink during concurrent workspace work; this task left it untouched. See [preservation.json](evidence/2026-09-19-universe/preservation.json).
