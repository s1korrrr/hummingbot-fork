# Rust candle service and native adapter

## Goal
Implement the approved dedicated Rust candle service, verify/audit it, then run a 900-second public all-live-OKX-USDC-spot 1m smoke with two actual Hummingbot data consumers. Target local receipt-to-adapter p99 <100ms; report venue-close delay separately. Production implementation readiness requires tests/audit and no known important defect; the smoke does not authorize deployment or trading.

## Current State
Owner is the existing Hummingbot task worktree `.worktrees/hummingbot-modular-integration-pr57`, branch `feat/andrzej_modular_stack_validation` at068702141f1b plus preserved candidate. Native source prior seal db029530c1990f8caa1892656e758fccec4e4489461378b45ebfcbf2096cda37. Prior public294pair diagnostic passed but actual publisher is per-pair native feeds plus snapshot polling. Python native executors, accounting and legacy controllers remain unchanged. Root/nested unrelated dirt and candidate archived in root `.audit/rust-candles-20260919`.

## Target State
Authority repository-process-only: public unauthenticated market data, local processes and repository edits. Forbidden: live rsibot-stack/config/data/secrets/order/account surfaces, deployments, pins, credentials, pushes/merges. Existing user approval covers this design and implementation; no new approval stage is necessary.

One service under `services/okx-candle-service`, opt-in stream adapter in modular_spot_core, default existing transport unchanged. A small shared connection pool (100pairs/socket), service-wide connection pacing with headroom for untouched processes sharing the IP, text ping/pong independent of traffic, bounded retries/backoff, history90 by default, 1m confirmed only, one bounded buffer perpair, per-symbol health. Fixed OKX public upstream URLs, TLS validation, no env proxies/redirects, loopback-only listener and bearer token from mode0600 file. REST seed/backfill uses native public candle semantics, bounded requests/rate/concurrency; no trading connectors. History continuity and source freshness are required; forming bars never enter confirmed history. No historical replay triggers entry eligibility.

### Wire contract v1
Loopback authenticated WebSocket `/v1/stream`; first client message `{"v":1,"op":"subscribe","pairs":["BTC-USDC"],"history":90}`. Subset unique catalog pairs, history within configured capacity. Server producer epoch unique perprocess, perpair monotonic sequence. JSON numeric decimal candle fields are strings; timestamps seconds. Every server frame has `v:1`, `type`, `epoch`, `published_at` (wall seconds).
- `snapshot`: pair, sequence, received_at (upstream receipt wall seconds), rows array of confirmed `{open_time,open,high,low,close,volume,quote_volume,confirmed:true}`. Initial/recovery seed, not entry-triggering.
- `candle`: pair, sequence, received_at, row (same shape). Contiguous append or latest-bar correction; duplicate identical bar ignored upstream. Sequence gaps force client resynchronization.
- `status`: pair, sequence, ready:false, reason. Invalidate only that pair; next ready message is a fresh snapshot.
- `heartbeat`: states map of subscribed pair -> `{ready:bool,sequence:integer}`, every2seconds. Readiness requires live acknowledged upstream and contiguous current-enough history. Missing/extra pairs or mismatched epoch/sequence invalidate reader. Same-sequence healthy heartbeat may refresh transport receipt, never candle age.
Health GET `/health` authenticated returns counters and perpairhealth; no write/admin endpoints. Stream backlog bounded; lagging clients disconnect/reseed, never silent confirmed-event loss. On connection start subscribe receiver BEFORE taking initial state snapshot, filter queued events <=perpair sentsequence. Snapshot and delta ordering serialized perclient.

## Risks and Failure Modes
Venue confirmation delay is outside local SLA. Warmup/reconnect REST rate pressure; clock skew; gaps/corrections/duplicates; slowconsumer backpressure; sequence/epoch mismatch; accidental loss of entry-seed semantics; oversized/untrusted frames; changed native source seals. Audit these. A service/process restart causes rewarm/reseed, not stale trading eligibility. Existing exits remain independent.

## Milestones
- [x] R1 Rust strict parser/state/protocol/server/collector, unit+failure tests, fmt/clippy/releasebuild.
- [x] R2 Python opt-in stream adapter and native DataPool binding with failure/parity tests.
- [x] R3 Independent audit, source-bound configuration/API/handoff compatibility, fixes and final source freeze.
- [x] R4 900-second public allUSDC smoke after readiness; two native consumers, exact source/binary/universe, perminute coverage, latency, planned disconnect/recovery, no unobserved loss. Preserve failures.
- [x] R5 Final reports/plan, beforeafter live/workspace identity, intentional patch/archive and rollback.

## Verification
`cargo fmt --check`; `cargo test --locked`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo build --release --locked` under service. Native Hummingbot conda Python with candidate PYTHONPATH: focused adapter + full modular/native executor/orchestrator/candle suites, existing hooks. Parent-owned `docs/controllers/modular_spot/rust_candle_smoke.py` performs local offline preflight then900second public test with frozen universe. Audit before finalsmoke; no capital actions. A failed check is repaired and rerun at relevant scope.

## Decision Log
2026-09-19 User approved Rust service + verification/audit and15minute smoke. Keep one existing task branch and separate newservice from native execution. Push updates remove polling overhead; Rust speed claim requires measurements. Current-live universe refetched immediately before smoke and retained. Snapshot90 history is bounded initial/recovery state; deltascarryoneconfirmedbar.

## Progress Log
2026-09-19 Pre-edit root/nested/candidate state and13livecontainer identities preserved. Rust1.97.1/cargo1.97.1 available. Parent owns plan, harness, finalintegration; workers own disjointRust/Pythonpaths; independent reviewer audits finalsource.

## Rollback / Recovery
Stop only named test PIDs and closeowned sockets. Defaults remain previous transport; no live deployment. Revert only taskdiff against archive; preserve preexistinguncommitted candidate and unrelatedroot/nested work. No service restart required. Production deployment remains separate authority.

2026-09-19 Implementation: Rust10tests+nativefixture parity+realwireauth/ordering/shutdown, fmt/clippy/release passed; advisoryaudit0vulnerabilities/no warnings. Native603tests+10subtests passed before finalrecipe transport wiring. Independentreview fixed strictclosebound andperpairmalformedrowisolation; smokenowrequiresincrementalcoverageoutsideexplicitoutages, monotonicduration andboundedrecovery.150second2pairpreflight healthy: 6confirmedupdates/reader, p99delivery2.76ms/2.33ms, noerrors. Full900secondacceptance notyetexecuted.

2026-09-19 Native recipe/compiler/profile now explicitlyselects data_transport=stream at history90; unsupportedhistory rejects early. Existing defaults snapshot300 unchanged. Finalnative605tests+10subtests pass, exactAPI_schema helperdefault+stream configsvalidate disabled, repairedroot188handoff accepts genuine sealedexport. Native source98b82026fecf5542ba8ae3992b973b6e9944b529cfe747d1871df966ec9379dc. RustactualupstreamduplexACK/pong/EOF regressions added beforefinalreleasefreeze.

2026-09-19 Finalsourceaudit APPROVED_FOR_900_SECOND_PUBLIC_SMOKE, receipt audit-before-smoke.json. Frozen release0086ee6f69bdadc887a4375fa3a755e0acefba0ff558cc3b505137c62dd2c4fb. 900-second all294pairrun started in smoke-15m-1; no deployment/activation.

2026-09-19 Final result: smoke-15m-3 PASS on release f7b509249bf3e02b83d76f4d4fd20d323a978176c368fb44a93f8c80fd72e61e after strict-manifest and real pre-ACK ordering repairs. 900.014 seconds, 294 pairs, two native consumers, 14 complete scored intervals each, 4,287 incremental events each. Local p99 28.51/28.23ms; reader restart .311s; producer cold recovery66.428s, with123pair-bars backfilled perreader in the interrupted interval. No unexpectederrors; finalallready. All44sourcefiles andbinary unchanged. Parent confirmed16Rusttests/fmt/clippy afterrun. Native605+10subtests and7harness regressions retained with their scope.13livecontainer identities/starttimes unchanged; root/nestedHEAD/status unchanged. See RUST_CANDLE_VALIDATION.md and completion-receipt.json for exact evidence, expected startup/restart errors, clock uncertainty and deployment limits. Sourceimplementation/smoke complete; deployment, Linux/container integration, longsoak andfulllegacystrategyparity remain separate work.
