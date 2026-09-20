# Crash recovery v2: no-order implementation receipt

Date: 2026-09-20. Remote parent: `fd3592dff2993bb4c7a0ff69bb594d673362b02c`.
PR: [Hummingbot #62](https://github.com/s1korrrr/hummingbot/pull/62).

## Result and boundary

Implemented the absolute closed-candle/R0 evidence subset of M1 and the pure
recipe/decision composition subset of M2. This is usable policy code with tests,
not a ControllerBase, a native execution adapter or a backtest result. Nothing
here can submit, cancel, replace or sell an exchange order. All recipe instances
require `execution_enabled=false`; all returned traces retain
`execution_authorized=false`.

**84 tests passed**, with warnings treated as errors, in a fresh partial-source
workspace using Python 3.13.5, Pydantic 2.13.4 and pandas 2.2.3. Native compiled
runtime, full native/legacy regression, independent audit, real cache/supervisor
state, economic baseline and deployment acceptance remain unavailable/not run.
The native import probe failed; it was not skipped or replaced by fake executors.

## Implemented files

| File | Implemented responsibility |
| --- | --- |
| `hummingbot/strategy_v2/components/market_stress_v1.py` | Bounded canonical CandleSnapshot receipt validation; exact absolute downside comparison; revision/epoch/history/cursor checks; source-bound stress evidence. |
| `hummingbot/strategy_v2/components/recovery_evidence_v1.py` | NORMAL/SHOCK_HOLD/STABILIZING/RECOVERY_ELIGIBLE state, causal low/reclaim, fixed expiry, invalidation and parameter-bound checkpoints. |
| `controllers/generic/crash_recovery_v2_core/recipe.py` | Strict single-dataset, OKX/USDC/1m no-order recipe; actual RecipeBinding reuse; sealed off/stress-only/recovery treatments. |
| `controllers/generic/crash_recovery_v2_core/runtime.py` | Pure observation and Opportunity/Prerequisite/DecisionTrace composition, mandatory acquisition declarations, independent reduction cursor, bounded checkpoint codec. |
| `test/crash_recovery_v2/` | Real canonical-model contract tests, deterministic transitions, corruption/replay/freshness cases, no-I/O checks and source-seal impact tests. |

The package initializer starts nothing. No native controller entrypoint or exact
model-admission exception was added. Existing canonical helpers and legacy
controllers/configurations/defaults/persistence were not edited.

## Exact first-revision semantics

`ReceivedCandles` wraps the existing canonical `CandleSnapshot` and an explicit
integer-millisecond first receipt time. The adapter must supply that time from
real observation evidence, not a later polling time. Publication seconds convert
upwards to milliseconds; event time is the completed minute close. Candle,
publication and receipt freshness are separate checks. Complete declared history
is required; no OHLC geometry/finality/gap failure is repaired or imputed.

The absolute detector compares the latest close against the highest close among
the explicitly selected preceding `lookback_bars`. It excludes the current candle
from that reference. Comparisons use exact rational arithmetic so ambient Decimal
precision cannot change a threshold decision. Thresholds are required inputs,
not recommended or optimized trading values. Methods other than
`closed_drawdown_v1` and `closed_reclaim_v1` reject; normalized/order-flow/learned
models and intrabar protection are not implemented.

A new observation seeds without acquisition. Repeated snapshots do not advance
stabilization or renew original availability. Revisions, overlapping-history
corrections, event gaps and epoch changes invalidate/reseed evidence rather than
replaying buys. Bad data preserves a shock and invalidates recovery eligibility.
A missing recovery checkpoint beside established stress history rejects.

A new shock outside SHOCK_HOLD starts a fresh market-low reference; consecutive
shock observations extend its minimum. This prevents an old recovered low from
qualifying an unrelated new shock. It never resets capital or inventory: neither
is owned by these reducers. Small new lows accumulate against the last qualifying
low before the configured tolerance triggers a stabilization reset.

Reclaim requires a new valid completed event and the configured elapsed interval.
Eligibility has a fixed evidence-relative deadline; duplicate observations cannot
renew it. Expiry or invalidated evidence requires fresh stabilization. All
parameters, dataset identity, cursors and state are bounded/validated, including
programmatically copied invalid Pydantic objects. Numeric evidence is limited to
40 coefficient digits and exponent magnitude 40 before rational conversion;
unsupported representations reject instead of consuming unbounded resources.

## Composition and checkpoint limits

The caller supplies a baseline opportunity and independently obtained native
prerequisite declarations. Missing `data`, `funding`, `native_admission`,
`ownership` or `risk` is UNAVAILABLE, not PASS. Caller-supplied `crash.*` names and
duplicate prerequisites reject. Recovery evidence cannot overwrite those checks.
The canonical decision protocol retains its seeding, expiry, consumption and
event-regression semantics. The overlay runs even with no new opportunity.

Reduction traces use their own cursor and the caller's reduction prerequisites;
they do not inherit BUY funding or recovery checks. This is a pure composition
test, not proof that live protective exits run. M3 must establish that separately.
Only one explicit candle dataset is currently supported. Extra acquisition
dependencies, proxy recipes and other venue/mode combinations reject.

The recipe seals baseline/exit references, a finite configured budget, acquisition
parameters, dataset and overlay mode. These are declarations, not proof of
baseline quality, balance or owner authority. `bind_recipe` consumes a source
hash independently verified by its caller; it does not inspect an installed
image. M0/M3 must establish that real binding.

Checkpoint encode/decode is bounded to 2,000,000 UTF-8 bytes, rejects ambiguous JSON
and verifies an externally retained digest plus recipe/source binding. It does
not write a database, implement CAS, attest ownership or replace native restore.
M4 remains open. A missing checkpoint with declared native obligations rejects.

## Verification actually run

```text
PYTHONPATH=. python -m pytest -q -W error test/crash_recovery_v2
84 passed in 2.10s

python -m compileall -q controllers/generic/crash_recovery_v2_core \
  hummingbot/strategy_v2/components/market_stress_v1.py \
  hummingbot/strategy_v2/components/recovery_evidence_v1.py test/crash_recovery_v2
exit 0
```

The initial failing tests preceded implementation. Additional red-to-green
regressions caught delayed first-receipt freshness, missing price evidence,
missing recovery checkpoint and old-low reuse. One test originally expected a
HOLD for a regressing event; inspection showed the genuine canonical protocol
raises, so that expectation was corrected without changing the protocol. A
subprocess test initially resolved Python's standard `test` package instead of
the task tests; its test-path setup was repaired without production changes.
The complete final new suite passed after those repairs.

The exact source hashes, command results and unsuccessful-attempt history are in
[VALIDATION.json](VALIDATION.json). Tests use synthetic candle fixtures, not
historical market data or economic fills. The source-impact test copies actual
component source files to a temporary directory and invokes the unchanged source
scanner/binding verifier; it demonstrates changed seals, not native deployment.

## Environment and preservation

Direct Git clone failed with `Could not resolve host: github.com`. Connected
GitHub reads/writes remained available. Eight canonical source modules were
reconstructed in the disposable workspace and verified byte-for-byte by Git blob
ID. Parent packages use namespace layout here: the complete native package
initialization, installed engine and broad legacy tests were not reproduced.
No connector, executor, recorder or fill simulator was mocked into production.

The actual native probe was:

```text
PYTHONPATH=. python -c 'from hummingbot.connector.connector_base import ConnectorBase; from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor'
ModuleNotFoundError: No module named 'hummingbot.connector'
exit 1
```

The new component files alter the recursive component source seal, as the spec
warned. Old images, source bundles, root pins, configurations and existing owner
state were not changed or resealed. This branch must not be installed into an
existing V1 owner to make its guard pass.

## Remaining gates and next action

M0 native host/package admission is still held. M2 has no verified actual v2
baseline. M3 native loader/factory/funding/risk/permission-revocation tests and M4
native persistence/cancel/late-fill/restore tests remain unimplemented. M5 needs
the actual Quants/program/result admission. M6-M8 have no economic or promotion
result. Independent review is NOT RUN; author review is not represented as a
separate audit.

The next implementation action is to run this new suite in the pinned owning
native environment, verify the actual isolated v2 host and source-impact matrix,
then implement acquisition-only native integration without modifying legacy
semantics. Resume existing research state before any separately authorized full
cached May-August economic comparison. Never substitute a shorter run or legacy
program label.

Rollback only this continuation's scoped source/test/document changes after
checking for later work. Preserve all native inventory, claims, risk state and
approved exits. No services were started, no orders were issued and no deployment
or merge occurred in this continuation.

## CI selection

The existing `.github/workflows/modular-native.yml` now includes `test/crash_recovery_v2` in its native suite and watches the new core/doc paths. Existing jobs, actions, permissions and test selections are retained. YAML and embedded shell syntax are checked locally; the hosted workflow and full native suite were not run here. No new workflow or deployment action was added.
