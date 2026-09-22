> 2026-09-22 integration correction: the stress and recovery evidence modules now
> live in `controllers/generic/crash_recovery_v2_core/`, outside the frozen shared
> component closure. The modular spot digest remains
> `14e76708f297834597beaa7b7eb38b89aa00e5ce1f781b01a406a8264c625224`.
> Older component paths and seal-change observations below describe the original
> proposal; this relocation supersedes them. The host remains nonexecuting and
> the core evaluator is not an admitted economic research runner.

# Crash recovery v2: no-order implementation receipt

Date: 2026-09-20. Remote parent: `fd3592dff2993bb4c7a0ff69bb594d673362b02c`.
PR: [Hummingbot #62](https://github.com/s1korrrr/hummingbot/pull/62).

## Result and boundary

Implemented the absolute closed-candle/R0 evidence subset of M1, the pure
recipe/decision composition subset of M2, and a non-executing isolated host
`controllers/generic/crash_recovery_v2.py`. Evidence modules still cannot submit,
cancel, replace or sell an exchange order. The host emits no executor actions.
All recipe instances require `execution_enabled=false`; all returned traces retain
`execution_authorized=false`.

**106 tests passed** in the pinned Hummingbot 3.13 environment with warnings
treated as errors. Earlier 84-test claim was partial-source only. Native compiled
runtime import now succeeds in this worktree. Full native/legacy regression,
independent audit, real cache/supervisor state, economic baseline and deployment
acceptance remain not run.

## Implemented files

| File | Implemented responsibility |
| --- | --- |
| `hummingbot/strategy_v2/components/market_stress_v1.py` | Bounded canonical CandleSnapshot receipt validation; exact absolute downside comparison; revision/epoch/history/cursor checks; source-bound stress evidence. |
| `hummingbot/strategy_v2/components/recovery_evidence_v1.py` | NORMAL/SHOCK_HOLD/STABILIZING/RECOVERY_ELIGIBLE state, causal low/reclaim, fixed expiry, invalidation and parameter-bound checkpoints. |
| `controllers/generic/crash_recovery_v2_core/recipe.py` | Strict single-dataset, OKX/USDC/1m no-order recipe; actual RecipeBinding reuse; sealed off/stress-only/recovery treatments. |
| `controllers/generic/crash_recovery_v2_core/runtime.py` | Pure observation and Opportunity/Prerequisite/DecisionTrace composition, mandatory acquisition declarations, independent reduction cursor, bounded checkpoint codec. |
| `controllers/generic/crash_recovery_v2.py` | Isolated non-executing ControllerBase host, host source seal, CheckpointStore. Never `ok_rsi`/`rsi_v5`. |
| `test/crash_recovery_v2/` | Canonical-model contracts plus compiled-env admission, revoke, persist, restore, and isolation tests. |

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

M0 isolated host is in this worktree. M3/M4 compiled-env subset passed. Full
modular-native.yml, `v2_with_controllers` loader, Quants replay, cached May-August
economics, independent audit, and promotion remain open. M5 Research OS fail-closed
admission lives in root/`research_os`. Do not relabel as `ok_rsi` or `okx_spot_dca`.
Do not install this branch onto a live V1/V2 owner.

Rollback only this continuation's scoped source/test/document changes after
checking for later work. Preserve all native inventory, claims, risk state and
approved exits. No services were started, no orders were issued and no deployment
or merge occurred in this continuation.

## 2026-09-21 compiled-env host

Isolated host `controllers/generic/crash_recovery_v2.py` plus `CheckpointStore`.
`compose_crash_prerequisites` exports acquire-only `crash.*` checks. Traces keep
`execution_authorized=false`. Host `entry_permission` is false and
`determine_executor_actions` is empty.

Pinned env: `/Users/s1kor/anaconda3/envs/hummingbot/bin/python` with this
worktree compiled in place.

```
PYTHONPATH="$PWD" python -m pytest -q -W error test/crash_recovery_v2
106 passed in 25.23s
```

Native import of `hummingbot.connector.connector_base` succeeded. Local
modular-native.yml pytest selection: 1479 passed, 1 failed
(`test_queued_create_rechecks_drawdown_and_operator_events`, untouched
ok_rsi/v2 runner files). Independent audit, Quants replay, and cached-window
economics were not run. Research OS fail-closed program
`okx_spot_crash_recovery_v2` is a root/`research_os` change, not this nested
commit.


## CI selection

The existing `.github/workflows/modular-native.yml` now includes `test/crash_recovery_v2` in its native suite and watches the new core/doc paths. Existing jobs, actions, permissions and test selections are retained. YAML and embedded shell syntax are checked locally. The hosted GitHub Actions run is not claimed here. No new workflow or deployment action was added.
