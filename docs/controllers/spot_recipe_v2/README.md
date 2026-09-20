# Spot lifecycle v2: native-evidence and proposal foundation

Implementation companion to `s1korrrr/rsibot` PR #197. This is a tested, non-executing
foundation, not a registered controller, live owner, economic adapter or new ledger.

## Implemented

- `hummingbot.strategy_v2.components.spot_lifecycle_v2`: immutable, bounded native
  scope/receipt/commitment records; lifecycle and management-universe projections;
  source-referenced continuity checks; strict bounded JSON round trips.
- `controllers.generic.spot_recipe_v2_core.native_views`: a read-only adapter for
  the existing **entry-terminal** native PositionExecutor receipt shape, and an
  exit assessment bridge calling the existing `arbitrate_exit` implementation.
- `controllers.generic.spot_recipe_v2_core.acquisition`: an initial-entry or DCA
  lifecycle prerequisite composed through canonical `Prerequisite`, `decide`,
  `DecisionCursor` and `DecisionTrace`. It supplies no setup, quantity or budget.

Native executors, recorder, floor calculation, order submission, reservation
transactions and recovery remain authoritative. No module starts a service, calls
an exchange, writes a database or dispatches an action. No existing caller is
rewired. Every returned assessment remains non-authorizing.

## Boundary and input semantics

`NativeScopeV2` binds program, owner, attributed executor, market, operating mode
and the existing `RecipeBinding` type. Support is explicitly OKX unleveraged spot;
paper connector identity must match paper mode. Observing a live-mode record does
not grant live execution. No account balance or inventory is adopted by parsing.

`NativeSnapshotV2` preserves native gross acquisition quote, gross sold base,
remaining base, accounting owner, terminal/finality flags and pending obligations.
Amounts accept exact Decimal/string/integer inputs, not floats or booleans. Decimal
inputs are bounded to 38 digits and exponent magnitude 36; counters/timestamps are
nonnegative signed-int64 values. Observation and availability are UTC milliseconds.
Collections are bounded to 64 commitments and 64 terminal references per snapshot,
1,024 scopes per management projection, and 128 KiB serialized snapshot bytes.

`capture_native_snapshot` reads `ExecutorInfo`-shaped evidence using fields already
consumed by `modular_spot_core/inventory.py`. It supports native entry states
`managing_fill`, `ended_no_fill` and `held_residual`; other states fail visibly.
The adapter hashes bounded receipt content, preserving fee-correction identity.

The caller MUST independently qualify scope/recipe binding, observation clocks,
reconciliation, fee completeness, and complete pending/terminal request evidence.
Those are explicit inputs, **not independently proved by this adapter**. In
particular, gross receipt totals cannot establish fee completeness. A terminal
reference is not authenticated merely because its hash has valid syntax.

Continuity checks refuse changed scope/recipe, regressing clocks/sequences,
changed content at the same sequence, changed request action, or disappearance of
a previously open request without a supplied terminal-receipt reference. They do
not create durable persistence, verify a referenced artifact's bytes, or release
capital. The future native host must obtain and verify real receipts and persist
its state using existing native mechanisms.

Freshness is measured from observation time, not publication time. Stale or
unreconciled flat evidence cannot establish CLOSED. Dust, held inventory, incomplete
fees and uncertain orders remain management obligations after entry deselection.
`RECONCILIATION_REQUIRED` is an unavailable-state projection, not a new executor
status. Publication/decoding never resets original clocks.

## Exit proposal bridge

`assess_exit(snapshot, view, proposals, floor, now_ms=..., max_age_ms=...)` accepts
canonical `NativeExitView`, `ExitProposal` and `NativeProfitFloor` values. It checks
scope, receipt, time, quantity and finality consistency, then delegates the actual
quantity quantization, maximum-not-sum arbitration and floor/bid/notional rules to
`arbitrate_exit`. Its result hash also binds the v2 recipe and full native snapshot.

Only a new reduction of a managing, entry-terminal, fee-complete reconciled scope
with no outstanding request is supported in this increment. Pending/uncertain
requests, retained-inventory handoff, mismatched reserved quantities and missing
quantity-specific floors hold that new proposal. No local cost-basis calculation,
market fallback or independent seller is added. A valid price-bounded proposal
can remain unfilled and is not a guarantee of final profit.

Never use this helper to suppress the entire native action loop. Existing native
protection, pending exits and reconciliation run independently of new proposal
eligibility. Retained inventory still requires its owning native management path.

## Acquisition lifecycle prerequisite

`evaluate_acquisition(snapshot, previous_cursor, opportunity, purpose=...,
now_ms=..., max_age_ms=..., prerequisites=...)` accepts canonical opportunities.
Initial entry needs READY; DCA needs managing inventory before liquidation and no
pending request. Unknown purposes, such as pyramiding, reject. Caller risk/capital
prerequisites remain vetoes, and at most 63 can be supplied beside the native gate.

PASS means lifecycle eligibility only. Sizing, fees, quote/base limits, stage
allocation and atomic native capital admission are deliberately not supplied.
The method returns a new canonical cursor for the caller to persist; it does not
mutate prior state. Same-event holds, revisions and replay retain existing cursor
semantics. The first event seeds rather than authorizes an entry.

The canonical cursor scope includes `acquisition_allowed`. Changing that scope on
an existing cursor is rejected; this increment does not reset/rebind it to implement
pause/resume. Use the supported native acquisition control and qualify that wiring
in the actual host. Shadow evaluation must persist a separate cursor namespace.
Exit assessment remains an independent call, not a branch under acquisition.

## Validation and limitations

Run in the verified owning environment:

```bash
PYTHONPATH="$PWD" python -m pytest -q test/spot_lifecycle_v2
```

Publication validation: **138 tests passed**, Python 3.13.5, Pydantic 2.13.4,
pytest 9.0.2; 91% combined statement/branch coverage over the new implementation.
Compilation and scoped whitespace checks were also run. Source-selected tests use
eight hash-verified canonical upstream blobs from native commit
`0f19b464ccd112b21ac69c6de0e5515250239aac`. They do not mock those algorithms.

The container could not resolve github.com for a clone. Its source snapshot omits
full native package initialization, compiled connectors, owner-wide conftest/setup
and the operator's environment. Executor inputs are synthetic shape fixtures, not
observed live `ExecutorInfo` producers. No full native suite, native connector run,
package/image build, actual recorder crash/restart, independent actor audit, market
inference, economic backtest, Research OS registration or deployment is claimed.
Formatting tools were not installed; no repository lint pass is claimed.

## Integration and rollback gate

Adding `spot_lifecycle_v2.py` changes recursive component source seals. The tests
explicitly show the old binding rejects the changed closure. No sealer, legacy
file, default, config, cache, root pin, image or checkpoint is changed here. Do not
install this candidate package into existing owners or automatically reseal them.
It requires a separately qualified isolated v2 package/host and native tests.

M1 records/projections and the bounded M2 bridge are implemented. M3 currently has
projection/continuity checks, not native trailing persistence. M4 currently has a
lifecycle prerequisite, not acquisition staging or sizing. M5 account authority,
M6 controller/recipe wiring, M7 actual native replay/recovery, M8 operator integration,
M9 research admission and M10 economics/operation remain outstanding.

Revert only these additions for an unactivated candidate; retain test evidence.
No service restart or state reset is required. Any future activated rollback must
continue native management of the actual orders and inventory.
