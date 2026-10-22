# Prospective entry-risk enrollment

This is an operator-approved, stopped-owner transition in Hummingbot's existing
native Metadata store. It is separate from restoration of historical risk state.
The module has no deployment, exchange or command-publishing interface.

## Contract

`plan_entry_risk_transition` reads the current owner and returns a proposal.
`apply_entry_risk_transition` owns one `BEGIN IMMEDIATE` transaction, rederives the
proposal against the current native digest, and commits the risk row and its
before/after archive together. Replays, changed proposals, changed native state,
future timestamps and requests at least 300 seconds old refuse. Explicit SQL
commit/rollback also works with Python's `autocommit=True` connection mode.
An already-open transaction is rejected.

The caller must hold the maintenance lock, verify the owner is stopped, prove
the effective entry fence, retain a consistent backup, and obtain the specific
operator approval. Request booleans record those assertions; they do not prove
process state or grant authority. The migration cannot replace native/venue
reconciliation, source admission or active-executor continuity checks.

| Existing state | Permitted transition | Preserved evidence |
| --- | --- | --- |
| Existing daily entry rail | Change only the approved limit and its enrollment binding, if any | Baseline, last mark/time, breach/pause latch, peaks, stopped state and operator receipts |
| Historical risk exists, no daily rail | Explicit pending daily enrollment, with all entries fenced | Existing historical peaks, stopped/drawdown latches and operator receipts |
| Entire historical risk row absent | Explicit new prospective scope; separate approval is mandatory | Historical peaks stay unavailable; no prior peak or daily PnL is inferred |

Version-2 risk state carries a required versioned `entry_risk_enrollment` receipt.
On first startup, pending enrollment requires all operator pauses and no existing
daily baseline. The first fresh, complete native controller PnL report supplies
the prospective baseline. The runner persists this baseline before evaluating a
create action and leaves the operator entry fence in place. A premature resume
command is rejected before writing a command receipt. Missing PnL, a removed
fence or failed persistence holds execution and preserves the durable evidence.

For a new scope with no historical peaks, the enrollment receipt continues to
identify that absence after the initial mark. Historical drawdown limits cannot
be enabled from this prospective history. Existing historical peaks are never
replaced by a first daily mark.

The limit is in the native PnL quote currency. The existing `DailyEntryRisk`
semantics remain: a loss at the exact limit latches an entry pause; UTC rollover
uses the last observed mark so downtime losses are retained. Increasing the limit
does not erase a breach or automatically resume entries. Protective SELL actions
remain subject to their existing native checks; enrollment does not submit orders.

## Recovery and compatibility

A completed enrollment must agree with the daily limit, initial clock and the
same-day baseline. A later UTC day's baseline follows the existing daily model.
Malformed, absent, nonfinite, conflicting or unsupported records fail closed.

Risk-state version 1 remains supported. Version 2 requires the new runner; an old
image cannot be assumed compatible after enrollment. Keep the original backup,
transition archive, exact candidate source and deployment receipts. Reverting an
image or YAML alone is not a risk-state rollback. Never restore a historical
database after orders, fills or managed state may have changed.

Native financial rows, reservations and executor ownership are not repaired by
this module. Complete those qualified owner operations before deriving the final
risk proposal, then bind the actual transition into the owner recovery receipt.
Do not rewrite the original running/preflight snapshot to make it match a later
approved risk state.

The exceptional V1 BNB/SOL planned-resume path accepts pending version-2 risk
only through the archived `prospective_daily_enrollment_preserve_history`
transition. Its original preflight remains version 1. Prepare independently
derives the allowed transition, matches that original history, reconstructs the
pre-transition financial digest, and binds the archive, current risk state,
controller set, source files and staged runner limit into the manifest. Pending
validation and claim repeat those checks. The legacy version-1 path retains its
existing requirements; a version number alone never admits enrollment.

Financial hashing canonicalizes only parsed `strategy-risk:` JSON values so
equivalent historic serialization is accepted. Other Metadata values and native
financial rows retain their stored values. The only omitted row when
reconstructing the pre-transition digest is the exact bound transition archive.
An unrelated Metadata change, archive deletion, rehashed contradictory receipt,
changed peak, removed fence or changed runner limit invalidates the proof.

## Verification

`test/modular_spot/test_entry_risk_enrollment.py` exercises real runner
construction/restore, rejected activation, first-mark persistence, invalid
numbers/types, baseline tampering, UTC rollover, preserved breach history,
financial-state drift, replay, transaction modes and injected archive failure.
A separate integration case uses a real NativePaperTradeExchange fill, executor
and native PnL report and independently derives its executable-bid mark. It
checks that the protective executor remains managed. It is not a complete live
runner or exchange execution proof.

The planned-resume regressions also run the actual risk planner/apply operation
before prepare, pending validation and claim, using a completed BNB ownership
fixture together with two suspended SOL executors. These are isolated fixtures;
live admission still requires the actual graceful suspension receipts and fresh
venue proof.

Use sealed native copies for rehearsal and disposable credential-free containers
with network disabled. Running-database snapshots use the SQLite Backup API;
immutable SQLite mode applies only to the resulting sealed copies. See the
[Python 3.13 transaction contract](https://docs.python.org/3.13/library/sqlite3.html#transaction-control-via-the-autocommit-attribute)
and [SQLite transaction semantics](https://www.sqlite.org/lang_transaction.html).
