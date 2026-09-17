# PR #52 — bd46d9bb persistence-boundary follow-up

## Verdict

**REQUEST CHANGES — not merge-ready.** Reviewed product SHA: `bd46d9bb167b17bf27f1a299e14dcd69d4cb17c3`, targeting `master` (`4a4372bcde4990355421a42df3b8ce6107299abb`). PR #52 is the consolidation candidate for #50/#51; do not merge those older alternatives to bypass the findings.

This follow-up contributes a runnable counterprobe and a **candidate patch, not an applied production change**. Hosted CI status is not the basis for the verdict. Nothing was deployed or restarted; no root component pin, live configuration, inventory policy, or database was changed.

## What changed since the older review

The actual latest commit adds the parent-lifecycle guard before native-child terminal reconciliation, rejects invalid/conflicting supplied gross totals and over-authorized OrderExecutor fills, and replaces the one-row held-transfer check with matching cumulative receipts. Those changes must not be described as absent merely because the PR body previously referred to `60e73c371`.

The combined entry diff contains a unified terminal stop source, cancellation-source propagation through OKX REST/WebSocket and InFlightOrder persistence, bounded maker repair, child identity binding, fresh permission checks, cooldown handling, and terminal-to-live state-regression protection. These are source-review observations, not a new native-runtime test result. The native PositionExecutor production diff in this PR changes close-fee estimation; it is not proof that every previously discussed SELL improvement has been implemented.

## Reproduced blockers

### P1 — valid native receipts fail the SQLite round trip

`PositionHoldLedger.executed_amount_base`, `executed_amount_quote`, and `cumulative_fee_paid_quote` use `SqliteDecimal(6)`. The native converter stores `int(value * 10**6)` and reads it back divided by `10**6`. Its JSON `order_payload` retains the original cumulative values and is used by `to_order_record()` when rebuilding the native held-order record.

`HLRSIController._executor_hold_released_to_bag()` compares the full-precision held receipt with the scaled numeric columns, exactly. Consequently a valid receipt can fail the transfer proof after persistence. Example using the quantity and limit from the supplied log: `0.028006 * 2379.63 = 66.64391778`, while the numeric quote column reads `66.643917`. This is an arithmetic example, not a claim that the canceled log order filled.

The new probes create a real SQLAlchemy model, commit it to SQLite, close the session, and load it in a new session. Three cases independently reproduce the problem for base, quote and fee precision. The failure can leave a valid held transfer unavailable and refuse later buys.

Do not fix this with an epsilon or by changing a live database's scale. The candidate compares exact native JSON values after verifying that the model's own persistence conversion produces the numeric columns. Legacy rows without sufficient exact precision remain unavailable.

### P1 — unknown held amounts are still treated as zero

`_executor_hold_required_fills()` skips an order when its base amount is absent, `None`, or empty, just as it skips a proven zero-fill order. A claimant with one matched fill plus a second unknown fill therefore passes whole-executor transfer with only the first row. Missing positive-fill quote and fee receipts are also defaulted to zero.

Four counterexamples reproduce this: unknown base (`None` and empty string), missing quote and missing fee. Unknown is not confirmed zero. Preserve skipping an explicit zero-fill child, but require finite known base, positive quote for a positive fill, and a known nonnegative fee.

### P1 — inconsistent JSON and numeric receipts are accepted

The current check ignores `order_payload`. A row whose numeric columns match the claimant is accepted even if its exact native payload reports another cumulative base amount or another client order ID. Since the native reconstruction path prefers that payload, these are conflicting records, not a completed transfer proof. Two counterexamples reproduce this.

The candidate validates payload/row identity and exact consistency with the native column conversion before using full-precision values. Sub-micro-unit stale payloads are still rejected; they are not accepted as numerically close.

## Candidate and actual validation

The candidate changes only the held-receipt comparison in `controllers/directional_trading/hl_rsi.py`: stricter required totals, one small validation helper, and replacement of the three raw-column reads with that helper. It does not change the database schema, entry strategy, retry budget, venue, execution owner, or SELL policy.

| Executed check | Result |
| --- | --- |
| Current bd46 controller-method excerpt + actual SQLite model/converter | 9 failed, 15 passed |
| Candidate applied to the same excerpt | 24 passed |
| Complete model and converter bytes against Git blob hashes | Matched |
| Probe/candidate syntax | Passed |
| Patch application, reverse check and resulting-method comparison on the source excerpt | Passed |

These are **24 source-extracted persistence-boundary checks**, not the full native Hummingbot suite. Controller startup, recorder availability, terminal ownership and decimal parsing are supplied by the harness. The two full model files are hash-verified; the controller is an excerpt, not a reconstructed full module. The nine failures are counterexamples, not nine observed live incidents.

Runtime: Python 3.13.5, SQLAlchemy 2.0.50, pytest 9.0.2. See `followup-bd46/evidence.json` for the source bindings. The full native suite, source-bound build, orchestrator application/recovery, exchange execution, and a separate review of this new candidate patch were **not run**.

## Shortest closure path

From the Hummingbot checkout, with the follow-up files present:

```bash
p=docs/audit/2026-09-17-ok-rsi-pr50-51-consolidation/followup-bd46
# This first run is intentionally red on the reviewed product commit.
RSIBOT_SOURCE_ROOT="$PWD" python -m pytest -q "$p/hold_receipt_boundary_checks.py"

# Do not blindly apply to a different controller revision.
test "$(git hash-object controllers/directional_trading/hl_rsi.py)" = 7745b9e51f4e6f99e2d54a344676e016af85981c
git apply --check "$p/hold_receipt_fix.patch"
git apply "$p/hold_receipt_fix.patch"
RSIBOT_SOURCE_ROOT="$PWD" python -m pytest -q "$p/hold_receipt_boundary_checks.py"
```

Then run the affected native suites on that exact candidate:

```bash
python -m pytest -q \
  test/test_hl_rsi_entry_control.py \
  test/test_ok_rsi_admission_safety.py \
  test/test_ok_rsi_down_only_entry_execution.py \
  test/test_ok_rsi_down_only_entry_factory.py \
  test/test_ok_rsi_entry_boundary_regressions.py \
  test/hummingbot/core/data_type/test_in_flight_order.py \
  test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py \
  test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py \
  test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py
```

Also verify that the actual recorder/orchestrator transfer and subsequent held-bag disposal agree with this receipt check; ledger equality alone does not independently establish successful state application. Preserve the existing native integration/recovery requirements. Record the final product SHA and obtain a separate review of the new fix before merging #52 into the fork's `master`. Updating the root rsibot pin and deploying the owner remain separate, admitted operations.
