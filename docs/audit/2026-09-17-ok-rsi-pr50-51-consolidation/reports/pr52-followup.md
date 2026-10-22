# PR #52 follow-up review — 2026-09-17

## Verdict and scope

**REQUEST CHANGES. Not approved for merge, deployment, or retirement of the original PRs.**

Reviewed starting head `5a53fa52540a897ed6ea300eeba1be290f4df569`, based on `4a4372bcde4990355421a42df3b8ce6107299abb`. This follow-up preserves the consolidation and makes a bounded executor correction. It does not establish that A–G are all closed.

Authority is repository-process-only: review, regression development, and updating this existing review branch. No changes to live configuration/state, no credentials, no engine restart, no deployment, no merge, and no root component-pin change. Historical audit reports remain historical evidence, not fresh approval of these changes.

## Source changes in this follow-up

Changed `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`:

- Enforce expiry/revocation when a submitted client identity has no attached native receipt. Retain the identity and owner; do not interpret this as zero fill.
- When a previously submitted local handle is absent, latch the stop and keep reconciliation required instead of ending as no-fill.
- Treat common synchronous quote lookup failures as unavailable book evidence so they cannot bypass the pending-state deadline path.
- Check revocation as well as expiry when an initial candidate has no usable numeric book.
- Handle a book disappearing between successive reads without reporting the child as resting or performing arithmetic on `None` during decision logging.
- Do not let a bound local reprice override explicit system/risk/price-limit/unknown cancellation sources. The existing local-cancel path remains available for source 1 or absent metadata; neither is automatic post-only repair authority. Source 31 remains the explicit post-only repair reason. Source 1 cannot by itself distinguish an external API/manual cancel from our own cancel; this attribution limitation is not declared resolved.
- Recheck the candidate against the last observed ask. A non-terminal HOLD with an unavailable or crossing book is a wait, not permission to submit. Preserve the bounded replacement authorization while waiting; do not burn the parent intent or mint another one.

Added `test/test_ok_rsi_entry_boundary_regressions.py`: 12 parameterized native-fixture cases for these changes. This file was syntax checked here; its native execution remains outstanding.

No changes to RSI signals, DCA budgets, entry price ceilings, authorized quantities, leverage, loss-realization policy, or live service ownership.

## Fresh validation actually performed

The full entry executor and pure reducer were loaded from exact source. The executor's original Git blob was verified as `67d293c0a997e27b240e052a9a402ea7011f2b9d`; the corrected executor blob is `b0dbb09051b121be0b98f682ce130c591119171f`. The unchanged reducer blob is `996b53ecee606b6990c3b835d1e86f92ed48fe56`.

The local harness substitutes native/runtime/connector boundaries and executes selected accounting methods as excerpts. It is NOT a native Hummingbot suite, venue test, recorder test, or merged-service startup test.

| Local check | Observed result |
| --- | --- |
| Before this correction: preserved entry controls plus new boundary probes | 10 passed, 12 failed |
| Same 22 probes against the corrected executor | 22 passed |
| Additional permission/capital/first-repair probes | 2 passed, 8 failed |
| Final combined isolated run | 24 passed, 8 failed; 0 errors, 0 skipped |
| Executor/reducer/new native test Python syntax | Passed |
| Native Hummingbot suite, source-bound image build, service startup | Not run here |

The failures are counterexamples grouped below, not eight observed production incidents. The native fixtures for the bounded changes must be run in the real build environment; isolated green results are not a substitute.

The original report records split local pytest counts 65 + 36 + 22 + 110 + 36 = 269, with additional subtests. Those are the author's historical results, not tests rerun in this follow-up. The original audit also records reused compiled extensions, an event-loop warning, and collection/loop-state problems with a combined invocation. These qualifications must remain visible. A clean source-bound native build has not been demonstrated by those counts.

## Remaining merge blockers

### P1 — D: required permission evidence still defaults to permission

`_live_entry_permission()` permits a controller without `status` and without `data_health`. The original report explicitly retains this to accommodate the default test helper. That is a test-fixture accommodation in production permission logic, not fail-closed admission.

Fresh isolated sequences each reach `place_order()` after terminal zero-fill reconciliation when either controller status or data-health evidence is removed. A separate case keeps the last candle-health flag true but makes connector and provider readiness false; numeric cached quotes still authorize the child.

The non-RUNNING status check and explicit false-health check are real improvements and their preserved probes pass. They do not establish fresh execution-book evidence.

Required correction: require an explicit current owner state and typed, current permission evidence; verify the relevant execution book rather than equating healthy candle data with live book readiness. Update test fixtures to supply real required fields rather than allowing missing fields in production. Reconciliation and owned-position exits must remain independent of entry permission. This follow-up has NOT implemented this broader contract change.

### P1 — E: transfer is inferred from current bag quantity, not native provenance

`_executor_hold_released_to_bag()` compares aggregate current held quantity with cumulative original executor leftovers sorted by time. That cannot establish which executor's receipts were actually transferred.

Two normal-lifecycle counterexamples change a bag from 0.4 to 0.3 after a partial sale, or from 0.4 to zero after full disposal, while retaining its done executor in history. The transfer predicate changes from true to false. `_executor_owned_base()` then returns unavailable for the held claimant, and the capital path returns Infinity. A legitimate held-position sale can therefore block new entries until the historical projection changes. This was reproduced with the actual selected controller methods, not an orchestrator/recorder run.

Conversely, an old unrelated bag of equal size can satisfy the new completed claimant's predicate. This is an ambiguous-input counterexample: the quantity-only projection cannot distinguish valid transfer from missing transfer. Equal balances are not identity evidence.

The existing native owner already retains order IDs in PositionHold and stores held-ledger entries with executor ID. Project that existing transfer evidence to the controller; do not add a competing ledger or infer it from age, quantity equality, or truncation of the last-20 executor history. A historical transfer must remain a transfer after downstream partial/full disposal. This follow-up has NOT changed the transfer/accounting contract.

### P1 — E: contradictory gross acquisition evidence is still clamped

`_executor_outstanding_base()` still uses a maximum of parsed acquired/net-filled quantities and then `max(0, authorized - filled)`. It does not reject gross acquisition above authorization or invalid/conflicting cumulative evidence before returning zero outstanding.

The negative-input probe uses authorized=1, owned=0.4, and cumulative gross acquisition=2. Owned-base validation accepts 0.4 and outstanding becomes zero. Required correction: reconcile all present measures, explicitly handle gross/net/base-fee distinctions, and report unavailable/reconciliation-required on invalid totals rather than manufacturing capacity. Do not merely compare gross and net quantities for equality, since legitimate base fees differ.

### P2 — C: cooldown begins only after the first replacement

The shared cooldown now works when `_entry_last_reprice_at` exists. The first confirmed post-only repair still bypasses it because the initial submission leaves that field unset. Initial submission at t=0 followed by cancellation/reconciliation at t=1 produces a second submission despite a 30-second cooldown in the probe.

Required correction: bind first-repair timing to the original actual submission timestamp, preserving the original intent deadline. Update fixture clocks accordingly. A separately authorized different first-repair cadence would require an explicit policy decision; it is not the currently claimed shared cooldown. This follow-up has NOT changed that cadence.

## Improvements confirmed in source or preserved probes

- Delayed OPEN/PENDING/PARTIAL updates no longer directly overwrite a terminal native state under the new guard. This does not prove every possible terminal-to-terminal ordering or tracker callback sequence.
- `cancel_source` is now included in native JSON serialization/restoration.
- Explicit STP source 32 takes precedence in the cancellation callback.
- Basic entry-pause retention, cancellation retry, pending-creation expiry, stopped-owner rejection when status is present, explicit false-health rejection, and later-child cooldown pass the preserved isolated controls.
- Residual close-fee estimation now calls `_close_order_amount()` and uses that quantity. Ordinary LIMIT is classified as potentially taker. Complete base-fee conservation and economic floor behavior remain native/economic validation requirements, not inferred from the old flat-fee probe.

## Hosted CI and operational evidence

At the reviewed starting head, image-verification run `35202092870` has failed job `105139015142` without step records. CI run `35202092743` has failed preliminary job `105139015919` without step records and skipped native build/test job `105139028915`. These are unavailable validation evidence, not failing product assertions. The previously reported billing restriction was not independently proved through a retrieved billing annotation here.

No native Hummingbot installation, Docker executable/socket, authenticated host terminal, or deployed owner access exists in this audit environment. No production logs or runtime services were inspected or restarted. No claims of clean live logs, end-to-end stability, independent delegated approval, or profitability are made.

## Required next validation and rollback

In the isolated native workspace, after correcting the remaining contracts, run the union of both original suites and the new boundary regressions at the exact final candidate commit. At minimum:

```bash
PYTHONPATH=. python -m pytest --import-mode=importlib -q \
  test/test_ok_rsi_entry_boundary_regressions.py \
  test/test_ok_rsi_down_only_entry_execution.py \
  test/test_ok_rsi_down_only_entry_factory.py \
  test/test_ok_rsi_down_only_entry_policy.py \
  test/test_ok_rsi_down_only_entry_config.py \
  test/test_hl_rsi_entry_control.py \
  test/test_ok_rsi_admission_safety.py \
  test/hummingbot/core/data_type/test_in_flight_order.py \
  test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py \
  test/hummingbot/strategy_v2/executors/position_executor
```

Retain failure output; do not skip assertions or relabel fixture failures as product passes. Add native recorder tests that transfer an identified held position and then partially/fully dispose of it, including truncated executor history. Add stale-book and missing-permission tests with valid native inputs. Then run the owning full suite, source-bound image verification, and a non-live startup/recovery smoke. A copied extension or cache hit is not by itself a fresh test/build receipt.

Rollback of this source-only follow-up is a reviewed revert of its commit on this review branch. There is no deployment or runtime rollback action because no runtime was changed. Do not close PRs #50/#51 as validated replacements until the consolidation gate genuinely passes.

Official references checked: https://www.okx.com/docs-v5/en/ and https://app.okx.com/docs-v5/trick_en/ . Post-only crossing is not STP; request acceptance is not cancellation completion; cumulative terminal fills and ownership remain authoritative.
