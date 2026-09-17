# t18 — Reviewer: re-review after t16 live-hold vs bag

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
Head: uncommitted working tree (combined index + unstaged vs base)
Diff: `reviews/review-4a4372bc-worktree.diff` (14 files, +2604/−305)
Audit: `reports/audit.md` (re-audit after t16; gate PASS on split pytest)
Prior review: `reports/t15-review.md` (spec PASS, quality NOT APPROVED on covering-sum over report order)
Fixer: `reports/t16-fix.md` (claims live / `SHUTTING_DOWN` never consume the bag; only done holds, oldest close first)

Implementer reports were treated as claims. Judgment is from the combined diff, the current `hl_rsi.py` helpers, Hummingbot `ExecutorInfo` / orchestrator bag write, the named tests, and the auditor’s commands.

## Verdicts

- Spec: **PASS**
- Quality: **APPROVED**
- Production-ready for this scope: **yes**

## Spec

**PASS.** A–G are present in the combined tree. t16 closed the named t15 hole (live / `SHUTTING_DOWN` hold consuming a prior bag because covering-sum walked `executors_info` / `_filter_same_side` report order). Empty-bag, undersized-bag, and on-list two-claimant E remain closed. Constraints are kept. Nothing required is missing. Extra volume is still the #50/#51 merge the completion bar asked for, not a third policy.

t15 Important (covering-sum prefix over report order) is **closed**. Live and `SHUTTING_DOWN` hold claimants never consume `positions_held`. Only `_executor_is_done` / `ExecutorInfo.is_done` (`status == TERMINATED`) hold claimants may be treated as released, and those are ordered by `(close_timestamp or timestamp, id)`, oldest first, not raw report order. Duplicate or missing age keys fail closed. Confirmed in the helpers and in the four t16 tests the auditor ran (eight named rows across both capital files).

t16 residual (later already-TERMINATED freeze with the prior done claimant dropped still matching a leftover-sized bag) is **not** a missing spec item. E’s helpers and empty-bag / undersized / two-claimant / live-vs-stored-bag tests exist. It is the KEEP “TERMINATED leftover + bag = 40” snapshot. Bag rows have no executor id. Spec E does not require a third ledger. See Quality: Minor 1.

### Item map

| Item | Verdict | Evidence |
| --- | --- | --- |
| A delayed OPEN cannot revive CANCELED/FILLED/FAILED | implemented | `InFlightOrder.update_with_order_update` skips those reopen states; TradeUpdate path unchanged; FILLED-after-CANCELED still assigns (`FILLED` is not in the refused `new_state` set). Tests: `test_update_with_order_update_does_not_reopen_*`, `test_update_with_trade_update_still_applies_after_canceled`, OKX cached REST reorder tests. Auditor: 36 passed. |
| B source 32 beats bound reprice | implemented | `process_order_canceled_event` classifies `"32"` (`OKX_STP_CANCEL_SOURCE`) before `_replacement_bound_to`, clears replacement, `super()`. Bound + missing source still confirms local reprice (`_replacement_bound_to` does not require `cancel_source`). 31 remains post-only. Test: `test_stp_cancel_source_does_not_place_bound_reprice_child`. Auditor: 65 passed. |
| C deadline/revocation off wait state; cooldown at submit | implemented | Wait branch calls `_revocation_or_deadline_reason()` then cancels the same identity. Cooldown sits immediately before `_submit_open_order` for both replacement purposes. Tests: `test_pending_create_expires_intent_without_dropping_identity`, `test_reprice_child_honors_cooldown_at_submit`, `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`. |
| D owner + execution data at child boundary | implemented | `_live_entry_permission` refuses non-RUNNING `status` and present `data_health.ready` false/missing. No `can_create_executor` on the child path. `_live_book()` is `Optional[tuple[Decimal, Decimal]]`; locked/crossed waits. Fills stay `managing_fill`. Tests: `test_stopped_owner_cannot_place_replacement_after_recon`, `test_data_health_unreadiness_blocks_child_despite_cached_quotes`. |
| E owned vs outstanding vs labels; empty-bag hold fail-closed; live holds do not consume bag | implemented | `managing_fill` does not zero remainder. Missing/conflict/over-authorized/non-finite owned → Infinity. `accounting_owner=held_position` or `CloseType.POSITION_HOLD` is not a release by itself. `_held_buy_inventory_observed()` is gone. Release requires `_executor_hold_released_to_bag`: claimant must be done, then oldest-first covering-sum of done leftovers vs `_held_buy_inventory_amount()`. Empty `positions_held` → leftover stays on `_buy_executors_for_capital`, owned/outstanding `None`, valuation Infinity, `can_create_executor` refuses `exposure_unavailable`. Undersized bag `0.1` vs leftover `0.4` refuses. Two on-list done leftovers `0.4` + bag `0.4`, listed newest-first → older owned `0`, newer unmatched, refuse. New live / `SHUTTING_DOWN` hold listed before a stored prior done hold, bag covers one leftover → new unmatched, prior may consume the bag, refuse. New live / `SHUTTING_DOWN` hold, prior absent, leftover-sized bag → new unmatched, refuse. Tests: managing_fill 0.1/0.9/100, TERMINATED leftover + bag = 40, missing/conflict, both empty-bag tests, both undersized/two-claimant tests, both t16 live/stored-bag tests (auditor: 8 named passed; 22 capital tests). |
| F next quantized close qty | implemented | `_expected_close_fee_rate` uses `_close_order_amount()`. `<= 0` or `None` → `None`. `is_maker` only for `LIMIT_MAKER`. Tests: 0.01 residual, 0.1 remainder after prior close, blocked qty holds. Auditor: 110 passed. |
| G `cancel_source` JSON | implemented | `to_json` writes the field; `from_json` restores non-blank; native JSON round-trip; restored CANCELED is not `is_open`. |
| Overlap merge | implemented | No conflict markers. Union of overlap tests present (65). |
| Preserve | implemented | Chronological `_build_backtest_signal_frame` kept (one walk). Ordinary LIMIT is taker. `cancel_source` persisted. Entry repair/pause/fill recovery kept. FILLED-after-CANCELED OrderUpdate not over-blocked. Bound identity is not treated as STP cause. Pause no longer `POSITION_HOLD`s a filled owner. |
| Authority | implemented | No merge, deploy, restart, live orders, or hummingbot checkout edits. |

### Missing

None versus the spec’s A–G list and preserve list.

### Extra (in-scope merge, not out of spec)

Same as t9/t12/t15: averaging-down gate (`averaging_score_low` vs owned underwater, runner exempt), pause no longer `POSITION_HOLD` in `ok_rsi.stop_actions_proposal`, backtest helper rewritten to one chronological inventory walk, spacing/drawdown using remaining basis. Required three-way merge, not a new trading policy invented here.

### Extra (not a spec fail)

None that violate “do not change approved trading policy.”

## Quality

**APPROVED.** t15’s report-order window is closed and tested. Empty-bag, undersized-bag, and two on-list done claimants stay refused. The remaining size-match among dropped done holds is the KEEP snapshot, not the live freeze path. No Critical. No open Important.

### Critical

None.

### Important

None.

t15 Important 1 is **closed**. Do not reopen covering-sum over report order.

Evidence:

- `_executor_hold_released_to_bag` returns False unless `_executor_is_done(executor)` (`hl_rsi.py`). Live and `SHUTTING_DOWN` holds never enter the covering walk.
- Product `executors_info` is `List[ExecutorInfo]`. `ExecutorInfo.is_done` is `status == TERMINATED`. `ExecutorBase.is_active` is False in `SHUTTING_DOWN`, but `_executor_is_done` prefers `is_done` when present, so a `SHUTTING_DOWN` `ExecutorInfo` does not consume the bag.
- Done hold claimants are collected by `_done_buy_hold_claimants_oldest_first`, sorted by `(close_timestamp or timestamp, id)`. Raw `_filter_same_side` / active-first report order is not the allocator. Missing or duplicate age keys return `None` and fail closed.
- `test_new_active_hold_before_stored_prior_hold_refuses_new_buy` (hl_rsi live stub listed before a stored prior) and `test_new_shutting_down_hold_before_stored_prior_hold_refuses_new_buy` (ok_rsi real `RunnableStatus.SHUTTING_DOWN` `ExecutorInfo`) both refuse BUY with `exposure_unavailable`. The prior done hold may consume the leftover-sized bag; the new claimant stays unmatched.
- `test_new_hold_without_prior_claimant_refuses_leftover_sized_bag` in both files: new live / `SHUTTING_DOWN` hold, prior absent, bag equals leftover, still unmatched, refuse.
- Two-claimant tests list newest-first (`[second, first]`) and still release the older close first.

Default product freeze path is live then `SHUTTING_DOWN` (`early_stop(keep_position=True)`), then `TERMINATED`. `get_all_reports` runs `_update_positions_from_done_executors` before assigning `positions_held` and `executors_info`. `can_create_executor` therefore sees a transferring freeze as not-done (Infinity) and a newly done freeze only after the bag write for that `POSITION_HOLD` (when `held_position_orders` exist). `PositionExecutor.control_shutdown_process` retags empty `held_position_orders` to `EARLY_STOP`, so a TERMINATED `POSITION_HOLD` has orders for that write. Same-pair bags merge via `_apply_executor_info_to_positions`. This is fail-closed for new acquisition while transfer is unproven. A third bag ledger is not required for this path.

### Minor

1. **Later already-TERMINATED freeze with the prior done claimant dropped still size-matches a leftover-sized bag.**

   `_executor_hold_released_to_bag` covering-sum is leftover size vs `_held_buy_inventory_amount()`, identity `is` on the current done list. `positions_held` rows have no executor id. If the prior released claimant has fallen out of `stored_executors` (newest `close_timestamp`, cap 20) and a later freeze is already `TERMINATED`, bag `>=` that leftover releases it.

   That snapshot is also the KEEP “TERMINATED leftover + bag = 40” case (`test_terminated_leftover_owned_stays_reserved_until_hold_transfer` / ok_rsi equivalent). On the product path the later freeze’s bag write has already merged into the same-pair `PositionHold` before admission, so dropped-prior inventory remains in the bag sum. The fail-open construction is a TERMINATED hold leftover equal to a bag that does not contain this leftover. Default freeze does not produce that: empty `held_position_orders` becomes `EARLY_STOP`, not a hold claimant.

   Untested. Severity **Minor**, not Important: the live / `SHUTTING_DOWN` admission window that t15 named is closed and tested; this remainder is size-matching among done holds after eviction, which is the intended no-double-count once the bag is the transferred inventory. Do not invent bag executor ids for this candidate. Do not reopen t15.

t9 Minors 1–11 remain open at Minor. Not restated. None of them are now Important or Critical.

t16 concern 2 (covering identity is `is`; age uses executor `id` only as a tiebreak) stays Minor. Product walks `executors_info` objects from one report list.

t16 concern 3 / mixed `MM`/`AM` staging is still a process risk if a later commit stages only the index. Minor.

`_executor_is_done` fallback `not is_active` would treat a live `SHUTTING_DOWN` executor object as done. Product capital uses `ExecutorInfo`. Residual only if helpers later receive live executor objects. Minor.

## t15 Important (covering-sum over report order) — closed

Do not reopen the live / `SHUTTING_DOWN` hold consuming a prior bag via `executors_info` order.

Evidence:

- Prefix over raw `_filter_same_side` report order is gone.
- `_executor_hold_released_to_bag` returns False when the claimant is not done, including with a leftover-sized bag and with a stored prior listed after the new freeze.
- Auditor: both empty-bag tests, both undersized/two-claimant tests, both new-hold-before-prior tests, both new-hold-without-prior tests. 8 passed / 14 deselected.

## t12 Important (any-bag matching) — still closed

Do not reopen the any-positive-bag finding for claimants that remain on `executors_info`. Live holds never match. Done holds still require covering-sum of older done leftovers first.

## t9 Important (empty bag) — still closed

Do not reopen the empty-bag finding. `early_stop(keep_position=True)` still writes `CloseType.POSITION_HOLD` and `accounting_owner=held_position` immediately. That flag is still not a capital release until covering-sum succeeds on a done claimant. Empty bag still Infinity.

## Auditor evidence (not re-run)

Split pytest and flake8 exited 0. Counts: 65 down-only, 36 native/OKX, 22 capital (was 18 before t16), 110 position_executor, 36 factory. Extra focused run: 8 named E / t16 tests passed. No Docker, CI, live, or exchange run. Concatenated union was not re-run. That matches the authority ceiling.

t16 residual was **cannot-verify** in the audit (no test proves premature release of a live hold; dropped-done later freeze untested). This review verifies the live / `SHUTTING_DOWN` close from the helpers plus `ExecutorInfo.is_done` / `get_all_reports` bag write. The dropped-done size-match is Minor, not a test-gate fail.

## Constraints

- Authority `repository-process-only`: held.
- Approved policy: not replaced with a third book. #50 repair + #51 permission/capital/fees.
- Late TradeUpdates and cumulative fills: still applied.
- Explicit 32 wins over bound reprice. 31 remains post-only. Bound id is not cause. Bound + missing `cancel_source` may still confirm local reprice.
- Expiry cancels the current identity; it does not invent a child.
- Child path does not rerun `can_create_executor`. Owned fills stay `managing_fill`.
- `_live_book() -> Optional[tuple[Decimal, Decimal]]` kept. Locked book waits.
- FILLED-after-CANCELED OrderUpdate still applies.
- Release only reconciled commitment. Fail closed for new acquisition when transfer is unproven.

## Production-ready

**Yes.** Spec is implemented and tested. Quality has no open Critical or Important. t15’s live covering-sum hole is closed. Empty-bag and undersized/two-claimant refuse remain. The dropped-done leftover-sized bag snapshot is the KEEP no-double-count case, Minor. Native/CI/live still not run; that is outside this authority ceiling, not a remaining product defect in scope.
