# t15 — Reviewer: re-review after t13 covering-sum

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
Head: uncommitted working tree (combined index + unstaged vs base)
Diff: `reviews/review-4a4372bc-worktree.diff` (14 files, +2485/−305)
Audit: `reports/audit.md` (re-audit after t13; gate PASS on split pytest)
Prior review: `reports/t12-review.md` (spec PASS, quality NOT APPROVED on any-bag matching)
Fixer: `reports/t13-fix.md` (claims covering-sum prefix walk; residual left open)

Implementer reports were treated as claims. Judgment is from the combined diff, the current `hl_rsi.py` helpers, Hummingbot report/store order, the named tests, and the auditor’s commands.

## Verdicts

- Spec: **PASS**
- Quality: **NOT APPROVED**
- Production-ready for this scope: **no**

## Spec

**PASS.** A–G are present in the combined tree. t13 closed the named t12 hole (any positive same-pair BUY bag releasing every transferring executor still on the list). Empty-bag E remains closed. Constraints are kept. Nothing required is missing. Extra volume is still the #50/#51 merge the completion bar asked for, not a third policy.

t12 Important (any positive same-pair BUY bag as completed transfer) is **closed** for claimants that remain on `executors_info` in leftover-allocation order. Empty bag still values Infinity and refuses a new BUY. Undersized bag and two on-list claimants with a bag that covers only the first leftover now refuse. Confirmed in code and in the four tests the auditor ran.

t13 residual (covering-sum only among claimants still on `executors_info`, and the prefix order of that list) is **not** a missing spec item. E’s helpers and empty-bag / undersized / two-claimant tests exist. It is a quality defect in the release predicate. Spec E is implemented; the predicate is still too weak to call production-ready. The covering-sum plan does not make the remaining fail-open acceptable.

### Item map

| Item | Verdict | Evidence |
| --- | --- | --- |
| A delayed OPEN cannot revive CANCELED/FILLED/FAILED | implemented | `InFlightOrder.update_with_order_update` skips those reopen states; TradeUpdate path unchanged; FILLED-after-CANCELED still assigns (`FILLED` is not in the refused `new_state` set). Tests: `test_update_with_order_update_does_not_reopen_*`, `test_update_with_trade_update_still_applies_after_canceled`, OKX cached REST reorder tests. Auditor: 36 passed. |
| B source 32 beats bound reprice | implemented | `process_order_canceled_event` classifies `"32"` before `_replacement_bound_to`, clears replacement, `super()`. Bound + missing source still confirms local reprice (`_replacement_bound_to` does not require `cancel_source`). Test: `test_stp_cancel_source_does_not_place_bound_reprice_child`. Auditor: 65 passed. |
| C deadline/revocation off wait state; cooldown at submit | implemented | Wait branch calls `_revocation_or_deadline_reason()` then cancels the same identity. Cooldown sits immediately before `place_order` / `_submit_open_order` for both replacement purposes. Tests: `test_pending_create_expires_intent_without_dropping_identity`, `test_reprice_child_honors_cooldown_at_submit`, `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`. |
| D owner + execution data at child boundary | implemented | `_live_entry_permission` refuses non-RUNNING `status` and present `data_health.ready` false/missing. No `can_create_executor` on the child path. `_live_book()` is `Optional[tuple[Decimal, Decimal]]`; locked/crossed waits. Fills stay `managing_fill`. Tests: `test_stopped_owner_cannot_place_replacement_after_recon`, `test_data_health_unreadiness_blocks_child_despite_cached_quotes`. |
| E owned vs outstanding vs labels; empty-bag hold fail-closed; covering-sum on-list | implemented | `managing_fill` does not zero remainder. Missing/conflict/over-authorized/non-finite owned → Infinity. `accounting_owner=held_position` or `CloseType.POSITION_HOLD` is not a release by itself. `_held_buy_inventory_observed()` is gone. Release requires `_executor_hold_released_to_bag` prefix covering-sum of current hold leftovers vs summed BUY bag. Empty `positions_held` → leftover stays on the walk, valuation Infinity, `can_create_executor` refuses `exposure_unavailable`. Undersized bag `0.1` vs leftover `0.4` refuses. Two on-list claimants leftover `0.4` each + bag `0.4` → first owned `0`, second unmatched, refuse. Tests: managing_fill 0.1/0.9/100, TERMINATED leftover + bag = 40, missing/conflict, both empty-bag tests, both undersized/prior-bag tests (auditor: 4 named passed; 18 capital tests). |
| F next quantized close qty | implemented | `_expected_close_fee_rate` uses `_close_order_amount()`. `<= 0` or `None` → `None`. `is_maker` only for `LIMIT_MAKER`. Tests: 0.01 residual, 0.1 remainder after prior close, blocked qty holds. Auditor: 110 passed. |
| G `cancel_source` JSON | implemented | `to_json` writes the field; `from_json` restores non-blank; native JSON round-trip; restored CANCELED is not `is_open`. |
| Overlap merge | implemented | No conflict markers. Union of overlap tests present (65). |
| Preserve | implemented | Chronological `_build_backtest_signal_frame` kept (one walk). Ordinary LIMIT is taker. `cancel_source` persisted. Entry repair/pause/fill recovery kept. FILLED-after-CANCELED OrderUpdate not over-blocked. Bound identity is not treated as STP cause. Pause no longer `POSITION_HOLD`s a filled owner. |
| Authority | implemented | No merge, deploy, restart, live orders, or hummingbot checkout edits. |

### Missing

None versus the spec’s A–G list and preserve list.

### Extra (in-scope merge, not out of spec)

Same as t9/t12: averaging-down gate (`averaging_score_low` vs owned underwater, runner exempt), pause no longer `POSITION_HOLD` in `ok_rsi.stop_actions_proposal`, backtest helper rewritten to one chronological inventory walk, spacing/drawdown using remaining basis. Required three-way merge, not a new trading policy invented here.

### Extra (not a spec fail)

None that violate “do not change approved trading policy.”

## Quality

**NOT APPROVED.** t12’s any-positive-bag window is closed and tested while every hold claimant remains on `executors_info` in leftover order. One Important capital-release hole remains: covering-sum is a prefix over current `executors_info` order, not proof that this leftover transferred. A later freeze, or a restart with a live bag, can consume a prior bag. t13 named the dropped-claimant half. The report-order half is in the same helpers. The plan chose covering-sum. That does not make it acceptable.

### Critical

None.

### Important

1. **Covering-sum still fail-opens when prior hold claimants are behind or gone from `executors_info`.**

   t12’s any-bag case is closed. `_held_buy_inventory_observed()` is gone. `_executor_hold_released_to_bag(executor)` now walks same-pair BUY hold claimants in `_filter_same_side(TradeType.BUY, active_only=False)` order and prefix-covers `_executor_reported_owned_base` leftovers against `_held_buy_inventory_amount()`. Empty bag, dust bag, and two on-list claimants with a bag that covers only the first leftover stay unmatched (`owned`/`outstanding` `None` → Infinity, leftover stays on `_buy_executors_for_capital`, `can_create_executor` refuses `exposure_unavailable`). `test_position_hold_without_held_bag_refuses_new_buy` and `test_position_hold_undersized_or_prior_bag_refuses_new_buy` cover those in both capital files. Auditor ran all four; they passed.

   Release is still not this leftover. The walk is `executors_info` order. Identity is `is`, not executor id. Leftover size is the reported remaining, which a released claimant still publishes after the bag write. Unmeasurable leftover fail-closes the *current* call (`return False`), it does not pin bag inventory to an executor.

   Runtime `executors_info` is not freeze order. `StrategyV2Base.update_executors_info` assigns `get_executors_report()`: active `executor.executor_info` objects first, then `stored_executors` (newest `close_timestamp` first, cap 20). `store_actions_proposal` stores done executors beyond `closed_executors_buffer` (30 in `scripts/v2_with_controllers.py` and `hummingbot-api/bots/scripts/v2_with_controllers.py`, 100 in `StrategyV2Base`). `_update_positions_from_done_executors` writes `positions_held` while the freeze is still in `active_executors`. After those rows are stored, they remain on the report *after* any new active holds.

   Default product path, not an exotic eviction footnote:

   - `bag_max_concurrent_holds` default 2. A later freeze is allowed while a live BUY bag already exists.
   - `max_executors_per_side` default 6, `bag_freeze_require_full_slots` default True. First freeze fills one bag row (`held_count` is bag rows, not executors). Capacity uses `_filter_same_side(..., active_only=True)`, so TERMINATED freeze slots free and new BUYs refill.
   - Once the first freeze is stored, covering-sum sees the new freeze first. Bag `>=` that leftover releases it. The stored prior freeze still claims hold and still reports leftover, but it is not in the prefix, so it no longer protects the new claimant.
   - Restart is the same inversion without waiting for the buffer: `_initialize_cached_performance` restores `positions_held` and `_record_stored_executor`s DB history (still cap 20). A post-restart freeze is active-first against that bag.
   - If the prior freeze falls out of the stored 20, it no longer consumes the bag at all. That is t13 concern 1. Order inversion happens first, while those claimants are still listed.

   Named tests never cover `[new_active_hold, stored_prior_hold]` or a dropped released claimant plus leftover-sized `positions_held`. Both two-claimant cases set `executors_info = [first, second]` with the unmatched leftover last. The TERMINATED leftover + same-size bag case is one claimant. t13 concern 1 is confirmed in the helpers plus orchestrator report order. Severity stays **Important**: it admits a new BUY with missing exposure on the second-freeze / resume path. Not Critical: empty bag still fails closed; kill/opposing gates still exist; this is admission, not an order send.

   Fix: keep the new claimant on the capital walk until held inventory is attributable to *this* leftover (executor id, or consume bag by id). Prefix covering-sum over report order is not proof of this transfer. Presence of a prior claimant after the new freeze in `executors_info` is not enough.

### Minor

t9 Minors 1–11 remain open at Minor. Not restated. None of them are now Important or Critical.

t13 did not fix them. Mixed `MM`/`AM` staging is still a process risk if a later commit stages only the index.

t13 concern 3 (Infinity vs a finite reserved leftover plus bag) is fail-closed under default `max_total_position_usd`. Not a defect.

## t12 Important (any-bag matching) — closed

Do not reopen the any-positive-bag finding for claimants that remain on `executors_info` in leftover order.

Evidence:

- `_held_buy_inventory_observed()` is absent from `hl_rsi.py`.
- `_executor_hold_released_to_bag` returns False on empty bag, on bag smaller than this leftover, and on a later on-list claimant after an earlier leftover has consumed the bag.
- `test_position_hold_undersized_or_prior_bag_refuses_new_buy`: leftover `0.4` + bag `0.1` refuses; two `POSITION_HOLD` leftovers `0.4` + bag `0.4` releases only the first.
- Empty-bag tests still pass in both capital files.

## t9 Important (empty bag) — still closed

Do not reopen the empty-bag finding. `early_stop(keep_position=True)` still writes `CloseType.POSITION_HOLD` and `accounting_owner=held_position` immediately. That flag is still not a capital release until covering-sum succeeds.

## Auditor evidence (not re-run)

Split pytest and flake8 exited 0. Counts: 65 down-only, 36 native/OKX, 18 capital (was 16 before t13), 110 position_executor, 36 factory. Extra focused run: both empty-bag and both undersized/prior-bag tests passed (4 passed / 14 deselected). No Docker, CI, live, or exchange run. Concatenated union was not re-run. That matches the authority ceiling.

t13 residual was **cannot-verify** in the audit (no test proves premature release after a released claimant is dropped, and no test puts a new active hold before a stored prior hold). This review verifies both from the helpers and from `get_executors_report` / `store_actions_proposal` / restart ingest. It is not a test-gate fail. It is a quality fail.

## Constraints

- Authority `repository-process-only`: held.
- Approved policy: not replaced with a third book. #50 repair + #51 permission/capital/fees.
- Late TradeUpdates and cumulative fills: still applied.
- Explicit 32 wins over bound reprice. 31 remains post-only. Bound id is not cause. Bound + missing `cancel_source` may still confirm local reprice.
- Expiry cancels the current identity; it does not invent a child.
- Child path does not rerun `can_create_executor`. Owned fills stay `managing_fill`.
- `_live_book() -> Optional[tuple[Decimal, Decimal]]` kept. Locked book waits.
- FILLED-after-CANCELED OrderUpdate still applies.

## Production-ready

**No.** Spec is implemented and tested. Quality is blocked on Important finding 1 (covering-sum vs this leftover once prior claimants are stored, ordered after a new freeze, or dropped). No Critical. Close that release window, keep the named E tests including empty-bag and undersized/two-claimant refuse, add a POSITION_HOLD + pre-existing bag where the prior claimant is missing or listed after the new freeze that still refuses a new BUY, then this candidate can be re-reviewed.
