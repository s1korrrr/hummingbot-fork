# t12 — Reviewer: re-review after t10 hold-flag fix

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
Head: uncommitted working tree (combined index + unstaged vs base)
Diff: `reviews/review-4a4372bc-worktree.diff` (14 files, +2377/−305)
Audit: `reports/audit.md` (re-audit after t10; gate PASS on split pytest)
Prior review: `reports/t9-review.md` (spec PASS, quality NOT APPROVED on empty-bag hold)
Fixer: `reports/t10-fix.md` (claims empty bag now Infinity / refuse BUY; residual matching rule left open)

Implementer reports were treated as claims. Judgment is from the combined diff, the current `hl_rsi.py` helpers, the named tests, and the auditor’s commands.

## Verdicts

- Spec: **PASS**
- Quality: **NOT APPROVED**
- Production-ready for this scope: **no**

## Spec

**PASS.** A–G are present in the combined tree. t10 closed the named empty-bag E hole that blocked t9. Constraints are kept. Nothing required is missing. Extra volume is still the #50/#51 merge the completion bar asked for, not a third policy.

t9 Important (`POSITION_HOLD` / `accounting_owner=held_position` dropping capital while `positions_held` is empty) is **closed**. Empty bag now values Infinity and refuses a new BUY. Confirmed in code and in the two tests the auditor ran.

t10 residual (any positive same-pair BUY bag counts as a completed transfer) is **not** a missing spec item. E’s helpers and empty-bag tests exist. It is a quality defect in the release predicate. Spec E is implemented; the predicate is too weak to call production-ready.

### Item map

| Item | Verdict | Evidence |
| --- | --- | --- |
| A delayed OPEN cannot revive CANCELED/FILLED/FAILED | implemented | `InFlightOrder.update_with_order_update` skips those reopen states; TradeUpdate path unchanged; FILLED-after-CANCELED still assigns. Tests: `test_update_with_order_update_does_not_reopen_*`, `test_update_with_trade_update_still_applies_after_canceled`, OKX cached REST reorder tests. Auditor: 36 passed. |
| B source 32 beats bound reprice | implemented | `process_order_canceled_event` classifies `"32"` before `_replacement_bound_to`, clears replacement, `super()`. Bound + missing source still confirms local reprice. Test: `test_stp_cancel_source_does_not_place_bound_reprice_child`. Auditor: 65 passed. |
| C deadline/revocation off wait state; cooldown at submit | implemented | Wait branch calls `_revocation_or_deadline_reason()` then cancels the same identity. Cooldown sits immediately before `place_order` for both replacement purposes. Tests: `test_pending_create_expires_intent_without_dropping_identity`, `test_reprice_child_honors_cooldown_at_submit`, `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`. |
| D owner + execution data at child boundary | implemented | `_live_entry_permission` refuses non-RUNNING `status` and present `data_health.ready` false/missing. No `can_create_executor` on the child path. `_live_book()` is `Optional[tuple[Decimal, Decimal]]`; locked/crossed waits. Fills stay `managing_fill`. Tests: `test_stopped_owner_cannot_place_replacement_after_recon`, `test_data_health_unreadiness_blocks_child_despite_cached_quotes`. |
| E owned vs outstanding vs labels; empty-bag hold fail-closed | implemented | `managing_fill` does not zero remainder. Missing/conflict/over-authorized/non-finite owned → Infinity. `accounting_owner=held_position` or `CloseType.POSITION_HOLD` is not a release by itself. Release requires `_executor_hold_released_to_bag`. Empty `positions_held` → owned/outstanding `None`, leftover stays on the walk, valuation Infinity, `can_create_executor` refuses `exposure_unavailable`. Tests: managing_fill 0.1/0.9/100, TERMINATED leftover + bag = 40, missing/conflict, `test_position_hold_without_held_bag_refuses_new_buy` in both capital files (auditor: 2 passed extra; 16 capital tests). |
| F next quantized close qty | implemented | `_expected_close_fee_rate` uses `_close_order_amount()`. `<= 0` or `None` → `None`. `is_maker` only for `LIMIT_MAKER`. Tests: 0.01 residual, 0.1 remainder after prior close, blocked qty holds. Auditor: 110 passed. |
| G `cancel_source` JSON | implemented | `to_json` writes the field; `from_json` restores non-blank; native JSON round-trip; restored CANCELED is not `is_open`. |
| Overlap merge | implemented | No conflict markers. Union of overlap tests present (65). |
| Preserve | implemented | Chronological `_build_backtest_signal_frame` kept (one walk). Ordinary LIMIT is taker. `cancel_source` persisted. Entry repair/pause/fill recovery kept. FILLED-after-CANCELED OrderUpdate not over-blocked. Bound identity is not treated as STP cause. Pause no longer `POSITION_HOLD`s a filled owner. |
| Authority | implemented | No merge, deploy, restart, live orders, or hummingbot checkout edits. |

### Missing

None versus the spec’s A–G list and preserve list.

### Extra (in-scope merge, not out of spec)

Same as t9: averaging-down gate (`averaging_score_low` vs owned underwater, runner exempt), pause no longer `POSITION_HOLD` in `ok_rsi.stop_actions_proposal`, backtest helper rewritten to one chronological inventory walk, spacing/drawdown using remaining basis. Required three-way merge, not a new trading policy invented here.

### Extra (not a spec fail)

None that violate “do not change approved trading policy.”

## Quality

**NOT APPROVED.** t9’s empty-bag release window is closed and tested. One Important capital-release hole remains: the matching bag is any positive same-pair BUY hold, so a prior or partial bag looks like this executor’s transfer completed. t10 named that residual. The plan chose that matching rule. That does not make it acceptable.

### Critical

None.

### Important

1. **Any positive same-pair BUY bag still releases every transferring executor.**

   t9’s empty-bag case is closed. `_executor_claims_held_position` is true on `accounting_owner=held_position` or `close_type=POSITION_HOLD`. Until a bag exists, owned and outstanding are `None`, the leftover stays on `_buy_executors_for_capital`, and `can_create_executor` refuses with `exposure_unavailable`. `test_position_hold_without_held_bag_refuses_new_buy` covers that in both capital files. Auditor ran both; they passed.

   Release is `_executor_hold_released_to_bag` = claim **and** `_held_buy_inventory_observed()`. The second helper returns True on the first same connector/pair/BUY `positions_held` row with a finite amount `> 0`. It does not check leftover size, executor identity, or that this claimant’s inventory is in the bag. Once that helper is True, `_executor_owned_base` and `_executor_outstanding_base` return `0`, `_executor_counts_toward_buy_capital` returns False, and `_get_total_position_value_usd` is finite (old bag only). `can_create_executor` therefore skips `exposure_unavailable`.

   That is fail-open for new acquisition while this leftover is unproven. Spec E: release only reconciled commitment; fail closed when transfer is unproven.

   Default product path, not an exotic concurrent-hold footnote:

   - `max_executors_per_side` default 6, `bag_freeze_require_full_slots` default True. `_check_bag_freeze_trigger` freezes every active BUY at once.
   - After the first leftover is written to `positions_held`, every remaining `POSITION_HOLD` executor is treated as released. Capacity uses `_filter_same_side(..., active_only=True)`, so a TERMINATED (and, after the first of six terminates, one free slot) does not keep the missing leftovers reserved. The capital sum is the partial bag.
   - `bag_max_concurrent_holds` default 2. A later freeze is allowed while a live bag already exists. Newly frozen executors match that prior bag immediately, including a dust row `> 0`.

   Named tests never cover `POSITION_HOLD` plus a pre-existing or undersized bag. `test_terminated_leftover_owned_*` always pairs the flag with a bag of the same size as that one leftover. t10 concern 1 is confirmed in the helpers. Severity stays **Important**: it admits a new BUY with missing exposure on the freeze path. Not Critical: empty bag still fails closed; kill/opposing gates still exist; this is admission, not an order send.

   Fix: keep the claimant on the capital walk (owned/outstanding `None` → Infinity, or leftover reserved) until held inventory covers this leftover. Existence of some other BUY bag is not proof of this transfer.

### Minor

t9 Minors 1–11 remain open at Minor. Not restated. None of them are now Important or Critical.

t10 did not fix them. Mixed `MM` staging is still a process risk if a later commit stages only the index.

## t9 Important (empty bag) — closed

`early_stop(keep_position=True)` still writes `CloseType.POSITION_HOLD` and `accounting_owner=held_position` immediately. That flag is no longer a capital release.

Evidence:

- `hl_rsi.py` `_executor_owned_base` / `_executor_outstanding_base` return `None` when the executor claims hold and `_held_buy_inventory_observed()` is false.
- `_executor_counts_toward_buy_capital` stays true in that window.
- `_get_total_position_value_usd` → Infinity; `can_create_executor` records `exposure_unavailable`.
- `test_hl_rsi_entry_control.py`: SHUTTING_DOWN-shaped mock, empty `positions_held`.
- `test_ok_rsi_admission_safety.py`: live `PositionExecutor` SHUTTING_DOWN + `POSITION_HOLD`, empty bag.

Do not reopen the empty-bag finding.

## Auditor evidence (not re-run)

Split pytest and flake8 exited 0. Counts: 65 down-only, 36 native/OKX, 16 capital (was 14 before t10), 110 position_executor, 36 factory. Extra focused run: both `test_position_hold_without_held_bag_refuses_new_buy` passed. No Docker, CI, live, or exchange run. Concatenated union was not re-run. That matches the authority ceiling.

t10 residual was **cannot-verify** in the audit (no test proves premature release onto a non-empty ledger). This review verifies it from the helpers and the freeze/hold defaults. It is not a test-gate fail. It is a quality fail.

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

**No.** Spec is implemented and tested. Quality is blocked on Important finding 1 (any-bag matching vs this leftover). No Critical. Close that release window, keep the named E tests including empty-bag refuse, add a POSITION_HOLD + pre-existing/undersized `positions_held` case that still refuses a new BUY or keeps the unmatched leftover reserved, then this candidate can be re-reviewed.
