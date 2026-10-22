# t3 — Overlap and merge conflict map

Read-only three-way of the two overlap files onto shared base `4a4372bcde4990355421a42df3b8ce6107299abb`.

- PR #50 (`ours` in merge-tree): `9334c20c3a09ffed92631b4fa1172285b776b116` at `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-down-only-postonly-repair`
- PR #51 (`theirs`): `d38705b0813a6e39747908cbce74f1f08015e0dc` at `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-entry-control-consistency`
- Method: `git merge-tree --write-tree --merge-base=4a4372bc 9334c20c3 d38705b08` (exit 1). Conflicted tree `d22b57867c8d95e5fc0a97496acfe83d6a2cb06e`. Disposable `/tmp` merge-file confirmed the same 6 hunks; test line numbers differ slightly under `git merge-file` because it does not reuse the recursive merge's post-image. **Use merge-tree ranges below.**
- Did not checkout `/Users/s1kor/dev/trading/rsibot/hummingbot`. Did not write into either PR worktree.

Conflicted files (only these two):

- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `test/test_ok_rsi_down_only_entry_execution.py`

**Conflict region count: 6** (`<<<<<<<` hunks). 4 in the executor, 2 in the test file.

Do not ship the merge-tree blob. Hunk 4 leaves mixed identifiers (`_live_maker_decision` body glued onto a `_submission_is_valid` header). Several required pieces auto-merged outside conflict markers and still need a conscious keep/drop.

---

## 1. Conflict regions (merge-tree line ranges and owning functions)

Line numbers are in the conflicted merge-tree blobs, not in either PR file.

### `down_only_entry_execution.py` (4 hunks)

| # | Lines | Function | #50 | #51 | t4 take |
| --- | --- | --- | --- | --- | --- |
| E1 | 215–220 | `control_open_order` | Early return when `_entry_replacement_purpose == ENTRY_REPLACEMENT_DOWNWARD_REPRICE` (`cancel_pending`) | Empty (no extra return) | **#51 empty.** The early return skips deadline and permission while a reprice cancel is outstanding (audit blocker C; breaks `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`). |
| E2 | 237–241 | `control_open_order` | `decision, best_bid, best_ask, now = self._live_maker_decision(self._entry_current_price)` | `now, best_bid, best_ask, decision = self._current_maker_decision()` | **#50 call shape.** Keep `_live_maker_decision`. After wiring permission (section 3), delete `_current_maker_decision`. |
| E3 | 323–395 | After `early_stop`: #50 `_bind_replacement` / `_clear_replacement` vs #51 `_live_book` / `_live_entry_permission` / `_current_maker_decision` | Repair bind/clear with purpose + pending target | Clock+book helper, controller permission, combined decision | **Both, not either.** Keep #50 bind/clear. Insert #51 `_live_entry_permission`. Do **not** take #51 `_live_book` or `_current_maker_decision` here (see section 2). |
| E4 | 635–701 | After `_submit_reprice_child`: #50 `_intent_age_expired` / `_tick_size` / `_live_book` / `_maker_safe_price` / start of `_live_maker_decision` vs #51 `_submission_is_valid` | Typed book, maker-safe price, intent-clock decision | Permission/book/intent recheck that terminals the intent | **#50 methods.** Fold #51's permission recheck into `_live_maker_decision` + `_intent_allows_price`. Do not keep a second `_submission_is_valid` path. Discard the auto-merged remainder after this hunk (it names `order_created_at` / `use_intent_clock` / `opposing_signal` in one broken function). |

### `test_ok_rsi_down_only_entry_execution.py` (2 hunks)

Git matched a shared `request_reprice` / `finish_reconciliation` tail, so hunk T2 is two different tests smashed together.

| # | Lines | Functions | #50 | #51 | t4 take |
| --- | --- | --- | --- | --- | --- |
| T1 | 702–833 | Starts after shared `test_full_fill_after_cancel_ack_supersedes_pending_reprice` | `test_post_only_repair_allows_same_price_when_book_is_unchanged`; start of `test_replacement_local_cross_waits_inside_original_intent` | `test_operator_pause_keeps_filled_trailing_owner_running`; `test_operator_pause_cancels_unfilled_acquisition_without_position_hold`; `test_operator_pause_cancels_partial_remainder_without_keep_position_stop`; `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`; `test_operator_pause_partial_fill_ack_keeps_managing_fill_owner`; header of `test_unbound_controller_cannot_place_replacement_after_recon` | **Keep all of both sides.** Concatenate. Do not pick one. |
| T2 | 840–1041 | Continuation of the smashed tail | Rest of #50 repair tests: `test_replacement_local_cross_waits_inside_original_intent` body, `test_pending_submission_does_not_resubmit_when_ask_crosses`, `test_confirmed_post_only_cancel_permits_one_fresh_book_repair`, `test_unknown_cancel_without_pending_target_does_not_repair_or_rest`, `test_local_cross_before_first_submission_waits_or_recomputes_inside_ceiling`, `test_cancel_with_unobserved_reported_fills_does_not_treat_missing_as_zero`, `test_partial_fill_during_post_only_cancel_manages_fill_without_repair`, `test_duplicate_cancel_and_late_reconciliation_do_not_revive_or_duplicate_child`, `test_stop_while_repair_pending_blocks_new_buy_and_keeps_fills` | Body of `test_unbound_controller_cannot_place_replacement_after_recon`; `test_replacement_does_not_submit_after_permission_or_book_revoked` | **Keep all #50 tests. Keep both #51 tests.** Adapt only the `locked_book` row of the last #51 test (section 4). |

---

## 2. `_live_book` signature: keep the #50 typed book

### #50 (`down_only_entry_execution.py` 562–571)

```python
def _live_book(self) -> Optional[tuple[Decimal, Decimal]]:
    bid = self._to_decimal(self.get_price(... BestBid))
    ask = self._to_decimal(self.get_price(... BestAsk))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or bid >= ask:
        return None
    return bid, ask
```

Contract: optional **(bid, ask)** execution evidence. `_to_decimal` is `PositionExecutor._to_decimal` (finite `Decimal` or `None`). Missing, non-finite, non-positive, locked, or crossed book is `None`, not a printable triple.

### #51 (`down_only_entry_execution.py` 235–243)

```python
def _live_book(self):
    now = Decimal(str(self._strategy.current_timestamp))
    best_bid = Decimal(str(self.get_price(... BestBid)))
    best_ask = Decimal(str(self.get_price(... BestAsk)))
    return now, best_bid, best_ask
```

Contract: untyped **(now, bid, ask)**. Clock is mixed into the book. `Decimal(str(...))` does not reject non-positive, locked, or crossed books; bad values raise or flow into `decide_down_only_maker`.

### What t4 must keep

Keep **#50**: `def _live_book(self) -> Optional[tuple[Decimal, Decimal]]`.

- Book evidence is `(bid, ask)` or `None`. That is the child-boundary data gate (audit blocker D).
- Clock stays in `_live_maker_decision` / `_intent_age_expired` as `Decimal(str(self._strategy.current_timestamp))`.
- Do not keep #51's 3-tuple. Callers that write `now, bid, ask = self._live_book()` are wrong after this merge.
- Route `_live_maker_decision` through `_live_book()`. #50 still duplicates `Decimal(str(self.get_price(...)))` inside `_live_maker_decision` (590–597). That bypasses the typed gate and must be removed in t4: if `_live_book()` is `None`, treat as unusable inputs (wait inside remaining intent age; terminal `total_age_expired` / `input_unavailable` only on the same rules #50 already uses in `_submit_initial_maker` / `_submit_reprice_child`).

---

## 3. One class: #50 cancel/repair/deadline + #51 permission/data gates

Skeleton is **#50** `DownOnlyMakerPositionExecutor`. Fold #51 gates into it. Do not keep two decision helpers.

Keep from #50 (already present outside most hunks; do not drop):

- Constants `OKX_POST_ONLY_CANCEL_SOURCE`, `ENTRY_REPLACEMENT_DOWNWARD_REPRICE`, `ENTRY_REPLACEMENT_POST_ONLY_REPAIR`
- `_entry_replacement_of_order_id` / `_entry_replacement_purpose`
- `_entry_terminal_reason` as a property alias of `_entry_stop_reason` (lets #51 tests that read `_entry_terminal_reason` keep working)
- `_acquisition_permitted` (executor running, not closed, no stop reason)
- `_replacement_bound_to`, `_is_confirmed_post_only_cancel`
- `_order_awaits_exchange_update`, `_order_is_live_for_price_decision` (PENDING_CREATE / no exchange id must not reprice)
- `_submit_initial_maker` + `_maker_safe_price` + `_intent_allows_price`
- `early_stop` that always `_clear_replacement` and keeps fills as `managing_fill`
- `process_order_canceled_event` repair bind on confirmed source `31`, unknown cancel → super (no silent rest)
- `_reconcile_expired_entry` gated on purpose + `_entry_reprice_cancel_confirmed`
- `_submit_reprice_child` purpose/budget/same-price-repair vs downward-only, wait on unusable book inside intent age

Keep from #51 (insert; some already auto-merged):

- `_live_entry_permission() -> tuple[bool, bool]` (`opposing_signal`, `risk_allowed` from `strategy.controllers[controller_id]`, `manual_kill_switch`, `_operator_entry_paused`; missing/unbound controller → `(False, False)`)
- `_request_current_cancel` 5s retry via parent `_entry_cancel_last_attempt_at` **plus** #50 replacement metadata in the lifecycle dict (merge-tree already combined this; keep it)
- Pending-target re-eval in `control_open_order`: if a cancel is already outstanding, still run `_live_maker_decision` and escalate `CANCEL_TERMINAL` (deadline / risk / opposing). That is blocker C. Drop E1's early return or this never runs for downward reprice.

Wire, do not duplicate:

1. In `_live_maker_decision`, replace hardcoded `opposing_signal=False, risk_allowed=True` with `_live_entry_permission()`. Use `_live_book()` for bid/ask. Keep `use_intent_clock` for `_intent_allows_price` / child boundary (same idea as #51 `_submission_is_valid` resetting order clock to intent).
2. `_intent_allows_price` remains the post-recon / pre-submit gate. Permission revocation terminals (`risk_blocked` / `opposing_signal`). Unusable or locally crossing book does **not** submit; inside remaining age it waits (`awaiting_order_update`), matching #50 repair. Do not copy #51 `_submission_is_valid`'s immediate `stop()` on locked book.
3. `_acquisition_permitted` stays executor-local. Controller pause/kill/signal is **not** that helper; it is `_live_entry_permission` inside the decision snapshot so `decide_down_only_maker` emits `CANCEL_TERMINAL` on a still-live OPEN/PARTIAL order (pause tests) without `early_stop(keep_position=True)`.
4. Filled path stays above decision (`OrderState.FILLED` → managing fill). Pause must not kill a filled trailing owner.
5. `__init__`: keep #50 replacement fields. Do not re-add a shadow `_entry_terminal_reason` instance attribute (#51/base). `_entry_cancel_last_attempt_at` is already initialized on `PositionExecutor`.

Auto-merged trap: after E2, merge-tree kept #51's `if self._entry_pending_target is not None:` block **and** #50's wait-state returns. t4 must order them so PENDING_CREATE still skips price chasing, but an OPEN order with outstanding reprice cancel still rechecks deadline and permission.

---

## 4. Test-file overlap

Helpers (auto-merged, no hunk): keep #51 `bind_controller` and the `make_executor` call to it, plus `SimpleNamespace`, `TrailingStop`, `CloseType`. Default `signal=1` so the 30 shared tests still see `risk_allowed=True`.

### Keep all 30 shared tests (no hunk; merge-tree took #50's parametrize)

Including `test_replacement_rechecks_book_and_deadline_after_reconciliation` **without** #51's extra `(12, "99.8", "99.9", "submission_would_cross")` row. Local cross at the child boundary waits inside the intent (#50 `test_replacement_local_cross_waits_inside_original_intent`), it does not terminal `submission_would_cross`.

### Keep all 10 #50-only tests

- `test_post_only_repair_allows_same_price_when_book_is_unchanged`
- `test_replacement_local_cross_waits_inside_original_intent`
- `test_pending_submission_does_not_resubmit_when_ask_crosses`
- `test_confirmed_post_only_cancel_permits_one_fresh_book_repair`
- `test_unknown_cancel_without_pending_target_does_not_repair_or_rest`
- `test_local_cross_before_first_submission_waits_or_recomputes_inside_ceiling`
- `test_cancel_with_unobserved_reported_fills_does_not_treat_missing_as_zero`
- `test_partial_fill_during_post_only_cancel_manages_fill_without_repair`
- `test_duplicate_cancel_and_late_reconciliation_do_not_revive_or_duplicate_child`
- `test_stop_while_repair_pending_blocks_new_buy_and_keeps_fills`

### Keep all 7 #51-only tests, with one row change

Keep as-is:

- `test_operator_pause_keeps_filled_trailing_owner_running`
- `test_operator_pause_cancels_unfilled_acquisition_without_position_hold`
- `test_operator_pause_cancels_partial_remainder_without_keep_position_stop`
- `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`
- `test_operator_pause_partial_fill_ack_keeps_managing_fill_owner`
- `test_unbound_controller_cannot_place_replacement_after_recon`

Adapt:

- `test_replacement_does_not_submit_after_permission_or_book_revoked`
  - Keep rows `pause`, `kill`, `opposing` (terminal `risk_blocked` / `opposing_signal`, no child).
  - Change `locked_book`: no child, `_entry_stop_reason is None`, state `awaiting_order_update` (same as #50 local-cross wait). Do not expect terminal `input_unavailable`. That assertion would kill post-only repair when the book is locked.

---

## 5. Ordered resolution recipe for t4 (no product edits here)

1. Copy #50 `down_only_entry_execution.py` as the working file. Do not start from the conflicted merge-tree blob.
2. Insert `_live_entry_permission` next to the #50 helpers (`_acquisition_permitted` / `_replacement_bound_to`).
3. E1: omit the downward-reprice early return.
4. E2: call `_live_maker_decision` (keep unpack `decision, best_bid, best_ask, now`).
5. In `control_open_order`, after a successful live decision: if `_entry_pending_target is not None`, keep #51's escalate-or-reissue-cancel block so deadline/permission still apply while cancel is outstanding. Keep #50 PENDING_CREATE / non-live waits **before** that decision so an unacked first submit cannot reprice.
6. E3: keep `_bind_replacement` / `_clear_replacement`. Do not paste #51 `_live_book` / `_current_maker_decision`.
7. Keep auto-merged `_request_current_cancel` (replacement fields + 5s retry). Keep #50 `process_order_canceled_event` / `_reconcile_expired_entry` / `_submit_reprice_child`.
8. E4: keep `_intent_age_expired`, `_tick_size`, typed `_live_book`, `_maker_safe_price`, `_live_maker_decision`, `_intent_allows_price`. Point `_live_maker_decision` at `_live_book()` + `_live_entry_permission()`. Drop `_submission_is_valid` and `_current_maker_decision`.
9. Tests: start from #50 file; add `bind_controller` into `make_executor` (copy from #51); append the seven #51 tests after the #50 repair tests; edit the `locked_book` row as in section 4.
10. Do not resolve by accepting ours/theirs on either file. A mechanical take of #50 drops pause/kill/unbound gates (blocker D). A mechanical take of #51 drops post-only repair, maker-safe wait, and typed book.
11. After edits, the overlap files must compile and the union of tests in section 4 must be present. Native pytest is t4/auditor work, not this map.

---

## Concerns for t4 (not unresolved here)

1. Merge-tree auto-merge is not a valid class. Especially E4 and the `pending_target` block sitting after #50 wait-state returns.
2. `#51 locked_book` / `submission_would_cross` terminal vs `#50` wait-inside-intent. Recipe keeps wait so repair survives. If t4 terminals locked books, #50 repair tests fail.
3. `#50 _live_maker_decision` currently ignores `_live_book()`. Leaving that in place makes the typed contract decorative.
4. `_order_awaits_exchange_update` includes `PENDING_CANCEL`. If a real cancel flips the mock/order to pending-cancel, deadline re-eval must still run (blocker C). The #51 retry test keeps state `OPEN`, so it does not prove the pending-cancel path.
