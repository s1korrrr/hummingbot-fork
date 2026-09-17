# t9 — Reviewer: consolidated candidate

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
Head: uncommitted working tree (combined index + unstaged vs base)
Diff: `reviews/review-4a4372bc-worktree.diff` (14 files, +2315/−305)
Audit: `reports/audit.md` (gate PASS on split pytest; concatenated union is an isolation hazard)

Implementer reports were treated as claims. Judgment is from the diff and the auditor’s commands.

## Verdicts

- Spec: **PASS**
- Quality: **NOT APPROVED**
- Production-ready for this scope: **no**

## Spec

**PASS.** Every A–G item is present in the combined tree, with a focused test the auditor actually ran. Constraints are kept. Nothing required is missing. The extra volume is the #50/#51 merge the completion bar asked for, not a third policy.

### Item map

| Item | Verdict | Evidence |
| --- | --- | --- |
| A delayed OPEN cannot revive CANCELED/FILLED/FAILED | implemented | `InFlightOrder.update_with_order_update` skips those reopen states; TradeUpdate path unchanged; FILLED-after-CANCELED still assigns. Tests: `test_update_with_order_update_does_not_reopen_*`, `test_update_with_trade_update_still_applies_after_canceled`, OKX cached REST reorder tests. Auditor: 36 passed. |
| B source 32 beats bound reprice | implemented | `process_order_canceled_event` classifies `"32"` before `_replacement_bound_to`, clears replacement, `super()`. Bound + missing source still confirms local reprice. Test: `test_stp_cancel_source_does_not_place_bound_reprice_child`. Auditor: 65 passed. |
| C deadline/revocation off wait state; cooldown at submit | implemented | Wait branch calls `_revocation_or_deadline_reason()` then cancels the same identity. Cooldown sits immediately before `place_order` for both replacement purposes. Tests: `test_pending_create_expires_intent_without_dropping_identity`, `test_reprice_child_honors_cooldown_at_submit`, `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`. |
| D owner + execution data at child boundary | implemented | `_live_entry_permission` refuses non-RUNNING `status` and present `data_health.ready` false/missing. No `can_create_executor` on the child path. `_live_book()` is `Optional[tuple[Decimal, Decimal]]`; locked/crossed waits. Fills stay `managing_fill`. Tests: `test_stopped_owner_cannot_place_replacement_after_recon`, `test_data_health_unreadiness_blocks_child_despite_cached_quotes`, adapted `locked_book`. |
| E owned vs outstanding vs labels | implemented | `managing_fill` no longer zeros remainder. TERMINATED leftover stays in `_buy_executors_for_capital` until `accounting_owner == held_position`. Missing/conflict/over-authorized owned → `None` → Infinity. Tests: managing_fill 0.1/0.9/100, TERMINATED leftover, missing/conflict receipts. Auditor: 14 passed. |
| F next quantized close qty | implemented | `_expected_close_fee_rate` uses `_close_order_amount()`. `<= 0` or `None` → `None`. `is_maker` only for `LIMIT_MAKER`. Tests: 0.01 residual, 0.1 remainder after prior close, blocked qty holds. Auditor: 110 passed. |
| G `cancel_source` JSON | implemented | `to_json` writes the field; `from_json` restores non-blank; `json.dumps`/`loads` round-trip; restored CANCELED is not `is_open`. Tracker restore still open-only (2 passed extra). |
| Overlap merge | implemented | No conflict markers. Union of overlap tests present (65). Started from #50 class, not merge-tree blob `d22b57867`. |
| Preserve | implemented | Chronological `_build_backtest_signal_frame` kept (one walk). Ordinary LIMIT is taker. `cancel_source` persisted. Entry repair/pause/fill recovery kept. FILLED-after-CANCELED OrderUpdate not over-blocked. Bound identity is not treated as STP cause. |
| Authority | implemented | No merge, deploy, restart, live orders, or hummingbot checkout edits. |

### Missing

None versus the spec’s A–G list and preserve list.

### Extra (in-scope merge, not out of spec)

PR #51 content that is not a named blocker: averaging-down gate (`averaging_score_low` vs owned underwater, runner exempt), pause no longer `POSITION_HOLD` in `ok_rsi.stop_actions_proposal`, backtest helper rewritten to one chronological inventory walk, spacing/drawdown using remaining basis. That is the required three-way merge, not a new trading policy invented here.

### Extra (not a spec fail)

None that violate “do not change approved trading policy.” The #51 averaging/pause semantics are the PR being folded in.

## Quality

**NOT APPROVED.** Spec behavior is in place, tests exist, and flake8 on product files was clean. One Important capital-release hole remains. The t2 recipe chose the `accounting_owner` flag as proof of transfer. That choice still ships a premature release.

### Critical

None.

### Important

1. **`POSITION_HOLD` drops executor capital before the hold bag exists.**

   `PositionExecutor.get_custom_info` sets `execution_receipt.accounting_owner` to `"held_position"` as soon as `close_type == CloseType.POSITION_HOLD`. `early_stop(keep_position=True)` writes that close type immediately and moves the executor to `SHUTTING_DOWN`.

   `HLRSIController._executor_counts_toward_buy_capital` and `_executor_owned_base` then treat that flag as a full drop: owned 0, outstanding 0, executor omitted from `_buy_executors_for_capital`. `_executor_acquisition_remainder_reconciled` also returns True on the same flag.

   The bag in `positions_held` is a later controller/orchestrator write. Until that write lands, `_get_total_position_value_usd` can understate exposure and `can_create_executor` can admit a new BUY. Kill and opposing signal have other gates. Bag freeze does not: it `POSITION_HOLD`s filled owners while capacity may free as those executors leave the active filter.

   Spec E: release only reconciled commitment. The named tests never cover `accounting_owner=held_position` with empty `positions_held`. `test_terminated_leftover_owned_*` always pairs the flag with a bag of the same size. t6 already named this window. It is still open.

   Fix: keep executor owned in the sum until matching `positions_held` inventory is observed, or treat the flag as transfer-in-progress (Infinity / refuse new buys), not as release.

### Minor

1. **Absent `data_health` still allows a child.** Unreadiness is enforced only when a `data_health` object is present. `bind_controller` omits it so the 30 shared tests stay `risk_allowed=True`. A running controller that stops publishing health entirely still places on cached quotes. Present `ready=False` is covered.

2. **Missing `controller.status` is treated as allowed.** Production `RunnableStatus` always exists. The string `"STOPPED"` row is a mock. Harmless for real owners, misleading if a future owner omits status.

3. **First replacement after the parent skips cooldown.** `_submit_reprice_child` only waits if `_entry_last_reprice_at` is set. That field is written after a child places, so parent → first repair is uncooled. The audit probe (1s after a prior child, 30s cooldown) is covered because the test stamps `last_reprice_at`. Shared count is enforced.

4. **`_intent_age_expired` swallows clock errors as not expired.** Wait-state expiry uses this helper. `_live_maker_decision` still terminals on other input exceptions. A broken `current_timestamp` leaves PENDING_CREATE owned and unexpired.

5. **Fee estimate mutates close-quantity state.** `_expected_close_fee_rate` calls `_close_order_amount()`. On `single_attempt` / sealed resume that writes `_close_quantity` and can warn every trailing tick. Economics of the residual tests are right. The helper is not a pure read.

6. **OKX WS delayed `live` never reaches the new guard.** `_user_stream_event_listener` looks up `all_updatable_orders`, which skips the TTL cache. A WS OPEN after cancel is dropped, so the object stays CANCELED. REST / `process_order_update` on the cached object is the path A actually needed, and it is tested. A WS-only test would have passed before A.

7. **`InFlightOrder.attributes` / `__eq__` omit `cancel_source`.** JSON round-trip is the persistence check. Equality will not catch a dropped field.

8. **Refused OPEN that still copies `exchange_order_id` returns `updated=True`.** Tracker `_trigger_order_completion` can fire a second cancel event on that object. State stays CANCELED. The OKX cached tests already have an exchange id, so they do not hit this.

9. **Concatenated pytest union: 23 `InFlightOrderPyUnitTests` setup errors.** Pre-existing `asyncio.get_event_loop()` after pytest-asyncio files. Split invocations are the honest suite. Do not claim a one-shot union.

10. **This worktree is not a compiled Cython checkout.** Collection used gitignored `*.so` copies. A clean clone fails until those modules exist. Not a product logic bug.

11. **Working tree is mixed `MM`.** Combined contents vs base are what was reviewed. A commit that stages only the index would drop unstaged A/E/F hunks. Process risk, not a spec miss.

## Auditor evidence (not re-run)

Split pytest and flake8 exited 0. Counts: 65 down-only, 36 native/OKX, 14 capital, 110 position_executor, 36 factory, plus 2 tracker restore tests. No Docker, CI, live, or exchange run. That matches the authority ceiling.

## Constraints

- Authority `repository-process-only`: held.
- Approved policy: not replaced with a third book. #50 repair + #51 permission/capital/fees.
- Late TradeUpdates and cumulative fills: still applied.
- Explicit 32 wins over bound reprice. 31 remains post-only. Bound id is not cause.
- Expiry cancels the current identity; it does not invent a child.
- Child path does not rerun `can_create_executor`. Owned fills stay `managing_fill`.
- `_live_book() -> Optional[tuple[Decimal, Decimal]]` kept. Locked book waits.
- FILLED-after-CANCELED OrderUpdate still applies.

## Production-ready

**No.** Spec is implemented and tested. Quality is blocked on Important finding 1 (hold flag vs hold bag). No Critical. Fix that release window, keep the named E tests, add a POSITION_HOLD + empty `positions_held` case that refuses a new BUY or keeps the leftover reserved, then this candidate can be re-reviewed.
