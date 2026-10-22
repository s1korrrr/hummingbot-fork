# t4 — Merge overlap files and fix B, C, D

Status: **DONE_WITH_CONCERNS**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation` at base `4a4372bcde4990355421a42df3b8ce6107299abb`
Git: no commit, no push, no PR. Checkout files are staged; overlap merge edits are unstaged on top of that (`MM`).

## What landed

Brought both PRs in without a merge commit:

- PR50-only from `9334c20c3` (untouched after checkout)
- PR51-only from `d38705b08` (untouched after checkout)
- Overlap started from **#50**, then folded #51 permission and closed B/C/D

Did not start from merge-tree blob `d22b57867`. Did not accept ours/theirs.

## Overlap recipe applied

1. #50 `DownOnlyMakerPositionExecutor` is the class.
2. `_live_entry_permission` sits next to `_acquisition_permitted`.
3. E1 downward-reprice early return omitted.
4. `_live_maker_decision` unpack remains `decision, best_bid, best_ask, now`.
5. PENDING_CREATE / missing exchange id / PENDING_CANCEL still skip price chasing, but deadline and permission run first. Outstanding cancel on a live OPEN order re-runs `_live_maker_decision` and escalates `CANCEL_TERMINAL`.
6. `_bind_replacement` / `_clear_replacement` kept. No #51 `_live_book` 3-tuple. No `_current_maker_decision`.
7. #50 `process_order_canceled_event` / `_reconcile_expired_entry` / `_submit_reprice_child` skeleton kept, then B and C.
8. Typed `_live_book() -> Optional[tuple[Decimal, Decimal]]` kept. Clock stays in `_live_maker_decision` / `_intent_age_expired`.
9. `_live_maker_decision` calls `_live_book()` and `_live_entry_permission()`. `_submission_is_valid` and `_current_maker_decision` are gone.
10. Tests: #50 file + `bind_controller` in `make_executor` (default `signal=1`) + all 7 #51 tests. `locked_book` row waits (`awaiting_order_update`, stop unset), not `input_unavailable`.

`_request_current_cancel` keeps #50 replacement metadata and #51's 5s retry (`_entry_cancel_last_attempt_at` already on `PositionExecutor`).

## Blocker B

Venue source is classified **before** `_replacement_bound_to`.

- `"32"` (`OKX_STP_CANCEL_SOURCE`): `_clear_replacement()` then `super()` / `exchange_canceled`. No child.
- `"31"`: existing post-only repair bind still works when unbound.
- Bound + missing `cancel_source`: local-reprice confirm unchanged (`test_confirmed_zero_fill_cancel_*`).

Test: `test_stp_cancel_source_does_not_place_bound_reprice_child`.

## Blocker C

- Wait states call `_revocation_or_deadline_reason()` (permission then `_intent_age_expired`) before returning. Expiry `_request_terminal_cancel`s the **same** identity. No new child. Stop latch set.
- Shared replacement **count** was already in `_submit_reprice_child`. **Cooldown** now sits immediately before `place_order` for both `downward_reprice` and `post_only_repair`. `_intent_allows_price(..., use_intent_clock=True)` is not used as a cooldown substitute. Cooldown wait leaves the child authorized and sets `awaiting_order_update`.

Tests: `test_pending_create_expires_intent_without_dropping_identity`, `test_reprice_child_honors_cooldown_at_submit`, plus #51 `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`.

## Blocker D

No `can_create_executor` on the child path.

`_live_entry_permission`:

- Missing/unbound controller → `(False, False)`
- Controller present and `status` set to anything other than `RunnableStatus.RUNNING` → `risk_allowed=False`
- `data_health` present with `ready` false or missing → `risk_allowed=False`
- Kill / operator pause / opposing signal kept from #51

Unusable book (`_live_book()` None) waits inside remaining age. Numeric quotes with `data_health.ready=False` refuse the child via permission.

If `_request_terminal_cancel` sees `cumulative_filled_base > 0` and no open order, it keeps `managing_fill` and does not `stop()` / `EXPIRED`.

Tests: `test_stopped_owner_cannot_place_replacement_after_recon`, `test_data_health_unreadiness_blocks_child_despite_cached_quotes`, existing pause/kill/opposing/unbound, `test_operator_pause_keeps_filled_trailing_owner_running`.

## Tests

Union required by t3 is present: 30 shared (including `test_replacement_rechecks_book_and_deadline_after_reconciliation` without the #51 `submission_would_cross` row), 10 #50-only, 7 #51-only with `locked_book` adapted, plus B/C/D regressions.

Command:

```bash
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/test_ok_rsi_down_only_entry_execution.py --import-mode=importlib -q
```

Result: **65 passed in 2.96s** (conda env `hummingbot`). flake8 on the two overlap files: clean. Not native CI/Docker.

This worktree had no compiled Cython `.so` modules. Pytest needed the gitignored `*.so` set rsynced from the PR50 worktree (`hummingbot-down-only-postonly-repair`). Those files were not staged.

## Files changed

Edited (overlap merge):

- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `test/test_ok_rsi_down_only_entry_execution.py`

Checkout only (do not treat as t4 product edits):

- PR50: `hummingbot/connector/exchange/okx/okx_exchange.py`, `hummingbot/core/data_type/in_flight_order.py`, `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`
- PR51: `controllers/directional_trading/hl_rsi.py`, `controllers/directional_trading/ok_rsi.py`, `hummingbot/strategy_v2/executors/position_executor/position_executor.py`, `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`, `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`, `test/test_hl_rsi_entry_control.py`, `test/test_ok_rsi_admission_safety.py`, `test/test_ok_rsi_down_only_entry_factory.py`

Not edited (owned by later tasks): `in_flight_order.py` beyond PR50 checkout (A/G = t5), `hl_rsi.py` capital helpers beyond PR51 checkout (E = t6), fee helpers in `position_executor.py` beyond PR51 checkout (F = t7). `/Users/s1kor/dev/trading/rsibot/hummingbot` was not touched.

## Concerns

1. `RunnableStatus` has no `STOPPED`. D uses `TERMINATED`, `SHUTTING_DOWN`, and a string `"STOPPED"` for "other non-running status". Missing `controller.status` is still treated as allowed so default `bind_controller` (no status) keeps the 30 shared tests `risk_allowed=True`.
2. Deadline/permission on `PENDING_CANCEL` uses the wait-state helper, not a full `_live_maker_decision` (book rails). The #51 retry test keeps state `OPEN`. Same gap t3 already named.
3. Local pytest is not a compiled worktree. It depended on gitignored `.so` copies from the PR50 tree. Re-running in a clean clone of this worktree will fail collection until Cython is built or those modules are present.
4. Cooldown wait leaves `_entry_child_submission_authorized` True (t1: "leave authorized or wait"). A later tick with elapsed cooldown will place.
5. `_revocation_or_deadline_reason` is a small extra helper so wait-state and None-book child paths share the same three gates. Not a second admission stack.
