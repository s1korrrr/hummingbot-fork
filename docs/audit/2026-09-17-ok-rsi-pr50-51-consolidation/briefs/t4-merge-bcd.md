# t4 — Merge overlap files and fix B, C, D

You are a grok-orchestra worker. Implement exactly this brief. Ponytail: smallest patch that closes the blockers. No extra architecture.

## Work in

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

Branch `feat/andrzej_ok_rsi_audit_consolidation` at base `4a4372bc`. Do **not** touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

USER_REQUESTED_GIT=no. Do not commit, push, or open PRs. Staging overlap/product files is fine.

## Read first

- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`
- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t3-overlap.md`
- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t1-pr50-context.md` (B, C)
- `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t2-pr51-context.md` (D)
- PR50 tree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-down-only-postonly-repair`
- PR51 tree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-entry-control-consistency`

## Write report to

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t4-merge-bcd.md`

## Bring both PRs into this worktree without a merge commit

Checkout **non-overlap** files, then keep #50 overlap as the starting point:

PR50-only:

- `hummingbot/connector/exchange/okx/okx_exchange.py`
- `hummingbot/core/data_type/in_flight_order.py`
- `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`

PR51-only:

- `controllers/directional_trading/hl_rsi.py`
- `controllers/directional_trading/ok_rsi.py`
- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`
- `test/test_ok_rsi_down_only_entry_factory.py`

Overlap start from **#50** (do not `checkout` #51 over these):

- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `test/test_ok_rsi_down_only_entry_execution.py`

Do not start from merge-tree blob `d22b57867`. Do not accept ours/theirs on the overlap files.

After checkout, **do not edit** PR50-only or PR51-only files beyond the checkout. t5/t6/t7 own A/G, E, F.

## Overlap merge recipe (from t3, now binding)

1. Copy #50 executor as working file (already true after checkout).
2. Insert `_live_entry_permission` next to `_acquisition_permitted`.
3. Omit the downward-reprice early return in `control_open_order` (E1 / blocker C).
4. Keep `_live_maker_decision` unpack `decision, best_bid, best_ask, now`.
5. Keep #50 PENDING_CREATE / non-live waits, but **deadline and permission still evaluate** before those returns (blocker C). If a cancel is outstanding on an OPEN order, still recheck deadline/permission and escalate terminal cancel.
6. Keep `_bind_replacement` / `_clear_replacement`. Do not paste #51 `_live_book` 3-tuple or `_current_maker_decision`.
7. Keep #50 `process_order_canceled_event` / `_reconcile_expired_entry` / `_submit_reprice_child` skeleton, then apply B and C below.
8. Keep `_intent_age_expired`, `_tick_size`, typed `_live_book`, `_maker_safe_price`, `_live_maker_decision`, `_intent_allows_price`.
9. `_live_maker_decision` must call `_live_book()` and `_live_entry_permission()`. Drop `_submission_is_valid` and `_current_maker_decision`.
10. Tests: start from #50 file; add #51 `bind_controller` into `make_executor`; keep all 30 shared + 10 #50-only + 7 #51-only tests; change `locked_book` row to wait (`awaiting_order_update`, stop unset), not terminal `input_unavailable`.

Keep #50 typed book:

```python
def _live_book(self) -> Optional[tuple[Decimal, Decimal]]:
```

Clock stays in `_live_maker_decision` / `_intent_age_expired`. If `_live_book()` is None, wait inside remaining intent age. Do **not** terminal `input_unavailable` on locked/crossed/missing quotes while age remains (repair must survive).

## Blocker B (must close)

In `process_order_canceled_event`, **classify venue source before** honoring `_replacement_bound_to`.

- OKX `"32"` (self-trade prevention) is prohibitive. Bound downward reprice must **not** authorize a child. Conservative `super()` / exchange-canceled path. Clear replacement.
- `"31"` may still bind/confirm post-only repair.
- Bound + **missing** `cancel_source`: keep current local-reprice confirm so existing none-source tests still pass. Do not require 31-or-die.
- Identity match is necessary, not sufficient to infer cause.

Test: pending downward reprice, `cancel_source="32"`, zero-fill recon → no `place_order` child.

## Blocker C (must close)

- Evaluate intent deadline and permission revocation **regardless of wait state** (`PENDING_CREATE`, `PENDING_CANCEL`, missing exchange id, outstanding reprice cancel).
- Expiry sets the stop latch and requests cancel of the **same** identity. It does not treat the in-flight order as gone and does not place a new child.
- Shared replacement **count and cooldown** at `_submit_reprice_child` immediately before `place_order`. Cooldown applies to both `downward_reprice` and `post_only_repair`. `_intent_allows_price` with `use_intent_clock=True` is not a substitute (it hides `last_reprice_at`).

Tests:

1. `PENDING_CREATE`, intent limit 300s, now 900s → stop latch set, no new child, current identity still owned / cancel requested.
2. `_entry_last_reprice_at` 1s ago, `entry_reprice_cooldown_seconds=30`, authorized repair or downward child → no `place_order`.

## Blocker D (must close)

Do **not** rerun `can_create_executor` / one-shot parent admission on the child path. Do not abandon owned fills when entry inputs fail.

In `_live_entry_permission`:

- Missing/unbound controller → `(False, False)` (already #51).
- Controller not RUNNING (STOPPED/TERMINATED/other non-running status still present in `strategy.controllers` with stale `signal >= 0`) → `risk_allowed=False`.
- `data_health.ready` false or missing when that object exists → `risk_allowed=False`.
- Keep kill / operator pause / opposing signal behavior from #51.

Unusable **book** is not the same as owner unreadiness. Numeric cached quotes with `data_health.ready=False` must still refuse the child via permission. Locked/crossed/`None` book waits inside remaining age (t3).

If `cumulative_filled_base > 0` (or equivalent owned fill) at a failed child gate: keep managing the fill; do not `stop()`/`EXPIRED` the owned position.

Tests:

1. After canceled-parent recon, owning controller TERMINATED or STOPPED, stale signal=1, still-good cached bid/ask → no child.
2. Provider/connector/data-health unreadiness (`data_health.ready=False`) with numeric cached quotes → no child.
3. Existing pause/kill/opposing and unbound-controller tests still pass.
4. Filled trailing owner on pause still stays running (`test_operator_pause_keeps_filled_trailing_owner_running`).

## Tests you must add or adapt

Union of both overlap test files, plus new regressions for 32, deadline-while-waiting, cooldown-at-submit, stopped owner, unreadiness-with-cached-quotes.

Discover how this repo runs pytest (conda env `hummingbot` if present). Run the overlap test file and any focused new tests. Do not claim native CI/Docker.

## Do not

- Edit `in_flight_order.py` beyond the PR50 checkout (A/G = t5)
- Edit `hl_rsi.py` capital helpers beyond the PR51 checkout (E = t6)
- Change fee helpers in `position_executor.py` beyond the PR51 checkout (F = t7)
- Change approved trading policy (one stop latch, repair only on 31, fills stay owned)
- Restart services, deploy, or invent runtime evidence

## Status

DONE | DONE_WITH_CONCERNS | NEEDS_CONTEXT | BLOCKED
