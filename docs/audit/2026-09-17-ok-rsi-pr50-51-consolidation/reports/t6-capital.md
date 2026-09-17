# t6 — Capital release (E)

Status: **DONE_WITH_CONCERNS**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation`
Git: no commit, no push, no PR. PR51 `hl_rsi.py` was already checked in; E is unstaged on top of that.

Did not edit `down_only_entry_execution.py`, `position_executor.py`, or `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## What landed

Owned inventory, unresolved acquisition, and report labels are separate.

- `_executor_acquisition_may_still_fill` still keys off `managing_fill` / `ended_no_fill` / `held_residual` for spacing and status. That is the report label.
- `_executor_outstanding_base` no longer treats those labels as "remainder is zero". Remainder stays reserved until a terminal confirmation exists.
- `_get_active_buy_executor_position_value_usd` walks buy executors with `active_only=False`, then keeps TERMINATED/non-active owners that still bear leftover or uncertain capital and have `accounting_owner != held_position`.
- Missing, non-finite, conflicting remaining vs receipt vs filled−closed, or owned > authorized returns `None` from `_executor_owned_base`, which values as Infinity and refuses new buys (`exposure_unavailable`). Missing filled on a still-responsible executor is no longer owned 0.

Terminal confirmation (any one): native order `CANCELED`/`FILLED`/`COMPLETED`, `open_order_expiry_reconciled`, `entry_lifecycle.terminal_exchange_timestamp`, `ended_no_fill`/`held_residual`, hold transfer, or `is_done` with no open native order / `reconciliation_required`. Open native states and `reconciliation_required` keep the remainder reserved. `FAILED` is not treated as reconciled.

SHUTTING_DOWN is not `is_done`, so pending remainder stays reserved unless one of the confirmations above is present.

## Composition probe

Authorized 1.0 at 100, acquired 0.1, 0.9 unresolved, `managing_fill`, no terminal confirmation:

- owned 0.1, outstanding 0.9, valuation 100, not owned-only 10.

Encoded as controller methods on a constructed executor in `test_managing_fill_without_terminal_confirmation_keeps_unresolved_commitment`, and again on a live `PositionExecutor.executor_info` in `test_managing_fill_label_without_terminal_confirmation_keeps_unresolved_remainder`.

## Other tests

- TERMINATED leftover remaining, `accounting_owner=executor` → reserved. Same inventory after `held_position` plus `positions_held` → same 40, no double count.
- Missing filled+remaining, remaining vs receipt mismatch, owned > authorized, non-finite remaining → Infinity.
- Existing SHUTTING_DOWN reserve test unchanged.
- Existing canceled-remainder test now sets `terminal_exchange_timestamp` so release is from confirmation, not the `managing_fill` label.

## Tests

```bash
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py --import-mode=importlib -q
```

Result: **14 passed in 6.35s** (conda env `hummingbot`). flake8 on the three owned files: clean. Not native CI/Docker.

## Files changed

- `controllers/directional_trading/hl_rsi.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`

## Concerns

1. Vanilla `PositionExecutor.get_custom_info()` does not publish `native_order_state`. Down-only does. Confirmation for a plain position executor is `terminal_exchange_timestamp`, expiry recon, `is_done`, or a hold receipt. The old canceled-remainder test had to stamp `terminal_exchange_timestamp`; the label alone would have kept 0.24 reserved.
2. `accounting_owner=held_position` drops the executor even if `positions_held` is still empty. That matches the spec transfer flag and can undercount during the window before the bag is recorded.
3. Capacity, cooldown, and drawdown still use `_filter_same_side(..., active_only=True)`, so a TERMINATED leftover does not occupy an executor slot. Only the capital sum includes it.
4. Missing filled still treats outstanding as the full authorized remainder (conservative). Valuation still fails closed because owned is unavailable.
