# t7 — Residual close fees (F)

Do not start until t4 has checked PR51 `position_executor.py` into the consolidation worktree. If that file is still base-only, STOP with NEEDS_CONTEXT.

## Work in

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

USER_REQUESTED_GIT=no. Do not commit/push/PR. Do not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## Read

- spec.md, reports/t2-pr51-context.md (section F), reports/t4-merge-bcd.md
- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t7-fees.md`

## Owned files

- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`

Do not edit `down_only_entry_execution.py` or `hl_rsi.py`.

## F

`_expected_close_fee_rate` / `_fee_rate_from_trade_fee` callers must use the **next quantized close quantity**, not original `open_filled_amount` after prior closes.

- Prefer the quantity `_close_order_amount` / `_single_attempt_spot_close_amount` would sell. If that helper blocks, hold (unknown/unavailable), do not invent a rate.
- Preserve LIMIT/taker classification from #51 (ordinary LIMIT is taker).
- Preserve unknown-fee hold, base-fee inventory conservation on entry, and calculated-floor vs market-order protection.
- Solve required proceeds under the existing no-loss interpretation.

Test: original fill 1.0, next sell 0.01 (or 0.1 remainder after 0.9 closed), same quote flat → floor uses `flat / (close_qty * price)`, not `flat / (1 * price)`. A price between the two floors holds.

## Status

DONE | DONE_WITH_CONCERNS | NEEDS_CONTEXT | BLOCKED
