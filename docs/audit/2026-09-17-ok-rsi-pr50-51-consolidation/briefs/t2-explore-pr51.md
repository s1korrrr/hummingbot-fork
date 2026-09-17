# t2 — Context packet PR #51

Read-only. No edits, no git writes.

## Sources

- Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-entry-control-consistency`
- Head: `d38705b0813a6e39747908cbce74f1f08015e0dc`
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`

## Files to inspect

- `controllers/directional_trading/hl_rsi.py`
- `controllers/directional_trading/ok_rsi.py`
- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`
- `test/test_ok_rsi_down_only_entry_execution.py`
- `test/test_ok_rsi_down_only_entry_factory.py`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t2-pr51-context.md`

Include:

1. Diffstat vs base
2. For blockers D, E, F: function names, line ranges, what is checked vs missing
3. `_live_entry_permission`, `_live_book`, `_submission_is_valid` signatures and call sites
4. How `managing_fill` / early_stop labels affect `_executor_acquisition_may_still_fill` and valuation
5. `_expected_close_fee_rate`, `_fee_rate_from_trade_fee`, `_min_no_loss_sell_price` and whether they use original `open_filled_amount`
6. Existing tests that miss TERMINATED owner, unreadiness with cached quotes, residual quantity fees, missing receipts
7. Suggested smallest patch approach, no implementation
