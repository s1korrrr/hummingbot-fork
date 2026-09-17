# Handoff — OK-RSI PR #50/#51 consolidation

Local candidate only. This branch is not a merge, pin, deploy, or restart.

## Status

A–G are closed on working tree vs `4a4372bc`. Split pytest (auditor, `reports/audit.md`): 65 down-only / 36 native+OKX / 22 capital / 110 position_executor / 36 factory. t18: spec PASS, quality APPROVED. No Critical or Important remaining.

This commit is the full working tree, not the old mixed index. Before restage the index was `+1283/−291` (13 files). The reviewed tree was `+2604/−305` (14 files), including unstaged-only `test/hummingbot/core/data_type/test_in_flight_order.py`. Mixed-staging (`MM`/`AM`) is historical if this commit matches `git diff 4a4372bc` for product files.

## Do not

- Merge, pin, or restart until a PR’s CI actually runs.
- Treat open GitHub PRs #50 (`9334c20c`) and #51 (`d38705b0`) as this candidate. Those heads stay old until this branch is pushed (not done here).
- Commit Cython `.so` copies. They are gitignored (`/**/*.so`) and must stay untracked.

## Evidence in this directory

Copied from orchestra run `20260917T063417Z`: `spec.md`, `ledger.md`, `README.md`, `briefs/`, `reports/`.

`reviews/review-4a4372bc-worktree.diff` was omitted (generated, large). Source of truth: `git diff 4a4372bc`.

The copied `ledger.md` snapshot still lists t19 as running. This file is the commit-time handoff.

## Product files vs `4a4372bc`

- `controllers/directional_trading/hl_rsi.py`
- `controllers/directional_trading/ok_rsi.py`
- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `hummingbot/connector/exchange/okx/okx_exchange.py`
- `hummingbot/core/data_type/in_flight_order.py`
- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`
- `test/hummingbot/core/data_type/test_in_flight_order.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`
- `test/test_hl_rsi_entry_control.py`
- `test/test_ok_rsi_admission_safety.py`
- `test/test_ok_rsi_down_only_entry_execution.py`
- `test/test_ok_rsi_down_only_entry_factory.py`
