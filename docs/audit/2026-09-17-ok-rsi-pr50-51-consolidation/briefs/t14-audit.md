# t14 — Re-audit after t13 covering-sum

You are the auditor/validator. Do not change product code. Do not weaken tests.

## Repo

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/audit.md` (overwrite; header "re-audit after t13")

## Run split (same as t11)

```bash
PY=/Users/s1kor/anaconda3/envs/hummingbot/bin/python
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
$PY -m pytest --import-mode=importlib -q test/test_ok_rsi_down_only_entry_execution.py
$PY -m pytest --import-mode=importlib -q test/hummingbot/core/data_type/test_in_flight_order.py test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py
$PY -m pytest --import-mode=importlib -q test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
$PY -m pytest --import-mode=importlib -q test/hummingbot/strategy_v2/executors/position_executor/
$PY -m pytest --import-mode=importlib -q test/test_ok_rsi_down_only_entry_factory.py
$PY -m flake8 controllers/directional_trading/hl_rsi.py controllers/directional_trading/ok_rsi.py controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py hummingbot/connector/exchange/okx/okx_exchange.py hummingbot/core/data_type/in_flight_order.py hummingbot/strategy_v2/executors/position_executor/position_executor.py
```

Confirm these tests exist, ran, and passed:

- `test_position_hold_without_held_bag_refuses_new_buy` (both files)
- `test_position_hold_undersized_or_prior_bag_refuses_new_buy` (both files)

Fail the gate if those are missing or failing, or if split suites/flake8 fail.

t13 residual: covering-sum only among claimants still on `executors_info`. Record as residual gap; do not fail solely on that unless a test proves premature release.

Return: gate PASS or FAIL, audit path, gaps.
