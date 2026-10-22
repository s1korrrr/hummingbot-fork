# t8 — Auditor: focused suites on the consolidated worktree

You are the auditor/validator in grok-orchestra. Do not change product code. Do not weaken tests.

## Repo

`/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`

Do not checkout or edit `/Users/s1kor/dev/trading/rsibot/hummingbot`.

## Spec / briefs / worker reports

- Spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`
- Ledger: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/ledger.md`
- t4: `.../reports/t4-merge-bcd.md`
- t5: `.../reports/t5-ag-native-order.md`
- t6: `.../reports/t6-capital.md`
- t7: `.../reports/t7-fees.md`

## Write evidence to

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/audit.md`

## How to test

Use the existing conda env, same as workers:

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -q
```

This worktree may need gitignored Cython `.so` modules (t4 rsynced them from the PR50 worktree). If collection fails, say so; do not pretend native CI ran. Do not start Docker, the live stack, or exchanges.

## Required commands (capture command, exit code, summary)

Run as one or a few invocations covering the union:

1. `test/test_ok_rsi_down_only_entry_execution.py`
2. `test/hummingbot/core/data_type/test_in_flight_order.py`
3. `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`
4. `test/test_hl_rsi_entry_control.py`
5. `test/test_ok_rsi_admission_safety.py`
6. `test/hummingbot/strategy_v2/executors/position_executor/`
7. `test/test_ok_rsi_down_only_entry_factory.py` if it exists in this tree
8. flake8 or the repo's existing linter on the touched product files only

## Spec items to mark implemented / missing / cannot verify

A delayed OPEN cannot revive CANCELED/FILLED
B source 32 beats bound reprice
C deadline while PENDING_CREATE; cooldown at child submit
D non-RUNNING owner and data_health unreadiness with cached quotes refuse child
E managing_fill does not release unresolved remainder; TERMINATED leftover reserved until hold transfer; missing/conflict → unavailable
F next quantized close qty for flat fees
G cancel_source JSON round-trip
Overlap file has no conflict markers
No Docker/live/CI claim unless you actually ran it

Fail the gate if new behavior has no tests, or if tests/linters fail.

Return: gate PASS or FAIL, audit path, bullet list of gaps.
