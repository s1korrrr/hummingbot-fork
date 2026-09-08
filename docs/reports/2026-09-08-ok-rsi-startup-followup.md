# OK RSI startup follow-up — 2026-09-08

Scope: repository-process-only diagnosis and isolated local repair of risk-state restoration. No activation, process termination, database migration, configuration edits, exchange requests, or external publication.

Completion checklist:
- [x] Inspect supplied startup log and source provenance.
- [x] Inspect selected SQLite database in read-only mode and identify bot processes without exposing secrets.
- [x] Reproduce rejected-state overwrite using native strategy construction and temporary SQLite.
- [x] Preserve rejected state and make blocked status explicit.
- [x] Verify focused tests and inspect final diff.
- [ ] Runtime recovery: blocked:external; account reconciliation and polling ownership remain unresolved.

## Findings

The supplied log records PID 2394 starting `ok_rsi.yml` from the original Hummingbot checkout. Current process inspection also found PID 3364 using `ok_rsi_sui_sell_only.yml` from that checkout. Telegram reports concurrent `getUpdates` consumers. These processes are consistent with the conflict; token identity and remote consumers were not inspected. Assign a single command-polling owner per Telegram bot token, or separate tokens/configuration. No process was stopped.

The inspected checkout is commit `2c7f5ee21`, branch `feat/andrzej_ok_rsi_30d_config_values`, not the newer integrated master repair. Preserve that checkout and its local configuration when preparing any subsequently authorized deployment.

Read-only inspection of `data/ok_rsi.sqlite` found 1,551 terminated and 32 unfinished executor rows. All unfinished rows have historical `rsi-v5-...` controller IDs, not current `ok-rsi-...` IDs. Those identifiers alone do not prove that positions/orders are resolved. The startup log proves that risk state was initially missing. The database now has zero peaks and `stopped=true`, matching the defective next-tick persistence path. Do not reinterpret that synthesized record as reconciled account risk history.

OKX public/private subscriptions and all five order books initialized in the supplied log. One later price request timed out. The log does not establish an endpoint or network root cause. The history-candle throttle message reports local warmup throttling, not an exchange rejection.

## Repair and validation

A restoration failure now latches a separate reconciliation error. Persistence preserves the original missing or invalid record, and admission rejects creates before further risk processing. Status displays `EXECUTION BLOCKED` for reconciliation failures and stopped strategies, including while connectors are unready.

Two new temporary-SQLite regression cases (missing and invalid state) failed before the patch and passed afterward. They exercise actual strategy construction, the next tick, queued admission, retained database evidence, and blocked status with network initialization disabled.

Command: `/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/test_ok_rsi_restart_safety.py test/hummingbot/strategy/test_strategy_v2_base.py -q --basetemp=/tmp/ok-rsi-restart-state-20260908`

Result: 49 passed, one pre-existing fixture event-loop deprecation warning. Direct flake8 passed. Direct isort was unavailable in the native environment; the repository pre-commit environment provided equivalent validation. All applicable pre-commit hooks passed, including isort, Flake8, autopep8, secret checks, and whitespace checks. `git diff --check` passed. An initial command named a nonexistent test file and collected no tests; the corrected command above is authoritative.

## Recovery boundary and rollback

The running processes and database were not changed. This patch prevents future evidence overwrite; it cannot reconstruct peaks already lost. Before any authorized restart/migration: retain a consistent backup; reconcile outstanding orders/fills, inventory and fees with OKX; establish defensible risk peaks and controller ownership; resolve the 32 historical unfinished rows; and retain a single Telegram polling owner. Do not delete rows, reset peaks, flip `stopped`, or use an empty database to bypass the guard. Local ledger inspection cannot supply exchange truth.

One-command local diagnosis from a patched checkout:

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python scripts/diagnostics/ok_rsi_recovery_status.py /Users/s1kor/dev/trading/rsibot/hummingbot/data/ok_rsi.sqlite --config-file ok_rsi.yml
```

The fix remains on isolated branch `feat/andrzej_ok_rsi_restart_diagnostics`. Rollback is removal/reversion of only this branch's scoped script/test/report changes while preserving runtime state; no runtime rollback is needed because this patch was not deployed.
