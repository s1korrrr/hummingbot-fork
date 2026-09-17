# t1 — Context packet PR #50

Read-only. No edits, no git writes.

## Sources

- Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-down-only-postonly-repair`
- Head: `9334c20c3a09ffed92631b4fa1172285b776b116`
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`

## Files to inspect

- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `hummingbot/connector/exchange/okx/okx_exchange.py`
- `hummingbot/core/data_type/in_flight_order.py`
- `test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py`
- `test/test_ok_rsi_down_only_entry_execution.py`

## Write

`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t1-pr50-context.md`

Include:

1. Diffstat vs base
2. For each blocker A, B, C, G: current function names, line ranges, branch order, what the audit says is wrong
3. Existing tests that almost cover the bug and what they miss (especially JSON round-trip and cancel-source 32 vs 31)
4. `update_with_order_update` current assignment of `current_state` and `last_update_timestamp`
5. `to_json`/`from_json` fields vs `cancel_source`
6. `process_order_canceled_event` branch order (replacement vs cancel-source classification)
7. `control_open_order` wait/deadline/cooldown control flow
8. Suggested smallest patch approach, no implementation
