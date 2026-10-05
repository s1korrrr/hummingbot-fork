# Modular executor and Meridian dust-proof root causes — 5 October 2026

Live V3 (`meridian_v3_unified`) cancelled half of its BUY entries within 3-40 s
of placement (`modular_entry_permission_revoked`; 12 of 25 in 72 hours), where
the same entries had rested for their full 300 s before 4 October.

| Cause | Change |
| --- | --- |
| A closed tranche with sub-lot dust stays in the orchestrator's active executors (`closed_executors_buffer`). Its `execution_order_snapshots` carry `cumulative_fee_paid_base/quote`, which `InFlightOrder.to_json` re-values through the rate oracle on every read. The Meridian terminal-dust proof hashed them, so every oracle refresh (~30 s) made the live proof differ from the recorder until the next 10 s sync. Every grant read raised `current executor changed recorded terminal dust proof`, all five symbols held (`portfolio_grant_hold`), and each resting entry re-checking its permission was revoked. It began when the first dust tranche closed inside a running process (2026-10-04 09:20:53 UTC). | `meridian_v3_unified_core/portfolio.py` compares order snapshots without those two oracle valuations; `order_fills` still binds each fee's token and amount. |
| An entry decision reserved by the ok_rsi gate is released only inside the owner's validator. Creates the engine refuses before that validator (risk/daily/operator refusal, resume hold, unavailable controller, an orchestrator create failure such as `Insufficient unreserved`) left it reserved, so that pair never bought again until a restart (observed 2026-09-20). | The engine reports such creates through an optional `create_action_refused(action)`; `native_composition.py` releases a BUY's decision there (re-arms only when no executor row exists). Frozen V1 `OKRSIController` does not implement it and is unchanged; `rsi_v5` has no release target, so the hook is a no-op there. |
| `ModularPositionExecutor.control_open_order` ended a revoked entry with the internal `_end_entry_acquisition`, so an intent revoked before submission waited forever in `reconciliation_required` and pinned its portfolio grant; a terminal entry was relabelled as revoked. | It calls the native `stop_entry_acquisition`, which ends an unsent intent as `EXPIRED`/`ended_no_fill` and leaves a terminal entry untouched. |

Evidence: 1 s polling of the native performance packet (holds at the oracle
refresh cadence, 5-8 s each), a 1 s series of recorder copies (only the closed
dust tranches' snapshot fee valuations change, at the oracle refresh), and a
real-object reproduction now kept as
`test/meridian_v3/test_terminal_dust_oracle_refresh.py` (fails before, passes after).

## Source identity

The modular closure deliberately changes from
`156119946b518b04a0116a6db993057e1ff96dee59e8fa99c973bab9c7889a7d` to
`b6a9dd1193d42fdb42637b1a8ea1bd7222bddf9bb40b820c54276133cd0c8321`.
Only `modular_spot_core/execution.py` and `modular_spot_core/native_composition.py`
change; no path is added or removed. V2 (`rsi_modular`) runs
`DownOnlyMakerPositionExecutor`, not `ModularPositionExecutor`, so the first change
does not reach it; the second adds only the refused-create release. Meridian's own
`portfolio.py` change moves the V3 source manifest; V3 adoption therefore needs
a re-sealed release and its identity migration under the existing procedure.
Historical receipts, checkpoints and deployed bindings keep their identities.
Nothing here is deployed.
