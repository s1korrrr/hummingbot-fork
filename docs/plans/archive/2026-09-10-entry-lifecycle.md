# Preserve entry fills and stop automatic entry replacement

## Goal

Implement the operator-approved entry policy: expiry/cancellation stops further acquisition, never liquidates acquired spot inventory; a canceled/rejected entry is not automatically replaced. Preserve precise inventory and provenance, avoid avoidable sell dust without overselling. Completion requires native event-sequence and controller restart proofs, exact accounting checks, independent review and a reviewable local diff.

## Current State

Owner: Hummingbot. Source HEAD 821753645c75713938503bdcc14eb022fbc09006 with pre-existing dirty runtime/controller work. Isolated branch feat/andrzej_entry_lifecycle at .worktrees/entry-lifecycle-20260910 carries an exact baseline of relevant source changes and native compiled-module symlinks. Original checkout must remain unchanged. Baseline receipts: /Users/s1kor/.codex/visualizations/2026/09/10/01a08cc2-ebca-7213-8d19-94112869af50/entry-lifecycle.

Current 30-second entry age expires partial orders into generic shutdown and MARKET liquidation. Canceled entry events clear the tracked order and permit retry. Completed fills and sub-lot residuals can be obscured by POSITION_HOLD scalar fields. Order/TradeFill six-decimal storage truncates small BTC quantities.

## Target State

- Authority: local live-adjacent engineering and non-destructive validation only. No exchange writes, orders, credentials, service restart, deployment, push, PR, or activation.
- Fixed 30-second deadline remains; it is measured from entry order creation and affects unfilled remainder, not held inventory.
- Cancel/expiry/rejection ends entry acquisition; reconcile uncertain orders with exposure still reserved; retain actual fills under trailing/risk management.
- No automatic replacement from executor or same controller signal. Require a fresh qualified decision after a genuine signal clear; preserve decision state through restart.
- Preserve original entry outcome and timestamps independently from exit/holding state.
- Retain exchange precision in persistence without reinterpreting existing database columns. Sell only position-owned available amount on venue quantity increments. Never round up to dollars or consume unrelated wallet inventory; true sub-lot dust remains exactly accounted.
- Preserve legitimate exit/risk behavior and other strategies unless explicitly opting into the new OK RSI entry policy.

## Risks and Failure Modes

Late fills after cancel; duplicate events; unavailable order state; lost exposure reservation; reboot rearming a consumed signal; repeated raw signal interpreted as fresh; taking unrelated base assets to round up; decimal schema incompatibility; restored trailing state loss; existing tests expecting old cancellation semantics.

## Milestones

### M1. Native entry lifecycle

Add failing event-sequence regressions, implement explicit entry acquisition/reconciliation state, prohibit automatic replacement and liquidation on expiry, retain filled management and provenance. Cover partial and late fills, zero fills, errors, uncertainty, and legitimate trailing exits.

### M2. Controller decision persistence

Integrate single-attempt policy in OK RSI construction and controller decision state. Test sustained signal, fresh signal clear/rearm, DCA constraints, queued creates and restart behavior.

### M3. Accounting and sell quantities

Add compatible exact-value persistence and true executed-volume evidence; validate exchange-lot bounded sells and exact remainder, including BTC precision and legacy DB reads. Do not migrate or mutate live data.

### M4. Integration and handoff

Run owner focused suites, native simulated event/restart smoke, independent review, diff checks and original baseline preservation checks. Document policy, expiry causes, residual limits, adoption requirements and rollback. Deliver task-only patch.

## Verification

Use /Users/s1kor/anaconda3/envs/hummingbot/bin/python with worktree cwd, pytest via owner environment. Focus on test/hummingbot/strategy_v2/executors/position_executor, test/test_ok_rsi*, recorder/model and orchestrator tests for changed boundaries. Exact commands and observed results appended below. Tests must not instantiate a live connector or use production DB/config state. Run git diff --check and artifact SHA checks.

## Decision Log

- 2026-09-10: User approved all proposed behavior. Keep 30 seconds initially, explicitly explain deadline. Dollar round-up is not feasible safely; maximize owned sale on lot increments and retain unavoidable residue. No live adoption authorized.
- 2026-09-10: Isolate work because original checkout has unrelated dirty runtime work and may be used by processes; preserve and snapshot instead of editing it.

## Progress Log

- M1–M3 complete. RED regressions reproduced cancellation replacement, expiry liquidation, exact BTC loss and late-fill ownership errors before implementation. New native InFlightOrder event sequences and local SQLite restart tests are green.
- M4 complete locally. Independent reviewer found four boundary defects in the first pass: late fills after a closed partial, mutable terminal receipts, episode exits being blocked, and subminimum filled positions staying active. All were corrected and retained as regressions. Second review found no remaining actionable findings in the corrected scope; independent focused run: 53 passed.
- Owner integration run: 190 passed, 38 subtests, 29.78 seconds. Boundary run: 78 passed, 9.03 seconds. These are simulated native connector/event and database checks, not exchange or deployed-runtime evidence.
- Ruff passed for the task Python files; git diff --check passed. Task patch and baseline-preservation receipt are in the artifact directory. No original source, runtime configuration or live database was changed by this task.

## Final validation commands

Owner environment: `/Users/s1kor/anaconda3/envs/hummingbot/bin/python`; cwd is the isolated worktree above.

```sh
python -m pytest test/hummingbot/strategy_v2/executors/position_executor test/hummingbot/strategy_v2/executors/test_held_inventory_accounting.py test/hummingbot/model/test_exact_order_values.py test/test_ok_rsi_entry_decisions.py test/test_ok_rsi_controller.py test/hummingbot/connector/test_markets_recorder.py test/hummingbot/model/test_trade_fill.py -q --disable-warnings --basetemp /tmp/entry-lifecycle-final-pytest-1
python -m pytest test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/test_okx_native_reconciliation.py test/hummingbot/connector/exchange/okx/test_okx_exchange.py -q --disable-warnings --basetemp /tmp/entry-lifecycle-boundary-pytest-1
```

## Remaining boundaries

- Restart preserves consumed-entry decisions and existing `recovery_required` blocking. Generic restart does not automatically restore management of existing live positions. The subsequent explicit, stopped-owner trailing-only handover is documented in `docs/controllers/planned-position-resume.md`; it does not replay an entry decision or provide generic crash recovery.
- Cold first use requires a completed clear candle before a subsequent qualified BUY. A crash after durable dispatch can miss an entry; no replacement is inferred.
- Episode BUY admission is blocked until that separate path supports durable authorization; episode reconciliation, stops and SELL management remain available.
- Exact receipts apply to new recorded events. Historical six-decimal data remains precision-limited; unrelated compatibility readers are not globally migrated.
- Native lot/minimum limits can leave an unsellable residual. Preserve it exactly; do not use unrelated wallet assets or round a sell above owned available quantity.
- Runtime adoption requires separately authorized reconciliation and deployment. No paper, canary, live or capital authority is granted.

## Rollback / Recovery

Original source and runtime remain untouched. Revert only task deltas from the isolated baseline if required. No live DB migration occurs. Future adoption must reconcile existing orders/inventory first and preserve exact restart state; retain original runtime as rollback until separately authorized deployment proof.
