# PR52 review at 60e73c371 — REQUEST CHANGES

Date: 2026-09-17. Repository: `s1korrrr/hummingbot`.
Reviewed product commit: `60e73c371d8cb6abc659018bd42b3f3880f209c7`.
Comparison parent: `4b46bd97526b660e9fe31374b731806f361a7083`.
PR base: `4a4372bcde4990355421a42df3b8ce6107299abb` (`master`).

This is a source/contract audit, not native runtime or deployment approval. The five-file follow-up was inspected, with targeted tracing into the native executor, recorder, connector readiness and market-data provider. PR #50 remains the old independent `9334c20c3` branch; #52 is the consolidation candidate. No merge, deployment, service restart, root pin update, trade, or economic backtest is part of this audit.

## Verdict

The newest commit corrects the explicit missing-status, missing-health, false-readiness and first-repair timing examples. Moving held-transfer lookup from bag quantity to native recorder identity is also the right direction. However, the implementation still fails the accounting contract at three boundaries. Do not call all entry permission/capital provenance findings closed.

Fresh isolated selection: **41 tests: 32 passed, 9 failed, zero errors/skips**. The nine failures are grouped below, not nine observed production incidents. No production-code change was made by this review. The published follow-up is documentation only.

## Positive controls

- Missing controller status and missing data-health evidence block a replacement.
- Explicit connector/provider `ready=False` prevents submission and preserves an otherwise valid bounded intent.
- Actual initial submission at t=0 anchors the shared cooldown: first repair waits at t=1 and submits at t=30 with a 30-second cooldown. Intent creation time does not reset.
- A matching recorded executor identity continues to be recognized after a held bag shrinks from 0.4 to 0.3 or zero. A different executor's row is not accepted.
- Position-executor cumulative gross 2 against authorization 1 becomes unavailable.
- The 22 selected earlier entry-boundary controls continue to pass.

These are source-level controls; this review did not rerun the author's native suites or prove all valid runtime paths covered.

## R1 — P1: terminal child releases the still-authorized parent intent

`controllers/directional_trading/hl_rsi.py`, `_executor_acquisition_remainder_reconciled`, especially the unconditional acceptance of a native `CANCELED` state (lines 1562–1563 in the reviewed commit).

The exact down-only executor callback produces `reconciliation_pending` for a canceled child eligible for post-only repair or bound downward repricing. The parent stop latch remains unset. The controller nevertheless treats the child's terminal state as evidence that the parent's acquisition remainder is reconciled and returns zero outstanding commitment.

Reproduction: authorization 1, acquired 0, child CANCELED, replacement bound, parent still permitted. Both source 31 post-only repair and source 1 bound local cancellation return outstanding **0**, where the existing intent still authorizes **1**. At price 100, this is a 100-quote undercount in this component's commitment calculation. The test composes the actual executor method with selected controller methods; it does not observe a live over-allocation.

Required correction: reserve the parent intent continuously across cancel acknowledgement, cumulative-fill reconciliation, cooldown and replacement submission. A terminal child is not a terminal parent. Release only the resolved remainder after acquisition authority has ended, or account explicitly for the remaining authorized commitment. Do not mint another parent admission, overlap child orders, or reset its lifetime.

Required native regression: cancel -> terminal child -> reconciliation pending/completed -> cooldown -> replacement, with controller/orchestrator capital checks between each event and revocation during the wait.

## R2 — P1: invalid or conflicting cumulative fills still become usable quantities

`hl_rsi.py`, `_executor_outstanding_base`, lines 1641–1669.

The new greater-than-authorization guard fixes one case, but malformed present fields still collapse into the same `None` used for absence. The helper also does not compare two native gross-fill receipts that are supposed to describe the same aggregate.

Observed isolated results:

| Input | Actual outstanding | Expected contract |
|---|---:|---|
| Authorized 1, net 0.4, cumulative gross `NaN` | 0.6 | Unavailable |
| Same, gross `Infinity` | 0.6 | Unavailable |
| Same, gross `broken` | 0.6 | Unavailable |
| Native receipt gross 0.4, acquisition cumulative gross 0.9 | 0.1 | Conflicting evidence; unavailable |
| OrderExecutor executed 2, authorized 1 | 0 | Unavailable |

The last case is a shared-helper branch, not evidence that the current non-episode OK-RSI configuration submits BUY OrderExecutors. Retain it as shared-code regression coverage.

Required correction: distinguish absent, invalid and valid measures; validate every supplied cumulative quantity before arithmetic; compare gross-to-gross receipts; reconcile gross/net through actual fees. Do not require gross quantity to equal net acquired inventory, and do not silently use the maximum to hide conflicting evidence. Both `execution_receipt.entry_gross_base` and down-only `entry_acquisition.cumulative_filled_base` are native aggregates of entry orders, so a discrepancy between those two is not explained by base fees.

Required native regression: build actual executor snapshots from fills with quote/base fees, then inject malformed and contradictory receipt fields. Validate admission refusal as well as the helper output. Include over-authorized OrderExecutor fills without changing that owner's accounting model.

## R3 — P1 evidence gate: one matching ledger row does not prove complete transfer

`hl_rsi.py`, `_executor_hold_released_to_bag`, especially its first matching-row `return True`.

Identity lookup replaces the previous quantity/age heuristic, but succeeds when **any** row matches. It does not establish that all positive-fill held order receipts, their cumulative amounts, or their fees were persisted consistently and incorporated into the owner projection.

Adversarial recorder-input probes:

- Executor hands off buy-1=0.1 and buy-2=0.3; only a matching buy-1=0.1 row is supplied. Whole-executor transfer is accepted.
- Executor hands off buy-1=0.4; matching row has old cumulative amount 0.1. Whole-executor transfer is accepted.

These deliberately incomplete/stale fixtures demonstrate a helper acceptance gap, not that the live recorder has produced these records. The recorder writes a batch transaction, but its writer also skips some malformed/ineligible order records. A transaction does not by itself establish correspondence to the entire sealed executor receipt set.

Required correction: make the existing native owner establish complete, identity-bound transfer/consumption evidence, and project that into capital accounting. Cover all required nonzero-fill order identities and cumulative quantities/fee payloads, including partial closes and dust. Avoid treating zero-fill canceled children as holdings. Verify persistence plus projection, not an arbitrary row-presence test. Keep genuine historical transfers recognized after partial/full disposal and after older executors disappear from the report.

Required native regression: recorder/orchestrator handoff with partial or stale ledger data, missing order records, persistence/application failure, disposal and truncated executor history. No second ledger or new service is needed.

## Additional validation gaps, not new observed incidents

1. Connector/provider readiness checks now reject explicit false. They are not a demonstrated per-pair execution-book freshness contract. `ExchangePyBase.ready` aggregates initialization flags; `MarketDataProvider.get_market_data_health` exposes ticker/candle observations, not the required execution-book sequence/receive-age proof. Exercise disconnection and stale-but-initialized books with the actual connector. Do not equate an unchanged price with a stale book, or reuse ticker freshness as order-book freshness.
2. `_hold_ledger_rows()` performs a synchronous full controller-history query. Capital helper calls can repeat it for multiple retained executors. Prefer one consistent owner-derived snapshot per valuation/admission pass after correctness is established. No production latency was measured here; do not introduce a long-lived stale authorization cache.
3. The native suites and actual connector/recorder transitions were not run here. Historical split-pass counts are not current candidate validation.

## Attached exit-scope research brief

The user-supplied `rsibot_ok_rsi_exit_scope_research_brief_2026-09-10.md` is a research-intake/design handoff. It explicitly disclaims controller implementation, economic execution, independent approval and Research OS persistence. Its historical cases, source versions and result counts are reported prior evidence, not current measurements reproduced by this review.

Its main distinction remains unresolved by this entry-maintenance PR: native executor PnL at decision time, requested-slice prospective liquidation surplus, and whole ACTIVE/CLOSING/HELD inventory surplus are different quantities. The frozen gate requires strictly positive requested and book surplus. Do not replace that historical convention or infer an approved current portfolio-exit rule from a profitable local lot.

No new portfolio-profit guard, no-loss exception, threshold change, historical backtest rerun or research-store write was performed. The requested read-only all-event reconciliation still needs the sealed local artifacts and independent review. Physical inventory/cash conservation and exactly-once fee treatment remain required irrespective of labels.

## Verification scope and reproduction

The complete executor blob is `f05d4f1a407ac8e761c0adf0bb5948add2cf4023` and reducer blob `996b53ecee606b6990c3b835d1e86f92ed48fe56`; both local byte sets match those Git blob hashes. The controller probes execute selected copied methods at blob `5395513af0041a7156b00e9384eb9aed494172c3`, not a fully imported or whole-file hash-verified controller. Runtime, connector, recorder and enum boundaries are substitutes. Test fixtures intentionally model faulty input where specified.

Run from the evidence-package root:

```sh
PR52_SOURCE=source python -m pytest probes/test_entry_boundary.py probes/test_preserved.py probes/test_latest_contracts.py -q --tb=short --junitxml=results/recheck.xml
```

Expected at reviewed product source: exit 1, **32 passed / 9 failed**. The failures are retained intentionally; they are not fixed implementation tests. The package contains `results/final_60e73c3.xml`, output, summary and source-integrity records.

Hosted workflow observations for the reviewed product commit:

- CI run `35212282097`: preliminary job `105172383857` failed with no step records; native build/stable-test job `105172395953` skipped.
- Image run `35212282180`: `verify_pr` job `105172384370` failed with no step records; release jobs skipped.

No executed native/build validation is established by those records. The previously reported billing restriction is not independently diagnosed here. No retries, purchases or account changes were requested in this audit.

## Approval and implementation sequence

Fix R1 and R2 with native receipt/admission regressions, then close R3 through the existing native recorder/orchestrator contract. Verify quote freshness in the real connector and run the union of the affected native suites on the exact final candidate, plus a source-bound build and non-live startup/recovery smoke. Obtain genuinely separate review after implementation. Keep #50/#51 history; do not merge them as alternatives to this consolidation. Deployment and capital authorization remain separate.

## Source locators

All code findings above are pinned to the reviewed commit:

- `controllers/directional_trading/hl_rsi.py`
- `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`
- `hummingbot/connector/markets_recorder.py`
- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `hummingbot/connector/exchange_py_base.py`
- `hummingbot/data_feed/market_data_provider.py`

Official documentation checked: https://www.okx.com/docs-v5/en/ and https://www.okx.com/docs-v5/trick_en/ . Order submission/cancellation request acceptance is not terminal fill reconciliation; maintain exact client/venue identities and cumulative fills. https://hummingbot.org/strategies/v2-strategies/executors/positionexecutor/ describes the architecture, not exact semantics of this fork.
