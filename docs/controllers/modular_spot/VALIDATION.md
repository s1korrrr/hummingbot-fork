# Validation receipt — native modular spot candidate

> Historical validation of the preserved PR #55 working candidate. The source, authority, unpublished status and output paths below describe that earlier run. For the fresh standalone publication checks and PR #55/#57 split, see [PUBLICATION_RECEIPT.md](PUBLICATION_RECEIPT.md). Generated historical evidence remains in the original checkout and is not part of this publication.

Date: 2026-09-19. Verdict: local implementation validation and bounded public
market-data path PASS after repairs. Strategy parity and operational activation HOLD.

## Source and authority

- Owner checkout: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-modular-spot-pr55`.
- Task branch: `feat/andrzej_isolated_modular_spot`.
- Local HEAD: `808919077af8b0fa88736a0690a725def31fff44`, plus preserved uncommitted
  M0-M7 product and this validation's repairs. Product is not published.
- Remote PR #55 at inspection: `2f9e4d0168df64d4c99a3d6e3055945a36ad572e`, OPEN draft.
  Three newer commits add documentation only: flash-crash recovery and mean-reversion
  context specifications/plan. Their additions were preserved in this local handoff;
  they are not implemented by this task. No branch switch, reset, rebase or publication.
- Native interpreter: `/Users/s1kor/anaconda3/envs/hummingbot/bin/python`, Python
  3.13.13. `PYTHONPATH` binds native imports to this complete worktree.
- Final modular Python package identity:
  `c0365c8a015e5e8a76cb455469b5c206d0490c0885a304354bf7e93a76869921`.
  This is SHA-256 of the sorted compact JSON path-to-file-SHA mapping in
  historical final-source.json (`evidence/2026-09-19/final-source.json` in the original checkout), not a Git commit or
  the historical M8 patch digest.
- Authority: repository-process-only; temporary credentials-free public market data
  and loopback readers. No trading, deployment, owner enrollment, economic backtest,
  account calls, inventory migration, component-pin change, commit, push or merge.

All 13 live container IDs, image IDs and start times were identical before/after.
The root and separate nested `hummingbot` Git statuses were also unchanged.
See historical stack-preservation.json (`evidence/2026-09-19/stack-preservation.json` in the original checkout).
This establishes non-interference; it is not an exchange/account reconciliation.

## Native reuse and product meaning

The modular controller uses native `ControllerBase`, `CandlesFactory`,
`CreateExecutorAction`, `PositionExecutor`, `OrderExecutor`, connectors,
`ExecutorOrchestrator` and `MarketsRecorder`. Local specializations constrain
admission and profit floors; native Hummingbot retains order/fill/cancel/lifecycle
ownership. No shared engine file was changed.

`BudgetStore` reserves proposed commitments in native Metadata for one recorder
and strategy configuration. It is a small admission guard, not a replacement
wallet or accounting engine. Independent bots using separate databases are outside
that coordination scope. This task does not need a new distributed allocator;
any later activation needs an explicitly admitted owner/account topology.

The current strategy policies are not OK RSI / RSI V5 parity:

| Boundary | Current modular baseline | Compatibility still required |
|---|---|---|
| Signals | RSI14 rebound / breakout | Existing scored reversal, indicator/context and confirmation behavior |
| Sizing/cooldown | Assigned tranche fractions, fixed cooldown | Exact selected legacy sizing, slots, adaptive cooldown and DCA rules |
| Entry | LIMIT, single attempt, configured lease | Selected legacy LIMIT_MAKER/entry-policy/expiry semantics |
| Exits | Fixed trailing/profit floor; separate floor-triggered sell-only liquidation | Legacy adaptive trailing, signal/fractional reductions and retained-inventory behavior |

These are source comparisons, not economic results. The amended SPEC/PLAN makes
reuse and parity the next implementation gate before strategy expansion.

## Reproduced defects and repairs

1. Sell-only pricing omitted acquisition fees: a native lot with gross cost 100
   and quote fees 5 could previously sell at 102. Complete native fill/fee evidence
   is now validated, bound to the handoff and included in the floor.
2. Stale persisted sale progress could hide current native fills and propose a
   duplicate reduction. Current native reports now take precedence; quantity or
   terminal-state regressions hold. Pending dispatch identities remain committed.
3. Unsupported base/flat exit fees could violate the quantity/profit bound.
   Unsupported forms now hold at proposal and immediately before native placement.
4. Sell-only handoffs accepted a different configured account. Account and connector
   identities are now checked at admission.
5. A missing required observation did not stop sell-only admission. It now holds.
6. Native retry evaluation occurs after `control_order`; the local reduce executor
   now checks its zero-retry ceiling before a repeat submission.
7. One HTTP 503 dataset reset healthy candle peers. Only the unavailable dataset
   is invalidated now. Recovery seeds a new watermark and cannot replay that bar.
8. HTTP responses could supply a different subscribed dataset; the requested key
   is now enforced. Response size is bounded during reading, before full buffering.
9. Lifecycle test tasks were not drained. Async fixture cleanup removes the three
   baseline coroutine warnings. Scoped lint/import-format defects were repaired.

Eight native sell-only regressions and three transport regressions were added.
The initial defect regressions failed before repairs; see the candle red/green
logs and native repair review. Unsupported native acquisition base/third-token
fees, positive flat exit fees and base-denominated exit percentages deliberately
remain holds. No invented fee conversion or market fallback was added.

The implemented sell-only behavior is bounded profit-floor liquidation. Its
`sell_signal=True` is not evidence of legacy RSI sell signals or trailing high-water
management. It is documented as that explicit baseline policy.

## Fresh final verification

Executed from this checkout after formatting:

```bash
PYTHONPATH="$PWD" /Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest -q \
  test/modular_spot \
  test/hummingbot/strategy_v2/executors/position_executor \
  test/hummingbot/strategy_v2/executors/order_executor \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/hummingbot/data_feed/candles_feed/okx_spot_candles \
  test/hummingbot/data_feed/candles_feed/test_candles_base.py \
  --basetemp=/tmp/pr55-post-hooks-native-20260919 \
  --junitxml=docs/controllers/modular_spot/evidence/2026-09-19/final-native.xml
```

**446 passed + 10 subtests passed, zero failures, zero skips, zero warnings:**
241 modular tests, 172 native executor/orchestrator tests plus 10 subtests, and
33 native OKX/base candle tests. historical Log (`evidence/2026-09-19/final-native.log` in the original checkout) ·
historical JUnit (`evidence/2026-09-19/final-native.xml` in the original checkout).

```bash
pre-commit run --files controllers/generic/modular_spot.py \
  controllers/generic/modular_spot_core/*.py test/modular_spot/*.py \
  docs/controllers/modular_spot/public_candle_smoke.py
git diff --check
```

All applicable hooks passed: whitespace, end-of-file, file size, flake8, private-key
checks, autopep8 and isort. YAML and eslint hooks had no matching files.
The first hook pass formatted imports; the second passed with no further edits.
historical Hook log (`evidence/2026-09-19/pre-commit-final.log` in the original checkout).
The README YAML also validates against the real native configuration class with
execution disabled and an explicit budget/account example.

Independent source audit and post-format confirmation found no open Critical or
Important defects in the repair scope. See historical review.md (`evidence/2026-09-19/review.md` in the original checkout).
This is a bounded review, not a claim of exhaustive proof of every future program.

## Observed public WebSocket path

```bash
PYTHONPATH="$PWD" /Users/s1kor/anaconda3/envs/hummingbot/bin/python \
  docs/controllers/modular_spot/public_candle_smoke.py --seconds 240 \
  --output docs/controllers/modular_spot/evidence/2026-09-19/public-candle-smoke.json
```

The repaired data path passed in 140.43 seconds:

- two native OKX public 1m feeds, BTC-USDC and ETH-USDC;
- three successive exchange-confirmed WebSocket bars for each pair;
- one native REST history warmup request per feed;
- 90 finalized contiguous bars available to each of two DataPool readers;
- an initial snapshot warms the reader; only a new bar enables entry readiness;
- closing/reconnecting one reader left the other reader and publisher available;
- recovered reader waited for a subsequent confirmed bar;
- final candle ages approximately 2.15 seconds;
- both readers connected without errors; hub had no errors at completion;
- temporary readers/publisher shut down cleanly; no account connector was created.

Initial HTTP 503s represented normal startup warmup; HTTP 200/304 responses then
served current snapshots. Exchange collection is WebSocket; local delivery is
HTTP snapshot polling, as specified. No consumer-WebSocket rewrite is required.
historical Receipt (`evidence/2026-09-19/public-candle-smoke.json` in the original checkout) ·
historical Log (`evidence/2026-09-19/public-candle-smoke.log` in the original checkout).
The smoke loaded repaired code before final import sorting; the final native test
suite and independent review bind the formatted source bytes. This was a bounded
public-feed test with reader reconnect, not an endurance soak or a public-socket
failure-injection campaign.

## Gates and next work

| Gate | Status |
|---|---|
| Current modular/native tests and scoped hooks | PASS |
| Independent scoped repair review | PASS |
| Bounded public native WebSocket → local reader availability | PASS |
| Existing live stack preservation | PASS |
| OK RSI / RSI V5 strategy parity | NOT IMPLEMENTED / NOT PROVEN |
| Planned-resume specialization restore | HOLD; native path still bypasses local selector |
| Supported sell-only fee forms | Bounded; unsupported forms HOLD |
| Long public-feed soak, crash recovery with real state, alerts/image/owner binding | NOT RUN |
| Economic comparison and paper/canary/live activation | NOT RUN / NOT AUTHORIZED |
| Publishing the uncommitted candidate | NOT DONE |

Next: review/publish this concrete candidate when authorized; freeze one current
OK RSI configuration; reuse its decision functions in a named compatibility policy;
prove signal/action/amount/expiry/exit parity in isolation; then repeat for RSI V5.
Keep native execution and recorder ownership. Resolve the separate planned-resume
owner change and operational/research gates before any legacy-owner handoff.
Preserved context/crash strategy specifications remain later work.

## Historical evidence and recovery

The original 61-pass/one-skipped-module receipt and later M8 230-pass receipt remain
historical. Fresh baseline here reproduced 230 modular tests and 172 native tests
plus 10 subtests, zero skips, before repairs. M8's original `182` combines test cases
and subtests. The previous owner README/VALIDATION/PLAN had not caught up with that
work; they are reconciled in this handoff.

The pre-task uncommitted candidate and owner docs are preserved under root
`.audit/pr55-validation-20260919/before.tar.gz`, with per-file SHA-256 and Git patch.
For rollback inspect the task-only incremental patch, restore only intended files
from that archive (previously dirty files) or recorded HEAD (previously clean
files), and remove only this task's new files if desired. Do not reset the whole
worktree, erase research artifacts, or touch the live stack.
No running service requires restart for these local changes.
