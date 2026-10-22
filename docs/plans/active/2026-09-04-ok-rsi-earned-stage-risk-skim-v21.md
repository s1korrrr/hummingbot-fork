# OK RSI earned-stage risk skim v21

## Objective and completion bar

Test a disabled-by-default native episode option that preserves most of V20's
next-stage confirmation while releasing a small, fixed part of each newly
earned distribution chunk to bound marked exposure. Complete the native slice
when the pure transition, public configuration, runtime status and receipts,
checkpoint identity, retry behavior, and both `buy_sell` and `sell_only` modes
are covered and the exact source can be replayed by Quants Lab.

This iteration changes sell timing only. It must not change a buy opportunity,
buy amount, entry price, budget, position cap, or distribution threshold.

## Frozen design and reversal condition

V20 improved modeled PnL by 15.3704526485555 USDC over V19 with the exact V19
buy path, but BNB maximum marked exposure reached 1.0177380363 times V19 and
failed the frozen 1.0025 limit. The maximum occurred immediately before the
stage-1 chunk was released at 7 ATR.

V21 keeps the existing 4/7/10-ATR thresholds and 80/60/40 percent remaining
fractions. When a stage is newly earned, it releases 10 percent of that stage's
scheduled chunk and leaves 90 percent awaiting the next threshold:

- at 4 ATR, target 98 percent instead of V19's 80 or V20's 100;
- at 7 ATR, confirm the rest of stage 1 and skim stage 2, target 78 percent;
- at 10 ATR, confirm the rest of stage 2 and skim stage 3, target 58 percent;
- a protected trailing breach still releases to the fully earned V19 stage
  target; risk, markup-loss, structural, thin-scout, recovery, and bounded
  exit rails keep their existing behavior.

The 10 percent is a fixed participation skim, not an asset-specific fit. A
ledger interpolation between V19 and V20 estimates BTC -0.3481684061 USDC,
ETH 0, BNB +14.1815757898 USDC, and aggregate +13.8334073837 USDC versus V19.
This estimate cannot reproduce native target revisions, fills, or equity-trail
feedback, so only the native replay can accept the candidate.

Reject V21 if it fails any unchanged V20 development gate: exact V19 buys,
maximum marked exposure, drawdown tolerance, risk/reconciliation invariants,
causal receipts, changed-sell attribution, at least 10 USDC aggregate PnL gain,
or the -1 USDC per-asset floor. A pass remains local development evidence and
does not grant paper, canary, live, order, or capital authority.

## Scope and authority

- Owner: native Hummingbot `ok_rsi_core` configuration, episode reducer,
  runtime adapter, focused tests, and plan pointers.
- Authority: research-only.
- Forbidden: live YAML changes, exchange access, orders, deployment, paper or
  canary activation, push, pull request, and any capital action.
- Rollback: leave the option false or revert the isolated V21 commit. V19 stays
  the accepted local comparator unless the exact Quants gate passes.

## Validation

1. Add RED tests for 98/78/58 percent targets, dependency validation, exact
   default-off identity, policy and receipt identity, protected reversal,
   retries, final-stage bounded behavior, and sell-only buy inertia.
2. Implement the pure target calculation and thin runtime mapping without
   copying controller logic into Quants Lab.
3. Run focused native tests, Ruff, PyCompile, and Git shadow checks.
4. Commit exact native paths and bind that commit into one continuous
   May-August BTC/ETH/BNB replay with the frozen V20 gate unchanged.

## Progress

- [x] Attribute the V20 exposure failure and freeze the V21 mechanism.
- [x] Add failing native behavior, configuration, identity, and receipt tests.
- [x] Implement the shared pure transition and runtime mapping.
- [x] Run focused native verification and commit exact paths.
- [ ] Hand the exact native source identity to Quants Lab.

## Native evidence

The RED selection failed because the public and pure risk-skim fields did not
exist. The completed implementation derives each 98/78/58 percent target from
the configured stage ladder and fixed 0.1 chunk fraction, emits policy
`EARNED_STAGE_RISK_SKIM_V10`, preserves exact default-off identity, and keeps
protected and forced exits intact. The focused new selection passed 17 tests.
The complete OK RSI native selection passed 327 tests and 19 subtests. Ruff,
PyCompile, and `git diff --check` passed on the touched paths.
