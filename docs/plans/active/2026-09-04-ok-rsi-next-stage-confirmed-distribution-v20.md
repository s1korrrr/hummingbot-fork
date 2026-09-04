# OK RSI next-stage confirmed distribution v20

## Objective and completion bar

Implement a disabled-by-default native episode option that waits for the next
configured distribution threshold before releasing the prior distribution
chunk. Complete the native slice when the pure transition, public
configuration, runtime status and receipts, checkpoint identity, retry
behavior, and both `buy_sell` and `sell_only` modes are covered and the exact
controller source can be replayed by Quants Lab.

This is one research iteration toward better buy and sell placement. It does
not treat lower buy frequency as an improvement and it does not change any buy
opportunity, buy amount, entry price, budget, or capital rule.

## Frozen design

V19 sells each configured distribution chunk as soon as its threshold is
earned. V20 adds one causal confirmation step using only the existing ladder:

- crossing 4 ATR earns stage 1 but retains the distribution-start inventory;
- crossing 7 ATR releases the stage-1 chunk to the configured 80 percent
  target;
- crossing 10 ATR releases the stage-2 chunk to the configured 60 percent
  target;
- the existing protected trailing breach releases to the currently earned V19
  stage target, so an armed gain is still realized on reversal;
- markup loss, hourly recovery loss, risk override, structural invalidation,
  thin-scout release, deep-recovery rotation, and the bounded runner exit keep
  their current fail-closed behavior.

The rule introduces no new fitted threshold or fraction. The public flag is
`episode_next_stage_confirmation`, it requires the V19 stage-preserving runner,
it changes checkpoint identity only while enabled, and it emits policy
`NEXT_STAGE_CONFIRMED_DISTRIBUTION_V9` with earned-stage and released-stage
receipts. Disabled behavior and identity remain exact.

## Scope and authority

- Owner: native Hummingbot `ok_rsi_core` configuration, episode reducer,
  runtime adapter, focused tests, and plan pointers.
- Authority: research-only. No paper, canary, live, exchange, deployment,
  push, pull request, order, or capital action.
- Forbidden scope: entry rules, distribution thresholds/fractions, fees,
  position limits, execution adapter semantics, or live configuration.
- Rollback: keep the option false or revert the isolated native commit.

## Validation

1. Add RED tests for the stage-1 arm, stage-2 and stage-3 confirmed releases,
   protected-trail release of the currently earned stage, 10-ATR bounded
   behavior, risk and markup release, request retry receipts, and exact
   default-off behavior.
2. Validate configuration dependency, identity persistence, public mapping,
   runtime status, and policy receipts.
3. Exercise both `buy_sell` and `sell_only`; `sell_only` must never create a
   buy request.
4. Run the focused OK RSI native suite, Ruff, PyCompile, `git diff --check`, and
   a shadow status/diff review.
5. Commit only the intended native paths and bind that exact commit in Quants
   Lab.

## Progress

- [x] Diagnose V19 sell episodes and freeze the stage-confirmation rule.
- [x] Add failing reducer, configuration, identity, and runtime tests.
- [x] Implement the shared pure transition and runtime receipts.
- [x] Run focused native validation and commit exact paths.
- [ ] Hand the exact source identity to Quants Lab.

## Native evidence

The RED selection produced 14 expected failures because the public and pure
configuration, policy identity, stage targets, and status fields did not yet
exist. After implementation, 18 targeted cases passed. The complete focused
OK RSI suite completed with `305 passed, 19 subtests passed`. Ruff, PyCompile,
and `git diff --check` passed on the touched native and test paths.
