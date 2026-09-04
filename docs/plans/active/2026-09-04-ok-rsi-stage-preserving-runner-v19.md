# OK RSI stage-preserving runner v19

## Objective and completion bar

Implement a disabled-by-default native episode option that retains the
inventory fraction already earned by the configured distribution stage when a
protected trail breach occurs. Complete the native slice when pure transitions,
public configuration, runtime status and receipts, checkpoint identity, and
both `buy_sell` and `sell_only` modes are covered and the exact commit can be
replayed by Quants Lab.

## Frozen design

V18 always jumped to the last configured remaining fraction. V19 instead uses
the existing distribution ladder and adds no threshold or fraction: retain the
distribution-start base below the first threshold, 80 percent from 4 through
less than 7 ATR, 60 percent from 7 through less than 10 ATR, and fully exit at
the existing 10 ATR V18 boundary. Markup loss, hourly recovery loss, risk
override, and structural invalidation still exit the retained inventory.

The new `episode_stage_preserving_runner` flag requires the bounded markup
runner, changes checkpoint identity only while enabled, and emits policy
`STAGE_PRESERVING_RUNNER_V8` plus causal target-stage status. Disabled behavior
and identity stay exact.

## Scope, authority, and validation

- Owner: native Hummingbot `ok_rsi_core` config, episode reducer, runtime
  adapter, focused tests, and plan pointers.
- Authority: research-only. No paper/canary/live activation, exchange action,
  deployment, push, pull request, order, or capital action.
- Validate target stages, boundary exit, markup/risk release, retry receipts,
  invalid configuration, restart identity, default-off parity, and sell-only
  buy inertia; then run the focused OK RSI suite, Ruff, and Git shadow diff.
- Rollback: keep the option false or revert the isolated native commit.

## Progress

- [x] Freeze the stage-derived rule before implementation.
- [x] Add failing reducer, configuration, identity, and runtime tests.
- [x] Implement the shared pure transition and runtime receipts.
- [x] Run focused native validation and commit exact paths.
- [ ] Hand the exact source identity to Quants Lab.

## Native evidence

The RED selection failed in 14 focused cases because the public and pure
configuration, policy identity, stage target, and runtime status did not exist.
The implementation derives every retained target from the configured ladder,
keeps V18 behavior exact while disabled, and publishes stage index, earned
threshold, fraction, target base, and the distinct V8 policy identity.

The focused native suite completed with `290 passed, 19 subtests passed` across
the episode reducer, context, runtime, execution, factory, entry pricing,
controller, and wallet tests. Ruff, PyCompile, and `git diff --check` passed on
all touched Python paths.
