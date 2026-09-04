# OK RSI recovery-strength runner v16: native contract

## Objective and completion bar

Add an opt-in shared `buy_sell` and `sell_only` policy that sizes the runner
retained after a weak, profitable deep recovery from the recovery's normalized
ATR extension. Complete the native slice when public config, validation,
checkpoint identity, pure reducer behavior, execution receipts, runtime status,
restart behavior, and both trade modes have focused passing tests.

## Scope and authority

Hummingbot owns the policy calculation, attributed-inventory target, durable
sell request, fill reconciliation, and checkpoint identity. This is local
research-only work. Paper, canary, live, exchange, wallet, deployment, push,
pull request, order, and capital actions are forbidden.

## Frozen policy

`DEEP_RECOVERY_STRENGTH_RUNNER_V6` reuses V14's exact qualification: deep
attributed inventory, recovery/defensive/exhausted origin, after-cost profit,
and an extension below the existing first distribution level. It changes only
the retained target:

```text
strength = clamp(distribution_extension_atr / first_distribution_atr, 0, 1)
target = distribution_start_base * final_remaining_fraction * strength
```

The resulting target is bounded from zero through the V14 runner, is monotonic,
and adds no pair-specific or fitted threshold. The policy uses a distinct
`RECOVERY_STRENGTH_RELEASE` intent and normal `DISTRIBUTE` lifecycle. It is
mutually exclusive with V15 full rotation.

## Invariants and validation

- disabled behavior and checkpoint identity remain exact;
- target and sell amount are formula-bound and venue-quantized;
- after-cost profit guard, attribution, pending-order subtraction, and hard cap
  remain unchanged;
- `sell_only` never emits a buy and touches only the explicit assignment;
- canceled orders retain their target and retry through the durable path;
- no copied decision logic outside `ok_rsi_core`.

Run the focused episode, runtime, controller, factory, and execution suites in
the owner environment, Ruff, `git diff --check`, and exact status/diff review.
Rollback by reverting only the V16 native commits; V14 and V15 remain opt-in.
