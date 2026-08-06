# ADR 0001: Isolated absolute-barrier position executor

- Status: Proposed
- Date: 2026-08-06
- Decision owners: RSI Bot / GeoShock maintainers

## Context

The existing `PositionExecutor` expresses stop loss and take profit as percentages of its runtime `entry_price`. After the open order completes, `entry_price` becomes the average executed fill price. This is correct for existing strategies, including RSI controllers, but incompatible with GeoShock's signed `TradeIntent`, which binds risk approval to exact absolute stop and target prices.

Converting signed prices to percentages at the reference limit price would change the effective barriers whenever the actual fill differs. That would break the policy-to-execution binding and make the signed maximum-loss calculation non-reproducible.

## Decision

Add an isolated executor type:

```text
absolute_barrier_position_executor
```

It subclasses `PositionExecutor` to reuse order lifecycle, close-order handling, fee accounting, retry behavior, and shutdown behavior while overriding only absolute-barrier semantics.

Version 1 is intentionally narrow:

- one absolute stop price;
- one absolute take-profit price;
- limit entry with a maximum age of 1–300 seconds;
- market stop, take-profit, and time-limit exits;
- mandatory time limit;
- leverage fixed to 1;
- no activation bounds;
- no trailing stop;
- no multi-level take profit;
- partial fills become protected as soon as exchange minimum size and notional permit a close;
- entry, stop, and target must already be aligned to the venue tick.

The API/risk boundary remains responsible for deterministic tightening quantization, signature checks, nonce consumption, portfolio/RSI conflict checks, account state, liquidity, and venue health.

## Alternatives considered

### Modify `PositionExecutor` with a barrier mode

Rejected for the first slice. It would add a semantic branch to a widely used executor and increase regression risk for existing percentage-based RSI behavior.

### Convert absolute prices into percentages at the reference entry

Rejected. A different actual fill rebases the barriers and weakens the signed risk decision.

### Sign a second management intent after the fill

Not selected. It adds a second network and signing round trip during a shock and leaves a period in which the position has no final management policy.

### Build a completely independent executor

Rejected. It would duplicate mature entry, close, retry, event, accounting, and shutdown behavior already implemented by `PositionExecutor`.

## Consequences

Positive:

- existing percentage executor behavior is unchanged;
- signed prices remain exact after fill;
- GeoShock support can be reviewed independently;
- future risk-bound strategies can reuse the executor.

Costs:

- Hummingbot API must explicitly register the new executor type;
- status/UI code must understand absolute barriers;
- market exits may experience slippage and require guardian monitoring;
- dust fills below venue minimums require preflight sizing and guardian reconciliation.

## Acceptance criteria

- invalid geometry or unsupported order semantics fail during config validation;
- unaligned prices fail before order creation;
- fill price changes do not alter stop or target prices;
- stop and target trigger on the close-side market price;
- protectable partial fills use the same absolute barriers;
- percentage `PositionExecutor` tests remain unchanged and pass;
- no live registration occurs until API nonce, preflight, and testnet gates are implemented.
