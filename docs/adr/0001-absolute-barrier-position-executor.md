# ADR 0001: Isolated absolute-barrier position executor

- Status: Proposed
- Date: 2026-08-06
- Decision owners: RSI Bot / GeoShock maintainers

## Context

The existing `PositionExecutor` expresses stop loss and take profit as percentages of its runtime `entry_price`. After the open order completes, `entry_price` becomes the average executed fill price. This is correct for existing strategies, including RSI controllers, but incompatible with GeoShock's signed `TradeIntent`, which binds risk approval to exact absolute stop and target prices.

Converting signed prices to percentages at the reference limit price would change the effective barriers whenever the actual fill differs. That would break the policy-to-execution binding and make the signed maximum-loss calculation non-reproducible.

The signed intent also includes an exact UTC `entry_deadline`. A relative open-order age alone is insufficient: API processing and executor startup consume part of the signed lifetime, and a partially filled order must stop accumulating exposure at the signed deadline without losing accounting for the already filled position.

## Decision

Add an isolated executor type:

```text
absolute_barrier_position_executor
```

It subclasses `PositionExecutor` to reuse order lifecycle, close-order handling, fee accounting, retry behavior, and shutdown behavior while overriding only the semantics that must remain bound to the signed decision.

Version 1 is intentionally narrow:

- one absolute stop price;
- one absolute take-profit price;
- limit entry;
- an exact timezone-aware UTC entry deadline, no more than 300 seconds after executor creation;
- a relative entry-order maximum age of 1–300 seconds as an additional earlier cap;
- market stop, take-profit, and time-limit exits;
- mandatory time limit;
- leverage fixed to 1;
- no activation bounds;
- no trailing stop;
- no multi-level take profit;
- partial fills become protected as soon as exchange minimum size and notional permit a close;
- at the signed deadline, an unfilled entry expires and a partially filled entry cancels only its remainder;
- an unacknowledged remainder cancellation is retried on a bounded cadence;
- a closed tracked order is reconciled even when the cancellation event is not delivered;
- the partially filled entry order remains tracked after cancellation so position and fee accounting are preserved;
- entry, stop, and target must already be aligned to the venue tick.

The generic executor type union is expanded to recognize the new isolated type. Existing `position_executor` semantics and RSI controller code remain unchanged.

The API/risk boundary remains responsible for deterministic tightening quantization, signature checks, atomic intent consumption, portfolio/RSI conflict checks, account state, liquidity, venue health, compatibility manifests, and unknown-outcome reconciliation.

## Alternatives considered

### Modify `PositionExecutor` with a barrier mode

Rejected for the first slice. It would add a semantic branch to a widely used executor and increase regression risk for existing percentage-based RSI behavior.

### Convert absolute prices into percentages at the reference entry

Rejected. A different actual fill rebases the barriers and weakens the signed risk decision.

### Use only relative open-order age

Rejected. Relative age starts when Hummingbot creates the order and can extend beyond the signed decision lifetime. The exact deadline is authoritative; relative maximum age is only a stricter local cap.

### Clear the tracked order on partial-fill cancellation

Rejected. Hummingbot's position amount, fee, close-volume, and PnL accounting rely on the tracked entry order. Clearing it after a partially filled cancellation can make real exposure appear flat.

### Retry executor creation when cancellation acknowledgement is missing

Rejected. No replacement entry is permitted under the same intent. The executor retries cancellation of the existing remainder only and continues managing filled exposure.

### Sign a second management intent after the fill

Not selected. It adds a second network and signing round trip during a shock and leaves a period in which the position has no final management policy.

### Build a completely independent executor

Rejected. It would duplicate mature entry, close, retry, event, accounting, and shutdown behavior already implemented by `PositionExecutor`.

## Consequences

Positive:

- existing percentage executor behavior is unchanged;
- signed prices remain exact after fill;
- signed entry lifetime is enforced independently of API and startup latency;
- protectable partial fills remain managed while their unfilled remainder is cancelled;
- cancellation-event loss no longer permits a single fire-and-forget cancel attempt;
- GeoShock support can be reviewed independently;
- future risk-bound strategies can reuse the executor.

Costs and residual risks:

- Hummingbot API must explicitly register the new executor type;
- status/UI code must understand absolute barriers and entry-cancellation state;
- market exits may experience slippage and require guardian monitoring;
- dust fills below venue minimums require preflight sizing and guardian reconciliation;
- full connector integration and testnet evidence are still required;
- a separate API reconciliation loop must detect unknown executor-creation outcomes and account mismatches.

## Acceptance criteria

- invalid geometry or unsupported order semantics fail during config validation;
- deadlines that are naive, non-UTC, expired at creation, or more than 300 seconds after creation fail validation;
- unaligned prices fail before order creation;
- fill price changes do not alter stop or target prices;
- stop and target trigger on the close-side market price;
- protectable partial fills use the same absolute barriers;
- unfilled entries expire at the exact signed deadline;
- partially filled entries cancel only their unfilled remainder at the deadline;
- missing cancellation acknowledgement causes bounded cancel retries, not a replacement entry;
- partially filled cancellation retains entry-order accounting;
- percentage `PositionExecutor` tests remain unchanged and pass;
- no live registration occurs until API admission, preflight, reconciliation, full CI, and testnet gates are implemented.
