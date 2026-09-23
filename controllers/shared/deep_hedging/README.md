# Deep Hedging v1: non-executing policy boundary

This is a research/shadow foundation, **not a deployable Hummingbot controller**.
`ShadowController` is not a `ControllerBase` subclass and cannot create executor
configs, orders, connectors, account changes or an execution owner. No existing
controller, deployment configuration, SUI owner or sell policy is modified.

## Implemented

`policy.load_policy(raw, expected_sha256, expected_context)` admits bounded,
strict JSON bytes. It verifies the externally supplied digest, exact context,
feature names/units/domains/normalization, dimensions, finite weights and explicit
`shadow_only` / `ENGINEERING_ONLY` labels. Immutable tuple weights are evaluated
with a small float64-compatible tanh network. The network emits an ordered band;
its target is the current holding clipped to that band. Overflow is rejected,
including before saturating nonlinearities. Hash identity is not approval.

`shadow.ShadowController.advise(snapshot, now=...)` produces either a held
reason or a bounded, quantized advisory target. It checks owner/book context,
market and account event/availability times, freshness, an explicit reconciliation
reference, position bounds, unresolved orders, sequence and clock regression.
Every decision has `execution_permitted=False` and no executor actions. Invalid
identity fields remain serializable as unavailable rather than leaking NaN.
No file or network I/O occurs during load or advice.

A reconciliation reference is **not verification of the receipt it names**.
Callers must verify native ownership and reconciliation evidence before making
snapshots. The live snapshot adapter is not implemented here. Oldest event times
must cover all features and account components, not just the newest quote.
Sequence guards are process-local, not a durable exactly-once order protocol.
After restart there is no automatic recovery, adoption or authority.

`shadow.bounded_spot_entry(...)` is a pure BUY-only contraction utility, not a
learned spot model or an admission. It returns zero or a size no greater than the
baseline proposal, independent portfolio cap, and free-cash budget including
costs. Free cash must already exclude native reservations. Pending cancellations
count as unresolved orders until confirmed. It cannot sell, short, release
reservations or increase a baseline purchase. It is **not wired into ok_rsi**.
A future integration must form the final quantity before executor fingerprinting
and durable reservation in every selected action path; never mutate a reserved
configuration afterwards.

Quantity arithmetic uses exact rational caps and isolated decimal conversion,
independent of the caller's Decimal precision. Input quantities are bounded to
64 significant digits and adjusted exponent +/-100 as serialization/numerical
limits, not market risk limits. Unix timestamps are bounded through year 9999.
Policy features/position limits and TTLs are explicit caller contracts.

## Local checks

From the owning checkout, in its supported Python environment:

```bash
python -m unittest discover -s test/controllers/shared -p 'test_deep_hedging_*.py' -v
```

The Quants Lab companion imports this exact module; it does not copy serving
logic. Its fixture test checks exported inference against the training graph.
No trained model artifact or credentials are committed.

## Remaining gates

A native controller requires a separately reviewed instrument/capital model,
connector-specific cash/margin/funding/settlement rules, verified snapshot
construction, outstanding-order and partial-fill semantics, durable ownership,
execution-boundary admission and reconciliation. Stale input handling must not
blindly flatten a derivative hedge or interfere with existing approved exits.
The SUI sell-only owner remains excluded from this policy's control scope.
No live inference, paper replay, economic backtest or independent audit is claimed.
