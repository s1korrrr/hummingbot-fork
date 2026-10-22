# Initial candidate funding admission

`NativeCompositionFields.funding` is required when a native modular wrapper has
`execution_mode: live` and `execution_enabled: true`. Disabled configurations and
explicit isolated paper sessions may omit it. Example configuration shape:

```yaml
funding:
  allocation_quote: '50'
  fee_reserve_fraction: '0.003'
```

The fee fraction above is illustrative, not an observed or guaranteed OKX fee.
It must be explicitly selected and checked against the intended account's fees.
The candidate policy admits a positive allocation up to 50 USDC. The restriction
belongs to this initial candidate configuration, not the global native engine.
Script `max_global_drawdown_quote` and `max_controller_drawdown_quote` remain
separate explicit loss controls. Funding does not activate a controller.

Scope is one controller, one USDC spot pair, one native recorder and one execution
instance. Sell-only wallet adoption, initial/legacy inventory, and SUI are excluded.
SUI is the known wallet-wide seller asset in the reviewed topology; other such
assets require exclusion before admission. This is not a general automatic
account-topology discovery mechanism. The API must admit only the selected single
candidate controller. Separate databases/instances do not share this allocation.

The admission guard runs after native controller validation and before native
executor intent persistence/start. It writes an immutable funded intent with a
Metadata CAS before dispatch. Native executors, trackers, exact TradeFill rows,
fees and PositionHold remain execution/accounting truth. The extension persists
only admission identities, amount/price/fee ceilings and native-fill hashes. A
small CAS header binds the complete audit set. Individual immutable Metadata rows
scale with native history; there is no fixed lifetime order or fill count limit.
The initial implementation reads the native owner history at admission, so its
cost grows with history; no long-history performance claim is made.

For each BUY, pending quote is remaining authorized gross base times the fixed
entry price, including the explicit fee allowance. The guard admits native limit
acquisitions only and verifies the connector's estimated fee is supported and
within that allowance. Exact actual-currency fees must use native flat-fee
receipts; estimated nonzero-percentage receipts and third-token fees are rejected.
Recorded actual fees above the admitted bound prevent further entries.

Funding already used is the greater of:

- Native cumulative cash consumption: BUY notional plus actual quote fees minus
  net SELL proceeds. Realized losses remain consumed after inventory is flat.
- Remaining acquisition basis and attributed remaining fees, computed by the
  actual native PositionHold implementation. This prevents realized profits from
  enlarging the configured inventory envelope.
- Zero.

Used funding plus all pending commitments plus the proposed fee-inclusive BUY
must fit the allocation. Unknown ownership, non-exact receipts, missing or changed
history, conflicting executor identities, changed configuration/source, or an
unresolved admitted intent fails closed for new acquisitions. Existing protective
SELL/Stop behavior remains native; a funding hold does not stop those actions.
Native wallet availability checks still apply separately.

## Cancellations, restarts and operational limits

Unused quantity remains reserved after cancellation, rejection, timeout or an
executor becoming terminal. Capacity is not released merely because an order is
absent or a wall-clock deadline passed. A later authoritative native fill consumes
that already reserved quantity. This deliberately avoids a late-fill reuse race.
Fully recorded gross fills replace the corresponding reservation with actual
native cash/cost usage. A failed dispatch after admission leaves an unresolved
intent and blocks subsequent admission pending reconciliation.

A cancel-only path can exhaust the allocation without spending cash. Status
exposes `funding_hold`, `pending_quote`, `available_quote`, `native_cash_used`,
`native_inventory_cost` and `unused_intent_release: explicit_reconciliation_required`.
There is no automatic/manual capacity-reset endpoint in this slice. Safe future
release requires an explicit native reconciliation transaction proving final
exchange quantity/fills, no remaining child/replacement authority, and complete
recorder coverage, then atomically retiring the unused admission. A single-attempt
intent may prove several terminal entry orders when the down-only maker replaced
an unfilled order (post-only repair or downward reprice); at most one of those
orders may carry fills, and every order needs its own terminal proof. Restarting,
deleting Metadata, or setting a new controller ID is not that reconciliation.

This is a candidate-local spending boundary. It does not lock shared OKX cash,
reserve liquidity, constrain old owners, guarantee order acceptance, or implement
an account-wide drawdown rail. Hosted CI status, local tests and this policy do not
authorize deployment or orders. Automatic canceled-intent capacity recovery and
long-history performance remain qualification limitations; this slice is not a
claim of full production readiness.
