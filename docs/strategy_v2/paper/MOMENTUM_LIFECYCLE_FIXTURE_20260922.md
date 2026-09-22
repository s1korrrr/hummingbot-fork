# Native momentum lifecycle qualification fixture

This is an explicit offline `NativePaperSession` adapter. Normal momentum
observation, shadow and paper configuration still rejects execution enablement;
live mode remains unavailable. A caller must separately supply an exact
`MomentumPaperPolicy` to the disposable session constructor. This option is not
exposed in controller configuration or a deployment path. Status marks the
session `offline_fixture: true` while keeping production execution authority false.

The host uses its unchanged H67 B0/C1 feature pipeline and setup policy. Fixture
initial cash and explicit sizing, capital, loss and native exit mappings must
match the recipe's existing policy digests. Prerequisites come from the actual
native account, recorder and source binding. Missing account valuation, ownership,
capital, loss allowance or fresh evidence cannot be replaced with a passing flag.
One fixture owns one controller and one unleveraged USDC account. The
`native_flat_reseed_v1` coordinator admits serial episodes only after every prior
native executor has durable terminal proof and the account is flat. A strictly
newer observation first seeds the unchanged canonical policy; a later distinct
setup may acquire. Immutable consumed-opportunity identities in recorder Metadata
prevent old events from becoming new acquisitions. The coordinator version is
part of the checkpoint binding; no recipe is automatically resealed. The fixture loss ceiling spans its whole lifetime; it does not reset at
a day boundary and is therefore stricter than a same-sized daily allowance.

Canonical policy state, opportunity/consumption identities and prerequisite
identity are checkpoint metadata in the existing native recorder. SQLite
`BEGIN IMMEDIATE` plus compare-and-swap commits that checkpoint and the native
`Executors` intent together, before an order-capable object starts. The native
orchestrator owns creation and the native `PositionExecutor` owns every fill,
fee, partial-entry cancellation, stop loss, trailing high-water and exit. There
is no new order, fill, inventory, or accounting ledger. The prior reference
JSON ClaimStore is not used by this executing offline fixture.

`await session.suspend()` is a fixture-only process-detachment boundary: save the
native executor receipts, market tracking state and existing step journal, then
unregister listeners without cancelling orders or changing native exit intent.
A matching restore preserves the original executor identity. Qualified recovery
requires exactly one current unfinished native executor, fully proven closed
historical owners, no outstanding orders or held-position ledger, an intact configuration/source/recipe binding, and a positive native
terminal singleton entry receipt. Both full and terminal partial entries are
supported. Exact remaining inventory, fees and persisted trailing state must
match; cumulative base and quote, each fill price/quantity/quote, order and trade
identities, fee receipts and native millisecond timestamps are checked against
the durable recorder before an executor is installed. Trailing peaks are never
reconstructed from a new quote.

Unstarted intents without native orders/fills can resume their original identity.
An outstanding entry, triggered/in-flight exit, missing receipt, inconsistent
trailing state, interrupted step journal or ambiguous ownership remains under
native reconciliation HOLD. This does not silently recreate a BUY or move held
inventory to a new owner. Fresh public books are required before management
continues; restored historical data is not presented as a fresh mark.

Local tests exercise canonical seed/breakout acquisition, repeat suppression,
real partial fills and fees, entry remainder cancellation, armed trailing state,
suspension/restoration and native exit. Failure-path tests cover insufficient
capital, unknown assets, policy binding changes and unfinished-entry recovery.
Synthetic execution evidence is not an economic result, production paper
activation, shared live-wallet qualification or promotion authority. Both
running rsibot stacks remain unchanged.

A process lease remains held on the canonical database sidecar path for the
fixture lifetime, acquired before SQL setup, marker/journal reads, connector
restoration or policy checkpoint reads and released after recorder cleanup.
Constructor failures also release it. A second process cannot read or restore the same paper account while
its owner is active. The native metadata acquisition pause persists through
restart and only blocks admission; native executor cancellation and exits keep
running. Synthetic resumed-versus-uninterrupted tests compare exact balances,
fill quantities/prices/fees, executor identities and preserved trailing state
across two serial episodes. Missing terminal proof blocks the next episode.
