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
One fixture owns one controller, one unleveraged USDC account and one acquisition
episode. The fixture loss ceiling spans its whole lifetime; it does not reset at
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
requires exactly one native executor, no outstanding orders or held-position
ledger, an intact configuration/source/recipe binding, and a positive native
terminal singleton entry receipt. Both full and terminal partial entries are
supported. Exact remaining inventory, fees and persisted trailing state must
match; trailing peaks are never reconstructed from a new quote.

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
