# Modular operational boundaries

The supported native compositions retain the existing OK RSI and RSI V5 policy,
PositionExecutor/OrderExecutor, tracker and recorder ownership. Live and paper
execution are explicit configuration identities. New compositions remain
`execution_enabled=false` unless an operator separately authorizes a qualified
instance.

## Runner safeguards

All three genuine modular models (`modular_spot`, `modular_ok_rsi`,
`modular_rsi_v5`) use the native runner's durable peak, stop and entry-pause state.
Missing, corrupt, non-finite or differently scoped history blocks new execution.
Global and per-controller drawdown limits must be finite and positive, with the
controller limit no greater than the global limit. API and direct runner paths
both enforce this. These are existing drawdown semantics, not a newly claimed
account-wide or daily-loss policy. The accounting exception for unknown-cost
wallet sales remains limited to its exact existing OK RSI policy conditions.

Operator entry pause does not stop filled protective executors. Generic modular
BUY admission checks the pause again at dispatch; its reduction path remains
separate. Missing/stale data and recovery faults retain their distinct gates.
Recovery HOLD can prevent new reduction actions; it is not proof that every
possible protective exit remains available after a fault.

Native operator events must name the exact MQTT application instance and carry a
finite issuance timestamp and bounded command ID. The runner rejects events
older than 60 seconds, before its process startup, more than five seconds in the
future, or no newer than the last accepted command for a target controller. The
API supplies the bot identity and sub-second timestamp. Its generated IDs are
unique; callers may retain an ID to retry one logical request.

Applied IDs are recorded as indexed native Metadata rows scoped by recorder
configuration and controller. A command receipt and its resulting risk state
commit in the same SQLite transaction. Reusing an ID after a later command or a
restart cannot undo the newer operator decision. Database errors latch a
reconciliation hold. Receipts grow with command history; preserve them with the
owner database and include its storage capacity in normal operational monitoring.
There is no automatic ID eviction or command-count stop limit. Publication to
MQTT is not an execution acknowledgement: verify the subsequent native operator
state/receipt. No live broker or Telegram delivery was exercised by fixture tests.

## Explicit paper binding

The opt-in offline session requires `execution_mode=paper`,
`connector_name=okx_paper_trade`, and `candles_connector=okx`, plus the exact new
native paper connector object. A legacy paper connector or real exchange object
is rejected. Ordinary sealed live recipe export does not switch itself into
paper mode; the research adapter constructs a separate, explicitly validated
paper configuration. Its database, balances and inventory never become live
inventory. See the native paper connector's `NATIVE_V2_PAPER.md` for supported
execution assumptions and source-bound restoration.

The session tests exercise full native controller decisions and execution, but
not the `V2WithControllers` runner's drawdown/operator behavior. Runner rails have
separate native integration tests. Synthetic book/trade fixtures establish
lifecycle correctness, not venue queue priority or economic validity.

## Candidate admission and rollback

A dedicated account or subaccount can isolate a new candidate from the three
existing owners. Its credentials/profile name alone does not prove isolation.
Shared-wallet operation requires reconciled, authoritative capital coordination
across the actual independent owner databases; recorder-local reservations are
insufficient. The current implementation does not establish that coordination.

Before any real-money activation, bind the exact engine/API image, controller
source seal, configuration, account identity, fresh balances/orders/owned
inventory, risk limits, monitoring and rollback evidence. Run the admitted
cached-window research recipe and a separately authorized isolated operational
soak. Tests and PR merges do not authorize account access or orders.

Rollback before activation is a source revert or removal of only the candidate's
isolated resources. The existing rsibot-stack must not be stopped, recreated or
reconfigured as part of candidate validation. Once money is involved, rollback
needs current native order/inventory reconciliation; reverting source alone does
not undo orders or fills.
