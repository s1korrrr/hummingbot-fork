# Planned position resume

This is an operator-assisted handover of existing inventory to its original
PositionExecutor. It is not automatic crash recovery, a new BUY decision, or
trading authority. Deployment and owner restart require their own authorization.
An ordinary manual or unexpected restart that finds an unfinished executor
blocks new executor creation across the stack and suppresses PositionExecutor
order submission and trailing control until reconciliation. This is a hold,
not a completed handover or a promise that an already-resting exchange SELL
cannot fill. Do not clear the unfinished row merely to restart trading.
An unspecified controller shutdown preference now retains inventory by default;
an explicit `early_stop_keep_position: false` still requests an exit. Retaining
inventory does not restore trailing management after restart. Use this handover
only for its proven scope, and leave other unfinished executors in startup
reconciliation rather than treating a held position as a resumed executor.

The supported scope is deliberately narrow: one completely FILLED OKX 1x spot
BUY entry per selected PositionExecutor, no exit order identity or fill, no exit
trigger, and trailing-only barriers. Time limits, stop loss, take profit, partial
entries, derivatives, other connectors and ambiguous ownership are rejected. The
original executor ID, complete configuration, timestamp, trailing trigger, move
count and trailing timestamps are retained. The resumed entry is permanently
sealed, including direct placement and late cancellation/failure events. Duplicate
fill receipts are ignored; a contradictory new fill blocks execution and requires
reconciliation. Native exit control uses the exact owned quantity and exchange
lot rules.

## Preparation

1. Capture a preflight SQLite snapshot while the explicitly selected executors
   are running. Preserve the selected IDs and configuration.
2. Strictly stop the owner with positions retained. Independently prove the
   process is stopped, retain a rollback backup, and pause database readers that
   could interfere with checkpointing. Checkpoint the target WAL before the
   read-only venue observation. Do not modify or replace database bytes to force
   a comparison to pass.
3. Produce a recent authenticated read-only OKX reconciliation report. The report
   must contain terminal order details, complete fills with actual fee currencies,
   empty regular and all supported algo-order scopes before and after observation,
   selected-inventory balance coverage, unchanged database SHA, and no blockers.
4. Assemble the final source/config tree and an evidence request. Every controller
   listed by the script, the script itself, client config, and encrypted OKX
   connector config must be included in `config_paths`. The helper hashes their
   bytes; it does not print or decrypt configuration contents.
5. Run preparation against the same stopped target database observed by the venue
   helper, using the preflight snapshot as the second input. The owner must remain
   stopped until the independently approved startup.

```sh
python -m hummingbot.strategy_v2.executors.planned_position_resume prepare \
  --db /path/to/stopped-owner.sqlite \
  --preflight-db /path/to/preflight.sqlite \
  --evidence /path/to/resume-evidence.json \
  --runtime-root /path/to/final-runtime-tree

python -m hummingbot.strategy_v2.executors.planned_position_resume inspect \
  --db /path/to/stopped-owner.sqlite
```

`--runtime-root` defaults to the source tree running the helper. It allows offline
preparation against a staged deployment tree. Native startup checks the same
relative files beneath its own source root; different file content is rejected.

The request is JSON with these fields:

- `schema: 1`, `owner`, `script_config` (the recorder's script filename),
  `owner_stopped: true`, `venue_open_orders: []`;
- `observed_at` and `expires_at` as UTC Unix seconds. Observation must be at most
  five minutes old; expiry must be in the future and at most fifteen minutes from
  preparation;
- `config_paths`: relative paths including `conf/scripts/<script_config>`, all
  script-listed `conf/controllers/...` files, `conf/conf_client.yml`, and
  `conf/connectors/okx.yml`;
- `reconciliation`: the complete `schema_version: 1`,
  `kind: okx_stopped_owner_get_reconciliation` report with `venue_queried: true`
  and `result: EVIDENCE_COMPLETE`;
- `executors`: explicit unique selections, each containing `executor_id`,
  `client_order_id`, `exchange_order_id`, `trading_pair`, `state: filled`, decimal
  strings `filled_base`, `filled_quote`, `net_base`, and `fills`. `fills` maps
  exchange trade IDs to `fill_base_amount`, `fill_price`, and the exact native
  fee JSON object. Values must agree with both raw venue fills and native receipts.

Preparation validates these claims against the actual database. It stores a
manifest in `PlannedPositionResume` and changes no executor/held ownership.
Missing empty additive exact-receipt tables are equivalent to empty tables;
creating them during native schema initialization does not invalidate the seal.
Changed rows, configuration, source, receipts, stale evidence or manifest reuse
fail closed. The manifest includes the original preflight and stopped-state
bindings plus exact ownership rows retained for review and rollback planning.

## Native startup and failure handling

Startup defers hydration until connector and market-data readiness, including
valid current bid/ask prices, rules, available inventory and no tracked open
orders for the selected pairs. New execution is blocked while the handover is
pending. Reports explicitly remain unavailable until ownership is established.
One SQLite transaction consumes the claim, marks each original executor
unfinished, removes only its selected held-ledger rows, and removes affected
aggregate Position projections that were corroborated by the complete exact
ledger. Those rows remain archived in the manifest. Unrelated ledger receipts
stay intact and rebuild their own held positions, including receipts in the same
pair. A later process restart after a completed trailing exit must not resurrect
the original held BUY.

Any exception after claim blocks subsequent attempts in that process. Quiesced
executor objects are removed from runtime persistence collections while the durable
unfinished claim remains recorded for reconciliation. Strict shutdown cannot report
reconciliation complete while the handover remains unresolved. A claimed
manifest is never automatically reused by another process; persisted unfinished
executors require explicit reconciliation. Keep all prepared database copies
stopped: a local SQLite transaction does not provide exclusivity across copied
databases or different machines.

A later explicitly authorized handover requires a new running preflight, strict
stopped-owner receipts and fresh venue reconciliation through the same procedure.
Preparation appends a new manifest only when every existing manifest is a valid
consumed claim. Prior rows and payloads remain unchanged; the new manifest binds
that history and rejects changes before claim. An unconsumed, unknown or malformed
history blocks preparation. This never replays a prior claim or bypasses current
ownership, source, configuration, expiry or reconciliation checks.

Before claim, rollback is the original stopped database and runtime backup. After
claim or any resumed exit, stop and reconcile orders/fills first; do not restore
an old database over new execution. Inspect the archived selected ledger and
projection rows, original entry receipt, current executor and venue state before
an explicitly authorized recovery. No automatic rollback or manifest deletion is
provided.

## Verification

Owner tests cover real native InFlightOrder stop/serialization, SQLite preparation,
waiting and armed trailing state, retained identity/configuration, no new BUY,
late event conflicts, native trailing SELL, same-pair unrelated inventory,
completed-exit restart without phantom holdings, stale binding/receipt conflicts,
old empty-sidecar schema initialization, and failures after claim. Native OKX
mocked REST tests separately prove that cancellation ACKs cannot release zero or
partial-fill ownership without cumulative-fill reconciliation. These tests make
no authenticated exchange requests and establish no deployment outcome.

## General restart protocol still required

SQLite is a durable local record, but an exchange acknowledgment, fill, or
cancel may arrive before the corresponding executor row is committed. A saved
order ID, a local `OPEN` state, or a zero local active-order count does not
prove the exchange state. The current planned handover rejects active entries
and exits; no normal startup path yet reattaches them automatically.

For manual and crash restarts alike, the next protocol must keep order emission
fenced while it reads WAL-inclusive native state and authenticated venue
orders/fills, validates the configured owner and exact order identities, then
reattaches each entry, exit, inventory quantity, and persisted trailing trigger.
Unknown or conflicting evidence remains held with a visible recovery reason.
The current startup hold also covers unfinished executor rows whose controller
is absent from the new config, pending planned-resume claims before rows load,
and BUY/SELL calls through the executor and StrategyV2Base order paths. A
configured controller must not trade around an unfinished removed controller.
These guards do not reconcile or cancel orders already resting at the venue.
Before resuming an owner on the shared V1/V2 OKX wallet, aggregate owned base
inventory across both stacks so one bot cannot sell inventory owned by the
other. A resting SELL can fill during downtime or cancellation; its final
fills must be reconciled before the position can be called held.

Release checks must inject crashes before and after submission, acknowledgment,
partial fill, cancel, trailing movement, exit submission/fill, and native commit;
verify no duplicate BUY/SELL, no trailing reset, one owner, and a stable
fail-closed result under stale or unavailable OKX responses. Until those checks
and exact-image qualification pass, the startup hold is the safe result rather
than automatic continuation.
