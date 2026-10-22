# RSI modular V2 funding upgrades

Funding binds a native recorder, all configured strategy peers, configuration and controller source identity. A source/config change with retained trading history needs an explicit transition. A healthy process or `buy_gate=ready` does not authorize a checkpoint reset.

## Entry sizing and status

The existing policy remains the default. `funding.resize_to_available_quote: true` opts a native controller into reducing its requested BUY quote amount to the smaller of remaining strategy funding and remaining pair capacity, after the configured fee reserve. Every peer in a strategy scope must have the identical funding policy. It never enlarges a signal's requested size, the allocation, or the position cap. Native price/amount quantization and exchange minimums still apply; final admission checks the exact executor and claims its capacity durably. If the minimum cannot fit, no BUY is dispatched. A failed sizing read holds that candidate. Protective native exits do not depend on the BUY sizing hint.

The September 23 V2 deployment began with a shared `allocation_quote: 1000` across five pairs, `max_position_quote: 105` for fee and retained-uncertainty headroom, and `max_total_position_usd: 100` plus `usd_per_entry: 100` on each controller. The subsequent requested configuration raises the shared ceiling to `2500` and each controller's position cap to `500`, with `max_position_quote: 505` covering the 0.3% fee reserve and historical uncertain claim. A funded entry is hard-capped at its configured `usd_per_entry: 100` even if dynamic signal sizing requests more. The allocation is a ceiling, not an instruction to place a 2,500-USDC order or a guarantee that shared-wallet cash is available. The script retains `max_daily_loss_quote: 50`, a BUY-entry pause that preserves SELL exits. Enrollment of that rail against an existing owner requires a reviewed native risk-state migration; adding the YAML key alone fails closed because the old risk row has no `daily_entry_risk` field. The migration starts from the persisted global PnL high-water, conservatively counting any prior decline toward the daily limit.

`entry_admission` reports `blocked`, `unverified`, or `conditional`. Conditional means the funding layer has no recorded hold; it is not a promise that market data, a signal, venue rules or subsequent admission will pass. Compact `gate` and detailed `buy_gate` expose funding/recovery holds; `native_buy_gate` retains the earlier strategy decision. `modular_composition.funding_sizing` reports requested, ceiling and effective quote. A full-entry configuration no longer advertises inactive scout/runner slices.

## Explicit transition API

This is a maintenance API, never called automatically by entry admission. Establish separate maintenance/deployment authority and reconcile the venue before applying it to a live owner. Use the exact qualified engine/controller source and the final owner configurations. Create the real `NativeFundingStore` against the native recorder with all genuine strategy peers bound.

```python
proposal = store.plan_binding_transition(expected_previous_binding)
# Review proposal, especially pending_quote, legacy_uncertain_quote,
# available_quote, inventory_cost, native_sha256 and exact target identity.
receipt = store.apply_binding_transition(proposal['transition_id'])
status = store.reconcile()
```

Planning performs read-only SQLite queries. Applying obtains a SQLite write transaction, reconstructs the proposal and rejects changed evidence. The transition requires no active executor/order, terminal native ownership and exact fill coverage, unchanged target configuration, and capacity sufficient for existing exposure and retained commitments. Durable pre-submit, tracker and supervised-recovery snapshots are checked even when no normal Order row exists. Unowned, uncertain, contradictory or incompletely recorded submissions refuse the transition.

The transaction preserves the exact old funding rows in a content-addressed receipt, rebinds funding fact identities, records native-proven releases and fills, and advances the funding header. It never changes native order, fill, executor, position or tracker rows. A failed transaction rolls back every funding edit. An already applied receipt is idempotent only under its matching identity and content hash; a process that lost an unapplied in-memory plan must plan again.

The old `never_started_intent_v1` marker is historical provenance, not proof that a venue submission never happened. A preexisting orphan carrying that marker remains fully reserved at amount × limit ceiling × (1 + fee reserve), globally once and against every pair limit because the legacy claim has no pair field. It is not counted as a released intent. An unknown orphan without that sealed legacy record blocks. Do not manufacture a marker, delete the claim, or use shared-wallet balances to erase the uncertainty.

## Live cutover requirements

1. Obtain explicit authority for the V2-only deployment and potential subsequent real orders. V1 remains untouched.
2. Re-read actual owner/image/mount/config identities and reconcile current OKX orders, fills and owned inventory read-only. The migration checks persisted evidence; it cannot certify exchange-wide state or exclude activity in another process.
3. Take a WAL-inclusive backup through SQLite in the execution container's filesystem context. Verify the copied DB with `PRAGMA integrity_check`, and retain config, source overlays and image identities. Cross-host snapshots that fail integrity are unusable evidence; never rebuild a live DB from one.
4. Rehearse on a copy using the exact deployed-image candidate and final configs. Inspect every peer's budget, record native-table hashes before/after, and refuse any uncertainty the transition does not explicitly retain.
5. During an approved quiescent V2 maintenance window, apply the reviewed transition with the exact target source/config identity. Source formatting changes also alter that identity. The target must not dispatch orders concurrently with maintenance.
6. Prepare the current native cutover/admission receipt required by the stack, then replace only `execution-rsi` with the qualified image/overlays using the stack's scoped `up -d --no-deps --pull never` procedure. An expired receipt or a plain restart is insufficient. Refresh V2 API lifecycle registration if its expected boot identity differs.
7. Verify the new native runtime identity, funding reconciliation for all peers, truthful gates, data freshness and subsequent real order lifecycle. A synthetic fill is not a venue fill. Maintain the existing exposure/loss/operator controls.

Before migration, rollback is retaining the existing owner/config/database unchanged. After migration, reverting source changes funding identity again and needs another explicit evidence-checked transition. Never overwrite a current native database with an old backup after venue activity; preserve and reconcile any new orders/fills first.
