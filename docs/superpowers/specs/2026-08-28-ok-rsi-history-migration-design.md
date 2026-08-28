# OK RSI History and Inventory Migration Design

## Status

- Approved in chat by the operator on 2026-08-28.
- Owner repository: `hummingbot/`.
- Target stage: live-adjacent repository and local-runtime preparation only.
- Forbidden without separate approval: unlocked strategy start, order placement, cancellation, withdrawal, credential mutation, or any other balance mutation.

## Objective

Create one truthful OKX USDC runtime identity that retains the complete RSI V5 database history, restores only verified open inventory as OK RSI bags with working average prices, exposes legacy and current performance without conflating venues, and never force-sells held inventory.

## Final Runtime Identity

- Script config: `conf/scripts/ok_rsi.yml`.
- Automatically selected database: `data/ok_rsi.sqlite`.
- Strategy launcher: `scripts/v2_with_controllers.py`.
- Connector: `okx`.
- Spot pairs: `BTC-USDC`, `ETH-USDC`, `SOL-USDC`, `XRP-USDC`, and `BNB-USDC`.
- Controller IDs: `ok-rsi-btc`, `ok-rsi-eth`, `ok-rsi-sol`, `ok-rsi-xrp`, and `ok-rsi-bnb`.
- Final manual kill-switch state: `false` for all five controllers.
- The unlocked final configuration remains stopped after verification.

`TradingCore.initialize_markets_recorder()` derives the SQLite basename from the V2 config source. Therefore `start --v2 ok_rsi.yml` selects `data/ok_rsi.sqlite`; no database alias or global client database override is required.

## Capital and Sell Invariants

Every final profile must preserve these invariants:

- `leverage: 1`, spot, and one-way mode.
- `sell_only_if_profitable: true`.
- `stop_loss: null`.
- `take_profit: null`.
- `time_limit: null`.
- `early_stop_keep_position: true`.
- Ordinary BUY exits remain trailing-only and cannot silently lose their exit contract.
- Drawdown, shutdown, stale-order cancellation, or runtime restart cannot force liquidation of held inventory.
- Signal and recovery sells remain bounded by tracked inventory and actual available OKX base balance.
- A migrated bag cannot sell below the controller's fee-aware cost basis and configured minimum-profit rule.
- Recovery sells remain partial and conditional on the configured RSI/reversal/trailing state machine.
- No transient trailing peak, cooldown, loss streak, or active executor state crosses from RSI V5 into OK RSI.

## Source and Target Databases

### Source

- Path: `data/rsi_v5.sqlite`.
- Authority: immutable legacy Binance/USDC execution history and the prior controller's stored open-position state.
- Current observed scope: 1,302 trade fills, 1,583 executors, six stored positions, and 414 position-hold ledger rows.

### Existing intermediate database

- Path: `data/ok_rsi_buy_sell.sqlite`.
- It is retained in the backup set but is not the final active database.
- Any existing OK RSI rows must be inventoried and collision-checked before import; they must not be silently discarded.

### Target

- Path: `data/ok_rsi.sqlite`.
- Created through the current Hummingbot schema path or an equivalent schema-compatible initialization before import.
- Must pass `PRAGMA quick_check` and `PRAGMA foreign_key_check` before and after migration.

## Historical Import Contract

The target contains the complete legacy history while preserving original identity:

- Original `TradeFill` rows retain `config_file_path=rsi_v5.yml`, `strategy=v2_with_controllers`, `market=binance`, original USDC symbols, order IDs, trade IDs, timestamps, prices, amounts, and fees.
- Original orders, order-status rows, executors, controller configurations, position rows, position-hold ledger entries, runtime events when present, and other compatible historical tables retain their original values.
- No Binance row is relabelled as OKX.
- No legacy controller ID is rewritten to an OK RSI ID.
- Target-only schema tables remain intact.
- Import order respects table keys and relationships.
- Duplicate rows with identical canonical content are accepted idempotently; a conflicting row with the same key fails the migration.
- The migrator records a canonical JSON receipt with source checksum, target pre/post checksums, schema fingerprints, per-table inserted/skipped counts, controller mappings, wallet reconciliation, and validation results.

Legacy history is available for reporting, but current OK RSI risk state uses only current OK RSI controller IDs. Legacy realized performance must be labelled `RSI V5 · Binance · USDC`; OK RSI performance must be labelled `OK RSI · OKX · USDC`.

## Inventory Seed Contract

Only the five shared base assets are eligible:

| Source | Target | Controller |
| --- | --- | --- |
| `binance.BTC-USDC` | `okx.BTC-USDC` | `ok-rsi-btc` |
| `binance.ETH-USDC` | `okx.ETH-USDC` | `ok-rsi-eth` |
| `binance.SOL-USDC` | `okx.SOL-USDC` | `ok-rsi-sol` |
| `binance.XRP-USDC` | `okx.XRP-USDC` | `ok-rsi-xrp` |
| `binance.BNB-USDC` | `okx.BNB-USDC` | `ok-rsi-bnb` |

EUR-USDC, USDC-PLN, and any unsupported source scope remain legacy reporting only.

For each eligible asset, the migrator must:

1. Decode the authoritative stored `Position` amount, breakeven, and cumulative held-position fees through Hummingbot's model types rather than treating scaled SQLite integers as economic decimals.
2. Independently reconstruct the remaining amount and weighted acquisition economics from position-ledger and trade-fill evidence.
3. Reject non-finite, non-positive, ambiguous, or materially inconsistent state.
4. Obtain the actual available OKX base balance through an authenticated read-only connector session that cannot create, cancel, or modify orders.
5. Require the proposed tracked amount to be no greater than the available balance.
6. Report any additional OKX balance as `untracked`; never invent a cost basis for it.
7. Fail closed when the available balance is below the source tracked amount beyond trading-rule dust tolerance. No proportional cost rewrite is allowed without a separately reviewed resolution.
8. Create an idempotent OK RSI position-hold seed with the mapped controller, OKX connector, USDC pair, source amount, breakeven, and attributable fees.
9. Include source database checksum and legacy position identity in the seed provenance.

The seed is inventory state, not an exchange fill. It must not be inserted as a fabricated OKX `TradeFill` or order.

## Average Price and First-Sell Report

After restoration, produce one row per asset containing:

- Available OKX base balance.
- Migrated tracked amount.
- Untracked balance.
- Restored fee-aware average/breakeven price.
- Current OKX mid price and quote value captured during the read-only session.
- Configured minimum-profit percentage.
- Minimum profitable signal-sell price.
- Configured bag-recovery target percentage.
- Recovery arm price.
- Recovery partial-exit fraction and first eligible sell amount.
- Configured recovery pullback and the resulting dynamic trigger description.
- RSI/reversal conditions that still prevent the price level from being an unconditional sell.

The report must distinguish a price threshold from an executable sell. The exact first execution price is unknowable in advance because signal and recovery exits require runtime RSI/reversal/trailing conditions, order quantization, available balance, and exchange liquidity.

## Configuration and Portability

- Rename the active local script config from `ok_rsi_buy_sell.yml` to `ok_rsi.yml`.
- Update safe committed fixtures and runtime documentation to the new name and IDs.
- Preserve full independently editable per-asset YAML surfaces.
- Do not expose credentials or the local password-verification file.
- Because runtime `conf/` files are intentionally ignored, the implementation must keep committed safe fixtures/templates synchronized and state clearly which local files were changed.

## Migration Utility

Add a focused script with these modes:

```text
--dry-run       inspect, reconcile, and emit a receipt without target mutation
--apply         apply the exact previously validated plan transactionally
--verify        reopen the target and independently verify rows, seeds, and checksums
```

Required properties:

- Dry-run is the default.
- Source and target paths are explicit and validated; source and target cannot be the same file.
- Apply refuses a running Hummingbot process or an SQLite database with an active writer.
- Backups use SQLite's online backup API and are checksum-verified before mutation.
- Apply uses one transaction for imported rows and inventory seeds.
- A receipt from dry-run is bound by checksum to apply inputs.
- Re-running the same migration is idempotent.
- A changed source, target, mapping, wallet snapshot, or config identity requires a new dry-run receipt.
- Errors are actionable and leave the target unchanged.

## Runtime Verification

Verification uses a separate temporary validation pack with all controllers manually locked. This does not change the required final unlocked state.

1. Confirm no existing Hummingbot strategy process is running.
2. Start `ok_rsi.yml` or its exact validation projection with all controllers locked.
3. Confirm the authenticated OKX connector and five USDC markets become ready.
4. Capture available balances and current mid prices without order actions.
5. Confirm each migrated controller reports the expected held amount, bag count, cost basis, sellable amount, shortfall, and untracked amount.
6. Confirm there are no active executors, order creations, cancellations, fills, repeated 429s, persistent ticker timeouts, or hidden recovery sells.
7. Stop cleanly.
8. Set all final local controller profiles to `manual_kill_switch: false`.
9. Leave the unlocked strategy stopped. Starting it requires separate live-order authorization.

## Tests and Completion Gates

- Fail-first loader test for `ok_rsi.yml` and short IDs.
- Fail-first config audit for USDC identity, no manual kill switches, and no-force-sell invariants.
- Migration tests for schema compatibility, complete history, unchanged provenance, collision rejection, idempotency, checksum binding, and rollback.
- Inventory tests for model decoding, five explicit mappings, unsupported-scope exclusion, balance shortfall, excess untracked balance, fee-aware breakeven, and seed restoration.
- Runtime restoration test proving `StrategyV2Base` supplies the seeded position to the matching OK RSI controller.
- Status/report tests for separated legacy/current history and first-sell economics.
- Focused OK RSI and executor-orchestrator suites under warnings-as-errors.
- Changed-file compilation, scoped pre-commit, YAML parsing, `git diff --check`, and final status/diff review.
- Actual databases pass pre/post SQLite integrity and receipt verification.
- Authenticated runtime evidence is read-only and contains no orders or credential disclosure.

## Rollback

- Keep immutable, checksum-verified backups of all source/intermediate/target databases outside the active `data/` filenames.
- Stop the controller before any rollback.
- Restore the prior script filename and per-asset identities from the recorded patch.
- Move the failed target aside and restore its verified pre-migration backup.
- `rsi_v5.sqlite` remains the legacy source and is never modified by the migration.
- Rollback does not cancel orders or sell inventory; any such live action requires separate approval.

## Acceptance Criteria

- `start --v2 ok_rsi.yml` deterministically selects `data/ok_rsi.sqlite`.
- Five OKX USDC controllers use the approved short IDs and final kill switches are false.
- The complete legacy history exists in the target with unchanged Binance provenance.
- Verified shared inventory restores under current OK RSI IDs with correct amount, breakeven, and fees.
- Unsupported or unexplained inventory is reported, not fabricated.
- No configuration or migration path can force-sell a held bag.
- A fresh read-only runtime displays correct inventory economics and produces the requested average-price/first-sell table.
- No unlocked live strategy run or order action occurs during implementation or verification.
