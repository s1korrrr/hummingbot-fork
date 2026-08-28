# OK RSI History and Inventory Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended when explicitly authorized) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the `ok_rsi.yml`/`ok_rsi.sqlite` OKX USDC runtime, preserve the complete RSI V5 history without falsifying venue identity, restore wallet-verified held inventory with correct average prices, and prove that migrated bags cannot be force-sold.

**Architecture:** A pure migration domain module inspects and plans immutable source history, a thin CLI performs checksum-bound backup/apply/verify operations, and a no-order snapshot strategy captures authenticated OKX balances and mid prices. Legacy history remains reporting-only under original Binance IDs; separately labelled position-ledger seeds give current short-ID OK RSI controllers their verified bag amount and cost basis.

**Tech Stack:** Python 3.13, SQLite, SQLAlchemy/Hummingbot models, Pydantic, YAML, `unittest`/`pytest`, Hummingbot Strategy V2 runtime.

**Spec:** `docs/superpowers/specs/2026-08-28-ok-rsi-history-migration-design.md`

## Global Constraints

- Owner repository is `hummingbot/`; root receives only the final nested gitlink in a separately authorized publication step.
- Target stage is live-adjacent repository and local-runtime preparation, not paper/canary/live promotion.
- Do not start the unlocked `ok_rsi.yml`, place/cancel orders, mutate credentials, withdraw, or mutate balances.
- Final identity is `ok_rsi.yml` → `ok_rsi.sqlite`, OKX spot USDC, leverage 1, IDs `ok-rsi-{asset}` for BTC/ETH/SOL/XRP/BNB.
- Final local kill switches are false, but the process remains stopped.
- Source `data/rsi_v5.sqlite` is immutable; every apply needs verified backups, a checksum-bound dry run, one transaction, idempotency, and pre/post integrity checks.
- Preserve original Binance history exactly. Never relabel legacy fills/orders/executors as OKX.
- Inventory seeds are not fills. They require explicit source-position, ledger/fill, wallet-balance, and cost-basis agreement.
- Never force-sell: no stop loss, time limit, fixed take profit, shutdown liquidation, below-breakeven signal/recovery sale, or sell above available base balance.
- Secrets and the local `conf/.password_verification copy` must remain unread, unmodified, untracked, and absent from outputs.

## File Structure

- Create `hummingbot/strategy_v2/migrations/__init__.py`: package marker only.
- Create `hummingbot/strategy_v2/migrations/ok_rsi_history.py`: pure models, source inspection, wallet reconciliation, history-copy plan, inventory seeds, receipts, apply, verification, and sell-economics calculations.
- Create `bin/ok_rsi_history_migration.py`: argument parsing and calls into the pure migration module; dry-run default.
- Create `scripts/ok_rsi_wallet_snapshot.py`: no-order authenticated balance/mid-price snapshot strategy.
- Create `test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py`: migration unit/integration tests using temporary SQLite databases.
- Create `test/test_ok_rsi_wallet_snapshot.py`: pure snapshot and no-order lifecycle tests.
- Modify `controllers/directional_trading/ok_rsi.py`: optional legacy controller IDs and reporting-only history surface.
- Modify `hummingbot/strategy_v2/executors/executor_orchestrator.py`: compute isolated legacy performance without injecting legacy executors into live decisions or positions.
- Modify `hummingbot/strategy/strategy_v2_base.py`: project the separate legacy report to controller status.
- Modify `test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py`: legacy/current isolation and restored-seed coverage.
- Modify `test/test_ok_rsi_controller.py`: new config name, short IDs, kill-switch/no-force-sell audit, status, and first-sell economics.
- Rename `test/fixtures/ok_rsi/scripts/ok_rsi_buy_sell.yml` to `test/fixtures/ok_rsi/scripts/ok_rsi.yml` and update five committed controller fixtures.
- Rename local ignored `conf/scripts/ok_rsi_buy_sell.yml` to `conf/scripts/ok_rsi.yml` and update five local controller YAMLs after code tests pass.
- Create local ignored `conf/scripts/ok_rsi_wallet_snapshot.yml` for the no-order snapshot run.
- Modify `docs/rsi-v5/ok-rsi-runtime.md`, `PLAN.md`, `TODO.md`, and `MEMORY.md` during closeout.

---

### Task 1: Lock the final configuration identity and no-force-sell contract

**Files:**
- Modify: `test/test_ok_rsi_controller.py`
- Rename: `test/fixtures/ok_rsi/scripts/ok_rsi_buy_sell.yml` → `test/fixtures/ok_rsi/scripts/ok_rsi.yml`
- Modify: `test/fixtures/ok_rsi/controllers/ok_rsi_{btc,eth,sol,xrp,bnb}.yml`
- Modify later after RED: local `conf/scripts/ok_rsi_buy_sell.yml` and five local `conf/controllers/ok_rsi_*.yml`

**Interfaces:**
- Consumes: existing `V2WithControllersConfig.load_controller_configs()` and `OKRSIControllerConfig` validation.
- Produces: canonical config identity and per-controller legacy mapping used by later tasks.

- [ ] **Step 1: Write the failing loader and invariant tests**

Add tests that load `ok_rsi.yml` and assert exactly:

```python
expected = {
    "ok-rsi-btc": ("BTC-USDC", "rsi-v5-btc-usdc-1m"),
    "ok-rsi-eth": ("ETH-USDC", "rsi-v5-eth-usdc-1m"),
    "ok-rsi-sol": ("SOL-USDC", "rsi-v5-sol-usdc-1m"),
    "ok-rsi-xrp": ("XRP-USDC", "rsi-v5-xrp-usdc-1m"),
    "ok-rsi-bnb": ("BNB-USDC", "rsi-v5-bnb-usdc-1m"),
}
```

For every loaded controller assert:

```python
assert config.connector_name == "okx"
assert config.leverage == 1
assert config.manual_kill_switch is False
assert config.sell_only_if_profitable is True
assert config.stop_loss is None
assert config.take_profit is None
assert config.time_limit is None
assert config.early_stop_keep_position is True
assert config.legacy_controller_ids == [expected[config.id][1]]
```

- [ ] **Step 2: Run RED**

Run:

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/test_ok_rsi_controller.py::OKRSIControllerTest::test_buy_sell_script_uses_canonical_ok_rsi_identity \
  test/test_ok_rsi_controller.py::OKRSIControllerTest::test_buy_sell_profiles_never_force_sell
```

Expected: FAIL because `ok_rsi.yml`, short IDs, false XRP kill switch, and `legacy_controller_ids` do not yet all exist.

- [ ] **Step 3: Implement the minimal config/model change**

Add to `OKRSIControllerConfig`:

```python
legacy_controller_ids: List[str] = Field(default_factory=list)
```

Reject blanks and duplicates while allowing one explicit legacy ID per current profile. Rename the committed script fixture, set short IDs, add the corresponding legacy ID, and set every final manual kill switch false. Preserve every unrelated per-asset setting.

- [ ] **Step 4: Run GREEN and loader smoke**

Run the two focused tests plus:

```bash
conda run -n hummingbot python -W error -m pytest -q test/test_ok_rsi_controller.py -k 'buy_sell or config'
```

Expected: focused tests pass with no warnings.

- [ ] **Step 5: Commit the config contract**

```bash
git add controllers/directional_trading/ok_rsi.py test/test_ok_rsi_controller.py test/fixtures/ok_rsi
git commit -m "refactor(controller): simplify OK RSI runtime identity"
```

---

### Task 2: Add a no-order authenticated wallet snapshot strategy

**Files:**
- Create: `scripts/ok_rsi_wallet_snapshot.py`
- Create: `test/test_ok_rsi_wallet_snapshot.py`

**Interfaces:**
- Produces: `WalletSnapshot` JSON consumed and checksum-bound by the migration planner.
- Output fields: schema version, capture timestamp, connector, per-pair available base, total base, available quote, mid price, trading-rule minimums, and source config identity.

- [ ] **Step 1: Write failing pure snapshot tests**

Define the wished-for API in tests:

```python
snapshot = build_wallet_snapshot(
    connector=fake_okx,
    trading_pairs=["BTC-USDC", "ETH-USDC"],
    captured_at_ms=1_787_940_000_000,
)
assert snapshot["connector"] == "okx"
assert snapshot["pairs"]["BTC-USDC"]["available_base"] == "0.25"
assert snapshot["pairs"]["BTC-USDC"]["mid_price"] == "65000"
```

Also assert that the strategy class exposes no call to `buy`, `sell`, `cancel`, `create_order`, or executor actions and writes once using an atomic temporary-path replacement.

- [ ] **Step 2: Run RED**

```bash
conda run -n hummingbot python -W error -m pytest -q test/test_ok_rsi_wallet_snapshot.py
```

Expected: collection/import failure because the snapshot script does not exist.

- [ ] **Step 3: Implement the minimal snapshot strategy**

Create:

```python
class OKRSIWalletSnapshotConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []
    connector_name: Literal["okx"] = "okx"
    trading_pairs: List[str]
    output_path: str = "data/ok_rsi_wallet_snapshot.json"

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets["okx"] = set(self.trading_pairs)
        return markets
```

`OKRSIWalletSnapshot` reads balances, order-book mid prices, and trading rules only after the connector is ready; it contains no order/cancel methods and atomically writes canonical JSON once.

- [ ] **Step 4: Run GREEN and safety inspection**

```bash
conda run -n hummingbot python -W error -m pytest -q test/test_ok_rsi_wallet_snapshot.py
rg -n "\.buy\(|\.sell\(|\.cancel\(|CreateExecutorAction|StopExecutorAction" scripts/ok_rsi_wallet_snapshot.py
```

Expected: tests pass; `rg` returns no order-capable call sites.

- [ ] **Step 5: Commit the snapshot utility**

```bash
git add scripts/ok_rsi_wallet_snapshot.py test/test_ok_rsi_wallet_snapshot.py
git commit -m "feat(controller): add read-only OKX wallet snapshot"
```

---

### Task 3: Build source inspection and wallet-reconciled migration planning

**Files:**
- Create: `hummingbot/strategy_v2/migrations/__init__.py`
- Create: `hummingbot/strategy_v2/migrations/ok_rsi_history.py`
- Create: `test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class AssetMapping:
    source_controller_id: str
    source_connector: str
    source_pair: str
    target_controller_id: str
    target_connector: str
    target_pair: str

@dataclass(frozen=True)
class SourcePosition:
    mapping: AssetMapping
    position_id: str
    amount: Decimal
    breakeven_price: Decimal
    cum_fees_quote: Decimal

@dataclass(frozen=True)
class InventorySeed:
    mapping: AssetMapping
    amount: Decimal
    breakeven_price: Decimal
    cum_fees_quote: Decimal
    available_balance: Decimal
    untracked_balance: Decimal

@dataclass(frozen=True)
class MigrationPlan:
    source_sha256: str
    target_pre_sha256: str
    wallet_snapshot_sha256: str
    table_actions: tuple[TableImport, ...]
    inventory_seeds: tuple[InventorySeed, ...]
```

- [ ] **Step 1: Write failing model-decoding and reconciliation tests**

Create temporary source SQLite fixtures through Hummingbot SQLAlchemy models so `SqliteDecimal(6)` values are decoded as economic decimals. Test:

- Exact five mappings.
- EUR and USDC-PLN exclusion.
- `Position` amount/breakeven/fee decoding.
- Independent ledger/fill net-amount reconciliation.
- Wallet balance equal to tracked amount.
- Excess wallet balance becomes `untracked_balance`.
- Wallet shortfall beyond dust raises `MigrationValidationError`.
- NaN, infinity, zero/negative basis, duplicate source scope, and mismatched reconstruction fail.

- [ ] **Step 2: Run RED**

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py -k 'inspect or reconcile or mapping'
```

Expected: import failure because the migration module does not exist.

- [ ] **Step 3: Implement source inspection and planning**

Implement these pure entry points:

```python
def inspect_legacy_source(source_path: Path) -> LegacySourceSnapshot: ...

def load_wallet_snapshot(path: Path) -> WalletSnapshot: ...

def plan_ok_rsi_migration(
    source_path: Path,
    target_path: Path,
    wallet_snapshot_path: Path,
    mappings: Sequence[AssetMapping] = OK_RSI_ASSET_MAPPINGS,
    dust_tolerance: Decimal = Decimal("0"),
) -> MigrationPlan: ...
```

Use SQLAlchemy model sessions for economic decoding and raw SQLite schema inspection for canonical row/key fingerprints. Validate exact config, connector, pair, side, finite decimals, and source/target path separation.

- [ ] **Step 4: Run GREEN**

Run the focused tests and the existing decimal/model tests:

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py \
  test/hummingbot/model/test_trade_fill.py
```

- [ ] **Step 5: Commit the planner**

```bash
git add hummingbot/strategy_v2/migrations test/hummingbot/strategy_v2/migrations
git commit -m "feat(persistence): plan OK RSI history migration"
```

---

### Task 4: Implement checksum-bound history import, backup, receipt, and verification

**Files:**
- Modify: `hummingbot/strategy_v2/migrations/ok_rsi_history.py`
- Create: `bin/ok_rsi_history_migration.py`
- Modify: `test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py`

**Interfaces:**
- Produces:

```python
def initialize_target_database(target_path: Path) -> None: ...
def backup_sqlite_database(source_path: Path, backup_path: Path) -> DatabaseDigest: ...
def write_dry_run_receipt(plan: MigrationPlan, receipt_path: Path) -> MigrationReceipt: ...
def apply_ok_rsi_migration(plan: MigrationPlan, receipt_path: Path) -> MigrationReceipt: ...
def verify_ok_rsi_migration(target_path: Path, receipt_path: Path) -> VerificationReport: ...
```

- [ ] **Step 1: Write failing transactional/import tests**

Cover:

- Target schema initialization includes every current Hummingbot table.
- Common legacy tables copy raw values unchanged.
- `Metadata` schema-version values are not overwritten by source metadata.
- The five accidental `ok-rsi-*-usdc-buy-sell` controller-only rows from the failed source experiment are recorded as excluded metadata, while all RSI V5 controller versions remain.
- Identical duplicate rows are skipped idempotently.
- Same-key/different-content rows reject before mutation.
- Injected mid-transaction failure rolls back every imported row and seed.
- Source checksum is unchanged after dry-run and apply.
- Changed source/target/wallet snapshot invalidates the dry-run receipt.
- Verified online backup checksum equals the source snapshot checksum.

- [ ] **Step 2: Run RED**

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py -k 'import or receipt or backup or rollback or idempotent'
```

Expected: failures for missing import/apply/verify functions.

- [ ] **Step 3: Implement one-transaction apply and CLI**

The CLI contract is:

```bash
python bin/ok_rsi_history_migration.py \
  --source data/rsi_v5.sqlite \
  --target data/ok_rsi.sqlite \
  --wallet-snapshot data/ok_rsi_wallet_snapshot.json \
  --receipt data/ok_rsi_migration_receipt.json \
  --dry-run
```

`--dry-run` is the default. `--apply` requires the matching receipt. `--verify` performs no mutation. Use SQLite online backup for recovery files and `BEGIN IMMEDIATE` for the target transaction after proving there is no active writer. Never use `INSERT OR REPLACE`; compare canonical rows and insert only absent identical-key rows.

- [ ] **Step 4: Run GREEN and CLI smoke on temporary databases**

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py
conda run -n hummingbot python bin/ok_rsi_history_migration.py --help
```

- [ ] **Step 5: Commit the importer**

```bash
git add hummingbot/strategy_v2/migrations/ok_rsi_history.py \
  bin/ok_rsi_history_migration.py \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py
git commit -m "feat(persistence): import legacy RSI history safely"
```

---

### Task 5: Restore verified seeds and isolate legacy performance from live decisions

**Files:**
- Modify: `hummingbot/strategy_v2/migrations/ok_rsi_history.py`
- Modify: `hummingbot/strategy_v2/executors/executor_orchestrator.py`
- Modify: `hummingbot/strategy/strategy_v2_base.py`
- Modify: `controllers/directional_trading/ok_rsi.py`
- Modify: `test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py`
- Modify: `test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py`
- Modify: `test/test_ok_rsi_controller.py`

**Interfaces:**
- Inventory seed IDs are deterministic: `legacy-rsi-v5:{source_sha256[:16]}:{asset}:position` and `...:{asset}:ledger`.
- `ExecutorOrchestrator.get_legacy_performance_report(controller_id)` returns a separate `PerformanceReport`; it never alters `executors_info`, `positions_held`, cooldowns, or current performance.

- [ ] **Step 1: Write failing seed-restoration and isolation tests**

Create a migrated BTC seed with known economics:

```python
amount = Decimal("0.004057")
breakeven = Decimal("65000")
fees = Decimal("1.25")
```

Assert a new `ExecutorOrchestrator` restores exactly one `ok-rsi-btc` BUY position with that amount/breakeven/fee, while old `rsi-v5-btc-usdc-1m` executors appear only in the legacy performance report. Assert current `executors_info` is empty and current consecutive-loss/cooldown calculations are unchanged.

- [ ] **Step 2: Run RED**

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py -k 'legacy or migrated_seed' \
  test/test_ok_rsi_controller.py -k 'legacy_history'
```

- [ ] **Step 3: Implement deterministic position and ledger seeds**

Create both `Position` and `PositionHoldLedger` rows through SQLAlchemy models. The ledger `order_payload` must include:

```python
{
    "provenance": "legacy-rsi-v5-inventory-seed",
    "source_sha256": plan.source_sha256,
    "source_position_id": source.position_id,
    "source_controller_id": mapping.source_controller_id,
    "source_connector": "binance",
    "source_pair": mapping.source_pair,
    "not_an_exchange_fill": True,
}
```

Use the existing held-position semantics so fee and breakeven are not double-counted. Add separate legacy-performance aggregation keyed only by the optional `legacy_controller_ids`; never append legacy executor objects to the current controller's `executors_info`.

- [ ] **Step 4: Run GREEN plus restoration regression**

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/test_ok_rsi_controller.py -k 'legacy or inventory or recovery or sell'
```

- [ ] **Step 5: Commit restoration and isolation**

```bash
git add hummingbot/strategy_v2/migrations/ok_rsi_history.py \
  hummingbot/strategy_v2/executors/executor_orchestrator.py \
  hummingbot/strategy/strategy_v2_base.py \
  controllers/directional_trading/ok_rsi.py \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/test_ok_rsi_controller.py
git commit -m "feat(controller): restore verified legacy inventory"
```

---

### Task 6: Add legacy/status and first-sell economics reporting

**Files:**
- Modify: `hummingbot/strategy_v2/migrations/ok_rsi_history.py`
- Modify: `controllers/directional_trading/ok_rsi.py`
- Modify: `controllers/directional_trading/hl_rsi.py` only if the shared status helper is the established boundary
- Modify: `test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py`
- Modify: `test/test_ok_rsi_controller.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class FirstSellEconomics:
    controller_id: str
    pair: str
    wallet_available: Decimal
    tracked_amount: Decimal
    untracked_amount: Decimal
    breakeven_price: Decimal
    minimum_signal_price: Decimal
    recovery_arm_price: Decimal
    recovery_fraction: Decimal
    first_recovery_amount: Decimal
    recovery_pullback_pct: Decimal
    conditions: tuple[str, ...]
```

- [ ] **Step 1: Write failing economics and status tests**

For a known BTC bag/config, assert:

```python
assert report.minimum_signal_price == breakeven * (Decimal("1") + min_profit_pct)
assert report.recovery_arm_price == breakeven * (
    Decimal("1") + max(min_profit_pct, bag_recovery_target_pct)
)
assert report.first_recovery_amount == min(tracked, wallet) * recovery_partial_exit_fraction
```

Assert status labels legacy history separately and says that the arm price is conditional, not a guaranteed sell. Assert no status/report helper emits an executor action.

- [ ] **Step 2: Run RED**

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py -k 'sell_economics' \
  test/test_ok_rsi_controller.py -k 'legacy_history_status or first_sell'
```

- [ ] **Step 3: Implement pure calculations and compact status projection**

Reuse the controller's existing min-profit, recovery-target, recovery-fraction, balance, and cost-basis semantics. Do not create a second sell decision engine. The receipt/CLI report uses the pure dataclass; controller status displays the same values from current processed state and separate legacy performance.

- [ ] **Step 4: Run GREEN and complete focused warning-clean suite**

```bash
conda run -n hummingbot python -W error -m pytest -q \
  test/test_ok_rsi_controller.py \
  test/test_hl_rsi_controller.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py \
  test/test_ok_rsi_wallet_snapshot.py
```

- [ ] **Step 5: Commit reporting**

```bash
git add controllers/directional_trading/ok_rsi.py controllers/directional_trading/hl_rsi.py \
  hummingbot/strategy_v2/migrations/ok_rsi_history.py \
  test/test_ok_rsi_controller.py test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py
git commit -m "feat(controller): report migrated bag economics"
```

---

### Task 7: Rename local runtime files and migrate the real databases safely

**Files/artifacts:**
- Rename local ignored: `conf/scripts/ok_rsi_buy_sell.yml` → `conf/scripts/ok_rsi.yml`
- Modify local ignored: `conf/controllers/ok_rsi_{btc,eth,sol,xrp,bnb}.yml`
- Create local ignored: `conf/scripts/ok_rsi_wallet_snapshot.yml`
- Read immutable: `data/rsi_v5.sqlite`, `data/ok_rsi_buy_sell.sqlite`
- Create: `data/ok_rsi.sqlite`, backup files, wallet snapshot, dry-run receipt, apply receipt, and final report

**Interfaces:**
- Consumes the tested CLI and no-order snapshot strategy.
- Produces the operator's real verified `ok_rsi.sqlite` and per-asset economics table.

- [ ] **Step 1: Prove processes are stopped and freeze recovery evidence**

Run read-only process/status checks. Record SHA-256 and SQLite integrity for both existing databases. Use the migration CLI's online backup mode to create timestamped backups under `data/backups/ok_rsi_history_migration_20260828/`; verify backup hashes before proceeding.

- [ ] **Step 2: Rename and validate local configs**

Rename the active script file, apply short IDs, add legacy IDs, retain USDC pairs, and keep kill switches true only for the pre-migration locked validation if using the real controller pack. Prefer the dedicated no-order snapshot strategy so final controller profiles can be written once with kill switches false.

Validate:

```bash
conda run -n hummingbot python - <<'PY'
from pathlib import Path
import yaml
for path in [Path("conf/scripts/ok_rsi.yml"), *sorted(Path("conf/controllers").glob("ok_rsi_*.yml"))]:
    yaml.safe_load(path.read_text())
    print(path)
PY
```

- [ ] **Step 3: Capture the authenticated no-order wallet snapshot**

Create `conf/scripts/ok_rsi_wallet_snapshot.yml` for the five USDC pairs and `data/ok_rsi_wallet_snapshot.json`. Start only the snapshot strategy through the actual Hummingbot loader, wait for the canonical JSON, then stop cleanly. Verify database row counts before/after prove zero new `Order`, `TradeFill`, `Executors`, and `PositionHoldLedger` rows.

- [ ] **Step 4: Run real dry-run and review every asset**

```bash
conda run -n hummingbot python bin/ok_rsi_history_migration.py \
  --source data/rsi_v5.sqlite \
  --target data/ok_rsi.sqlite \
  --wallet-snapshot data/ok_rsi_wallet_snapshot.json \
  --receipt data/ok_rsi_migration_receipt.json \
  --dry-run
```

Require five accepted mappings, zero wallet shortfalls, exact source row counts, source checksum stability, and no unresolved collisions. If any wallet is below its legacy position, stop and report the exact asset/amount discrepancy; do not apply.

- [ ] **Step 5: Apply and independently verify**

```bash
conda run -n hummingbot python bin/ok_rsi_history_migration.py \
  --source data/rsi_v5.sqlite \
  --target data/ok_rsi.sqlite \
  --wallet-snapshot data/ok_rsi_wallet_snapshot.json \
  --receipt data/ok_rsi_migration_receipt.json \
  --apply

conda run -n hummingbot python bin/ok_rsi_history_migration.py \
  --target data/ok_rsi.sqlite \
  --receipt data/ok_rsi_migration_receipt.json \
  --verify
```

Verify source SHA unchanged, target integrity, imported per-table counts, five position/ledger seeds, no active current executors, and exact receipt binding.

- [ ] **Step 6: Run locked restoration smoke and leave final runtime stopped**

Temporarily project the same five controllers with manual kill switches true, start against `ok_rsi.sqlite`, capture status, and prove amount/breakeven/fees/sellable/shortfall/untracked values. Audit target order/fill/executor counts for zero mutations, stop cleanly, restore all final local kill switches to false, and do not start the unlocked pack.

- [ ] **Step 7: Generate the requested final table**

Use verified target positions, wallet snapshot, live mid prices, and exact YAMLs to print BTC/ETH/SOL/XRP/BNB wallet, tracked, untracked, average, minimum signal price, recovery arm, raw/quantized first partial amount, pullback, and remaining RSI/reversal conditions.

---

### Task 8: Full validation, documentation, review, and closeout

**Files:**
- Modify: `docs/rsi-v5/ok-rsi-runtime.md`
- Modify: `PLAN.md`
- Modify: `TODO.md`
- Modify: `MEMORY.md`
- Append applicable repository daily/lesson stores per `AGENTS.md`

**Interfaces:**
- Produces final local handoff, exact commands, rollback, residual risks, and authority boundary.

- [ ] **Step 1: Update runtime documentation**

Document `start --v2 ok_rsi.yml`, automatic `ok_rsi.sqlite`, short IDs, legacy/current reporting split, migration commands, backups, rollback, and the fact that false kill switches do not authorize an agent to start live trading.

- [ ] **Step 2: Run the full scoped gate**

```bash
conda run -n hummingbot python -m py_compile \
  controllers/directional_trading/ok_rsi.py \
  scripts/ok_rsi_wallet_snapshot.py \
  bin/ok_rsi_history_migration.py \
  hummingbot/strategy_v2/migrations/ok_rsi_history.py

conda run -n hummingbot python -W error -m pytest -q \
  test/test_ok_rsi_controller.py \
  test/test_hl_rsi_controller.py \
  test/test_ok_rsi_wallet_snapshot.py \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/hummingbot/connector/test_markets_recorder.py

conda run -n hummingbot pre-commit run --files \
  controllers/directional_trading/ok_rsi.py \
  controllers/directional_trading/hl_rsi.py \
  hummingbot/strategy/strategy_v2_base.py \
  hummingbot/strategy_v2/executors/executor_orchestrator.py \
  hummingbot/strategy_v2/migrations/__init__.py \
  hummingbot/strategy_v2/migrations/ok_rsi_history.py \
  scripts/ok_rsi_wallet_snapshot.py \
  bin/ok_rsi_history_migration.py \
  test/test_ok_rsi_controller.py \
  test/test_ok_rsi_wallet_snapshot.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py \
  test/fixtures/ok_rsi/scripts/ok_rsi.yml \
  test/fixtures/ok_rsi/controllers/ok_rsi_btc.yml \
  test/fixtures/ok_rsi/controllers/ok_rsi_eth.yml \
  test/fixtures/ok_rsi/controllers/ok_rsi_sol.yml \
  test/fixtures/ok_rsi/controllers/ok_rsi_xrp.yml \
  test/fixtures/ok_rsi/controllers/ok_rsi_bnb.yml
git diff --check
git status --short
```

- [ ] **Step 3: Review exact diff and operational artifacts**

Check no secrets, private balances, database files, receipts, backups, or ignored local configs are staged. Confirm no force-sell path, no provenance rewrite, no legacy/current risk contamination, no hidden unlocked start, and no database mutation outside the approved target/backups.

- [ ] **Step 4: Commit closeout**

```bash
git add docs/rsi-v5/ok-rsi-runtime.md PLAN.md TODO.md MEMORY.md docs/agent-memory/agent_lessons.jsonl
git commit -m "docs(controller): record OK RSI history migration"
```

- [ ] **Step 5: Verified handoff**

Report exact commits, changed local ignored config paths, backup/receipt paths and checksums, tests and runtime evidence, database row counts, per-asset economics table, final stopped process state, and any wallet discrepancy that prevented a seed. Do not claim paper/canary/live readiness or start the unlocked controller.
