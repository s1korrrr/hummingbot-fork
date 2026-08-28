"""Fail-closed RSI V5 history and inventory migration for OK RSI."""

import hashlib
import json
import os
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from hummingbot.model import get_declarative_base
from hummingbot.model.inventory_cost import InventoryCost  # noqa: F401
from hummingbot.model.position import Position


class MigrationValidationError(ValueError):
    pass


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
    timestamp: int
    amount: Decimal
    breakeven_price: Decimal
    cum_fees_quote: Decimal


@dataclass(frozen=True)
class InventorySeed:
    source: SourcePosition
    available_balance: Decimal
    total_balance: Decimal
    untracked_balance: Decimal

    @property
    def mapping(self) -> AssetMapping:
        return self.source.mapping

    @property
    def amount(self) -> Decimal:
        return self.source.amount

    @property
    def breakeven_price(self) -> Decimal:
        return self.source.breakeven_price

    @property
    def cum_fees_quote(self) -> Decimal:
        return self.source.cum_fees_quote


@dataclass(frozen=True)
class MigrationPlan:
    source_path: Path
    target_path: Path
    wallet_snapshot_path: Path
    source_sha256: str
    target_pre_sha256: str
    wallet_snapshot_sha256: str
    inventory_seeds: tuple[InventorySeed, ...]


@dataclass(frozen=True)
class MigrationResult:
    source_sha256: str
    target_sha256: str
    imported_rows: int
    skipped_rows: int
    excluded_controller_rows: int
    seed_count: int


@dataclass(frozen=True)
class VerificationReport:
    target_sha256: str
    integrity_check: str
    current_position_count: int
    current_ledger_count: int


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


OK_RSI_ASSET_MAPPINGS = tuple(
    AssetMapping(
        source_controller_id=f"rsi-v5-{asset.lower()}-usdc-1m",
        source_connector="binance",
        source_pair=f"{asset}-USDC",
        target_controller_id=f"ok-rsi-{asset.lower()}",
        target_connector="okx",
        target_pair=f"{asset}-USDC",
    )
    for asset in ("BTC", "ETH", "SOL", "XRP", "BNB")
)

_FAILED_OK_RSI_IDS = {
    f"ok-rsi-{asset.lower()}-usdc-buy-sell" for asset in ("BTC", "ETH", "SOL", "XRP", "BNB")
}
_MISSING_DIGEST = "MISSING"


def file_sha256(path: Path) -> str:
    if not path.exists():
        return _MISSING_DIGEST
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _finite_positive(value: Decimal, label: str) -> Decimal:
    if not value.is_finite() or value <= 0:
        raise MigrationValidationError(f"{label} must be positive and finite")
    return value


def inspect_legacy_source(
    source_path: Path,
    mappings: Sequence[AssetMapping] = OK_RSI_ASSET_MAPPINGS,
) -> tuple[SourcePosition, ...]:
    if not source_path.is_file():
        raise MigrationValidationError(f"source database does not exist: {source_path}")
    engine = create_engine(f"sqlite:///{source_path}")
    try:
        with Session(engine) as session:
            positions = []
            for mapping in mappings:
                matches = session.query(Position).filter(Position.controller_id == mapping.source_controller_id).all()
                if len(matches) != 1:
                    raise MigrationValidationError(
                        f"expected one source Position for {mapping.source_controller_id}, found {len(matches)}"
                    )
                position = matches[0]
                if (
                    position.connector_name != mapping.source_connector
                    or position.trading_pair != mapping.source_pair
                    or position.side != "BUY"
                ):
                    raise MigrationValidationError(f"source scope mismatch for {mapping.source_controller_id}")
                amount = _finite_positive(Decimal(position.amount), f"{mapping.source_pair} amount")
                basis = _finite_positive(Decimal(position.breakeven_price), f"{mapping.source_pair} basis")
                fees = Decimal(position.cum_fees_quote)
                if not fees.is_finite() or fees < 0:
                    raise MigrationValidationError(f"{mapping.source_pair} fees must be non-negative and finite")
                positions.append(SourcePosition(
                    mapping=mapping, position_id=position.id, timestamp=int(position.timestamp), amount=amount,
                    breakeven_price=basis, cum_fees_quote=fees,
                ))
            return tuple(positions)
    finally:
        engine.dispose()


def load_wallet_snapshot(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise MigrationValidationError(f"invalid wallet snapshot: {error}") from error
    if payload.get("schema_version") != 1 or payload.get("connector") != "okx":
        raise MigrationValidationError("wallet snapshot must be schema version 1 from okx")
    if payload.get("source_config_identity") != "ok_rsi.yml":
        raise MigrationValidationError("wallet snapshot must be bound to ok_rsi.yml")
    return payload


def plan_ok_rsi_migration(
    source_path: Path,
    target_path: Path,
    wallet_snapshot_path: Path,
    mappings: Sequence[AssetMapping] = OK_RSI_ASSET_MAPPINGS,
    dust_tolerance: Decimal = Decimal("0"),
) -> MigrationPlan:
    source_path = source_path.resolve()
    target_path = target_path.resolve()
    wallet_snapshot_path = wallet_snapshot_path.resolve()
    if source_path == target_path:
        raise MigrationValidationError("source and target databases must be different files")
    if dust_tolerance < 0 or not dust_tolerance.is_finite():
        raise MigrationValidationError("dust tolerance must be non-negative and finite")
    source_positions = inspect_legacy_source(source_path, mappings)
    wallet = load_wallet_snapshot(wallet_snapshot_path)
    seeds = []
    for source in source_positions:
        pair_payload = wallet.get("pairs", {}).get(source.mapping.target_pair)
        if not isinstance(pair_payload, dict):
            raise MigrationValidationError(f"wallet snapshot is missing {source.mapping.target_pair}")
        try:
            available = Decimal(str(pair_payload["available_base"]))
            total = Decimal(str(pair_payload["total_base"]))
        except (KeyError, TypeError, ValueError) as error:
            raise MigrationValidationError(f"invalid wallet balance for {source.mapping.target_pair}") from error
        if not available.is_finite() or not total.is_finite() or available < 0 or total < available:
            raise MigrationValidationError(f"invalid wallet balance for {source.mapping.target_pair}")
        shortfall = source.amount - total
        if shortfall > dust_tolerance:
            raise MigrationValidationError(
                f"{source.mapping.target_pair} wallet shortfall: tracked={source.amount} total={total} shortfall={shortfall}"
            )
        seeds.append(InventorySeed(
            source=source,
            available_balance=available,
            total_balance=total,
            untracked_balance=max(Decimal("0"), total - source.amount),
        ))
    return MigrationPlan(
        source_path=source_path, target_path=target_path, wallet_snapshot_path=wallet_snapshot_path,
        source_sha256=file_sha256(source_path), target_pre_sha256=file_sha256(target_path),
        wallet_snapshot_sha256=file_sha256(wallet_snapshot_path), inventory_seeds=tuple(seeds),
    )


def initialize_target_database(target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{target_path}")
    try:
        get_declarative_base().metadata.create_all(engine)
    finally:
        engine.dispose()
    with closing(sqlite3.connect(target_path)) as connection:
        connection.execute(
            'insert or ignore into "Metadata" (key, value) values (?, ?)',
            ("local_db_version", "20260327"),
        )


def backup_sqlite_database(source_path: Path, backup_path: Path) -> str:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    source_uri = f"file:{source_path.resolve()}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as source, closing(sqlite3.connect(backup_path)) as target:
        source.backup(target)
    with closing(sqlite3.connect(backup_path)) as connection:
        result = connection.execute("pragma integrity_check").fetchone()[0]
    if result != "ok":
        raise MigrationValidationError(f"backup integrity check failed: {result}")
    return file_sha256(backup_path)


def _primary_key_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in sorted(connection.execute(f'pragma table_info("{table}")'), key=lambda row: row[5]) if row[5]]


def _failed_controller_row(columns: list[str], row: tuple) -> bool:
    payload = dict(zip(columns, row))
    candidates = {str(payload.get("id") or "")}
    try:
        config = json.loads(payload.get("config") or "{}")
        candidates.add(str(config.get("id") or ""))
    except (TypeError, json.JSONDecodeError):
        pass
    return bool(candidates & _FAILED_OK_RSI_IDS)


def _scaled(value: Decimal) -> int:
    return int(value * Decimal("1000000"))


def _insert_seed(connection: sqlite3.Connection, seed: InventorySeed, source_sha256: str) -> bool:
    asset = seed.mapping.target_pair.split("-", 1)[0].lower()
    prefix = f"legacy-rsi-v5:{source_sha256[:16]}:{asset}"
    position_id = f"{prefix}:position"
    ledger_id = f"{prefix}:ledger"
    existing = connection.execute('select id from "Position" where id = ?', (position_id,)).fetchone()
    if existing is not None:
        return False
    quote = seed.amount * seed.breakeven_price
    connection.execute(
        'insert into "Position" (id,controller_id,connector_name,side,trading_pair,timestamp,'
        'volume_traded_quote,amount,breakeven_price,unrealized_pnl_quote,realized_pnl_quote,cum_fees_quote) '
        'values (?,?,?,?,?,?,?,?,?,?,?,?)',
        (position_id, seed.mapping.target_controller_id, seed.mapping.target_connector, "BUY",
         seed.mapping.target_pair, seed.source.timestamp, _scaled(quote), _scaled(seed.amount),
         _scaled(seed.breakeven_price), 0, 0, _scaled(seed.cum_fees_quote)),
    )
    payload = {
        "provenance": "legacy-rsi-v5-inventory-seed", "source_sha256": source_sha256,
        "source_position_id": seed.source.position_id,
        "source_controller_id": seed.mapping.source_controller_id,
        "source_connector": seed.mapping.source_connector, "source_pair": seed.mapping.source_pair,
        "not_an_exchange_fill": True, "client_order_id": ledger_id, "trade_type": "BUY",
        "price": str(seed.breakeven_price), "amount": str(seed.amount),
        "executed_amount_base": str(seed.amount), "executed_amount_quote": str(quote),
        "cumulative_fee_paid_quote": str(seed.cum_fees_quote), "last_update_timestamp": seed.source.timestamp,
    }
    connection.execute(
        'insert into "PositionHoldLedger" (id,controller_id,executor_id,connector_name,trading_pair,'
        'position_side,order_side,timestamp,client_order_id,exchange_order_id,price,amount,'
        'executed_amount_base,executed_amount_quote,cumulative_fee_paid_quote,order_payload) '
        'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (ledger_id, seed.mapping.target_controller_id, prefix, seed.mapping.target_connector,
         seed.mapping.target_pair, None, "BUY", seed.source.timestamp, ledger_id, None,
         _scaled(seed.breakeven_price), _scaled(seed.amount), _scaled(seed.amount), _scaled(quote),
         _scaled(seed.cum_fees_quote), json.dumps(payload, sort_keys=True, separators=(",", ":"))),
    )
    return True


def apply_ok_rsi_migration(plan: MigrationPlan) -> MigrationResult:
    if file_sha256(plan.source_path) != plan.source_sha256:
        raise MigrationValidationError("source checksum changed after planning")
    if file_sha256(plan.wallet_snapshot_path) != plan.wallet_snapshot_sha256:
        raise MigrationValidationError("wallet snapshot checksum changed after planning")
    if file_sha256(plan.target_path) != plan.target_pre_sha256:
        raise MigrationValidationError("target checksum changed after planning")
    initialize_target_database(plan.target_path)
    imported = skipped = excluded = seeded = 0
    source_uri = f"file:{plan.source_path}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as source, closing(sqlite3.connect(plan.target_path)) as target:
        target.execute("begin immediate")
        try:
            source_tables = {
                row[0] for row in source.execute("select name from sqlite_master where type='table'")
                if not row[0].startswith("sqlite_") and row[0] != "Metadata"
            }
            target_tables = {
                row[0] for row in target.execute("select name from sqlite_master where type='table'")
            }
            for table in sorted(source_tables & target_tables):
                source_columns = [row[1] for row in source.execute(f'pragma table_info("{table}")')]
                target_columns = {row[1] for row in target.execute(f'pragma table_info("{table}")')}
                columns = [column for column in source_columns if column in target_columns]
                if not columns:
                    continue
                quoted = ",".join(f'"{column}"' for column in columns)
                placeholders = ",".join("?" for _ in columns)
                key_columns = [column for column in _primary_key_columns(source, table) if column in columns]
                for row in source.execute(f'select {quoted} from "{table}"'):
                    if table == "Controllers" and _failed_controller_row(columns, row):
                        excluded += 1
                        continue
                    if key_columns:
                        row_map = dict(zip(columns, row))
                        predicate = " and ".join(f'"{column}" is ?' for column in key_columns)
                        existing = target.execute(
                            f'select {quoted} from "{table}" where {predicate}',
                            tuple(row_map[column] for column in key_columns),
                        ).fetchone()
                    else:
                        predicate = " and ".join(f'"{column}" is ?' for column in columns)
                        existing = target.execute(
                            f'select {quoted} from "{table}" where {predicate}', row,
                        ).fetchone()
                    if existing is not None:
                        if tuple(existing) != tuple(row):
                            raise MigrationValidationError(f"conflicting row in {table}")
                        skipped += 1
                        continue
                    target.execute(f'insert into "{table}" ({quoted}) values ({placeholders})', tuple(row))
                    imported += 1
            for seed in plan.inventory_seeds:
                if _insert_seed(target, seed, plan.source_sha256):
                    seeded += 1
            target.commit()
        except Exception:
            target.rollback()
            raise
    if file_sha256(plan.source_path) != plan.source_sha256:
        raise MigrationValidationError("source checksum changed during apply")
    return MigrationResult(
        source_sha256=plan.source_sha256, target_sha256=file_sha256(plan.target_path),
        imported_rows=imported, skipped_rows=skipped, excluded_controller_rows=excluded, seed_count=seeded,
    )


def calculate_first_sell_economics(
    *, controller_id: str, pair: str, wallet_available: Decimal, tracked_amount: Decimal,
    untracked_amount: Decimal, breakeven_price: Decimal, min_profit_pct: Decimal,
    bag_recovery_target_pct: Decimal, recovery_partial_exit_fraction: Decimal,
    recovery_pullback_pct: Decimal,
) -> FirstSellEconomics:
    values = (wallet_available, tracked_amount, untracked_amount, breakeven_price, min_profit_pct,
              bag_recovery_target_pct, recovery_partial_exit_fraction, recovery_pullback_pct)
    if not all(value.is_finite() and value >= 0 for value in values):
        raise MigrationValidationError("sell economics inputs must be non-negative and finite")
    if breakeven_price <= 0 or tracked_amount <= 0 or not Decimal("0") < recovery_partial_exit_fraction <= 1:
        raise MigrationValidationError("sell economics require positive basis/amount and a bounded fraction")
    minimum_signal_price = breakeven_price * (Decimal("1") + min_profit_pct)
    recovery_arm_price = breakeven_price * (Decimal("1") + max(min_profit_pct, bag_recovery_target_pct))
    return FirstSellEconomics(
        controller_id=controller_id, pair=pair, wallet_available=wallet_available,
        tracked_amount=tracked_amount, untracked_amount=untracked_amount, breakeven_price=breakeven_price,
        minimum_signal_price=minimum_signal_price, recovery_arm_price=recovery_arm_price,
        recovery_fraction=recovery_partial_exit_fraction,
        first_recovery_amount=min(tracked_amount, wallet_available) * recovery_partial_exit_fraction,
        recovery_pullback_pct=recovery_pullback_pct,
        conditions=(
            "Conditional arm only; no sale is guaranteed at this price.",
            "A sell still requires the configured RSI/reversal or recovery pullback trigger.",
            "Execution remains bounded by tracked inventory, available balance, and exchange quantization.",
        ),
    )


def migration_plan_to_dict(plan: MigrationPlan) -> dict:
    def encode(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {key: encode(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [encode(item) for item in value]
        return value
    return encode(asdict(plan))


def _write_canonical_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    with temporary.open("w", encoding="utf-8") as output:
        output.write(serialized)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def write_dry_run_receipt(plan: MigrationPlan, receipt_path: Path) -> dict:
    payload = {
        "schema_version": 1,
        "status": "dry-run-approved",
        "plan": migration_plan_to_dict(plan),
    }
    _write_canonical_json(receipt_path, payload)
    return payload


def load_migration_receipt(receipt_path: Path) -> dict:
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise MigrationValidationError(f"invalid migration receipt: {error}") from error
    if payload.get("schema_version") != 1 or payload.get("status") not in {"dry-run-approved", "applied"}:
        raise MigrationValidationError("migration receipt has an unsupported schema or status")
    return payload


def assert_receipt_matches_plan(receipt: dict, plan: MigrationPlan) -> None:
    recorded = receipt.get("plan") or {}
    current = migration_plan_to_dict(plan)
    binding_fields = (
        "source_path", "target_path", "wallet_snapshot_path", "source_sha256",
        "target_pre_sha256", "wallet_snapshot_sha256", "inventory_seeds",
    )
    differences = [field for field in binding_fields if recorded.get(field) != current.get(field)]
    if differences:
        raise MigrationValidationError("receipt does not match current plan: " + ", ".join(differences))


def write_applied_receipt(plan: MigrationPlan, result: MigrationResult, receipt_path: Path) -> dict:
    payload = {
        "schema_version": 1,
        "status": "applied",
        "plan": migration_plan_to_dict(plan),
        "result": asdict(result),
    }
    _write_canonical_json(receipt_path, payload)
    return payload


def verify_ok_rsi_migration(target_path: Path, receipt_path: Path) -> VerificationReport:
    receipt = load_migration_receipt(receipt_path)
    if receipt.get("status") != "applied":
        raise MigrationValidationError("verification requires an applied receipt")
    target_path = target_path.resolve()
    recorded_target = Path(receipt["plan"]["target_path"])
    if target_path != recorded_target:
        raise MigrationValidationError("verification target does not match receipt")
    digest = file_sha256(target_path)
    if digest != receipt.get("result", {}).get("target_sha256"):
        raise MigrationValidationError("target checksum does not match applied receipt")
    with closing(sqlite3.connect(f"file:{target_path}?mode=ro", uri=True)) as connection:
        integrity = connection.execute("pragma integrity_check").fetchone()[0]
        positions = connection.execute(
            'select count(*) from "Position" where controller_id like \'ok-rsi-%\''
        ).fetchone()[0]
        ledger = connection.execute(
            'select count(*) from "PositionHoldLedger" where controller_id like \'ok-rsi-%\''
        ).fetchone()[0]
    if integrity != "ok" or positions != len(OK_RSI_ASSET_MAPPINGS) or ledger != len(OK_RSI_ASSET_MAPPINGS):
        raise MigrationValidationError(
            f"target verification failed: integrity={integrity} positions={positions} ledger={ledger}"
        )
    return VerificationReport(
        target_sha256=digest, integrity_check=integrity,
        current_position_count=positions, current_ledger_count=ledger,
    )
