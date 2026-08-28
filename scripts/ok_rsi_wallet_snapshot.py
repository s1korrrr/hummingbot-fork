"""One-shot, read-only OKX balance snapshot for OK RSI migration."""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Literal

from pydantic import Field, field_validator

from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase


def _decimal_text(value, *, field_name: str, positive: bool = False) -> str:
    decimal_value = Decimal(str(value))
    if not decimal_value.is_finite() or decimal_value < 0 or (positive and decimal_value <= 0):
        raise ValueError(f"{field_name} must be {'positive' if positive else 'non-negative'} and finite")
    normalized = decimal_value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def build_wallet_snapshot(
    *,
    connector,
    trading_pairs: List[str],
    captured_at_ms: int,
    source_config_identity: str,
) -> dict:
    if connector.name != "okx":
        raise ValueError("wallet snapshot requires the authenticated OKX connector")
    pairs = {}
    for trading_pair in trading_pairs:
        base, quote = trading_pair.split("-", 1)
        trading_rule = connector.trading_rules[trading_pair]
        pairs[trading_pair] = {
            "available_base": _decimal_text(
                connector.get_available_balance(base), field_name=f"{base} available balance"
            ),
            "total_base": _decimal_text(connector.get_balance(base), field_name=f"{base} total balance"),
            "available_quote": _decimal_text(
                connector.get_available_balance(quote), field_name=f"{quote} available balance"
            ),
            "mid_price": _decimal_text(
                connector.get_mid_price(trading_pair), field_name=f"{trading_pair} mid price", positive=True
            ),
            "min_order_size": _decimal_text(
                trading_rule.min_order_size, field_name=f"{trading_pair} minimum order size"
            ),
            "min_notional_size": _decimal_text(
                trading_rule.min_notional_size, field_name=f"{trading_pair} minimum notional"
            ),
        }
    captured_at = datetime.fromtimestamp(captured_at_ms / 1000, tz=timezone.utc)
    return {
        "schema_version": 1,
        "captured_at_ms": captured_at_ms,
        "captured_at": captured_at.isoformat().replace("+00:00", "Z"),
        "connector": "okx",
        "source_config_identity": source_config_identity,
        "pairs": pairs,
    }


def write_wallet_snapshot(path: Path, snapshot: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    serialized = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    with temporary_path.open("w", encoding="utf-8") as output:
        output.write(serialized)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary_path, path)


class OKRSIWalletSnapshotConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = Field(default_factory=list, exclude=True)
    connector_name: Literal["okx"] = "okx"
    trading_pairs: List[str]
    output_path: str = "data/ok_rsi_wallet_snapshot.json"
    source_config_identity: str = "ok_rsi.yml"

    @field_validator("trading_pairs", mode="before")
    @classmethod
    def validate_trading_pairs(cls, value):
        if not isinstance(value, list) or not value:
            raise ValueError("trading_pairs must be a non-empty list")
        normalized = [str(pair).strip().upper() for pair in value]
        if len(normalized) != len(set(normalized)):
            raise ValueError("trading_pairs must be unique")
        if any(len(pair.split("-")) != 2 or not pair.endswith("-USDC") for pair in normalized):
            raise ValueError("wallet snapshot pairs must use BASE-USDC format")
        return normalized

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.connector_name] = set(self.trading_pairs)
        return markets


class OKRSIWalletSnapshot(StrategyV2Base):
    """Capture one authenticated snapshot without producing executor or order actions."""

    def __init__(self, connectors: Dict, config: OKRSIWalletSnapshotConfig):
        super().__init__(connectors, config)
        self.config = config
        self._snapshot_written = False

    def on_tick(self):
        if self._snapshot_written:
            return
        connector = self.connectors[self.config.connector_name]
        if not connector.ready:
            return
        snapshot = build_wallet_snapshot(
            connector=connector,
            trading_pairs=self.config.trading_pairs,
            captured_at_ms=int(self.current_timestamp * 1000),
            source_config_identity=self.config.source_config_identity,
        )
        write_wallet_snapshot(Path(self.config.output_path), snapshot)
        self._snapshot_written = True
        self.logger().info(f"Wrote read-only OK RSI wallet snapshot to {self.config.output_path}")
