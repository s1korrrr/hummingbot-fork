from __future__ import annotations

import inspect
import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Literal, Mapping, Optional, Set

import pandas as pd
from pydantic import Field, field_validator, model_validator

# isort: off
try:  # Hummingbot mounts this tree as the top-level controllers package.
    from controllers.directional_trading.hyperliquid_portfolio_rsi import (
        build_pair_policies_from_wallet,
        build_portfolio_plan,
    )
    from controllers.directional_trading.hyperliquid_portfolio_rsi_features import (
        PairFeatureConfig,
        build_pair_signal_snapshot,
    )
    from controllers.shared.hyperliquid_portfolio_signal_client import HyperliquidPortfolioSignalClient
except ImportError:  # Hummingbot API imports its checked-in mirror through bots.controllers.
    from bots.controllers.directional_trading.hyperliquid_portfolio_rsi import (
        build_pair_policies_from_wallet,
        build_portfolio_plan,
    )
    from bots.controllers.directional_trading.hyperliquid_portfolio_rsi_features import (
        PairFeatureConfig,
        build_pair_signal_snapshot,
    )
    from bots.controllers.shared.hyperliquid_portfolio_signal_client import HyperliquidPortfolioSignalClient
# isort: on

from hummingbot.core.data_type.common import MarketDict, PositionAction, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction

ZERO = Decimal("0")
CONTROLLER_STATE_SCHEMA = "rsibot.hyperliquid_portfolio_rsi.controller_state.v1"
PORTFOLIO_SIGNAL_SCHEMA = "rsibot.hyperliquid_portfolio_rsi.portfolio_signals.v1"
HYPERLIQUID_SPOT_CANDLE_INTERVALS = frozenset({
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d",
})


def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _iso_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp_seconds(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except (TypeError, ValueError):
            return None
    while abs(numeric) >= 100_000_000_000:
        numeric /= 1_000.0
    return numeric if math.isfinite(numeric) else None


def _pair_for_asset(asset: str, quote_asset: str) -> str:
    return f"{asset.strip().upper()}-{quote_asset.strip().upper()}"


class HyperliquidPortfolioRSIControllerConfig(ControllerConfigBase):
    controller_name: str = "hyperliquid_portfolio_rsi_v2"
    controller_type: str = "directional_trading"

    wallet_connector_name: str = Field(default="hyperliquid", json_schema_extra={"is_updatable": False})
    execution_connector_name: str = Field(
        default="hyperliquid_paper_trade",
        json_schema_extra={"is_updatable": False},
    )
    quote_asset: str = Field(default="USDC", json_schema_extra={"is_updatable": False})
    buy_sell_assets: List[str] = Field(default_factory=list, json_schema_extra={"is_updatable": False})
    protected_assets: List[str] = Field(default_factory=list, json_schema_extra={"is_updatable": True})
    pair_policy_overrides: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        json_schema_extra={"is_updatable": True},
    )

    mode: Literal["shadow", "paper"] = Field(default="shadow", json_schema_extra={"is_updatable": True})
    paper_actions_enabled: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    paper_bootstrap_from_wallet: bool = Field(default=True, json_schema_extra={"is_updatable": False})
    order_timeout_seconds: float = Field(default=60.0, ge=5.0, json_schema_extra={"is_updatable": True})
    interval: str = Field(default="1m", json_schema_extra={"is_updatable": False})
    max_records: int = Field(default=200, ge=50, json_schema_extra={"is_updatable": True})
    order_book_depth_levels: int = Field(default=20, ge=1, le=100, json_schema_extra={"is_updatable": True})

    rsi_length: int = Field(default=10, ge=2, json_schema_extra={"is_updatable": False})
    rsi_buy_threshold: float = Field(default=31.0, gt=0.0, lt=100.0, json_schema_extra={"is_updatable": True})
    rsi_sell_threshold: float = Field(default=75.0, gt=0.0, lt=100.0, json_schema_extra={"is_updatable": True})
    bb_length: int = Field(default=20, ge=5, json_schema_extra={"is_updatable": False})
    bb_std: float = Field(default=2.0, ge=0.5, json_schema_extra={"is_updatable": True})
    macd_fast: int = Field(default=12, ge=2, json_schema_extra={"is_updatable": False})
    macd_slow: int = Field(default=26, ge=5, json_schema_extra={"is_updatable": False})
    macd_signal: int = Field(default=9, ge=2, json_schema_extra={"is_updatable": False})
    ema_fast: int = Field(default=9, ge=2, json_schema_extra={"is_updatable": False})
    ema_slow: int = Field(default=21, ge=2, json_schema_extra={"is_updatable": False})
    atr_length: int = Field(default=14, ge=2, json_schema_extra={"is_updatable": False})
    min_atr_pct: float = Field(default=0.0002, ge=0.0, json_schema_extra={"is_updatable": True})
    max_atr_pct: float = Field(default=0.03, gt=0.0, json_schema_extra={"is_updatable": True})
    max_candle_age_intervals: float = Field(default=2.5, gt=0.0, json_schema_extra={"is_updatable": True})
    max_book_age_seconds: float = Field(default=15.0, gt=0.0, json_schema_extra={"is_updatable": True})
    max_spread_bps: float = Field(default=30.0, ge=0.0, json_schema_extra={"is_updatable": True})
    min_depth_quote: float = Field(default=1_000.0, ge=0.0, json_schema_extra={"is_updatable": True})
    max_slippage_bps: float = Field(default=25.0, ge=0.0, json_schema_extra={"is_updatable": True})
    estimated_fee_bps_per_side: float = Field(default=10.0, ge=0.0, json_schema_extra={"is_updatable": True})
    max_total_cost_bps_per_side: float = Field(default=50.0, ge=0.0, json_schema_extra={"is_updatable": True})
    advisory_required: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    advisory_max_age_seconds: float = Field(default=120.0, gt=0.0, json_schema_extra={"is_updatable": True})
    advisory_weight: float = Field(default=0.0, ge=0.0, le=1.0, json_schema_extra={"is_updatable": True})
    advisory_api_enabled: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    advisory_api_base_url: Optional[str] = Field(default=None, json_schema_extra={"is_updatable": False})
    advisory_api_poll_seconds: float = Field(default=15.0, ge=1.0, json_schema_extra={"is_updatable": True})
    advisory_api_timeout_seconds: float = Field(default=2.0, ge=0.1, le=30.0, json_schema_extra={"is_updatable": True})

    default_sell_fraction_per_tick: Decimal = Field(
        default=Decimal("0.20"), gt=ZERO, le=Decimal("1"), json_schema_extra={"is_updatable": True}
    )
    buy_sell_sell_fraction_per_tick: Decimal = Field(
        default=Decimal("0.20"), gt=ZERO, le=Decimal("1"), json_schema_extra={"is_updatable": True}
    )
    min_trade_value_quote: Decimal = Field(
        default=Decimal("10"), ge=Decimal("10"), json_schema_extra={"is_updatable": True}
    )
    usd_per_entry_quote: Decimal = Field(
        default=Decimal("30"), ge=Decimal("10"), json_schema_extra={"is_updatable": True}
    )
    max_symbol_exposure_quote: Decimal = Field(
        default=Decimal("500"), gt=ZERO, json_schema_extra={"is_updatable": True}
    )
    min_quote_reserve: Decimal = Field(default=Decimal("100"), ge=ZERO, json_schema_extra={"is_updatable": True})
    gross_exposure_cap_quote: Decimal = Field(
        default=Decimal("2500"), gt=ZERO, json_schema_extra={"is_updatable": True}
    )
    turnover_cap_quote: Decimal = Field(
        default=Decimal("500"), gt=ZERO, json_schema_extra={"is_updatable": True}
    )
    max_orders_per_tick: int = Field(default=2, ge=1, le=25, json_schema_extra={"is_updatable": True})
    cooldown_time: int = Field(default=90, ge=0, json_schema_extra={"is_updatable": True})
    daily_loss_limit_pct: Decimal = Field(
        default=Decimal("0.03"), gt=ZERO, le=Decimal("1"), json_schema_extra={"is_updatable": True}
    )
    drawdown_limit_pct: Decimal = Field(
        default=Decimal("0.10"), gt=ZERO, le=Decimal("1"), json_schema_extra={"is_updatable": True}
    )

    @field_validator("quote_asset", mode="before")
    @classmethod
    def validate_quote_asset(cls, value: Any) -> str:
        normalized = str(value or "USDC").strip().upper()
        if normalized != "USDC":
            raise ValueError("Hyperliquid portfolio RSI spot controller currently requires USDC quote")
        return normalized

    @field_validator("buy_sell_assets", "protected_assets", mode="before")
    @classmethod
    def normalize_assets(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [item for item in value.split(",")]
        if not isinstance(value, list):
            raise ValueError("asset lists must be a list or comma-separated string")
        normalized: List[str] = []
        for item in value:
            asset = str(item or "").strip().upper()
            if not asset:
                continue
            if "-" in asset or "/" in asset:
                raise ValueError("asset lists contain base asset symbols, not trading pairs")
            if asset not in normalized:
                normalized.append(asset)
        return normalized

    @model_validator(mode="after")
    def validate_execution_boundary(self):
        if self.wallet_connector_name.endswith("_paper_trade"):
            raise ValueError("wallet_connector_name must be the authenticated Hyperliquid connector")
        if self.paper_actions_enabled and self.mode != "paper":
            raise ValueError("paper_actions_enabled requires mode='paper'")
        if self.paper_actions_enabled and not self.execution_connector_name.endswith("_paper_trade"):
            raise ValueError("paper actions may only target a *_paper_trade connector")
        if self.rsi_buy_threshold >= self.rsi_sell_threshold:
            raise ValueError("rsi_buy_threshold must be below rsi_sell_threshold")
        if self.macd_fast >= self.macd_slow:
            raise ValueError("macd_fast must be below macd_slow")
        if self.interval not in HYPERLIQUID_SPOT_CANDLE_INTERVALS:
            raise ValueError(
                f"interval must be supported by Hyperliquid spot candles: "
                f"{','.join(sorted(HYPERLIQUID_SPOT_CANDLE_INTERVALS))}"
            )
        try:
            PairFeatureConfig(interval=self.interval)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        initial_pairs = {_pair_for_asset(asset, self.quote_asset) for asset in self.buy_sell_assets}
        if initial_pairs:
            markets.setdefault(self.wallet_connector_name, set()).update(initial_pairs)
            if self.mode == "paper":
                markets.setdefault(self.execution_connector_name, set()).update(initial_pairs)
        return markets


class HyperliquidPortfolioRSIController(ControllerBase):
    def __init__(
        self,
        config: HyperliquidPortfolioRSIControllerConfig,
        *args,
        advisory_provider: Optional[Any] = None,
        **kwargs,
    ):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self._advisory_provider = advisory_provider or self._build_advisory_provider()
        self._initialized_pairs: Set[str] = set()
        self._last_action_timestamp: Dict[tuple[str, TradeType], float] = {}
        self._consumed_plan_ids: Set[str] = set()
        self._day_key: Optional[str] = None
        self._day_start_equity: Optional[Decimal] = None
        self._peak_equity: Decimal = ZERO
        self._turnover_used_quote: Decimal = ZERO
        self._paper_balance_bootstrapped = False
        self._initialize_rate_sources()

    def _build_advisory_provider(self) -> Optional[HyperliquidPortfolioSignalClient]:
        if not self.config.advisory_api_enabled:
            return None
        return HyperliquidPortfolioSignalClient(
            base_url=self.config.advisory_api_base_url,
            poll_seconds=float(self.config.advisory_api_poll_seconds),
            max_age_seconds=float(self.config.advisory_max_age_seconds),
            request_timeout_seconds=float(self.config.advisory_api_timeout_seconds),
            enabled=True,
        )

    def _initial_pairs(self) -> List[str]:
        return [_pair_for_asset(asset, self.config.quote_asset) for asset in self.config.buy_sell_assets]

    def get_candles_config(self) -> List[CandlesConfig]:
        return [
            CandlesConfig(
                connector=self.config.wallet_connector_name,
                trading_pair=pair,
                interval=self.config.interval,
                max_records=int(self.config.max_records),
            )
            for pair in self._initial_pairs()
        ]

    def _initialize_rate_sources(self):
        try:
            pairs = [
                ConnectorPair(connector_name=self.config.wallet_connector_name, trading_pair=pair)
                for pair in self._initial_pairs()
            ]
            if pairs:
                self.market_data_provider.initialize_rate_sources(pairs)
        except Exception as exc:
            self.logger().warning(f"Hyperliquid portfolio rate-source initialization failed: {exc}")

    def _feature_config(self) -> PairFeatureConfig:
        return PairFeatureConfig(
            interval=self.config.interval,
            rsi_length=int(self.config.rsi_length),
            rsi_buy_threshold=float(self.config.rsi_buy_threshold),
            rsi_sell_threshold=float(self.config.rsi_sell_threshold),
            bb_length=int(self.config.bb_length),
            bb_std=float(self.config.bb_std),
            macd_fast=int(self.config.macd_fast),
            macd_slow=int(self.config.macd_slow),
            macd_signal=int(self.config.macd_signal),
            ema_fast=int(self.config.ema_fast),
            ema_slow=int(self.config.ema_slow),
            atr_length=int(self.config.atr_length),
            min_atr_pct=float(self.config.min_atr_pct),
            max_atr_pct=float(self.config.max_atr_pct),
            max_candle_age_intervals=float(self.config.max_candle_age_intervals),
            max_book_age_seconds=float(self.config.max_book_age_seconds),
            max_spread_bps=float(self.config.max_spread_bps),
            min_depth_quote=float(self.config.min_depth_quote),
            target_notional_quote=float(self.config.usd_per_entry_quote),
            max_slippage_bps=float(self.config.max_slippage_bps),
            estimated_fee_bps_per_side=float(self.config.estimated_fee_bps_per_side),
            max_total_cost_bps_per_side=float(self.config.max_total_cost_bps_per_side),
            advisory_required=bool(self.config.advisory_required),
            advisory_max_age_seconds=float(self.config.advisory_max_age_seconds),
            advisory_weight=float(self.config.advisory_weight),
        )

    async def update_processed_data(self):
        now = float(self.market_data_provider.time())
        wallet_connector, wallet_status = self._wallet_connector()
        reference_wallet_snapshot = self._portfolio_snapshot(
            wallet_connector,
            wallet_status,
            now,
            source_connector=self.config.wallet_connector_name,
        )
        portfolio_connector, portfolio_status, paper_balance_sync = self._portfolio_connector(
            wallet_connector,
            wallet_status,
        )
        if portfolio_connector is wallet_connector:
            portfolio_snapshot = reference_wallet_snapshot
        else:
            portfolio_snapshot = self._portfolio_snapshot(
                portfolio_connector,
                portfolio_status,
                now,
                source_connector=self.config.execution_connector_name,
            )
        tradable_pairs = await self._all_trading_pairs(wallet_connector)
        pair_policies = self._pair_policies(portfolio_snapshot, tradable_pairs)
        enabled_pairs = [policy["pair_id"] for policy in pair_policies if policy.get("enabled")]
        initialization = await self._initialize_dynamic_pairs(enabled_pairs)

        pair_signals: Dict[str, Dict[str, Any]] = {}
        for policy in pair_policies:
            pair_id = str(policy["pair_id"])
            pair_signals[pair_id] = await self._pair_signal(
                policy=policy,
                now=now,
            )
        signal_snapshots = self._portfolio_signals(pair_signals, now)
        self._apply_feature_prices(portfolio_snapshot, pair_policies, pair_signals)
        risk_gate_state = self._risk_gate_state(portfolio_snapshot, now)
        plan = build_portfolio_plan(
            pair_policies=pair_policies,
            portfolio_snapshot=portfolio_snapshot,
            signal_snapshots=signal_snapshots,
            risk_gate_state=risk_gate_state,
        )
        self.processed_data = {
            "schema": CONTROLLER_STATE_SCHEMA,
            "as_of": _iso_timestamp(now),
            "mode": self.config.mode,
            "portfolio_snapshot": portfolio_snapshot,
            "reference_wallet_snapshot": (
                reference_wallet_snapshot if portfolio_connector is not wallet_connector else None
            ),
            "paper_balance_sync": paper_balance_sync,
            "pair_policies": pair_policies,
            "signal_snapshots": signal_snapshots,
            "risk_gate_state": risk_gate_state,
            "portfolio_plan": plan.to_dict(),
            "dynamic_pair_initialization": initialization,
            "action_telemetry": self._empty_action_telemetry(now),
        }

    def _wallet_connector(self) -> tuple[Optional[Any], str]:
        try:
            connector = self.market_data_provider.get_connector(self.config.wallet_connector_name)
        except Exception:
            return None, "unavailable"
        ready = getattr(connector, "ready", False)
        return connector, "fresh" if ready is True else "partial"

    def _portfolio_connector(
        self,
        wallet_connector: Optional[Any],
        wallet_status: str,
    ) -> tuple[Optional[Any], str, Dict[str, Any]]:
        sync = {
            "status": "not_requested",
            "source_connector": self.config.wallet_connector_name,
            "target_connector": self.config.execution_connector_name,
            "bootstrap_from_wallet": bool(self.config.paper_bootstrap_from_wallet),
        }
        if self.config.mode != "paper" or not self.config.paper_actions_enabled:
            return wallet_connector, wallet_status, sync
        try:
            execution_connector = self.market_data_provider.get_connector(self.config.execution_connector_name)
        except Exception:
            sync["status"] = "execution_connector_unavailable"
            return None, "unavailable", sync

        if self.config.paper_bootstrap_from_wallet and not self._paper_balance_bootstrapped:
            if wallet_connector is None or wallet_status != "fresh":
                sync["status"] = "wallet_not_ready"
                return None, "unavailable", sync
            set_balance = getattr(execution_connector, "set_balance", None)
            if not callable(set_balance):
                sync["status"] = "paper_balance_setter_unavailable"
                return None, "unavailable", sync
            try:
                source_balances = dict(wallet_connector.get_all_balances())
                target_balances = dict(execution_connector.get_all_balances())
                for asset in sorted(set(source_balances) | set(target_balances)):
                    set_balance(str(asset).upper(), max(_decimal(source_balances.get(asset)), ZERO))
            except Exception:
                sync["status"] = "bootstrap_failed"
                return None, "unavailable", sync
            self._paper_balance_bootstrapped = True
            sync["status"] = "bootstrapped"
        else:
            sync["status"] = "active" if self._paper_balance_bootstrapped else "configured_balances"
        ready = getattr(execution_connector, "ready", False)
        return execution_connector, "fresh" if ready is True else "partial", sync

    def _portfolio_snapshot(
        self,
        connector: Optional[Any],
        wallet_status: str,
        now: float,
        *,
        source_connector: str,
    ) -> Dict[str, Any]:
        balances: Dict[str, Dict[str, str]] = {}
        if connector is not None:
            try:
                raw_balances = dict(connector.get_all_balances())
            except Exception:
                raw_balances = {}
                wallet_status = "partial"
            for raw_asset, raw_total in raw_balances.items():
                asset = str(raw_asset).strip().upper()
                total = max(_decimal(raw_total), ZERO)
                try:
                    available = max(_decimal(connector.get_available_balance(asset)), ZERO)
                except Exception:
                    available = ZERO
                    wallet_status = "partial"
                balances[asset] = {
                    "total_units": str(total),
                    "available_units": str(min(available, total)),
                    "price_quote": "1" if asset == self.config.quote_asset else "0",
                    "min_trade_value_quote": str(self.config.min_trade_value_quote),
                }
        for asset in self.config.buy_sell_assets:
            balances.setdefault(asset, {
                "total_units": "0",
                "available_units": "0",
                "price_quote": "0",
                "min_trade_value_quote": str(self.config.min_trade_value_quote),
            })
        quote_balance = balances.get(self.config.quote_asset, {})
        return {
            "schema": "rsibot.hyperliquid_portfolio_rsi.wallet_snapshot.v1",
            "snapshot_id": f"{self.config.id}:{int(now * 1_000)}",
            "as_of": _iso_timestamp(now),
            "refresh_status": wallet_status,
            "source_connector": source_connector,
            "quote_asset": self.config.quote_asset,
            "quote_available": str(quote_balance.get("available_units", "0")),
            "balances": balances,
            "open_orders": self._open_orders(connector),
        }

    def _open_orders(self, connector: Optional[Any]) -> List[Dict[str, Any]]:
        if connector is None:
            return []
        try:
            orders = list(connector.limit_orders)
        except Exception:
            return []
        result: List[Dict[str, Any]] = []
        for order in orders:
            pair_id = str(getattr(order, "trading_pair", "") or "")
            quantity = _decimal(getattr(order, "quantity", ZERO))
            filled = _decimal(getattr(order, "filled_quantity", ZERO))
            side = "buy" if bool(getattr(order, "is_buy", False)) else "sell"
            result.append({
                "client_order_id": str(getattr(order, "client_order_id", "") or ""),
                "pair_id": pair_id,
                "base_asset": pair_id.split("-", 1)[0] if pair_id else "",
                "side": side,
                "quantity": str(quantity),
                "remaining_units": str(max(quantity - filled, ZERO)),
            })
        return result

    async def _all_trading_pairs(self, connector: Optional[Any]) -> List[str]:
        if connector is None:
            return []
        try:
            return sorted(set(await connector.all_trading_pairs()))
        except Exception:
            return []

    def _pair_policies(
        self,
        portfolio_snapshot: Mapping[str, Any],
        tradable_pairs: List[str],
    ) -> List[Dict[str, Any]]:
        buy_assets = [
            {
                "asset": asset,
                "max_symbol_exposure_quote": str(self.config.max_symbol_exposure_quote),
                "min_quote_reserve": str(self.config.min_quote_reserve),
                "min_trade_value_quote": str(self.config.min_trade_value_quote),
            }
            for asset in self.config.buy_sell_assets
        ]
        policies = build_pair_policies_from_wallet(
            portfolio_snapshot=portfolio_snapshot,
            buy_sell_assets=buy_assets,
            protected_assets=self.config.protected_assets,
            quote_asset=self.config.quote_asset,
            tradable_pairs=tradable_pairs,
            default_sell_fraction_per_tick=str(self.config.default_sell_fraction_per_tick),
            default_min_trade_value_quote=str(self.config.min_trade_value_quote),
            default_max_symbol_exposure_quote=str(self.config.max_symbol_exposure_quote),
            default_min_quote_reserve=str(self.config.min_quote_reserve),
        )
        allowed_override_fields = {
            "enabled",
            "max_sell_fraction_per_tick",
            "max_symbol_exposure_quote",
            "min_quote_reserve",
            "min_trade_value_quote",
        }
        for policy in policies:
            if policy["mode"] == "buy_sell":
                policy["max_sell_fraction_per_tick"] = str(self.config.buy_sell_sell_fraction_per_tick)
            override = (
                self.config.pair_policy_overrides.get(str(policy["pair_id"]))
                or self.config.pair_policy_overrides.get(str(policy["base_asset"]))
                or {}
            )
            for field_name in allowed_override_fields:
                if field_name in override:
                    policy[field_name] = override[field_name]
        return policies

    async def _initialize_dynamic_pairs(self, enabled_pairs: List[str]) -> Dict[str, Any]:
        pending = [pair for pair in enabled_pairs if pair not in self._initialized_pairs]
        result: Dict[str, Any] = {"requested": pending, "wallet": {}, "execution": {}}
        if not pending:
            return result
        try:
            wallet_result = await self.market_data_provider.initialize_order_books(
                self.config.wallet_connector_name,
                pending,
            )
        except Exception:
            wallet_result = {pair: False for pair in pending}
        result["wallet"] = wallet_result
        for pair in pending:
            try:
                self.market_data_provider.initialize_candles_feed(CandlesConfig(
                    connector=self.config.wallet_connector_name,
                    trading_pair=pair,
                    interval=self.config.interval,
                    max_records=int(self.config.max_records),
                ))
            except Exception:
                wallet_result[pair] = False
        if self.config.mode == "paper":
            try:
                result["execution"] = await self.market_data_provider.initialize_order_books(
                    self.config.execution_connector_name,
                    pending,
                )
            except Exception:
                result["execution"] = {pair: False for pair in pending}
        self._initialized_pairs.update(pair for pair, ready in wallet_result.items() if ready is True)
        return result

    async def _pair_signal(self, *, policy: Mapping[str, Any], now: float) -> Dict[str, Any]:
        pair_id = str(policy["pair_id"])
        symbol_status = str(policy.get("symbol_mapping_status") or "unresolved")
        if not bool(policy.get("enabled")):
            return build_pair_signal_snapshot(
                pair_id=pair_id,
                candles=pd.DataFrame(),
                market_snapshot={},
                trading_rule={},
                as_of=now,
                config=self._feature_config(),
                symbol_mapping_status=symbol_status,
            )
        try:
            candles = self.market_data_provider.get_candles_df(
                connector_name=self.config.wallet_connector_name,
                trading_pair=pair_id,
                interval=self.config.interval,
                max_records=int(self.config.max_records),
            )
        except Exception:
            candles = pd.DataFrame()
        try:
            market_snapshot = self._market_snapshot(pair_id)
        except Exception:
            market_snapshot = {}
        try:
            trading_rule = self.market_data_provider.get_trading_rules(
                self.config.wallet_connector_name,
                pair_id,
            )
        except Exception:
            trading_rule = {}
        advisory = await self._advisory_snapshot(pair_id)
        return build_pair_signal_snapshot(
            pair_id=pair_id,
            candles=candles,
            market_snapshot=market_snapshot,
            trading_rule=trading_rule,
            as_of=now,
            config=self._feature_config(),
            advisory=advisory,
            symbol_mapping_status=symbol_status,
        )

    def _market_snapshot(self, pair_id: str) -> Dict[str, Any]:
        order_book = self.market_data_provider.get_order_book(self.config.wallet_connector_name, pair_id)
        bids, asks = order_book.snapshot
        if bids.empty or asks.empty:
            return {"timestamp": getattr(order_book, "snapshot_uid", 0)}
        bid_rows = bids.copy()
        ask_rows = asks.copy()
        bid_rows["price"] = pd.to_numeric(bid_rows["price"], errors="coerce")
        bid_rows["amount"] = pd.to_numeric(bid_rows["amount"], errors="coerce")
        ask_rows["price"] = pd.to_numeric(ask_rows["price"], errors="coerce")
        ask_rows["amount"] = pd.to_numeric(ask_rows["amount"], errors="coerce")
        bid_rows = bid_rows.dropna(subset=["price", "amount"]).nlargest(
            int(self.config.order_book_depth_levels), "price"
        )
        ask_rows = ask_rows.dropna(subset=["price", "amount"]).nsmallest(
            int(self.config.order_book_depth_levels), "price"
        )
        return {
            "timestamp": getattr(order_book, "snapshot_uid", 0),
            "bid_price": float(bid_rows["price"].iloc[0]) if not bid_rows.empty else None,
            "ask_price": float(ask_rows["price"].iloc[0]) if not ask_rows.empty else None,
            "bid_depth_quote": float((bid_rows["price"] * bid_rows["amount"]).sum()),
            "ask_depth_quote": float((ask_rows["price"] * ask_rows["amount"]).sum()),
        }

    async def _advisory_snapshot(self, pair_id: str) -> Optional[Mapping[str, Any]]:
        if self._advisory_provider is None:
            return None
        try:
            payload = self._advisory_provider.snapshot(pair_id)
            if inspect.isawaitable(payload):
                payload = await payload
            return payload if isinstance(payload, Mapping) else None
        except Exception:
            return None

    def _portfolio_signals(self, pair_signals: Dict[str, Dict[str, Any]], now: float) -> Dict[str, Any]:
        enabled_signals = [signal for signal in pair_signals.values() if signal.get("symbol_mapping_status") == "resolved"]
        cutoffs = [str(signal.get("feature_cutoff")) for signal in enabled_signals if signal.get("feature_cutoff")]
        all_fresh = bool(enabled_signals) and all(
            signal.get("freshness_status") == "fresh" for signal in enabled_signals
        )
        return {
            "schema": PORTFOLIO_SIGNAL_SCHEMA,
            "as_of": _iso_timestamp(now),
            "freshness_status": "fresh" if all_fresh else "partial",
            "feature_cutoff": min(cutoffs) if cutoffs else None,
            "pairs": pair_signals,
        }

    def _apply_feature_prices(
        self,
        portfolio_snapshot: Dict[str, Any],
        pair_policies: List[Dict[str, Any]],
        pair_signals: Dict[str, Dict[str, Any]],
    ):
        balances = portfolio_snapshot["balances"]
        for policy in pair_policies:
            asset = str(policy["base_asset"])
            signal = pair_signals.get(str(policy["pair_id"]), {})
            price = _decimal(signal.get("price_quote"), ZERO)
            if asset in balances and price > ZERO:
                balances[asset]["price_quote"] = str(price)
                min_notional = _decimal((signal.get("trading_rule") or {}).get("min_notional_size"), ZERO)
                if min_notional > ZERO:
                    balances[asset]["min_trade_value_quote"] = str(min_notional)

    def _risk_gate_state(self, portfolio_snapshot: Mapping[str, Any], now: float) -> Dict[str, Any]:
        equity = ZERO
        gross_exposure = ZERO
        for asset, raw_balance in (portfolio_snapshot.get("balances") or {}).items():
            total = _decimal(raw_balance.get("total_units"), ZERO)
            price = _decimal(raw_balance.get("price_quote"), ZERO)
            value = total * price
            equity += value
            if str(asset).upper() != self.config.quote_asset:
                gross_exposure += value
        day_key = datetime.fromtimestamp(now, tz=timezone.utc).date().isoformat()
        if day_key != self._day_key:
            self._day_key = day_key
            self._day_start_equity = equity
            self._turnover_used_quote = ZERO
        self._peak_equity = max(self._peak_equity, equity)
        day_start = self._day_start_equity or equity
        daily_loss_pct = max((day_start - equity) / day_start, ZERO) if day_start > ZERO else ZERO
        drawdown_pct = max((self._peak_equity - equity) / self._peak_equity, ZERO) if self._peak_equity > ZERO else ZERO
        return {
            "request_id": str(portfolio_snapshot.get("snapshot_id")),
            "shadow_mode": self.config.mode == "shadow",
            "mode": self.config.mode,
            "paper_actions_enabled": bool(self.config.paper_actions_enabled),
            "manual_kill_switch": bool(self.config.manual_kill_switch),
            "usd_per_entry_quote": str(self.config.usd_per_entry_quote),
            "gross_exposure_cap_quote": str(self.config.gross_exposure_cap_quote),
            "gross_exposure_quote": str(gross_exposure),
            "turnover_cap_quote": str(self.config.turnover_cap_quote),
            "turnover_used_quote": str(self._turnover_used_quote),
            "max_orders_per_tick": int(self.config.max_orders_per_tick),
            "daily_loss_limit_pct": str(self.config.daily_loss_limit_pct),
            "daily_loss_pct": str(daily_loss_pct),
            "drawdown_limit_pct": str(self.config.drawdown_limit_pct),
            "drawdown_pct": str(drawdown_pct),
            "equity_quote": str(equity),
        }

    def determine_executor_actions(self) -> List[ExecutorAction]:
        now = float(self.market_data_provider.time())
        if self.config.mode != "paper" or not self.config.paper_actions_enabled:
            self._record_action_telemetry(now, [], "execution_mode_blocked")
            return []
        if bool(self.config.manual_kill_switch):
            stop_actions: List[ExecutorAction] = []
            for executor in self.executors_info or []:
                if (
                    bool(getattr(executor, "is_active", False))
                    and getattr(executor, "controller_id", None) == self.config.id
                    and getattr(getattr(executor, "config", None), "connector_name", None)
                    == self.config.execution_connector_name
                ):
                    stop_actions.append(StopExecutorAction(
                        controller_id=self.config.id,
                        executor_id=str(executor.id),
                        keep_position=False,
                    ))
            self._record_action_telemetry(now, [], "manual_kill_switch", stop_actions)
            return stop_actions
        stale_executor_actions = self._stale_executor_stop_actions(now)
        if stale_executor_actions:
            self._record_action_telemetry(
                now,
                [],
                "stale_executor_timeout",
                stale_executor_actions,
            )
            return stale_executor_actions
        plan = self.processed_data.get("portfolio_plan") or {}
        if plan.get("readiness") != "paper_ready":
            self._record_action_telemetry(now, [], "paper_plan_not_ready")
            return []
        plan_id = str(plan.get("plan_id") or plan.get("request_id") or id(plan))
        if plan_id in self._consumed_plan_ids:
            self._record_action_telemetry(now, [], "plan_already_consumed")
            return []
        pair_signals = (self.processed_data.get("signal_snapshots") or {}).get("pairs") or {}
        actions: List[ExecutorAction] = []
        skipped: List[Dict[str, str]] = []
        for row in plan.get("rows") or []:
            if len(actions) >= int(self.config.max_orders_per_tick):
                skipped.append({"pair_id": str(row.get("pair_id")), "reason": "max_orders_per_tick"})
                continue
            target_action = str(row.get("target_action") or "none").lower()
            if target_action not in {"buy", "sell"}:
                continue
            pair_id = str(row.get("pair_id") or "")
            side = TradeType.BUY if target_action == "buy" else TradeType.SELL
            feature = pair_signals.get(pair_id) or {}
            if not self._feature_actionable(feature, now):
                skipped.append({"pair_id": pair_id, "reason": "feature_not_actionable"})
                continue
            if self._has_active_executor(pair_id, side):
                skipped.append({"pair_id": pair_id, "reason": "active_executor"})
                continue
            last_action = self._last_action_timestamp.get((pair_id, side), 0.0)
            if now - last_action < float(self.config.cooldown_time):
                skipped.append({"pair_id": pair_id, "reason": "cooldown"})
                continue
            amount = _decimal(row.get("proposed_units"), ZERO)
            proposed_value = _decimal(row.get("proposed_value_quote"), ZERO)
            market = feature.get("market") or {}
            raw_price = market.get("ask_price") if side == TradeType.BUY else market.get("bid_price")
            price = _decimal(raw_price, ZERO)
            if amount <= ZERO or price <= ZERO or proposed_value < self.config.min_trade_value_quote:
                skipped.append({"pair_id": pair_id, "reason": "invalid_order_size"})
                continue
            try:
                amount = self.market_data_provider.quantize_order_amount(
                    self.config.execution_connector_name,
                    pair_id,
                    amount,
                )
                price = self.market_data_provider.quantize_order_price(
                    self.config.execution_connector_name,
                    pair_id,
                    price,
                )
            except Exception:
                skipped.append({"pair_id": pair_id, "reason": "quantization_failed"})
                continue
            if amount <= ZERO or price <= ZERO or amount * price < self.config.min_trade_value_quote:
                skipped.append({"pair_id": pair_id, "reason": "quantized_below_minimum"})
                continue
            actions.append(CreateExecutorAction(
                controller_id=self.config.id,
                executor_config=OrderExecutorConfig(
                    timestamp=now,
                    connector_name=self.config.execution_connector_name,
                    trading_pair=pair_id,
                    side=side,
                    amount=amount,
                    price=price,
                    execution_strategy=ExecutionStrategy.LIMIT,
                    position_action=(PositionAction.OPEN if side == TradeType.BUY else PositionAction.CLOSE),
                    leverage=1,
                ),
            ))
            self._last_action_timestamp[(pair_id, side)] = now
            self._turnover_used_quote += amount * price
        plan["created_order_count"] = len(actions)
        if actions:
            self._consumed_plan_ids.add(plan_id)
        self._record_action_telemetry(now, skipped, None, actions)
        return actions

    def _stale_executor_stop_actions(self, now: float) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        for executor in self.executors_info or []:
            if not bool(getattr(executor, "is_active", False)):
                continue
            config = getattr(executor, "config", None)
            if (
                getattr(executor, "controller_id", None) != self.config.id
                or getattr(config, "connector_name", None) != self.config.execution_connector_name
            ):
                continue
            timestamp = _float(
                getattr(executor, "timestamp", getattr(config, "timestamp", now)),
                now,
            )
            if now - timestamp < float(self.config.order_timeout_seconds):
                continue
            actions.append(StopExecutorAction(
                controller_id=self.config.id,
                executor_id=str(executor.id),
                keep_position=True,
            ))
        return actions

    def _feature_actionable(self, feature: Mapping[str, Any], now: float) -> bool:
        if feature.get("actionable") is not True or feature.get("freshness_status") != "fresh":
            return False
        if feature.get("symbol_mapping_status") != "resolved" or feature.get("blockers"):
            return False
        cutoff = _timestamp_seconds(feature.get("feature_cutoff"))
        max_age = float(self.config.max_candle_age_intervals) * float(
            {"s": 1, "m": 60, "h": 3_600, "d": 86_400}[self.config.interval[-1]]
        ) * int(self.config.interval[:-1])
        return cutoff is not None and -5.0 <= now - cutoff <= max_age

    def _has_active_executor(self, pair_id: str, side: TradeType) -> bool:
        for executor in self.executors_info or []:
            if not bool(getattr(executor, "is_active", False)):
                continue
            config = getattr(executor, "config", None)
            if config is None:
                continue
            if (
                getattr(config, "connector_name", None) == self.config.execution_connector_name
                and getattr(config, "trading_pair", None) == pair_id
                and getattr(config, "side", None) == side
            ):
                return True
        return False

    @staticmethod
    def _empty_action_telemetry(now: float) -> Dict[str, Any]:
        return {
            "schema": "rsibot.hyperliquid_portfolio_rsi.action_telemetry.v1",
            "as_of": _iso_timestamp(now),
            "created_order_count": 0,
            "stopped_executor_count": 0,
            "blocked_reason": None,
            "skipped": [],
        }

    def _record_action_telemetry(
        self,
        now: float,
        skipped: List[Dict[str, str]],
        blocked_reason: Optional[str],
        actions: Optional[List[ExecutorAction]] = None,
    ):
        self.processed_data["action_telemetry"] = {
            "schema": "rsibot.hyperliquid_portfolio_rsi.action_telemetry.v1",
            "as_of": _iso_timestamp(now),
            "created_order_count": sum(isinstance(action, CreateExecutorAction) for action in actions or []),
            "stopped_executor_count": sum(isinstance(action, StopExecutorAction) for action in actions or []),
            "blocked_reason": blocked_reason,
            "skipped": skipped,
        }


__all__ = [
    "HyperliquidPortfolioRSIController",
    "HyperliquidPortfolioRSIControllerConfig",
]
