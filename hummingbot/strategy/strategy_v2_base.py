import asyncio
import importlib
import inspect
import json
import logging
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import numpy as np
import pandas as pd
import yaml
from pydantic import BaseModel, Field, PrivateAttr, field_validator

from hummingbot.client import settings
from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import MarketDict, PositionMode, PriceType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import OrderType, PositionAction
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.exceptions import InvalidController
from hummingbot.logger import HummingbotLogger
from hummingbot.remote_iface.mqtt import ETopicPublisher
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.controllers.market_making_controller_base import MarketMakingControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import PositionSummary
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    ExecutorAction,
    StopExecutorAction,
    StoreExecutorAction,
)
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

lsb_logger = None
s_decimal_nan = Decimal("NaN")
USD_STABLE_TOKENS = {"USD", "USDT", "USDC", "FDUSD", "BUSD", "DAI", "TUSD", "USDE"}

# Lazy-loaded to avoid circular import (strategy_v2_base -> executor_orchestrator -> executors -> strategy_v2_base)
ExecutorOrchestrator = None


def _get_executor_orchestrator_class():
    global ExecutorOrchestrator
    if ExecutorOrchestrator is None:
        from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator as _cls
        ExecutorOrchestrator = _cls
    return ExecutorOrchestrator


class StrategyV2ConfigBase(BaseClientModel):
    """
    Base class for version 2 strategy configurations.
    Every V2 script must have a config class that inherits from this.

    Fields:
    - script_file_name: The script file that this config is for (set via os.path.basename(__file__) in subclass).
    - controllers_config: Optional controller configuration file paths.
    - candles_config: Candles configurations for strategy-level data feeds. Controllers may also define their own candles.

    Markets are defined via the update_markets() method, which subclasses should override to specify their required markets.
    This ensures consistency with controller configurations and allows for programmatic market definition.

    Subclasses can define their own `candles_config` field using the
    static utility method `parse_candles_config_str()`.
    """
    script_file_name: str = ""
    controllers_config: List[str] = Field(
        default=[],
        json_schema_extra={
            "prompt": "Enter controller configurations (comma-separated file paths), leave it empty if none: ",
            "prompt_on_new": True,
        }
    )
    _controller_module_mtimes: Dict[str, float] = PrivateAttr(default_factory=dict)

    @field_validator("controllers_config", mode="before")
    @classmethod
    def parse_controllers_config(cls, v):
        # Parse string input into a list of file paths
        if isinstance(v, str):
            if v == "":
                return []
            return [item.strip() for item in v.split(',') if item.strip()]
        if v is None:
            return []
        return v

    def _load_controller_module(self, module_path: str, reload_changed_modules: bool = False):
        module = importlib.import_module(module_path)
        try:
            module_file = inspect.getsourcefile(module)
        except TypeError:
            module_file = None
        module_file = module_file or getattr(module, "__file__", None)
        if module_file is not None and module_file.endswith(".pyc"):
            module_file = module_file[:-1]

        current_mtime = None
        if module_file is not None and os.path.exists(module_file):
            current_mtime = os.path.getmtime(module_file)
            previous_mtime = self._controller_module_mtimes.get(module_path)
            if (
                reload_changed_modules
                and previous_mtime is not None
                and current_mtime > previous_mtime
            ):
                module = importlib.reload(module)

        if current_mtime is not None:
            self._controller_module_mtimes[module_path] = current_mtime

        return module

    def load_controller_configs(self, reload_changed_modules: bool = False):
        loaded_configs = []
        for config_path in self.controllers_config:
            full_path = os.path.join(settings.CONTROLLERS_CONF_DIR_PATH, config_path)
            with open(full_path, 'r') as file:
                config_data = yaml.safe_load(file)

            controller_type = config_data.get('controller_type')
            controller_name = config_data.get('controller_name')

            if not controller_type or not controller_name:
                raise ValueError(f"Missing controller_type or controller_name in {config_path}")

            module_path = f"{settings.CONTROLLERS_MODULE}.{controller_type}.{controller_name}"
            module = self._load_controller_module(
                module_path=module_path,
                reload_changed_modules=reload_changed_modules,
            )

            config_class = next((member for member_name, member in inspect.getmembers(module)
                                 if inspect.isclass(member) and member not in [ControllerConfigBase,
                                                                               MarketMakingControllerConfigBase,
                                                                               DirectionalTradingControllerConfigBase]
                                 and (issubclass(member, ControllerConfigBase))), None)
            if not config_class:
                raise InvalidController(f"No configuration class found in the module {controller_name}.")

            loaded_configs.append(config_class(**config_data))

        return loaded_configs

    @staticmethod
    def parse_markets_str(v: str) -> Dict[str, Set[str]]:
        markets_dict = {}
        if v.strip():
            exchanges = v.split(':')
            for exchange in exchanges:
                parts = exchange.split('.')
                if len(parts) != 2 or not parts[1]:
                    raise ValueError(f"Invalid market format in segment '{exchange}'. "
                                     "Expected format: 'exchange.tp1,tp2'")
                exchange_name, trading_pairs = parts
                markets_dict[exchange_name] = set(trading_pairs.split(','))
        return markets_dict

    @staticmethod
    def parse_candles_config_str(v: str) -> List[CandlesConfig]:
        configs = []
        if v.strip():
            entries = v.split(':')
            for entry in entries:
                parts = entry.split('.')
                if len(parts) != 4:
                    raise ValueError(f"Invalid candles config format in segment '{entry}'. "
                                     "Expected format: 'exchange.tradingpair.interval.maxrecords'")
                connector, trading_pair, interval, max_records_str = parts
                try:
                    max_records = int(max_records_str)
                except ValueError:
                    raise ValueError(f"Invalid max_records value '{max_records_str}' in segment '{entry}'. "
                                     "max_records should be an integer.")
                config = CandlesConfig(
                    connector=connector,
                    trading_pair=trading_pair,
                    interval=interval,
                    max_records=max_records
                )
                configs.append(config)
        return configs

    def update_markets(self, markets: MarketDict) -> MarketDict:
        """
        Update the markets dict with strategy-specific markets.
        Subclasses should override this method to add their markets.

        :param markets: Current markets dictionary
        :return: Updated markets dictionary
        """
        return markets


class StrategyV2Base(StrategyPyBase):
    """
    Unified base class for both simple script strategies and V2 strategies using smart components.

    V2 infrastructure (MarketDataProvider, ExecutorOrchestrator, actions_queue) is always initialized.
    When config is a StrategyV2ConfigBase, controllers are loaded and orchestration runs automatically.
    When config is None or a simple BaseModel, simple scripts can still use executors and market data on demand.
    """
    # Class-level markets definition used by both simple scripts and V2 strategies
    markets: Dict[str, Set[str]] = {}

    # V2-specific class attributes
    _last_config_update_ts: float = 0
    closed_executors_buffer: int = 100
    max_executors_close_attempts: int = 10
    config_update_interval: int = 10
    STATUS_SECTION_ICONS: Dict[str, str] = {
        "SIGNALS": "📡",
        "PLANS": "🧭",
        "ACTIVE EXECUTORS": "⚙️",
        "POSITIONS HELD": "👜",
        "PERFORMANCE SUMMARY": "💰",
    }

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global lsb_logger
        if lsb_logger is None:
            lsb_logger = logging.getLogger(__name__)
        return lsb_logger

    @classmethod
    def update_markets(cls, config: "StrategyV2ConfigBase", markets: MarketDict) -> MarketDict:
        """
        Update the markets dict with strategy-specific markets.
        Subclasses should override this method to add their markets.

        :param config: Strategy configuration
        :param markets: Current markets dictionary
        :return: Updated markets dictionary
        """
        return markets

    @classmethod
    def init_markets(cls, config: BaseModel):
        """
        Initialize the markets that the strategy is going to use. This method is called when the strategy is created in
        the start command. Can be overridden to implement custom behavior.

        Merges markets from controllers and the strategy config via their respective update_markets methods.
        """
        if isinstance(config, StrategyV2ConfigBase):
            markets = MarketDict({})
            # From controllers
            controllers_configs = config.load_controller_configs()
            for controller_config in controllers_configs:
                markets = controller_config.update_markets(markets)
            # From strategy config
            markets = config.update_markets(markets)
            cls.markets = markets
        else:
            raise NotImplementedError

    def initialize_candles(self):
        """
        Initialize candles for the strategy. This method collects candles configurations
        from controllers only.
        """
        # From controllers (after they are initialized)
        for controller in self.controllers.values():
            controller.initialize_candles()

    def get_candles_df(self, connector_name: str, trading_pair: str, interval: str) -> pd.DataFrame:
        """
        Get candles data as DataFrame for the specified parameters.

        :param connector_name: Name of the connector (e.g., 'binance')
        :param trading_pair: Trading pair (e.g., 'BTC-USDT')
        :param interval: Candle interval (e.g., '1m', '5m', '1h')
        :return: DataFrame with candle data (OHLCV)
        """
        return self.market_data_provider.get_candles_df(
            connector_name=connector_name,
            trading_pair=trading_pair,
            interval=interval
        )

    def __init__(self, connectors: Dict[str, ConnectorBase], config: Optional[BaseModel] = None):
        """
        Initialize the strategy.

        :param connectors: A dictionary of connector names and their corresponding connector.
        :param config: Optional configuration. If StrategyV2ConfigBase, enables controller orchestration.
        """
        super().__init__()
        self.connectors: Dict[str, ConnectorBase] = connectors
        self.ready_to_trade: bool = False
        self.add_markets(list(connectors.values()))
        self.config = config

        # Always initialize V2 infrastructure
        self.controllers: Dict[str, ControllerBase] = {}
        self.controller_reports: Dict[str, Dict] = {}
        self.market_data_provider = MarketDataProvider(connectors)
        self._is_stop_triggered = False
        self._controller_reports_valid = False
        self._controllers_started = False
        self._last_controller_reload_error: Optional[str] = None
        self._last_runtime_db_sync_ts: float = 0
        self._last_runtime_status_snapshot_ts: float = 0
        self._last_chart_snapshot_prune_ts: float = 0
        self.mqtt_enabled = False
        self._pub: Optional[ETopicPublisher] = None

        self.actions_queue = asyncio.Queue()
        self.listen_to_executor_actions_task: asyncio.Task = asyncio.create_task(self.listen_to_executor_actions())

        # Initialize controllers from config if available
        if isinstance(config, StrategyV2ConfigBase):
            self.initialize_controllers()
            # Initialize candles after controllers are set up
            self.initialize_candles()

        self.executor_orchestrator = _get_executor_orchestrator_class()(
            strategy=self,
            initial_positions_by_controller=self._collect_initial_positions()
        )

    # -------------------------------------------------------------------------
    # Shared methods (simple + V2 modes)
    # -------------------------------------------------------------------------

    def tick(self, timestamp: float):
        """
        Clock tick entry point, is run every second (on normal tick setting).
        Checks if all connectors are ready, if so the strategy is ready to trade.

        :param timestamp: current tick timestamp
        """
        if not self.ready_to_trade:
            self.ready_to_trade = all(ex.ready for ex in self.connectors.values())
            if not self.ready_to_trade:
                for con in [c for c in self.connectors.values() if not c.ready]:
                    self.logger().warning(f"{con.name} is not ready. Please wait...")
                return
        else:
            self.on_tick()

    def on_tick(self):
        """
        An event which is called on every tick. When controllers are configured, runs executor orchestration.
        Simple scripts override this method for custom logic.
        """
        if self.controllers:
            self.update_executors_info()
            self._sync_runtime_state_to_db_if_due()
            self._write_chart_snapshots_if_due()
            self._write_runtime_status_snapshot_if_due()
            self.update_controllers_configs()
            if self.market_data_provider.ready and not self._is_stop_triggered:
                executor_actions: List[ExecutorAction] = self.determine_executor_actions()
                for action in executor_actions:
                    self.executor_orchestrator.execute_action(action)

    async def on_stop(self):
        """
        Called when the strategy is stopped. Shuts down controllers, executors, and market data provider.
        """
        self._is_stop_triggered = True
        self._controllers_started = False

        # Stop controllers FIRST to prevent new executor actions
        for controller in self.controllers.values():
            controller.stop()

        if self.listen_to_executor_actions_task:
            self.listen_to_executor_actions_task.cancel()
        keep_position_by_controller = self._shutdown_keep_position_preferences()
        await self.executor_orchestrator.stop(
            self.max_executors_close_attempts,
            keep_position_by_controller=keep_position_by_controller,
        )
        self.market_data_provider.stop()
        self.executor_orchestrator.store_all_executors()
        if self.mqtt_enabled:
            self._pub({controller_id: {} for controller_id in self.controllers.keys()})
            self._pub = None

    def _shutdown_keep_position_preferences(self) -> Dict[str, bool]:
        preferences: Dict[str, bool] = {}
        for controller_id, controller in self.controllers.items():
            config = getattr(controller, "config", None)
            keep_position = False
            for attr_name in ("early_stop_keep_position", "keep_position"):
                attr_value = getattr(config, attr_name, None)
                if isinstance(attr_value, bool):
                    keep_position = attr_value
                    break
            preferences[controller_id] = keep_position
            self.logger().info(
                f"Shutdown preference | controller={controller_id} keep_position={keep_position}"
            )
        return preferences

    def buy(self,
            connector_name: str,
            trading_pair: str,
            amount: Decimal,
            order_type: OrderType,
            price=s_decimal_nan,
            position_action=PositionAction.OPEN) -> str:
        """
        A wrapper function to buy_with_specific_market.

        :param connector_name: The name of the connector
        :param trading_pair: The market trading pair
        :param amount: An order amount in base token value
        :param order_type: The type of the order
        :param price: An order price
        :param position_action: A position action (for perpetual market only)

        :return: The client assigned id for the new order
        """
        market_pair = self._market_trading_pair_tuple(connector_name, trading_pair)
        self.logger().debug(f"Creating {trading_pair} buy order: price: {price} amount: {amount}.")
        return self.buy_with_specific_market(market_pair, amount, order_type, price, position_action=position_action)

    def sell(self,
             connector_name: str,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType,
             price=s_decimal_nan,
             position_action=PositionAction.OPEN) -> str:
        """
        A wrapper function to sell_with_specific_market.

        :param connector_name: The name of the connector
        :param trading_pair: The market trading pair
        :param amount: An order amount in base token value
        :param order_type: The type of the order
        :param price: An order price
        :param position_action: A position action (for perpetual market only)

        :return: The client assigned id for the new order
        """
        market_pair = self._market_trading_pair_tuple(connector_name, trading_pair)
        self.logger().debug(f"Creating {trading_pair} sell order: price: {price} amount: {amount}.")
        return self.sell_with_specific_market(market_pair, amount, order_type, price, position_action=position_action)

    def cancel(self,
               connector_name: str,
               trading_pair: str,
               order_id: str):
        """
        A wrapper function to cancel_order.

        :param connector_name: The name of the connector
        :param trading_pair: The market trading pair
        :param order_id: The identifier assigned by the client of the order to be cancelled
        """
        market_pair = self._market_trading_pair_tuple(connector_name, trading_pair)
        self.cancel_order(market_trading_pair_tuple=market_pair, order_id=order_id)

    def get_active_orders(self, connector_name: str) -> List[LimitOrder]:
        """
        Returns a list of active orders for a connector.
        :param connector_name: The name of the connector.
        :return: A list of active orders
        """
        orders = self.order_tracker.active_limit_orders
        connector = self.connectors[connector_name]
        return [o[1] for o in orders if o[0] == connector]

    def get_assets(self, connector_name: str) -> List[str]:
        """
        Returns a unique list of unique of token names sorted alphabetically

        :param connector_name: The name of the connector

        :return: A list of token names
        """
        result: Set = set()
        for trading_pair in self.markets[connector_name]:
            result.update(split_hb_trading_pair(trading_pair))
        return sorted(result)

    def get_market_trading_pair_tuples(self) -> List[MarketTradingPairTuple]:
        """
        Returns a list of MarketTradingPairTuple for all connectors and trading pairs combination.
        """
        result: List[MarketTradingPairTuple] = []
        for name, connector in self.connectors.items():
            for trading_pair in self.markets[name]:
                result.append(self._market_trading_pair_tuple(name, trading_pair))
        return result

    def get_balance_df(self) -> pd.DataFrame:
        """
        Returns a data frame for all asset balances for displaying purpose.
        """
        columns: List[str] = ["Exchange", "Asset", "Total Balance", "Available Balance", "Value (USD)"]
        data: List[Any] = []
        for connector_name, connector in self.connectors.items():
            for asset in self.get_assets(connector_name):
                total_balance = float(connector.get_balance(asset))
                available_balance = float(connector.get_available_balance(asset))
                usd_rate = self._get_asset_usd_rate(connector_name=connector_name, asset=asset)
                usd_value = round(float(Decimal(str(total_balance)) * usd_rate), 2) if usd_rate is not None else ""
                data.append([connector_name,
                             asset,
                             total_balance,
                             available_balance,
                             usd_value])
        df = pd.DataFrame(data=data, columns=columns).replace(np.nan, '', regex=True)
        df.sort_values(by=["Exchange", "Asset"], inplace=True)
        return df

    def _get_balances_snapshot(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for connector_name, connector in self.connectors.items():
            for asset in self.get_assets(connector_name):
                total_balance = float(connector.get_balance(asset))
                available_balance = float(connector.get_available_balance(asset))
                usd_rate = self._get_asset_usd_rate(connector_name=connector_name, asset=asset)
                value_quote = round(float(Decimal(str(total_balance)) * usd_rate), 2) if usd_rate is not None else 0.0
                rows.append(
                    {
                        "exchange": connector_name,
                        "asset": asset,
                        "total_balance": total_balance,
                        "available_balance": available_balance,
                        "value_quote": value_quote,
                    }
                )
        rows.sort(key=lambda row: (row["exchange"], row["asset"]))
        return rows

    def _get_asset_usd_rate(self, connector_name: str, asset: str) -> Optional[Decimal]:
        asset = asset.upper()
        if asset in USD_STABLE_TOKENS:
            return Decimal("1")

        connector_pairs = self.markets.get(connector_name, [])
        for trading_pair in connector_pairs:
            try:
                base_asset, quote_asset = split_hb_trading_pair(trading_pair)
                mid_price = self.market_data_provider.get_price_by_type(
                    connector_name,
                    trading_pair,
                    PriceType.MidPrice,
                )
                if mid_price is None:
                    continue
                mid_price = Decimal(str(mid_price))
                if mid_price <= 0:
                    continue
                if base_asset == asset and quote_asset.upper() in USD_STABLE_TOKENS:
                    return mid_price
                if quote_asset == asset and base_asset.upper() in USD_STABLE_TOKENS:
                    return Decimal("1") / mid_price
            except Exception:
                continue

        oracle = RateOracle.get_instance()
        quote_candidates = [oracle.quote_token.upper()] if oracle.quote_token else []
        quote_candidates.extend(token for token in USD_STABLE_TOKENS if token not in quote_candidates)
        for quote_token in quote_candidates:
            try:
                rate = oracle.get_pair_rate(combine_to_hb_trading_pair(base=asset, quote=quote_token))
            except Exception:
                rate = None
            if rate is not None:
                return Decimal(str(rate))
        return None

    def active_orders_df(self) -> pd.DataFrame:
        """
        Return a data frame of all active orders for displaying purpose.
        """
        columns = ["Exchange", "Market", "Side", "Price", "Amount", "Age"]
        data = []
        for connector_name, connector in self.connectors.items():
            for order in self.get_active_orders(connector_name):
                age_txt = "n/a" if order.age() <= 0. else pd.Timestamp(order.age(), unit='s').strftime('%H:%M:%S')
                data.append([
                    connector_name,
                    order.trading_pair,
                    "buy" if order.is_buy else "sell",
                    float(order.price),
                    float(order.quantity),
                    age_txt
                ])
        if not data:
            raise ValueError
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Exchange", "Market", "Side"], inplace=True)
        return df

    def _runtime_status_snapshot_enabled(self) -> bool:
        return bool(getattr(self.config, "runtime_status_snapshot_enabled", False))

    def _runtime_db_sync_enabled(self) -> bool:
        return bool(getattr(self.config, "runtime_db_sync_enabled", False))

    def _chart_snapshots_enabled(self) -> bool:
        return bool(getattr(self.config, "chart_snapshots_enabled", True))

    def _chart_snapshot_base_retention_days(self) -> int:
        try:
            value = int(getattr(self.config, "chart_snapshot_base_retention_days", 30))
        except (TypeError, ValueError):
            value = 30
        return max(1, value)

    def _chart_snapshot_higher_interval_retention_days(self) -> int:
        try:
            value = int(getattr(self.config, "chart_snapshot_higher_interval_retention_days", 90))
        except (TypeError, ValueError):
            value = 90
        return max(1, value)

    def _chart_snapshot_prune_interval(self) -> int:
        try:
            value = int(getattr(self.config, "chart_snapshot_prune_interval", 3600))
        except (TypeError, ValueError):
            value = 3600
        return max(300, value)

    def _runtime_db_sync_interval(self) -> int:
        try:
            value = int(getattr(self.config, "runtime_db_sync_interval", 10))
        except (TypeError, ValueError):
            value = 10
        return max(5, value)

    def _sync_runtime_state_to_db_if_due(self):
        if not self._runtime_db_sync_enabled() or not self.ready_to_trade:
            return
        if self.current_timestamp - self._last_runtime_db_sync_ts < self._runtime_db_sync_interval():
            return
        try:
            self.executor_orchestrator.sync_live_state_to_db()
            self._last_runtime_db_sync_ts = self.current_timestamp
        except Exception as e:
            self.logger().warning(f"Failed to sync runtime state to db: {e}")

    def _write_chart_snapshots_if_due(self):
        if not self._chart_snapshots_enabled() or not self.ready_to_trade:
            return
        try:
            recorder = MarketsRecorder.get_instance()
        except Exception:
            return
        bot_name = self._runtime_status_snapshot_bot_name()
        prune_enabled = self.current_timestamp - self._last_chart_snapshot_prune_ts >= self._chart_snapshot_prune_interval()
        wrote_rows = False
        for controller in self.controllers.values():
            build_fn = getattr(controller, "build_chart_snapshot_rows", None)
            if not callable(build_fn):
                continue
            rows = build_fn(bot_name=bot_name)
            for row in rows:
                recorder.store_or_update_chart_snapshot(row)
                wrote_rows = True
            if prune_enabled:
                prune_fn = getattr(controller, "chart_snapshot_prune_requests", None)
                if callable(prune_fn):
                    for request in prune_fn():
                        recorder.prune_chart_snapshots(**request)
        if prune_enabled and wrote_rows:
            self._last_chart_snapshot_prune_ts = self.current_timestamp

    def _runtime_status_snapshot_interval(self) -> int:
        try:
            value = int(getattr(self.config, "runtime_status_snapshot_interval", 10))
        except (TypeError, ValueError):
            value = 10
        return max(5, value)

    def _runtime_status_snapshot_dir(self) -> Path:
        configured = str(getattr(self.config, "runtime_status_snapshot_dir", "data/runtime_status") or "data/runtime_status")
        base_path = Path(configured).expanduser()
        if base_path.is_absolute():
            return base_path
        return settings.root_path() / base_path

    def _runtime_status_snapshot_bot_name(self) -> str:
        try:
            config_file_path = MarketsRecorder.get_instance().config_file_path
        except Exception:
            config_file_path = ""
        config_name = Path(str(config_file_path or "")).name
        if config_name.endswith(".yml"):
            return config_name[:-4]
        if config_name.endswith(".yaml"):
            return config_name[:-5]
        script_name = Path(str(getattr(self.config, "script_file_name", "strategy_v2"))).stem
        return config_name or script_name or "strategy_v2"

    def _write_runtime_status_snapshot_if_due(self):
        if not self._runtime_status_snapshot_enabled() or not self.ready_to_trade:
            return
        if self.current_timestamp - self._last_runtime_status_snapshot_ts < self._runtime_status_snapshot_interval():
            return
        try:
            snapshot = self._build_runtime_status_snapshot()
            snapshot_dir = self._runtime_status_snapshot_dir()
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = snapshot_dir / f"{snapshot['bot_name']}.status.json"
            tmp_path = snapshot_path.with_suffix(f"{snapshot_path.suffix}.tmp")
            tmp_path.write_text(json.dumps(snapshot, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")
            tmp_path.replace(snapshot_path)
            self._last_runtime_status_snapshot_ts = self.current_timestamp
        except Exception as e:
            self.logger().warning(f"Failed to write runtime status snapshot: {e}")

    def _build_runtime_status_snapshot(self) -> Dict[str, Any]:
        controller_entries = self._build_controller_status_entries()
        balances = self._get_balances_snapshot()
        focus_queue = [
            {
                "pair": entry["summary"].get("pair", entry["controller_id"]),
                "focus": entry["summary"].get("focus", ""),
            }
            for entry in controller_entries
            if int(entry["summary"].get("attention_score", 0)) > 0
            and entry["summary"].get("note") not in ("", "monitoring")
        ][:5]

        active_executors: List[Dict[str, Any]] = []
        positions_held: List[Dict[str, Any]] = []
        controllers: List[Dict[str, Any]] = []

        for entry in controller_entries:
            summary = entry["summary"]
            pair = summary.get("pair", entry["controller_id"])
            controllers.append(
                {
                    "pair": pair,
                    "price_quote": float(self._safe_decimal(summary.get("price", 0))) if str(summary.get("price", "n/a")) != "n/a" else None,
                    "state": summary.get("state", "n/a"),
                    "signal": summary.get("signal", "n/a"),
                    "score": summary.get("score", "n/a"),
                    "breakeven_price": float(self._safe_decimal(summary.get("avg_buy", 0))) if str(summary.get("avg_buy", "n/a")) != "n/a" else None,
                    "unrealized_pnl_pct": float(self._safe_decimal(str(summary.get("u_pnl_pct", "0")).replace("%", ""))) if str(summary.get("u_pnl_pct", "n/a")) != "n/a" else None,
                    "execs": summary.get("execs", "n/a"),
                    "exposure_text": summary.get("exposure", "n/a"),
                    "gate": summary.get("gate", "n/a"),
                }
            )

            for executor in entry["executors"]:
                if not executor.is_active:
                    continue
                side = self._compact_side_label(executor.side or getattr(executor.config, "side", None))
                active_executors.append(
                    {
                        "pair": pair,
                        "side": side.lower() if side else None,
                        "executor_type": self._compact_executor_type(executor.type),
                        "status": (self._compact_enum_label(executor.status) or "unknown").lower(),
                        "age": self._format_executor_age_short(executor.timestamp),
                        "net_pnl_quote": float(self._safe_decimal(executor.net_pnl_quote)),
                        "net_pnl_pct": float(self._safe_decimal(executor.net_pnl_pct) * Decimal("100")),
                        "filled_amount_quote": float(self._safe_decimal(executor.filled_amount_quote)),
                    }
                )

            for position in entry["positions"]:
                positions_held.append(
                    {
                        "controller_id": entry["controller_id"],
                        "pair": pair,
                        "side": getattr(position.side, "name", str(position.side)).lower(),
                        "amount_base": float(self._safe_decimal(position.amount)),
                        "value_quote": float(self._safe_decimal(position.amount) * self._safe_decimal(position.breakeven_price)),
                        "breakeven_price": float(self._safe_decimal(position.breakeven_price)),
                        "unrealized_pnl_quote": float(self._safe_decimal(position.unrealized_pnl_quote)),
                        "realized_pnl_quote": float(self._safe_decimal(position.realized_pnl_quote)),
                        "fees_quote": float(self._safe_decimal(position.cum_fees_quote)),
                    }
                )

        active_orders_count = sum(len(self.get_active_orders(connector_name)) for connector_name in self.connectors)
        balance_value_quote = round(sum(row["value_quote"] for row in balances), 6)
        positions_value_quote = round(sum(row["value_quote"] for row in positions_held), 6)
        realized_pnl_quote = round(sum(row["realized_pnl_quote"] for row in positions_held), 6)
        unrealized_pnl_quote = round(sum(row["unrealized_pnl_quote"] for row in positions_held), 6)
        fees_quote = round(sum(row["fees_quote"] for row in positions_held), 6)

        return {
            "schema_version": 1,
            "bot_name": self._runtime_status_snapshot_bot_name(),
            "updated_at": pd.Timestamp(float(self.current_timestamp), unit="s", tz="UTC").isoformat(),
            "active_orders_count": active_orders_count,
            "balances": balances,
            "controllers": controllers,
            "focus_queue": focus_queue,
            "active_executors": active_executors,
            "positions_held": positions_held,
            "summary": {
                "balance_value_quote": balance_value_quote,
                "active_executor_count": len(active_executors),
                "positions_held_count": len(positions_held),
                "controller_count": len(controllers),
                "positions_value_quote": positions_value_quote,
                "realized_pnl_quote": realized_pnl_quote,
                "unrealized_pnl_quote": unrealized_pnl_quote,
                "fees_quote": fees_quote,
                "net_pnl_quote": round(realized_pnl_quote + unrealized_pnl_quote - fees_quote, 6),
            },
        }

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders.
        In V2 mode, also shows controller reports and performance summary.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            df = self.active_orders_df()
            lines.extend(["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        if self.controllers:
            # Controller sections
            performance_data = []
            global_realized = Decimal("0")
            global_unrealized = Decimal("0")
            global_total = Decimal("0")
            global_volume = Decimal("0")
            controller_entries = self._build_controller_status_entries()
            # Operator console: shared summary layout (SIGNALS / PLANS / runtime tables).
            # Per-controller sections are suppressed entirely when ``to_format_status()`` returns an empty body;
            # the ACTIVE EXECUTORS / POSITIONS HELD tables already carry the per-runtime detail.
            lines.extend(self._signals_summary_lines(controller_entries))
            lines.extend(self._plans_summary_lines(controller_entries))
            lines.extend(self._active_executor_lines(controller_entries))
            lines.extend(self._positions_summary_lines(controller_entries))

            for entry in controller_entries:
                controller_id = entry["controller_id"]
                controller = entry["controller"]
                executors_list = entry["executors"]
                performance_report = entry["performance"]

                controller_body: List[str] = []
                try:
                    controller_body = list(controller.to_format_status())
                except Exception as e:
                    self.logger().error(f"Error formatting status for controller {controller_id}: {e}", exc_info=True)
                    controller_body = self._controller_status_error_lines(controller_id, e)

                if controller_body:
                    active_executors = [executor for executor in executors_list if executor.is_active]
                    exec_stats_line = (
                        self._active_executor_stats_line(active_executors) if active_executors else None
                    )
                    lines.append(f"\n{self._controller_section_header(entry)}")
                    lines.extend(controller_body)
                    if exec_stats_line is not None:
                        lines.append(exec_stats_line)

                # Collect performance data for summary table
                if performance_report:
                    realized_pnl_quote = self._safe_decimal(getattr(performance_report, "realized_pnl_quote", 0))
                    unrealized_pnl_quote = self._safe_decimal(getattr(performance_report, "unrealized_pnl_quote", 0))
                    global_pnl_quote = self._safe_decimal(getattr(performance_report, "global_pnl_quote", 0))
                    volume_traded = self._safe_decimal(getattr(performance_report, "volume_traded", 0))
                    global_realized += realized_pnl_quote
                    global_unrealized += unrealized_pnl_quote
                    global_total += global_pnl_quote
                    global_volume += volume_traded
                    performance_data.append({
                        "Controller": controller_id,
                        "Realized PnL": f"${realized_pnl_quote:.2f}",
                        "Unrealized PnL": f"${unrealized_pnl_quote:.2f}",
                        "Global PnL": f"${global_pnl_quote:.2f}",
                        "Global PnL %": f"{self._safe_decimal(getattr(performance_report, 'global_pnl_pct', 0)):.2f}%",
                        "Volume Traded": f"${volume_traded:.2f}"
                    })

            # Performance summary table
            if performance_data:
                lines.append(f"\n{self._status_section_title('PERFORMANCE SUMMARY')}")

                global_pnl_pct = (global_total / global_volume) * 100 if global_volume > 0 else Decimal(0)

                # Add global row
                performance_data.append({
                    "Controller": "GLOBAL TOTAL",
                    "Realized PnL": f"${global_realized:.2f}",
                    "Unrealized PnL": f"${global_unrealized:.2f}",
                    "Global PnL": f"${global_total:.2f}",
                    "Global PnL %": f"{global_pnl_pct:.2f}%",
                    "Volume Traded": f"${global_volume:.2f}"
                })

                performance_df = pd.DataFrame(performance_data)
                lines.append(format_df_for_printout(performance_df, table_format="psql", index=False))
        else:
            # Simple mode: just warnings
            warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
            if len(warning_lines) > 0:
                lines.extend(["", "*** WARNINGS ***"] + warning_lines)
            return "\n".join(lines)

        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)

    def _market_trading_pair_tuple(self,
                                   connector_name: str,
                                   trading_pair: str) -> MarketTradingPairTuple:
        """
        Creates and returns a new MarketTradingPairTuple

        :param connector_name: The name of the connector
        :param trading_pair: The trading pair
        :return: A new MarketTradingPairTuple object.
        """
        base, quote = split_hb_trading_pair(trading_pair)
        return MarketTradingPairTuple(self.connectors[connector_name], trading_pair, base, quote)

    # -------------------------------------------------------------------------
    # V2-specific methods
    # -------------------------------------------------------------------------

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()
        # Check if MQTT is enabled at runtime
        from hummingbot.client.hummingbot_application import HummingbotApplication
        if HummingbotApplication.main_application()._mqtt is not None:
            self.mqtt_enabled = True
            self._pub = ETopicPublisher("performance", use_bot_prefix=True)

        # Start controllers
        for controller in self.controllers.values():
            controller.start()
        self._controllers_started = True

    def apply_initial_setting(self):
        """
        Apply initial settings for the strategy, such as setting position mode and leverage for all connectors.
        """
        pass

    def _collect_initial_positions(self) -> Dict[str, List]:
        """
        Collect initial positions from all controller configurations.
        Returns a dictionary mapping controller_id -> list of InitialPositionConfig.
        """
        if not self.config:
            return {}

        initial_positions_by_controller = {}
        try:
            controllers_configs = self.config.load_controller_configs()
            for controller_config in controllers_configs:
                if hasattr(controller_config, 'initial_positions') and controller_config.initial_positions:
                    initial_positions_by_controller[controller_config.id] = controller_config.initial_positions
        except Exception as e:
            self.logger().error(f"Error collecting initial positions: {e}", exc_info=True)

        return initial_positions_by_controller

    def initialize_controllers(self):
        """
        Initialize the controllers based on the provided configuration.
        """
        controllers_configs = self.config.load_controller_configs()
        for controller_config in controllers_configs:
            self.add_controller(controller_config)
            MarketsRecorder.get_instance().store_controller_config(controller_config)

    def add_controller(self, config: ControllerConfigBase):
        try:
            # Generate unique ID if not set to avoid race conditions
            if not config.id or config.id.strip() == "":
                from hummingbot.strategy_v2.utils.common import generate_unique_id
                config.id = generate_unique_id()
            controller = config.get_controller_class()(config, self.market_data_provider, self.actions_queue)
            self.controllers[config.id] = controller
            if self._controllers_started:
                controller.start()
        except Exception as e:
            self.logger().error(f"Error adding controller: {e}", exc_info=True)

    @staticmethod
    def _copy_controller_runtime_state(source_controller: ControllerBase, target_controller: ControllerBase):
        transfer_guard = getattr(target_controller, "validate_runtime_state_transfer", None)
        if callable(transfer_guard):
            transfer_guard(source_controller)
        target_controller.executors_info = list(source_controller.executors_info)
        target_controller.positions_held = list(source_controller.positions_held)
        target_controller.performance_report = source_controller.performance_report
        source_processed = source_controller.processed_data
        if isinstance(source_processed, dict):
            target_controller.processed_data = dict(source_processed)
        else:
            target_controller.processed_data = source_processed
        target_controller.executors_update_event.set()

    def _replace_controller(self, controller_config: ControllerConfigBase):
        current_controller = self.controllers.get(controller_config.id)
        if current_controller is None:
            self.add_controller(controller_config)
            return

        if getattr(current_controller, "allow_implementation_hot_reload", True) is False:
            raise RuntimeError("Controller implementation changed; restart with ownership reconciliation is required")
        replacement_class = controller_config.get_controller_class()
        replacement_controller = replacement_class(controller_config, self.market_data_provider, self.actions_queue)
        self._copy_controller_runtime_state(current_controller, replacement_controller)

        current_controller.stop()
        self.controllers[controller_config.id] = replacement_controller
        if self._controllers_started:
            replacement_controller.start()

        self.logger().info(
            "Reloaded controller implementation | "
            f"controller={controller_config.id} "
            f"old_class={current_controller.__class__.__name__} "
            f"new_class={replacement_class.__name__}"
        )

    def update_controllers_configs(self):
        """
        Update the controllers configurations based on the provided configuration.
        """
        if not isinstance(self.config, StrategyV2ConfigBase):
            return
        if self._last_config_update_ts + self.config_update_interval < self.current_timestamp:
            self._last_config_update_ts = self.current_timestamp
            try:
                controllers_configs = self.config.load_controller_configs(reload_changed_modules=True)
                for controller_config in controllers_configs:
                    current_controller = self.controllers.get(controller_config.id)
                    if current_controller is None:
                        self.add_controller(controller_config)
                        continue

                    if current_controller.__class__ is not controller_config.get_controller_class():
                        self._replace_controller(controller_config)
                    else:
                        current_controller.update_config(controller_config)

                if self._last_controller_reload_error is not None:
                    self.logger().info("Controller config reload recovered.")
                    self._last_controller_reload_error = None
            except Exception as e:
                error_label = f"{type(e).__name__}: {e}"
                if error_label != self._last_controller_reload_error:
                    self.logger().error(f"Controller config reload failed: {error_label}", exc_info=True)
                    self._last_controller_reload_error = error_label

    def validate_create_action(self, action: CreateExecutorAction) -> bool:
        return not self._is_stop_triggered

    async def listen_to_executor_actions(self):
        """
        Asynchronously listen to actions from the controllers and execute them.
        """
        while True:
            try:
                actions = await self.actions_queue.get()
                self.update_executors_info()
                for action in actions:
                    controller = self.controllers.get(action.controller_id)
                    if isinstance(action, CreateExecutorAction):
                        if not self.validate_create_action(action):
                            continue
                        if (self._is_stop_triggered or not self._controller_reports_valid or controller is None
                                or controller.status != RunnableStatus.RUNNING or controller.config.manual_kill_switch):
                            self.logger().warning(f"Rejected queued create for stopped or unavailable controller {action.controller_id}")
                            continue
                    self.executor_orchestrator.execute_actions([action])
                    self.update_executors_info()
                for controller_id in {action.controller_id for action in actions}:
                    controller = self.controllers.get(controller_id)
                    if controller is not None:
                        controller.executors_info = self.get_executors_by_controller(controller_id)
                        controller.executors_update_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error executing action: {e}", exc_info=True)

    def update_executors_info(self):
        """
        Update the unified controller reports and publish the updates to the active controllers.
        """
        try:
            self._controller_reports_valid = False
            # Get all reports in a single call and store them
            self.controller_reports = self.executor_orchestrator.get_all_reports()

            # Update each controller with its specific data
            for controller_id, controller in self.controllers.items():
                controller_report = self.controller_reports.get(controller_id, {})
                controller.executors_info = controller_report.get("executors", [])
                controller.positions_held = controller_report.get("positions", [])
                controller.performance_report = controller_report.get("performance", [])
                controller.executors_update_event.set()
            self._controller_reports_valid = True
        except Exception as e:
            self._controller_reports_valid = False
            self.logger().error(f"Error updating controller reports: {e}", exc_info=True)

    @staticmethod
    def is_perpetual(connector: str) -> bool:
        return "perpetual" in connector

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determine actions based on the provided executor handler report.
        """
        actions = []
        actions.extend(self.create_actions_proposal())
        actions.extend(self.stop_actions_proposal())
        actions.extend(self.store_actions_proposal())
        return actions

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        Create actions proposal based on the current state of the executors.
        """
        raise NotImplementedError

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Create a list of actions to stop the executors based on order refresh and early stop conditions.
        """
        raise NotImplementedError

    def store_actions_proposal(self) -> List[StoreExecutorAction]:
        """
        Create a list of actions to store the executors that have been stopped.
        """
        potential_executors_to_store = self.filter_executors(
            executors=self.get_active_executors(),
            filter_func=lambda x: x.is_done)
        sorted_executors = sorted(potential_executors_to_store, key=lambda x: x.timestamp, reverse=True)
        if len(sorted_executors) > self.closed_executors_buffer:
            return [StoreExecutorAction(executor_id=executor.id, controller_id=executor.controller_id) for executor in
                    sorted_executors[self.closed_executors_buffer:]]
        return []

    def get_executors_by_controller(self, controller_id: str) -> List[ExecutorInfo]:
        """Get executors for a specific controller from the unified reports."""
        return self.controller_reports.get(controller_id, {}).get("executors", [])

    def get_active_executors_by_controller(self, controller_id: str) -> List[ExecutorInfo]:
        """Get live in-memory executors for a specific controller."""
        return [
            executor.executor_info
            for executor in self.executor_orchestrator.active_executors.get(controller_id, [])
            if executor
        ]

    def get_active_executors(self) -> List[ExecutorInfo]:
        """Get all live in-memory executors across controllers."""
        return [
            executor_info
            for controller_id in self.executor_orchestrator.active_executors.keys()
            for executor_info in self.get_active_executors_by_controller(controller_id)
        ]

    def get_all_executors(self) -> List[ExecutorInfo]:
        """Get all executors from all controllers."""
        return [executor for executors_list in [report.get("executors", []) for report in self.controller_reports.values()] for executor in executors_list]

    def get_positions_by_controller(self, controller_id: str) -> List[PositionSummary]:
        """Get positions for a specific controller from the unified reports."""
        return self.controller_reports.get(controller_id, {}).get("positions", [])

    def get_performance_report(self, controller_id: str):
        """Get performance report for a specific controller."""
        return self.controller_reports.get(controller_id, {}).get("performance")

    def _safe_controller_status_summary(self, controller_id: str, controller: ControllerBase) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        try:
            summary = controller.get_status_summary() or {}
        except Exception as e:
            self.logger().error(f"Error building status summary for controller {controller_id}: {e}", exc_info=True)

        config = getattr(controller, "config", None)
        trading_pair = summary.get("pair") or getattr(config, "trading_pair", None) or controller_id
        normalized_summary = {
            "controller_id": controller_id,
            "pair": trading_pair,
            "price": summary.get("price", "n/a"),
            "timestamp": summary.get("timestamp", "n/a"),
            "state": summary.get("state", "n/a"),
            "signal": summary.get("signal", "n/a"),
            "score": summary.get("score", "n/a"),
            "regime": summary.get("regime", "n/a"),
            "avg_buy": summary.get("avg_buy", "n/a"),
            "exposure": summary.get("exposure", "n/a"),
            "execs": summary.get("execs", "n/a"),
            "u_pnl": summary.get("u_pnl", "n/a"),
            "u_pnl_pct": summary.get("u_pnl_pct", "n/a"),
            "rsi": summary.get("rsi"),
            "rsi_proximity": summary.get("rsi_proximity"),
            "buy_gauge": summary.get("buy_gauge"),
            "sell_gauge": summary.get("sell_gauge"),
            "gates_mask": summary.get("gates_mask"),
            "gate": summary.get("gate", "n/a"),
            "note": summary.get("note", ""),
            "focus": summary.get("focus") or summary.get("note", ""),
            "attention_score": summary.get("attention_score", 0),
            "plan_mode": summary.get("plan_mode"),
            "plan_target": summary.get("plan_target"),
            "plan_anchor": summary.get("plan_anchor"),
            "plan_next": summary.get("plan_next"),
            "plan_slice": summary.get("plan_slice"),
            "plan_liquidity": summary.get("plan_liquidity"),
            "plan_sellable": summary.get("plan_sellable"),
            "plan_held": summary.get("plan_held"),
            "plan_active": summary.get("plan_active"),
            "plan_untracked": summary.get("plan_untracked"),
            "plan_free": summary.get("plan_free"),
            "plan_managed": summary.get("plan_managed"),
        }
        return normalized_summary

    def _build_controller_status_entries(self) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for controller_id, controller in self.controllers.items():
            executors = self.get_executors_by_controller(controller_id)
            positions = self.get_positions_by_controller(controller_id)
            performance = self.get_performance_report(controller_id)
            summary = self._safe_controller_status_summary(controller_id, controller)
            summary = self._enrich_controller_status_summary(
                summary=summary,
                executors=executors,
                positions=positions,
                performance=performance,
            )
            entries.append({
                "controller_id": controller_id,
                "controller": controller,
                "summary": summary,
                "executors": executors,
                "positions": positions,
                "performance": performance,
            })
        entries.sort(
            key=lambda entry: (
                str(entry["summary"].get("pair", entry["controller_id"])),
                entry["controller_id"],
            )
        )
        return entries

    @staticmethod
    def _safe_decimal(value: Any) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    @classmethod
    def _format_summary_quote(cls, value: Any) -> str:
        return f"${cls._safe_decimal(value):.2f}"

    @classmethod
    def _format_summary_quote_pct(cls, quote_value: Any, pct_value: Any) -> str:
        quote = cls._format_summary_quote(quote_value)
        try:
            pct = f"{cls._safe_decimal(pct_value):.2f}%"
        except Exception:
            pct = "n/a"
        return f"{quote} / {pct}"

    @classmethod
    def _enrich_controller_status_summary(
        cls,
        *,
        summary: Dict[str, Any],
        executors: List[ExecutorInfo],
        positions: List[PositionSummary],
        performance: Any,
    ) -> Dict[str, Any]:
        enriched = dict(summary)
        active_executors = [executor for executor in executors if executor.is_active]
        active_buy_execs = sum(
            1
            for executor in active_executors
            if cls._compact_side_label(executor.side or getattr(executor.config, "side", None)) == "BUY"
        )
        active_sell_execs = sum(
            1
            for executor in active_executors
            if cls._compact_side_label(executor.side or getattr(executor.config, "side", None)) == "SELL"
        )
        held_positions = sum(1 for position in positions if getattr(position, "amount", Decimal("0")) > 0)

        if performance is not None:
            realized_pnl_quote = cls._safe_decimal(getattr(performance, "realized_pnl_quote", 0))
            unrealized_pnl_quote = cls._safe_decimal(getattr(performance, "unrealized_pnl_quote", 0))
            unrealized_pnl_pct = getattr(performance, "unrealized_pnl_pct", None)
            global_pnl_quote = cls._safe_decimal(getattr(performance, "global_pnl_quote", 0))
            global_pnl_pct = getattr(performance, "global_pnl_pct", None)
        else:
            realized_pnl_quote = sum((cls._safe_decimal(position.realized_pnl_quote) for position in positions), Decimal("0"))
            unrealized_pnl_quote = sum((cls._safe_decimal(position.unrealized_pnl_quote) for position in positions), Decimal("0"))
            global_pnl_quote = sum((cls._position_global_pnl_quote(position) for position in positions), Decimal("0"))
            unrealized_pnl_pct = None
            global_pnl_pct = None

        focus_note = enriched.get("note", "")
        enriched["focus"] = focus_note
        enriched["execs"] = f"A{len(active_executors)} | B{active_buy_execs} S{active_sell_execs} H{held_positions}"
        enriched["r_pnl"] = cls._format_summary_quote(realized_pnl_quote)
        enriched["u_pnl"] = cls._format_summary_quote_pct(unrealized_pnl_quote, unrealized_pnl_pct)
        enriched["u_pnl_pct"] = f"{cls._safe_decimal(unrealized_pnl_pct):.2f}%" if unrealized_pnl_pct is not None else enriched.get("u_pnl_pct", "n/a")
        enriched["g_pnl"] = cls._format_summary_quote_pct(global_pnl_quote, global_pnl_pct)
        return enriched

    @classmethod
    def _position_global_pnl_quote(cls, position: Any) -> Decimal:
        global_pnl = getattr(position, "global_pnl_quote", None)
        if global_pnl is not None:
            return cls._safe_decimal(global_pnl)
        return (
            cls._safe_decimal(getattr(position, "unrealized_pnl_quote", 0))
            + cls._safe_decimal(getattr(position, "realized_pnl_quote", 0))
            - cls._safe_decimal(getattr(position, "cum_fees_quote", 0))
        )

    @classmethod
    def _ranked_controller_entries(
        cls, controller_entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Sort controller entries by ``attention_score`` desc, then by pair / id for stability.

        Used by the ``SIGNALS`` / ``PLANS`` summary tables so high-attention
        controllers bubble to the top and the tables share a single, predictable
        row ordering for the operator's eye to scan.
        """
        return sorted(
            controller_entries,
            key=lambda entry: (
                -int(entry["summary"].get("attention_score", 0) or 0),
                str(entry["summary"].get("pair", entry["controller_id"])),
                entry["controller_id"],
            ),
        )

    @classmethod
    def _status_section_title(cls, title: str) -> str:
        icon = cls.STATUS_SECTION_ICONS.get(title)
        return f"{icon} {title}" if icon else title

    @classmethod
    def _signals_summary_lines(cls, controller_entries: List[Dict[str, Any]]) -> List[str]:
        """Render the operator-console ``SIGNALS`` table.

        Columns: ``pair / timestamp / price / avg / rsi / buy / sell / gates /
        focus``. Buy and sell are ASCII gauges (``██░░░░░░░░ 24%``) for proximity
        to the BUY trigger and the SELL profit target; ``gates`` is the compact
        gate truth table (HOLDING) or buy-gate label (FLAT). Controllers that do
        not provide gauge / proximity fields render ``—`` so non-RSI strategies
        degrade gracefully. The table is skipped entirely when no controller
        exposes any signal field.
        """
        if len(controller_entries) == 0:
            return []

        ranked_entries = cls._ranked_controller_entries(controller_entries)
        signal_rows = []
        any_signal_field = False
        for entry in ranked_entries:
            summary = entry["summary"]
            rsi_cell = summary.get("rsi_proximity") or summary.get("rsi") or "—"
            buy_cell = summary.get("buy_gauge") or "—"
            sell_cell = summary.get("sell_gauge") or "—"
            gates_cell = summary.get("gates_mask") or "—"
            focus_cell = summary.get("focus") or summary.get("note") or "—"
            if any(value not in (None, "", "—") for value in (
                summary.get("timestamp"),
                summary.get("price"),
                summary.get("avg_buy"),
                rsi_cell,
                buy_cell,
                sell_cell,
                gates_cell,
                focus_cell,
            )):
                any_signal_field = True
            signal_rows.append({
                "pair": summary.get("pair", entry["controller_id"]),
                "timestamp": summary.get("timestamp", "n/a"),
                "price": summary.get("price", "n/a"),
                "avg": summary.get("avg_buy", "n/a"),
                "rsi": rsi_cell,
                "buy": buy_cell,
                "sell": sell_cell,
                "gates": gates_cell,
                "focus": focus_cell,
            })

        if not any_signal_field:
            return []

        signals_df = pd.DataFrame(signal_rows)
        return [
            f"\n{cls._status_section_title('SIGNALS')}",
            format_df_for_printout(signals_df, table_format="psql", index=False),
        ]

    @staticmethod
    def _status_cell_is_empty(value: Any) -> bool:
        return value in (None, "", "—")

    @classmethod
    def _inventory_cells_from_summary(cls, summary: Dict[str, Any]) -> Dict[str, str]:
        """Normalize legacy ``plan_liquidity`` into explicit inventory columns.

        Older controller summaries only exposed ``plan_liquidity``. Treating that
        value as ``sellable`` made flat entry rows show idle wallet cash in the
        sellable column. Keep the compatibility path, but classify ``idle`` /
        ``free`` text as wallet headroom instead of inventory.
        """
        liquidity = str(summary.get("plan_liquidity") or "")
        sellable = summary.get("plan_sellable")
        free = summary.get("plan_free") or summary.get("plan_untracked")

        if cls._status_cell_is_empty(sellable):
            if liquidity.startswith("sellable "):
                sellable_part, _, free_part = liquidity.partition(" · ")
                sellable = sellable_part.removeprefix("sellable ").strip() or "—"
                if cls._status_cell_is_empty(free) and free_part.startswith(("idle ", "free ")):
                    free = free_part
            elif liquidity.startswith("sell "):
                sellable_part, _, free_part = liquidity.partition(" · ")
                sellable = sellable_part.removeprefix("sell ").strip() or "—"
                if cls._status_cell_is_empty(free) and free_part.startswith(("idle ", "free ")):
                    free = free_part
            else:
                sellable = "—"

        if cls._status_cell_is_empty(free):
            free = liquidity if liquidity.startswith(("idle ", "free ")) else "—"

        return {
            "sellable": str(sellable or "—"),
            "bag": str(summary.get("plan_held") or "—"),
            "active": str(summary.get("plan_active") or "—"),
            "free": str(free or "—"),
            "managed": str(summary.get("plan_managed") or "—"),
        }

    @classmethod
    def _compact_inventory_cell(cls, inventory_cells: Dict[str, str]) -> str:
        """Render inventory reconciliation as one scan-friendly PLANS cell."""
        sellable = inventory_cells.get("sellable", "—")
        bag = inventory_cells.get("bag", "—")
        active = inventory_cells.get("active", "—")
        free = inventory_cells.get("free", "—")
        managed = inventory_cells.get("managed", "—")

        parts: List[str] = []
        if not cls._status_cell_is_empty(sellable):
            parts.append(f"sell {sellable}")
        if not cls._status_cell_is_empty(bag):
            parts.append(f"bag {bag}")
        if not cls._status_cell_is_empty(active) and active != "$0":
            parts.append(f"active {active}")
        if not cls._status_cell_is_empty(free) and free != "$0":
            parts.append(str(free))
        if (
            not cls._status_cell_is_empty(managed)
            and managed not in ("$0", bag)
        ):
            parts.append(f"managed {managed}")
        return " · ".join(parts) if parts else "—"

    @classmethod
    def _plans_summary_lines(cls, controller_entries: List[Dict[str, Any]]) -> List[str]:
        """Render the operator-console ``PLANS`` table.

        The ``PLANS`` table carries action mechanics plus compact inventory
        reconciliation: ``pair / mode / target / anchor / next / slice /
        inventory``. Cell semantics adapt per ``plan_mode``:

        - ``EXITS``   → ``anchor`` = min-profit price, ``next`` = recovery arm price.
        - ``TRAIL``   → ``anchor`` = peak price, ``next`` = pullback % + partial flag.
        - ``ENTRIES`` → ``target`` = ``rsi≤<buy_threshold>``, ``anchor`` / ``next`` =
          scout / runner slices.

        Controllers that do not expose ``plan_*`` fields render ``—`` so non-RSI
        strategies still appear in the table. The table is skipped entirely when
        no controller exposes any plan or inventory field.
        """
        if len(controller_entries) == 0:
            return []

        ranked_entries = cls._ranked_controller_entries(controller_entries)
        plan_rows = []
        any_plan_field = False
        any_inventory_field = False
        for entry in ranked_entries:
            summary = entry["summary"]
            plan_mode = summary.get("plan_mode") or "—"
            plan_target = summary.get("plan_target") or "—"
            plan_anchor = summary.get("plan_anchor") or "—"
            plan_next = summary.get("plan_next") or "—"
            plan_slice = summary.get("plan_slice") or "—"
            inventory_cells = cls._inventory_cells_from_summary(summary)
            if any(value not in (None, "", "—") for value in (
                plan_mode,
                plan_target,
                plan_anchor,
                plan_next,
                plan_slice,
            )):
                any_plan_field = True
            if any(value not in (None, "", "—") for value in (
                inventory_cells["sellable"],
                inventory_cells["bag"],
                inventory_cells["active"],
                inventory_cells["free"],
                inventory_cells["managed"],
            )):
                any_inventory_field = True
            plan_rows.append({
                "pair": summary.get("pair", entry["controller_id"]),
                "mode": plan_mode,
                "target": plan_target,
                "anchor": plan_anchor,
                "next": plan_next,
                "slice": plan_slice,
                "inventory": cls._compact_inventory_cell(inventory_cells),
            })

        if not any_plan_field and not any_inventory_field:
            return []

        plans_df = pd.DataFrame(plan_rows)
        return [
            f"\n{cls._status_section_title('PLANS')}",
            format_df_for_printout(plans_df, table_format="psql", index=False),
        ]

    @staticmethod
    def _controller_attention_lines(controller_entries: List[Dict[str, Any]]) -> List[str]:
        """Deprecated: focus information is now folded into the SIGNALS ``focus`` column.

        Kept as a no-op so external callers / regression tests that rely on the API
        keep working; the live ``format_status()`` render path no longer calls it.
        """
        return []

    def _active_executor_lines(self, controller_entries: List[Dict[str, Any]]) -> List[str]:
        active_rows: List[Dict[str, Any]] = []
        for entry in controller_entries:
            pair = entry["summary"].get("pair", entry["controller_id"])
            for executor in entry["executors"]:
                if not executor.is_active:
                    continue
                side = self._compact_side_label(executor.side or getattr(executor.config, "side", None)) or "n/a"
                executor_type = self._compact_executor_type(executor.type)
                status_label = self._compact_enum_label(executor.status) or "unknown"
                active_rows.append({
                    "pair": pair,
                    "side": side,
                    "type": executor_type,
                    "status": status_label,
                    "age": self._format_executor_age_short(executor.timestamp),
                    "PnL": self._format_summary_quote_pct(
                        executor.net_pnl_quote,
                        self._safe_decimal(executor.net_pnl_pct) * Decimal("100"),
                    ),
                    "Fill": self._format_summary_quote(executor.filled_amount_quote),
                })
                custom_fields = self._executor_custom_status_columns(executor.custom_info or {})
                compact_fields = {
                    "level": custom_fields.get("level"),
                    "entry": custom_fields.get("avg_entry")
                    or self._format_executor_compact_decimal(
                        getattr(executor.config, "entry_price", None),
                        precision=6,
                    ),
                    "trail": custom_fields.get("trail"),
                    "trail_gap": custom_fields.get("trail_gap"),
                    "retries": custom_fields.get("retries"),
                }
                active_rows[-1].update(
                    {key: value for key, value in compact_fields.items() if value not in (None, "")}
                )

        if len(active_rows) == 0:
            return []

        active_df = pd.DataFrame(active_rows)
        return [
            f"\n{self._status_section_title('ACTIVE EXECUTORS')} ({len(active_rows)})",
            format_df_for_printout(active_df, table_format="psql", index=False),
        ]

    def _active_executor_stats_line(self, active_executors: List[ExecutorInfo]) -> str:
        buy_count = 0
        sell_count = 0
        total_pnl_quote = Decimal("0")
        total_pnl_pct = Decimal("0")
        total_fill_quote = Decimal("0")
        oldest_timestamp = None
        trail_counts: Dict[str, int] = {}

        for executor in active_executors:
            side = self._compact_side_label(executor.side or getattr(executor.config, "side", None))
            if side == "BUY":
                buy_count += 1
            elif side == "SELL":
                sell_count += 1

            total_pnl_quote += self._safe_decimal(executor.net_pnl_quote)
            total_pnl_pct += self._safe_decimal(executor.net_pnl_pct)
            total_fill_quote += self._safe_decimal(executor.filled_amount_quote)

            timestamp = getattr(executor, "timestamp", None)
            if timestamp is not None and (oldest_timestamp is None or timestamp < oldest_timestamp):
                oldest_timestamp = timestamp

            trailing_state = (executor.custom_info or {}).get("trailing_state")
            if trailing_state not in (None, "", "disabled"):
                trail_label = str(trailing_state)
                trail_counts[trail_label] = trail_counts.get(trail_label, 0) + 1

        avg_pnl_pct = total_pnl_pct / Decimal(len(active_executors))
        oldest_age = self._format_executor_age_short(oldest_timestamp) if oldest_timestamp is not None else "n/a"
        trail_summary = ", ".join(f"{state}:{count}" for state, count in sorted(trail_counts.items())) or "none"
        return (
            "   active_exec "
            f"total={len(active_executors)} | "
            f"buy={buy_count} sell={sell_count} | "
            f"pnl={self._format_summary_quote_pct(total_pnl_quote, avg_pnl_pct * Decimal('100'))} | "
            f"fill={self._format_summary_quote(total_fill_quote)} | "
            f"oldest={oldest_age} | "
            f"trail={trail_summary}"
        )

    @classmethod
    def _positions_summary_lines(cls, controller_entries: List[Dict[str, Any]]) -> List[str]:
        position_rows: List[Dict[str, Any]] = []
        for entry in controller_entries:
            pair = entry["summary"].get("pair", entry["controller_id"])
            for position in entry["positions"]:
                position_rows.append({
                    "pair": pair,
                    "amount": f"{position.amount:.4f}",
                    "value": f"${position.amount * position.breakeven_price:.2f}",
                    "breakeven": f"{position.breakeven_price:.6f}",
                    "uPnL": f"${position.unrealized_pnl_quote:+.2f}",
                    "rPnL": f"${position.realized_pnl_quote:+.2f}",
                    "fees": f"${position.cum_fees_quote:.2f}",
                })

        if len(position_rows) == 0:
            return []

        positions_df = pd.DataFrame(position_rows)
        return [
            f"\n{cls._status_section_title('POSITIONS HELD')} ({len(position_rows)})",
            format_df_for_printout(positions_df, table_format="psql", index=False),
        ]

    @staticmethod
    def _controller_section_header(entry: Dict[str, Any]) -> str:
        summary = entry["summary"]
        return (
            f"Controller: {entry['controller_id']} | "
            f"{summary.get('pair', entry['controller_id'])} | "
            f"{summary.get('state', 'n/a')} | "
            f"{summary.get('signal', 'n/a')}"
        )

    @staticmethod
    def _controller_status_error_lines(controller_id: str, exception: Exception) -> List[str]:
        error_label = f"{type(exception).__name__}: {exception}"
        return [
            f"  Status formatter error for `{controller_id}`: {error_label}",
            "  Controller-specific status is unavailable; shared executor and position data continue below.",
        ]

    @staticmethod
    def _format_executor_status_value(value: Any) -> Any:
        if value is None:
            return None
        if hasattr(value, "name") and hasattr(value, "value"):
            return value.name
        if isinstance(value, list):
            if len(value) == 0:
                return 0
            preview_items = ", ".join(str(item) for item in value[:2])
            overflow = f" (+{len(value) - 2})" if len(value) > 2 else ""
            return f"{preview_items}{overflow}"
        if isinstance(value, dict):
            return f"{len(value)} keys"
        if isinstance(value, float):
            return round(value, 2)
        return value

    @staticmethod
    def _format_executor_status_pct(value: Any, precision: int = 2) -> Optional[str]:
        if value is None:
            return None
        try:
            return f"{Decimal(str(value)) * Decimal('100'):.{precision}f}%"
        except Exception:
            return None

    @staticmethod
    def _format_executor_status_price(value: Any, precision: int = 6) -> Optional[str]:
        if value is None:
            return None
        try:
            return f"{Decimal(str(value)):.{precision}f}"
        except Exception:
            return None

    @classmethod
    def _executor_custom_status_columns(cls, custom_info: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not custom_info:
            return {}

        fields: Dict[str, Any] = {}
        direct_mappings = {
            "level_id": "level",
            "current_position_average_price": "avg_entry",
            "close_price": "close_price",
        }
        for source_key, target_key in direct_mappings.items():
            formatted_value = cls._format_executor_status_value(custom_info.get(source_key))
            if formatted_value is not None:
                fields[target_key] = formatted_value

        current_retries = custom_info.get("current_retries")
        max_retries = custom_info.get("max_retries")
        if current_retries is not None or max_retries is not None:
            if current_retries is not None and max_retries is not None:
                fields["retries"] = f"{current_retries}/{max_retries}"
            elif current_retries is not None:
                fields["retries"] = cls._format_executor_status_value(current_retries)
            else:
                fields["retries"] = cls._format_executor_status_value(max_retries)

        order_id = custom_info.get("order_id")
        order_ids = custom_info.get("order_ids")
        if order_id:
            fields["order_ref"] = order_id
        elif order_ids:
            fields["order_ref"] = cls._format_executor_status_value(order_ids)

        if "held_position_orders" in custom_info:
            held_position_orders = custom_info.get("held_position_orders") or []
            fields["held_orders"] = len(held_position_orders)

        trailing_state = custom_info.get("trailing_state")
        if trailing_state not in (None, "disabled"):
            trailing_move_count = custom_info.get("trailing_move_count") or 0
            if trailing_state == "armed" and trailing_move_count:
                fields["trail"] = f"armed/{trailing_move_count}"
            else:
                fields["trail"] = trailing_state

            trailing_activation_price = cls._format_executor_status_price(custom_info.get("trailing_activation_price"))
            trailing_trigger_price = cls._format_executor_status_price(custom_info.get("trailing_trigger_price"))
            if trailing_activation_price is not None:
                fields["trail_arm"] = trailing_activation_price
            if trailing_trigger_price is not None:
                fields["trail_stop"] = trailing_trigger_price

            if trailing_state in ("pending", "waiting"):
                trailing_gap = cls._format_executor_status_pct(custom_info.get("distance_to_trailing_activation_pct"))
            else:
                trailing_gap = cls._format_executor_status_pct(custom_info.get("distance_to_trailing_trigger_pct"))
            if trailing_gap is not None:
                fields["trail_gap"] = trailing_gap

        return fields

    @staticmethod
    def _compact_enum_label(value: Any) -> Optional[str]:
        if value is None:
            return None
        if hasattr(value, "name"):
            label = value.name
        else:
            label = str(value).split(".")[-1]
        return label.replace("_", "-").lower()

    @staticmethod
    def _compact_executor_type(executor_type: Any) -> str:
        label = str(executor_type or "executor")
        if label.endswith("_executor"):
            label = label[:-len("_executor")]
        return label.replace("_", "-")

    @staticmethod
    def _compact_side_label(side: Any) -> Optional[str]:
        if side is None:
            return None
        if hasattr(side, "name"):
            return side.name
        return str(side).split(".")[-1].upper()

    @staticmethod
    def _format_executor_compact_decimal(value: Any, precision: int = 4) -> Optional[str]:
        if value is None:
            return None
        try:
            decimal_value = Decimal(str(value))
            if decimal_value.is_nan():
                return "NaN"
            formatted_value = f"{decimal_value:.{precision}f}"
            if "." in formatted_value:
                formatted_value = formatted_value.rstrip("0").rstrip(".")
            return formatted_value
        except Exception:
            return None

    @classmethod
    def _format_executor_compact_quote(cls, value: Any, precision: int = 4) -> Optional[str]:
        formatted_value = cls._format_executor_compact_decimal(value, precision=precision)
        if formatted_value is None:
            return None
        if formatted_value.startswith("-"):
            return f"-${formatted_value[1:]}"
        if formatted_value.startswith("+"):
            return f"+${formatted_value[1:]}"
        return f"${formatted_value}"

    @staticmethod
    def _abbreviate_identifier(value: Any, prefix: int = 8, suffix: int = 6) -> Optional[str]:
        if value is None:
            return None
        text = str(value)
        if len(text) <= prefix + suffix + 3:
            return text
        return f"{text[:prefix]}...{text[-suffix:]}"

    @classmethod
    def _compact_order_references(cls, custom_info: Optional[Dict[str, Any]], custom_fields: Dict[str, Any]) -> Optional[str]:
        references: List[str] = []
        if custom_info:
            order_id = custom_info.get("order_id")
            if order_id:
                references.append(str(order_id))
            for order_ref in custom_info.get("order_ids") or []:
                if order_ref:
                    references.append(str(order_ref))
            for held_order in custom_info.get("held_position_orders") or []:
                if isinstance(held_order, dict) and held_order.get("order_id"):
                    references.append(str(held_order["order_id"]))

        if not references and custom_fields.get("order_ref"):
            references.extend(ref.strip() for ref in str(custom_fields["order_ref"]).split(",") if ref.strip())

        deduplicated_refs = list(dict.fromkeys(ref for ref in references if ref))
        if not deduplicated_refs:
            return None

        abbreviated_refs = [cls._abbreviate_identifier(ref) or ref for ref in deduplicated_refs]
        preview = ", ".join(abbreviated_refs[:2])
        overflow = f" (+{len(abbreviated_refs) - 2})" if len(abbreviated_refs) > 2 else ""
        return f"{preview}{overflow}"

    @staticmethod
    def _executor_side_emoji(side: Optional[str]) -> str:
        if side == "BUY":
            return "🟢"
        if side == "SELL":
            return "🔴"
        return "⚪"

    @staticmethod
    def _executor_status_emoji(status_label: str) -> str:
        status_icons = {
            "running": "🟢",
            "shutting-down": "🟡",
            "terminated": "⏹",
            "not-started": "⚪",
        }
        return status_icons.get(status_label, "🔹")

    @staticmethod
    def _executor_close_emoji(close_type: Optional[str]) -> str:
        close_icons = {
            "time-limit": "⏳",
            "stop-loss": "🛑",
            "take-profit": "🎯",
            "expired": "⌛",
            "early-stop": "🧯",
            "trailing-stop": "🧵",
            "insufficient-balance": "⚠️",
            "failed": "❌",
            "completed": "✅",
            "position-hold": "🧊",
            "window-end": "🪟",
        }
        return close_icons.get(close_type, "🏁")

    def _format_executor_age_short(self, timestamp: Any) -> str:
        try:
            age_seconds = max(0, int(float(self.current_timestamp) - float(timestamp)))
        except Exception:
            return "n/a"

        days, remainder = divmod(age_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)

        if days > 0:
            return f"{days}d{hours:02d}h"
        if hours > 0:
            return f"{hours}h{minutes:02d}m"
        if minutes > 0:
            return f"{minutes}m{seconds:02d}s"
        return f"{seconds}s"

    @classmethod
    def _executor_status_lines(cls, executor_info: ExecutorInfo, age_label: str, index: int) -> List[str]:
        custom_fields = cls._executor_custom_status_columns(executor_info.custom_info or {})

        side = cls._compact_side_label(executor_info.side or getattr(executor_info.config, "side", None))
        executor_type = cls._compact_executor_type(executor_info.type)
        status_label = cls._compact_enum_label(executor_info.status) or "unknown"
        close_type = cls._compact_enum_label(executor_info.close_type)
        pnl_pct = cls._format_executor_status_pct(executor_info.net_pnl_pct)
        pnl_quote = cls._format_executor_compact_quote(executor_info.net_pnl_quote)
        filled_quote = cls._format_executor_compact_quote(executor_info.filled_amount_quote)

        side_emoji = cls._executor_side_emoji(side)
        status_emoji = cls._executor_status_emoji(status_label)
        close_emoji = cls._executor_close_emoji(close_type) if close_type else None

        headline_title = f"    {index}. {side_emoji} {((side + ' ') if side else '')}{executor_type}"
        headline_parts = [headline_title, f"{status_emoji} {status_label}"]
        if close_type:
            headline_parts.append(f"{close_emoji} {close_type}")
        headline_parts.append(f"⏱ {age_label}")

        trade_parts: List[str] = []
        if pnl_pct or pnl_quote:
            if pnl_pct and pnl_quote:
                trade_parts.append(f"💰 PnL {pnl_pct} / {pnl_quote}")
            else:
                trade_parts.append(f"💰 PnL {pnl_pct or pnl_quote}")
        if filled_quote:
            trade_parts.append(f"📦 Fill {filled_quote}")

        entry_price = custom_fields.get("avg_entry")
        if entry_price is None:
            entry_price = cls._format_executor_compact_decimal(getattr(executor_info.config, "entry_price", None), precision=6)
        close_price = custom_fields.get("close_price")
        if entry_price is not None and close_price is not None:
            trade_parts.append(f"🎯 Px {entry_price} -> {close_price}")
        elif entry_price is not None:
            trade_parts.append(f"🎯 Entry {entry_price}")
        elif close_price is not None:
            trade_parts.append(f"🎯 Close {close_price}")

        detail_parts: List[str] = []
        if custom_fields.get("level") is not None:
            detail_parts.append(f"🪜 Level {custom_fields['level']}")
        if custom_fields.get("retries") is not None:
            detail_parts.append(f"🔁 Retries {custom_fields['retries']}")
        if custom_fields.get("trail") is not None:
            detail_parts.append(f"🧵 Trail {custom_fields['trail']}")
        if custom_fields.get("trail_arm") is not None:
            detail_parts.append(f"🚀 Arm {custom_fields['trail_arm']}")
        if custom_fields.get("trail_stop") is not None:
            detail_parts.append(f"🛑 Stop {custom_fields['trail_stop']}")
        if custom_fields.get("trail_gap") is not None:
            detail_parts.append(f"↔ Gap {custom_fields['trail_gap']}")
        if custom_fields.get("held_orders") is not None:
            detail_parts.append(f"🧺 Held {custom_fields['held_orders']}")
        if executor_info.is_trading:
            trade_state = "live"
        elif executor_info.is_done:
            trade_state = "done"
        else:
            trade_state = "idle"
        detail_parts.append(f"📡 {trade_state}")

        lines = [" | ".join(part for part in headline_parts if part)]
        if trade_parts:
            lines.append("       " + " | ".join(trade_parts))
        if detail_parts:
            lines.append("       " + " | ".join(detail_parts))

        references_line = cls._compact_order_references(executor_info.custom_info or {}, custom_fields)
        if references_line:
            lines.append(f"       🏷 Refs {references_line}")

        return lines

    def _format_recent_executor_lines(self, recent_executors: List[ExecutorInfo]) -> List[str]:
        lines: List[str] = []
        for index, executor_info in enumerate(recent_executors, start=1):
            age_label = self._format_executor_age_short(executor_info.timestamp)
            lines.extend(self._executor_status_lines(executor_info, age_label=age_label, index=index))
        return lines

    def set_leverage(self, connector: str, trading_pair: str, leverage: int):
        self.connectors[connector].set_leverage(trading_pair, leverage)

    def set_position_mode(self, connector: str, position_mode: PositionMode):
        self.connectors[connector].set_position_mode(position_mode)

    def filter_executors(self, executors: List[ExecutorInfo], filter_func: Callable[[ExecutorInfo], bool]) -> List[ExecutorInfo]:
        return [executor for executor in executors if filter_func(executor)]

    @staticmethod
    def executors_info_to_df(executors_info: List[ExecutorInfo]) -> pd.DataFrame:
        """
        Convert a list of executor handler info to a dataframe.
        """
        if len(executors_info) == 0:
            return pd.DataFrame()

        executor_rows = []
        for executor_info in executors_info:
            row = executor_info.to_dict()
            custom_info = row.get("custom_info", {})
            row.update(StrategyV2Base._executor_custom_status_columns(custom_info))
            executor_rows.append(row)

        df = pd.DataFrame(executor_rows)
        # Convert the enum values to integers
        df["status"] = df["status"].apply(lambda x: x.value)

        # Sort the DataFrame
        df.sort_values(by="status", ascending=True, inplace=True)

        # Convert back to enums for display
        df["status"] = df["status"].apply(RunnableStatus)
        return df
