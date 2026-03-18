import asyncio
import importlib
import inspect
import logging
import os
from decimal import Decimal
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
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import MarketDict, PositionMode
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import OrderType, PositionAction
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
        self._controllers_started = False
        self._last_controller_reload_error: Optional[str] = None
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
        columns: List[str] = ["Exchange", "Asset", "Total Balance", "Available Balance"]
        data: List[Any] = []
        for connector_name, connector in self.connectors.items():
            for asset in self.get_assets(connector_name):
                data.append([connector_name,
                             asset,
                             float(connector.get_balance(asset)),
                             float(connector.get_available_balance(asset))])
        df = pd.DataFrame(data=data, columns=columns).replace(np.nan, '', regex=True)
        df.sort_values(by=["Exchange", "Asset"], inplace=True)
        return df

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
            controller_entries = self._build_controller_status_entries()
            lines.extend(self._controller_summary_lines(controller_entries))
            lines.extend(self._controller_attention_lines(controller_entries))

            for entry in controller_entries:
                controller_id = entry["controller_id"]
                controller = entry["controller"]
                controller_summary = entry["summary"]
                executors_list = entry["executors"]
                positions = entry["positions"]
                performance_report = entry["performance"]

                lines.append(f"\n{'=' * 60}")
                lines.append(self._controller_section_header(entry))
                lines.append(f"{'=' * 60}")
                if controller_summary.get("note") not in ("", "monitoring"):
                    lines.append(
                        f"Focus: {controller_summary.get('note')} | "
                        f"Exposure {controller_summary.get('exposure', 'n/a')} | "
                        f"Gate {controller_summary.get('gate', 'n/a')}"
                    )

                # Controller status
                try:
                    lines.extend(controller.to_format_status())
                except Exception as e:
                    self.logger().error(f"Error formatting status for controller {controller_id}: {e}", exc_info=True)
                    lines.extend(self._controller_status_error_lines(controller_id, e))

                # Recent executors table
                if executors_list:
                    lines.append("\n  🕘 Recent Executors (Last 3):")
                    # Sort by timestamp and take the most recent rows
                    recent_executors = sorted(executors_list, key=lambda x: x.timestamp, reverse=True)[:3]
                    lines.extend(self._format_recent_executor_lines(recent_executors))
                else:
                    lines.append("  No executors found.")

                # Positions table
                if positions:
                    lines.append("\n  Positions Held:")
                    positions_data = []
                    for pos in positions:
                        positions_data.append({
                            "Connector": pos.connector_name,
                            "Trading Pair": pos.trading_pair,
                            "Side": pos.side.name,
                            "Amount": f"{pos.amount:.4f}",
                            "Value (USD)": f"${pos.amount * pos.breakeven_price:.2f}",
                            "Breakeven Price": f"{pos.breakeven_price:.6f}",
                            "Unrealized PnL": f"${pos.unrealized_pnl_quote:+.2f}",
                            "Realized PnL": f"${pos.realized_pnl_quote:+.2f}",
                            "Fees": f"${pos.cum_fees_quote:.2f}"
                        })
                    positions_df = pd.DataFrame(positions_data)
                    lines.append(format_df_for_printout(positions_df, table_format="psql", index=False))
                else:
                    lines.append("  No positions held.")

                # Collect performance data for summary table
                if performance_report:
                    performance_data.append({
                        "Controller": controller_id,
                        "Realized PnL": f"${performance_report.realized_pnl_quote:.2f}",
                        "Unrealized PnL": f"${performance_report.unrealized_pnl_quote:.2f}",
                        "Global PnL": f"${performance_report.global_pnl_quote:.2f}",
                        "Global PnL %": f"{performance_report.global_pnl_pct:.2f}%",
                        "Volume Traded": f"${performance_report.volume_traded:.2f}"
                    })

            # Performance summary table
            if performance_data:
                lines.append(f"\n{'=' * 80}")
                lines.append("PERFORMANCE SUMMARY")
                lines.append(f"{'=' * 80}")

                # Calculate global totals
                global_realized = sum(Decimal(p["Realized PnL"].replace("$", "")) for p in performance_data)
                global_unrealized = sum(Decimal(p["Unrealized PnL"].replace("$", "")) for p in performance_data)
                global_total = global_realized + global_unrealized
                global_volume = sum(Decimal(p["Volume Traded"].replace("$", "")) for p in performance_data)
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

    async def listen_to_executor_actions(self):
        """
        Asynchronously listen to actions from the controllers and execute them.
        """
        while True:
            try:
                actions = await self.actions_queue.get()
                self.executor_orchestrator.execute_actions(actions)
                self.update_executors_info()
                controller_id = actions[0].controller_id
                controller = self.controllers.get(controller_id)
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
            # Get all reports in a single call and store them
            self.controller_reports = self.executor_orchestrator.get_all_reports()

            # Update each controller with its specific data
            for controller_id, controller in self.controllers.items():
                controller_report = self.controller_reports.get(controller_id, {})
                controller.executors_info = controller_report.get("executors", [])
                controller.positions_held = controller_report.get("positions", [])
                controller.performance_report = controller_report.get("performance", [])
                controller.executors_update_event.set()
        except Exception as e:
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
            executors=self.get_all_executors(),
            filter_func=lambda x: x.is_done)
        sorted_executors = sorted(potential_executors_to_store, key=lambda x: x.timestamp, reverse=True)
        if len(sorted_executors) > self.closed_executors_buffer:
            return [StoreExecutorAction(executor_id=executor.id, controller_id=executor.controller_id) for executor in
                    sorted_executors[self.closed_executors_buffer:]]
        return []

    def get_executors_by_controller(self, controller_id: str) -> List[ExecutorInfo]:
        """Get executors for a specific controller from the unified reports."""
        return self.controller_reports.get(controller_id, {}).get("executors", [])

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
            "state": summary.get("state", "n/a"),
            "signal": summary.get("signal", "n/a"),
            "score": summary.get("score", "n/a"),
            "regime": summary.get("regime", "n/a"),
            "exposure": summary.get("exposure", "n/a"),
            "execs": summary.get("execs", "n/a"),
            "u_pnl": summary.get("u_pnl", "n/a"),
            "gate": summary.get("gate", "n/a"),
            "note": summary.get("note", ""),
            "attention_score": summary.get("attention_score", 0),
        }
        return normalized_summary

    def _build_controller_status_entries(self) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for controller_id, controller in self.controllers.items():
            entries.append({
                "controller_id": controller_id,
                "controller": controller,
                "summary": self._safe_controller_status_summary(controller_id, controller),
                "executors": self.get_executors_by_controller(controller_id),
                "positions": self.get_positions_by_controller(controller_id),
                "performance": self.get_performance_report(controller_id),
            })
        entries.sort(
            key=lambda entry: (
                -int(entry["summary"].get("attention_score", 0)),
                str(entry["summary"].get("pair", entry["controller_id"])),
                entry["controller_id"],
            )
        )
        return entries

    @staticmethod
    def _controller_summary_lines(controller_entries: List[Dict[str, Any]]) -> List[str]:
        if len(controller_entries) == 0:
            return []

        summary_rows = []
        for entry in controller_entries:
            summary = entry["summary"]
            summary_rows.append({
                "pair": summary.get("pair", entry["controller_id"]),
                "state": summary.get("state", "n/a"),
                "signal": summary.get("signal", "n/a"),
                "score": summary.get("score", "n/a"),
                "regime": summary.get("regime", "n/a"),
                "exposure": summary.get("exposure", "n/a"),
                "execs": summary.get("execs", "n/a"),
                "uPnL": summary.get("u_pnl", "n/a"),
                "gate": summary.get("gate", "n/a"),
                "note": summary.get("note", ""),
            })

        summary_df = pd.DataFrame(summary_rows)
        return [
            f"\n{'=' * 110}",
            "CONTROLLERS SUMMARY",
            f"{'=' * 110}",
            format_df_for_printout(summary_df, table_format="psql", index=False),
        ]

    @staticmethod
    def _controller_attention_lines(controller_entries: List[Dict[str, Any]]) -> List[str]:
        attention_entries = [
            entry
            for entry in controller_entries
            if int(entry["summary"].get("attention_score", 0)) > 0
            and entry["summary"].get("note") not in ("", "monitoring")
        ]
        if len(attention_entries) == 0:
            return []

        lines = ["", "Priority Queue:"]
        for entry in attention_entries[:5]:
            summary = entry["summary"]
            lines.append(
                f"  • {summary.get('pair', entry['controller_id'])} | "
                f"{summary.get('state', 'n/a')} | {summary.get('note', '')}"
            )
        return lines

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
