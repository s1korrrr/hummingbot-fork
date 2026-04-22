import importlib.util
import sys
import types
import unittest
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict


def _install_package(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        module.__path__ = []
        sys.modules[name] = module
    return module


def _install_module(name: str) -> types.ModuleType:
    parts = name.split(".")
    for index in range(1, len(parts)):
        _install_package(".".join(parts[:index]))
    module = types.ModuleType(name)
    sys.modules[name] = module
    return module


def _install_rsi_v55_stubs() -> None:
    sys.modules.setdefault("pandas_ta", types.ModuleType("pandas_ta"))

    config_types = _install_module("hummingbot.client.config.config_data_types")

    class ClientFieldData:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    config_types.ClientFieldData = ClientFieldData

    common = _install_module("hummingbot.core.data_type.common")

    class OrderType(Enum):
        LIMIT = 1
        LIMIT_MAKER = 2
        MARKET = 3

    class PositionAction(Enum):
        CLOSE = "CLOSE"

    class PositionMode(Enum):
        ONEWAY = "ONEWAY"

    class PriceType(Enum):
        MidPrice = "mid_price"
        BestAsk = "best_ask"
        BestBid = "best_bid"

    class TradeType(Enum):
        BUY = "BUY"
        SELL = "SELL"

    common.OrderType = OrderType
    common.PositionAction = PositionAction
    common.PositionMode = PositionMode
    common.PriceType = PriceType
    common.TradeType = TradeType

    trading_rule_module = _install_module("hummingbot.connector.trading_rule")

    @dataclass
    class TradingRule:
        min_order_size: Decimal = Decimal("0")
        min_base_amount_increment: Decimal = Decimal("0")
        min_notional_size: Decimal = Decimal("0")
        min_order_value: Decimal = Decimal("0")
        min_price_increment: Decimal = Decimal("0.01")

    trading_rule_module.TradingRule = TradingRule

    candle_types = _install_module("hummingbot.data_feed.candles_feed.data_types")

    @dataclass
    class CandlesConfig:
        connector: str
        trading_pair: str
        interval: str
        max_records: int

    candle_types.CandlesConfig = CandlesConfig

    logger_module = _install_module("hummingbot.logger")
    logger_module.HummingbotLogger = object

    controller_base_module = _install_module("hummingbot.strategy_v2.controllers.directional_trading_controller_base")

    class DirectionalTradingControllerConfigBase(BaseModel):
        id: str = "test-controller"
        connector_name: str = "binance"
        trading_pair: str = "BNB-USDT"
        max_executors_per_side: int = 5
        cooldown_time: int = 90
        leverage: int = 1
        position_mode: PositionMode = PositionMode.ONEWAY
        total_amount_quote: Decimal = Decimal("100")
        candles_config: list = []
        manual_kill_switch: bool = False
        take_profit_order_type: OrderType = OrderType.LIMIT_MAKER
        trailing_stop_order_type: OrderType = OrderType.LIMIT
        trailing_stop_limit_max_age_seconds: int = 3
        model_config = ConfigDict(arbitrary_types_allowed=True)

    class DirectionalTradingControllerBase:
        def __init__(self, config, *args, **kwargs):
            self.config = config
            self.executors_info = []
            self.positions_held = []
            self.market_data_provider = kwargs.get("market_data_provider")
            self.actions_queue = kwargs.get("actions_queue")
            self.processed_data = {}

        @staticmethod
        def filter_executors(executors, filter_func):
            return [executor for executor in executors if filter_func(executor)]

    controller_base_module.DirectionalTradingControllerBase = DirectionalTradingControllerBase
    controller_base_module.DirectionalTradingControllerConfigBase = DirectionalTradingControllerConfigBase

    order_executor_module = _install_module("hummingbot.strategy_v2.executors.order_executor.data_types")

    class ExecutionStrategy(Enum):
        LIMIT = "LIMIT"
        LIMIT_MAKER = "LIMIT_MAKER"
        LIMIT_CHASER = "LIMIT_CHASER"
        MARKET = "MARKET"

    @dataclass
    class LimitChaserConfig:
        distance: Decimal
        refresh_threshold: Decimal

    @dataclass
    class OrderExecutorConfig:
        timestamp: float
        connector_name: str
        trading_pair: str
        side: TradeType
        amount: Decimal
        price: Decimal | None
        execution_strategy: ExecutionStrategy
        position_action: PositionAction
        leverage: int
        level_id: str | None = None
        chaser_config: object | None = None

    order_executor_module.ExecutionStrategy = ExecutionStrategy
    order_executor_module.LimitChaserConfig = LimitChaserConfig
    order_executor_module.OrderExecutorConfig = OrderExecutorConfig

    position_executor_module = _install_module("hummingbot.strategy_v2.executors.position_executor.data_types")

    @dataclass
    class TrailingStop:
        activation_price: Decimal
        trailing_delta: Decimal

    @dataclass
    class TripleBarrierConfig:
        stop_loss: Decimal | None = None
        take_profit: Decimal | None = None
        time_limit: int | None = None
        trailing_stop: TrailingStop | None = None
        open_order_type: OrderType | None = None
        take_profit_order_type: OrderType | None = None
        trailing_stop_order_type: OrderType | None = None
        trailing_stop_limit_max_age_seconds: int | None = None
        stop_loss_order_type: OrderType | None = None
        time_limit_order_type: OrderType | None = None

    @dataclass
    class PositionExecutorConfig:
        timestamp: float
        connector_name: str
        trading_pair: str
        side: TradeType
        entry_price: Decimal
        amount: Decimal
        triple_barrier_config: TripleBarrierConfig
        leverage: int
        activation_bounds: list | None = None
        level_id: str | None = None

    position_executor_module.PositionExecutorConfig = PositionExecutorConfig
    position_executor_module.TrailingStop = TrailingStop
    position_executor_module.TripleBarrierConfig = TripleBarrierConfig

    executor_actions_module = _install_module("hummingbot.strategy_v2.models.executor_actions")

    @dataclass
    class CreateExecutorAction:
        executor_config: object = None
        controller_id: str | None = None

    @dataclass
    class StopExecutorAction:
        executor_id: str | None = None
        controller_id: str | None = None
        keep_position: bool = False

    executor_actions_module.CreateExecutorAction = CreateExecutorAction
    executor_actions_module.ExecutorAction = object
    executor_actions_module.StopExecutorAction = StopExecutorAction

    executors_module = _install_module("hummingbot.strategy_v2.models.executors")

    class CloseType(Enum):
        INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
        POSITION_HOLD = "POSITION_HOLD"

    executors_module.CloseType = CloseType

    strategy_utils_common = _install_module("hummingbot.strategy_v2.utils.common")

    def parse_enum_value(enum_cls, value, field_name=None):
        if isinstance(value, enum_cls):
            return value
        if isinstance(value, str):
            normalized = value.replace(f"{enum_cls.__name__}.", "")
            for member in enum_cls:
                if member.name == normalized or str(member.value) == normalized:
                    return member
        try:
            return enum_cls(value)
        except Exception:
            raise ValueError(f"Invalid {field_name or 'enum'} value: {value}")

    strategy_utils_common.parse_enum_value = parse_enum_value

    market_analysis_module = _install_module("hummingbot.strategy_v2.utils.market_analysis")

    class TrendDirection(Enum):
        UP = "up"
        DOWN = "down"
        RANGE = "range"

    @dataclass
    class MarketRegime:
        timestamp: float
        regime_label: str
        confidence: float = 0.0

    @dataclass
    class MultiTimeframeTrend:
        direction: object = field(default_factory=lambda: SimpleNamespace(value="range"))
        confidence: float = 0.0
        alignment_score: float = 0.0
        states: dict | None = field(default_factory=dict)

        def to_dict(self):
            return {
                "direction": getattr(self.direction, "value", self.direction),
                "confidence": self.confidence,
                "alignment_score": self.alignment_score,
                "states": self.states or {},
            }

    class StrictHMMRequiredError(RuntimeError):
        pass

    class MarketRegimeDetector:
        def __init__(self, *args, **kwargs):
            self._is_fit = False

        def fit(self, df):
            self._is_fit = True

        def detect(self, df):
            return None

    market_analysis_module.MarketRegime = MarketRegime
    market_analysis_module.MarketRegimeDetector = MarketRegimeDetector
    market_analysis_module.MultiTimeframeTrend = MultiTimeframeTrend
    market_analysis_module.StrictHMMRequiredError = StrictHMMRequiredError
    market_analysis_module.TrendDirection = TrendDirection

    rsi_signals_module = _install_module("controllers.directional_trading.rsi_signals")
    rsi_signals_module.analyze_rsi_trend_confirmation = lambda *args, **kwargs: MultiTimeframeTrend(direction=TrendDirection.RANGE)
    rsi_signals_module.calculate_rsi_mean_reversion_score = lambda *args, **kwargs: 0.0
    rsi_signals_module.calculate_signal_strength = lambda *args, **kwargs: 0.5
    rsi_signals_module.calculate_volume_ratio = lambda *args, **kwargs: None
    rsi_signals_module.get_adaptive_trailing_params = lambda atr_pct, base_activation, base_delta, **kwargs: (base_activation, base_delta)


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class RSIv55RiskModuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_snapshot = {
            name: module
            for name, module in sys.modules.items()
            if (
                name == "pandas_ta"
                or name == "hummingbot"
                or name.startswith("hummingbot.")
                or name == "controllers.directional_trading.rsi_signals"
                or name == "controllers.directional_trading.rsi_v55"
                or name == "controllers.directional_trading.rsi_v55_risk"
            )
        }
        _install_rsi_v55_stubs()
        root = Path(__file__).resolve().parent.parent
        cls.risk_module = _load_module(
            root / "controllers" / "directional_trading" / "rsi_v55_risk.py",
            "controllers.directional_trading.rsi_v55_risk",
        )
        cls.v5_module = _load_module(
            root / "controllers" / "directional_trading" / "rsi_v55.py",
            "controllers.directional_trading.rsi_v55",
        )
        cls.PriceType = sys.modules["hummingbot.core.data_type.common"].PriceType
        cls.TradeType = sys.modules["hummingbot.core.data_type.common"].TradeType

    @classmethod
    def tearDownClass(cls) -> None:
        for name in [
            module_name for module_name in sys.modules
            if (
                module_name == "pandas_ta"
                or module_name == "hummingbot"
                or module_name.startswith("hummingbot.")
                or module_name == "controllers.directional_trading.rsi_signals"
                or module_name == "controllers.directional_trading.rsi_v55"
                or module_name == "controllers.directional_trading.rsi_v55_risk"
            )
        ]:
            sys.modules.pop(name, None)
        sys.modules.update(cls._module_snapshot)

    def _make_market_data_provider(self):
        price_type = self.PriceType

        class MarketDataProvider:
            def initialize_rate_sources(self):
                return None

            def time(self):
                return 1000.0

            def get_price_by_type(self, connector, pair, kind):
                return Decimal("100.00") if kind == price_type.BestBid else Decimal("100.50")

            def get_balance(self, connector, asset):
                return Decimal("1000")

            def get_trading_rules(self, connector, pair):
                return SimpleNamespace(
                    min_price_increment=Decimal("0.01"),
                    min_order_size=Decimal("0"),
                    min_base_amount_increment=Decimal("0.0001"),
                    min_notional_size=Decimal("5"),
                    min_order_value=Decimal("5"),
                )

            def quantize_order_amount(self, connector, pair, amount):
                return amount

        return MarketDataProvider()

    def _make_controller(self, **config_overrides):
        controller = self.v5_module.RSIv55Controller(
            config=self.v5_module.RSIv55ControllerConfig(
                id="rsi-v5-risk-test",
                connector_name="binance",
                trading_pair="BNB-USDT",
                **config_overrides,
            ),
            market_data_provider=self._make_market_data_provider(),
            actions_queue=None,
        )
        controller.processed_data = {"signal_state": {"condition_ok": True, "size_multiplier": 1.0}}
        return controller

    def test_risk_engine_classifies_market_structure_extremes(self):
        engine = self.risk_module.RSIv55RiskEngine()

        supportive = engine.classify_market_structure(
            risk_profile="balanced",
            trend_direction="up",
            trend_confidence=0.9,
            regime_label="Trend_Up",
            atr_percentile=0.35,
            signal_quality=0.9,
            inventory_utilization=0.1,
            held_bag_load=0.0,
            consecutive_loss_active=False,
            flash_crash_active=False,
            trend_sensitivity=1.0,
            regime_sensitivity=1.0,
        )
        hostile = engine.classify_market_structure(
            risk_profile="balanced",
            trend_direction="down",
            trend_confidence=0.8,
            regime_label="Trend_Down",
            atr_percentile=0.82,
            signal_quality=0.35,
            inventory_utilization=0.45,
            held_bag_load=0.3,
            consecutive_loss_active=False,
            flash_crash_active=False,
            trend_sensitivity=1.0,
            regime_sensitivity=1.0,
        )
        defensive = engine.classify_market_structure(
            risk_profile="balanced",
            trend_direction="range",
            trend_confidence=0.0,
            regime_label="Range",
            atr_percentile=0.4,
            signal_quality=0.7,
            inventory_utilization=0.1,
            held_bag_load=0.0,
            consecutive_loss_active=True,
            flash_crash_active=False,
            trend_sensitivity=1.0,
            regime_sensitivity=1.0,
        )

        self.assertEqual("supportive", supportive)
        self.assertEqual("defensive", hostile)
        self.assertEqual("defensive", defensive)

    def test_runner_add_is_blocked_by_risk_decision(self):
        controller = self._make_controller()
        controller.processed_data = {
            "signal_state": {"condition_ok": True, "buy_entry_role": "runner", "size_multiplier": 1.0},
            "risk_decision": {
                "risk_mode": "hostile",
                "allow_entry": True,
                "allow_runner_add": False,
                "entry_fraction": 0.3,
                "entry_budget_usd": 9.0,
                "effective_max_position_usd": 200.0,
                "sell_patience": "tight",
                "trailing_profile": "tight",
                "held_bag_exit_pressure": "elevated",
                "gate_reason": "risk_runner_block",
            },
        }

        self.assertFalse(controller.can_create_executor(1))
        self.assertEqual("risk_runner_block", controller._last_gate_reason[self.TradeType.BUY])

    def test_effective_max_position_uses_risk_cap(self):
        controller = self._make_controller(max_total_position_usd=Decimal("500"))
        controller.processed_data = {
            "signal_state": {"condition_ok": True, "buy_entry_role": "full", "size_multiplier": 1.0},
            "risk_decision": {
                "risk_mode": "hostile",
                "allow_entry": True,
                "allow_runner_add": False,
                "entry_fraction": 1.0,
                "entry_budget_usd": 30.0,
                "effective_max_position_usd": 25.0,
                "sell_patience": "tight",
                "trailing_profile": "tight",
                "held_bag_exit_pressure": "elevated",
                "gate_reason": "ready",
            },
        }
        controller.executors_info = [
            SimpleNamespace(
                is_active=True,
                connector_name="binance",
                trading_pair="BNB-USDT",
                side=self.TradeType.BUY,
                timestamp=0.0,
                filled_amount_quote=Decimal("10"),
                custom_info={"current_position_average_price": Decimal("100")},
            )
        ]

        self.assertFalse(controller.can_create_executor(1))
        self.assertEqual("max_position", controller._last_gate_reason[self.TradeType.BUY])

    def test_trailing_profile_scales_buy_executor_trailing(self):
        controller = self._make_controller(
            trailing_activation_pct=0.05,
            trailing_delta_pct=0.04,
            risk_hostile_trailing_scale=0.8,
        )
        controller.processed_data = {
            "signal_state": {"condition_ok": True, "buy_entry_role": "full", "size_multiplier": 1.0},
            "risk_decision": {
                "risk_mode": "hostile",
                "allow_entry": True,
                "allow_runner_add": False,
                "entry_fraction": 1.0,
                "entry_budget_usd": 30.0,
                "effective_max_position_usd": 300.0,
                "sell_patience": "tight",
                "trailing_profile": "tight",
                "held_bag_exit_pressure": "elevated",
                "gate_reason": "ready",
            },
        }

        config = controller.get_executor_config(self.TradeType.BUY, Decimal("100"), Decimal("1"))
        trailing = config.triple_barrier_config.trailing_stop

        self.assertIsNotNone(trailing)
        self.assertEqual(Decimal("0.040"), trailing.activation_price)
        self.assertEqual(Decimal("0.032"), trailing.trailing_delta)

    def test_held_bag_exit_pressure_reduces_recovery_sell_threshold(self):
        controller = self._make_controller()
        controller.processed_data = {
            "signal_state": {},
            "risk_decision": {
                "risk_mode": "defensive",
                "allow_entry": False,
                "allow_runner_add": False,
                "entry_fraction": 0.0,
                "entry_budget_usd": 0.0,
                "effective_max_position_usd": 100.0,
                "sell_patience": "urgent",
                "trailing_profile": "defensive",
                "held_bag_exit_pressure": "urgent",
                "gate_reason": "risk_defensive",
            },
        }

        self.assertEqual(
            float(controller.config.sell_rsi_overbought) - 4.0,
            controller._held_bag_recovery_sell_rsi_threshold(),
        )

    def test_status_includes_risk_summary_line(self):
        controller = self._make_controller()
        controller.processed_data = {
            "signal": 1,
            "indicators": {"rsi": 31.2, "cost_basis": 99.0},
            "thresholds": {"rsi_buy": 31.0, "rsi_sell": 76.0},
            "signal_state": {
                "signal_score": 3,
                "condition_ok": True,
                "condition_reason": "ok",
                "buy_decision": "ready",
                "buy_reason": "ready",
                "sell_decision": "hold",
                "sell_reason": "trend_hold",
                "risk_mode": "hostile",
                "risk_entry_budget_usd": 12.5,
                "risk_effective_max_position_usd": 220.0,
                "risk_sell_patience": "tight",
                "risk_trailing_profile": "tight",
                "risk_gate_reason": "risk_runner_block",
            },
        }

        status = "\n".join(controller.to_format_status())

        self.assertIn("mode=hostile", status)
        self.assertIn("budget=$12.50", status)
        self.assertIn("trail=tight", status)
        self.assertIn("gate=risk-runner", status)


if __name__ == "__main__":
    unittest.main()
