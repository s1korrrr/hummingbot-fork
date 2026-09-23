import asyncio
import logging
import uuid
from collections import deque
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

from hummingbot.client.native_lifecycle import ShutdownResult
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import PositionAction, PositionMode, PriceType, TradeType
from hummingbot.logger import HummingbotLogger
from hummingbot.model.exact_order_values import OrderExact
from hummingbot.model.order import Order
from hummingbot.model.position import Position
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.controllers.native_policy_identity import is_ok_rsi_policy
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount
from hummingbot.strategy_v2.executors.planned_position_resume import claim, pending_manifest
from hummingbot.strategy_v2.executors.position_executor.data_types import parse_position_config

if TYPE_CHECKING:
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.data_types import PositionSummary
from hummingbot.strategy_v2.executors.dca_executor.dca_executor import DCAExecutor
from hummingbot.strategy_v2.executors.grid_executor.grid_executor import GridExecutor
from hummingbot.strategy_v2.executors.lp_executor.lp_executor import LPExecutor
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.twap_executor.twap_executor import TWAPExecutor
from hummingbot.strategy_v2.executors.wallet_sale_accounting import (
    validate_completed_wallet_order,
    validate_wallet_sale_summary,
)
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    ExecutorAction,
    StopExecutorAction,
    StoreExecutorAction,
)
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo, PerformanceReport

PositionScope = Tuple[str, str, str, Optional[TradeType]]


class PositionHold:
    def __init__(self, connector_name: str, trading_pair: str, side: TradeType):
        self.connector_name = connector_name
        self.trading_pair = trading_pair
        self.side = side
        # Store only client order IDs
        self.order_ids = set()
        self._order_records: Dict[str, Dict[str, Any]] = {}
        self._initial_totals = None

        # Pre-calculated metrics
        self.volume_traded_quote = Decimal("0")
        self.cum_fees_quote = Decimal("0")

        # Separate tracking for buys and sells
        self.buy_amount_base = Decimal("0")
        self.buy_amount_quote = Decimal("0")
        self.sell_amount_base = Decimal("0")
        self.sell_amount_quote = Decimal("0")

    def add_order_record(self, order: Dict[str, Any]):
        order_id = order.get("client_order_id") or order.get("order_id")
        if not order_id:
            raise ValueError("Held order requires a stable order identity")
        trade_type = order.get("trade_type")
        if trade_type not in (TradeType.BUY.name, TradeType.SELL.name):
            raise ValueError(f"Invalid held order side: {trade_type}")
        base = Decimal(str(order.get("executed_amount_base", 0)))
        quote = Decimal(str(order.get("executed_amount_quote", 0)))
        fees = Decimal(str(order.get("cumulative_fee_paid_quote", 0)))
        if not all(value.is_finite() for value in (base, quote, fees)) or base < 0 or quote < 0:
            raise ValueError(f"Invalid held order amounts: {order_id}")
        explicit_base_fee = "actual_base_fee" in order
        base_fee = Decimal(str(order["actual_base_fee"])) if explicit_base_fee else base_fee_amount(order, self.trading_pair.split('-')[0])
        if not base_fee.is_finite() or (explicit_base_fee and base_fee < 0):
            raise ValueError("invalid actual base fee in held-position receipt")
        gross_base = base
        gross_quote = quote
        if base > 0:
            direction = Decimal('1') if trade_type == 'BUY' else Decimal('-1')
            if not explicit_base_fee:
                # Explicit episode quote fees exclude base fees; their gross cost must remain intact.
                quote -= direction * base_fee * (quote / base)
            base -= direction * base_fee
        if base < 0:
            raise ValueError(f'Base fees exceed acquired quantity: {order_id}')
        previous = self._order_records.get(order_id)
        if previous is not None and gross_base < previous["gross_executed_amount_base"]:
            return  # A stale cumulative update must not roll back an observed fill.
        if self._initial_totals is None:
            self._initial_totals = (self.buy_amount_base, self.buy_amount_quote, self.sell_amount_base,
                                    self.sell_amount_quote, self.cum_fees_quote)
        old_base = previous["executed_amount_base"] if previous else Decimal("0")
        old_quote = previous["executed_amount_quote"] if previous else Decimal("0")
        old_volume = previous["gross_executed_amount_quote"] if previous else Decimal("0")
        old_fees = previous["cumulative_fee_paid_quote"] if previous else Decimal("0")
        if previous is not None and previous["trade_type"] != trade_type:
            raise ValueError(f"Order side changed for {order_id}")
        self._order_records[order_id] = dict(order, gross_executed_amount_base=gross_base, gross_executed_amount_quote=gross_quote, executed_amount_base=base,
                                             executed_amount_quote=quote, cumulative_fee_paid_quote=fees)
        self.order_ids.add(order_id)
        self.volume_traded_quote += gross_quote - old_volume
        if trade_type == TradeType.BUY.name:
            self.buy_amount_base += base - old_base
            self.buy_amount_quote += quote - old_quote
        else:
            self.sell_amount_base += base - old_base
            self.sell_amount_quote += quote - old_quote
        self.cum_fees_quote += fees - old_fees

    def add_orders_from_executor(self, executor: ExecutorInfo):
        held_position_orders = (executor.custom_info or {}).get("held_position_orders") or []
        for order in held_position_orders:
            self.add_order_record(order)

    def get_position_summary(self, mid_price: Decimal):
        # Handle NaN quote amounts by calculating them lazily
        if self.buy_amount_quote.is_nan() and self.buy_amount_base > 0:
            self.buy_amount_quote = self.buy_amount_base * mid_price
        if self.sell_amount_quote.is_nan() and self.sell_amount_base > 0:
            self.sell_amount_quote = self.sell_amount_base * mid_price

        # Calculate buy and sell breakeven prices
        buy_breakeven_price = self.buy_amount_quote / self.buy_amount_base if self.buy_amount_base > 0 else Decimal("0")
        sell_breakeven_price = self.sell_amount_quote / self.sell_amount_base if self.sell_amount_base > 0 else Decimal("0")

        # Calculate matched volume (minimum of buy and sell base amounts)
        matched_amount_base = min(self.buy_amount_base, self.sell_amount_base)

        # Calculate realized PnL from matched volume
        realized_pnl_quote = (sell_breakeven_price - buy_breakeven_price) * matched_amount_base if matched_amount_base > 0 else Decimal("0")

        # Calculate net position amount and direction
        net_amount_base = self.buy_amount_base - self.sell_amount_base
        is_net_long = net_amount_base >= 0

        # Calculate unrealized PnL for the remaining unmatched volume
        unrealized_pnl_quote = Decimal("0")
        breakeven_price = Decimal("0")
        if net_amount_base != 0:
            if is_net_long:
                # Long position: remaining buy amount
                remaining_base = net_amount_base
                remaining_quote = self.buy_amount_quote - (matched_amount_base * buy_breakeven_price)
                breakeven_price = remaining_quote / remaining_base
                unrealized_pnl_quote = (mid_price - breakeven_price) * remaining_base
            else:
                # Short position: remaining sell amount
                remaining_base = abs(net_amount_base)
                remaining_quote = self.sell_amount_quote - (matched_amount_base * sell_breakeven_price)
                breakeven_price = remaining_quote / remaining_base
                unrealized_pnl_quote = (breakeven_price - mid_price) * remaining_base

        remaining_fees_quote = self.cum_fees_quote if net_amount_base else Decimal("0")
        if self._order_records:
            initial_buy, initial_buy_quote, initial_sell, initial_sell_quote, _ = self._initial_totals
            net_amount_base = initial_buy - initial_sell
            initial_buy_price = initial_buy_quote / initial_buy if initial_buy else Decimal("0")
            initial_sell_price = initial_sell_quote / initial_sell if initial_sell else Decimal("0")
            if initial_buy_price.is_nan():
                initial_buy_price = mid_price
            if initial_sell_price.is_nan():
                initial_sell_price = mid_price
            breakeven_price = initial_buy_price if net_amount_base > 0 else initial_sell_price
            realized_pnl_quote = (initial_sell_price - initial_buy_price) * min(initial_buy, initial_sell)
            remaining_fees_quote = self._initial_totals[4] if net_amount_base else Decimal("0")
            for record in self._order_records.values():
                amount = record["executed_amount_base"]
                if amount <= 0:
                    continue
                price = record["executed_amount_quote"] / amount
                signed_amount = amount if record["trade_type"] == TradeType.BUY.name else -amount
                if net_amount_base == 0 or net_amount_base * signed_amount > 0:
                    remaining_fees_quote += record["cumulative_fee_paid_quote"]
                    total_amount = abs(net_amount_base) + amount
                    breakeven_price = (abs(net_amount_base) * breakeven_price + amount * price) / total_amount
                else:
                    matched = min(abs(net_amount_base), amount)
                    remaining_fees_quote *= (abs(net_amount_base) - matched) / abs(net_amount_base)
                    if amount > abs(net_amount_base):
                        remaining_fees_quote = record["cumulative_fee_paid_quote"] * (amount - matched) / amount
                    direction = Decimal("1") if net_amount_base > 0 else Decimal("-1")
                    realized_pnl_quote += (price - breakeven_price) * matched * direction
                    if amount > abs(net_amount_base):
                        breakeven_price = price
                net_amount_base += signed_amount
                if net_amount_base == 0:
                    breakeven_price = Decimal("0")
            is_net_long = net_amount_base >= 0
            unrealized_pnl_quote = (mid_price - breakeven_price) * net_amount_base

        return PositionSummary(
            connector_name=self.connector_name,
            trading_pair=self.trading_pair,
            volume_traded_quote=self.volume_traded_quote,
            amount=abs(net_amount_base),
            side=TradeType.BUY if is_net_long else TradeType.SELL,
            breakeven_price=breakeven_price,
            unrealized_pnl_quote=unrealized_pnl_quote,
            realized_pnl_quote=realized_pnl_quote,
            cum_fees_quote=self.cum_fees_quote, remaining_fees_quote=remaining_fees_quote)


class ExecutorOrchestrator:
    """
    Orchestrator for various executors.
    """
    _logger = None
    _executor_mapping = {
        "position_executor": PositionExecutor,
        "grid_executor": GridExecutor,
        "dca_executor": DCAExecutor,
        "arbitrage_executor": ArbitrageExecutor,
        "twap_executor": TWAPExecutor,
        "xemm_executor": XEMMExecutor,
        "order_executor": OrderExecutor,
        "lp_executor": LPExecutor,
    }

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 strategy: "StrategyV2Base",
                 executors_update_interval: float = 1.0,
                 executors_max_retries: int = 10,
                 initial_positions_by_controller: Optional[dict] = None,
                 executor_start: Optional[Callable] = None,
                 executor_restore: Optional[Callable] = None):
        self.strategy = strategy
        self._executor_start = executor_start
        self._executor_restore = executor_restore
        self.executors_update_interval = executors_update_interval
        self.executors_max_retries = executors_max_retries
        self.active_executors = {}
        self.stored_executors = {}
        self.positions_held = {}
        self.executors_ids_position_held = deque(maxlen=50)
        self.cached_performance = {}
        self._wallet_sale_receipts = {}
        self._wallet_sale_position_summaries = []
        self._non_hold_order_ids: Set[Tuple[str, str, str]] = set()
        self._non_hold_fill_ids: Set[Tuple[str, str, str, str]] = set()
        self.recovery_required: Dict[str, List[str]] = {}
        self._resume_owned_order_keys = set()
        self._resume_position_scopes = set()
        self._planned_resume_pending = False
        self._planned_resume_failure = None
        self.initial_positions_by_controller = initial_positions_by_controller or {}
        self._initialize_cached_performance()

    def _initialize_cached_performance(self, allow_planned_resume=False):
        """
        Initialize cached performance by querying the database for stored executors and positions.
        If initial positions are provided for a controller, skip loading database positions for that controller.
        """
        for controller_id in self.strategy.controllers.keys():
            if controller_id not in self.cached_performance:
                self.cached_performance[controller_id] = PerformanceReport()
                self.active_executors[controller_id] = []
                self.stored_executors[controller_id] = []
                self.positions_held[controller_id] = []
        markets_recorder = MarketsRecorder.get_instance()
        db_path = getattr(getattr(markets_recorder, "sql_manager", None), "db_path", None)
        if not allow_planned_resume and isinstance(db_path, (str, Path)) and pending_manifest(db_path) is not None:
            self._planned_resume_pending = True
            return  # No held reconstruction or ownership mutation before connectors are ready.
        resumed = self._claim_planned_positions(markets_recorder)
        db_executors = markets_recorder.get_all_executors()
        restored_executors = []
        for executor in db_executors:
            controller_id = executor.controller_id
            if controller_id not in self.strategy.controllers:
                if not executor.is_done:
                    self.recovery_required.setdefault(controller_id, []).append(executor.id)
                    self.logger().error(
                        f"Startup reconciliation required | controller={controller_id} executor={executor.id} "
                        "Persisted execution belongs to a controller absent from current config. "
                        "New execution is blocked until ownership is reconciled."
                    )
                continue
            if executor.id in resumed:
                continue
            if not executor.is_done and self._executor_restore is not None:
                restored = self._executor_restore(executor)
                if restored is not None:
                    if restored.config != executor.config or restored.status != RunnableStatus.NOT_STARTED:
                        raise ValueError('Restored executor identity or lifecycle differs')
                    restored_executors.append((controller_id, restored, self._owned_order_keys(executor)))
                    self._resume_owned_order_keys.update(self._owned_order_keys(executor))
                    self._non_hold_order_ids.update(self._owned_order_keys(executor))
                    resumed.add(executor.id)
                    continue
            if not executor.is_done:
                self.recovery_required.setdefault(controller_id, []).append(executor.id)
                self.logger().error(
                    f"Startup reconciliation required | controller={controller_id} executor={executor.id} "
                    "Persisted execution is unfinished. New execution is blocked until order ownership is reconciled."
                )
            else:
                self._update_cached_performance(controller_id, executor)
            self._record_stored_executor(controller_id, executor)

        for controller_id, controller in self.strategy.controllers.items():
            ingest = getattr(controller, '_ingest_closed_executor_results', None)
            if ingest is not None:
                ingest([info for info in db_executors if info.controller_id == controller_id])

        # Create initial positions from config overrides first
        self._create_initial_positions()

        restored_position_scopes = self._restore_positions_from_ledger(
            markets_recorder.get_all_position_hold_ledger_entries()
        )

        # Load positions from database only for controllers without initial position overrides
        db_positions = markets_recorder.get_all_positions()
        wallet_scopes = {key[:3] for key in self._wallet_sale_owners()}
        for position in db_positions:
            controller_id = position.controller_id
            # Skip if this controller has initial position overrides
            if controller_id in self.initial_positions_by_controller or controller_id not in self.strategy.controllers:
                continue
            # Skip if the connector/trading pair is not in the current strategy markets
            if (position.connector_name not in self.strategy.markets or
                    position.trading_pair not in self.strategy.markets.get(position.connector_name, set())):
                self.logger().warning(f"Skipping position for {position.connector_name}.{position.trading_pair} - "
                                      f"not available in current strategy markets")
                continue
            if (position.side == "SELL" and
                    (controller_id, position.connector_name, position.trading_pair) in wallet_scopes):
                self._wallet_sale_position_summaries.append(position)
                continue  # Validate this historical synthetic summary against exact receipts before any report.
            if not self._is_valid_stored_position(position):
                self.logger().warning(
                    f"Skipping invalid stored position | controller={controller_id} "
                    f"pair={position.trading_pair} side={position.side} amount={position.amount} "
                    f"breakeven={position.breakeven_price}"
                )
                continue
            position_scope = self._position_scope(
                controller_id=controller_id,
                connector_name=position.connector_name,
                trading_pair=position.trading_pair,
                side=TradeType.BUY if position.side == TradeType.BUY.name else TradeType.SELL,
            )
            if self._is_protected_position_scope(restored_position_scopes | self._resume_position_scopes, position_scope):
                continue
            self._load_position_from_db(controller_id, position)
            restored_position_scopes.add(position_scope)
        self._restore_positions_from_executor_history(
            executor_infos=db_executors,
            protected_position_scopes=restored_position_scopes,
        )
        trade_fills = []
        config_file_path = getattr(markets_recorder, "config_file_path", None)
        if config_file_path:
            trade_fills = markets_recorder.get_trades_for_config(config_file_path)
        self._restore_positions_from_trade_fills(
            trade_fills=trade_fills,
            protected_position_scopes=self._current_position_scopes(),
        )
        # A restored paper book is deliberately unavailable until explicit new
        # input. Defer live objects until history reconstruction has used the
        # proven native ownership keys, without requesting a fabricated mark.
        for controller_id, executor, order_keys in restored_executors:
            self.active_executors[controller_id].append(executor)
            self._non_hold_order_ids.difference_update(order_keys)
        for executors in self.active_executors.values():
            for executor in executors:
                if executor.config.id in resumed:
                    if self._executor_start is None:
                        executor.start()
                    else:
                        self._executor_start(executor)

    def resume_planned_positions_if_ready(self):
        if not self._planned_resume_pending:
            return True
        if self._planned_resume_failure is not None:
            return False
        if not self.strategy.ready_to_trade or not self.strategy.market_data_provider.ready:
            return False
        try:
            self._initialize_cached_performance(allow_planned_resume=True)
        except Exception as error:
            self._planned_resume_failure = f"{type(error).__name__}: {error}"
            for controller_id, executors in self.active_executors.items():
                for executor in executors:
                    executor.stop()
                    unresolved = self.recovery_required.setdefault(controller_id, [])
                    if executor.config.id not in unresolved:
                        unresolved.append(executor.config.id)
                # The claim already persisted unfinished ownership. Quiesced
                # objects must never overwrite that row as a completed exit.
                executors.clear()
            self.logger().exception("Planned resume failed; restart reconciliation required; execution blocked")
            return False
        self._planned_resume_pending = False
        return True

    def _claim_planned_positions(self, recorder):
        db_path = getattr(getattr(recorder, "sql_manager", None), "db_path", None)
        if not isinstance(db_path, (str, Path)):
            return set()
        manifest = pending_manifest(db_path)
        if manifest is None:
            return set()
        instances = []
        required_base = {}
        for position in manifest["positions"]:
            config = parse_position_config(position["config"])
            controller = self.strategy.controllers.get(position["controller_id"])
            if (controller is None or position["controller_id"] in self.initial_positions_by_controller
                    or config.connector_name not in self.strategy.connectors
                    or config.trading_pair not in self.strategy.markets.get(config.connector_name, set())):
                raise ValueError("Planned position owner/market is unavailable or overridden")
            if (controller.config.connector_name != config.connector_name
                    or controller.config.trading_pair != config.trading_pair):
                raise ValueError("Planned position controller identity differs")
            connector = self.strategy.connectors[config.connector_name]
            if any(order.trading_pair == config.trading_pair for order in connector.in_flight_orders.values()):
                raise ValueError("Planned resume has unreconciled open orders")
            for price_type in (PriceType.BestBid, PriceType.BestAsk):
                price = Decimal(str(connector.get_price_by_type(config.trading_pair, price_type)))
                if not price.is_finite() or price <= 0:
                    raise ValueError("Planned resume price is unavailable")
            base = config.trading_pair.split('-')[0]
            key = config.connector_name, base
            required_base[key] = required_base.get(key, Decimal(0)) + Decimal(position["net_base"])
            executor_class = PositionExecutor
            if getattr(config, 'entry_acquisition_policy', None) == 'down_only_maker_v1':
                from controllers.directional_trading.ok_rsi_core.config import OKRSIControllerConfig
                trusted_owner = (type(controller.config) is OKRSIControllerConfig
                                 or getattr(controller.config, 'controller_name', None) != 'ok_rsi'
                                 and is_ok_rsi_policy(controller.config))
                if (not trusted_owner
                        or getattr(controller.config, 'entry_acquisition_policy', None) != 'down_only_maker_v1'):
                    raise ValueError('Planned down-only native owner policy differs')
                from controllers.directional_trading.ok_rsi_core.down_only_entry_execution import (
                    DownOnlyMakerPositionExecutor,
                )
                executor_class = DownOnlyMakerPositionExecutor
            # The consumed manifest authorizes sealed existing inventory. Do not
            # request a new entry factory authorization or create a new BUY intent.
            executor = executor_class(self.strategy, config, self.executors_update_interval, self.executors_max_retries)
            executor.hydrate_planned_position(position, manifest["id"])
            instances.append(executor)
        for (connector_name, base), amount in required_base.items():
            available = Decimal(str(self.strategy.connectors[connector_name].get_available_balance(base)))
            if not available.is_finite() or available < amount:
                raise ValueError("Planned resume inventory is not available")
        # No listener/control loop starts before all preconditions and the durable
        # transaction succeed. A crash after this claim requires reconciliation.
        claim(db_path, manifest)
        for executor in instances:
            config = executor.config
            self.active_executors[config.controller_id].append(executor)
            self._resume_owned_order_keys.add((config.connector_name, config.trading_pair, executor._open_order.order_id))
            self._resume_position_scopes.add(self._position_scope(
                config.controller_id, config.connector_name, config.trading_pair, None))
        return {executor.config.id for executor in instances}

    def _update_cached_performance(self, controller_id: str, executor_info: ExecutorInfo):
        """
        Update the cached performance for a specific controller with an executor's information.
        """
        if controller_id not in self.cached_performance:
            self.cached_performance[controller_id] = PerformanceReport()
        report = self.cached_performance[controller_id]
        if (executor_info.close_type != CloseType.POSITION_HOLD and executor_info.type != "order_executor"
                and not self._entry_without_fill_handed_off(executor_info)):
            if self._has_single_attempt_receipts(executor_info):
                self._non_hold_fill_ids.update(self._accounted_fill_keys(executor_info))
            else:
                self._non_hold_order_ids.update(self._owned_order_keys(executor_info))
        # Only add to realized PnL if not a position hold (consistent with generate_performance_report)
        if executor_info.close_type != CloseType.POSITION_HOLD and not self._is_unknown_wallet_sale(executor_info):
            report.realized_pnl_quote += executor_info.net_pnl_quote
            report.volume_traded += executor_info.filled_amount_quote
        if executor_info.close_type:
            report.close_type_counts[executor_info.close_type] = report.close_type_counts.get(executor_info.close_type,
                                                                                              0) + 1

    @staticmethod
    def _should_store_executor_for_history(executor_info: ExecutorInfo) -> bool:
        return executor_info.is_done or not executor_info.is_active

    @staticmethod
    def _position_scope(
        controller_id: str,
        connector_name: str,
        trading_pair: str,
        side: Optional[TradeType],
    ) -> PositionScope:
        return controller_id, connector_name, trading_pair, side

    @staticmethod
    def _is_protected_position_scope(
        protected_position_scopes: Set[PositionScope],
        position_scope: PositionScope,
    ) -> bool:
        controller_id, connector_name, trading_pair, side = position_scope
        return any(
            protected_controller_id == controller_id
            and protected_connector_name == connector_name
            and protected_trading_pair == trading_pair
            and (
                protected_side is None
                or side is None
                or protected_side == side
            )
            for protected_controller_id, protected_connector_name, protected_trading_pair, protected_side
            in protected_position_scopes
        )

    @staticmethod
    def _position_hold_amount(position: PositionHold) -> Decimal:
        return abs(position.buy_amount_base - position.sell_amount_base)

    @staticmethod
    def _is_valid_stored_position(position: Position) -> bool:
        try:
            amount = Decimal(str(position.amount))
            breakeven = Decimal(str(position.breakeven_price))
        except Exception:
            return False
        return amount > Decimal("0") and breakeven > Decimal("0")

    def _current_position_scopes(self) -> Set[PositionScope]:
        position_scopes: Set[PositionScope] = set()
        for controller_id, positions in self.positions_held.items():
            for position in positions:
                if self._position_hold_amount(position) == Decimal("0"):
                    continue
                position_scopes.add(
                    self._position_scope(
                        controller_id=controller_id,
                        connector_name=position.connector_name,
                        trading_pair=position.trading_pair,
                        side=position.side,
                    )
                )
        return position_scopes

    def _controller_ids_by_market_pair(self) -> Tuple[Dict[Tuple[str, str], str], Set[Tuple[str, str]]]:
        controller_by_market_pair: Dict[Tuple[str, str], str] = {}
        ambiguous_market_pairs: Set[Tuple[str, str]] = set()
        for controller_id, controller in self.strategy.controllers.items():
            controller_config = getattr(controller, "config", None)
            connector_name = getattr(controller_config, "connector_name", None)
            trading_pair = getattr(controller_config, "trading_pair", None)
            if connector_name is None or trading_pair is None:
                connector_name = getattr(controller, "connector_name", None)
                trading_pair = getattr(controller, "trading_pair", None)
            if connector_name is None or trading_pair is None:
                continue
            market_pair = (connector_name, trading_pair)
            existing_controller_id = controller_by_market_pair.get(market_pair)
            if existing_controller_id is not None and existing_controller_id != controller_id:
                ambiguous_market_pairs.add(market_pair)
                controller_by_market_pair.pop(market_pair, None)
            elif market_pair not in ambiguous_market_pairs:
                controller_by_market_pair[market_pair] = controller_id
        return controller_by_market_pair, ambiguous_market_pairs

    @staticmethod
    def _has_held_position_orders(executor_info: ExecutorInfo) -> bool:
        held_position_orders = (executor_info.custom_info or {}).get("held_position_orders") or []
        return executor_info.close_type == CloseType.POSITION_HOLD and len(held_position_orders) > 0

    def _record_stored_executor(self, controller_id: str, executor_info: ExecutorInfo):
        if not self._should_store_executor_for_history(executor_info):
            return
        if controller_id not in self.stored_executors:
            self.stored_executors[controller_id] = []
        persisted_custom_info = dict(executor_info.custom_info or {})
        persisted_custom_info.setdefault("persistence_source", "db")
        stored_executor = executor_info.model_copy(
            deep=True,
            update={"custom_info": persisted_custom_info},
        )
        deduped_executors = [
            stored
            for stored in self.stored_executors[controller_id]
            if stored.id != stored_executor.id
        ]
        deduped_executors.append(stored_executor)
        deduped_executors.sort(
            key=lambda info: info.close_timestamp if info.close_timestamp is not None else info.timestamp,
            reverse=True,
        )
        self.stored_executors[controller_id] = deduped_executors[:20]

    @staticmethod
    def _position_side_from_name(side: Optional[str]) -> Optional[TradeType]:
        if side in (None, "", "None"):
            return None
        return TradeType.BUY if side == TradeType.BUY.name else TradeType.SELL

    def _apply_order_record_to_positions(
        self,
        controller_id: str,
        connector_name: str,
        trading_pair: str,
        position_side: Optional[TradeType],
        default_side: TradeType,
        order_record: Dict[str, Any],
    ):
        positions = self.positions_held.setdefault(controller_id, [])
        existing_position = self._find_existing_position(
            positions=positions,
            connector_name=connector_name,
            trading_pair=trading_pair,
            position_side=position_side,
        )

        if existing_position:
            existing_position.add_order_record(order_record)
        else:
            position = PositionHold(
                connector_name,
                trading_pair,
                position_side if position_side else default_side,
            )
            position.add_order_record(order_record)
            positions.append(position)

    def _apply_executor_info_to_positions(self, controller_id: str, executor_info: ExecutorInfo):
        if self._is_unknown_wallet_sale(executor_info):
            return  # Wallet disposals have no acquired position or cost to reconstruct.
        held_position_orders = (executor_info.custom_info or {}).get("held_position_orders") or []
        if len(held_position_orders) == 0:
            return

        position_side = self._determine_position_side(executor_info)
        for order in held_position_orders:
            self._apply_order_record_to_positions(
                controller_id=controller_id,
                connector_name=executor_info.connector_name,
                trading_pair=executor_info.trading_pair,
                position_side=position_side,
                default_side=executor_info.config.side,
                order_record=order,
            )

    def _persist_position_hold_entries_from_executor_info(self, executor_info: ExecutorInfo):
        if self._is_unknown_wallet_sale(executor_info):
            return  # Exact native fills and executor identity retain the sale receipt.
        if not self._has_held_position_orders(executor_info) or executor_info.controller_id is None:
            return
        position_side = self._determine_position_side(executor_info)

        MarketsRecorder.get_instance().store_or_update_position_hold_ledger_entries(
            controller_id=executor_info.controller_id,
            executor_id=executor_info.id,
            connector_name=executor_info.connector_name,
            trading_pair=executor_info.trading_pair,
            held_position_orders=(executor_info.custom_info or {}).get("held_position_orders") or [],
            position_side=position_side.name if position_side is not None else None,
            fallback_timestamp=(
                executor_info.close_timestamp
                if executor_info.close_timestamp is not None
                else int(executor_info.timestamp)
            ),
        )

    def _prune_flat_positions(self, controller_id: str):
        positions = self.positions_held.get(controller_id, [])
        self.positions_held[controller_id] = [
            position
            for position in positions
            if self._position_hold_amount(position) > Decimal("0") or position.volume_traded_quote > Decimal("0") or position.cum_fees_quote != Decimal("0")
        ]

    def _restore_positions_from_ledger(
        self,
        ledger_entries,
    ) -> Set[PositionScope]:
        restored_position_scopes: Set[PositionScope] = set()
        restored_controller_ids: Set[str] = set()
        wallet_owners = self._wallet_sale_owners()

        for entry in ledger_entries:
            if (entry.connector_name, entry.trading_pair, entry.client_order_id) in self._resume_owned_order_keys:
                continue
            controller_id = entry.controller_id
            if controller_id in self.initial_positions_by_controller or controller_id not in self.strategy.controllers:
                continue
            if (
                entry.connector_name not in self.strategy.markets
                or entry.trading_pair not in self.strategy.markets.get(entry.connector_name, set())
            ):
                continue

            key = (controller_id, entry.connector_name, entry.trading_pair, entry.client_order_id)
            if self._wallet_sale_ledger_matches(entry, wallet_owners.get(key)):
                continue  # Corroborate against native fills at report refresh; retain the original row.
            position_side = self._position_side_from_name(entry.position_side)
            self._apply_order_record_to_positions(
                controller_id=controller_id,
                connector_name=entry.connector_name,
                trading_pair=entry.trading_pair,
                position_side=position_side,
                default_side=self._position_side_from_name(entry.order_side) or TradeType.BUY,
                order_record=entry.to_order_record(),
            )
            restored_position_scopes.add(
                self._position_scope(
                    controller_id=controller_id,
                    connector_name=entry.connector_name,
                    trading_pair=entry.trading_pair,
                    side=position_side,
                )
            )
            restored_controller_ids.add(controller_id)

        for controller_id in restored_controller_ids:
            self._prune_flat_positions(controller_id)
            for position in self.positions_held.get(controller_id, []):
                try:
                    mid_price = self.strategy.market_data_provider.get_price_by_type(
                        position.connector_name, position.trading_pair, PriceType.MidPrice
                    )
                except Exception:
                    mid_price = Decimal("0")
                position_summary = position.get_position_summary(mid_price or Decimal("0"))
                self.logger().info(
                    f"Recovered held position from ledger | controller={controller_id} "
                    f"pair={position_summary.trading_pair} side={position_summary.side.name} "
                    f"amount={position_summary.amount} breakeven={position_summary.breakeven_price}"
                )

        return restored_position_scopes

    def _restore_positions_from_executor_history(
        self,
        executor_infos: List[ExecutorInfo],
        protected_position_scopes: Optional[Set[PositionScope]] = None,
    ):
        protected_position_scopes = protected_position_scopes or set()
        restored_controller_ids: Set[str] = set()
        restorable_executor_infos = sorted(
            (
                executor_info
                for executor_info in executor_infos
                if self._has_held_position_orders(executor_info)
                and not (self._owned_order_keys(executor_info) & self._resume_owned_order_keys)
            ),
            key=lambda info: (
                info.close_timestamp if info.close_timestamp is not None else info.timestamp,
                info.timestamp,
            ),
        )

        for executor_info in restorable_executor_infos:
            controller_id = executor_info.controller_id
            if controller_id in self.initial_positions_by_controller or controller_id not in self.strategy.controllers:
                continue
            if (
                executor_info.connector_name not in self.strategy.markets
                or executor_info.trading_pair not in self.strategy.markets.get(executor_info.connector_name, set())
            ):
                continue

            position_scope = self._position_scope(
                controller_id=controller_id,
                connector_name=executor_info.connector_name,
                trading_pair=executor_info.trading_pair,
                side=self._determine_position_side(executor_info),
            )
            if self._is_protected_position_scope(protected_position_scopes, position_scope):
                continue

            self._apply_executor_info_to_positions(controller_id, executor_info)
            self._persist_position_hold_entries_from_executor_info(executor_info)
            restored_controller_ids.add(controller_id)

        for controller_id in restored_controller_ids:
            self._prune_flat_positions(controller_id)
            for position in self.positions_held.get(controller_id, []):
                try:
                    mid_price = self.strategy.market_data_provider.get_price_by_type(
                        position.connector_name, position.trading_pair, PriceType.MidPrice
                    )
                except Exception:
                    mid_price = Decimal("0")
                position_summary = position.get_position_summary(mid_price or Decimal("0"))
                self.logger().info(
                    f"Recovered held position from executor history | controller={controller_id} "
                    f"pair={position_summary.trading_pair} side={position_summary.side.name} "
                    f"amount={position_summary.amount} breakeven={position_summary.breakeven_price}"
                )

    def _restore_positions_from_trade_fills(
        self,
        trade_fills: List[TradeFill],
        protected_position_scopes: Optional[Set[PositionScope]] = None,
    ):
        protected_position_scopes = protected_position_scopes or set()
        controller_by_pair, _ = self._controller_ids_by_market_pair()
        candidates = [fill for fill in trade_fills if not self._is_protected_position_scope(
            protected_position_scopes, self._position_scope(
                controller_by_pair.get((fill.market, fill.symbol)), fill.market, fill.symbol, None))]
        if not candidates:
            return
        try:
            _, positions = self._build_reconciled_positions_from_trade_fills(candidates, check_existing=False)
        except RuntimeError as error:
            self.logger().warning(str(error))
            return  # Report refresh will retain the unavailable state and block new execution.
        for (controller_id, connector, pair), position in positions.items():
            for record in position._order_records.values():
                self._apply_order_record_to_positions(
                    controller_id, connector, pair, None, position.side,
                    dict(record, executed_amount_base=record["gross_executed_amount_base"],
                         executed_amount_quote=record["gross_executed_amount_quote"]))

    @staticmethod
    def _trade_fill_order_record(trade_fill: TradeFill) -> Dict[str, Any]:
        if trade_fill.has_exact_receipt is not True:
            raise ValueError("Legacy TradeFill has no exact inventory receipt")
        executed_amount_base = trade_fill.amount_decimal
        price = trade_fill.price_decimal
        quote_fee = trade_fill.trade_fee_in_quote_decimal
        if any(Decimal(str(stored)) != Decimal(int(exact * 1000000)) / 1000000
               for stored, exact in ((trade_fill.amount, executed_amount_base), (trade_fill.price, price))):
            raise ValueError("Native encoding conflicts with exact receipt")
        if quote_fee is None:
            raise ValueError("Exact TradeFill quote fee is unavailable")
        if (trade_fill.trade_fee_in_quote is None or
                Decimal(str(trade_fill.trade_fee_in_quote)) != Decimal(int(quote_fee * 1000000)) / 1000000):
            raise ValueError("Native quote fee encoding conflicts with exact receipt")
        ExecutorOrchestrator._validate_reconciliation_fee(trade_fill.trade_fee)
        executed_amount_quote = price * executed_amount_base
        client_order_id = f"{trade_fill.order_id}:{trade_fill.exchange_trade_id}"
        return {
            "client_order_id": client_order_id,
            "order_id": trade_fill.order_id,
            "exchange_order_id": trade_fill.order_id,
            "trade_type": str(trade_fill.trade_type or "").upper(),
            "price": price,
            "executed_amount_base": executed_amount_base,
            "executed_amount_quote": executed_amount_quote,
            "cumulative_fee_paid_quote": quote_fee,
            "precision": "event_exact",
            "amount": executed_amount_base,
            "order_fills": {str(trade_fill.exchange_trade_id): {
                "fill_base_amount": str(executed_amount_base), "fill_price": str(price),
                "fill_quote_amount": str(executed_amount_quote), "fee": trade_fill.trade_fee,
                "fee_in_quote": str(quote_fee)}},
            "last_update_timestamp": int(trade_fill.timestamp),
        }

    @staticmethod
    def _has_single_attempt_receipts(executor_info: ExecutorInfo) -> bool:
        custom = executor_info.custom_info or {}
        return ((custom.get("entry_lifecycle") or {}).get("policy") == "single_attempt"
                and isinstance(custom.get("execution_order_snapshots"), list))

    @staticmethod
    def _accounted_fill_keys(executor_info: ExecutorInfo) -> Set[Tuple[str, str, str, str]]:
        if executor_info.close_type == CloseType.POSITION_HOLD:
            return set()
        return {(executor_info.connector_name, executor_info.trading_pair,
                 str(order["client_order_id"]), str(trade_id))
                for order in (executor_info.custom_info or {}).get("execution_order_snapshots", [])
                for trade_id in order.get("order_fills", {})}

    def _executor_accounted_fill_keys(self, *, controller_ids: Optional[Set[str]] = None) -> Set[Tuple[str, str, str, str]]:
        keys = set(self._non_hold_fill_ids)
        for controller_id, executors in self.active_executors.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            for executor in executors:
                info = executor.executor_info
                if info.is_done and self._has_single_attempt_receipts(info):
                    keys.update(self._accounted_fill_keys(info))
        return keys

    @staticmethod
    def _entry_without_fill_handed_off(executor_info: ExecutorInfo) -> bool:
        """A terminal empty entry must not hide a subsequently recorded late fill.

        Its decision remains consumed, but late inventory belongs to the durable
        held-position reconciliation rather than a terminated executor listener.
        """
        lifecycle = (executor_info.custom_info or {}).get("entry_lifecycle") or {}
        return (executor_info.is_done and lifecycle.get("policy") == "single_attempt"
                and lifecycle.get("state") == "ended_no_fill")

    @staticmethod
    def _owned_order_keys(executor_info: ExecutorInfo) -> Set[Tuple[str, str, str]]:
        custom = executor_info.custom_info or {}
        order_ids = custom.get("order_ids") or []
        return {(executor_info.connector_name, executor_info.trading_pair, str(order_id))
                for order_id in order_ids if order_id}

    def _executor_owned_order_keys(self, *, controller_ids: Optional[Set[str]] = None) -> Set[Tuple[str, str, str]]:
        owned = set(self._non_hold_order_ids)
        for controller_id, executors in self.active_executors.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            for executor in executors:
                info = executor.executor_info
                if (info.type == "order_executor" or (info.is_done and info.close_type == CloseType.POSITION_HOLD)
                        or self._entry_without_fill_handed_off(info)
                        or (info.is_done and self._has_single_attempt_receipts(info))):
                    continue
                keys = self._owned_order_keys(info)
                if info.filled_amount_quote > 0 and not keys:
                    raise RuntimeError(f"Cannot reconcile executor {info.id}: filled execution has no order identities")
                owned.update(keys)
        return owned

    @staticmethod
    def _validate_reconciliation_fee(fee):
        if not isinstance(fee, dict) or not {"percent", "flat_fees"} <= fee.keys():
            raise ValueError("Fill fee receipt is incomplete")
        percent = Decimal(str(fee["percent"]))
        if not percent.is_finite() or percent != 0 or not isinstance(fee["flat_fees"], list):
            raise ValueError("Exact actual-currency fill fees required")
        for flat in fee["flat_fees"]:
            if not isinstance(flat.get("token"), str) or not flat["token"] or not Decimal(str(flat["amount"])).is_finite():
                raise ValueError("Invalid actual-currency fill fee")

    @classmethod
    def _corroborated_ledger_order(cls, entry, fills):
        payload = entry.order_payload
        required = {"client_order_id", "exchange_order_id", "trade_type", "executed_amount_base",
                    "executed_amount_quote", "cumulative_fee_paid_quote", "order_fills"}
        if not isinstance(payload, dict) or not required <= payload.keys():
            raise ValueError("Exact ledger order receipt is incomplete")
        if (payload["client_order_id"] != entry.client_order_id
                or payload["exchange_order_id"] != entry.exchange_order_id
                or payload["trade_type"] != entry.order_side):
            raise ValueError("Ledger order identity conflict")
        receipts = payload["order_fills"]
        if not isinstance(receipts, dict) or not receipts or set(receipts) != {str(fill.exchange_trade_id) for fill in fills}:
            raise ValueError("Ledger and native fill coverage differs")
        base, quote = Decimal("0"), Decimal("0")
        for fill in fills:
            receipt = receipts[str(fill.exchange_trade_id)]
            if any(receipt.get(field) != expected for field, expected in (
                    ("trade_id", str(fill.exchange_trade_id)), ("client_order_id", entry.client_order_id),
                    ("exchange_order_id", entry.exchange_order_id), ("trading_pair", entry.trading_pair))):
                raise ValueError("Ledger fill identity conflict")
            amount = Decimal(str(receipt["fill_base_amount"]))
            price = Decimal(str(receipt["fill_price"]))
            quote_amount = Decimal(str(receipt["fill_quote_amount"]))
            if (not all(value.is_finite() and value > 0 for value in (amount, price, quote_amount))
                    or amount * price != quote_amount or fill.trade_type != entry.order_side):
                raise ValueError("Ledger fill amounts conflict")
            cls._validate_reconciliation_fee(receipt["fee"])
            if receipt["fee"] != fill.trade_fee:
                raise ValueError("Native and ledger fill fee conflict")
            for stored, exact in ((fill.amount, amount), (fill.price, price)):
                if stored is None or Decimal(str(stored)) != Decimal(int(exact * 1000000)) / 1000000:
                    raise ValueError("Native legacy encoding conflicts with exact ledger")
            if fill.has_exact_receipt:
                if fill.amount_decimal != amount or fill.price_decimal != price:
                    raise ValueError("Native exact receipt conflicts with ledger")
                # Validate optional exact quote-fee metadata without manufacturing missing valuation.
                exact_fee = fill.trade_fee_in_quote_decimal
                if exact_fee is not None and (fill.trade_fee_in_quote is None or
                                              Decimal(str(fill.trade_fee_in_quote)) != Decimal(int(exact_fee * 1000000)) / 1000000):
                    raise ValueError("Native quote fee encoding conflicts with exact receipt")
            base += amount
            quote += quote_amount
        if base != Decimal(str(payload["executed_amount_base"])) or quote != Decimal(str(payload["executed_amount_quote"])):
            raise ValueError("Ledger aggregate and fill totals conflict")
        if "actual_base_fee" in payload and Decimal(str(payload["actual_base_fee"])) != base_fee_amount(
                payload, entry.trading_pair.split("-")[0]):
            raise ValueError("Explicit base fee conflicts with ledger fills")
        return dict(payload)

    def _is_unknown_wallet_sale(self, info: ExecutorInfo) -> bool:
        """Recognize only the explicit OK RSI spot-wallet disposal policy."""
        controller = self.strategy.controllers.get(info.controller_id)
        policy = getattr(controller, "config", None)
        config = info.config
        return (is_ok_rsi_policy(policy)
                and getattr(policy, "trade_mode", None) == "sell_only"
                and getattr(policy, "sell_only_strict_fraction", False) is True
                and getattr(policy, "sell_only_if_profitable", True) is False
                and info.type == "order_executor" and config.connector_name == "okx"
                and config.side == TradeType.SELL and config.position_action == PositionAction.CLOSE
                and config.level_id == "signal_exit" and config.min_sell_price is None
                and config.leverage == 1)

    @staticmethod
    def _wallet_sale_ledger_matches(entry, owner) -> bool:
        return (owner is not None and entry.executor_id == owner.id and entry.order_side == "SELL"
                and entry.position_side in (None, "", "None"))

    def _wallet_sale_owners(self, *, controller_ids: Optional[Set[str]] = None):
        owners = {}
        for controller in self.strategy.controllers:
            if controller_ids is not None and controller not in controller_ids:
                continue
            infos = {info.id: info for info in self.stored_executors.get(controller, [])}
            infos.update({executor.executor_info.id: executor.executor_info
                          for executor in self.active_executors.get(controller, [])})
            for info in infos.values():
                if not self._is_unknown_wallet_sale(info):
                    continue
                for connector, pair, order_id in self._owned_order_keys(info):
                    key = (controller, connector, pair, order_id)
                    if key in owners:
                        raise ValueError("Wallet sale order has ambiguous executor ownership")
                    owners[key] = info
        return owners

    def _build_reconciled_positions_from_trade_fills(
        self,
        trade_fills: List[TradeFill],
        ledger_entries=None,
        *,
        controller_ids: Optional[Set[str]] = None,
        check_existing=True,
        config_file_path=None,
    ) -> Tuple[Set[Tuple[str, str, str]], Dict[Tuple[str, str, str], PositionHold]]:
        """Build atomically from exact receipts; legacy rows can only corroborate ledger truth."""
        controller_by_pair, ambiguous = self._controller_ids_by_market_pair()
        selected_pairs = {(getattr(getattr(c, "config", c), "connector_name", None),
                           getattr(getattr(c, "config", c), "trading_pair", None))
                          for cid, c in self.strategy.controllers.items()
                          if controller_ids is None or cid in controller_ids}
        owned_orders = self._executor_owned_order_keys(controller_ids=controller_ids)
        accounted_fills = self._executor_accounted_fill_keys(controller_ids=controller_ids)
        groups, ledger_by_order, positions = {}, {}, {}
        wallet_sales, wallet_amounts = {}, {}
        owned_scopes = {(controller, position.connector_name, position.trading_pair)
                        for controller, held in self.positions_held.items() for position in held
                        if controller_ids is None or controller in controller_ids}
        try:
            wallet_owners = self._wallet_sale_owners(controller_ids=controller_ids)
            for fill in sorted(trade_fills, key=lambda fill: (fill.timestamp, fill.order_id, str(fill.exchange_trade_id))):
                pair = (fill.market, fill.symbol)
                if controller_ids is not None and pair not in selected_pairs:
                    continue
                if pair in ambiguous:
                    raise ValueError("Ambiguous fill ownership")
                controller = controller_by_pair.get(pair)
                if (controller is None or (controller_ids is not None and controller not in controller_ids)
                        or controller in self.initial_positions_by_controller
                        or "_perpetual" in fill.market or fill.symbol not in self.strategy.markets.get(fill.market, set())):
                    continue
                key = (controller, fill.market, fill.symbol, fill.order_id)
                if (config_file_path is not None and fill.config_file_path != config_file_path
                        or fill.base_asset != fill.symbol.split("-")[0]
                        or fill.quote_asset != fill.symbol.split("-")[1]
                        or fill.leverage != 1 or fill.position not in (None, "NIL")):
                    raise ValueError("Native fill attribution conflicts with spot scope")
                if not fill.order_id or not fill.exchange_trade_id or fill.trade_type not in {"BUY", "SELL"}:
                    raise ValueError("Invalid native fill identity")
                if fill.trade_type == "BUY":
                    owned_scopes.add(key[:3])
                order_key = (fill.market, fill.symbol, str(fill.order_id))
                if order_key in owned_orders or (*order_key, str(fill.exchange_trade_id)) in accounted_fills:
                    continue
                group = groups.setdefault(key, {})
                if group and any(existing.trade_type != fill.trade_type for existing in group.values()):
                    raise ValueError("Native fills disagree on immutable order side")
                identity = str(fill.exchange_trade_id)
                if identity in group:
                    raise ValueError("Duplicate native fill identity")
                group[identity] = fill
            for entry in ledger_entries or []:
                pair = (entry.connector_name, entry.trading_pair)
                controller = controller_by_pair.get(pair)
                if (controller is None or (controller_ids is not None and controller not in controller_ids)
                        or controller in self.initial_positions_by_controller
                        or "_perpetual" in entry.connector_name):
                    continue
                if controller != entry.controller_id or entry.position_side not in (None, "", "None"):
                    raise ValueError("Ledger ownership conflict")
                key = (controller, *pair, entry.client_order_id)
                if not self._wallet_sale_ledger_matches(entry, wallet_owners.get(key)):
                    owned_scopes.add(key[:3])
                if (*pair, entry.client_order_id) in owned_orders:
                    continue
                if key in ledger_by_order:
                    raise ValueError("Duplicate ledger order identity")
                ledger_by_order[key] = entry
            if not set(ledger_by_order) <= set(groups):
                raise ValueError("Native fills omit exact ledger orders")
            # Include flat histories: absence of a position summary is not proof of no prior fills.
            for controller, held in (self.positions_held.items() if check_existing else []):
                if controller_ids is not None and controller not in controller_ids:
                    continue
                if controller in self.initial_positions_by_controller:
                    continue
                for position in held:
                    if "_perpetual" in position.connector_name:
                        continue
                    if not position._order_records and self._position_hold_amount(position) != 0:
                        raise ValueError("Stored position has no exact order history")
                    for record in position._order_records.values():
                        order_id = record.get("order_id") or record.get("client_order_id")
                        if (position.connector_name, position.trading_pair, order_id) in owned_orders:
                            continue
                        key = (controller, position.connector_name, position.trading_pair, order_id)
                        if key not in groups or not set(record.get("order_fills", {})) <= set(groups[key]):
                            raise ValueError("Native fills omit existing exact held history")
                        entry = ledger_by_order.get(key)
                        if (entry is not None and set(record.get("order_fills", {})) == set(entry.order_payload["order_fills"])
                                and Decimal(str(record["cumulative_fee_paid_quote"])) !=
                                Decimal(str(entry.order_payload["cumulative_fee_paid_quote"]))):
                            raise ValueError("Existing exact ledger quote fee changed under the same fill coverage")
                        for trade_id, previous in record.get("order_fills", {}).items():
                            fill = groups[key][trade_id]
                            entry = ledger_by_order.get(key)
                            current = (entry.order_payload["order_fills"][trade_id] if entry is not None else
                                       self._trade_fill_order_record(fill)["order_fills"][trade_id])
                            if "fee_in_quote" in previous and Decimal(str(previous["fee_in_quote"])) != fill.trade_fee_in_quote_decimal:
                                raise ValueError("Existing exact quote fee changed under the same identity")
                            if (record["trade_type"] != fill.trade_type or previous.get("fee") != current.get("fee")
                                    or any(Decimal(str(previous[field])) != Decimal(str(current[field])) for field in
                                           ("fill_base_amount", "fill_price", "fill_quote_amount"))):
                                raise ValueError("Existing exact fill changed under the same identity")
            replay = []
            for key, native in groups.items():
                entry = ledger_by_order.get(key)
                fills = list(native.values())
                owner = wallet_owners.get(key) if key[:3] not in owned_scopes else None
                if owner is not None and all(fill.trade_type == "SELL" for fill in fills):
                    if entry is not None:
                        payload = self._corroborated_ledger_order(entry, fills)
                        native_fees = sum((self._trade_fill_order_record(fill)["cumulative_fee_paid_quote"]
                                           for fill in fills), Decimal("0"))
                        if Decimal(str(payload["cumulative_fee_paid_quote"])) != native_fees:
                            raise ValueError("Wallet sale ledger quote fee conflicts with exact native fees")
                    for fill in fills:
                        record = self._trade_fill_order_record(fill)
                        amount = record["executed_amount_base"]
                        wallet_amounts[owner.id] = wallet_amounts.get(owner.id, Decimal("0")) + amount
                        if not owner.config.amount.is_finite() or wallet_amounts[owner.id] > owner.config.amount:
                            raise ValueError("Wallet sale fills exceed the owning executor amount")
                        wallet_sales.setdefault(key[:3], []).append({
                            "connector_name": fill.market, "trading_pair": fill.symbol,
                            "order_id": fill.order_id, "trade_id": str(fill.exchange_trade_id), "timestamp": fill.timestamp,
                            "amount_base": amount, "proceeds_quote": record["executed_amount_quote"],
                            "fees_quote": record["cumulative_fee_paid_quote"], "fee": fill.trade_fee})
                    continue
                if entry is not None:
                    replay.append((entry.timestamp, entry.client_order_id, "", key[:3],
                                   self._corroborated_ledger_order(entry, fills)))
                else:
                    for fill in fills:
                        replay.append((fill.timestamp, fill.order_id, str(fill.exchange_trade_id), key[:3],
                                       self._trade_fill_order_record(fill)))
            # Replay ledger orders in the recorder's authoritative timestamp/client-ID order.
            # Native fills for other orders retain their own event sequence, including interleaving.
            for _, _, _, key, record in sorted(replay, key=lambda item: item[:3]):
                controller, connector, pair = key
                position = positions.setdefault(key, PositionHold(connector, pair, TradeType.BUY))
                position.add_order_record(record)
            for owner in {info.id: info for info in wallet_owners.values()}.values():
                if owner.is_done or owner.status == RunnableStatus.SHUTTING_DOWN:
                    scope = (owner.controller_id, owner.connector_name, owner.trading_pair)
                    if scope not in owned_scopes:
                        observed = Decimal(str(owner.custom_info.get("executed_amount_base")))
                        if not observed.is_finite() or observed != wallet_amounts.get(owner.id, Decimal("0")):
                            raise ValueError("Wallet sale native fills omit recorded execution")
            for scope, previous_sales in self._wallet_sale_receipts.items():
                if controller_ids is not None and scope[0] not in controller_ids:
                    continue
                current = {(sale["order_id"], sale["trade_id"]): sale for sale in wallet_sales.get(scope, [])}
                if any(current.get((sale["order_id"], sale["trade_id"])) != sale for sale in previous_sales):
                    raise ValueError("Previously observed wallet sale receipts changed or disappeared")
            for summary in self._wallet_sale_position_summaries:
                if controller_ids is not None and summary.controller_id not in controller_ids:
                    continue
                scope = (summary.controller_id, summary.connector_name, summary.trading_pair)
                validate_wallet_sale_summary(summary, wallet_sales.get(scope, []))
            if any(position.buy_amount_base < position.sell_amount_base for position in positions.values()):
                raise ValueError("Spot fill history produces unowned negative inventory")
        except (ValueError, TypeError, KeyError, ArithmeticError) as error:
            raise RuntimeError(f"Held inventory reconciliation unavailable: {error}") from error
        retained = {} if controller_ids is None else {
            scope: sales for scope, sales in self._wallet_sale_receipts.items() if scope[0] not in controller_ids}
        self._wallet_sale_receipts = retained
        self._wallet_sale_receipts.update(deepcopy(wallet_sales))
        return set(positions) | set(wallet_sales), positions

    def _reconcile_positions_from_trade_fills(self, *, controller_ids: Optional[Set[str]] = None):
        try:
            markets_recorder = MarketsRecorder.get_instance()
        except TypeError:
            return
        config_file_path = getattr(markets_recorder, "config_file_path", None)
        if not config_file_path:
            return
        try:
            trade_fills = markets_recorder.get_trades_for_config(config_file_path)
        except Exception as e:
            raise RuntimeError("Held inventory reconciliation unavailable: native fill read failed") from e
        ledger_entries = markets_recorder.get_all_position_hold_ledger_entries()
        seen_market_pairs, reconciled_positions = self._build_reconciled_positions_from_trade_fills(
            trade_fills, ledger_entries, config_file_path=config_file_path, controller_ids=controller_ids)
        self._recover_completed_wallet_sales(markets_recorder, ledger_entries, controller_ids=controller_ids)
        for controller_id, connector_name, trading_pair in seen_market_pairs:
            positions = self.positions_held.setdefault(controller_id, [])
            existing_position = self._find_existing_position(
                positions=positions,
                connector_name=connector_name,
                trading_pair=trading_pair,
                position_side=None,
            )
            replacement_position = reconciled_positions.get((controller_id, connector_name, trading_pair))
            positions = [
                position for position in positions
                if not (position.connector_name == connector_name and position.trading_pair == trading_pair)
            ]
            if replacement_position is not None:
                positions.append(replacement_position)
                try:
                    mid_price = self.strategy.market_data_provider.get_price_by_type(
                        connector_name, trading_pair, PriceType.MidPrice
                    )
                except Exception:
                    mid_price = Decimal("0")
                replacement_summary = replacement_position.get_position_summary(mid_price or Decimal("0"))
                existing_amount = Decimal("0")
                if existing_position is not None:
                    existing_amount = existing_position.get_position_summary(mid_price or Decimal("0")).amount
                if existing_amount != replacement_summary.amount:
                    self.logger().info(
                        f"Reconciled held position from TradeFill | controller={controller_id} "
                        f"pair={trading_pair} amount={replacement_summary.amount} "
                        f"breakeven={replacement_summary.breakeven_price}"
                    )
            self.positions_held[controller_id] = positions

    def _recover_completed_wallet_sales(self, recorder, ledger_entries, *, controller_ids: Optional[Set[str]] = None):
        """Resolve only fully filled saved wallet sellers using immutable local execution receipts."""
        recovered = []
        ledger = {(entry.controller_id, entry.connector_name, entry.trading_pair, entry.client_order_id): entry
                  for entry in ledger_entries}
        try:
            for controller_id, infos in self.stored_executors.items():
                if controller_ids is not None and controller_id not in controller_ids:
                    continue
                for info in infos:
                    if info.is_done or not self._is_unknown_wallet_sale(info):
                        continue
                    scope = (controller_id, info.connector_name, info.trading_pair)
                    sales = self._wallet_sale_receipts.get(scope, [])
                    order_ids = {key[2] for key in self._owned_order_keys(info)}
                    payloads = info.custom_info.get("held_position_orders", [])
                    if (info.status != RunnableStatus.SHUTTING_DOWN or info.close_type != CloseType.POSITION_HOLD
                            or not order_ids or len(payloads) != len(order_ids)
                            or {order.get("client_order_id") for order in payloads} != order_ids
                            or {sale["order_id"] for sale in sales if sale["order_id"] in order_ids} != order_ids):
                        raise ValueError("Saved wallet seller lacks complete terminal receipt coverage")
                    total = Decimal("0")
                    with recorder._sql_manager.get_new_session() as session:
                        for payload in payloads:
                            oid = payload["client_order_id"]
                            total += validate_completed_wallet_order(
                                info, payload, ledger.get((*scope, oid)), session.get(Order, oid),
                                session.get(OrderExact, oid), recorder.config_file_path)
                    if (total != info.config.amount or total != Decimal(str(info.custom_info.get("executed_amount_base")))):
                        raise ValueError("Saved wallet seller was not completely filled")
                    recovered.append((controller_id, info))
        except Exception as error:
            raise RuntimeError(f"Held inventory reconciliation unavailable: wallet sale terminal proof: {error}") from error
        for controller_id, info in recovered:
            replacement = info.model_copy(update={
                "status": RunnableStatus.TERMINATED, "is_active": False, "is_trading": False,
                "custom_info": dict(info.custom_info, wallet_sale_terminal_recovery="exact_completed_order_receipts")})
            self._record_stored_executor(controller_id, replacement)
            self._update_cached_performance(controller_id, replacement)
            pending = self.recovery_required.get(controller_id, [])
            remaining = [item for item in pending if item != info.id]
            if remaining:
                self.recovery_required[controller_id] = remaining
            else:
                self.recovery_required.pop(controller_id, None)

    def _load_position_from_db(self, controller_id: str, db_position: Position):
        """
        Load a position from the database and recreate it as a PositionHold object.
        Since the database only stores net position data, we reconstruct the PositionHold
        with the assumption that it represents the remaining net position.
        """
        # Convert the database position back to a PositionHold object
        side = TradeType.BUY if db_position.side == "BUY" else TradeType.SELL
        position_hold = PositionHold(db_position.connector_name, db_position.trading_pair, side)

        # Set the aggregated values from the database
        position_hold.volume_traded_quote = db_position.volume_traded_quote
        position_hold.cum_fees_quote = db_position.cum_fees_quote

        # Since the database stores the net position, we need to reconstruct the buy/sell amounts
        # We assume this represents the remaining unmatched position after any realized trades
        if db_position.side == "BUY":
            # This is a net long position
            position_hold.buy_amount_base = db_position.amount
            position_hold.buy_amount_quote = db_position.amount * db_position.breakeven_price
            position_hold.sell_amount_base = Decimal("0")
            position_hold.sell_amount_quote = Decimal("0")
        else:
            # This is a net short position
            position_hold.sell_amount_base = db_position.amount
            position_hold.sell_amount_quote = db_position.amount * db_position.breakeven_price
            position_hold.buy_amount_base = Decimal("0")
            position_hold.buy_amount_quote = Decimal("0")

        # Add to positions held
        self.positions_held[controller_id].append(position_hold)
        self.logger().info(
            f"Restored held position | controller={controller_id} pair={db_position.trading_pair} "
            f"side={db_position.side} amount={db_position.amount} breakeven={db_position.breakeven_price}"
        )

    def _create_initial_positions(self):
        """
        Create initial positions from config overrides.
        Uses NaN for quote amounts initially - they will be calculated lazily when needed.
        """
        for controller_id, initial_positions in self.initial_positions_by_controller.items():
            if controller_id not in self.cached_performance:
                self.cached_performance[controller_id] = PerformanceReport()
                self.active_executors[controller_id] = []
                self.stored_executors[controller_id] = []
                self.positions_held[controller_id] = []

            for position_config in initial_positions:
                # Create PositionHold object
                position_hold = PositionHold(
                    position_config.connector_name,
                    position_config.trading_pair,
                    position_config.side
                )

                # Set amounts based on side, using NaN for quote amounts
                if position_config.side == TradeType.BUY:
                    position_hold.buy_amount_base = position_config.amount
                    position_hold.buy_amount_quote = Decimal("NaN")  # Will be calculated lazily
                    position_hold.sell_amount_base = Decimal("0")
                    position_hold.sell_amount_quote = Decimal("0")
                else:
                    position_hold.sell_amount_base = position_config.amount
                    position_hold.sell_amount_quote = Decimal("NaN")  # Will be calculated lazily
                    position_hold.buy_amount_base = Decimal("0")
                    position_hold.buy_amount_quote = Decimal("0")

                # Set fees and volume to 0 (as specified - this is a fresh start)
                position_hold.volume_traded_quote = Decimal("0")
                position_hold.cum_fees_quote = Decimal("0")

                # Add to positions held
                self.positions_held[controller_id].append(position_hold)

                self.logger().info(f"Created initial position for controller {controller_id}: {position_config.amount} "
                                   f"{position_config.side.name} {position_config.trading_pair} on {position_config.connector_name}")

    async def stop(self,
                   max_executors_close_attempts: int = 3,
                   keep_position_by_controller: Optional[Dict[str, bool]] = None,
                   strict: bool = False):
        """
        Stop the orchestrator task and all active executors.
        """
        if self._planned_resume_pending:
            return ShutdownResult(False, "Planned position resume unresolved; durable ownership requires reconciliation")
        keep_position_by_controller = keep_position_by_controller or {}
        if strict:
            try:
                for controller_id, executors in self.active_executors.items():
                    for executor in executors:
                        if not executor.is_closed:
                            executor.early_stop(keep_position=keep_position_by_controller.get(controller_id, True))
                for _ in range(max_executors_close_attempts):
                    if all(executor.executor_info.is_done for executors in self.active_executors.values() for executor in executors):
                        break
                    await asyncio.sleep(2.0)
                if any(not executor.executor_info.is_done for executors in self.active_executors.values() for executor in executors):
                    return ShutdownResult(False, "Executor shutdown timed out; runtime collections retained")
                self.store_all_positions(clear=False, strict=True)
                self.store_all_executors(clear=False)
                return ShutdownResult(True)
            except Exception as error:
                self.logger().exception("Strict executor shutdown failed; runtime collections retained")
                return ShutdownResult(False, f"Executor persistence/shutdown failed: {type(error).__name__}")
        # first we stop all active executors
        for controller_id, executors_list in self.active_executors.items():
            keep_position = keep_position_by_controller.get(controller_id, True)
            for executor in executors_list:
                if not executor.is_closed:
                    executor.early_stop(keep_position=keep_position)
        for _ in range(max_executors_close_attempts):
            if all([executor.executor_info.is_done for executors_list in self.active_executors.values()
                    for executor in executors_list]):
                break
            await asyncio.sleep(2.0)
        else:
            pending_executors = [
                executor.config.id for executors_list in self.active_executors.values()
                for executor in executors_list if not executor.executor_info.is_done
            ]
            if pending_executors:
                self.logger().warning(
                    f"Shutdown timed out while waiting for executors to stop: {pending_executors}"
                )
        # Store all positions and executors
        self.store_all_positions()
        self.store_all_executors()
        # Clear executors and trigger garbage collection
        self.active_executors.clear()

    def store_all_positions(self, *, clear: bool = True, strict: bool = False, controller_ids: Optional[Set[str]] = None):
        """
        Store or update all positions in the database.
        """
        self._update_positions_from_done_executors(controller_ids=controller_ids)
        markets_recorder = MarketsRecorder.get_instance()
        for controller_id, positions_list in self.positions_held.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            if controller_id is None:
                if strict and positions_list:
                    raise ValueError("Cannot reconcile held positions without controller identity")
                self.logger().warning(f"Skipping {len(positions_list)} position(s) with no controller_id")
                continue
            for position in positions_list:
                # Skip if the connector/trading pair is not in the current strategy markets
                if (position.connector_name not in self.strategy.markets or
                        position.trading_pair not in self.strategy.markets.get(position.connector_name, set())):
                    if strict:
                        raise ValueError("Cannot reconcile held positions outside current strategy markets")
                    self.logger().warning(f"Skipping position storage for {position.connector_name}.{position.trading_pair} - "
                                          f"not available in current strategy markets")
                    continue
                mid_price = self.strategy.market_data_provider.get_price_by_type(
                    position.connector_name, position.trading_pair, PriceType.MidPrice)
                position_summary = position.get_position_summary(mid_price)

                # Create a Position record (id will only be used for new positions)
                position_record = Position(
                    id=str(uuid.uuid4()),
                    controller_id=controller_id,
                    connector_name=position_summary.connector_name,
                    trading_pair=position_summary.trading_pair,
                    side=position_summary.side.name,
                    timestamp=int(self.strategy.current_timestamp * 1e3),
                    volume_traded_quote=position_summary.volume_traded_quote,
                    amount=position_summary.amount,
                    breakeven_price=position_summary.breakeven_price,
                    unrealized_pnl_quote=position_summary.unrealized_pnl_quote,
                    realized_pnl_quote=position_summary.realized_pnl_quote,
                    cum_fees_quote=position_summary.cum_fees_quote,
                )
                # Store or update the position in the database
                markets_recorder.update_or_store_position(position_record)
                if position_summary.amount > Decimal("0"):
                    self.logger().info(
                        f"Stored held position | controller={controller_id} pair={position_summary.trading_pair} "
                        f"side={position_summary.side.name} amount={position_summary.amount} "
                        f"breakeven={position_summary.breakeven_price}"
                    )
                else:
                    self.logger().info(
                        f"Cleared flat held position | controller={controller_id} pair={position_summary.trading_pair} "
                        f"side={position_summary.side.name}"
                    )

        # Clear all positions after storing (avoid modifying list while iterating)
        if clear:
            if controller_ids is None:
                self.positions_held.clear()
            else:
                for controller_id in controller_ids:
                    self.positions_held.pop(controller_id, None)

    def sync_live_positions_to_db(self, *, controller_ids: Optional[Set[str]] = None):
        """
        Mirror the current in-memory held positions into the database without clearing them.
        This keeps SQLite closer to live runtime state while preserving the existing shutdown flow.
        """
        self._update_positions_from_done_executors(controller_ids=controller_ids)
        markets_recorder = MarketsRecorder.get_instance()
        live_position_keys: Set[Tuple[str, str, str, str]] = set()

        for controller_id, positions_list in self.positions_held.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            if controller_id is None:
                continue
            for position in positions_list:
                if (
                    position.connector_name not in self.strategy.markets
                    or position.trading_pair not in self.strategy.markets.get(position.connector_name, set())
                ):
                    continue
                mid_price = self.strategy.market_data_provider.get_price_by_type(
                    position.connector_name,
                    position.trading_pair,
                    PriceType.MidPrice,
                )
                position_summary = position.get_position_summary(mid_price)
                live_position_keys.add(
                    (
                        controller_id,
                        position_summary.connector_name,
                        position_summary.trading_pair,
                        position_summary.side.name,
                    )
                )
                position_record = Position(
                    id=str(uuid.uuid4()),
                    controller_id=controller_id,
                    connector_name=position_summary.connector_name,
                    trading_pair=position_summary.trading_pair,
                    side=position_summary.side.name,
                    timestamp=int(self.strategy.current_timestamp * 1e3),
                    volume_traded_quote=position_summary.volume_traded_quote,
                    amount=position_summary.amount,
                    breakeven_price=position_summary.breakeven_price,
                    unrealized_pnl_quote=position_summary.unrealized_pnl_quote,
                    realized_pnl_quote=position_summary.realized_pnl_quote,
                    cum_fees_quote=position_summary.cum_fees_quote,
                )
                markets_recorder.update_or_store_position(position_record)

        for controller_id in self.strategy.controllers.keys():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            for db_position in markets_recorder.get_positions_by_controller(controller_id):
                position_key = (
                    controller_id,
                    db_position.connector_name,
                    db_position.trading_pair,
                    db_position.side,
                )
                if position_key in live_position_keys:
                    continue
                markets_recorder.update_or_store_position(
                    Position(
                        id=db_position.id,
                        controller_id=db_position.controller_id,
                        connector_name=db_position.connector_name,
                        trading_pair=db_position.trading_pair,
                        side=db_position.side,
                        timestamp=int(self.strategy.current_timestamp * 1e3),
                        volume_traded_quote=Decimal("0"),
                        amount=Decimal("0"),
                        breakeven_price=Decimal("0"),
                        unrealized_pnl_quote=Decimal("0"),
                        realized_pnl_quote=Decimal("0"),
                        cum_fees_quote=db_position.cum_fees_quote,
                    )
                )

    def store_all_executors(self, *, clear: bool = True, controller_ids: Optional[Set[str]] = None):
        for controller_id, executors_list in self.active_executors.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            for executor in executors_list:
                # Store the executor in the database
                MarketsRecorder.get_instance().store_or_update_executor(executor)
                self._persist_position_hold_entries_from_executor_info(executor.executor_info)
                if clear:
                    self._update_cached_performance(controller_id, executor.executor_info)
                    self._record_stored_executor(controller_id, executor.executor_info)
        # Remove the executors from the list
        if clear:
            if controller_ids is None:
                self.active_executors = {}
            else:
                for controller_id in controller_ids:
                    self.active_executors.pop(controller_id, None)

    def sync_live_executors_to_db(self, *, controller_ids: Optional[Set[str]] = None):
        """
        Mirror active and recently finished executors into the database without mutating the in-memory sets.
        """
        for controller_id, executors_list in self.active_executors.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            for executor in executors_list:
                MarketsRecorder.get_instance().store_or_update_executor(executor)
                self._persist_position_hold_entries_from_executor_info(executor.executor_info)

    def sync_live_state_to_db(self, *, controller_ids: Optional[Set[str]] = None):
        self.sync_live_positions_to_db(controller_ids=controller_ids)
        self.sync_live_executors_to_db(controller_ids=controller_ids)

    def execute_action(self, action: ExecutorAction):
        """
        Execute the action and handle executors based on action type.
        """
        controller_id = action.controller_id
        if controller_id is None:
            self.logger().error(f"Received action with controller_id=None: {action}. "
                                "Check that the controller config has a valid 'id' field.")
            return
        if controller_id not in self.cached_performance:
            self.active_executors[controller_id] = []
            self.stored_executors[controller_id] = []
            self.positions_held[controller_id] = []
            self.cached_performance[controller_id] = PerformanceReport()

        if isinstance(action, CreateExecutorAction):
            self.create_executor(action)
        elif isinstance(action, StopExecutorAction):
            self.stop_executor(action)
        elif isinstance(action, StoreExecutorAction):
            self.store_executor(action)

    def execute_actions(self, actions: List[ExecutorAction]):
        """
        Execute a list of actions.
        """
        for action in actions:
            self.execute_action(action)

    def create_executor(self, action: CreateExecutorAction):
        """
        Create an executor based on the configuration in the action.
        """
        if getattr(self.strategy, "_unattended_restart_hold", False) is True:
            raise RuntimeError("V2 unattended restart hold; executor creation is blocked")
        if self._planned_resume_pending:
            raise RuntimeError("Planned position ownership transfer is pending; execution is blocked")
        controller_id = action.controller_id
        executor_config = action.executor_config
        if controller_id in getattr(self.strategy, "_controller_restart_required", {}):
            raise RuntimeError(f"Controller {controller_id} requires a process restart; new execution is blocked")
        unresolved = {owner: ids for owner, ids in self.recovery_required.items() if ids}
        if unresolved:
            raise RuntimeError(
                f"Stack requires startup reconciliation of executors {unresolved}; "
                "new execution remains blocked"
            )
        if any(executor.config.id == executor_config.id for executor in self.active_executors.get(controller_id, [])):
            raise ValueError(f"Duplicate executor id: {executor_config.id}")

        if getattr(self.strategy, "_is_stop_triggered", False) is True:
            raise RuntimeError("Strategy is stopping; new execution is blocked")
        # Check transient account availability before a controller can persist a
        # funded intent. A normal cash shortage must not create an orphan claim.
        if getattr(executor_config, 'connector_name', None) == 'okx':
            owned = {key[2] for executors in self.active_executors.values() for executor in executors
                     for key in self._owned_order_keys(executor.executor_info)}
            unknown = [oid for oid, order in self.strategy.connectors['okx'].in_flight_orders.items()
                       if order.trading_pair == executor_config.trading_pair and oid not in owned]
            if unknown:
                raise RuntimeError(f'Unowned OKX orders require reconciliation: {unknown}')
        self._validate_okx_wallet_reservation(executor_config, controller_id=controller_id)
        controller = self.strategy.controllers.get(controller_id)
        if controller is not None:
            if getattr(self.strategy, "_controller_reports_valid", None) is False:
                raise RuntimeError("Current execution reports are unavailable")
            peers = getattr(controller, '_funding_peers', None)
            report_ids = {peer.config.id for peer in peers} if isinstance(peers, tuple) and peers else {controller_id}
            reports = self.get_executors_report(controller_ids=report_ids)
            if isinstance(peers, tuple) and peers:
                for peer in peers:
                    if self.strategy.controllers.get(peer.config.id) is not peer:
                        raise RuntimeError('Funding owner identity changed; reconciliation required')
                    peer.executors_info = reports.get(peer.config.id, [])
            else:
                controller.executors_info = reports.get(controller_id, [])
            validator = getattr(controller, "validate_executor_config", None)
            if validator is not None and not validator(executor_config):
                self.logger().warning(f"Rejected executor {executor_config.id}: current controller gates do not permit execution")
                return
        executor_config.controller_id = controller_id

        executor_class = self._resolve_executor_class(controller_id, executor_config)
        if executor_class is not None:
            executor = executor_class(
                strategy=self.strategy,
                config=executor_config,
                update_interval=self.executors_update_interval,
                max_retries=self.executors_max_retries,
            )
        else:
            raise ValueError("Unsupported executor config type")

        # Persist intent before starting anything that can submit an order. A failed write must not start execution.
        MarketsRecorder.get_instance().store_or_update_executor(executor)
        commit = getattr(controller, "commit_entry_executor", None)
        if callable(commit):
            commit(executor_config)
        self.active_executors[controller_id].append(executor)
        if self._executor_start is None:
            executor.start()
        else:
            self._executor_start(executor)
        self.logger().debug(f"Created {type(executor).__name__} for controller {controller_id}")

    def _validate_okx_wallet_reservation(self, config, *, controller_id=None):
        if getattr(config, "connector_name", None) != "okx" or config.type not in {"position_executor", "order_executor"}:
            return
        base, quote = config.trading_pair.split("-")
        asset = quote if config.side == TradeType.BUY else base
        connector = self.strategy.connectors[config.connector_name]
        available = Decimal(str(connector.get_available_balance(asset)))

        def funded_multiplier(owner_id):
            from hummingbot.strategy_v2.controllers.native_policy_identity import is_modular_policy
            owner = self.strategy.controllers.get(owner_id)
            policy = getattr(getattr(owner, 'config', None), 'funding', None)
            if owner is not None and is_modular_policy(owner.config) and policy is not None:
                return Decimal('1') + policy.fee_reserve_fraction
            return Decimal('1')
        pending = Decimal("0")
        for executors in self.active_executors.values():
            for executor in executors:
                other = executor.config
                if executor.is_closed or getattr(other, "connector_name", None) != config.connector_name:
                    continue
                if other.type not in {"position_executor", "order_executor"} or other.side != config.side:
                    continue
                other_base, other_quote = other.trading_pair.split("-")
                if (other_quote if other.side == TradeType.BUY else other_base) != asset:
                    continue
                order = getattr(executor, "_open_order", None) if other.type == "position_executor" else getattr(executor, "_order", None)
                if order is not None and order.order is not None and order.order.exchange_order_id is not None:
                    continue  # The connector/exchange balance now owns the reservation.
                remaining = Decimal(str(other.amount))
                if other.type == "position_executor":
                    remaining -= executor.open_filled_amount
                else:
                    remaining -= sum((item.executed_amount_base for item in executor._all_orders()), Decimal("0"))
                if other.side == TradeType.BUY:
                    price = getattr(other, "entry_price", None) or getattr(other, "price", None)
                    if price is None:
                        raise RuntimeError("Pending order has no reservable price")
                    remaining *= Decimal(str(price))
                    remaining *= funded_multiplier(other.controller_id)
                pending += max(Decimal("0"), remaining)
        required = Decimal(str(config.amount))
        if config.side == TradeType.BUY:
            price = getattr(config, "entry_price", None) or getattr(config, "price", None)
            if price is None:
                raise RuntimeError("New order has no reservable price")
            required *= Decimal(str(price))
            required *= funded_multiplier(controller_id or config.controller_id)
        if not all(value.is_finite() for value in (available, pending, required)) or required <= 0 or required + pending > available:
            raise RuntimeError(f"Insufficient unreserved {asset}: required={required}, pending={pending}, available={available}")

    def _resolve_executor_class(self, controller_id: str, executor_config):
        controller = self.strategy.controllers.get(controller_id)
        if controller is not None:
            override = controller.get_executor_class_for_config(executor_config)
            if override is not None:
                return override
        return self._executor_mapping.get(executor_config.type)

    def stop_executor(self, action: StopExecutorAction):
        """
        Stop an executor based on the action details.
        """
        controller_id = action.controller_id
        executor_id = action.executor_id

        executor = next(
            (executor for executor in self.active_executors[controller_id] if executor.config.id == executor_id),
            None)
        if not executor:
            self.logger().error(f"Executor ID {executor_id} not found for controller {controller_id}.")
            return
        executor.early_stop(action.keep_position)

    def _update_positions_from_done_executors(self, *, controller_ids: Optional[Set[str]] = None):
        """
        Update positions from executors that are done but haven't been processed yet.
        This is called before generating reports to ensure position state is current.
        """
        for controller_id, executors_list in self.active_executors.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            # Filter executors that need position updates
            executors_to_process = [
                executor for executor in executors_list
                if (executor.executor_info.is_done and
                    executor.executor_info.close_type == CloseType.POSITION_HOLD and
                    executor.executor_info.config.id not in self.executors_ids_position_held)
            ]

            # Skip if no executors to process
            if not executors_to_process:
                continue

            for executor in executors_to_process:
                executor_info = executor.executor_info
                self.executors_ids_position_held.append(executor_info.config.id)
                self._persist_position_hold_entries_from_executor_info(executor_info)
                self._apply_executor_info_to_positions(controller_id, executor_info)
            self._prune_flat_positions(controller_id)

    def _determine_position_side(self, executor_info: ExecutorInfo) -> Optional[TradeType]:
        """
        Determine the position side for an executor, handling perpetual markets.
        """
        is_perpetual = "_perpetual" in executor_info.connector_name
        if not is_perpetual:
            return None

        market = self.strategy.connectors.get(executor_info.connector_name)
        if not market or not hasattr(market, 'position_mode'):
            return None

        position_mode = market.position_mode
        if hasattr(executor_info.config, "position_action") and position_mode == PositionMode.HEDGE:
            opposite_side = TradeType.BUY if executor_info.config.side == TradeType.SELL else TradeType.SELL
            return opposite_side if executor_info.config.position_action == PositionAction.CLOSE else executor_info.config.side

        return executor_info.config.side

    def _find_existing_position(
        self,
        positions: List[PositionHold],
        connector_name: str,
        trading_pair: str,
        position_side: Optional[TradeType],
    ) -> Optional[PositionHold]:
        """
        Find an existing position that matches the executor's trading pair and side.
        """
        for position in positions:
            if (position.trading_pair == trading_pair and
                    position.connector_name == connector_name):

                # If we have a specific position side, match it
                if position_side is not None:
                    if position.side == position_side:
                        return position
                else:
                    # No specific side requirement, return first match
                    return position

        return None

    def store_executor(self, action: StoreExecutorAction):
        """
        Store executor data based on the action details and update cached performance.
        """
        controller_id = action.controller_id
        executor_id = action.executor_id

        executor = next(
            (executor for executor in self.active_executors[controller_id] if executor.config.id == executor_id),
            None)
        if not executor:
            self.logger().error(f"Executor ID {executor_id} not found for controller {controller_id}.")
            return
        if executor.is_active:
            self.logger().error(f"Executor ID {executor_id} is still active.")
            return
        try:
            MarketsRecorder.get_instance().store_or_update_executor(executor)
            self._persist_position_hold_entries_from_executor_info(executor.executor_info)
            self._update_cached_performance(controller_id, executor.executor_info)
            self._record_stored_executor(controller_id, executor.executor_info)
        except Exception as e:
            self.logger().error(f"Error storing executor id {executor_id}: {str(e)}.")
            self.logger().error(f"Executor info: {executor.executor_info} | Config: {executor.config}")
            return

        self.active_executors[controller_id].remove(executor)
        del executor
        # Trigger garbage collection after executor cleanup

    def get_executors_report(self, *, controller_ids: Optional[Set[str]] = None) -> Dict[str, List[ExecutorInfo]]:
        """
        Generate a report of all executors.
        """
        report = {}
        selected_ids = set(self.active_executors) | set(self.stored_executors)
        if controller_ids is not None:
            selected_ids &= controller_ids
        for controller_id in selected_ids:
            active_executors = [executor.executor_info for executor in self.active_executors.get(controller_id, []) if executor]
            active_executor_ids = {executor.id for executor in active_executors}
            stored_executors = [
                executor
                for executor in self.stored_executors.get(controller_id, [])
                if executor.id not in active_executor_ids
            ]
            report[controller_id] = []
            for info in active_executors + stored_executors:
                scope = (controller_id, info.connector_name, info.trading_pair)
                orders = {key[2] for key in self._owned_order_keys(info)}
                sales = [sale for sale in self._wallet_sale_receipts.get(scope, []) if sale["order_id"] in orders]
                if sales:
                    info = info.model_copy(update={
                        "net_pnl_quote": None, "net_pnl_pct": None,
                        "cum_fees_quote": sum((sale["fees_quote"] for sale in sales), Decimal("0")),
                        "filled_amount_quote": sum((sale["proceeds_quote"] for sale in sales), Decimal("0")),
                        "custom_info": dict(info.custom_info, pnl_available=False,
                                            pnl_unavailable_reason="unknown_wallet_acquisition_cost")})
                report[controller_id].append(info)
        return report

    def get_positions_report(self, *, controller_ids: Optional[Set[str]] = None) -> Dict[str, List[PositionSummary]]:
        """
        Generate a report of all positions held.
        """
        report = {}
        for controller_id, positions_list in self.positions_held.items():
            if controller_ids is not None and controller_id not in controller_ids:
                continue
            positions_summary = []
            for position in positions_list:
                mid_price = self.strategy.market_data_provider.get_price_by_type(
                    position.connector_name, position.trading_pair, PriceType.MidPrice)
                summary = position.get_position_summary(mid_price)
                if summary.amount > Decimal("0"):
                    positions_summary.append(summary)
            report[controller_id] = positions_summary
        return report

    def get_all_reports(self, *, controller_ids: Optional[Set[str]] = None) -> Dict[str, Dict]:
        """
        Generate native reports for all owners, or only explicitly selected owners.

        Scoped calls never mark or persist an excluded owner. Their previous native
        records remain intact; the caller must expose excluded valuation as unavailable.
        An empty scope selects nobody, while None preserves the all-owner behavior.
        """
        if self._planned_resume_pending:
            raise RuntimeError("Planned position resume pending; reports unavailable until ownership is established")
        scope = {} if controller_ids is None else {"controller_ids": controller_ids}
        # Update any pending position holds from done executors
        self._update_positions_from_done_executors(**scope)
        self._reconcile_positions_from_trade_fills(**scope)

        # Generate all reports
        executors_report = self.get_executors_report(**scope)
        positions_report = self.get_positions_report(**scope)

        # Get all controller IDs
        all_controller_ids = set(list(self.active_executors.keys()) +
                                 list(self.positions_held.keys()) +
                                 list(self.cached_performance.keys()))

        if controller_ids is not None:
            all_controller_ids &= controller_ids

        # Use dict comprehension to compile reports for each controller
        return {
            controller_id: {
                "executors": executors_report.get(controller_id, []),
                "positions": positions_report.get(controller_id, []),
                "performance": self.generate_performance_report(controller_id)
            }
            for controller_id in all_controller_ids
        }

    def generate_performance_report(self, controller_id: str) -> PerformanceReport:
        # Create a new report starting from cached base values
        report = PerformanceReport()
        cached_report = self.cached_performance.get(controller_id, PerformanceReport())

        # Start with cached values (from DB)
        report.realized_pnl_quote = cached_report.realized_pnl_quote
        report.volume_traded = cached_report.volume_traded
        report.close_type_counts = cached_report.close_type_counts.copy() if cached_report.close_type_counts else {}

        # Add data from active executors
        active_executors = self.active_executors.get(controller_id, [])
        positions = self.positions_held.get(controller_id, [])

        for executor in active_executors:
            executor_info = executor.executor_info
            if not executor_info.is_done:
                if not self._is_unknown_wallet_sale(executor_info):
                    report.unrealized_pnl_quote += executor_info.net_pnl_quote
                    report.volume_traded += executor_info.filled_amount_quote
            else:
                # For done executors, only add to realized PnL if they're not already in position holds
                # Position holds will be counted separately to avoid double counting
                if executor_info.close_type != CloseType.POSITION_HOLD and not self._is_unknown_wallet_sale(executor_info):
                    report.realized_pnl_quote += executor_info.net_pnl_quote
                    report.volume_traded += executor_info.filled_amount_quote
                if executor_info.close_type:
                    report.close_type_counts[executor_info.close_type] = report.close_type_counts.get(executor_info.close_type, 0) + 1

        # Add data from positions held and collect position summaries
        positions_summary = []
        for position in positions:
            # Skip if the connector/trading pair is not in the current strategy markets
            if (position.connector_name not in self.strategy.markets or
                    position.trading_pair not in self.strategy.markets.get(position.connector_name, set())):
                self.logger().warning(f"Skipping position in performance report for {position.connector_name}.{position.trading_pair} - "
                                      f"not available in current strategy markets")
                continue
            mid_price = self.strategy.market_data_provider.get_price_by_type(
                position.connector_name, position.trading_pair, PriceType.MidPrice)
            position_summary = position.get_position_summary(mid_price if not mid_price.is_nan() else Decimal("0"))

            # Update report with position data
            # PositionSummary exposes gross components; charge paid fees exactly once.
            report.realized_pnl_quote += position_summary.realized_pnl_quote - position_summary.cum_fees_quote
            report.volume_traded += position_summary.volume_traded_quote
            report.unrealized_pnl_quote += position_summary.unrealized_pnl_quote
            if position_summary.amount > Decimal("0"):
                positions_summary.append(position_summary)

        # Set the positions summary (don't use dynamic attribute)
        report.positions_summary = positions_summary

        # Calculate global PNL values
        report.global_pnl_quote = report.unrealized_pnl_quote + report.realized_pnl_quote
        report.global_pnl_pct = (report.global_pnl_quote / report.volume_traded) * 100 if report.volume_traded != 0 else Decimal(0)

        # Calculate individual PNL percentages
        report.unrealized_pnl_pct = (report.unrealized_pnl_quote / report.volume_traded) * 100 if report.volume_traded != 0 else Decimal(0)
        report.realized_pnl_pct = (report.realized_pnl_quote / report.volume_traded) * 100 if report.volume_traded != 0 else Decimal(0)

        report.wallet_sales = [sale for scope, sales in self._wallet_sale_receipts.items()
                               if scope[0] == controller_id for sale in sales]
        if report.wallet_sales:
            report.pnl_available = False
            report.pnl_unavailable_reason = "unknown_wallet_acquisition_cost"
            for field in ("realized_pnl_quote", "realized_pnl_pct", "unrealized_pnl_quote",
                          "unrealized_pnl_pct", "global_pnl_quote", "global_pnl_pct"):
                setattr(report, field, None)
            # These scopes have no held position; native receipts are the sale volume truth.
            report.volume_traded += sum((sale["proceeds_quote"] for sale in report.wallet_sales), Decimal("0"))
        return report
