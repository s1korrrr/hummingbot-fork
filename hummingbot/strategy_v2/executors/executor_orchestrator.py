import asyncio
import logging
import uuid
from collections import deque
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import PositionAction, PositionMode, PriceType, TradeType
from hummingbot.logger import HummingbotLogger
from hummingbot.model.position import Position
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors.order_accounting import base_fee_amount

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
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
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
                 initial_positions_by_controller: Optional[dict] = None):
        self.strategy = strategy
        self.executors_update_interval = executors_update_interval
        self.executors_max_retries = executors_max_retries
        self.active_executors = {}
        self.stored_executors = {}
        self.positions_held = {}
        self.executors_ids_position_held = deque(maxlen=50)
        self.cached_performance = {}
        self._non_hold_order_ids: Set[Tuple[str, str, str]] = set()
        self.recovery_required: Dict[str, List[str]] = {}
        self.initial_positions_by_controller = initial_positions_by_controller or {}
        self._initialize_cached_performance()

    def _initialize_cached_performance(self):
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
        db_executors = markets_recorder.get_all_executors()
        for executor in db_executors:
            controller_id = executor.controller_id
            if controller_id not in self.strategy.controllers:
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
            if self._is_protected_position_scope(restored_position_scopes, position_scope):
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

    def _update_cached_performance(self, controller_id: str, executor_info: ExecutorInfo):
        """
        Update the cached performance for a specific controller with an executor's information.
        """
        if controller_id not in self.cached_performance:
            self.cached_performance[controller_id] = PerformanceReport()
        report = self.cached_performance[controller_id]
        if executor_info.close_type != CloseType.POSITION_HOLD and executor_info.type != "order_executor":
            self._non_hold_order_ids.update(self._owned_order_keys(executor_info))
        # Only add to realized PnL if not a position hold (consistent with generate_performance_report)
        if executor_info.close_type != CloseType.POSITION_HOLD:
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

        for entry in ledger_entries:
            controller_id = entry.controller_id
            if controller_id in self.initial_positions_by_controller or controller_id not in self.strategy.controllers:
                continue
            if (
                entry.connector_name not in self.strategy.markets
                or entry.trading_pair not in self.strategy.markets.get(entry.connector_name, set())
            ):
                continue

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
        if len(trade_fills) == 0:
            return

        controller_by_market_pair, ambiguous_market_pairs = self._controller_ids_by_market_pair()
        if len(controller_by_market_pair) == 0 and len(ambiguous_market_pairs) == 0:
            return

        for connector_name, trading_pair in ambiguous_market_pairs:
            self.logger().warning(
                f"Skipping TradeFill recovery for {connector_name}.{trading_pair} due to ambiguous controller mapping"
            )

        aggregated_order_records: Dict[Tuple[str, str, str, Optional[TradeType], str], Dict[str, Any]] = {}
        skipped_scopes: Set[Tuple[str, str]] = set()
        for trade_fill in sorted(
            trade_fills,
            key=lambda fill: (
                fill.timestamp,
                fill.order_id,
                fill.exchange_trade_id,
            ),
        ):
            connector_name = trade_fill.market
            trading_pair = trade_fill.symbol
            market_pair = (connector_name, trading_pair)
            controller_id = controller_by_market_pair.get(market_pair)
            if controller_id is None:
                continue
            if controller_id in self.initial_positions_by_controller:
                continue
            if connector_name not in self.strategy.markets or trading_pair not in self.strategy.markets.get(connector_name, set()):
                continue
            if "_perpetual" in connector_name:
                if market_pair not in skipped_scopes:
                    self.logger().warning(
                        f"Skipping TradeFill recovery for {connector_name}.{trading_pair} because perpetual fills "
                        "need explicit position-side attribution"
                    )
                    skipped_scopes.add(market_pair)
                continue

            position_scope = self._position_scope(
                controller_id=controller_id,
                connector_name=connector_name,
                trading_pair=trading_pair,
                side=None,
            )
            if self._is_protected_position_scope(protected_position_scopes, position_scope):
                continue

            trade_type = str(trade_fill.trade_type or "").upper()
            if trade_type not in {TradeType.BUY.name, TradeType.SELL.name}:
                continue

            aggregate_key = (
                controller_id,
                connector_name,
                trading_pair,
                None,
                trade_fill.order_id,
            )
            aggregated = aggregated_order_records.setdefault(
                aggregate_key,
                {
                    "client_order_id": trade_fill.order_id,
                    "order_id": trade_fill.order_id,
                    "exchange_order_id": trade_fill.order_id,
                    "trade_type": trade_type,
                    "order_fills": {},
                    "price_quote_times_base": Decimal("0"),
                    "executed_amount_base": Decimal("0"),
                    "executed_amount_quote": Decimal("0"),
                    "cumulative_fee_paid_quote": Decimal("0"),
                    "amount": Decimal("0"),
                    "last_update_timestamp": int(trade_fill.timestamp),
                },
            )
            executed_amount_base = Decimal(str(trade_fill.amount or 0))
            executed_amount_quote = Decimal(str(trade_fill.price or 0)) * executed_amount_base
            aggregated["price_quote_times_base"] += executed_amount_quote
            aggregated["executed_amount_base"] += executed_amount_base
            aggregated["executed_amount_quote"] += executed_amount_quote
            aggregated["cumulative_fee_paid_quote"] += Decimal(str(trade_fill.trade_fee_in_quote or 0))
            aggregated["order_fills"][str(trade_fill.exchange_trade_id)] = {
                "fill_base_amount": str(executed_amount_base), "fee": trade_fill.trade_fee or {}}
            aggregated["amount"] += executed_amount_base
            aggregated["last_update_timestamp"] = max(aggregated["last_update_timestamp"], int(trade_fill.timestamp))

        restored_controller_ids: Set[str] = set()
        for (controller_id, connector_name, trading_pair, position_side, _), order_record in aggregated_order_records.items():
            if order_record["executed_amount_base"] <= Decimal("0"):
                continue
            order_record["price"] = (
                order_record["price_quote_times_base"] / order_record["executed_amount_base"]
            )
            order_record.pop("price_quote_times_base", None)
            self._apply_order_record_to_positions(
                controller_id=controller_id,
                connector_name=connector_name,
                trading_pair=trading_pair,
                position_side=position_side,
                default_side=TradeType.BUY if order_record["trade_type"] == TradeType.BUY.name else TradeType.SELL,
                order_record=order_record,
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
                    f"Recovered held position from TradeFill | controller={controller_id} "
                    f"pair={position_summary.trading_pair} side={position_summary.side.name} "
                    f"amount={position_summary.amount} breakeven={position_summary.breakeven_price}"
                )

    @staticmethod
    def _trade_fill_order_record(trade_fill: TradeFill) -> Dict[str, Any]:
        executed_amount_base = Decimal(str(trade_fill.amount or 0))
        executed_amount_quote = Decimal(str(trade_fill.price or 0)) * executed_amount_base
        client_order_id = f"{trade_fill.order_id}:{trade_fill.exchange_trade_id}"
        return {
            "client_order_id": client_order_id,
            "order_id": trade_fill.order_id,
            "exchange_order_id": trade_fill.order_id,
            "trade_type": str(trade_fill.trade_type or "").upper(),
            "price": Decimal(str(trade_fill.price or 0)),
            "executed_amount_base": executed_amount_base,
            "executed_amount_quote": executed_amount_quote,
            "cumulative_fee_paid_quote": Decimal(str(trade_fill.trade_fee_in_quote or 0)),
            "amount": executed_amount_base,
            "order_fills": {str(trade_fill.exchange_trade_id): {
                "fill_base_amount": str(executed_amount_base), "fee": trade_fill.trade_fee or {}}},
            "last_update_timestamp": int(trade_fill.timestamp),
        }

    @staticmethod
    def _owned_order_keys(executor_info: ExecutorInfo) -> Set[Tuple[str, str, str]]:
        custom = executor_info.custom_info or {}
        order_ids = custom.get("order_ids") or []
        return {(executor_info.connector_name, executor_info.trading_pair, str(order_id))
                for order_id in order_ids if order_id}

    def _executor_owned_order_keys(self) -> Set[Tuple[str, str, str]]:
        owned = set(self._non_hold_order_ids)
        for executors in self.active_executors.values():
            for executor in executors:
                info = executor.executor_info
                if info.type == "order_executor" or (info.is_done and info.close_type == CloseType.POSITION_HOLD):
                    continue
                keys = self._owned_order_keys(info)
                if info.filled_amount_quote > 0 and not keys:
                    raise RuntimeError(f"Cannot reconcile executor {info.id}: filled execution has no order identities")
                owned.update(keys)
        return owned

    def _build_reconciled_positions_from_trade_fills(
        self,
        trade_fills: List[TradeFill],
    ) -> Tuple[Set[Tuple[str, str, str]], Dict[Tuple[str, str, str], PositionHold]]:
        controller_by_market_pair, ambiguous_market_pairs = self._controller_ids_by_market_pair()
        if len(controller_by_market_pair) == 0 and len(ambiguous_market_pairs) == 0:
            return set(), {}

        owned_orders = self._executor_owned_order_keys()
        positions_by_market_pair: Dict[Tuple[str, str, str], PositionHold] = {}
        seen_market_pairs: Set[Tuple[str, str, str]] = set()
        for trade_fill in sorted(
            trade_fills,
            key=lambda fill: (
                fill.timestamp,
                fill.order_id,
                fill.exchange_trade_id,
            ),
        ):
            connector_name = trade_fill.market
            trading_pair = trade_fill.symbol
            if (connector_name, trading_pair) in ambiguous_market_pairs:
                continue
            controller_id = controller_by_market_pair.get((connector_name, trading_pair))
            if controller_id is None or controller_id in self.initial_positions_by_controller:
                continue
            if connector_name not in self.strategy.markets or trading_pair not in self.strategy.markets.get(connector_name, set()):
                continue
            if "_perpetual" in connector_name:
                continue
            key = (controller_id, connector_name, trading_pair)
            seen_market_pairs.add(key)
            if (connector_name, trading_pair, trade_fill.order_id) in owned_orders:
                continue
            order_record = self._trade_fill_order_record(trade_fill)
            if order_record["trade_type"] not in {TradeType.BUY.name, TradeType.SELL.name}:
                continue
            key = (controller_id, connector_name, trading_pair)
            seen_market_pairs.add(key)
            position = positions_by_market_pair.get(key)
            if position is None:
                default_side = TradeType.BUY if order_record["trade_type"] == TradeType.BUY.name else TradeType.SELL
                position = PositionHold(connector_name, trading_pair, default_side)
                positions_by_market_pair[key] = position
            position.add_order_record(order_record)

        return seen_market_pairs, positions_by_market_pair

    def _reconcile_positions_from_trade_fills(self):
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
            self.logger().warning(f"Failed to reconcile positions from TradeFill: {e}")
            return
        if len(trade_fills) == 0:
            return
        seen_market_pairs, reconciled_positions = self._build_reconciled_positions_from_trade_fills(trade_fills)
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
                   keep_position_by_controller: Optional[Dict[str, bool]] = None):
        """
        Stop the orchestrator task and all active executors.
        """
        keep_position_by_controller = keep_position_by_controller or {}
        # first we stop all active executors
        for controller_id, executors_list in self.active_executors.items():
            keep_position = keep_position_by_controller.get(controller_id, False)
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

    def store_all_positions(self):
        """
        Store or update all positions in the database.
        """
        self._update_positions_from_done_executors()
        markets_recorder = MarketsRecorder.get_instance()
        for controller_id, positions_list in self.positions_held.items():
            if controller_id is None:
                self.logger().warning(f"Skipping {len(positions_list)} position(s) with no controller_id")
                continue
            for position in positions_list:
                # Skip if the connector/trading pair is not in the current strategy markets
                if (position.connector_name not in self.strategy.markets or
                        position.trading_pair not in self.strategy.markets.get(position.connector_name, set())):
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
        self.positions_held.clear()

    def sync_live_positions_to_db(self):
        """
        Mirror the current in-memory held positions into the database without clearing them.
        This keeps SQLite closer to live runtime state while preserving the existing shutdown flow.
        """
        self._update_positions_from_done_executors()
        markets_recorder = MarketsRecorder.get_instance()
        live_position_keys: Set[Tuple[str, str, str, str]] = set()

        for controller_id, positions_list in self.positions_held.items():
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

    def store_all_executors(self):
        for controller_id, executors_list in self.active_executors.items():
            for executor in executors_list:
                # Store the executor in the database
                MarketsRecorder.get_instance().store_or_update_executor(executor)
                self._persist_position_hold_entries_from_executor_info(executor.executor_info)
                self._update_cached_performance(controller_id, executor.executor_info)
                self._record_stored_executor(controller_id, executor.executor_info)
        # Remove the executors from the list
        self.active_executors = {}

    def sync_live_executors_to_db(self):
        """
        Mirror active and recently finished executors into the database without mutating the in-memory sets.
        """
        for executors_list in self.active_executors.values():
            for executor in executors_list:
                MarketsRecorder.get_instance().store_or_update_executor(executor)
                self._persist_position_hold_entries_from_executor_info(executor.executor_info)

    def sync_live_state_to_db(self):
        self.sync_live_positions_to_db()
        self.sync_live_executors_to_db()

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
        controller_id = action.controller_id
        executor_config = action.executor_config
        if controller_id in getattr(self.strategy, "_controller_restart_required", {}):
            raise RuntimeError(f"Controller {controller_id} requires a process restart; new execution is blocked")
        if self.recovery_required.get(controller_id):
            raise RuntimeError(
                f"Controller {controller_id} requires startup reconciliation of executors "
                f"{self.recovery_required[controller_id]}; execution remains blocked"
            )
        if any(executor.config.id == executor_config.id for executor in self.active_executors.get(controller_id, [])):
            raise ValueError(f"Duplicate executor id: {executor_config.id}")

        if getattr(self.strategy, "_is_stop_triggered", False) is True:
            raise RuntimeError("Strategy is stopping; new execution is blocked")
        controller = self.strategy.controllers.get(controller_id)
        if controller is not None:
            if getattr(self.strategy, "_controller_reports_valid", None) is False:
                raise RuntimeError("Current execution reports are unavailable")
            controller.executors_info = self.get_executors_report().get(controller_id, [])
            validator = getattr(controller, "validate_executor_config", None)
            if validator is not None and not validator(executor_config):
                self.logger().warning(f"Rejected executor {executor_config.id}: current controller gates do not permit execution")
                return
        if getattr(executor_config, 'connector_name', None) == 'okx':
            owned = {key[2] for executors in self.active_executors.values() for executor in executors
                     for key in self._owned_order_keys(executor.executor_info)}
            unknown = [oid for oid, order in self.strategy.connectors['okx'].in_flight_orders.items()
                       if order.trading_pair == executor_config.trading_pair and oid not in owned]
            if unknown:
                raise RuntimeError(f'Unowned OKX orders require reconciliation: {unknown}')
        self._validate_okx_wallet_reservation(executor_config)
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
        self.active_executors[controller_id].append(executor)
        executor.start()
        self.logger().debug(f"Created {type(executor).__name__} for controller {controller_id}")

    def _validate_okx_wallet_reservation(self, config):
        if getattr(config, "connector_name", None) != "okx" or config.type not in {"position_executor", "order_executor"}:
            return
        base, quote = config.trading_pair.split("-")
        asset = quote if config.side == TradeType.BUY else base
        connector = self.strategy.connectors[config.connector_name]
        available = Decimal(str(connector.get_available_balance(asset)))
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
                pending += max(Decimal("0"), remaining)
        required = Decimal(str(config.amount))
        if config.side == TradeType.BUY:
            price = getattr(config, "entry_price", None) or getattr(config, "price", None)
            if price is None:
                raise RuntimeError("New order has no reservable price")
            required *= Decimal(str(price))
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

    def _update_positions_from_done_executors(self):
        """
        Update positions from executors that are done but haven't been processed yet.
        This is called before generating reports to ensure position state is current.
        """
        for controller_id, executors_list in self.active_executors.items():
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

    def get_executors_report(self) -> Dict[str, List[ExecutorInfo]]:
        """
        Generate a report of all executors.
        """
        report = {}
        controller_ids = set(list(self.active_executors.keys()) + list(self.stored_executors.keys()))
        for controller_id in controller_ids:
            active_executors = [executor.executor_info for executor in self.active_executors.get(controller_id, []) if executor]
            active_executor_ids = {executor.id for executor in active_executors}
            stored_executors = [
                executor
                for executor in self.stored_executors.get(controller_id, [])
                if executor.id not in active_executor_ids
            ]
            report[controller_id] = active_executors + stored_executors
        return report

    def get_positions_report(self) -> Dict[str, List[PositionSummary]]:
        """
        Generate a report of all positions held.
        """
        report = {}
        for controller_id, positions_list in self.positions_held.items():
            positions_summary = []
            for position in positions_list:
                mid_price = self.strategy.market_data_provider.get_price_by_type(
                    position.connector_name, position.trading_pair, PriceType.MidPrice)
                summary = position.get_position_summary(mid_price)
                if summary.amount > Decimal("0"):
                    positions_summary.append(summary)
            report[controller_id] = positions_summary
        return report

    def get_all_reports(self) -> Dict[str, Dict]:
        """
        Generate a unified report containing executors, positions, and performance for all controllers.
        Returns a dictionary with controller_id as key and a dict containing all reports as value.
        """
        # Update any pending position holds from done executors
        self._update_positions_from_done_executors()
        self._reconcile_positions_from_trade_fills()

        # Generate all reports
        executors_report = self.get_executors_report()
        positions_report = self.get_positions_report()

        # Get all controller IDs
        all_controller_ids = set(list(self.active_executors.keys()) +
                                 list(self.positions_held.keys()) +
                                 list(self.cached_performance.keys()))

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
                report.unrealized_pnl_quote += executor_info.net_pnl_quote
                report.volume_traded += executor_info.filled_amount_quote
            else:
                # For done executors, only add to realized PnL if they're not already in position holds
                # Position holds will be counted separately to avoid double counting
                if executor_info.close_type != CloseType.POSITION_HOLD:
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

        return report
