import asyncio
import hashlib
import json
import logging
import os.path
import threading
import time
from contextlib import contextmanager
from decimal import Decimal
from enum import Enum
from shutil import move
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import event as sql_event, func, inspect, update
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Query, Session

from hummingbot import data_path
from hummingbot.client.config.client_config_map import MarketDataCollectionConfigMap
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.utils import TradeFillOrderDetails
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import (
    OKX_FILL_TIME_SOURCE,
    InFlightOrder,
    normalize_execution_timestamp_ms,
)
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    FundingPaymentCompletedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    PositionAction,
    RangePositionLiquidityAddedEvent,
    RangePositionLiquidityRemovedEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.model.chart_snapshot import ChartSnapshot
from hummingbot.model.controller_candle_watermark import (
    CONTROLLER_CANDLE_WATERMARK_SCHEMA_VERSION,
    ControllerCandleWatermark,
    ControllerCandleWatermarkNamespace,
    ControllerCandleWatermarkOutcome,
    ControllerCandleWatermarkResult,
    ControllerCandleWatermarkState,
)
from hummingbot.model.controllers import Controllers
from hummingbot.model.exact_order_values import OrderExact, TradeFillExact, decimal_text, trade_fills_for_config
from hummingbot.model.executors import Executors
from hummingbot.model.funding_payment import FundingPayment
from hummingbot.model.market_data import MarketData
from hummingbot.model.market_state import MarketState
from hummingbot.model.metadata import Metadata
from hummingbot.model.order import Order
from hummingbot.model.order_status import OrderStatus
from hummingbot.model.position import Position
from hummingbot.model.position_hold_ledger import PositionHoldLedger
from hummingbot.model.range_position_update import RangePositionUpdate
from hummingbot.model.rsi_v5_runtime_event import RSIV5RuntimeEvent
from hummingbot.model.sql_connection_manager import SQLConnectionManager
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.controllers.controller_base import ControllerConfigBase
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class MarketsRecorder:
    _logger = None
    _shared_instance: "MarketsRecorder" = None
    market_event_tag_map: Dict[int, MarketEvent] = {
        event_obj.value: event_obj
        for event_obj in MarketEvent.__members__.values()
    }

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @classmethod
    def _execution_timestamp_attestation(cls, evt: OrderFilledEvent) -> Tuple[Optional[int], Optional[str]]:
        timestamp_ms = evt.execution_timestamp_ms
        source = evt.execution_timestamp_source
        if timestamp_ms is None and source is None:
            return None, None
        if type(timestamp_ms) is not int or source != OKX_FILL_TIME_SOURCE:
            cls.logger().error("Invalid fill execution-time attestation for %s/%s; storing it as unknown",
                               evt.order_id, evt.exchange_trade_id)
            return None, None
        normalized = normalize_execution_timestamp_ms(timestamp_ms, evt.timestamp)
        if normalized is None:
            cls.logger().error("Out-of-range or future fill execution time for %s/%s; storing it as unknown",
                               evt.order_id, evt.exchange_trade_id)
            return None, None
        return normalized, source

    @classmethod
    def get_instance(cls, *args, **kwargs) -> "MarketsRecorder":
        if cls._shared_instance is None:
            cls._shared_instance = MarketsRecorder(*args, **kwargs)
        return cls._shared_instance

    @classmethod
    def get_existing_instance(cls) -> Optional["MarketsRecorder"]:
        """Return the initialized recorder without implicitly constructing one."""
        return cls._shared_instance

    def __init__(self,
                 sql: SQLConnectionManager,
                 markets: List[ConnectorBase],
                 config_file_path: str,
                 strategy_name: str,
                 market_data_collection: MarketDataCollectionConfigMap):
        if threading.current_thread() != threading.main_thread():
            raise EnvironmentError("MarketsRecorded can only be initialized from the main thread.")

        self._ev_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._sql_manager: SQLConnectionManager = sql
        self._markets: List[ConnectorBase] = markets
        self._config_file_path: str = config_file_path
        self._strategy_name: str = strategy_name
        self._market_data_collection_config: MarketDataCollectionConfigMap = market_data_collection
        self._market_data_collection_task: Optional[asyncio.Task] = None
        self._executor_reads: Optional[Dict[str, Any]] = None
        self._executor_scope: Optional[Dict[str, Any]] = None
        self._executor_scope_watched = False
        # Internal collection of trade fills in connector will be used for remote/local history reconciliation
        for market in self._markets:
            trade_fills = self.get_trades_for_config(self._config_file_path, 2000)
            market.add_trade_fills_from_market_recorder({TradeFillOrderDetails(tf.market,
                                                                               tf.exchange_trade_id,
                                                                               tf.symbol) for tf in trade_fills})

            exchange_order_ids = self.get_orders_for_config_and_market(self._config_file_path, market, True, 2000)
            market.add_exchange_order_ids_from_market_recorder({o.exchange_order_id: o.id for o in exchange_order_ids})

        self._create_order_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_create_order)
        self._fill_order_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_fill_order)
        self._cancel_order_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_cancel_order)
        self._fail_order_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_fail_order)
        self._complete_order_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_complete_order)
        self._expire_order_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_expire_order)
        self._funding_payment_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_complete_funding_payment)
        self._update_range_position_forwarder: SourceInfoEventForwarder = SourceInfoEventForwarder(self._did_update_range_position)

        self._event_pairs: List[Tuple[MarketEvent, SourceInfoEventForwarder]] = [
            (MarketEvent.BuyOrderCreated, self._create_order_forwarder),
            (MarketEvent.SellOrderCreated, self._create_order_forwarder),
            (MarketEvent.OrderFilled, self._fill_order_forwarder),
            (MarketEvent.OrderCancelled, self._cancel_order_forwarder),
            (MarketEvent.OrderFailure, self._fail_order_forwarder),
            (MarketEvent.BuyOrderCompleted, self._complete_order_forwarder),
            (MarketEvent.SellOrderCompleted, self._complete_order_forwarder),
            (MarketEvent.OrderExpired, self._expire_order_forwarder),
            (MarketEvent.FundingPaymentCompleted, self._funding_payment_forwarder),
            (MarketEvent.RangePositionLiquidityAdded, self._update_range_position_forwarder),
            (MarketEvent.RangePositionLiquidityRemoved, self._update_range_position_forwarder),
        ]
        MarketsRecorder._shared_instance = self
        from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
        for market in self._markets:
            if is_native_paper_connector(market):
                market.bind_terminal_recorder(self)

    def _start_market_data_recording(self):
        self._market_data_collection_task = self._ev_loop.create_task(self._record_market_data())

    async def _record_market_data(self):
        while True:
            try:
                if all(ex.ready for ex in self._markets):
                    with self._sql_manager.get_new_session() as session:
                        with session.begin():
                            for market in self._markets:
                                exchange = market.display_name
                                for trading_pair in market.trading_pairs:
                                    mid_price = market.get_price_by_type(trading_pair, PriceType.MidPrice)
                                    best_bid = market.get_price_by_type(trading_pair, PriceType.BestBid)
                                    best_ask = market.get_price_by_type(trading_pair, PriceType.BestAsk)
                                    order_book = market.get_order_book(trading_pair)
                                    depth = self._market_data_collection_config.market_data_collection_depth + 1
                                    market_data = MarketData(
                                        timestamp=self.db_timestamp,
                                        exchange=exchange,
                                        trading_pair=trading_pair,
                                        mid_price=mid_price,
                                        best_bid=best_bid,
                                        best_ask=best_ask,
                                        order_book={
                                            "bid": list(order_book.bid_entries())[:depth],
                                            "ask": list(order_book.ask_entries())[:depth]}
                                    )
                                    session.add(market_data)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error("Unexpected error while recording market data.", e)
            finally:
                await self._sleep(self._market_data_collection_config.market_data_collection_interval)

    @property
    def sql_manager(self) -> SQLConnectionManager:
        return self._sql_manager

    @property
    def config_file_path(self) -> str:
        return self._config_file_path

    @property
    def strategy_name(self) -> str:
        return self._strategy_name

    @property
    def db_timestamp(self) -> int:
        return int(time.time() * 1e3)

    def _controller_candle_watermark_schema_result(self) -> Optional[ControllerCandleWatermarkResult]:
        """Return a typed failure when a hot-loaded or incompatible store cannot honor v1."""
        expected_columns = [
            ("controller_id", "TEXT", False, 1),
            ("connector_name", "TEXT", False, 2),
            ("trading_pair", "TEXT", False, 3),
            ("interval", "TEXT", False, 4),
            ("candle_open_timestamp_s", "BIGINT", False, 0),
            ("revision", "BIGINT", False, 0),
            ("schema_version", "INTEGER", False, 0),
            ("updated_at_ms", "BIGINT", False, 0),
        ]
        try:
            inspector = inspect(self._sql_manager.engine)
            if ControllerCandleWatermark.__tablename__ not in inspector.get_table_names():
                return ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    reason="watermark table unavailable; process restart required after model registration",
                )
            actual_columns = [
                (column["name"], str(column["type"]), column["nullable"], column["primary_key"])
                for column in inspector.get_columns(ControllerCandleWatermark.__tablename__)
            ]
            primary_key = inspector.get_pk_constraint(ControllerCandleWatermark.__tablename__).get(
                "constrained_columns"
            )
        except (OperationalError, SQLAlchemyError, RuntimeError) as exc:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                reason=f"watermark schema inspection unavailable: {type(exc).__name__}",
            )
        if actual_columns != expected_columns or primary_key != [
            "controller_id",
            "connector_name",
            "trading_pair",
            "interval",
        ]:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                reason="watermark table schema is incompatible with CONTROLLER_CANDLE_WATERMARK_V1",
            )
        return None

    @staticmethod
    def _controller_candle_watermark_filters(namespace: ControllerCandleWatermarkNamespace):
        return (
            ControllerCandleWatermark.controller_id == namespace.controller_id,
            ControllerCandleWatermark.connector_name == namespace.connector_name,
            ControllerCandleWatermark.trading_pair == namespace.trading_pair,
            ControllerCandleWatermark.interval == namespace.interval,
        )

    def load_controller_candle_watermark(
        self, namespace: ControllerCandleWatermarkNamespace
    ) -> ControllerCandleWatermarkResult:
        try:
            if not isinstance(namespace, ControllerCandleWatermarkNamespace):
                raise ValueError("namespace must be a ControllerCandleWatermarkNamespace")
        except ValueError as exc:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                reason=str(exc),
            )
        schema_failure = self._controller_candle_watermark_schema_result()
        if schema_failure is not None:
            return schema_failure

        session = None
        try:
            session = self._sql_manager.get_new_session()
            record = session.query(ControllerCandleWatermark).filter(
                *self._controller_candle_watermark_filters(namespace)
            ).one_or_none()
            if record is None:
                return ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    reason="watermark namespace row is missing; explicit research seed required",
                )
            try:
                state = record.to_state()
            except (TypeError, ValueError) as exc:
                return ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                    reason=f"watermark row is corrupt: {exc}",
                )
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
                state=state,
            )
        except (OperationalError, SQLAlchemyError, RuntimeError) as exc:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                reason=f"watermark load unavailable: {type(exc).__name__}",
            )
        finally:
            if session is not None:
                session.close()

    def seed_controller_candle_watermark(
        self, state: ControllerCandleWatermarkState
    ) -> ControllerCandleWatermarkResult:
        """Explicit research-harness bootstrap; live controllers must never call this method."""
        if not isinstance(state, ControllerCandleWatermarkState):
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                reason="seed state must be a ControllerCandleWatermarkState",
            )
        schema_failure = self._controller_candle_watermark_schema_result()
        if schema_failure is not None:
            return schema_failure

        existing = self.load_controller_candle_watermark(state.namespace)
        if existing.state is not None:
            if existing.state == state:
                return existing
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                state=existing.state,
                reason="watermark namespace is already seeded with different state",
            )
        if (
            existing.outcome is not ControllerCandleWatermarkOutcome.INVALID_STATE
            or existing.reason != "watermark namespace row is missing; explicit research seed required"
        ):
            return existing

        session = None
        try:
            session = self._sql_manager.get_new_session()
        except (OperationalError, SQLAlchemyError, RuntimeError) as exc:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                reason=f"watermark seed session unavailable: {type(exc).__name__}",
            )
        try:
            session.add(ControllerCandleWatermark(
                controller_id=state.namespace.controller_id,
                connector_name=state.namespace.connector_name,
                trading_pair=state.namespace.trading_pair,
                interval=state.namespace.interval,
                candle_open_timestamp_s=state.candle_open_timestamp_s,
                revision=state.revision,
                schema_version=state.schema_version,
                updated_at_ms=state.updated_at_ms,
            ))
            session.flush()
            session.commit()
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.SEEDED,
                state=state,
            )
        except OperationalError as exc:
            session.rollback()
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                reason=f"watermark seed unavailable: {type(exc).__name__}",
            )
        except (SQLAlchemyError, RuntimeError) as exc:
            session.rollback()
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.COMMIT_FAILED,
                reason=f"watermark seed failed and rolled back: {type(exc).__name__}",
            )
        finally:
            if session is not None:
                session.close()

    def compare_and_advance_controller_candle_watermark(
        self,
        prior_state: ControllerCandleWatermarkState,
        *,
        desired_candle_open_timestamp_s: int,
        updated_at_ms: int,
    ) -> ControllerCandleWatermarkResult:
        if not isinstance(prior_state, ControllerCandleWatermarkState):
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                reason="prior state must be a ControllerCandleWatermarkState",
            )
        try:
            desired_state = ControllerCandleWatermarkState(
                namespace=prior_state.namespace,
                candle_open_timestamp_s=desired_candle_open_timestamp_s,
                revision=prior_state.revision + 1,
                schema_version=CONTROLLER_CANDLE_WATERMARK_SCHEMA_VERSION,
                updated_at_ms=updated_at_ms,
            )
        except (TypeError, ValueError) as exc:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.INVALID_STATE,
                state=prior_state,
                reason=f"desired watermark is invalid: {exc}",
            )
        if desired_candle_open_timestamp_s < prior_state.candle_open_timestamp_s:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.REGRESSION,
                state=prior_state,
                reason="desired candle regresses below loaded watermark",
            )
        if desired_candle_open_timestamp_s == prior_state.candle_open_timestamp_s:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
                state=prior_state,
            )
        schema_failure = self._controller_candle_watermark_schema_result()
        if schema_failure is not None:
            return schema_failure

        session = None
        try:
            session = self._sql_manager.get_new_session()
        except (OperationalError, SQLAlchemyError, RuntimeError) as exc:
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                state=prior_state,
                reason=f"watermark compare-and-swap session unavailable: {type(exc).__name__}",
            )
        try:
            statement = update(ControllerCandleWatermark).where(
                *self._controller_candle_watermark_filters(prior_state.namespace),
                ControllerCandleWatermark.candle_open_timestamp_s == prior_state.candle_open_timestamp_s,
                ControllerCandleWatermark.revision == prior_state.revision,
                ControllerCandleWatermark.schema_version == prior_state.schema_version,
            ).values(
                candle_open_timestamp_s=desired_state.candle_open_timestamp_s,
                revision=desired_state.revision,
                schema_version=desired_state.schema_version,
                updated_at_ms=desired_state.updated_at_ms,
            )
            result = session.execute(statement)
            session.flush()
            if result.rowcount == 1:
                try:
                    session.commit()
                except Exception as exc:
                    session.rollback()
                    return ControllerCandleWatermarkResult(
                        outcome=ControllerCandleWatermarkOutcome.COMMIT_FAILED,
                        state=prior_state,
                        reason=f"watermark commit failed and rolled back: {type(exc).__name__}",
                    )
                return ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.ADVANCED_BY_CALLER,
                    state=desired_state,
                )

            session.rollback()
            persisted = self.load_controller_candle_watermark(prior_state.namespace)
            if persisted.state is None:
                return persisted
            if persisted.state.candle_open_timestamp_s == desired_candle_open_timestamp_s:
                return ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.ALREADY_COMMITTED,
                    state=persisted.state,
                )
            if persisted.state.candle_open_timestamp_s > desired_candle_open_timestamp_s:
                return ControllerCandleWatermarkResult(
                    outcome=ControllerCandleWatermarkOutcome.REGRESSION,
                    state=persisted.state,
                    reason="persisted watermark is ahead of desired candle",
                )
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.CAS_CONFLICT,
                state=persisted.state,
                reason="loaded timestamp or revision lost compare-and-swap",
            )
        except OperationalError as exc:
            session.rollback()
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.STORE_UNAVAILABLE,
                state=prior_state,
                reason=f"watermark compare-and-swap unavailable: {type(exc).__name__}",
            )
        except (SQLAlchemyError, RuntimeError) as exc:
            session.rollback()
            return ControllerCandleWatermarkResult(
                outcome=ControllerCandleWatermarkOutcome.COMMIT_FAILED,
                state=prior_state,
                reason=f"watermark compare-and-swap failed and rolled back: {type(exc).__name__}",
            )
        finally:
            if session is not None:
                session.close()

    def start(self):
        for market in self._markets:
            for event_pair in self._event_pairs:
                market.add_listener(event_pair[0], event_pair[1])
        if self._market_data_collection_config.market_data_collection_enabled:
            self._start_market_data_recording()

    def add_market(self, market: ConnectorBase):
        """Add a new market/connector dynamically."""
        if market not in self._markets:
            self._markets.append(market)

            # Add trade fills from recorder
            trade_fills = self.get_trades_for_config(self._config_file_path, 2000)
            market.add_trade_fills_from_market_recorder({TradeFillOrderDetails(tf.market,
                                                                               tf.exchange_trade_id,
                                                                               tf.symbol) for tf in trade_fills
                                                         if tf.market == market.name})

            # Add exchange order IDs
            exchange_order_ids = self.get_orders_for_config_and_market(self._config_file_path, market, True, 2000)
            market.add_exchange_order_ids_from_market_recorder({o.exchange_order_id: o.id for o in exchange_order_ids})

            # Add event listeners
            for event_pair in self._event_pairs:
                market.add_listener(event_pair[0], event_pair[1])

    def remove_market(self, market: ConnectorBase):
        """Remove a market/connector dynamically."""
        if market in self._markets:
            # Remove event listeners
            for event_pair in self._event_pairs:
                market.remove_listener(event_pair[0], event_pair[1])

            # Remove from markets list
            self._markets.remove(market)

    def stop(self):
        for market in self._markets:
            for event_pair in self._event_pairs:
                market.remove_listener(event_pair[0], event_pair[1])
        if self._market_data_collection_task is not None:
            self._market_data_collection_task.cancel()

    def store_or_update_executor(self, executor, executor_info: Optional[ExecutorInfo] = None):
        """`executor_info` may carry the info the caller already built for `executor` in the same pass.

        With cached executor reads, a row this recorder last wrote with identical content is
        not rewritten; any other writer to the Executors table clears that knowledge first.
        """
        info = executor.executor_info if executor_info is None else executor_info
        if info.id != executor.config.id:
            raise ValueError("Executor info belongs to another executor")
        serialized_config = info.model_dump_json()
        reads = self._executor_reads
        if reads is not None and reads["written"].get(info.id) == serialized_config:
            return
        with self._sql_manager.get_new_session() as session:
            existing_executor = session.query(Executors).filter(Executors.id == executor.config.id).one_or_none()
            executor_dict = json.loads(serialized_config)
            if existing_executor:
                # Update existing executor
                for attr, value in executor_dict.items():
                    setattr(existing_executor, attr, value)
            else:
                # Insert new executor
                new_executor = Executors(**executor_dict)
                session.add(new_executor)
            if reads is None:
                session.commit()
                return
            reads["own"] = True
            try:
                session.commit()
            finally:
                reads["own"] = False
            reads["written"][info.id] = serialized_config

    @staticmethod
    def _same_rsi_v5_runtime_fact(row: RSIV5RuntimeEvent, event: Dict[str, Any]) -> bool:
        return bool(
            row.controller_id == event["controller_id"]
            and row.event_id == event["event_id"]
            and row.episode_id == event["episode_id"]
            and row.event_type == event["event_type"]
            and row.payload_json == event["payload_json"]
        )

    def append_rsi_v5_runtime_events(
        self,
        events: List[Dict[str, Any]],
    ) -> List[RSIV5RuntimeEvent]:
        """Append one controller-local batch atomically; existing facts are immutable."""
        if not events:
            return []
        controller_ids = {str(event["controller_id"]) for event in events}
        if len(controller_ids) != 1:
            raise ValueError("one RSI runtime append batch cannot span controllers")
        controller_id = next(iter(controller_ids))
        persisted: List[RSIV5RuntimeEvent] = []
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                next_sequence = (
                    session.query(func.max(RSIV5RuntimeEvent.sequence))
                    .filter(RSIV5RuntimeEvent.controller_id == controller_id)
                    .scalar()
                    or 0
                ) + 1
                for event in events:
                    existing = (
                        session.query(RSIV5RuntimeEvent)
                        .filter(RSIV5RuntimeEvent.event_id == event["event_id"])
                        .one_or_none()
                    )
                    if existing is not None:
                        if not self._same_rsi_v5_runtime_fact(existing, event):
                            raise RuntimeError(
                                f"event_id {event['event_id']!r} already records a different fact"
                            )
                        persisted.append(existing)
                        continue
                    requested_sequence = event.get("sequence")
                    if requested_sequence is not None and int(requested_sequence) != next_sequence:
                        raise RuntimeError(
                            f"sequence {requested_sequence} conflicts with next sequence {next_sequence}"
                        )
                    row = RSIV5RuntimeEvent(
                        controller_id=controller_id,
                        sequence=next_sequence,
                        event_id=str(event["event_id"]),
                        episode_id=str(event["episode_id"]),
                        event_type=str(event["event_type"]),
                        payload_json=str(event["payload_json"]),
                        created_at=float(event["created_at"]),
                    )
                    session.add(row)
                    session.flush()
                    persisted.append(row)
                    next_sequence += 1
                for row in persisted:
                    if row in session:
                        session.expunge(row)
        return persisted

    def get_rsi_v5_runtime_events(
        self,
        controller_id: str,
    ) -> List[RSIV5RuntimeEvent]:
        with self._sql_manager.get_new_session() as session:
            rows = (
                session.query(RSIV5RuntimeEvent)
                .filter(RSIV5RuntimeEvent.controller_id == str(controller_id))
                .order_by(RSIV5RuntimeEvent.sequence.asc())
                .all()
            )
            for row in rows:
                session.expunge(row)
        return rows

    def store_or_update_chart_snapshot(self, snapshot: Dict[str, Any]):
        self.store_or_update_chart_snapshots([snapshot])

    def store_or_update_chart_snapshots(self, snapshots: List[Dict[str, Any]]):
        """Commit a bounded chart batch atomically; omitted fields retain their values."""
        if not snapshots:
            return
        with self._sql_manager.get_new_session() as session:
            for snapshot in snapshots:
                identity = {
                    "controller_id": snapshot["controller_id"],
                    "pair": snapshot["pair"],
                    "interval": snapshot["interval"],
                    "candle_timestamp": snapshot["candle_timestamp"],
                }
                existing_snapshot = session.query(ChartSnapshot).filter_by(**identity).one_or_none()
                if existing_snapshot:
                    for attr, value in snapshot.items():
                        if attr != "id":
                            setattr(existing_snapshot, attr, value)
                else:
                    session.add(ChartSnapshot(**snapshot))
            session.commit()

    def prune_chart_snapshots(self, *, controller_id: str, pair: str, interval: str, keep_after_timestamp: int):
        with self._sql_manager.get_new_session() as session:
            (
                session.query(ChartSnapshot)
                .filter(
                    ChartSnapshot.controller_id == controller_id,
                    ChartSnapshot.pair == pair,
                    ChartSnapshot.interval == interval,
                    ChartSnapshot.candle_timestamp < keep_after_timestamp,
                )
                .delete()
            )
            session.commit()

    def store_position(self, position: Position):
        with self._sql_manager.get_new_session() as session:
            session.add(position)
            session.commit()

    def update_or_store_position(self, position: Position):
        with self._sql_manager.get_new_session() as session:
            # Check if a position already exists for this controller, connector, trading pair, and side
            existing_position = session.query(Position).filter(
                Position.controller_id == position.controller_id,
                Position.connector_name == position.connector_name,
                Position.trading_pair == position.trading_pair,
                Position.side == position.side
            ).first()

            if Decimal(str(position.amount)) <= Decimal("0"):
                if existing_position:
                    session.delete(existing_position)
                session.commit()
                return

            if existing_position:
                # Update the existing position
                existing_position.timestamp = position.timestamp
                existing_position.volume_traded_quote = position.volume_traded_quote
                existing_position.amount = position.amount
                existing_position.breakeven_price = position.breakeven_price
                existing_position.unrealized_pnl_quote = position.unrealized_pnl_quote
                existing_position.realized_pnl_quote = position.realized_pnl_quote
                existing_position.cum_fees_quote = position.cum_fees_quote
            else:
                # Insert new position
                session.add(position)

            session.commit()

    @classmethod
    def _safe_decimal(cls, value: Any) -> Decimal:
        try:
            decimal_value = Decimal(str(value))
        except Exception:
            return Decimal("0")
        return decimal_value if decimal_value.is_finite() else Decimal("0")

    @classmethod
    def _safe_timestamp(cls, value: Any) -> int:
        try:
            return int(float(value))
        except Exception:
            return 0

    @classmethod
    def _sanitize_json_value(cls, value: Any):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, Enum):
            return value.name
        if isinstance(value, dict):
            return {
                key: cls._sanitize_json_value(nested_value)
                for key, nested_value in value.items()
            }
        if isinstance(value, list):
            return [cls._sanitize_json_value(item) for item in value]
        return value

    @staticmethod
    def _position_hold_ledger_entry_id(
        controller_id: str,
        connector_name: str,
        trading_pair: str,
        position_side: Optional[str],
        client_order_id: str,
    ) -> str:
        return "|".join([
            controller_id,
            connector_name,
            trading_pair,
            position_side or "spot",
            client_order_id,
        ])

    def store_or_update_position_hold_ledger_entries(
        self,
        controller_id: str,
        executor_id: str,
        connector_name: str,
        trading_pair: str,
        held_position_orders: List[Dict[str, Any]],
        position_side: Optional[str] = None,
        fallback_timestamp: Optional[int] = None,
    ):
        if not controller_id or not executor_id or len(held_position_orders) == 0:
            return

        fallback_timestamp = fallback_timestamp or self.db_timestamp
        with self._sql_manager.get_new_session() as session:
            for order in held_position_orders:
                client_order_id = str(order.get("client_order_id") or order.get("order_id") or "")
                if not client_order_id:
                    self.logger().warning(
                        f"Skipping held position order without client_order_id | controller={controller_id} "
                        f"executor={executor_id} pair={trading_pair}"
                    )
                    continue

                executed_amount_base = self._safe_decimal(order.get("executed_amount_base"))
                if executed_amount_base <= Decimal("0"):
                    continue
                order_side = str(order.get("trade_type") or "").upper()
                if order_side == "":
                    self.logger().warning(
                        f"Skipping held position order without trade_type | controller={controller_id} "
                        f"executor={executor_id} client_order_id={client_order_id}"
                    )
                    continue

                entry = PositionHoldLedger(
                    id=self._position_hold_ledger_entry_id(
                        controller_id=controller_id,
                        connector_name=connector_name,
                        trading_pair=trading_pair,
                        position_side=position_side,
                        client_order_id=client_order_id,
                    ),
                    controller_id=controller_id,
                    executor_id=executor_id,
                    connector_name=connector_name,
                    trading_pair=trading_pair,
                    position_side=position_side,
                    order_side=order_side,
                    timestamp=self._safe_timestamp(
                        order.get("last_update_timestamp")
                        or order.get("creation_timestamp")
                        or fallback_timestamp
                    ),
                    client_order_id=client_order_id,
                    exchange_order_id=order.get("exchange_order_id"),
                    price=self._safe_decimal(order.get("price")),
                    amount=self._safe_decimal(order.get("amount")),
                    executed_amount_base=executed_amount_base,
                    executed_amount_quote=self._safe_decimal(order.get("executed_amount_quote")),
                    cumulative_fee_paid_quote=self._safe_decimal(order.get("cumulative_fee_paid_quote")),
                    order_payload=self._sanitize_json_value(order),
                )
                session.merge(entry)
            session.commit()

    def get_position_hold_ledger_entries_by_controller(
        self, controller_id: Optional[str] = None
    ) -> List[PositionHoldLedger]:
        with self._sql_manager.get_new_session() as session:
            query = session.query(PositionHoldLedger).order_by(
                PositionHoldLedger.timestamp.asc(),
                PositionHoldLedger.client_order_id.asc(),
            )
            if controller_id is not None:
                query = query.filter(PositionHoldLedger.controller_id == controller_id)
            return query.all()

    def get_all_position_hold_ledger_entries(self) -> List[PositionHoldLedger]:
        return self.get_position_hold_ledger_entries_by_controller(controller_id=None)

    def store_controller_config(self, controller_config: ControllerConfigBase):
        with self._sql_manager.get_new_session() as session:
            config = json.loads(controller_config.json())
            base_columns = ["id", "timestamp", "type"]
            controller = Controllers(id=config["id"],
                                     timestamp=time.time(),
                                     type=config["controller_type"],
                                     config={k: v for k, v in config.items() if k not in base_columns})
            session.add(controller)
            session.commit()

    def get_executors_by_ids(self, executor_ids: List[str]):
        with self._sql_manager.get_new_session() as session:
            executors = session.query(Executors).filter(Executors.id.in_(executor_ids)).all()
            return executors

    def get_executors_by_controller(self, controller_id: str = None) -> List[ExecutorInfo]:
        if self._executor_reads is not None or self._executor_scope is not None:
            # Same rows, in the same table order, as the filtered query.
            return [info for info in self._all_executor_infos() if info.controller_id == controller_id]
        with self._sql_manager.get_new_session() as session:
            executors = session.query(Executors).filter(Executors.controller_id == controller_id).all()
            return [executor.to_executor_info() for executor in executors]

    def cache_executor_reads(self):
        """Serve `get_all_executors` from memory until the Executors table is written.

        Only for a database with one owning process (the native paper session): any
        statement through this engine that names Executors and is not a read clears
        the cache, and so does the commit or rollback that ends that transaction.
        Writes from another process are not observed, so a live owner must not opt in.
        """
        if self._executor_reads is not None:
            return
        reads = self._executor_reads = {"rows": None, "pending": False, "own": False, "written": {}}

        def statement(conn, cursor, text, parameters, context, executemany):
            head = text.lstrip()[:8].upper()
            if not head.startswith(("SELECT", "PRAGMA")) and "EXECUTORS" in text.upper():
                reads["rows"], reads["pending"] = None, True
                if not reads["own"]:
                    reads["written"].clear()

        def boundary(conn):
            if reads["pending"]:
                reads["rows"], reads["pending"] = None, False

        engine = self._sql_manager.engine
        sql_event.listen(engine, "before_cursor_execute", statement)
        sql_event.listen(engine, "commit", boundary)
        sql_event.listen(engine, "rollback", boundary)

    @contextmanager
    def shared_executor_reads(self):
        """Serve repeated Executors reads inside one synchronous decision from one query.

        Safe for a live owner: sharing ends with the scope, so a write by any other process
        is seen by the next scope, and any statement through this engine that writes the
        Executors table clears the shared rows at once (as `cache_executor_reads` does).
        The caller must not await inside the scope. Nested scopes share the outermost one;
        a paper session's process-lifetime cache already serves these reads.
        """
        if self._executor_scope is not None or self._executor_reads is not None:
            yield
            return
        self._watch_executor_scope_writes()
        self._executor_scope = {"rows": None, "pending": False}
        try:
            yield
        finally:
            self._executor_scope = None

    def _watch_executor_scope_writes(self):
        if self._executor_scope_watched:
            return

        def statement(conn, cursor, text, parameters, context, executemany):
            scope = self._executor_scope
            head = text.lstrip()[:8].upper()
            if scope is not None and not head.startswith(("SELECT", "PRAGMA")) and "EXECUTORS" in text.upper():
                scope["rows"], scope["pending"] = None, True

        def boundary(conn):
            scope = self._executor_scope
            if scope is not None and scope["pending"]:
                scope["rows"], scope["pending"] = None, False

        engine = self._sql_manager.engine
        sql_event.listen(engine, "before_cursor_execute", statement)
        sql_event.listen(engine, "commit", boundary)
        sql_event.listen(engine, "rollback", boundary)
        self._executor_scope_watched = True

    def get_all_executors(self) -> List[ExecutorInfo]:
        return self._all_executor_infos()

    def _all_executor_infos(self) -> List[ExecutorInfo]:
        reads = self._executor_reads if self._executor_reads is not None else self._executor_scope
        if reads is not None and reads["rows"] is not None:
            return [info.model_copy() for info in reads["rows"]]
        with self._sql_manager.get_new_session() as session:
            executors = session.query(Executors).all()
            infos = [executor.to_executor_info() for executor in executors]
        if reads is None or reads["pending"]:
            return infos
        reads["rows"] = infos
        return [info.model_copy() for info in infos]

    def get_positions_by_ids(self, position_ids: List[str]) -> List[Position]:
        with self._sql_manager.get_new_session() as session:
            positions = session.query(Position).filter(Position.id.in_(position_ids)).all()
            return positions

    def get_positions_by_controller(self, controller_id: str = None) -> List[Position]:
        with self._sql_manager.get_new_session() as session:
            positions = session.query(Position).filter(Position.controller_id == controller_id).all()
            return positions

    def get_all_positions(self) -> List[Position]:
        with self._sql_manager.get_new_session() as session:
            positions = session.query(Position).all()
            return positions

    def get_orders_for_config_and_market(self, config_file_path: str, market: ConnectorBase,
                                         with_exchange_order_id_present: Optional[bool] = False,
                                         number_of_rows: Optional[int] = None) -> List[Order]:
        with self._sql_manager.get_new_session() as session:
            filters = [Order.config_file_path == config_file_path,
                       Order.market == market.display_name]
            if with_exchange_order_id_present:
                filters.append(Order.exchange_order_id.isnot(None))
            query: Query = (session
                            .query(Order)
                            .filter(*filters)
                            .order_by(Order.creation_timestamp))
            if number_of_rows is None:
                return query.all()
            else:
                return query.limit(number_of_rows).all()

    def get_trades_for_config(self, config_file_path: str, number_of_rows: Optional[int] = None) -> List[TradeFill]:
        with self._sql_manager.get_new_session() as session:
            return trade_fills_for_config(session, config_file_path, number_of_rows)

    def load_strategy_risk_state(self) -> Optional[dict]:
        with self._sql_manager.get_new_session() as session:
            row = session.get(Metadata, f"strategy-risk:{self._config_file_path}")
            return json.loads(row.value) if row is not None else None

    def _operator_command_key(self, controller_id: str, command_id: str) -> str:
        if (not isinstance(controller_id, str) or not 1 <= len(controller_id) <= 180
                or not isinstance(command_id, str) or not 1 <= len(command_id) <= 128):
            raise ValueError('Invalid native operator receipt identity')
        identity = json.dumps([self._config_file_path, controller_id, command_id], separators=(',', ':'))
        return 'strategy-operator:' + hashlib.sha256(identity.encode()).hexdigest()

    def has_operator_command(self, controller_id: str, command_id: str) -> bool:
        with self._sql_manager.get_new_session() as session:
            return session.get(Metadata, self._operator_command_key(controller_id, command_id)) is not None

    def store_strategy_risk_state(self, state: dict, *, operator_receipts=()):
        payload = json.dumps(state, allow_nan=False, sort_keys=True)
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                for receipt in operator_receipts:
                    if (not isinstance(receipt, dict) or set(receipt) != {'controller_id', 'command_id', 'requested_at'}
                            or type(receipt['requested_at']) not in (int, float)
                            or not Decimal(str(receipt['requested_at'])).is_finite() or receipt['requested_at'] < 0):
                        raise ValueError('Invalid native operator receipt')
                    receipt_key = self._operator_command_key(receipt['controller_id'], receipt['command_id'])
                    if session.get(Metadata, receipt_key) is not None:
                        raise ValueError('Native operator command already applied')
                    session.add(Metadata(key=receipt_key, value=json.dumps(receipt, allow_nan=False, sort_keys=True)))
                key = f"strategy-risk:{self._config_file_path}"
                row = session.get(Metadata, key)
                if row is None:
                    session.add(Metadata(key=key, value=payload))
                else:
                    row.value = payload

    def save_market_states(self, config_file_path: str, market: ConnectorBase, session: Session):
        market_states: Optional[MarketState] = self.get_market_states(config_file_path, market, session=session)
        timestamp: int = self.db_timestamp

        if market_states is not None:
            market_states.saved_state = market.tracking_states
            market_states.timestamp = timestamp
        else:
            market_states = MarketState(config_file_path=config_file_path,
                                        market=market.display_name,
                                        timestamp=timestamp,
                                        saved_state=market.tracking_states)
            session.add(market_states)

    def restore_market_states(self, config_file_path: str, market: ConnectorBase):
        with self._sql_manager.get_new_session() as session:
            market_states: Optional[MarketState] = self.get_market_states(config_file_path, market, session=session)

            if market_states is not None:
                market.restore_tracking_states(market_states.saved_state)

    def get_market_states(self,
                          config_file_path: str,
                          market: ConnectorBase,
                          session: Session) -> Optional[MarketState]:
        query: Query = (session
                        .query(MarketState)
                        .filter(MarketState.config_file_path == config_file_path,
                                MarketState.market == market.display_name))
        market_states: Optional[MarketState] = query.one_or_none()
        return market_states

    def _did_create_order(self,
                          event_tag: int,
                          market: ConnectorBase,
                          evt: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._did_create_order, event_tag, market, evt)
            return

        base_asset, quote_asset = evt.trading_pair.split("-")
        timestamp = int(evt.creation_timestamp * 1e3)
        event_type: MarketEvent = self.market_event_tag_map[event_tag]

        with self._sql_manager.get_new_session() as session:
            with session.begin():
                existing_order = session.get(Order, evt.order_id)
                if existing_order is not None:
                    exact_order = session.get(OrderExact, evt.order_id)
                    if (existing_order.market != market.display_name or existing_order.symbol != evt.trading_pair
                            or (exact_order is not None and (
                                Decimal(exact_order.amount) != evt.amount
                                or Decimal(exact_order.price) != (evt.price if evt.price == evt.price else 0)))):
                        raise ValueError(f"Conflicting order creation event: {evt.order_id}")
                    return  # Replayed creation event; preserve the original receipt and latest status.
                session.add(OrderExact(order_id=evt.order_id, amount=decimal_text(evt.amount),
                                       price=decimal_text(evt.price if evt.price == evt.price else 0)))
                order_record: Order = Order(id=evt.order_id,
                                            config_file_path=self._config_file_path,
                                            strategy=self._strategy_name,
                                            market=market.display_name,
                                            symbol=evt.trading_pair,
                                            base_asset=base_asset,
                                            quote_asset=quote_asset,
                                            creation_timestamp=timestamp,
                                            order_type=evt.type.name,
                                            amount=Decimal(evt.amount),
                                            leverage=evt.leverage if evt.leverage else 1,
                                            price=Decimal(evt.price) if evt.price == evt.price else Decimal(0),
                                            position=evt.position if evt.position else PositionAction.NIL.value,
                                            last_status=event_type.name,
                                            last_update_timestamp=timestamp,
                                            exchange_order_id=evt.exchange_order_id)
                order_status: OrderStatus = OrderStatus(order=order_record,
                                                        timestamp=timestamp,
                                                        status=event_type.name)
                session.add(order_record)
                session.add(order_status)
                market.add_exchange_order_ids_from_market_recorder({evt.exchange_order_id: evt.order_id})
                self.save_market_states(self._config_file_path, market, session=session)

    def _did_fill_order(self,
                        event_tag: int,
                        market: ConnectorBase,
                        evt: OrderFilledEvent):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._did_fill_order, event_tag, market, evt)
            return

        base_asset, quote_asset = evt.trading_pair.split("-")
        timestamp: int = int(evt.timestamp * 1e3) if evt.timestamp is not None else self.db_timestamp
        execution_timestamp_ms, execution_timestamp_source = self._execution_timestamp_attestation(evt)
        event_type: MarketEvent = self.market_event_tag_map[event_tag]
        order_id: str = evt.order_id

        with self._sql_manager.get_new_session() as session:
            with session.begin():
                existing_fill = session.query(TradeFill).filter_by(
                    market=market.display_name, order_id=order_id, exchange_trade_id=evt.exchange_trade_id
                ).one_or_none()
                if existing_fill is not None:
                    exact_fill = session.get(TradeFillExact, (market.display_name, order_id, evt.exchange_trade_id))
                    if exact_fill is not None and (
                            Decimal(exact_fill.amount) != evt.amount or Decimal(exact_fill.price) != evt.price):
                        raise ValueError(f"Conflicting exact duplicate fill receipt for {order_id}/{evt.exchange_trade_id}")
                    amount = Decimal(TradeFill.decimal_text(evt.amount))
                    price = Decimal(TradeFill.decimal_text(evt.price))
                    if existing_fill.exact_amount is None:
                        amount = Decimal(int(amount * 10 ** 6)) / 10 ** 6
                    if existing_fill.exact_price is None:
                        price = Decimal(int(price * 10 ** 6)) / 10 ** 6
                    if (existing_fill.amount_decimal != amount or existing_fill.price_decimal != price
                            or existing_fill.symbol != evt.trading_pair or existing_fill.trade_type != evt.trade_type.name
                            or existing_fill.trade_fee != evt.trade_fee.to_json()):
                        raise ValueError(f"Conflicting duplicate fill receipt for {order_id}/{evt.exchange_trade_id}")
                    existing_execution_time = normalize_execution_timestamp_ms(
                        existing_fill.execution_timestamp_ms,
                        existing_fill.timestamp / 1e3,
                    ) if type(existing_fill.execution_timestamp_ms) is int else None
                    existing_execution_source = existing_fill.execution_timestamp_source
                    if (existing_execution_time is not None
                            and existing_execution_source == OKX_FILL_TIME_SOURCE):
                        if (execution_timestamp_ms is not None
                                and (existing_execution_time != execution_timestamp_ms
                                     or existing_execution_source != execution_timestamp_source)):
                            self.logger().error(
                                "Conflicting duplicate fill execution-time attestation for %s/%s; "
                                "preserving the first attestation",
                                order_id, evt.exchange_trade_id,
                            )
                    elif (existing_fill.execution_timestamp_ms is None
                          and existing_fill.execution_timestamp_source is None
                          and execution_timestamp_ms is not None):
                        # A financially identical receipt can add missing venue evidence, but never overwrite known data.
                        existing_fill.execution_timestamp_ms = execution_timestamp_ms
                        existing_fill.execution_timestamp_source = execution_timestamp_source
                        self.logger().info(
                            "Enriched duplicate fill execution-time attestation for %s/%s",
                            order_id, evt.exchange_trade_id,
                        )
                    return  # Stable venue/order/fill identity: redelivery must not duplicate receipts or status.
                # Try to find the order record, and update it if necessary.
                order_record: Optional[Order] = session.query(Order).filter(Order.id == order_id).one_or_none()
                if order_record is not None:
                    order_record.last_status = event_type.name
                    order_record.last_update_timestamp = timestamp

                # Order status and trade fill record should be added even if the order record is not found, because it's
                # possible for fill event to come in before the order created event for market orders.
                order_status: OrderStatus = OrderStatus(order_id=order_id,
                                                        timestamp=timestamp,
                                                        status=event_type.name)
                exact_fee_in_quote = None
                try:
                    fee_in_quote = evt.trade_fee.fee_amount_in_token(
                        trading_pair=evt.trading_pair,
                        price=evt.price,
                        order_amount=evt.amount,
                        token=quote_asset,
                        exchange=market
                    )
                    exact_fee_in_quote = TradeFill.decimal_text(fee_in_quote)
                except Exception as e:
                    self.logger().error(f"Error calculating fee in quote: {e}; fee remains unavailable.")
                    fee_in_quote = None
                    exact_fee_in_quote = None
                trade_fill_record: TradeFill = TradeFill(
                    config_file_path=self.config_file_path,
                    strategy=self.strategy_name,
                    market=market.display_name,
                    symbol=evt.trading_pair,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    timestamp=timestamp,
                    order_id=order_id,
                    trade_type=evt.trade_type.name,
                    order_type=evt.order_type.name,
                    price=evt.price,
                    amount=evt.amount,
                    leverage=evt.leverage if evt.leverage else 1,
                    trade_fee=evt.trade_fee.to_json(),
                    trade_fee_in_quote=fee_in_quote,
                    exact_amount=TradeFill.decimal_text(evt.amount),
                    exact_price=TradeFill.decimal_text(evt.price),
                    exact_trade_fee_in_quote=exact_fee_in_quote,
                    exact_receipt_source="order_filled_event_v1",
                    execution_timestamp_ms=execution_timestamp_ms,
                    execution_timestamp_source=execution_timestamp_source,
                    exchange_trade_id=evt.exchange_trade_id,
                    position=evt.position if evt.position else PositionAction.NIL.value,
                )
                session.add(order_status)
                session.add(trade_fill_record)
                session.add(TradeFillExact(market=market.display_name, order_id=order_id,
                                           exchange_trade_id=evt.exchange_trade_id,
                                           amount=decimal_text(evt.amount), price=decimal_text(evt.price),
                                           fee_in_quote=exact_fee_in_quote))
                self.save_market_states(self._config_file_path, market, session=session)

                market.add_trade_fills_from_market_recorder({TradeFillOrderDetails(trade_fill_record.market,
                                                                                   trade_fill_record.exchange_trade_id,
                                                                                   trade_fill_record.symbol)})

    def _did_complete_funding_payment(self,
                                      event_tag: int,
                                      market: ConnectorBase,
                                      evt: FundingPaymentCompletedEvent):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._did_complete_funding_payment, event_tag, market, evt)
            return

        timestamp: float = evt.timestamp

        with self._sql_manager.get_new_session() as session:
            with session.begin():
                # Try to find the funding payment has been recorded already.
                payment_record: Optional[FundingPayment] = session.query(FundingPayment).filter(
                    FundingPayment.timestamp == timestamp).one_or_none()
                if payment_record is None:
                    funding_payment_record: FundingPayment = FundingPayment(timestamp=timestamp,
                                                                            config_file_path=self.config_file_path,
                                                                            market=market.display_name,
                                                                            rate=evt.funding_rate,
                                                                            symbol=evt.trading_pair,
                                                                            amount=float(evt.amount))
                    session.add(funding_payment_record)

    @staticmethod
    def _csv_matches_header(file_path: str, header: tuple) -> bool:
        df = pd.read_csv(file_path, header=None)
        return tuple(df.iloc[0].values) == header

    def append_to_csv(self, trade: TradeFill):
        csv_filename = "trades_" + trade.config_file_path[:-4] + ".csv"
        csv_path = os.path.join(data_path(), csv_filename)

        field_names = tuple(trade.attribute_names_for_file_export())
        field_data = tuple(trade.value_for_file_export(attr) for attr in field_names)

        # adding extra field "age"
        # // indicates order is a paper order so 'n/a'. For real orders, calculate age.
        age = pd.Timestamp(int((trade.timestamp * 1e-3) - (trade.order.creation_timestamp * 1e-3)), unit='s').strftime(
            '%H:%M:%S') if (trade.order is not None and "//" not in trade.order_id) else "n/a"
        field_names += ("age",)
        field_data += (age,)

        if (os.path.exists(csv_path) and (not self._csv_matches_header(csv_path, field_names))):
            move(csv_path, csv_path[:-4] + '_old_' + pd.Timestamp.utcnow().strftime("%Y%m%d-%H%M%S") + ".csv")

        if not os.path.exists(csv_path):
            df_header = pd.DataFrame([field_names])
            df_header.to_csv(csv_path, mode='a', header=False, index=False)
        df = pd.DataFrame([field_data])
        df.to_csv(csv_path, mode='a', header=False, index=False)

    def _update_order_status(self,
                             event_tag: int,
                             market: ConnectorBase,
                             evt: Union[OrderCancelledEvent,
                                        MarketOrderFailureEvent,
                                        BuyOrderCompletedEvent,
                                        SellOrderCompletedEvent,
                                        OrderExpiredEvent]):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._update_order_status, event_tag, market, evt)
            return

        timestamp: int = self.db_timestamp
        event_type: MarketEvent = self.market_event_tag_map[event_tag]
        order_id: str = evt.order_id

        with self._sql_manager.get_new_session() as session:
            with session.begin():
                order_record: Optional[Order] = session.query(Order).filter(Order.id == order_id).one_or_none()

                if order_record is not None:
                    order_record.last_status = event_type.name
                    order_record.last_update_timestamp = timestamp
                    order_status: OrderStatus = OrderStatus(order_id=order_id,
                                                            timestamp=timestamp,
                                                            status=event_type.name)
                    session.add(order_status)
                    self.save_market_states(self._config_file_path, market, session=session)

    def _did_cancel_order(self,
                          event_tag: int,
                          market: ConnectorBase,
                          evt: OrderCancelledEvent):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._did_cancel_order, event_tag, market, evt)
            return
        with self._sql_manager.get_new_session() as session:
            missing = session.get(Order, evt.order_id) is None
        if missing:
            # A post-only cancel may arrive before creation, including through
            # PENDING_CANCEL or with a late exchange ID. Recover the receipt
            # from the genuine tracked order; never infer acceptance from the
            # cancellation's client ID alone or emit another connector event.
            tracker = getattr(market, '_order_tracker', None)
            tracked = tracker.fetch_order(evt.order_id) if tracker is not None else None
            if (isinstance(tracked, InFlightOrder) and tracked.is_cancelled
                    and tracked.exchange_order_id and tracked.exchange_order_id == evt.exchange_order_id):
                buy = tracked.trade_type == TradeType.BUY
                created_event = BuyOrderCreatedEvent if buy else SellOrderCreatedEvent
                created_tag = MarketEvent.BuyOrderCreated if buy else MarketEvent.SellOrderCreated
                self._did_create_order(created_tag.value, market, created_event(
                    evt.timestamp, tracked.order_type, tracked.trading_pair, tracked.amount,
                    tracked.price, tracked.client_order_id, tracked.creation_timestamp,
                    exchange_order_id=tracked.exchange_order_id, leverage=tracked.leverage,
                    position=tracked.position.value))
            else:
                self.logger().warning(f'Cannot persist cancellation before creation without a matching '
                                      f'native terminal order receipt: {evt.order_id}')
        self._update_order_status(event_tag, market, evt)

    def _did_fail_order(self,
                        event_tag: int,
                        market: ConnectorBase,
                        evt: MarketOrderFailureEvent):
        self._update_order_status(event_tag, market, evt)

    def _did_complete_order(self,
                            event_tag: int,
                            market: ConnectorBase,
                            evt: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        self._update_order_status(event_tag, market, evt)

    def _did_expire_order(self,
                          event_tag: int,
                          market: ConnectorBase,
                          evt: OrderExpiredEvent):
        self._update_order_status(event_tag, market, evt)

    def _did_update_range_position(self,
                                   event_tag: int,
                                   connector: ConnectorBase,
                                   evt: Union[RangePositionLiquidityAddedEvent, RangePositionLiquidityRemovedEvent]):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._did_update_range_position, event_tag, connector, evt)
            return

        timestamp: int = self.db_timestamp
        event_type: MarketEvent = self.market_event_tag_map[event_tag]

        # Determine order_action based on event type
        order_action = None
        if event_type == MarketEvent.RangePositionLiquidityAdded:
            order_action = "ADD"
        elif event_type == MarketEvent.RangePositionLiquidityRemoved:
            order_action = "REMOVE"

        # Calculate trade_fee_in_quote similar to _did_fill_order
        trading_pair = getattr(evt, 'trading_pair', None)
        mid_price = Decimal(str(getattr(evt, 'mid_price', 0) or 0))
        base_amount = Decimal(str(getattr(evt, 'base_amount', 0) or 0))
        fee_in_quote = Decimal("0")
        if trading_pair:
            _, quote_asset = trading_pair.split("-")
            try:
                fee_in_quote = evt.trade_fee.fee_amount_in_token(
                    trading_pair=trading_pair,
                    price=mid_price,
                    order_amount=base_amount,
                    token=quote_asset,
                    exchange=connector
                )
            except Exception as e:
                self.logger().error(f"Error calculating fee in quote for LP position: {e}, will be stored as 0.")
                fee_in_quote = Decimal("0")

        with self._sql_manager.get_new_session() as session:
            with session.begin():
                rp_update: RangePositionUpdate = RangePositionUpdate(
                    hb_id=evt.order_id,
                    timestamp=timestamp,
                    tx_hash=evt.exchange_order_id,
                    token_id=getattr(evt, 'token_id', 0) or 0,
                    trade_fee=evt.trade_fee.to_json(),
                    trade_fee_in_quote=float(fee_in_quote),
                    # P&L tracking fields
                    config_file_path=self._config_file_path,
                    market=connector.display_name,
                    order_action=order_action,
                    trading_pair=trading_pair,
                    position_address=getattr(evt, 'position_address', None),
                    lower_price=float(getattr(evt, 'lower_price', 0) or 0),
                    upper_price=float(getattr(evt, 'upper_price', 0) or 0),
                    mid_price=float(mid_price),
                    base_amount=float(base_amount),
                    quote_amount=float(getattr(evt, 'quote_amount', 0) or 0),
                    base_fee=float(getattr(evt, 'base_fee', 0) or 0),
                    quote_fee=float(getattr(evt, 'quote_fee', 0) or 0),
                    # Rent tracking: position_rent on ADD, position_rent_refunded on REMOVE
                    position_rent=float(getattr(evt, 'position_rent', 0) or 0),
                    position_rent_refunded=float(getattr(evt, 'position_rent_refunded', 0) or 0),
                )
                session.add(rp_update)
                self.save_market_states(self._config_file_path, connector, session=session)

    @staticmethod
    async def _sleep(delay):
        """
        A wrapper function that facilitates patching the sleep in unit tests without affecting the asyncio module
        """
        await asyncio.sleep(delay)
