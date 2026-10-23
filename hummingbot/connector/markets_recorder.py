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
from sqlalchemy import event as sql_event, func, inspect, tuple_, update
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Query, Session
from sqlalchemy.orm.exc import MultipleResultsFound

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
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLWriteCancelledAfterCommit
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.controllers.controller_base import ControllerConfigBase
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class MarketsRecorder:
    _logger = None
    _shared_instance: "MarketsRecorder" = None
    _native_terminal_order_statuses = frozenset({
        MarketEvent.BuyOrderCompleted.name,
        MarketEvent.SellOrderCompleted.name,
        MarketEvent.OrderCancelled.name,
        'OrderCanceled',  # Legacy connector spelling retained in native databases.
        MarketEvent.OrderExpired.name,
        MarketEvent.OrderFailure.name,
    })
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
        self._async_financial_events = False
        self._native_recorder_sequence = 0
        self._financial_event_queue: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._financial_event_worker: Optional[asyncio.Task] = None
        self._financial_event_pending: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._financial_event_failed: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._financial_event_replay: Dict[Any, Any] = {}
        self._financial_event_reconciliation_needed = False
        self._financial_event_error: Optional[BaseException] = None
        self._financial_event_current_job: Optional[Dict[str, Any]] = None
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

    def enable_async_financial_events(self):
        """Opt a qualified native runtime into acknowledged off-loop writes."""
        prefix = f"native-recorder-sequence:{self._config_file_path}:"
        with self._sql_manager.get_new_session() as session:
            values = session.query(Metadata.key, Metadata.value).all()
        sequences = []
        for key, value in values:
            if not key.startswith(prefix):
                continue
            try:
                sequence = int(value)
            except (TypeError, ValueError) as error:
                raise ValueError("Invalid persisted native recorder sequence") from error
            if sequence < 0:
                raise ValueError("Invalid persisted native recorder sequence")
            sequences.append(sequence)
        self._native_recorder_sequence = max(sequences, default=0)
        self._async_financial_events = True
        for market in self._markets:
            self._restore_order_event_replays(market)

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

    async def stop_async(self):
        """Detach callbacks and durably drain native financial writes before release."""
        self.stop()
        await self.flush_financial_events()
        worker = self._financial_event_worker
        if worker is not None and not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        self._financial_event_worker = None

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

    def store_executor_info_snapshot(self, serialized_info: str):
        """Upsert an immutable ExecutorInfo serialization from the SQL writer lane."""
        executor_dict = json.loads(serialized_info)
        executor_id = executor_dict.get('id')
        if not isinstance(executor_id, str) or not executor_id:
            raise ValueError('Executor snapshot has no stable identity')
        with self._sql_manager.get_new_session() as session:
            existing = session.query(Executors).filter(Executors.id == executor_id).one_or_none()
            if existing is None:
                session.add(Executors(**executor_dict))
            else:
                for attr, value in executor_dict.items():
                    setattr(existing, attr, value)
            session.commit()

    async def store_executor_info_snapshot_async(self, serialized_info: str):
        """Persist a loop-captured executor projection on the bounded SQL lane."""
        if not isinstance(serialized_info, str):
            raise TypeError("executor snapshot must be serialized before SQL submission")
        payload = self.capture_executor_info_payload(serialized_info)
        await self._sql_manager.write_queue.write(
            lambda payload=payload: self._store_executor_info_payload(payload)
        )

    def capture_executor_info_payload(self, serialized_info: str):
        info = json.loads(serialized_info)
        executor_id = info.get('id')
        if not isinstance(executor_id, str) or not executor_id:
            raise ValueError('Executor snapshot has no stable identity')
        sequence = None
        if self._async_financial_events:
            self._native_recorder_sequence += 1
            sequence = self._native_recorder_sequence
        return {'executor_id': executor_id, 'executor_info_json': serialized_info,
                'recorder_sequence': sequence}

    def _store_executor_info_payload(self, payload):
        info = json.loads(payload['executor_info_json'])
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                if not self._claim_recorder_sequence(session, payload, 'executor', payload['executor_id']):
                    return False
                existing = session.query(Executors).filter(Executors.id == payload['executor_id']).one_or_none()
                if existing is None:
                    session.add(Executors(**info))
                else:
                    for attr, value in info.items():
                        setattr(existing, attr, value)
        return True

    async def store_position_hold_ledger_entries_async(self, **payload):
        """Persist loop-captured hold-ledger rows on the bounded SQL lane."""
        frozen = json.loads(json.dumps(payload, allow_nan=False, sort_keys=True))
        await self._sql_manager.write_queue.write(
            lambda frozen=frozen: self.store_or_update_position_hold_ledger_entries(**frozen)
        )

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
        identity_columns = ("controller_id", "pair", "interval", "candle_timestamp")
        identities = [tuple(snapshot[column] for column in identity_columns) for snapshot in snapshots]
        # Coercible identities and duplicate keys need the original per-row
        # lookup/autoflush behavior, including intermediate constraint checks.
        canonical_identities = all(
            all(type(value) is str for value in identity[:3]) and type(identity[3]) is int
            for identity in identities
        ) and len(set(identities)) == len(identities)
        with self._sql_manager.get_new_session() as session:
            existing_by_identity = {}
            if canonical_identities:
                unique_identities = list(dict.fromkeys(identities))
                chart_identity = tuple_(
                    ChartSnapshot.controller_id,
                    ChartSnapshot.pair,
                    ChartSnapshot.interval,
                    ChartSnapshot.candle_timestamp,
                )
                # Four bound parameters per identity; 200 remains below the
                # older SQLite default variable limit of 999 while bounding read work.
                for offset in range(0, len(unique_identities), 200):
                    chunk = unique_identities[offset:offset + 200]
                    for row in session.query(ChartSnapshot).filter(chart_identity.in_(chunk)).all():
                        row_identity = (row.controller_id, row.pair, row.interval, row.candle_timestamp)
                        if row_identity in existing_by_identity:
                            # The previous per-key one_or_none() failed closed
                            # if an old database lacked the unique constraint.
                            raise MultipleResultsFound("Duplicate chart snapshot identity")
                        existing_by_identity[row_identity] = row

            for snapshot, identity_values in zip(snapshots, identities):
                if canonical_identities:
                    existing_snapshot = existing_by_identity.get(identity_values)
                else:
                    identity = dict(zip(identity_columns, identity_values))
                    existing_snapshot = session.query(ChartSnapshot).filter_by(**identity).one_or_none()
                if existing_snapshot:
                    for attr, value in snapshot.items():
                        if attr != "id":
                            setattr(existing_snapshot, attr, value)
                else:
                    new_snapshot = ChartSnapshot(**snapshot)
                    session.add(new_snapshot)
                    if canonical_identities:
                        existing_by_identity[identity_values] = new_snapshot
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

    def store_live_position_snapshot(self, positions, controller_ids, timestamp, recorder_sequences=None):
        """Commit a loop-captured live-position projection as one bounded transaction."""
        controller_ids = tuple(controller_ids)
        position_rows = tuple(dict(row) for row in positions)
        sequences = dict(recorder_sequences or {})
        with self._sql_manager.get_new_session() as session:
            accepted_ids = []
            for controller_id in controller_ids:
                sequence = sequences.get(controller_id)
                if self._claim_recorder_sequence(
                        session, {'recorder_sequence': sequence}, 'position', controller_id):
                    accepted_ids.append(controller_id)
            controller_ids = tuple(accepted_ids)
            position_rows = tuple(row for row in position_rows if row['controller_id'] in controller_ids)
            live_keys = {(row['controller_id'], row['connector_name'], row['trading_pair'], row['side'])
                         for row in position_rows if Decimal(str(row['amount'])) > 0}
            existing = session.query(Position).filter(Position.controller_id.in_(controller_ids)).all() if controller_ids else []
            by_key = {(row.controller_id, row.connector_name, row.trading_pair, row.side): row for row in existing}
            for values in position_rows:
                key = (values['controller_id'], values['connector_name'], values['trading_pair'], values['side'])
                row = by_key.get(key)
                if Decimal(str(values['amount'])) <= 0:
                    if row is not None:
                        session.delete(row)
                    continue
                if row is None:
                    session.add(Position(**values))
                    continue
                for field in ('timestamp', 'volume_traded_quote', 'amount', 'breakeven_price',
                              'unrealized_pnl_quote', 'realized_pnl_quote', 'cum_fees_quote'):
                    setattr(row, field, values[field])
            for key, row in by_key.items():
                if key in live_keys:
                    continue
                row.timestamp = timestamp
                row.volume_traded_quote = Decimal('0')
                row.amount = Decimal('0')
                row.breakeven_price = Decimal('0')
                row.unrealized_pnl_quote = Decimal('0')
                row.realized_pnl_quote = Decimal('0')
                # Preserve cumulative fees, matching the existing stale-position path.
            session.commit()

    async def store_live_position_snapshot_async(self, positions, controller_ids, timestamp, recorder_sequences=None):
        immutable_rows = tuple(dict(row) for row in positions)
        immutable_ids = tuple(controller_ids)
        immutable_sequences = dict(recorder_sequences or {})
        await self._sql_manager.write_queue.write(
            lambda: self.store_live_position_snapshot(
                immutable_rows, immutable_ids, timestamp, immutable_sequences)
        )

    def capture_live_position_snapshot_sequences(self, controller_ids):
        if not self._async_financial_events:
            return {}
        sequences = {}
        for controller_id in sorted(set(controller_ids)):
            if not isinstance(controller_id, str) or not controller_id:
                raise ValueError('Invalid native position snapshot owner')
            self._native_recorder_sequence += 1
            sequences[controller_id] = self._native_recorder_sequence
        return sequences

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
        self._store_controller_config_payloads((controller_config.json(),))

    def _store_controller_config_payloads(self, serialized_configs):
        configs = tuple(json.loads(value) for value in serialized_configs)
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                for config in configs:
                    base_columns = ["id", "timestamp", "type"]
                    controller = Controllers(id=config["id"], timestamp=time.time(), type=config["controller_type"],
                                             config={k: v for k, v in config.items() if k not in base_columns})
                    session.add(controller)

    async def store_controller_configs_async(self, serialized_configs):
        if not isinstance(serialized_configs, tuple) or any(not isinstance(value, str) for value in serialized_configs):
            raise TypeError("controller config snapshots must be serialized before SQL submission")
        await self._sql_manager.write_queue.write(
            lambda serialized_configs=serialized_configs: self._store_controller_config_payloads(serialized_configs)
        )

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

    def get_order_status_history(self, order_id: str) -> List[str]:
        """Return immutable native status receipts for one order in event-time order."""
        with self._sql_manager.get_new_session() as session:
            rows = (session.query(OrderStatus.status)
                    .filter(OrderStatus.order_id == order_id)
                    .order_by(OrderStatus.timestamp, OrderStatus.id)
                    .all())
            return [status for (status,) in rows]

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

    async def has_operator_command_async(self, controller_id: str, command_id: str) -> bool:
        key = self._operator_command_key(controller_id, command_id)
        return await self._sql_manager.write_queue.write(
            lambda key=key: self._has_operator_command_key(key)
        )

    def _has_operator_command_key(self, key: str) -> bool:
        with self._sql_manager.get_new_session() as session:
            return session.get(Metadata, key) is not None

    def store_strategy_risk_state(self, state: dict, *, operator_receipts=()):
        payload = json.dumps(state, allow_nan=False, sort_keys=True)
        serialized_receipts = self._serialize_operator_receipts(operator_receipts)
        self._store_strategy_risk_state_payload(payload, serialized_receipts)

    async def store_strategy_risk_state_async(self, state: dict, *, operator_receipts=()):
        """Persist a risk snapshot off-loop and return only after its transaction commits."""
        payload = json.dumps(state, allow_nan=False, sort_keys=True)
        serialized_receipts = self._serialize_operator_receipts(operator_receipts)
        await self._sql_manager.write_queue.write(
            lambda: self._store_strategy_risk_state_payload(payload, serialized_receipts)
        )

    async def latch_strategy_risk_stop_async(self):
        """Set only the stop bit on an existing row, preserving unreconciled evidence."""
        await self._sql_manager.write_queue.write(self._latch_strategy_risk_stop)

    def _latch_strategy_risk_stop(self):
        key = f"strategy-risk:{self._config_file_path}"
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                row = session.get(Metadata, key)
                if row is None:
                    return False
                state = json.loads(row.value)
                if not isinstance(state, dict) or type(state.get('stopped')) is not bool:
                    raise ValueError('Cannot latch an unreconciled malformed risk state')
                state['stopped'] = True
                row.value = json.dumps(state, allow_nan=False, sort_keys=True)
                return True

    def _serialize_operator_receipts(self, operator_receipts):
        serialized = []
        for receipt in operator_receipts:
            if (not isinstance(receipt, dict) or set(receipt) != {'controller_id', 'command_id', 'requested_at'}
                    or type(receipt['requested_at']) not in (int, float)
                    or not Decimal(str(receipt['requested_at'])).is_finite() or receipt['requested_at'] < 0):
                raise ValueError('Invalid native operator receipt')
            key = self._operator_command_key(receipt['controller_id'], receipt['command_id'])
            serialized.append((key, json.dumps(receipt, allow_nan=False, sort_keys=True)))
        return tuple(serialized)

    def _store_strategy_risk_state_payload(self, payload, serialized_receipts):
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                for receipt_key, receipt_json in serialized_receipts:
                    if session.get(Metadata, receipt_key) is not None:
                        raise ValueError('Native operator command already applied')
                    session.add(Metadata(key=receipt_key, value=receipt_json))
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

    async def store_submitted_order_checkpoint_async(self, config_file_path: str, market: ConnectorBase,
                                                     order_json: dict, client_order_id: str):
        """Commit the immutable pre-submit tracker checkpoint and identity off-loop."""
        market_state = {**self._capture_market_state_payload(market), 'market': market.display_name}
        payload = json.dumps(order_json, sort_keys=True)
        market_state.update({'config_file_path': config_file_path})
        key = 'v2-submitted-order:' + client_order_id
        await self._sql_manager.write_queue.write(
            lambda: self._store_submitted_order_checkpoint(
                config_file_path, market_state, key, payload))

    def _store_submitted_order_checkpoint(self, config_file_path, market_state, receipt_key, order_payload):
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                self._store_market_state_payload(session, market_state)
                if session.get(Metadata, receipt_key) is not None:
                    raise ValueError('Durable native client order ID was already submitted')
                session.add(Metadata(key=receipt_key, value=json.dumps({
                    'owner': config_file_path, 'order': json.loads(order_payload)}, sort_keys=True)))

    def restore_market_states(self, config_file_path: str, market: ConnectorBase):
        with self._sql_manager.get_new_session() as session:
            market_states: Optional[MarketState] = self.get_market_states(config_file_path, market, session=session)

            if market_states is not None:
                market.restore_tracking_states(market_states.saved_state)
                replay_pending = getattr(self, "_restore_order_event_replays", None)
                if callable(replay_pending):
                    replay_pending(market)
                tracker = getattr(market, "_order_tracker", None)
                if tracker is not None and any(
                        order.order_fills or order.recorder_pending_events
                        for order in tracker.all_fillable_orders.values()):
                    self._financial_event_reconciliation_needed = True

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

    def _capture_market_state_payload(self, market):
        sequence = None
        if self._async_financial_events:
            self._native_recorder_sequence += 1
            sequence = self._native_recorder_sequence
        return {
            # Connector tracker state uses NaN for a MARKET order's unknown
            # limit price; the historical SQL JSON column preserves that token.
            'market_state_json': json.dumps(market.tracking_states, sort_keys=True),
            'market_state_timestamp': self.db_timestamp,
            'recorder_sequence': sequence,
        }

    def _claim_recorder_sequence(self, session, payload, scope, identity):
        sequence = payload.get('recorder_sequence')
        if sequence is None:
            return True  # Legacy synchronous callers retain their existing contract.
        if type(sequence) is not int or sequence <= 0:
            raise ValueError('Invalid native recorder capture sequence')
        key = f"native-recorder-sequence:{self._config_file_path}:{scope}:{identity}"
        marker = session.get(Metadata, key)
        if marker is not None:
            previous = int(marker.value)
            if previous < 0:
                raise ValueError('Invalid persisted native recorder sequence')
            if sequence <= previous:
                return False
            marker.value = str(sequence)
        else:
            session.add(Metadata(key=key, value=str(sequence)))
        return True

    def _apply_order_status_payload(self, session, order, payload):
        if self._claim_recorder_sequence(session, payload, 'order', payload['order_id']):
            current_status = order.last_status
            incoming_status = payload['event_status']
            current_terminal = current_status in self._native_terminal_order_statuses
            incoming_terminal = incoming_status in self._native_terminal_order_statuses
            # A captured native terminal event closes the lifecycle. A delayed
            # or replayed fill/create callback cannot reopen it, even when its
            # callback timestamp is equal or later. Conflicting terminal
            # receipts remain in OrderStatus for reconciliation; do not choose
            # a later terminal by capture order and hide that conflict.
            terminal_transition = (current_terminal and incoming_terminal
                                   and incoming_status == current_status
                                   and payload['timestamp'] >= order.last_update_timestamp)
            nonterminal_transition = (not current_terminal and not incoming_terminal
                                      and payload['timestamp'] >= order.last_update_timestamp)
            if terminal_transition or nonterminal_transition or (not current_terminal and incoming_terminal):
                order.last_status = payload['event_status']
                order.last_update_timestamp = payload['timestamp']
        duplicate = (payload.get('recorder_sequence') is not None and session.query(OrderStatus).filter_by(
            order_id=payload['order_id'], timestamp=payload['timestamp'], status=payload['event_status'],
        ).first() is not None)
        if not duplicate:
            session.add(OrderStatus(order_id=payload['order_id'], timestamp=payload['timestamp'],
                                    status=payload['event_status']))

    def _store_market_state_payload(self, session, payload):
        if not self._claim_recorder_sequence(session, payload, 'market', payload['market']):
            return
        saved = json.loads(payload['market_state_json'])
        # A newer captured projection can still contain an earlier event's
        # pending marker. Remove only markers whose canonical SQL receipt is
        # committed in this same transaction; never infer ACK from admission.
        for order_state in saved.values():
            if not isinstance(order_state, dict):
                continue
            pending = order_state.get('recorder_pending_events')
            if not isinstance(pending, dict):
                continue
            for key, event in tuple(pending.items()):
                source = event.get('payload', {}) if isinstance(event, dict) else {}
                order_id = source.get('order_id')
                if event.get('kind') == 'order-create':
                    acknowledged = order_id is not None and session.get(OrderExact, order_id) is not None
                else:
                    acknowledged = (event.get('kind') == 'order-status' and session.query(OrderStatus).filter_by(
                        order_id=order_id, timestamp=source.get('timestamp'), status=source.get('event_status'),
                    ).first() is not None)
                if acknowledged:
                    pending.pop(key)
        state = session.query(MarketState).filter_by(
            config_file_path=payload['config_file_path'], market=payload['market'],
        ).one_or_none()
        if state is None:
            session.add(MarketState(config_file_path=payload['config_file_path'], market=payload['market'],
                                    timestamp=payload['market_state_timestamp'], saved_state=saved))
        else:
            state.timestamp = payload['market_state_timestamp']
            state.saved_state = saved

    def _capture_order_creation(self, event_tag, market, evt):
        base_asset, quote_asset = evt.trading_pair.split('-')
        price = evt.price if evt.price == evt.price else Decimal('0')
        return {
            'order_id': evt.order_id, 'config_file_path': self._config_file_path,
            'strategy': self._strategy_name, 'market': market.display_name, 'symbol': evt.trading_pair,
            'base_asset': base_asset, 'quote_asset': quote_asset, 'timestamp': int(evt.creation_timestamp * 1e3),
            'order_type': evt.type.name, 'amount': decimal_text(evt.amount), 'price': decimal_text(price),
            'leverage': evt.leverage if evt.leverage else 1,
            'position': evt.position if evt.position else PositionAction.NIL.value,
            'event_status': self.market_event_tag_map[event_tag].name, 'exchange_order_id': evt.exchange_order_id,
        }

    def _create_order_from_payload(self, session, payload, *, sequence=True):
        existing = session.get(Order, payload['order_id'])
        if existing is not None:
            exact = session.get(OrderExact, payload['order_id'])
            if (existing.market != payload['market'] or existing.symbol != payload['symbol']
                    or (exact is not None and (Decimal(exact.amount) != Decimal(payload['amount'])
                                               or Decimal(exact.price) != Decimal(payload['price'])))):
                raise ValueError(f"Conflicting order creation event: {payload['order_id']}")
            if exact is None and payload.get('recorder_sequence') is not None:
                # A genuine re-delivered native creation receipt can enrich a
                # legacy six-decimal row only after matching its stored values.
                amount = Decimal(int(Decimal(payload['amount']) * 10**6)) / 10**6
                price = Decimal(int(Decimal(payload['price']) * 10**6)) / 10**6
                if existing.amount != amount or existing.price != price:
                    raise ValueError(f"Conflicting legacy order creation receipt: {payload['order_id']}")
                session.add(OrderExact(order_id=payload['order_id'], amount=payload['amount'], price=payload['price']))
            return False
        session.add(OrderExact(order_id=payload['order_id'], amount=payload['amount'], price=payload['price']))
        order = Order(id=payload['order_id'], config_file_path=payload['config_file_path'], strategy=payload['strategy'],
                      market=payload['market'], symbol=payload['symbol'], base_asset=payload['base_asset'],
                      quote_asset=payload['quote_asset'], creation_timestamp=payload['timestamp'],
                      order_type=payload['order_type'], amount=Decimal(payload['amount']), leverage=payload['leverage'],
                      price=Decimal(payload['price']), position=payload['position'], last_status=payload['event_status'],
                      last_update_timestamp=payload['timestamp'], exchange_order_id=payload['exchange_order_id'])
        session.add(order)
        if sequence:
            self._apply_order_status_payload(session, order, payload)
        else:
            session.add(OrderStatus(order_id=payload['order_id'], timestamp=payload['timestamp'],
                                    status=payload['event_status']))
        return True

    def _persist_order_creation_payload(self, payload):
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                created = self._create_order_from_payload(session, payload)
                if created or payload.get('recorder_sequence') is not None:
                    self._store_market_state_payload(session, payload)
        return {'created': created}

    def _persist_order_status_payload(self, payload):
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                order = session.get(Order, payload['order_id'])
                recovered = False
                if order is None and payload.get('creation_if_missing') is not None:
                    recovered = self._create_order_from_payload(session, payload['creation_if_missing'], sequence=False)
                    session.flush()
                    order = session.get(Order, payload['order_id'])
                if order is not None:
                    self._apply_order_status_payload(session, order, payload)
                if order is not None or payload.get('recorder_sequence') is not None:
                    self._store_market_state_payload(session, payload)
        return {'found': order is not None, 'created': recovered}

    def _enqueue_order_event(self, market, payload, kind):
        event_key = f"{kind}:{payload['event_status']}:{payload['timestamp']}"
        identity = (payload['market'], payload['order_id'], event_key)
        tracker = getattr(market, '_order_tracker', None)
        tracked = tracker.fetch_order(payload['order_id']) if tracker is not None else None
        pending = tracked.recorder_pending_events if isinstance(tracked, InFlightOrder) else None
        source = {'identity': list(identity), 'kind': kind, 'payload': json.loads(json.dumps(payload, allow_nan=False))}
        if pending is not None:
            previous = pending.get(event_key)
            if previous is not None and previous != source:
                error = ValueError('Conflicting pending native order event')
                self._financial_event_error = error
                raise error
            pending.setdefault(event_key, source)
        captured = {**payload, **self._capture_market_state_payload(market)}
        frozen = json.dumps(captured, allow_nan=False, sort_keys=True)
        operation = self._persist_order_creation_payload if kind == 'order-create' else self._persist_order_status_payload

        def after_commit(result):
            if pending is not None:
                pending.pop(event_key, None)
            self.unregister_financial_replay(identity)
            if result['created']:
                creation = payload if kind == 'order-create' else payload['creation_if_missing']
                market.add_exchange_order_ids_from_market_recorder({creation['exchange_order_id']: payload['order_id']})
            if kind == 'order-status' and not result['found'] and payload.get('cancel_event'):
                self.logger().warning('Cannot persist cancellation before creation without a matching '
                                      'native terminal order receipt: %s', payload['order_id'])

        def replay():
            return self.enqueue_financial_write(identity, lambda: operation(json.loads(frozen)), after_commit,
                                                kind=kind, source=source)

        self.register_financial_replay(identity, replay)
        if not replay():
            self._financial_event_reconciliation_needed = True
            self.logger().error('Financial order-event queue full; exact native receipt retained for reconciliation')

    def _restore_order_event_replays(self, market):
        if not self._async_financial_events:
            # Legacy/bootstrap restore retains the native source metadata; only
            # an explicitly enabled runtime may admit asynchronous work.
            return
        tracker = getattr(market, '_order_tracker', None)
        if tracker is None:
            return
        for order in tracker.all_fillable_orders.values():
            for event in tuple(order.recorder_pending_events.values()):
                self._enqueue_order_event(market, event['payload'], event['kind'])

    def _did_create_order(self, event_tag: int, market: ConnectorBase,
                          evt: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._did_create_order, event_tag, market, evt)
            return
        payload = self._capture_order_creation(event_tag, market, evt)
        if self._async_financial_events:
            self._enqueue_order_event(market, payload, 'order-create')
            return
        payload.update(self._capture_market_state_payload(market))
        result = self._persist_order_creation_payload(payload)
        if result['created']:
            market.add_exchange_order_ids_from_market_recorder({evt.exchange_order_id: evt.order_id})

    def _did_fill_order(self,
                        event_tag: int,
                        market: ConnectorBase,
                        evt: OrderFilledEvent):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._did_fill_order, event_tag, market, evt)
            return

        if self._async_financial_events:
            self._enqueue_financial_fill(event_tag, market, evt)
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

    def _capture_financial_fill(self, event_tag: int, market: ConnectorBase, evt: OrderFilledEvent):
        base_asset, quote_asset = evt.trading_pair.split("-")
        execution_timestamp_ms, execution_timestamp_source = self._execution_timestamp_attestation(evt)
        fee_in_quote = None
        try:
            fee_in_quote = evt.trade_fee.fee_amount_in_token(
                trading_pair=evt.trading_pair, price=evt.price, order_amount=evt.amount,
                token=quote_asset, exchange=market,
            )
        except Exception as error:
            self.logger().error("Error calculating fee in quote: %s; fee remains unavailable.", error)
        payload = {
            "config_file_path": self._config_file_path,
            "strategy": self._strategy_name,
            "market": market.display_name,
            "symbol": evt.trading_pair,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "timestamp": int(evt.timestamp * 1e3) if evt.timestamp is not None else self.db_timestamp,
            "order_id": evt.order_id,
            "trade_type": evt.trade_type.name,
            "order_type": evt.order_type.name,
            "price": decimal_text(evt.price),
            "amount": decimal_text(evt.amount),
            "leverage": evt.leverage if evt.leverage else 1,
            "trade_fee_json": json.dumps(evt.trade_fee.to_json(), allow_nan=False, sort_keys=True),
            "trade_fee_in_quote": None if fee_in_quote is None else str(fee_in_quote),
            "exact_trade_fee_in_quote": None if fee_in_quote is None else TradeFill.decimal_text(fee_in_quote),
            "execution_timestamp_ms": execution_timestamp_ms,
            "execution_timestamp_source": execution_timestamp_source,
            "exchange_trade_id": evt.exchange_trade_id,
            "position": evt.position if evt.position else PositionAction.NIL.value,
            "event_status": self.market_event_tag_map[event_tag].name,
            "event_receipt_timestamp": evt.timestamp,
        }
        payload.update(self._capture_market_state_payload(market))
        payload["config_file_path"] = self._config_file_path
        identity = (market.display_name, evt.order_id, evt.exchange_trade_id)
        return identity, {"identity": identity, "market_object": market, "payload": payload}

    def _enqueue_financial_fill(self, event_tag: int, market: ConnectorBase, evt: OrderFilledEvent):
        identity, job = self._capture_financial_fill(event_tag, market, evt)
        payload = job["payload"]
        accepted = self.enqueue_financial_write(
            identity,
            lambda payload=payload: self._persist_financial_fill(payload),
            lambda _result, market=market, payload=payload: market.add_trade_fills_from_market_recorder(
                {TradeFillOrderDetails(payload["market"], payload["exchange_trade_id"], payload["symbol"])}
            ),
            kind="fill",
            source=job,
        )
        if not accepted:
            # The tracker has already recorded this exact TradeUpdate. A later
            # ACK barrier replays it from tracker state before terminal cleanup.
            self._financial_event_reconciliation_needed = True
            self.logger().error("Financial fill queue full; exact tracker receipt requires reconciliation")

    def enqueue_financial_write(self, identity, operation, after_commit=None, *, kind, source=None):
        """Admit one immutable, idempotent DB write and optional owner-loop effect.

        `operation` must use only captured immutable data. `after_commit` runs on
        the owner loop after the DB operation returned (which means commit for
        recorder transactions). A False result means capacity rejected the job;
        callers must retain or reconcile the exact source receipt.
        """
        existing = self._financial_event_pending.get(identity) or self._financial_event_failed.get(identity)
        if existing is not None:
            if self._financial_event_source_signature(existing.get("source")) != self._financial_event_source_signature(source):
                self._financial_event_error = ValueError("Conflicting duplicate native financial receipt")
                self._financial_event_reconciliation_needed = True
                self.logger().error("Conflicting duplicate native financial receipt identity; reconciliation required")
                return False
            return True
        if self._financial_event_worker is None or self._financial_event_worker.done():
            self._financial_event_worker = asyncio.get_running_loop().create_task(self._run_financial_event_writer())
        job = {
            "identity": identity,
            "kind": kind,
            "operation": operation,
            "after_commit": after_commit,
            "source": source,
        }
        try:
            self._financial_event_queue.put_nowait(job)
        except asyncio.QueueFull:
            return False
        self._financial_event_pending[identity] = job
        return True

    @staticmethod
    def _financial_event_source_signature(source):
        """Compare immutable receipt facts while excluding replaceable projections."""
        if not isinstance(source, dict):
            return json.dumps(source, allow_nan=False, sort_keys=True, default=str)
        payload = source.get("payload")
        if not isinstance(payload, dict):
            return json.dumps(source, allow_nan=False, sort_keys=True, default=str)
        projection_fields = {"market_state_json", "market_state_timestamp", "recorder_sequence"}
        canonical = {key: value for key, value in source.items() if key not in {"payload", "market_object"}}
        canonical["payload"] = {key: value for key, value in payload.items() if key not in projection_fields}
        return json.dumps(canonical, allow_nan=False, sort_keys=True, default=str)

    def register_financial_replay(self, identity, replay):
        """Register a source-owned exact DTO replay for capacity rejection recovery."""
        if not callable(replay):
            raise TypeError("financial replay must be callable")
        self._financial_event_replay[identity] = replay

    def unregister_financial_replay(self, identity):
        self._financial_event_replay.pop(identity, None)

    def financial_persistence_status(self):
        replay_only_count = len(set(self._financial_event_replay) - set(self._financial_event_pending)
                                - set(self._financial_event_failed))
        pending = bool(self._financial_event_pending or self._financial_event_replay
                       or not self._financial_event_queue.empty()
                       or self._financial_event_current_job is not None
                       or self._financial_event_reconciliation_needed)
        failed = bool(self._financial_event_failed or self._financial_event_error is not None)
        return {
            "mode": "acknowledged_async" if self._async_financial_events else "legacy_sync",
            "pending_count": len(self._financial_event_pending) + replay_only_count,
            "failed_count": len(self._financial_event_failed),
            "reconciliation_needed": self._financial_event_reconciliation_needed,
            "acknowledged": (not pending and not failed) if self._async_financial_events else None,
            "error_type": type(self._financial_event_error).__name__ if self._financial_event_error else None,
        }

    async def _run_financial_event_writer(self):
        while True:
            job = await self._financial_event_queue.get()
            self._financial_event_current_job = job
            try:
                result = await self._sql_manager.write_queue.write(job["operation"])
                if job["after_commit"] is not None:
                    job["after_commit"](result)
            except SQLWriteCancelledAfterCommit as cancelled:
                try:
                    if job["after_commit"] is not None:
                        job["after_commit"](cancelled.result)
                except Exception as error:
                    identity = job["identity"]
                    self._financial_event_failed[identity] = job
                    self._financial_event_error = error
                    self.logger().error("Committed native financial write requires reconciliation | kind=%s error=%s",
                                        job["kind"], type(error).__name__)
                else:
                    identity = job["identity"]
                    self._financial_event_pending.pop(identity, None)
                    self._financial_event_failed.pop(identity, None)
                    self._financial_event_replay.pop(identity, None)
                    if not self._financial_event_failed:
                        self._financial_event_error = None
                    self.logger().info("Acknowledged committed native financial write after cancellation | kind=%s",
                                       job["kind"])
                raise asyncio.CancelledError
            except asyncio.CancelledError:
                identity = job["identity"]
                self._financial_event_failed[identity] = job
                self._financial_event_error = asyncio.CancelledError()
                self.logger().error("Native financial write worker cancelled before acknowledged completion | kind=%s",
                                    job["kind"])
                raise
            except Exception as error:
                identity = job["identity"]
                self._financial_event_failed[identity] = job
                self._financial_event_error = error
                self.logger().error("Native financial write failed; exact receipt remains pending | kind=%s error=%s",
                                    job["kind"], type(error).__name__)
            else:
                identity = job["identity"]
                self._financial_event_pending.pop(identity, None)
                self._financial_event_replay.pop(identity, None)
                if job.get("retrying"):
                    self.logger().info("Recovered native financial write | kind=%s", job["kind"])
                self._financial_event_failed.pop(identity, None)
                if not self._financial_event_failed:
                    self._financial_event_error = None
            finally:
                self._financial_event_current_job = None
                self._financial_event_queue.task_done()

    def _persist_financial_fill(self, payload: Dict[str, Any]):
        fee_json = json.loads(payload["trade_fee_json"])
        exact_fee = payload["exact_trade_fee_in_quote"]
        timestamp = payload["timestamp"]
        with self._sql_manager.get_new_session() as session:
            with session.begin():
                existing = session.query(TradeFill).filter_by(
                    market=payload["market"], order_id=payload["order_id"],
                    exchange_trade_id=payload["exchange_trade_id"],
                ).one_or_none()
                if existing is not None:
                    exact = session.get(TradeFillExact, (
                        payload["market"], payload["order_id"], payload["exchange_trade_id"]
                    ))
                    if exact is not None and (
                            Decimal(exact.amount) != Decimal(payload["amount"])
                            or Decimal(exact.price) != Decimal(payload["price"])):
                        raise ValueError("Conflicting exact duplicate fill receipt")
                    amount = Decimal(TradeFill.decimal_text(Decimal(payload["amount"])))
                    price = Decimal(TradeFill.decimal_text(Decimal(payload["price"])))
                    if existing.exact_amount is None:
                        amount = Decimal(int(amount * 10 ** 6)) / 10 ** 6
                    if existing.exact_price is None:
                        price = Decimal(int(price * 10 ** 6)) / 10 ** 6
                    if (existing.amount_decimal != amount or existing.price_decimal != price
                            or existing.symbol != payload["symbol"]
                            or existing.trade_type != payload["trade_type"]
                            or existing.trade_fee != fee_json):
                        raise ValueError("Conflicting duplicate fill receipt")
                    existing_time = normalize_execution_timestamp_ms(
                        existing.execution_timestamp_ms, existing.timestamp / 1e3
                    ) if type(existing.execution_timestamp_ms) is int else None
                    if (existing_time is None and existing.execution_timestamp_ms is None
                            and existing.execution_timestamp_source is None
                            and payload["execution_timestamp_ms"] is not None):
                        existing.execution_timestamp_ms = payload["execution_timestamp_ms"]
                        existing.execution_timestamp_source = payload["execution_timestamp_source"]
                    order = session.get(Order, payload["order_id"])
                    if order is not None:
                        self._apply_order_status_payload(session, order, payload)
                    elif payload.get("recorder_sequence") is not None and session.query(OrderStatus).filter_by(
                            order_id=payload["order_id"], timestamp=timestamp,
                            status=payload["event_status"]).first() is None:
                        session.add(OrderStatus(order_id=payload["order_id"], timestamp=timestamp,
                                                status=payload["event_status"]))
                    self._store_market_state_payload(session, payload)
                    return

                order = session.query(Order).filter(Order.id == payload["order_id"]).one_or_none()
                if order is not None:
                    self._apply_order_status_payload(session, order, payload)
                elif payload.get("recorder_sequence") is None or session.query(OrderStatus).filter_by(
                        order_id=payload["order_id"], timestamp=timestamp,
                        status=payload["event_status"]).first() is None:
                    session.add(OrderStatus(order_id=payload["order_id"], timestamp=timestamp,
                                            status=payload["event_status"]))
                session.add(TradeFill(
                    config_file_path=payload["config_file_path"], strategy=payload["strategy"],
                    market=payload["market"], symbol=payload["symbol"],
                    base_asset=payload["base_asset"], quote_asset=payload["quote_asset"],
                    timestamp=timestamp, order_id=payload["order_id"],
                    trade_type=payload["trade_type"], order_type=payload["order_type"],
                    price=Decimal(payload["price"]), amount=Decimal(payload["amount"]),
                    leverage=payload["leverage"], trade_fee=fee_json,
                    trade_fee_in_quote=(None if payload["trade_fee_in_quote"] is None
                                        else Decimal(payload["trade_fee_in_quote"])),
                    exact_amount=payload["amount"], exact_price=payload["price"],
                    exact_trade_fee_in_quote=exact_fee, exact_receipt_source="order_filled_event_v1",
                    execution_timestamp_ms=payload["execution_timestamp_ms"],
                    execution_timestamp_source=payload["execution_timestamp_source"],
                    exchange_trade_id=payload["exchange_trade_id"], position=payload["position"],
                ))
                session.add(TradeFillExact(
                    market=payload["market"], order_id=payload["order_id"],
                    exchange_trade_id=payload["exchange_trade_id"],
                    amount=payload["amount"], price=payload["price"], fee_in_quote=exact_fee,
                ))
                self._store_market_state_payload(session, payload)

    async def flush_financial_events(self):
        """Wait for admitted fills, replay tracker-held receipts, and return only after commit."""
        if not self._async_financial_events:
            return
        needs_writer = (not self._financial_event_queue.empty() or bool(self._financial_event_pending)
                        or bool(self._financial_event_failed) or self._financial_event_reconciliation_needed)
        if needs_writer and (self._financial_event_worker is None or self._financial_event_worker.done()):
            self._financial_event_worker = asyncio.get_running_loop().create_task(self._run_financial_event_writer())
        for identity, job in list(self._financial_event_failed.items()):
            job["retrying"] = True
            await self._financial_event_queue.put(job)
            self._financial_event_failed.pop(identity, None)
            self._financial_event_pending[identity] = job
        queued = {job["identity"] for job in tuple(self._financial_event_queue._queue)}
        active = (self._financial_event_current_job["identity"]
                  if self._financial_event_current_job is not None else None)
        for identity, job in list(self._financial_event_pending.items()):
            if identity not in queued and identity != active and identity not in self._financial_event_failed:
                await self._financial_event_queue.put(job)
                queued.add(identity)
        reconciled_this_flush = set()
        while True:
            for replay in tuple(self._financial_event_replay.values()):
                if not replay():
                    self._financial_event_reconciliation_needed = True
            if self._financial_event_reconciliation_needed:
                self._financial_event_reconciliation_needed = False
                for market in self._markets:
                    tracker = getattr(market, "_order_tracker", None)
                    if tracker is None:
                        continue
                    for order in tracker.all_fillable_orders.values():
                        for fill_update in order.order_fills.values():
                            identity = (market.display_name, order.client_order_id, fill_update.trade_id)
                            if (identity in self._financial_event_pending or identity in self._financial_event_failed
                                    or identity in reconciled_this_flush):
                                continue
                            event_tag = MarketEvent.OrderFilled
                            evt = OrderFilledEvent(
                                timestamp=(fill_update.receipt_timestamp if fill_update.receipt_timestamp is not None
                                           else fill_update.fill_timestamp),
                                order_id=order.client_order_id, trading_pair=order.trading_pair,
                                trade_type=order.trade_type, order_type=order.order_type,
                                price=fill_update.fill_price, amount=fill_update.fill_base_amount, trade_fee=fill_update.fee,
                                exchange_trade_id=fill_update.trade_id, exchange_order_id=fill_update.exchange_order_id,
                                leverage=int(order.leverage), position=order.position.value,
                                execution_timestamp_ms=fill_update.execution_timestamp_ms,
                                execution_timestamp_source=fill_update.execution_timestamp_source,
                            )
                            _, job = self._capture_financial_fill(event_tag.value, market, evt)
                            payload = job["payload"]
                            accepted = self.enqueue_financial_write(
                                job["identity"],
                                lambda payload=payload: self._persist_financial_fill(payload),
                                lambda _result, market=market, payload=payload:
                                    market.add_trade_fills_from_market_recorder({TradeFillOrderDetails(
                                        payload["market"], payload["exchange_trade_id"], payload["symbol"]
                                    )}),
                                kind="fill",
                                source=job,
                            )
                            if not accepted:
                                # Capacity may clear as the current queue drains;
                                # keep the tracker receipt visible for another pass.
                                self._financial_event_reconciliation_needed = True
                            else:
                                reconciled_this_flush.add(identity)
            await self._financial_event_queue.join()
            if self._financial_event_error is not None or not self._financial_event_reconciliation_needed:
                break
        if self._financial_event_error is not None:
            error = self._financial_event_error
            self._financial_event_error = None
            raise RuntimeError("Financial fill persistence has not been durably acknowledged") from error

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

    def _update_order_status(self, event_tag: int, market: ConnectorBase,
                             evt: Union[OrderCancelledEvent, MarketOrderFailureEvent, BuyOrderCompletedEvent,
                                        SellOrderCompletedEvent, OrderExpiredEvent]):
        if threading.current_thread() != threading.main_thread():
            self._ev_loop.call_soon_threadsafe(self._update_order_status, event_tag, market, evt)
            return
        payload = {
            'order_id': evt.order_id, 'config_file_path': self._config_file_path,
            'market': market.display_name, 'timestamp': self.db_timestamp,
            'event_status': self.market_event_tag_map[event_tag].name,
            'cancel_event': isinstance(evt, OrderCancelledEvent), 'creation_if_missing': None,
        }
        if payload['cancel_event']:
            tracker = getattr(market, '_order_tracker', None)
            tracked = tracker.fetch_order(evt.order_id) if tracker is not None else None
            if (isinstance(tracked, InFlightOrder) and tracked.is_cancelled
                    and tracked.exchange_order_id and tracked.exchange_order_id == evt.exchange_order_id):
                buy = tracked.trade_type == TradeType.BUY
                created_event = BuyOrderCreatedEvent if buy else SellOrderCreatedEvent
                created_tag = MarketEvent.BuyOrderCreated if buy else MarketEvent.SellOrderCreated
                payload['creation_if_missing'] = self._capture_order_creation(created_tag.value, market, created_event(
                    evt.timestamp, tracked.order_type, tracked.trading_pair, tracked.amount,
                    tracked.price, tracked.client_order_id, tracked.creation_timestamp,
                    exchange_order_id=tracked.exchange_order_id, leverage=tracked.leverage,
                    position=tracked.position.value))
        if self._async_financial_events:
            self._enqueue_order_event(market, payload, 'order-status')
            return
        payload.update(self._capture_market_state_payload(market))
        result = self._persist_order_status_payload(payload)
        if result['created']:
            creation = payload['creation_if_missing']
            market.add_exchange_order_ids_from_market_recorder({creation['exchange_order_id']: evt.order_id})
        if not result['found'] and payload['cancel_event']:
            self.logger().warning('Cannot persist cancellation before creation without a matching '
                                  'native terminal order receipt: %s', evt.order_id)

    def _did_cancel_order(self, event_tag: int, market: ConnectorBase, evt: OrderCancelledEvent):
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
