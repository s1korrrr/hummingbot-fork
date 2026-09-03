"""Adapter from actual OK RSI market/executor reports to durable episode actions."""

import hashlib
import json
from dataclasses import asdict, replace
from decimal import Decimal

from controllers.directional_trading.ok_rsi_core.context import (
    HISTORY_RECORDS,
    ContextUnavailable,
    completed_market_context,
)
from controllers.directional_trading.ok_rsi_core.episode import (
    EpisodeConfig,
    EpisodeInputError,
    EpisodeState,
    MarketSnapshot,
    OrderUpdate,
    minimum_profit_price,
    transition,
)
from controllers.directional_trading.ok_rsi_core.execution import EPISODE_LEVEL_PREFIX, EpisodeOrderExecutorConfig
from controllers.directional_trading.rsi_v5_runtime_store import RSIV5RuntimeEvent, RSIV5RuntimeStore
from hummingbot.core.data_type.common import PositionAction, PriceType, TradeType
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction

CHECKPOINT = "OK_RSI_EPISODE_CHECKPOINT"
ZERO = Decimal("0")


class ReconciliationPending(EpisodeInputError):
    """Native fill detail is incomplete; keep reservations and recheck reports."""


def guard_disabled_restore(config, store=None):
    """Disabling a flag cannot silently transfer an unresolved episode's exits."""
    if store is None:
        from hummingbot.connector.markets_recorder import MarketsRecorder
        recorder = MarketsRecorder.get_existing_instance()
        if recorder is None:
            return
        store = RSIV5RuntimeStore(recorder)
    events = store.load(config.id)
    episode_events = [event for event in events if event.event_type == CHECKPOINT]
    if not episode_events:
        return
    state = EpisodeState.from_dict(episode_events[-1].payload["state"])
    if state.base > 0 or any(not order.done for order in state.orders.values()):
        raise ValueError("cannot disable an unresolved durable OK RSI episode")


def policy_config(config):
    hard_cap = Decimal(str(config.max_total_position_usd))
    configured_budget = config.episode_budget_quote
    budget = hard_cap if configured_budget is None else min(hard_cap, Decimal(str(configured_budget)))
    return EpisodeConfig(
        controller_id=config.id, connector=config.connector_name, trading_pair=config.trading_pair,
        mode=config.trade_mode, budget_quote=budget, mark_cap_quote=hard_cap,
        fee_rate=config.episode_fee_rate, min_profit=Decimal(str(config.min_profit_pct_for_sell)),
        require_profit=config.trade_mode == "buy_sell" or config.sell_only_if_profitable,
        sequential_scale_ins=config.episode_sequential_scale_ins,
        require_scale_in_signal=config.episode_require_scale_in_signal,
        stabilization_bars=config.episode_scale_in_stabilization_bars,
        hostile_risk_latch=config.episode_hostile_risk_latch,
        recovery_exit_enabled=config.episode_recovery_exit_enabled,
        assignment_id=config.episode_assignment_id, initial_base=config.episode_initial_base,
        initial_cost=config.episode_initial_cost,
    )


class ClosedCandleProvider:
    """Controller-local view; never changes shared provider/feed state."""

    def __init__(self, provider):
        self.provider = provider

    def __getattr__(self, name):
        return getattr(self.provider, name)

    def get_candles_df(self, *args, **kwargs):
        from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
        frame = self.provider.get_candles_df(*args, **kwargs)
        if frame is None or frame.empty:
            return frame
        interval = kwargs.get("interval", args[2] if len(args) > 2 else "1m")
        seconds = CandlesBase.interval_to_seconds[interval]
        return frame.loc[frame.timestamp.astype(float) + seconds <= self.provider.time()].copy()


class EpisodeRuntime:
    def __init__(self, controller, store=None):
        self.controller = controller
        self.config = policy_config(controller.config)
        if store is None:
            from hummingbot.connector.markets_recorder import MarketsRecorder
            recorder = MarketsRecorder.get_existing_instance()
            if recorder is None:
                raise RuntimeError("OK RSI episode mode requires initialized durable MarketsRecorder")
            store = RSIV5RuntimeStore(recorder)
        self.store = store
        self.state = EpisodeState()
        self.sequence = 0
        self.hold_reason = None
        self.reason = "not_evaluated"
        self.last_market = None
        self._last_payload = None
        try:
            events = store.load(self.config.controller_id)
            for sequence, event in enumerate(events, 1):
                if event.sequence != sequence or event.event_type != CHECKPOINT:
                    raise EpisodeInputError("incompatible episode event sequence/type")
                self.state = EpisodeState.from_dict(event.payload["state"])
                if self.state.config_hash != self.config.identity:
                    raise EpisodeInputError("episode configuration changed")
                self.sequence = sequence
                self._last_payload = event.payload
            if self._last_payload is not None and self._last_payload.get("market"):
                self.last_market = MarketSnapshot(**self._last_payload["market"])
        except (ValueError, TypeError, KeyError, RuntimeError) as exc:
            self.hold_reason = f"restore_failed:{exc}"

    def status(self):
        profit_floor = minimum_profit_price(self.config, self.state)
        return {"enabled": True, "phase": self.state.phase, "episode_id": self.state.episode_id,
                "reason": self.reason, "hold_reason": self.hold_reason,
                "base": str(self.state.base), "gross_spent": str(self.state.gross_spent),
                "cost": str(self.state.cost), "cost_known": self.state.cost_known,
                "sell_proceeds": str(self.state.sell_proceeds),
                "minimum_profit_price": str(profit_floor) if profit_floor is not None else None,
                "adverse_entry_latched": self.state.adverse_entry_latched,
                "target_base": str(self.state.target_base), "levels": list(self.state.levels),
                "pending": [key for key, order in self.state.orders.items() if not order.done],
                "config_hash": self.config.identity, "checkpoint_sequence": self.sequence}

    def _updates_and_ownership(self):
        updates = []
        c = self.controller
        for executor in c.executors_info:
            if executor.connector_name != self.config.connector or executor.trading_pair != self.config.trading_pair:
                continue
            if executor.id not in self.state.orders:
                if executor.is_active or not getattr(executor, "is_done", True):
                    raise EpisodeInputError("unassigned_active_executor")
                continue
            receipt = (executor.custom_info or {}).get("episode_receipt")
            if not isinstance(receipt, dict):
                raise EpisodeInputError("native_execution_receipt_unavailable")
            errors = receipt.get("errors", [])
            if receipt.get("fees_known") is False and errors and all(
                isinstance(error, str) and (error == "incomplete_fill_records" or error.startswith("unresolved_native_order:"))
                for error in errors
            ):
                raise ReconciliationPending(",".join(errors))
            updates.append(OrderUpdate(
                request_id=executor.id, filled_base=receipt["gross_base"], filled_quote=receipt["gross_quote"],
                fee_base=receipt["base_fee"], fee_quote=receipt["quote_fee"],
                done=receipt["final"], fees_known=receipt["fees_known"],
            ))
        held = sum((Decimal(str(position.amount)) for position in c.positions_held
                    if position.connector_name == self.config.connector and position.trading_pair == self.config.trading_pair
                    and position.side == TradeType.BUY), ZERO)
        # Native PositionSummary has no order identities. Initial adoption is
        # never inferred. Once tagged orders exist the reducer consumes their
        # receipts only; framework position totals are not added a second time.
        if held > 0 and not self.state.orders:
            raise EpisodeInputError("unassigned_held_inventory")
        return updates

    def _snapshot(self):
        c = self.controller
        provider = c.market_data_provider
        frame = provider.get_candles_df(
            connector_name=c.config.candles_connector, trading_pair=c.config.trading_pair,
            interval="1m", max_records=HISTORY_RECORDS)
        context = completed_market_context(frame, provider.time())
        rule = c._require_trading_rule()
        base, quote = c.config.trading_pair.split("-")
        base_available = c._get_available_asset_balance(base)
        quote_available = c._get_available_asset_balance(quote)
        if base_available is None or quote_available is None:
            raise ContextUnavailable("wallet availability unknown")
        state = (c.processed_data or {}).get("signal_state", {})
        buy_allowed = not (c._operator_entry_paused or c._check_consecutive_loss_pause()
                           or c._is_flash_crash_cooldown(provider.time()))
        buy_allowed = buy_allowed and bool(state.get("condition_ok", False))
        if c.config.strict_hmm_mode and (c.processed_data or {}).get("regime_error"):
            buy_allowed = False
        snapshot = MarketSnapshot(
            **context,
            bid=provider.get_price_by_type(c.config.connector_name, c.config.trading_pair, PriceType.BestBid),
            ask=provider.get_price_by_type(c.config.connector_name, c.config.trading_pair, PriceType.BestAsk),
            quote_available=quote_available, base_available=base_available,
            min_amount=c._rule_decimal(rule, "min_order_size"),
            min_notional=max(c._rule_decimal(rule, "min_notional_size"), c._rule_decimal(rule, "min_order_value")),
            amount_step=c._base_amount_increment(rule), price_step=c._rule_decimal(rule, "min_price_increment"),
            entry_signal=int((c.processed_data or {}).get("signal", 0)) > 0,
            valid=not c.config.manual_kill_switch, buy_allowed=bool(buy_allowed),
        )
        self.last_market = snapshot
        return snapshot

    def _save(self, state, market):
        serialized_market = {key: str(value) if isinstance(value, Decimal) else value for key, value in asdict(market).items()}
        payload = {"state": state.to_dict(), "market": serialized_market}
        if payload == self._last_payload:
            return
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        event = RSIV5RuntimeEvent(
            controller_id=self.config.controller_id,
            event_id=f"ok-rsi:{self.config.controller_id}:{self.sequence + 1}:{digest}",
            episode_id=state.episode_id or f"runtime:{self.config.controller_id}", event_type=CHECKPOINT,
            payload=payload, created_at=market.timestamp, sequence=self.sequence + 1)
        stored = self.store.append_many([event])[0]
        self.sequence = stored.sequence
        self._last_payload = payload

    def determine_actions(self):
        if self.hold_reason:
            return []
        try:
            if policy_config(self.controller.config).identity != self.config.identity:
                raise EpisodeInputError("episode_configuration_changed")
            updates = self._updates_and_ownership()
            try:
                market = self._snapshot()
            except (ContextUnavailable, ArithmeticError, AttributeError, KeyError, RuntimeError, TypeError, ValueError) as exc:
                self.reason = f"context_unavailable:{exc}"
                if self.last_market is None:
                    return []
                market = replace(self.last_market, valid=False)
            result = transition(self.config, self.state, market, updates)
            try:
                self._save(result.state, market)
            except Exception as exc:
                # Storage backends expose different exception hierarchies. No
                # action may leave this boundary after any failed durable write.
                self.hold_reason = f"persistence_failed:{exc}"
                return []
            self.state = result.state
            self.reason = result.reason
            if self.controller.config.manual_kill_switch:
                return []
            actions = [StopExecutorAction(controller_id=self.config.controller_id, executor_id=request_id, keep_position=True)
                       for request_id in result.cancel_ids
                       if any(ex.id == request_id and ex.is_active for ex in self.controller.executors_info)]
            for request in result.requests:
                executor_config = EpisodeOrderExecutorConfig(
                    id=request.id, timestamp=market.timestamp, connector_name=self.config.connector,
                    trading_pair=self.config.trading_pair, side=TradeType[request.side], amount=request.amount,
                    price=request.price, position_action=PositionAction.OPEN if request.side == "BUY" else PositionAction.CLOSE,
                    execution_strategy=ExecutionStrategy.LIMIT_MAKER, leverage=1,
                    level_id=f"{EPISODE_LEVEL_PREFIX}{request.episode_id}:{request.level}",
                    episode_id=request.episode_id, episode_level=request.level,
                    episode_revision=request.revision, episode_expires_at=request.expires_at,
                    episode_intent=request.intent,
                )
                actions.append(CreateExecutorAction(controller_id=self.config.controller_id, executor_config=executor_config))
            return actions
        except ReconciliationPending as exc:
            self.reason = f"reconciliation_pending:{exc}"
            return []
        except (EpisodeInputError, ValueError, TypeError, KeyError, ArithmeticError) as exc:
            self.hold_reason = str(exc)
            return []
