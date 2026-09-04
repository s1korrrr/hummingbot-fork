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
    acquisition_levels,
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
        setup_bars=config.episode_scale_in_setup_bars,
        stabilization_bars=config.episode_scale_in_stabilization_bars,
        rebound_atr=config.episode_rebound_atr,
        level_fractions=tuple(config.episode_level_fractions),
        dip_atr_levels=tuple(config.episode_dip_atr_levels),
        basis_improvement_atr=config.episode_basis_improvement_atr,
        last_fill_gap_atr=config.episode_last_fill_gap_atr,
        distribution_atr_levels=tuple(config.episode_distribution_atr_levels),
        remaining_fractions=tuple(config.episode_remaining_fractions),
        trail_atr=config.episode_trail_atr,
        trail_min=config.episode_trail_min,
        trail_max=config.episode_trail_max,
        hostile_risk_latch=config.episode_hostile_risk_latch,
        recovery_exit_enabled=config.episode_recovery_exit_enabled,
        adaptive_entry_pricing=config.episode_adaptive_entry_pricing,
        adaptive_reserved_entry=config.episode_adaptive_reserved_entry,
        adaptive_reference_fallback=config.episode_adaptive_reference_fallback,
        thin_scout_release=config.episode_thin_scout_release,
        thin_scout_rearm=config.episode_thin_scout_rearm,
        profitable_episode_rearm=config.episode_profitable_rearm,
        scout_rebound_confirmation=config.episode_scout_rebound_confirmation,
        recovery_aware_scout=config.episode_recovery_aware_scout,
        recovery_rebound_scout=config.episode_recovery_rebound_scout,
        structure_held_runner=config.episode_structure_held_runner,
        markup_release_runner=config.episode_markup_release_runner,
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
        runner_target = (
            self.state.distribution_start_base * self.config.remaining_fractions[-1]
            if self.state.distribution_start_base
            else ZERO
        )
        risk_clear = None if self.last_market is None else not (
            self.last_market.cascade or self.last_market.hostile or self.last_market.reset
        )
        buy_orders = [
            order for order in self.state.orders.values() if order.request.side == "BUY"
        ]
        last_entry_progress = max(
            buy_orders,
            key=lambda order: (order.request.expires_at, order.request.revision),
        ) if buy_orders else None
        last_entry = last_entry_progress.request if last_entry_progress else None
        active_setup = (
            (self.config.scout_rebound_confirmation or self.config.recovery_rebound_scout)
            and self.state.base == 0
            and self.state.setup_level == 0
        )
        setup_policy = (
            "RECOVERY_REBOUND_SCOUT_V1"
            if active_setup and self.config.recovery_rebound_scout
            else "SCOUT_REBOUND_V1"
            if active_setup
            else None
        )
        levels = acquisition_levels(self.state)
        thin_scout_inventory = (
            self.config.mode == "buy_sell"
            and self.state.base > ZERO
            and levels == (0,)
        )
        distribution_extension = (
            (self.state.peak - self.state.distribution_basis)
            / self.state.distribution_atr
            if self.state.distribution_atr > ZERO
            else None
        )
        if self.state.exit_intent == "THIN_SCOUT_RELEASE":
            exit_policy = "THIN_SCOUT_RELEASE_V1"
        elif self.config.markup_release_runner:
            exit_policy = "MARKUP_RELEASED_RUNNER_V1"
        elif self.config.structure_held_runner:
            exit_policy = "STRUCTURE_HELD_RUNNER_V1"
        else:
            exit_policy = "DEFAULT"
        return {"enabled": True, "phase": self.state.phase, "episode_id": self.state.episode_id,
                "reason": self.reason, "hold_reason": self.hold_reason,
                "base": str(self.state.base), "gross_spent": str(self.state.gross_spent),
                "cost": str(self.state.cost), "cost_known": self.state.cost_known,
                "sell_proceeds": str(self.state.sell_proceeds),
                "minimum_profit_price": str(profit_floor) if profit_floor is not None else None,
                "adverse_entry_latched": self.state.adverse_entry_latched,
                "last_entry_pricing_policy": last_entry.pricing_policy if last_entry else None,
                "last_entry_reference_price": str(last_entry.reference_price) if last_entry else None,
                "last_entry_selected_price": str(last_entry.price) if last_entry else None,
                "last_entry_discount": str(last_entry.pricing_discount) if last_entry else None,
                "last_entry_discount_atr": str(last_entry.pricing_discount_atr) if last_entry else None,
                "last_entry_confidence": str(last_entry.signal_confidence) if last_entry else None,
                "last_entry_trend_alignment": str(last_entry.trend_alignment) if last_entry else None,
                "last_entry_parent_request_id": last_entry.parent_request_id if last_entry else None,
                "last_entry_expires_at": last_entry.expires_at if last_entry else None,
                "last_entry_reserved_quote": (
                    str(last_entry.reserved_quote) if last_entry else None
                ),
                "last_entry_termination_requested": (
                    last_entry_progress.termination_requested if last_entry_progress else None
                ),
                "last_entry_terminal_cause": (
                    last_entry_progress.terminal_cause if last_entry_progress else None
                ),
                "entry_reservation_allowed": (
                    getattr(self.last_market, "reservation_allowed", None)
                    if self.last_market
                    else None
                ),
                "entry_market_bid": (
                    str(getattr(self.last_market, "bid", None))
                    if getattr(self.last_market, "bid", None) is not None
                    else None
                ),
                "entry_market_price_step": (
                    str(getattr(self.last_market, "price_step", None))
                    if getattr(self.last_market, "price_step", None) is not None
                    else None
                ),
                "entry_market_valid": getattr(self.last_market, "valid", None) if self.last_market else None,
                "entry_market_cascade": getattr(self.last_market, "cascade", None) if self.last_market else None,
                "entry_market_hostile": getattr(self.last_market, "hostile", None) if self.last_market else None,
                "entry_market_recovery": getattr(self.last_market, "recovery", None) if self.last_market else None,
                "entry_market_markup": getattr(self.last_market, "markup", None) if self.last_market else None,
                "entry_market_buy_allowed": getattr(self.last_market, "buy_allowed", None) if self.last_market else None,
                "thin_scout_release_enabled": self.config.thin_scout_release,
                "thin_scout_rearm_enabled": self.config.thin_scout_rearm,
                "profitable_episode_rearm_enabled": self.config.profitable_episode_rearm,
                "thin_scout_inventory": thin_scout_inventory,
                "acquisition_levels": list(levels),
                "distribution_start_base": str(self.state.distribution_start_base),
                "distribution_basis": str(self.state.distribution_basis),
                "distribution_atr": str(self.state.distribution_atr),
                "distribution_extension_atr": (
                    str(distribution_extension)
                    if distribution_extension is not None
                    else None
                ),
                "first_distribution_atr": str(self.config.distribution_atr_levels[0]),
                "setup_policy": setup_policy,
                "setup_started_at": self.state.setup_start if active_setup else None,
                "setup_confirmation_started_at": (
                    (self.state.setup_confirmation_start or None) if active_setup else None
                ),
                "setup_low": str(self.state.setup_low) if active_setup else None,
                "last_entry_setup_policy": last_entry.setup_policy if last_entry else None,
                "last_entry_setup_started_at": last_entry.setup_started_at if last_entry else None,
                "last_entry_setup_confirmation_started_at": (
                    last_entry.setup_confirmation_started_at if last_entry else None
                ),
                "last_entry_setup_low": str(last_entry.setup_low) if last_entry else None,
                "last_entry_setup_rebound": str(last_entry.setup_rebound) if last_entry else None,
                "last_entry_setup_no_new_low": last_entry.setup_no_new_low if last_entry else None,
                "exit_policy": exit_policy,
                "exit_hourly_recovered": self.last_market.hourly_recovered if self.last_market else None,
                "exit_markup": getattr(self.last_market, "markup", None) if self.last_market else None,
                "exit_risk_clear": risk_clear,
                "structure_runner_active": self.state.structure_runner_active,
                "structure_runner_target_base": str(runner_target),
                "last_exit_trail_breach": self.state.last_exit_trail_breach,
                "last_exit_hourly_recovered": self.state.last_exit_hourly_recovered,
                "last_exit_risk_clear": self.state.last_exit_risk_clear,
                "exit_target_revision": self.state.exit_target_revision,
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
                terminal_cause=receipt.get("terminal_cause", "UNRECORDED"),
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
        entry_confidence = Decimal("1")
        trend_alignment = Decimal("0")
        if self.config.mode == "buy_sell" and self.config.adaptive_entry_pricing:
            if "signal_strength_score" not in state:
                raise ContextUnavailable("adaptive entry signal confidence unavailable")
            trend = (c.processed_data or {}).get("trend_confirmation")
            if not isinstance(trend, dict) or "alignment_score" not in trend:
                raise ContextUnavailable("adaptive entry trend alignment unavailable")
            entry_confidence = state["signal_strength_score"]
            trend_alignment = trend["alignment_score"]
        reservation_allowed = not (
            c._operator_entry_paused
            or c._check_consecutive_loss_pause()
            or c._is_flash_crash_cooldown(provider.time())
        )
        if c.config.strict_hmm_mode and (c.processed_data or {}).get("regime_error"):
            reservation_allowed = False
        buy_allowed = reservation_allowed and bool(state.get("condition_ok", False))
        snapshot = MarketSnapshot(
            **context,
            bid=provider.get_price_by_type(c.config.connector_name, c.config.trading_pair, PriceType.BestBid),
            ask=provider.get_price_by_type(c.config.connector_name, c.config.trading_pair, PriceType.BestAsk),
            quote_available=quote_available, base_available=base_available,
            min_amount=c._rule_decimal(rule, "min_order_size"),
            min_notional=max(c._rule_decimal(rule, "min_notional_size"), c._rule_decimal(rule, "min_order_value")),
            amount_step=c._base_amount_increment(rule), price_step=c._rule_decimal(rule, "min_price_increment"),
            entry_signal=int((c.processed_data or {}).get("signal", 0)) > 0,
            entry_confidence=entry_confidence, trend_alignment=trend_alignment,
            valid=not c.config.manual_kill_switch,
            buy_allowed=bool(buy_allowed),
            reservation_allowed=bool(reservation_allowed),
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
            actions = [StopExecutorAction(controller_id=self.config.controller_id, executor_id=request_id, keep_position=True)
                       for request_id in result.cancel_ids
                       if any(ex.id == request_id and ex.is_active for ex in self.controller.executors_info)]
            if self.controller.config.manual_kill_switch:
                return actions
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
                    episode_entry_parent_request_id=request.parent_request_id,
                    episode_setup_policy=request.setup_policy,
                    episode_setup_started_at=request.setup_started_at,
                    episode_setup_confirmation_started_at=request.setup_confirmation_started_at,
                    episode_setup_low=request.setup_low,
                    episode_setup_rebound=request.setup_rebound,
                    episode_setup_no_new_low=request.setup_no_new_low,
                    episode_exit_policy=request.exit_policy,
                    episode_exit_hourly_recovered=request.exit_hourly_recovered,
                    episode_exit_risk_clear=request.exit_risk_clear,
                    episode_exit_trail_breach=request.exit_trail_breach,
                    episode_exit_target_base=request.exit_target_base,
                    episode_exit_target_revision=request.exit_target_revision,
                )
                actions.append(CreateExecutorAction(controller_id=self.config.controller_id, executor_config=executor_config))
            return actions
        except ReconciliationPending as exc:
            self.reason = f"reconciliation_pending:{exc}"
            return []
        except (EpisodeInputError, ValueError, TypeError, KeyError, ArithmeticError) as exc:
            self.hold_reason = str(exc)
            return []
