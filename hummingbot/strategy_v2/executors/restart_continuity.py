"""Restart continuity for native spot position executors.

A restart (planned or crash) must return every live position executor with the
same orders, fills, trailing high-water and lifecycle it had before the process
went away. The executor row is the durable record: a planned restart suspends
the executor (row stays RUNNING, ``restart_suspended`` marker), a crash leaves
the last checkpoint (trailing moves are checkpointed synchronously).

At boot, every unfinished row with complete order snapshots is rebuilt into the
original executor. Orders that were not terminal in the snapshot are handed back
to the connector's order tracker so venue status/fill polling reconciles them;
the executor then shares the tracker's live ``InFlightOrder`` object.

Rows that cannot be rebuilt are reported, never used to block order flow.
"""
import copy
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

RESTART_CONTINUITY_CONTRACT_VERSION = 1  # checked by stack entrypoints before an unattended boot
RESTART_SUSPEND_KEY = "restart_suspended"
RESTART_RESTORE_KEY = "restart_restored"
RESTART_ENTRY_STOP_REASON = "restart_continuity"

TERMINAL_STATES = {OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED}


class RestartContinuityError(ValueError):
    """A persisted executor row cannot be rebuilt exactly."""


@dataclass
class ContinuityPlan:
    executor_id: str
    controller_id: str
    status: RunnableStatus
    close_type: Optional[CloseType]
    entry_orders: List[InFlightOrder]
    exit_orders: List[InFlightOrder]
    custom_info: Dict[str, Any]
    trailing: Dict[str, Any]
    suspended: bool
    notes: List[str] = field(default_factory=list)

    @property
    def entry_terminal(self) -> bool:
        return not self.entry_orders or self.entry_orders[-1].current_state in TERMINAL_STATES

    @property
    def entry_filled_base(self) -> Decimal:
        return sum((order.executed_amount_base for order in self.entry_orders), Decimal("0"))

    @property
    def exit_filled_base(self) -> Decimal:
        return sum((order.executed_amount_base for order in self.exit_orders), Decimal("0"))

    @property
    def open_exit_order(self) -> Optional[InFlightOrder]:
        live = [order for order in self.exit_orders if order.current_state not in TERMINAL_STATES]
        return live[-1] if live else None


def _status(value) -> RunnableStatus:
    if isinstance(value, RunnableStatus):
        return value
    return RunnableStatus(int(value))


def _close_type(value) -> Optional[CloseType]:
    if value is None or isinstance(value, CloseType):
        return value
    return CloseType(int(value))


def _trailing(custom: Dict[str, Any]) -> Dict[str, Any]:
    trigger = custom.get("trailing_stop_trigger_pct")
    move_count = custom.get("trailing_move_count") or 0
    if isinstance(move_count, bool) or not isinstance(move_count, int) or move_count < 0:
        raise RestartContinuityError("Persisted trailing move count is invalid")
    if trigger is not None:
        trigger = Decimal(str(trigger))
        if not trigger.is_finite():
            raise RestartContinuityError("Persisted trailing trigger is not finite")
    return {
        "trailing_stop_trigger_pct": trigger,
        "trailing_move_count": move_count,
        "trailing_armed_timestamp": custom.get("trailing_armed_timestamp"),
        "trailing_last_update_timestamp": custom.get("trailing_last_update_timestamp"),
    }


def continuity_plan(info) -> Optional[ContinuityPlan]:
    """Return a rebuild plan for an unfinished spot position row, else None.

    None means the row is not a continuity candidate (finished, another executor
    type, or a legacy row without order snapshots). A candidate whose persisted
    facts are contradictory raises ``RestartContinuityError``.
    """
    if info is None or getattr(info, "type", None) != "position_executor":
        return None
    status = _status(info.status)
    if status not in (RunnableStatus.RUNNING, RunnableStatus.SHUTTING_DOWN):
        return None
    custom = dict(info.custom_info or {})
    snapshots = custom.get("execution_order_snapshots")
    if not isinstance(snapshots, list):
        return None
    config = info.config
    if getattr(config, "side", None) != TradeType.BUY or getattr(config, "leverage", 1) not in (1, None):
        return None
    if custom.get("resume_conflicting_fill") is not None:
        raise RestartContinuityError("Persisted executor carries an unresolved fill conflict")
    orders = []
    seen = set()
    for row in snapshots:
        if not isinstance(row, dict):
            raise RestartContinuityError("Order snapshot is not an object")
        order = InFlightOrder.from_json(row)
        if order.client_order_id in seen:
            raise RestartContinuityError("Duplicate order snapshot")
        seen.add(order.client_order_id)
        if order.trading_pair != config.trading_pair:
            raise RestartContinuityError("Order snapshot trading pair differs from executor")
        orders.append(order)
    recorded_ids = custom.get("order_ids")
    if isinstance(recorded_ids, list) and sorted(recorded_ids) != sorted(seen):
        raise RestartContinuityError("Order snapshots differ from recorded order ids")
    entry_orders = [order for order in orders if order.trade_type == TradeType.BUY]
    exit_orders = [order for order in orders if order.trade_type == TradeType.SELL]
    if any(order.current_state not in TERMINAL_STATES for order in entry_orders[:-1]):
        raise RestartContinuityError("Only the latest entry order may remain open")
    if len([order for order in exit_orders if order.current_state not in TERMINAL_STATES]) > 1:
        raise RestartContinuityError("More than one exit order remains open")
    plan = ContinuityPlan(
        executor_id=info.id,
        controller_id=info.controller_id,
        status=status,
        close_type=_close_type(info.close_type),
        entry_orders=entry_orders,
        exit_orders=exit_orders,
        custom_info=copy.deepcopy(custom),
        trailing=_trailing(custom),
        suspended=isinstance(custom.get(RESTART_SUSPEND_KEY), dict),
    )
    if plan.exit_filled_base > plan.entry_filled_base:
        raise RestartContinuityError("Persisted exits exceed persisted entry fills")
    if plan.entry_terminal and plan.entry_filled_base <= 0 and not plan.exit_orders:
        # A terminal zero-fill entry owns nothing; the executor ends normally on start.
        plan.notes.append("terminal_zero_fill_entry")
    return plan


def is_continuity_candidate(info) -> bool:
    try:
        return continuity_plan(info) is not None
    except (RestartContinuityError, ValueError, TypeError, KeyError, ArithmeticError):
        return True  # Reported during restore; never silently treated as absent.
