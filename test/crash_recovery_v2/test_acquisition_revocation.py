from decimal import Decimal as D

import pytest

from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

from .test_composition import evaluate
from .test_native_admission import host_controller


def test_overlay_hold_maps_to_entry_permission_false():
    x = evaluate(acquire=False)
    x = evaluate(x.state, (90,), acquire=False)
    assert x.recovery.state.phase == 'SHOCK_HOLD'
    assert x.execution_authorized is False
    controller = host_controller()
    assert controller.entry_permission(object()) is False
    assert controller.entry_permission(object(), check_funds=False) is False
    assert controller.determine_executor_actions() == []
    assert not any(isinstance(item, StopExecutorAction) for item in controller.determine_executor_actions())


def _native_lifecycle():
    try:
        from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
    except ModuleNotFoundError as exc:
        pytest.skip(f'{exc}. ./compile in this worktree is the next action')
    from test.modular_spot.test_native_lifecycle import (
        apply_fill,
        filled_event,
        make_modular_executor,
        tracked,
        view_of,
    )
    return PositionExecutor, apply_fill, filled_event, make_modular_executor, tracked, view_of


def _overlay():
    controller = host_controller()
    from test.modular_spot.test_native_lifecycle import config
    native_owner_config = config()

    class Overlay:
        # A gate overlay preserves the native execution owner's configuration.
        # It only substitutes entry admission, never the protective exit floor.
        config = native_owner_config

        def entry_permission(self, config, check_funds=True):
            return controller.entry_permission(config, check_funds=check_funds)

    return controller, Overlay()


def test_overlay_hold_cancels_remaining_buy_without_stop_or_early_stop():
    _, _, _, make_modular_executor, tracked, view_of = _native_lifecycle()
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.core.data_type.in_flight_order import OrderState

    x = evaluate(acquire=False)
    x = evaluate(x.state, (90,), acquire=False)
    assert x.recovery.state.phase == 'SHOCK_HOLD'
    controller, overlay = _overlay()
    assert controller.entry_permission(object()) is False

    executor = make_modular_executor()
    remainder = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    remainder.order.creation_timestamp = executor._strategy.current_timestamp
    executor._open_order = remainder
    executor._entry_submitted = True
    executor._trailing_stop_trigger_pct = D('.02')
    executor._strategy.controllers[executor.config.controller_id] = overlay
    executor.place_order.reset_mock()
    executor.control_open_order()
    executor._strategy.cancel.assert_called_once_with(
        connector_name='okx', trading_pair='BTC-USDC', order_id='entry')
    assert executor._entry_stop_reason == 'modular_entry_permission_revoked'
    assert executor._entry_lifecycle['state'] == 'cancel_pending'
    executor.place_order.assert_not_called()
    executor.control_barriers()
    market_calls = [c for c in executor.place_order.call_args_list
                    if c.kwargs.get('order_type') == OrderType.MARKET]
    assert market_calls == []
    assert view_of(executor).remaining_base == D('.4')
    assert executor.close_type is None
    assert executor._trailing_stop_trigger_pct == D('.02')
    assert controller.determine_executor_actions() == []


def test_rejected_cancel_keeps_partial_fill_and_does_not_resubmit():
    _, _, _, make_modular_executor, tracked, view_of = _native_lifecycle()
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.core.data_type.in_flight_order import OrderState

    controller, overlay = _overlay()
    executor = make_modular_executor()
    remainder = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    remainder.order.creation_timestamp = executor._strategy.current_timestamp
    executor._open_order = remainder
    executor._entry_submitted = True
    executor._strategy.controllers[executor.config.controller_id] = overlay
    executor.control_open_order()
    executor._strategy.cancel.assert_called()
    assert remainder.order.current_state == OrderState.PARTIALLY_FILLED
    executor.place_order.reset_mock()
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert view_of(executor).remaining_base == D('.4')
    assert executor.close_type is None
    assert controller.determine_executor_actions() == []


def test_late_fill_after_overlay_cancel_is_owned_and_does_not_rearm_buy():
    _, apply_fill, filled_event, make_modular_executor, tracked, view_of = _native_lifecycle()
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.core.data_type.in_flight_order import OrderState

    controller, overlay = _overlay()
    executor = make_modular_executor()
    remainder = tracked('entry', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    remainder.order.creation_timestamp = executor._strategy.current_timestamp
    executor._open_order = remainder
    executor._entry_submitted = True
    executor._trailing_stop_trigger_pct = D('.02')
    executor._strategy.controllers[executor.config.controller_id] = overlay
    executor.control_open_order()
    apply_fill(remainder.order, trade_id='late', fill='.6')
    remainder.order.current_state = OrderState.FILLED
    executor.process_order_filled_event(0, None, filled_event(remainder, trade_id='late', fill='.6'))
    held = view_of(executor)
    assert held.remaining_base == D('1')
    assert held.entry_quote == D('100')
    executor.place_order.reset_mock()
    executor.control_open_order()
    executor.place_order.assert_not_called()
    assert executor.close_type is None
    assert executor._trailing_stop_trigger_pct == D('.02')
    assert controller.determine_executor_actions() == []


def test_filled_position_keeps_trailing_when_overlay_holds():
    _, _, _, make_modular_executor, tracked, view_of = _native_lifecycle()
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.core.data_type.in_flight_order import OrderState

    controller, overlay = _overlay()
    executor = make_modular_executor()
    filled = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    executor._open_order = filled
    executor._entry_submitted = True
    executor._trailing_stop_trigger_pct = D('.02')
    executor._strategy.controllers[executor.config.controller_id] = overlay
    executor.place_order.reset_mock()
    executor._strategy.cancel.reset_mock()
    executor.control_open_order()
    executor._strategy.cancel.assert_not_called()
    executor.control_barriers()
    market_calls = [c for c in executor.place_order.call_args_list
                    if c.kwargs.get('order_type') == OrderType.MARKET]
    assert market_calls == []
    assert view_of(executor).remaining_base == D('1')
    assert executor.close_type is None
    assert executor._trailing_stop_trigger_pct == D('.02')
    assert not any(isinstance(item, StopExecutorAction) for item in controller.determine_executor_actions())
