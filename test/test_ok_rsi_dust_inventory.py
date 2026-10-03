"""Dust inventory stays accounted but does not pin EXITS or block a fresh buy."""
from decimal import Decimal
from test.test_ok_rsi_sell_policy import controller, position
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType


@pytest.fixture(autouse=True)
def _complete_native_history():
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = []
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        yield recorder


def dust_position(bot):
    return position(bot, amount="0.0000005224", basis="769.1", fees="0")


def buy_ready(bot):
    bot.processed_data = {
        "signal": 1,
        "timestamp": 1725000000.0,
        "signal_state": {
            "condition_ok": True, "timestamp": 1725000000.0, "signal_score": 3,
            "buy_entry_role": "full",
        },
    }


def test_dust_stays_accounted_and_does_not_select_exits_or_block_buy():
    bot = controller(price="800", minimum="1")
    bot.config.use_adaptive_trailing = False
    bot.positions_held = [dust_position(bot)]
    inventory = bot._aggregate_held_inventory(TradeType.BUY)
    assert inventory.total_amount == Decimal("0.0000005224")
    assert bot._inventory_is_dust(inventory.total_amount, Decimal("800"))
    assert not bot._has_buy_inventory()
    assert bot._held_bag_count() == 0
    assert bot._get_total_position_value_usd() == Decimal("0.00041792")
    assert bot._compute_plan_fields()["plan_mode"] == "ENTRIES"
    assert bot._has_sufficient_avg_entry_gap_for_buy(Decimal("800"))
    buy_ready(bot)
    assert bot.can_create_executor(1)


def test_held_and_terminal_dust_both_count_toward_the_position_cap():
    bot = controller(price="800", minimum="1")
    held = dust_position(bot)
    bot.positions_held = [held]
    object.__setattr__(bot.config, "max_total_position_usd", Decimal("0.0008"))
    object.__setattr__(bot.config, "usd_per_entry", Decimal("0.0004"))
    bot._terminal_dust_reserve_base = lambda: Decimal("0.0000005224")

    managed_value = bot._get_total_position_value_usd()

    assert managed_value == Decimal("0.00083584")
    assert managed_value > bot.config.max_total_position_usd
    # The terminal reserve affects cap accounting only, not the held-bag sell path.
    assert bot._get_sellable_inventory_amount() == held.amount


def test_normal_position_still_exits_and_does_not_open_another_full_entry():
    bot = controller(price="10", minimum="1")
    bot.config.use_adaptive_trailing = False
    bot.positions_held = [position(bot, amount="100", basis="5")]
    bot.config.max_total_position_usd = Decimal("50")
    assert bot._has_buy_inventory()
    assert bot._compute_plan_fields()["plan_mode"] == "EXITS"
    buy_ready(bot)
    assert not bot.can_create_executor(1)
    assert bot._last_gate_reason.get(TradeType.BUY) == "max_position"
