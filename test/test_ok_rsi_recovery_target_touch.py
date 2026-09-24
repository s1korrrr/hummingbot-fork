"""Held-bag recovery can keep its trail after touching target (R1 challenger, default off)."""
from decimal import Decimal
from test.test_ok_rsi_sell_policy import controller, position

# Basis 5, fees 1 on 100 units: net pnl = (price - 5) / 5 - 0.002.
# Target 10% arms at 5.60 (+11.8%); 5.45 is +8.8% (below target, 2.7% pullback);
# the executor's owned cost floor is 5 * 1.02 + 1 / 100 = 5.11.
ARM_PRICE = Decimal("5.60")
FADED_PRICE = Decimal("5.45")
BELOW_FLOOR_PRICE = Decimal("5.05")


def recovery_bot(*, keep_trail):
    bot = controller(balance="1000", price="5.5")
    bot.config.bag_recovery_target_pct = 0.10
    bot.config.recovery_keep_trail_after_target = keep_trail
    bot.positions_held = [position(bot)]
    return bot


def arm(bot):
    assert bot._check_held_bag_recovery(current_price=ARM_PRICE, now_ts=1724999990, current_signal=0) == []
    assert len(bot._recovery_trails) == 1


def test_flag_defaults_off_and_keeps_legacy_disarm_below_target():
    bot = recovery_bot(keep_trail=False)
    assert type(bot.config).model_fields["recovery_keep_trail_after_target"].default is False
    arm(bot)
    actions = bot._check_held_bag_recovery(current_price=FADED_PRICE, now_ts=1725000000, current_signal=0)
    assert actions == []
    assert bot._recovery_trails == {}


def test_touched_target_trail_sells_fading_spike_above_cost_floor():
    bot = recovery_bot(keep_trail=True)
    arm(bot)
    actions = bot._check_held_bag_recovery(current_price=FADED_PRICE, now_ts=1725000000, current_signal=0)
    assert len(actions) == 1
    config = actions[0].executor_config
    assert config.level_id.startswith("recovery_exit:")
    assert config.min_sell_price == Decimal("5.11")


def test_touched_target_trail_disarms_below_cost_floor():
    bot = recovery_bot(keep_trail=True)
    arm(bot)
    actions = bot._check_held_bag_recovery(current_price=BELOW_FLOOR_PRICE, now_ts=1725000000, current_signal=0)
    assert actions == []
    assert bot._recovery_trails == {}


def test_bag_that_never_touched_target_is_not_armed_by_the_flag():
    bot = recovery_bot(keep_trail=True)
    actions = bot._check_held_bag_recovery(current_price=FADED_PRICE, now_ts=1725000000, current_signal=0)
    assert actions == []
    assert bot._recovery_trails == {}
