"""Order proposals retain configured sizing and a fee-covered owned cost floor."""
import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig
from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
from hummingbot.core.data_type.common import TradeType


def controller(mode="buy_sell", *, legacy=False, balance="100", price="10", minimum="0.01"):
    provider = MagicMock()
    provider.time.return_value = 1725000000.0
    provider.get_available_balance.return_value = Decimal(balance)
    provider.get_price_by_type.return_value = Decimal(price)
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=Decimal("0.01"), min_order_size=Decimal("0.00000001"),
        min_base_amount_increment=Decimal("0.00000001"), min_notional_size=Decimal(minimum),
        min_order_value=Decimal(minimum),
    )
    cls, config_cls = (HLRSIController, HLRSIControllerConfig) if legacy else (OKRSIController, OKRSIControllerConfig)
    bot = cls(config=config_cls(
        id="sell-policy", connector_name="hyperliquid" if legacy else "okx", trading_pair="SUI-USDC",
        trade_mode=mode, strict_hmm_mode=False, usd_per_entry=Decimal("25"),
        signal_sell_inventory_slice_multiplier=2, recovery_partial_exit_fraction=0.2,
        sell_only_if_profitable=True, min_profit_pct_for_sell=0.02,
    ), market_data_provider=provider, actions_queue=asyncio.Queue())
    bot.executors_info = []
    bot.positions_held = []
    bot.processed_data = {"signal": -1, "signal_state": {"condition_ok": True, "rsi": 90, "ema_fast": 11}}
    return bot


def position(bot, amount="100", basis="5", fees="1"):
    return SimpleNamespace(connector_name=bot.config.connector_name, trading_pair=bot.config.trading_pair,
                           side=TradeType.BUY, amount=Decimal(amount),
                           breakeven_price=Decimal(basis) if basis is not None else None,
                           remaining_fees_quote=Decimal(fees))


def recovery(bot):
    # Actual arming followed by reversal; no patched sizing/gates or fabricated action.
    bot._check_held_bag_recovery(current_price=Decimal("11"), now_ts=1724999990, current_signal=-1)
    return bot._check_held_bag_recovery(current_price=Decimal("10"), now_ts=1725000000, current_signal=-1)


@pytest.mark.parametrize("mode,expected", [("buy_sell", "5"), ("sell_only", "10")])
def test_recovery_uses_same_target_as_signal_sale(mode, expected):
    bot = controller(mode)
    bot.positions_held = [position(bot)]
    actions = recovery(bot)
    assert len(actions) == 1
    config = actions[0].executor_config
    assert config.amount == (Decimal("50") / config.price if mode == "buy_sell" else Decimal(expected))
    assert actions[0].executor_config.min_sell_price == Decimal("5.11")


def test_multiple_held_positions_create_only_one_recovery_sale():
    bot = controller(balance="200")
    bot.positions_held = [position(bot), position(bot, basis="6")]
    actions = recovery(bot)
    assert len(actions) == 1
    assert actions[0].executor_config.amount * actions[0].executor_config.price == Decimal("50")


def test_hl_recovery_retains_legacy_fraction_and_multiple_positions():
    bot = controller(legacy=True, balance="200")
    bot.positions_held = [position(bot), position(bot, basis="6")]
    actions = recovery(bot)
    assert len(actions) == 2
    assert [a.executor_config.amount for a in actions] == [Decimal("20"), Decimal("20")]


def test_signal_sell_binds_owned_basis_remaining_fees_and_profit_floor():
    bot = controller()
    bot.positions_held = [position(bot, amount="10", basis="5", fees="1"),
                          position(bot, amount="10", basis="7", fees="3")]
    config = bot.get_executor_config(TradeType.SELL, Decimal("10"), Decimal("999"))
    assert config.amount == Decimal("50") / config.price
    assert config.min_sell_price == Decimal("6.32")


@pytest.mark.parametrize("basis,fees", [(None, "1"), ("NaN", "1"), ("5", "NaN"), ("5", "-1")])
def test_signal_sell_fails_closed_for_unavailable_cost_or_fees(basis, fees):
    bot = controller("sell_only")
    bot.positions_held = [position(bot, basis=basis, fees=fees)]
    with pytest.raises(ValueError, match="cost basis|fees"):
        bot.get_executor_config(TradeType.SELL, Decimal("10"), Decimal("999"))


def test_signal_sell_fails_closed_without_owned_basis():
    bot = controller("sell_only")
    with pytest.raises(ValueError, match="cost basis"):
        bot.get_executor_config(TradeType.SELL, Decimal("10"), Decimal("999"))


def test_sell_only_never_exceeds_inventory_with_owned_cost_even_with_dust():
    bot = controller("sell_only", minimum="5")
    bot.positions_held = [position(bot, amount="0.7")]
    config = bot.get_executor_config(TradeType.SELL, Decimal("10"), Decimal("999"))
    assert config.amount == Decimal("0.7")
    assert config.min_sell_price is not None


@pytest.mark.parametrize("amount,minimum,expected", [("0.7", "5", "0.7"), ("100", "120", "12")])
def test_recovery_preserves_minimum_notional_and_dust(amount, minimum, expected):
    bot = controller(minimum=minimum)
    bot.positions_held = [position(bot, amount=amount, fees="0")]
    actions = recovery(bot)
    assert len(actions) == 1
    config = actions[0].executor_config
    if amount == "0.7":
        assert config.amount == Decimal(expected)
    else:
        assert Decimal(minimum) <= config.amount * config.price < Decimal(minimum) + Decimal("0.000001")


def test_normal_and_recovery_cannot_both_sell_in_one_action_proposal():
    bot = controller()
    bot.config.durable_candle_watermark_enabled = False
    bot.positions_held = [position(bot)]
    bot._check_held_bag_recovery(current_price=Decimal("11"), now_ts=1724999990, current_signal=-1)
    actions = bot.determine_executor_actions()
    sells = [a for a in actions if getattr(getattr(a, "executor_config", None), "side", None) == TradeType.SELL]
    assert len(sells) == 1
    assert sells[0].executor_config.amount * sells[0].executor_config.price == Decimal("50")


def test_normal_sell_order_keeps_profit_floor_when_market_drops_and_exit_fee_applies():
    from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
    from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor

    bot = controller()
    bot.positions_held = [position(bot)]
    config = bot.get_executor_config(TradeType.SELL, Decimal("10"), Decimal("999"))
    strategy = MagicMock()
    connector = MagicMock()
    connector.get_fee.return_value = AddedToCostTradeFee(percent=Decimal("0.001"))
    strategy.connectors = {"okx": connector}
    executor = OrderExecutor(strategy, config)
    executor.get_price = MagicMock(return_value=Decimal("4"))
    executor.get_trading_rules = MagicMock(return_value=bot.market_data_provider.get_trading_rules.return_value)
    executor.place_order = MagicMock(return_value="local-test-order")
    executor.place_open_order()
    order = executor.place_order.call_args.kwargs
    assert order["amount"] == config.amount
    assert order["price"] * Decimal("0.999") >= Decimal("5.11")


def test_signal_sell_rejects_missing_fee_record():
    bot = controller("sell_only")
    owned = position(bot)
    del owned.remaining_fees_quote
    bot.positions_held = [owned]
    with pytest.raises(ValueError, match="fees"):
        bot.get_executor_config(TradeType.SELL, Decimal("10"), Decimal("999"))


def test_signal_sell_rejects_incomplete_mixed_owned_basis():
    bot = controller("sell_only")
    bot.positions_held = [position(bot, amount="50"), position(bot, amount="50", basis=None)]
    with pytest.raises(ValueError, match="cost basis"):
        bot.get_executor_config(TradeType.SELL, Decimal("10"), Decimal("999"))


def strict_wallet_controller(*, balance="100", minimum="0.01"):
    bot = controller("sell_only", balance=balance, minimum=minimum)
    bot.config = OKRSIControllerConfig(**{
        **bot.config.model_dump(), 'sell_only_if_profitable': False,
        'sell_only_strict_fraction': True,
    })
    return bot


@pytest.mark.parametrize('balance', ['100', '1'])
def test_strict_wallet_signal_sells_ten_percent_without_basis_or_small_wallet_sweep(balance):
    bot = strict_wallet_controller(balance=balance)
    assert not bot.positions_held
    config = bot.get_executor_config(TradeType.SELL, Decimal('10'), Decimal('999'))
    assert config.amount == Decimal(balance) * Decimal('0.1')
    assert config.min_sell_price is None
    assert bot._check_held_bag_recovery(current_price=Decimal('10'), now_ts=1725000000, current_signal=-1) == []
    with pytest.raises(ValueError, match='BUY executor construction is forbidden'):
        bot.get_executor_config(TradeType.BUY, Decimal('10'), Decimal('999'))


@pytest.mark.parametrize('balance,minimum', [('1', '5'), ('0.7', '5')])
def test_strict_wallet_signal_skips_below_minimum_instead_of_uplift_or_sweep(balance, minimum):
    bot = strict_wallet_controller(balance=balance, minimum=minimum)
    with pytest.raises(ValueError, match='valid sell amount'):
        bot.get_executor_config(TradeType.SELL, Decimal('10'), Decimal('999'))


def test_strict_wallet_keeps_fraction_even_when_remainder_would_be_dust():
    bot = strict_wallet_controller(balance='1', minimum='5')
    bot.config.sell_only_inventory_fraction = Decimal('.8')
    config = bot.get_executor_config(TradeType.SELL, Decimal('10'), Decimal('999'))
    assert config.amount == Decimal('.8')


@pytest.mark.parametrize('balance,minimum,expected', [('100', '0.01', '10'), ('1', '0.01', '.1'), ('1', '5', None)])
def test_strict_recovery_obeys_wallet_fraction_and_skips_venue_minimum(balance, minimum, expected):
    bot = strict_wallet_controller(balance=balance, minimum=minimum)
    bot.positions_held = [position(bot, amount=balance, fees='0')]
    actions = recovery(bot)
    if expected is None:
        assert actions == []
    else:
        assert len(actions) == 1
        assert actions[0].executor_config.amount == Decimal(expected)


def test_strict_fraction_policy_cannot_be_disabled_by_hot_reload():
    bot = strict_wallet_controller()
    assert bot.config.sell_only_strict_fraction is True
    requested = OKRSIControllerConfig(**{**bot.config.model_dump(), 'sell_only_strict_fraction': False})
    with pytest.raises(ValueError, match='restart required.*sell_only_strict_fraction'):
        bot.update_config(requested)
    assert bot.config.sell_only_strict_fraction is True


def test_strict_fraction_does_not_change_main_minimum_and_dust_policy():
    bot = controller(minimum='120')
    bot.config = OKRSIControllerConfig(**{**bot.config.model_dump(), 'sell_only_strict_fraction': True})
    bot.positions_held = [position(bot, fees='0')]
    config = bot.get_executor_config(TradeType.SELL, Decimal('10'), Decimal('999'))
    assert config.amount * config.price >= Decimal('120')


@pytest.mark.parametrize('recovery_exit', [False, True])
def test_strict_wallet_admission_rechecks_fraction_after_available_balance_drops(recovery_exit):
    from hummingbot.strategy_v2.models.base import RunnableStatus
    bot = strict_wallet_controller()
    bot._status = RunnableStatus.RUNNING
    if recovery_exit:
        bot.positions_held = [position(bot, fees='0')]
        config = recovery(bot)[0].executor_config
    else:
        config = bot.get_executor_config(TradeType.SELL, Decimal('10'), Decimal('999'))
    bot.processed_data['data_health'] = {'ready': True}
    assert bot.validate_executor_config(config)
    bot.market_data_provider.get_available_balance.return_value = Decimal('50')
    assert not bot.validate_executor_config(config)
    assert bot._last_gate_reason[TradeType.SELL] == 'sell_fraction_exceeded'


def test_strict_wallet_never_accepts_quantization_above_fraction():
    bot = strict_wallet_controller()
    bot.market_data_provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount + Decimal('1')
    with pytest.raises(ValueError, match='valid sell amount'):
        bot.get_executor_config(TradeType.SELL, Decimal('10'), Decimal('999'))


def test_strict_wallet_below_minimum_produces_no_action_or_construction_error():
    bot = strict_wallet_controller(balance='1', minimum='5')
    bot.config.durable_candle_watermark_enabled = False
    bot._entry_decision_gate = MagicMock()  # BUY admission storage is unrelated to this SELL proposal.
    assert bot.create_actions_proposal() == []
    assert bot._last_gate_reason[TradeType.SELL] == 'sell_fraction_below_minimum'
