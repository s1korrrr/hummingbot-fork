import asyncio
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy_v2.components.held_inventory import (
    BagRecoveryPolicy,
    HeldInventory,
    has_sell_price_gap,
    net_pnl_pct,
    reanchor_after_sell,
    recovery_min_sell_price,
    recovery_slice_amount,
    recovery_step,
    should_hold_as_bag,
)

ROOT = str(Path(__file__).resolve().parents[4])
D = Decimal


class HeldInventoryPolicyTest(unittest.TestCase):
    def test_bag_trigger_is_percentage_only(self):
        policy = BagRecoveryPolicy()
        self.assertFalse(should_hold_as_bag(D("100"), D("97.01"), policy))
        self.assertTrue(should_hold_as_bag(D("100"), D("97"), policy))
        self.assertFalse(should_hold_as_bag(D("100"), D("50"), BagRecoveryPolicy(early_stop_drawdown_pct=D("0"))))
        self.assertFalse(should_hold_as_bag(None, D("50"), policy))

    def test_held_inventory_blends_bags_like_position_hold(self):
        bag = HeldInventory().add_buy(D("1"), D("100"), D("0.1")).add_buy(D("1"), D("90"), D("0.09"))
        self.assertEqual(bag.amount, D("2"))
        self.assertEqual(bag.breakeven_price, D("95"))
        bag = bag.add_sell(D("0.5"), D("50"), D("0.05"))
        self.assertEqual(bag.amount, D("1.5"))
        self.assertEqual(bag.breakeven_price, D("95"))
        self.assertEqual(bag.realized_pnl_quote, D("2.5"))
        self.assertEqual(bag.fees_quote, D("0.24"))
        with self.assertRaises(ValueError):
            bag.add_sell(D("2"), D("1"))

    def test_net_pnl_floor_gap_and_slice(self):
        policy = BagRecoveryPolicy()
        self.assertEqual(net_pnl_pct(D("2"), D("100"), D("2"), D("105")), D("0.04"))
        self.assertEqual(recovery_min_sell_price(D("100"), D("2"), D("2"), policy), D("103"))
        self.assertIsNone(recovery_min_sell_price(D("100"), D("2"), D("2"),
                                                  BagRecoveryPolicy(sell_only_if_profitable=False)))
        self.assertTrue(has_sell_price_gap(D("101"), None, policy))
        self.assertFalse(has_sell_price_gap(D("100.5"), D("100"), policy))
        self.assertTrue(has_sell_price_gap(D("99"), D("100"), policy))
        self.assertEqual(recovery_slice_amount(D("5"), D("100"), policy), D("2"))
        self.assertEqual(recovery_slice_amount(D("2.05"), D("100"), policy, min_quote=D("10")), D("2.05"))
        self.assertEqual(recovery_slice_amount(D("0.05"), D("100"), policy, min_quote=D("10")), D("0"))

    def test_trail_disarms_below_target_and_restarts_on_breakeven_change(self):
        policy = BagRecoveryPolicy()
        kwargs = dict(amount=D("1"), remaining_fees_quote=D("0"), ema_fast=None, policy=policy)
        trail, decision = recovery_step(None, breakeven_price=D("100"), price=D("105"), rsi=70.0, **kwargs)
        self.assertEqual((trail, decision), (None, "wait_overbought"))
        trail, decision = recovery_step(None, breakeven_price=D("100"), price=D("105"), rsi=80.0, **kwargs)
        self.assertEqual(decision, "armed")
        same, decision = recovery_step(trail, breakeven_price=D("100"), price=D("103.9"), rsi=80.0, **kwargs)
        self.assertEqual((same, decision), (None, "below_target"))
        restarted, decision = recovery_step(trail, breakeven_price=D("99"), price=D("105"), rsi=70.0, **kwargs)
        self.assertEqual((restarted, decision), (None, "wait_overbought"))


def _hl_controller(policy: BagRecoveryPolicy):
    # Import the repository's controllers without purging already-loaded modules,
    # which would break class identity for tests collected later in the session.
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig
    from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy

    provider = MagicMock()
    provider.time.return_value = 1_725_000_000.0
    provider.get_available_balance.side_effect = lambda connector, asset: D("1000")
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=D("0.001"), min_order_size=D("0.001"), min_base_amount_increment=D("0.001"),
        min_notional_size=D("10"), min_order_value=D("10"))
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
        D("25.000") if price_type == PriceType.BestBid else D("25.010"))
    controller = HLRSIController(
        config=HLRSIControllerConfig(
            id="bag-parity", connector_name="hyperliquid", trading_pair="HYPE-USDC", trade_mode="buy_sell",
            strict_hmm_mode=False, sell_only_if_profitable=policy.sell_only_if_profitable,
            min_profit_pct_for_sell=policy.min_profit_pct_for_sell,
            bag_recovery_target_pct=float(policy.recovery_target_pct),
            sell_rsi_overbought=policy.recovery_arm_rsi,
            recovery_trail_pullback_pct=float(policy.recovery_trail_pullback_pct),
            recovery_rsi_rollover_delta=policy.recovery_rsi_rollover_delta,
            sell_executor_price_gap_threshold=policy.sell_price_gap_pct,
            recovery_sell_execution_strategy=ExecutionStrategy.MARKET,
        ),
        market_data_provider=provider, actions_queue=asyncio.Queue())
    controller.executors_info = []
    return controller


class HeldInventoryLiveParityTest(unittest.TestCase):
    """The component must reproduce HLRSIController._check_held_bag_recovery decisions tick by tick."""

    PATH = [  # price, rsi, ema_fast
        (D("24.00"), 55.0, 24.2), (D("26.30"), 70.0, 25.5), (D("26.40"), 79.0, 25.9),
        (D("26.80"), 82.0, 26.3), (D("27.10"), 84.0, 26.6), (D("27.00"), 81.0, 26.9),
        (D("26.95"), 80.0, 27.05), (D("26.85"), 76.0, 27.0), (D("26.30"), 72.0, 26.8),
        (D("25.00"), 60.0, 26.0), (D("26.50"), 79.0, 26.0), (D("26.60"), 80.0, 26.2),
        (D("26.20"), 77.5, 26.4), (D("27.50"), 85.0, 26.8), (D("27.25"), 83.0, 27.3),
    ]

    def run_live(self, policy, position):
        controller = _hl_controller(policy)
        controller.positions_held = [position]
        decisions = []
        for index, (price, rsi, ema) in enumerate(self.PATH):
            controller.processed_data = {"signal": 0, "signal_state": {"rsi": rsi, "ema_fast": ema}}
            actions = controller._check_held_bag_recovery(current_price=price, now_ts=1_725_000_000.0 + index,
                                                          current_signal=0)
            decisions.append(bool(actions))
        return decisions

    def run_component(self, policy, position):
        trail, decisions = None, []
        for price, rsi, ema in self.PATH:
            trail, decision = recovery_step(trail, amount=position.amount, breakeven_price=position.breakeven_price,
                                            remaining_fees_quote=position.remaining_fees_quote, price=price,
                                            rsi=rsi, ema_fast=ema, policy=policy)
            sold = decision.startswith("sell:")
            if sold:
                trail = reanchor_after_sell(trail, price, rsi)
            decisions.append(sold)
        return decisions

    def test_sell_timing_matches_live_controller(self):
        for policy in (BagRecoveryPolicy(), BagRecoveryPolicy(recovery_target_pct=D("0.06")),
                       BagRecoveryPolicy(recovery_trail_pullback_pct=D("0.02"), recovery_rsi_rollover_delta=5.0)):
            position = SimpleNamespace(connector_name="hyperliquid", trading_pair="HYPE-USDC", side=TradeType.BUY,
                                       amount=D("40"), breakeven_price=D("25"), remaining_fees_quote=D("0.4"))
            live = self.run_live(policy, position)
            self.assertTrue(any(live), "fixture must exercise at least one live recovery sell")
            self.assertEqual(self.run_component(policy, position), live)


if __name__ == "__main__":
    unittest.main()
