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
        self.assertEqual((bag.amount, bag.breakeven_price, bag.remaining_fees_quote), (D("2"), D("95"), D("0.19")))
        bag = bag.add_sell(D("0.5"), D("50"), D("0.05"))
        self.assertEqual((bag.amount, bag.breakeven_price), (D("1.5"), D("95")))
        self.assertEqual(bag.realized_pnl_quote, D("2.5"))
        self.assertEqual(bag.remaining_fees_quote, D("0.1425"))
        self.assertEqual(bag.fees_paid_quote, D("0.24"))
        with self.assertRaises(ValueError):
            bag.add_sell(D("2"), D("1"))

    def test_running_average_resets_when_flat_like_live(self):
        # Live PositionHold: buy 1@100, sell .5@110, buy .5@80 -> breakeven 90 (not lifetime 93.33).
        bag = HeldInventory().add_buy(D("1"), D("100")).add_sell(D("0.5"), D("55")).add_buy(D("0.5"), D("40"))
        self.assertEqual(bag.breakeven_price, D("90"))
        flat = HeldInventory().add_buy(D("1"), D("600"), D("0.6")).add_sell(D("1"), D("650"))
        self.assertEqual((flat.amount, flat.breakeven_price, flat.remaining_fees_quote), (D("0"), D("0"), D("0")))
        again = flat.add_buy(D("1"), D("850"), D("0.85"))
        self.assertEqual((again.breakeven_price, again.remaining_fees_quote), (D("850"), D("0.85")))
        self.assertEqual(again.pnl_quote(D("850")), D("50") - D("1.45"))

    def test_matches_live_position_hold_summary(self):
        from hummingbot.core.data_type.common import TradeType as T
        from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold

        live = PositionHold("binance", "BNB-USDC", T.BUY)
        ours = HeldInventory()
        fills = [("BUY", "1", "100", "0.1"), ("BUY", "2", "180", "0.18"), ("SELL", "1.5", "150", "0.15"),
                 ("BUY", "0.5", "40", "0.04"), ("SELL", "2", "210", "0.21"), ("BUY", "1", "95", "0.095")]
        for index, (side, base, quote, fee) in enumerate(fills):
            live.add_order_record({"client_order_id": f"o{index}", "trade_type": side, "executed_amount_base": base,
                                   "executed_amount_quote": quote, "cumulative_fee_paid_quote": fee,
                                   "actual_base_fee": "0"})
            ours = (ours.add_buy if side == "BUY" else ours.add_sell)(D(base), D(quote), D(fee))
            summary = live.get_position_summary(D("100"))
            self.assertEqual(summary.amount, ours.amount)
            self.assertEqual(summary.breakeven_price.quantize(D("1e-12")), ours.breakeven_price.quantize(D("1e-12")))
            self.assertEqual(summary.realized_pnl_quote.quantize(D("1e-12")), ours.realized_pnl_quote.quantize(D("1e-12")))
            self.assertEqual(summary.remaining_fees_quote.quantize(D("1e-12")),
                             ours.remaining_fees_quote.quantize(D("1e-12")))

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
        self.assertEqual(recovery_slice_amount(D("2.05"), D("100"), policy, min_quote=D("10")), D("2"))
        self.assertEqual(recovery_slice_amount(D("0.05"), D("100"), policy, min_quote=D("10")), D("0"))
        # OK RSI: a remainder worth <= the sweep cap is taken; a larger sweep falls back to the valid slice.
        self.assertEqual(recovery_slice_amount(D("2.04"), D("100"), policy, min_quote=D("10"),
                                               remainder_sweep_quote=D("5")), D("2.04"))
        self.assertEqual(recovery_slice_amount(D("2.08"), D("100"), policy, min_quote=D("10"),
                                               remainder_sweep_quote=D("5")), D("2"))

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


class OKRSIRecoveryParityTest(unittest.TestCase):
    """Randomized paths through the real OKRSIController recovery vs the component, step by step."""

    TICK = D("0.01")

    def _bot(self):
        if ROOT not in sys.path:
            sys.path.insert(0, ROOT)
        from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig

        provider = MagicMock()
        provider.time.return_value = 1725000000.0
        provider.get_available_balance.return_value = D("1000000")
        provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
        provider.get_trading_rules.return_value = SimpleNamespace(
            min_price_increment=self.TICK, min_order_size=D("0.001"), min_base_amount_increment=D("0.001"),
            min_notional_size=D("1"), min_order_value=D("1"))
        bot = OKRSIController(config=OKRSIControllerConfig(
            id="bag-parity-okrsi", connector_name="okx", trading_pair="BNB-USDC", trade_mode="buy_sell",
            strict_hmm_mode=False, usd_per_entry=D("100"), signal_sell_inventory_slice_multiplier=2,
            sell_only_if_profitable=True, min_profit_pct_for_sell=0.02, bag_recovery_target_pct=0.04,
            sell_rsi_overbought=78.0, recovery_trail_pullback_pct=0.008, recovery_rsi_rollover_delta=2.0,
            sell_executor_price_gap_threshold=D("0.01"), sell_rsi_rollover_delta=0.5,
            trend_confirmation_enabled=True, sell_trend_hold_min_confidence=0.2,
            # Live BNB values (V1/V2 mounted configs, 2026-09-25 ground truth).
            early_stop_drawdown_pct=0.03, early_stop_keep_position=True, recovery_cancel_stale_order_pct=0.005,
            trend_sell_threshold_boost=4.0,
        ), market_data_provider=provider, actions_queue=asyncio.Queue())
        bot.executors_info = []
        return bot, provider

    def test_randomized_paths_match_live_decisions_amounts_prices_and_floors(self):
        import random

        from hummingbot.strategy_v2.components.held_inventory import maker_sell_price, sell_trend_hold
        from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

        policy = BagRecoveryPolicy(sell_price_gap_pct=D("0.01"), slice_quote=D("200"))
        sweep = None
        total_sells = 0
        branches = {"sell:pullback-trail": 0, "sell:rsi-rollover": 0, "sell:sell-signal": 0,
                    "trend-held-signal": 0, "gap-blocked": 0, "breakeven-changed": 0}
        for seed in range(40):
            rng = random.Random(seed)
            bot, provider = self._bot()
            sweep = D(str(bot.config.sell_remainder_sweep_max_quote))
            amount, breakeven, fees = D(str(rng.choice([1.5, 3.2, 6.0]))), D("600"), D(str(rng.choice([0, 0.4, 1.8])))
            position = SimpleNamespace(connector_name="okx", trading_pair="BNB-USDC", side=TradeType.BUY,
                                       amount=amount, breakeven_price=breakeven, remaining_fees_quote=fees)
            bot.positions_held = [position]
            price, rsi, prev_rsi, trail, last_sell = D("640"), 70.0, 70.0, None, None
            for step in range(120):
                price = (price * D(str(1 + rng.gauss(0.0005, 0.006)))).quantize(self.TICK)
                prev_rsi, rsi = rsi, max(1.0, min(99.0, rsi + rng.gauss(0, 4)))
                ema = float(price) * (1 + rng.gauss(0, 0.004))
                signal = -1 if rng.random() < 0.15 else 0
                strong_up = rng.random() < 0.4
                if step == 60 and rng.random() < 0.5:  # a new bag merges: breakeven changes mid-trail
                    position.breakeven_price = D("610")
                    position.amount += D("1")
                    amount = position.amount
                    branches["breakeven-changed"] += 1
                provider.get_price_by_type.return_value = price
                bot.processed_data = {"signal": signal,
                                      "signal_state": {"rsi": rsi, "rsi_prev": prev_rsi, "ema_fast": ema},
                                      "trend_confirmation": {"direction": "up" if strong_up else "range",
                                                             "confidence": 0.9 if strong_up else 0.0}}
                proposed = bot._check_held_bag_recovery(current_price=price, now_ts=1725000000.0 + step,
                                                        current_signal=signal)
                actions = [a for a in proposed if isinstance(a, CreateExecutorAction)]
                hold = sell_trend_hold(strong_up, price, rsi, prev_rsi, ema, bot.config.sell_rsi_rollover_delta)
                trail, decision = recovery_step(trail, amount=position.amount, breakeven_price=position.breakeven_price,
                                                remaining_fees_quote=position.remaining_fees_quote, price=price,
                                                rsi=rsi, ema_fast=ema, policy=policy, sell_signal=signal < 0,
                                                trend_hold=hold)
                expected = None
                if decision == "hold" and signal < 0 and hold:
                    branches["trend-held-signal"] += 1
                if decision.startswith("sell:"):
                    branches[decision] += 1
                    sell_price = maker_sell_price(price, price, self.TICK)
                    size = recovery_slice_amount(position.amount, sell_price, policy, min_amount=D("0.001"),
                                                 min_quote=D("1"), remainder_sweep_quote=sweep)
                    if size > 0 and has_sell_price_gap(sell_price, last_sell, policy):
                        expected = (size, sell_price, recovery_min_sell_price(position.breakeven_price, position.amount,
                                                                              position.remaining_fees_quote, policy))
                        trail = reanchor_after_sell(trail, price, rsi)
                    else:
                        branches["gap-blocked"] += 1
                live = None
                if actions:
                    config = actions[0].executor_config
                    live = (config.amount, config.price, config.min_sell_price)
                    bot.executors_info.append(SimpleNamespace(
                        id=f"sell{seed}_{step}", connector_name="okx", trading_pair="BNB-USDC", side=TradeType.SELL,
                        is_active=False, filled_amount_quote=config.amount * config.price, type="order_executor",
                        timestamp=1725000000.0 + step, close_timestamp=1725000000.0 + step, config=config,
                        custom_info={"level_id": config.level_id}))
                    last_sell = config.price
                    total_sells += 1
                self.assertEqual(live, expected, f"seed={seed} step={step} decision={decision}")
        self.assertGreater(total_sells, 20, "randomized fixture must exercise many live recovery sells")
        self.assertTrue(all(count > 0 for count in branches.values()), branches)


class OKRSISellSignalParityTest(unittest.TestCase):
    """The component's bag sell signal and threshold match the live methods on randomized states."""

    def test_threshold_and_signal_match_live(self):
        import random

        from hummingbot.strategy_v2.components.held_inventory import bag_sell_signal, net_pnl_pct, sell_signal_threshold
        from hummingbot.strategy_v2.utils.market_analysis import MultiTimeframeTrend, TrendDirection

        helper = OKRSIRecoveryParityTest()
        policy = BagRecoveryPolicy()
        fired = 0
        rng = random.Random(7)
        for case in range(3000):
            bot, provider = helper._bot()
            price = D(str(round(rng.uniform(560, 700), 2)))
            breakeven = D(str(round(rng.uniform(540, 660), 2)))
            amount, fees = D(str(rng.choice([0.5, 2, 4.2]))), D(str(rng.choice([0, 0.3, 1.2])))
            bot.positions_held = [SimpleNamespace(connector_name="okx", trading_pair="BNB-USDC", side=TradeType.BUY,
                                                  amount=amount, breakeven_price=breakeven,
                                                  remaining_fees_quote=fees)]
            provider.get_price_by_type.return_value = price
            regime = rng.choice([None, "Range_LV", "Trend_Up_HV", "Trend_Down_LV", "Unknown"])
            regime_conf = rng.random()
            direction = rng.choice([TrendDirection.UP, TrendDirection.DOWN, TrendDirection.RANGE])
            trend_conf = rng.random()
            trend = MultiTimeframeTrend(direction=direction, confidence=trend_conf, alignment_score=0.0)
            _, live_sell = bot._get_dynamic_rsi_thresholds(regime, regime_conf)
            _, live_sell = bot._apply_trend_confirmation_to_thresholds(
                rsi_buy_eff=32.0, rsi_sell_eff=live_sell, trend_confirmation=trend)
            ours = sell_signal_threshold(78.0, regime_label=regime, regime_confidence=regime_conf,
                                         trend_direction=direction.value, trend_confidence=trend_conf)
            self.assertAlmostEqual(live_sell, ours, places=12, msg=f"case={case}")
            rsi, rsi_prev = rng.uniform(60, 99), rng.uniform(60, 99)
            ema = float(price) * (1 + rng.gauss(0, 0.003))
            macd, macd_prev = rng.gauss(0, 1), rng.gauss(0, 1)
            bot.processed_data = {"signal": 0, "signal_state": {"rsi": rsi, "rsi_prev": rsi_prev, "ema_fast": ema},
                                  "trend_confirmation": {"direction": direction.value, "confidence": trend_conf}}
            live = bot._determine_signal(rsi, 0, 32.0, live_sell, float(price), rsi_prev=rsi_prev, ema_fast=ema,
                                         macd_hist=macd, macd_hist_prev=macd_prev)
            strong_up = direction == TrendDirection.UP and trend_conf >= bot.config.sell_trend_hold_min_confidence
            component = bag_sell_signal(rsi=rsi, rsi_prev=rsi_prev, sell_threshold=ours, price=price, ema_fast=ema,
                                        macd_hist=macd, macd_hist_prev=macd_prev,
                                        inventory_net_pnl=net_pnl_pct(amount, breakeven, fees, price),
                                        strong_bullish_trend=strong_up, trail_reversal_ready=False, policy=policy)
            self.assertEqual(live == -1, component, f"case={case}")
            fired += component
        self.assertGreater(fired, 50)


class ChaserParityTest(unittest.TestCase):
    def test_chaser_price_and_refresh_match_order_executor(self):
        import random

        from hummingbot.strategy_v2.components.held_inventory import chaser_needs_refresh, chaser_sell_price
        from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, LimitChaserConfig
        from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor

        rng = random.Random(3)
        for case in range(500):
            market = D(str(round(rng.uniform(500, 800), 2)))
            floor = D(str(round(rng.uniform(480, 820), 4))) if rng.random() < 0.8 else None
            rate = D(str(rng.choice([0, 0.0008, 0.001])))
            fee = SimpleNamespace(percent=rate, fee_amount_in_token=lambda pair, price, amount, token, exchange: price * amount * rate)
            config = SimpleNamespace(execution_strategy=ExecutionStrategy.LIMIT_CHASER, side=TradeType.SELL,
                                     chaser_config=LimitChaserConfig(distance=D("0.002"), refresh_threshold=D("0.003")),
                                     min_sell_price=floor, amount=D("0.6"), connector_name="okx", trading_pair="BNB-USDC")
            fake = SimpleNamespace(config=config, current_market_price=market,
                                   connectors={"okx": SimpleNamespace(get_fee=lambda **kwargs: fee)},
                                   _all_orders=lambda: [], get_order_type=lambda: None,
                                   get_trading_rules=lambda connector, pair: SimpleNamespace(min_price_increment=D("0.01")))
            fake._unbounded_order_price = lambda f=fake: OrderExecutor._unbounded_order_price(f)
            fake._to_price_tick = OrderExecutor._to_price_tick
            live = OrderExecutor.get_order_price(fake)
            ours = chaser_sell_price(market, distance=D("0.002"), min_sell_price=floor, fee_rate=rate, tick=D("0.01"))
            self.assertEqual(live, ours, f"case={case}")
            order_price = D(str(round(float(ours) * rng.uniform(0.99, 1.01), 2)))
            live_refresh = abs(live - order_price) > market * config.chaser_config.refresh_threshold
            self.assertEqual(live_refresh, chaser_needs_refresh(order_price, ours, market, D("0.003")))


class EarlyStopAndStaleCancelParityTest(unittest.TestCase):
    def test_early_stop_matches_live_drawdown_stop_on_fill_basis(self):
        import random

        from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

        helper = OKRSIRecoveryParityTest()
        policy = BagRecoveryPolicy()
        rng = random.Random(11)
        stops = 0
        for case in range(400):
            bot, provider = helper._bot()
            fill = D(str(round(rng.uniform(550, 700), 2)))
            price = (fill * D(str(rng.uniform(0.95, 1.01)))).quantize(D("0.01"))
            executor = SimpleNamespace(
                id=f"buy{case}", controller_id=bot.config.id, connector_name="okx", trading_pair="BNB-USDC",
                side=TradeType.BUY, is_active=True, is_done=False, type="position_executor", timestamp=1.0,
                filled_amount_quote=fill * D("0.3"), net_amount_quote=fill * D("0.3"),
                config=SimpleNamespace(side=TradeType.BUY, entry_price=fill * D("1.002"), amount=D("0.3"),
                                       connector_name="okx", trading_pair="BNB-USDC", price=None),
                custom_info={"current_position_average_price": str(fill), "held_position_orders": [],
                             "open_order_filled_amount_base": "0.3", "open_order_filled_amount_quote": str(fill * D("0.3"))})
            bot.executors_info = [executor]
            actions = bot._check_active_executor_drawdown_stop(price)
            live = any(isinstance(a, StopExecutorAction) and a.keep_position for a in actions)
            self.assertEqual(live, should_hold_as_bag(fill, price, policy), f"case={case} fill={fill} price={price}")
            stops += live
        self.assertGreater(stops, 50)

    def test_stale_cancel_matches_live(self):
        from hummingbot.strategy_v2.components.held_inventory import (
            RecoveryTrail,
            sell_trend_hold,
            stale_recovery_sell,
            trail_reversal,
        )
        from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

        helper = OKRSIRecoveryParityTest()
        for strong_up in (True, False):
            for move in (D("1.004"), D("1.005"), D("1.02")):
                bot, provider = helper._bot()
                proposal = D("650.00")
                price = proposal * move
                bot.processed_data = {"signal": 0, "signal_state": {"rsi": 80.0, "rsi_prev": 79.0, "ema_fast": 600.0},
                                      "trend_confirmation": {"direction": "up" if strong_up else "range",
                                                             "confidence": 0.9 if strong_up else 0.0}}
                bot.executors_info = [SimpleNamespace(
                    id="sell", is_active=True, connector_name="okx", trading_pair="BNB-USDC", side=TradeType.SELL,
                    type="order_executor", config=SimpleNamespace(price=proposal), custom_info={"level_id": "recovery_exit:k"})]
                for reversing in (False, True):
                    bot._recovery_trails = {}
                    trail = None
                    if reversing:  # a frozen trail whose peak is 1% above: pullback reversal ready
                        from controllers.directional_trading.hl_rsi import RecoveryTrailState
                        peak = price * D("1.01")
                        bot._recovery_trails = {"k": RecoveryTrailState(
                            position_key="k", armed_timestamp=0.0, arm_price=peak, peak_price=peak,
                            tracked_amount=D("1"), peak_rsi=80.0, target_profit_pct=D("0.04"), last_reason="armed")}
                        trail = RecoveryTrail(D("600"), peak, peak, 80.0, D("1"))
                    live = any(isinstance(a, StopExecutorAction)
                               for a in bot._maybe_cancel_stale_recovery_sell_executors(price))
                    ready = any(trail_reversal(trail, price, 80.0, 600.0, BagRecoveryPolicy()))
                    hold = sell_trend_hold(strong_up, price, 80.0, 79.0, 600.0, bot.config.sell_rsi_rollover_delta,
                                           trail_reversal_ready=ready)
                    self.assertEqual(live, stale_recovery_sell(hold, price, proposal, D("0.005")),
                                     (strong_up, move, reversing))
