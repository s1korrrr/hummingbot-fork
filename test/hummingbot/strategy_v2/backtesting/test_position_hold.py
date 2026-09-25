import unittest
from decimal import Decimal

import numpy as np
import pandas as pd

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.backtesting.backtesting_engine_base import BacktestingEngineBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType

D = Decimal


def _frame(prices):
    # The pinned simulator slices integer labels positionally; use positional stamps.
    stamps = np.arange(len(prices))
    return pd.DataFrame({"timestamp": stamps, "open": prices, "high": prices, "low": prices, "close": prices,
                         "volume": 1.0}, index=stamps)


def _config(identifier="tranche", side=TradeType.BUY):
    return PositionExecutorConfig(
        id=identifier, controller_id="c", timestamp=0, connector_name="binance",
        trading_pair="BNB-USDC", side=side, amount=D("1"), entry_price=D("100"),
        triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET))


class PositionHoldBacktestTest(unittest.TestCase):
    def _engine(self, prices, config):
        engine = BacktestingEngineBase()
        engine.stopped_executors_info = []
        simulation = engine.position_executor_simulator.simulate(_frame(prices), config, 0.003)
        engine.active_executor_simulations = [simulation]
        return engine

    def test_keep_position_holds_inventory_frees_slot_and_preserves_equity(self):
        engine = self._engine([100.0, 98.0, 96.0, 95.0, 99.0, 104.0], _config())
        stop_at = 3
        before = engine.active_executor_simulations[0].get_executor_info_at_timestamp(stop_at)
        engine.handle_stop_action(StopExecutorAction(controller_id="c", executor_id="tranche", keep_position=True),
                                  stop_at)
        self.assertEqual(engine.active_executor_simulations, [])
        info = engine.stopped_executors_info[-1]
        self.assertEqual(info.close_type, CloseType.POSITION_HOLD)
        self.assertEqual((info.net_pnl_quote, info.cum_fees_quote), (D(0), D(0)))
        held = engine.held_positions[("binance", "BNB-USDC")]
        self.assertEqual((held.amount, held.breakeven_price), (D("1"), D("100")))
        key = ("binance", "BNB-USDC")
        self.assertAlmostEqual(float(engine.held_positions_pnl_quote({key: D("95")})), float(before.net_pnl_quote), 9)
        engine.sell_held_position("binance", "BNB-USDC", D("0.4"), D("104"))
        self.assertEqual(engine.held_positions[key].amount, D("0.6"))
        expected = D("0.4") * 4 + D("0.6") * 4 - before.cum_fees_quote
        self.assertEqual(engine.held_positions_pnl_quote({key: D("104")}), expected)

    def test_default_stop_still_realizes_and_holds_nothing(self):
        engine = self._engine([100.0, 98.0, 96.0, 95.0], _config())
        engine.handle_stop_action(StopExecutorAction(controller_id="c", executor_id="tranche"), 2)
        self.assertEqual(engine.stopped_executors_info[-1].close_type, CloseType.EARLY_STOP)
        self.assertEqual(engine.held_positions, {})

    def test_keep_position_on_already_closed_executor_holds_nothing(self):
        engine = self._engine([100.0, 98.0, 96.0], _config())
        engine.handle_stop_action(StopExecutorAction(controller_id="c", executor_id="tranche", keep_position=True), 5)
        self.assertEqual(engine.held_positions, {})

    def test_hold_rejects_short_side(self):
        engine = self._engine([100.0, 101.0, 102.0, 103.0], _config(side=TradeType.SELL))
        with self.assertRaisesRegex(ValueError, "long BUY"):
            engine.handle_stop_action(StopExecutorAction(controller_id="c", executor_id="tranche",
                                                         keep_position=True), 1)

    def test_two_holds_blend_into_one_position(self):
        engine = self._engine([100.0, 96.0, 90.0, 90.0], _config("a"))
        second = engine.position_executor_simulator.simulate(
            _frame([90.0, 90.0, 90.0, 90.0]), _config("b").model_copy(update={"entry_price": D("90")}), 0.003)
        engine.active_executor_simulations.append(second)
        for identifier in ("a", "b"):
            engine.handle_stop_action(StopExecutorAction(controller_id="c", executor_id=identifier,
                                                         keep_position=True), 1)
        held = engine.held_positions[("binance", "BNB-USDC")]
        self.assertEqual((held.amount, held.breakeven_price), (D("2"), D("95")))


if __name__ == "__main__":
    unittest.main()
