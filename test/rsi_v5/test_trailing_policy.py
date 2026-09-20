import unittest
from decimal import Decimal

from controllers.directional_trading.ok_rsi_core import trailing as ok_rsi_trailing
from controllers.shared import trailing as shared_trailing


class TrailingPolicyCharacterizationTest(unittest.TestCase):
    @staticmethod
    def _selection(policy):
        return policy(
            atr_pct=0.004,
            baseline_atr_pct=0.002,
            base_activation=Decimal("0.015"),
            base_delta=Decimal("0.0035"),
            activation_min=Decimal("0.003"),
            activation_max=Decimal("0.05"),
            delta_min=Decimal("0.001"),
            delta_max=Decimal("0.02"),
            min_locked_profit=Decimal("0.004"),
            activation_exponent=0.70,
            delta_exponent=0.35,
            min_multiplier=0.5,
            max_multiplier=2.5,
        )

    def test_existing_continuous_adaptive_selection_is_stable(self):
        selection = self._selection(ok_rsi_trailing.calculate_continuous_adaptive_trailing)

        self.assertEqual(selection.activation, Decimal("0.024367571890687065"))
        self.assertEqual(selection.delta, Decimal("0.00446096219561741770"))
        self.assertEqual(selection.volatility_ratio, Decimal("2.0"))
        self.assertEqual(selection.initial_locked_profit, Decimal("0.01990660969506964730"))
        self.assertEqual(selection.policy, "adaptive_continuous")

    def test_ok_rsi_import_path_reexports_the_shared_policy_exactly(self):
        self.assertIs(
            ok_rsi_trailing.ContinuousTrailingSelection,
            shared_trailing.ContinuousTrailingSelection,
        )
        self.assertIs(
            ok_rsi_trailing.calculate_continuous_adaptive_trailing,
            shared_trailing.calculate_continuous_adaptive_trailing,
        )
        self.assertEqual(
            self._selection(ok_rsi_trailing.calculate_continuous_adaptive_trailing),
            self._selection(shared_trailing.calculate_continuous_adaptive_trailing),
        )

    def test_existing_policy_rejects_impossible_locked_profit_bounds(self):
        with self.assertRaisesRegex(ValueError, "minimum locked profit"):
            ok_rsi_trailing.calculate_continuous_adaptive_trailing(
                atr_pct=0.002,
                baseline_atr_pct=0.002,
                base_activation=Decimal("0.005"),
                base_delta=Decimal("0.004"),
                activation_min=Decimal("0.003"),
                activation_max=Decimal("0.005"),
                delta_min=Decimal("0.001"),
                delta_max=Decimal("0.0045"),
                min_locked_profit=Decimal("0.004"),
                activation_exponent=0.70,
                delta_exponent=0.35,
                min_multiplier=0.5,
                max_multiplier=2.5,
            )


if __name__ == "__main__":
    unittest.main()
