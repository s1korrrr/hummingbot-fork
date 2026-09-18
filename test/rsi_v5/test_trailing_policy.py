import unittest
from decimal import Decimal

from controllers.directional_trading.ok_rsi_core.trailing import calculate_continuous_adaptive_trailing


class TrailingPolicyCharacterizationTest(unittest.TestCase):
    def test_existing_continuous_adaptive_selection_is_stable(self):
        selection = calculate_continuous_adaptive_trailing(
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

        self.assertEqual(selection.activation, Decimal("0.024367571890687065"))
        self.assertEqual(selection.delta, Decimal("0.00446096219561741770"))
        self.assertEqual(selection.volatility_ratio, Decimal("2.0"))
        self.assertEqual(selection.initial_locked_profit, Decimal("0.01990660969506964730"))
        self.assertEqual(selection.policy, "adaptive_continuous")

    def test_existing_policy_rejects_impossible_locked_profit_bounds(self):
        with self.assertRaisesRegex(ValueError, "minimum locked profit"):
            calculate_continuous_adaptive_trailing(
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
