import unittest
from unittest.mock import patch

import pandas as pd

from hummingbot.strategy_v2.utils import market_analysis as market_analysis_module


class MarketAnalysisStrictHMMTest(unittest.TestCase):
    def test_strict_hmm_mode_requires_real_backend(self):
        with patch.object(market_analysis_module, "GaussianHMM", None):
            with self.assertRaises(market_analysis_module.StrictHMMRequiredError):
                market_analysis_module.MarketRegimeDetector(
                    use_hmm=True,
                    require_hmm=True,
                    allow_rule_based_fallback=False,
                )

    def test_analyze_volume_returns_neutral_for_non_finite_latest_volume(self):
        df = pd.DataFrame({
            "high": [11] * 20,
            "low": [9] * 20,
            "close": [10] * 20,
            "volume": [100] * 19 + [float("nan")],
        })

        result = market_analysis_module.analyze_volume(df, lookback=20)

        self.assertEqual(result.current_volume, 0.0)
        self.assertEqual(result.volume_ratio, 1.0)
        self.assertEqual(result.condition, market_analysis_module.VolumeCondition.NORMAL)

    def test_detect_support_resistance_missing_close_returns_default(self):
        df = pd.DataFrame({
            "high": [11] * 20,
            "low": [9] * 20,
        })

        result = market_analysis_module.detect_support_resistance(df)

        self.assertIsNone(result.nearest_support)
        self.assertIsNone(result.nearest_resistance)
        self.assertEqual(result.price_position, 0.5)


if __name__ == "__main__":
    unittest.main()
