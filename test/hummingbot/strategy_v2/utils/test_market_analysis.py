import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
