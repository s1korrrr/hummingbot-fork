"""None drawdown fields stay unlimited; $100 entries need funding le=101."""
from pathlib import Path

from scripts.v2_with_controllers import V2WithControllersConfig

ROOT = Path(__file__).resolve().parents[1]


def test_checkout_script_defaults_are_unlimited():
    config = V2WithControllersConfig.model_construct()
    assert config.max_global_drawdown_quote is None
    assert config.max_controller_drawdown_quote is None
    assert config.max_daily_loss_quote is None
    assert not config.max_global_drawdown_quote
    assert not config.max_controller_drawdown_quote


def test_modular_script_treats_missing_limits_as_unlimited():
    text = (ROOT / "scripts/v2_with_controllers.py").read_text()
    assert "max_daily_loss_quote: Optional[Decimal] = Field(default=None" in text
    assert "Native modular execution requires finite positive loss limits" not in text
    assert "Persisted daily loss limit was removed; explicit reconciliation required" not in text
    assert "finite and positive when set" in text


def test_funding_ceiling_allows_hundred_dollar_entry():
    text = (ROOT / "controllers/generic/modular_spot_core/native_funding.py").read_text()
    assert "allocation_quote: Decimal = Field(gt=0, le=101)" in text
    assert "max_position_quote: Decimal = Field(default=50, gt=0, le=101)" in text
    assert "le=50)" not in text
