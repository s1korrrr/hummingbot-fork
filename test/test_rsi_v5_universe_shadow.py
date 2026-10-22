import importlib.util
import sys
import types
from pathlib import Path


def _install_package(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        module.__path__ = []
        sys.modules[name] = module
    return module


def _load_module(path: Path, module_name: str):
    parts = module_name.split(".")
    for index in range(1, len(parts)):
        _install_package(".".join(parts[:index]))
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


# The real (empty) parent packages, never empty-path stand-ins: a stand-in installed at
# collection time would shadow ``controllers`` for every later import in the process.
import controllers.directional_trading  # noqa: E402,F401

ROOT = Path(__file__).resolve().parents[1]
_load_module(
    ROOT / "controllers" / "directional_trading" / "rsi_v55_risk.py",
    "controllers.directional_trading.rsi_v55_risk",
)
shadow = _load_module(
    ROOT / "controllers" / "directional_trading" / "rsi_v5_universe_shadow.py",
    "controllers.directional_trading.rsi_v5_universe_shadow",
)


def _config():
    return shadow.RSIv5UniverseShadowControllerConfig()


def _portfolio(**overrides):
    payload = {
        "quote_balance": 1000.0,
        "available_quote": 900.0,
        "active_positions": 1,
        "active_quote_exposure": 100.0,
        "pair_exposure_quote": {"BNB-USDT": 100.0},
        "held_bag_count": 0,
        "cooldown_pairs": [],
        "kill_switch": False,
    }
    payload.update(overrides)
    return shadow.PortfolioDecisionState(**payload)


def _composite(**overrides):
    payload = {
        "expected_return_short": 0.001,
        "expected_return_medium": 0.0015,
        "expected_return_long": 0.0025,
        "adverse_excursion": -0.012,
        "confidence": 0.62,
        "coverage_pct": 92.0,
        "fresh_count": 184,
        "fallback_count": 0,
        "stale_count": 0,
        "missing_count": 0,
        "generated_at": 1_777_000_000.0,
        "feature_cutoff": 1_776_999_940.0,
        "provider": "fixture",
    }
    payload.update(overrides)
    return shadow.KronosCompositeState(**payload)


def _pair(**overrides):
    payload = {
        "pair": "BNB-USDT",
        "timestamp": 1_777_000_060.0,
        "action": "buy",
        "score": 84.0,
        "signal_score": 4,
        "signal_state": {
            "buy_entry_role": "full",
            "sell_profitability_ok": True,
            "sell_trend_hold": True,
        },
        "screener_rank": 1,
        "ranking_score": 0.91,
        "blockers": [],
        "size_multiplier": 1.0,
        "base_entry_fraction": 1.0,
        "buy_entry_role": "full",
    }
    payload.update(overrides)
    return shadow.UniversePairState(**payload)


def test_classifies_composite_regime_defaults():
    config = _config()
    portfolio = _portfolio()

    assert shadow.classify_kronos_composite(_composite(), portfolio, config).risk_mode == "supportive"
    assert (
        shadow.classify_kronos_composite(
            _composite(expected_return_long=-0.003, adverse_excursion=-0.012),
            portfolio,
            config,
        ).risk_mode
        == "hostile"
    )
    assert (
        shadow.classify_kronos_composite(
            _composite(expected_return_long=0.0005, adverse_excursion=-0.015, confidence=0.50),
            portfolio,
            config,
        ).risk_mode
        == "neutral"
    )


def test_stale_missing_or_fallback_kronos_fails_closed():
    config = _config()
    portfolio = _portfolio()

    for composite in (
        _composite(stale=True, stale_count=1),
        _composite(missing=True, missing_count=1),
        _composite(is_fallback=True, fallback_count=1),
        _composite(coverage_pct=60.0),
        _composite(confidence=0.20),
    ):
        decision = shadow.classify_kronos_composite(composite, portfolio, config)
        assert decision.risk_mode == "defensive"
        assert "kronos_defensive" in decision.blockers


def test_v55_risk_decision_controls_budget_cap_and_trailing_profile():
    config = _config()
    pair = _pair()
    portfolio = _portfolio()

    supportive = shadow.build_universe_decision(
        pair=pair,
        composite=_composite(),
        portfolio=portfolio,
        config=config,
        variant="composite_risk_trailing",
    )
    assert supportive.risk_mode == "supportive"
    assert supportive.action == "buy"
    assert supportive.entry_budget_usd == 34.5
    assert supportive.effective_max_position_usd == 500.0
    assert supportive.trailing_profile == "loose"

    hostile = shadow.build_universe_decision(
        pair=_pair(buy_entry_role="runner", signal_state={"buy_entry_role": "runner"}),
        composite=_composite(expected_return_long=-0.004),
        portfolio=portfolio,
        config=config,
        variant="composite_exit_pressure",
    )
    assert hostile.risk_mode == "hostile"
    assert hostile.action == "hold"
    assert hostile.allow_runner_add is False
    assert hostile.effective_max_position_usd == 375.0
    assert hostile.held_bag_exit_pressure == "elevated"
    assert "risk_runner_block" in hostile.blockers


def test_backtest_shadow_controller_never_emits_executor_actions():
    controller = shadow.RSIv5UniverseShadowController(_config())
    decisions = controller.evaluate(
        pairs=[_pair()],
        composite=_composite(),
        portfolio=_portfolio(),
        variant="composite_risk_v55",
    )

    assert len(decisions) == 1
    assert decisions[0].action == "buy"
    assert controller.determine_executor_actions() == []
