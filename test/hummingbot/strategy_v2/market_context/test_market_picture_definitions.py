import pytest

from hummingbot.strategy_v2.market_context.market_picture.definitions import (
    DEFINITIONS,
    HORIZONS_MINUTES,
    PARTICIPATION_PREDICATES,
    REGISTRY_HASH,
    definition_for_metric,
    definition_ref_for_metric,
    get_definition,
)


def test_definition_registry_is_versioned_complete_and_deterministic():
    assert HORIZONS_MINUTES == (1, 5, 15, 60, 240, 1440)
    assert set(PARTICIPATION_PREDICATES) <= DEFINITIONS.keys()
    assert get_definition("pearson_log_1h_90d").parameters == (
        ("history_hours", 2160),
        ("interval_minutes", 60),
        ("min_coverage", 0.95),
        ("min_pairs", 60),
        ("no_gap_spanning_return", True),
    )
    assert len(REGISTRY_HASH) == 64
    assert (
        REGISTRY_HASH
        == __import__(
            "hummingbot.strategy_v2.market_context.market_picture.definitions", fromlist=["REGISTRY_HASH"]
        ).REGISTRY_HASH
    )
    assert definition_for_metric("return_10080m").definition_id == "return_pct"
    assert definition_ref_for_metric("price", code_hash="a" * 64).unit == "price_quote"


def test_unknown_metric_definition_does_not_fallback():
    with pytest.raises(ValueError):
        get_definition("legacy_or_unknown")
