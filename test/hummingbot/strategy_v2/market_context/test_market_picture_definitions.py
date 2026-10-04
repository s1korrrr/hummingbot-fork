import pytest

from hummingbot.strategy_v2.market_context.market_picture.definitions import (
    DEFINITIONS,
    HORIZONS_MINUTES,
    PARTICIPATION_PREDICATES,
    REGISTRY_HASH,
    correlation_definition_id,
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


def test_hourly_relative_volume_definitions_are_distinct_from_the_single_minute_ones():
    hourly = get_definition("rvol1h")
    assert (hourly.definition_version, hourly.unit) == ("1.0.0", "ratio")
    parameters = dict(hourly.parameters)
    assert (parameters["current_minutes"], parameters["baseline_hours"], parameters["required_minutes"]) == (60, 24, 1500)
    assert parameters["zero_baseline"] == "unavailable"
    assert hourly.parameter_hash != get_definition("rvol24h").parameter_hash
    aggregate = get_definition("median_relative_volume_1h")
    assert dict(aggregate.parameters)["source_metric"] == "rvol_1h"
    assert aggregate.parameter_hash != get_definition("median_relative_volume_24h").parameter_hash
    assert definition_for_metric("rvol_1h").definition_id == "rvol1h"
    assert definition_for_metric("rvol_24h").definition_id == "rvol24h"  # legacy id stays resolvable


def test_each_correlation_window_has_its_own_definition_and_the_90d_identity_is_unchanged():
    assert correlation_definition_id(2160) == "pearson_log_1h_90d"
    assert get_definition("pearson_log_1h_90d").parameter_hash == (
        "5f9d9afc68947663f937069ce9a013ba4e419e353b0792398cec108908eabfeb"  # noqa: mock (definition registry hash)
    )
    hashes = set()
    for hours in (2160, 720, 336, 168, 72, 24):
        definition = get_definition(correlation_definition_id(hours))
        assert dict(definition.parameters)["history_hours"] == hours
        assert dict(definition.parameters)["min_coverage"] == 0.95
        hashes.add(definition.parameter_hash)
    assert len(hashes) == 6
    assert dict(get_definition("pearson_log_1h_24h").parameters)["min_pairs"] == 23
    with pytest.raises(ValueError):
        correlation_definition_id(48)
