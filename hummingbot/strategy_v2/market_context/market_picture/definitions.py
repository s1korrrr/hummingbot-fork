"""Versioned Market Picture metric definitions; labels alone never identify a formula."""

from dataclasses import dataclass
from hashlib import sha256

from hummingbot.strategy_v2.components.recipe_contract import canonical_json

from .contracts import DefinitionRefV1
from .metrics import HORIZONS_MINUTES

FEATURE_SET_VERSION = "market-picture.observation.v1"
REGISTRY_VERSION = "market-picture.definitions.v1"
RETURN_HORIZONS = HORIZONS_MINUTES
PARTICIPATION_PREDICATES = (
    "above_ema21",
    "rsi_above_50",
    "rsi_above_70",
    "rsi_below_30",
    "atr14_compression_p20",
    "adx14_above_25",
    "rvol20_above_1_5",
    "rv24h_above_80pct",
)


@dataclass(frozen=True)
class MetricDefinition:
    definition_id: str
    definition_version: str
    unit: str
    parameters: tuple[tuple[str, object], ...]
    description: str

    @property
    def parameter_hash(self) -> str:
        return sha256(canonical_json(dict(self.parameters)).encode()).hexdigest()

    @property
    def definition_hash(self) -> str:
        return sha256(
            canonical_json(
                {
                    "definition_id": self.definition_id,
                    "definition_version": self.definition_version,
                    "unit": self.unit,
                    "parameters": dict(self.parameters),
                }
            ).encode()
        ).hexdigest()


def _definition(definition_id: str, unit: str, parameters: dict, description: str) -> MetricDefinition:
    return MetricDefinition(definition_id, "1.0.0", unit, tuple(sorted(parameters.items())), description)


DEFINITIONS = {
    "return_pct": _definition(
        "return_pct",
        "return_percent",
        {"formula": "100*(close_t/close_t_minus_h-1)", "closed_bars": True},
        "Simple close-to-close percent return.",
    ),
    "price_close": _definition(
        "price_close",
        "price_quote",
        {"source": "latest-confirmed-closed-minute"},
        "Last confirmed close in the declared quote numeraire.",
    ),
    "breadth_counts": _definition(
        "breadth_counts",
        "count",
        {"sign": "unrounded_return", "zero": "unchanged"},
        "Advance, decline and unchanged counts over a pinned eligible cohort.",
    ),
    "breadth_instrument_count": _definition(
        "breadth_instrument_count",
        "instruments",
        {"unit_semantics": "integer-instrument-count", "zero_when_empty": True},
        "Count of instruments in the pinned eligible or valid breadth cohort.",
    ),
    "breadth_share": _definition(
        "breadth_share",
        "share_fraction",
        {
            "formula": "category_count/eligible_count",
            "categories": ["advance", "decline", "unchanged"],
            "zero_denominator": "unavailable",
        },
        "Descriptive advance, decline or unchanged share of the pinned eligible cohort.",
    ),
    "instrument_coverage": _definition(
        "instrument_coverage",
        "share_fraction",
        {"formula": "valid_count/eligible_count", "zero_denominator": "unavailable"},
        "Fraction of the pinned eligible cohort with valid observations.",
    ),
    "breadth_return_mean": _definition(
        "breadth_return_mean",
        "return_percent",
        {"aggregation": "arithmetic_mean", "cohort": "valid_horizon_returns"},
        "Mean same-horizon arithmetic return across valid cohort members.",
    ),
    "breadth_return_median": _definition(
        "breadth_return_median",
        "return_percent",
        {"aggregation": "median", "even_sample": "midpoint", "cohort": "valid_horizon_returns"},
        "Median same-horizon arithmetic return across valid cohort members.",
    ),
    "breadth_downside_magnitude": _definition(
        "breadth_downside_magnitude",
        "return_percent",
        {"formula": "max(-mean_return_percent,0)", "cohort": "valid_horizon_returns"},
        "Magnitude of negative mean cohort return, or zero when the mean is nonnegative.",
    ),
    "breadth_return_dispersion": _definition(
        "breadth_return_dispersion",
        "percentage_points",
        {
            "aggregation": "population_standard_deviation",
            "ddof": 0,
            "minimum_samples": 2,
            "cohort": "valid_horizon_returns",
        },
        "Population standard deviation of same-horizon returns in percentage points.",
    ),
    "breadth_pressure": _definition(
        "breadth_pressure",
        "index_minus3_to3",
        {
            "formula": "3*(advances-declines)/valid_count",
            "range": [-3, 3],
            "decimal_precision": 28,
            "rounding": "ROUND_HALF_EVEN",
            "serialization": "fixed-strip-trailing-zero",
        },
        "Descriptive net-breadth pressure, not flow or a forecast.",
    ),
    "breadth_pressure_delta": _definition(
        "breadth_pressure_delta",
        "index_points",
        {"formula": "current_pressure-previous_pressure", "range": [-6, 6], "matched_cohort": True},
        "Matched-cohort breadth pressure change in index points.",
    ),
    "rvol_delta": _definition(
        "rvol_delta",
        "ratio_points",
        {"formula": "current_rvol-previous_rvol", "matched_cohort": True},
        "Matched-cohort relative-volume change in ratio points.",
    ),
    "share_delta": _definition(
        "share_delta",
        "percentage_points",
        {"formula": "100*(current_share-previous_share)", "matched_cohort": True, "comparison_horizon": "1h"},
        "Matched-cohort one-hour change in participation share, in percentage points.",
    ),
    "volatility_delta": _definition(
        "volatility_delta",
        "percentage_points",
        {"formula": "100*(current_median_rv-previous_median_rv)", "matched_cohort": True, "comparison_horizon": "1h"},
        "Matched-cohort one-hour change in median realized volatility, in percentage points.",
    ),
    "trend_strength_delta": _definition(
        "trend_strength_delta",
        "index_points",
        {"formula": "current_mean_adx-previous_mean_adx", "matched_cohort": True, "comparison_horizon": "1h"},
        "Matched-cohort one-hour change in mean ADX trend strength, in index points.",
    ),
    "breadth_pressure_band": _definition(
        "breadth_pressure_band",
        "count",
        {"edges": [-3, -1.5, -0.5, 0.5, 1.5, 3], "policy": "named-v1"},
        "Display bands for the unsmoothed breadth-pressure value.",
    ),
    "rvol20": _definition(
        "rvol20",
        "ratio",
        {"baseline_bars": 20, "current_excluded": True},
        "Current quote volume divided by preceding 20 same-interval bars mean.",
    ),
    "rvol24h": _definition(
        "rvol24h",
        "ratio",
        {"baseline_bars": 1440, "current_excluded": True},
        "Current minute quote volume divided by prior 24-hour mean minute quote volume.",
    ),
    "rv24h_annualized": _definition(
        "rv24h_annualized",
        "volatility_fraction_annualized",
        {"returns": 1440, "log": True, "ddof": 1, "periods_per_year": 525600},
        "Annualized sample volatility of 1,440 contiguous minute log returns.",
    ),
    "ema21": _definition(
        "ema21",
        "price_quote",
        {"length": 21, "seed": "first-close", "adjust": False},
        "Causal EMA21 of confirmed closes.",
    ),
    "rsi14_wilder": _definition(
        "rsi14_wilder",
        "rsi_0_100",
        {"length": 14, "seed": "initial-arithmetic-mean", "update": "wilder"},
        "Wilder RSI14, with explicit seed and zero-gain/loss behavior.",
    ),
    "atr14_wilder": _definition(
        "atr14_wilder",
        "price_quote",
        {"length": 14, "seed": "initial-arithmetic-mean", "update": "wilder"},
        "Wilder ATR14 from true range.",
    ),
    "atr14_percent": _definition(
        "atr14_percent", "atr_percent", {"formula": "100*atr14/close"}, "ATR14 normalized by close in percent."
    ),
    "atr14_percentile_200": _definition(
        "atr14_percentile_200",
        "percentile_0_100",
        {"history": 200, "current_excluded": True, "ties": "midrank"},
        "Current ATR14 percent rank against preceding 200 valid values.",
    ),
    "adx14_wilder": _definition(
        "adx14_wilder",
        "adx_0_100",
        {"length": 14, "seed": "wilder-sma", "directional": False},
        "Wilder ADX14 trend strength; not direction.",
    ),
    "di_plus14": _definition(
        "di_plus14", "percent", {"length": 14, "seed": "wilder-sma"}, "Wilder positive directional index."
    ),
    "di_minus14": _definition(
        "di_minus14", "percent", {"length": 14, "seed": "wilder-sma"}, "Wilder negative directional index."
    ),
    "pearson_log_1h_90d": _definition(
        "pearson_log_1h_90d",
        "correlation_minus1_to1",
        {
            "interval_minutes": 60,
            "history_hours": 2160,
            "min_coverage": 0.95,
            "min_pairs": 60,
            "no_gap_spanning_return": True,
        },
        "Pearson correlation of paired completed 1h log returns over 90 days.",
    ),
    "high_break_24h": _definition(
        "high_break_24h",
        "count",
        {"event_bar": "latest-closed-1m-high", "prior_bars": 1440, "strict": True},
        "New high versus prior 1,440 closed minute highs.",
    ),
    "low_break_24h": _definition(
        "low_break_24h",
        "count",
        {"event_bar": "latest-closed-1m-low", "prior_bars": 1440, "strict": True},
        "New low versus prior 1,440 closed minute lows.",
    ),
    "high_break_7d": _definition(
        "high_break_7d",
        "count",
        {"event_bar": "latest-closed-1m-high", "prior_bars": 10080, "strict": True},
        "New high versus prior seven days of closed minute highs.",
    ),
    "low_break_7d": _definition(
        "low_break_7d",
        "count",
        {"event_bar": "latest-closed-1m-low", "prior_bars": 10080, "strict": True},
        "New low versus prior seven days of closed minute lows.",
    ),
    "high_break_10d_1h": _definition(
        "high_break_10d_1h",
        "count",
        {"event_bar": "latest-closed-1h-close", "prior_bars": 240, "strict": True},
        "Latest completed hourly close breaks prior 240 completed hourly highs.",
    ),
    "high_low_52w": _definition(
        "high_low_52w",
        "count",
        {"event_bar": "latest-completed-utc-day", "prior_days": 364, "strict": True},
        "Completed-day high/low break of previous 364 UTC days.",
    ),
    "relative_return": _definition(
        "relative_return",
        "percentage_points",
        {"formula": "asset_return_pct-benchmark_return_pct", "matched_horizon": True},
        "Same-horizon arithmetic relative return in percentage points.",
    ),
    "leaders_laggards": _definition(
        "leaders_laggards",
        "return_percent",
        {"leader_count": 5, "laggard_count": 5, "ties": "instrument_id"},
        "Stable sorting of positive leaders and negative laggards.",
    ),
}

for _name in PARTICIPATION_PREDICATES:
    _parameters = {
        "above_ema21": {"threshold": "close>ema21", "strict": True},
        "rsi_above_50": {"threshold": "rsi14>50", "strict": True},
        "rsi_above_70": {"threshold": "rsi14>70", "strict": True},
        "rsi_below_30": {"threshold": "rsi14<30", "strict": True},
        "atr14_compression_p20": {"threshold": "atr14_percentile_200<=20", "strict_rank": False},
        "adx14_above_25": {"threshold": "adx14>25", "strict": True},
        "rvol20_above_1_5": {"threshold": "rvol20>1.5", "strict": True},
        "rv24h_above_80pct": {"threshold": "rv24h_annualized>0.8", "strict": True},
    }[_name]
    DEFINITIONS[_name] = _definition(
        _name, "share_fraction", _parameters, "Overlapping descriptive participation predicate."
    )
    DEFINITIONS[f"{_name}_membership"] = _definition(
        f"{_name}_membership",
        "count",
        {**_parameters, "encoding": "binary-0-or-1"},
        "Per-instrument binary membership in the named descriptive predicate.",
    )

REGISTRY_HASH = sha256(
    canonical_json({key: value.definition_hash for key, value in sorted(DEFINITIONS.items())}).encode()
).hexdigest()


def get_definition(definition_id: str) -> MetricDefinition:
    try:
        return DEFINITIONS[definition_id]
    except KeyError as exc:
        raise ValueError(f"unknown market-picture definition: {definition_id}") from exc


def definition_for_metric(metric_id: str) -> MetricDefinition:
    """Resolve the canonical definition for frozen engine asset-metric IDs."""
    if metric_id == "price":
        return get_definition("price_close")
    if metric_id.startswith("return_"):
        return get_definition("return_pct")
    if metric_id.startswith("relative_24h_"):
        return get_definition("relative_return")
    mapping = {
        "ema21": "ema21",
        "rsi14": "rsi14_wilder",
        "adx14": "adx14_wilder",
        "atr14_percent": "atr14_percent",
        "atr14_percentile": "atr14_percentile_200",
        "rvol20": "rvol20",
        "rvol_24h": "rvol24h",
        "realized_volatility_24h": "rv24h_annualized",
    }
    try:
        return get_definition(mapping[metric_id])
    except KeyError as exc:
        raise ValueError(f"unknown metric definition for: {metric_id}") from exc


def definition_ref_for_metric(metric_id: str, *, code_hash: str) -> DefinitionRefV1:
    definition = definition_for_metric(metric_id)
    return DefinitionRefV1(
        definition_id=definition.definition_id,
        definition_version=definition.definition_version,
        parameter_hash=definition.parameter_hash,
        code_hash=code_hash,
        unit=definition.unit,
    )
