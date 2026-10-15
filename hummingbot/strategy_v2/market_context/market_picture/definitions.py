"""Versioned Market Picture metric definitions; labels alone never identify a formula."""

from dataclasses import dataclass
from hashlib import sha256

from hummingbot.strategy_v2.components.recipe_contract import canonical_json

from .contracts import DefinitionRefV1
from .metrics import (
    CORRELATION_MIN_COVERAGE,
    CORRELATION_WINDOW_HOURS,
    HORIZONS_MINUTES,
    RVOL_1H_BASELINE_HOURS,
    RVOL_1H_BLOCK_MINUTES,
    RVOL_1H_REQUIRED_MINUTES,
    correlation_min_pairs,
)

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
    "return_distribution_40_bins": _definition(
        "return_distribution_40_bins",
        "return_percent",
        {
            "spans_percent": {"1": "0.5", "5": "1", "15": "2", "60": "3", "240": "5", "1440": "10"},
            "interior_bins": 40,
            "interior": "lower-inclusive-upper-exclusive",
            "last_interior_upper": "inclusive",
            "overflow": "strictly-outside-symmetric-span",
        },
        "Fixed symmetric empirical return bins per horizon, with two explicit overflow bins.",
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
    "breadth_pressure_1bp": _definition(
        "breadth_pressure_1bp",
        "index_minus1_to1",
        {
            "formula": "(advances-declines)/valid_count",
            "advance_if_return_gt": "0.0001",
            "decline_if_return_lt": "-0.0001",
            "otherwise": "unchanged",
            "missing": "excluded_from_valid_count",
            "range": [-1, 1],
            "return_unit": "fraction",
        },
        "Normalized breadth pressure using a distinct one-basis-point neutral band.",
    ),
    "breadth_pressure_change": _definition(
        "breadth_pressure_change",
        "index_points",
        {"formula": "pressure_t-pressure_t_minus_tau", "matched_cohort": True},
        "Matched-cohort normalized breadth pressure change.",
    ),
    "breadth_pressure_velocity": _definition(
        "breadth_pressure_velocity",
        "index_points_per_minute",
        {"formula": "(pressure_t-pressure_t_minus_tau)/tau_minutes", "matched_cohort": True},
        "Matched-cohort normalized breadth pressure change per minute.",
    ),
    "breadth_pressure_acceleration": _definition(
        "breadth_pressure_acceleration",
        "index_points_per_minute_squared",
        {
            "formula": "(pressure_t-2*pressure_t_minus_tau+pressure_t_minus_2tau)/tau_minutes^2",
            "matched_cohort": True,
            "requires_contiguous_endpoints": True,
        },
        "Matched-cohort normalized breadth pressure acceleration.",
    ),
    "breadth_price_divergence": _definition(
        "breadth_price_divergence",
        "count",
        {
            "rule": "new_confirmed_24h_close_high_and_pressure_change_negative_or_new_low_and_pressure_change_positive",
            "prior_range_minutes": 1440,
            "future_data": False,
            "descriptive_only": True,
        },
        "Causal price and matched breadth disagreement flag; not a pivot or forecast.",
    ),
    "contribution_top5_abs": _definition(
        "contribution_top5_abs",
        "share_fraction",
        {
            "formula": "sum(top5(abs(weight_i*return_i)))/sum(abs(weight_i*return_i))",
            "weight_source": "frozen_prior_interval_weights",
            "missing_positive_weight_member": "unavailable",
            "zero_gross_threshold": "1e-12",
        },
        "Top-five absolute return contribution share using frozen prior weights.",
    ),
    "cohort_equal_weight_return": _definition(
        "cohort_equal_weight_return",
        "return_percent",
        {
            "formula": "sum(r_i)/N_start",
            "weights": "equal_at_interval_start",
            "missing_positive_weight_member": "unavailable",
        },
        "Full frozen-cohort equal-weight return.",
    ),
    "cohort_capped_liquidity_return": _definition(
        "cohort_capped_liquidity_return",
        "return_percent",
        {
            "formula": "sum(w_i*r_i)",
            "weights": "previous_completed_utc_day_quote_turnover",
            "cap": "0.10",
            "redistribution": "iterative_proportional_uncapped",
            "minimum_positive_members": 10,
            "missing_positive_weight_member": "unavailable",
        },
        "Full frozen-cohort return with capped prior-day liquidity weights.",
    ),
    "return_concentration_abs_top5_equal": _definition(
        "return_concentration_abs_top5_equal",
        "share_fraction",
        {
            "formula": "sum(top5(abs(r_i/N)))/sum(abs(r_i/N))",
            "weights": "equal_at_interval_start",
            "zero_gross_threshold": "1e-12",
            "missing_positive_weight_member": "unavailable",
        },
        "Top-five absolute contribution share under equal start weights.",
    ),
    "return_concentration_abs_top5_capped_liquidity": _definition(
        "return_concentration_abs_top5_capped_liquidity",
        "share_fraction",
        {
            "formula": "sum(top5(abs(w_i*r_i)))/sum(abs(w_i*r_i))",
            "weights": "previous_completed_utc_day_quote_turnover",
            "cap": "0.10",
            "redistribution": "iterative_proportional_uncapped",
            "zero_gross_threshold": "1e-12",
            "missing_positive_weight_member": "unavailable",
        },
        "Top-five absolute contribution share under capped prior-day liquidity weights.",
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
    "breadth_advancing_share_change": _definition(
        "breadth_advancing_share_change",
        "percentage_points",
        {
            "formula": "100*(share_now-share_prior)",
            "matched_cohort": True,
            "comparison_window": "metric-specific tau encoded in metric_id",
            "advancing_threshold": "strictly-greater-than-1bp simple return",
        },
        "Matched-cohort change in advancing share over the declared trajectory interval, in percentage points.",
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
    "median_relative_volume_24h": _definition(
        "median_relative_volume_24h",
        "ratio",
        {"aggregation": "median", "source_metric": "rvol_24h", "cohort": "valid_instruments"},
        "Cross-sectional median 24-hour relative volume over valid instruments.",
    ),
    "rvol1h": _definition(
        "rvol1h",
        "ratio",
        {
            "current_minutes": RVOL_1H_BLOCK_MINUTES,
            "baseline_hours": RVOL_1H_BASELINE_HOURS,
            "baseline": "mean-of-prior-non-overlapping-hourly-sums",
            "required_minutes": RVOL_1H_REQUIRED_MINUTES,
            "current_excluded": True,
            "zero_baseline": "unavailable",
        },
        "Quote volume of the last 60 closed minutes divided by the mean of the prior 24 non-overlapping hourly sums.",
    ),
    "median_relative_volume_1h": _definition(
        "median_relative_volume_1h",
        "ratio",
        {"aggregation": "median", "source_metric": "rvol_1h", "cohort": "valid_instruments"},
        "Cross-sectional median hourly relative volume over valid instruments.",
    ),
    "mean_adx14": _definition(
        "mean_adx14",
        "adx_0_100",
        {"aggregation": "arithmetic_mean", "source_metric": "adx14", "cohort": "valid_instruments"},
        "Cross-sectional mean ADX14 trend strength over valid instruments.",
    ),
    "median_realized_volatility_24h": _definition(
        "median_realized_volatility_24h",
        "volatility_fraction_annualized",
        {"aggregation": "median", "source_metric": "realized_volatility_24h", "cohort": "valid_instruments"},
        "Cross-sectional median annualized realized volatility over valid instruments.",
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
    "daily_high_break_20d": _definition(
        "daily_high_break_20d",
        "count",
        {
            "current": "confirmed-daily-high",
            "prior_days": 20,
            "comparison": "strict-greater-than",
            "interval": "1d-utc",
        },
        "Current confirmed UTC daily high above the prior 20 completed UTC daily highs.",
    ),
    "daily_low_break_20d": _definition(
        "daily_low_break_20d",
        "count",
        {"current": "confirmed-daily-low", "prior_days": 20, "comparison": "strict-less-than", "interval": "1d-utc"},
        "Current confirmed UTC daily low below the prior 20 completed UTC daily lows.",
    ),
    "daily_close_break_high_20d": _definition(
        "daily_close_break_high_20d",
        "count",
        {
            "current": "confirmed-daily-close",
            "prior_days": 20,
            "range": "prior-daily-highs",
            "comparison": "strict-greater-than",
            "interval": "1d-utc",
        },
        "Current confirmed UTC daily close above the prior 20 completed daily highs.",
    ),
    "daily_close_break_low_20d": _definition(
        "daily_close_break_low_20d",
        "count",
        {
            "current": "confirmed-daily-close",
            "prior_days": 20,
            "range": "prior-daily-lows",
            "comparison": "strict-less-than",
            "interval": "1d-utc",
        },
        "Current confirmed UTC daily close below the prior 20 completed daily lows.",
    ),
    **{
        key: _definition(
            key,
            unit,
            parameters,
            description,
        )
        for key, unit, parameters, description in (
            (
                f"daily_sma_{window}",
                "price_quote",
                {
                    "window_days": window,
                    "formula": f"mean(last_{window}_confirmed_utc_daily_closes)",
                    "interval": "1d-utc",
                    "decimal_precision": 28,
                    "rounding": "ROUND_HALF_EVEN",
                    "serialization": "fixed-strip-trailing-zero",
                },
                f"Simple moving average of the latest {window} confirmed UTC daily closes.",
            )
            for window in (20, 50, 200)
        )
    },
    **{
        metric_id: _definition(metric_id, unit, parameters, description)
        for window in (20, 50, 200)
        for metric_id, unit, parameters, description in (
            (
                f"above_sma{window}_share",
                "share_fraction",
                {"interval": "1h", "window_bars": window, "formula": "close_t>sma_t", "strict": True},
                f"Share of valid assets whose latest confirmed hourly close is above SMA{window}.",
            ),
            (
                f"sma{window}_slope_pct_per_hour_median",
                "return_percent",
                {
                    "interval": "1h",
                    "window_bars": window,
                    "formula": "100*(sma_t/sma_t_minus_1-1)",
                    "aggregation": "median",
                },
                f"Median one-hour percentage change in confirmed hourly SMA{window}.",
            ),
            (
                f"sma{window}_cross_up_count",
                "instruments",
                {
                    "interval": "1h",
                    "window_bars": window,
                    "formula": "close_prev<=sma_prev and close_t>sma_t",
                    "missing_comparison": "unavailable",
                },
                f"Valid assets crossing above confirmed hourly SMA{window}.",
            ),
            (
                f"sma{window}_cross_down_count",
                "instruments",
                {
                    "interval": "1h",
                    "window_bars": window,
                    "formula": "close_prev>=sma_prev and close_t<sma_t",
                    "missing_comparison": "unavailable",
                },
                f"Valid assets crossing below confirmed hourly SMA{window}.",
            ),
        )
    },
    **{
        f"trend_class_{label}_count": _definition(
            f"trend_class_{label}_count",
            "instruments",
            {
                "interval": "1h",
                "bullish": "close>SMA20>SMA50 and SMA20_t>SMA20_t_minus_3h",
                "bearish": "close<SMA20<SMA50 and SMA20_t<SMA20_t_minus_3h",
                "otherwise": "neutral",
                "minimum_bars": 53,
                "valid_population_only": True,
            },
            f"Valid hourly trend-classified assets counted as {label}.",
        )
        for label in ("bullish", "bearish", "neutral")
    },
    "efficiency_ratio_20_median": _definition(
        "efficiency_ratio_20_median",
        "ratio",
        {
            "interval": "1h",
            "window_bars": 20,
            "formula": "abs(close_t-close_t_minus_20)/sum(abs(delta_close))",
            "flat_complete_path": "0",
            "minimum_bars": 21,
            "aggregation": "median",
        },
        "Median 20-hour close-path efficiency over complete confirmed hourly bars.",
    ),
    **{
        key: _definition(
            key,
            unit,
            parameters,
            description,
        )
        for window in (20, 50, 200)
        for key, unit, parameters, description in (
            (
                f"daily_sma_{window}_above_close",
                "share_fraction",
                {"window_days": window, "formula": "close_t>sma_t", "interval": "1d-utc"},
                f"Whether the latest confirmed daily close is above daily SMA{window}.",
            ),
            (
                f"daily_sma_{window}_slope_pct_per_day",
                "return_percent",
                {
                    "window_days": window,
                    "formula": "100*(sma_t/sma_t_minus_1-1)",
                    "step": "one-utc-day",
                    "decimal_precision": 28,
                    "rounding": "ROUND_HALF_EVEN",
                    "serialization": "fixed-strip-trailing-zero",
                },
                f"One-UTC-day percent change in daily SMA{window}.",
            ),
            (
                f"daily_sma_{window}_cross",
                "index_points",
                {
                    "window_days": window,
                    "formula": "cross-up=1,cross-down=-1,otherwise=0",
                    "requires": "previous-and-current-confirmed-close-and-sma",
                    "interval": "1d-utc",
                },
                f"Close crossing daily SMA{window}: +1 up, -1 down, 0 no cross.",
            ),
        )
    },
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
    **{
        f"pearson_log_1h_{hours}h": _definition(
            f"pearson_log_1h_{hours}h",
            "correlation_minus1_to1",
            {
                "interval_minutes": 60,
                "history_hours": hours,
                "min_coverage": CORRELATION_MIN_COVERAGE,
                "min_pairs": correlation_min_pairs(hours),
                "no_gap_spanning_return": True,
            },
            f"Pearson correlation of paired completed 1h log returns over {hours} hours.",
        )
        for hours in CORRELATION_WINDOW_HOURS
        if hours != 2160
    },
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
    "high_break_52w": _definition(
        "high_break_52w",
        "count",
        {"event_bar": "latest-completed-utc-day-high", "prior_days": 364, "strict": True},
        "Per-instrument completed-day high break against the prior 364 UTC days.",
    ),
    "low_break_52w": _definition(
        "low_break_52w",
        "count",
        {"event_bar": "latest-completed-utc-day-low", "prior_days": 364, "strict": True},
        "Per-instrument completed-day low break against the prior 364 UTC days.",
    ),
    "market_high_break_count_24h": _definition(
        "market_high_break_count_24h",
        "instruments",
        {"aggregation": "sum_true", "source_metric": "high_break_24h", "cohort": "eligible_instruments"},
        "Number of eligible instruments making a strict 24-hour high break.",
    ),
    "market_low_break_count_24h": _definition(
        "market_low_break_count_24h",
        "instruments",
        {"aggregation": "sum_true", "source_metric": "low_break_24h", "cohort": "eligible_instruments"},
        "Number of eligible instruments making a strict 24-hour low break.",
    ),
    "market_high_break_count_52w": _definition(
        "market_high_break_count_52w",
        "instruments",
        {"aggregation": "sum_true", "source_metric": "high_break_52w", "cohort": "eligible_instruments"},
        "Number of eligible instruments making a strict 52-week high break.",
    ),
    "market_low_break_count_52w": _definition(
        "market_low_break_count_52w",
        "instruments",
        {"aggregation": "sum_true", "source_metric": "low_break_52w", "cohort": "eligible_instruments"},
        "Number of eligible instruments making a strict 52-week low break.",
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
    "relative_return_delta": _definition(
        "relative_return_delta",
        "percentage_points",
        {
            "formula": "median(current_asset_minus_benchmark_24h)-median(previous_asset_minus_benchmark_24h)",
            "comparison_interval_minutes": 60,
            "matched_cohort": True,
            "source_metric": "RANK03_simple_relative_24h",
        },
        "One-hour change in median existing 24-hour benchmark-relative return.",
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

EVENT_PARAMETERS = {
    "rsi_threshold_crossing": {
        "source": "rsi14_wilder",
        "above": "70",
        "below": "30",
        "interval_ms": 60000,
        "rearm": "opposite-side-or-equal",
        "missing": "break-consecutive-chain",
    },
    "rvol_threshold_crossing": {
        "source": "rvol20",
        "above": "1.5",
        "severe_above": "3",
        "interval_ms": 60000,
        "rearm": "at-or-below-threshold",
        "missing": "break-consecutive-chain",
    },
    "breadth_threshold_crossing": {
        "source": "breadth_share",
        "above": "0.6",
        "below": "0.4",
        "minimum_coverage": "0.95",
        "same_valid_cohort": True,
        "interval_ms": 60000,
        "missing": "break-consecutive-chain",
    },
    "breadth_thrust_event": {
        "source": "normalized_15m_advance_share",
        "matched_cohort": "full-frozen-universe-throughout",
        "expansion_change_pp": ">=15 over 15m",
        "expansion_persistence": ">=0.60 for final three consecutive 1m endpoints",
        "contraction_change_pp": "<=-15 over 15m",
        "contraction_persistence": "<=0.40 for final three consecutive 1m endpoints",
        "minimum_coverage": "1.0",
        "definition_registry_stable": True,
        "missing_or_gap": "break-chain",
        "restart_identity": "stable-occurrence-key",
    },
    "trend_continuation_event": {
        "source": "adx14_wilder",
        "adx_above": "25",
        "valid_bars": 3,
        "direction": "unchanged-up-or-down",
        "adx": "strictly-increasing",
        "interval_ms": 60000,
        "rearm": "condition-false-or-missing",
    },
    "regime_transition_event": {
        "source": "exact-owner-record",
        "same_instrument_and_schema": True,
        "sequence": "consecutive",
        "interval_ms": 60000,
        "missing": "break-consecutive-chain",
    },
    "provider_quality_transition": {
        "source": "provider-state-and-valid-expected-counts",
        "emit": "state-or-count-change",
        "first_observation": "baseline-only",
    },
    "observation_correction_event": {
        "source": "immutable-revision-lineage",
        "identity": "stable-occurrence",
        "actions": ["supersede", "retract"],
        "availability": "receipt-time",
        "rewrite_original": False,
    },
    "taker_side_volume_spike": {
        "source": "qualified-complete-taker-side-quote-volume",
        "prior_minutes": 20,
        "multiple_above": "2",
        "side_share_at_least": "0.65",
        "zero_baseline": "unavailable",
        "dedup": "provider-trade-identity",
    },
    "low_break_10d_1h": {
        "event_bar": "latest-completed-1h-close",
        "prior_hours": 240,
        "comparison": "close<min(prior-low)",
        "strict": True,
    },
}
for _event_id, _event_parameters in EVENT_PARAMETERS.items():
    DEFINITIONS[_event_id] = _definition(
        _event_id,
        "count",
        _event_parameters,
        "Descriptive persisted event occurrence; never an order or trading permission.",
    )

for _micro_id, _micro_unit, _micro_parameters, _micro_description in (
    (
        "spot_executed_flow_1m",
        "volume_quote",
        {
            "window_ms": 60000,
            "window": "half-open",
            "side": "verified-aggressor",
            "aggregate_quantity": "already-summed-count-not-multiplier",
            "unknown": "separate-notional-no-confirmation",
            "duplicate": "channel-instrument-trade-id-with-overlap-rejection",
            "cumulative": "explicit-reset-no-gap-bridge",
        },
        "Executed spot buy/sell/unknown notional; never derived from candle direction.",
    ),
    (
        "spot_visible_book_cost",
        "basis_points",
        {
            "reference": "mid",
            "size": "explicit-base-or-quote",
            "max_age_ms": 5000,
            "partial": "covered-vwap-only-full-size-null",
            "fees": "separate-provenance-required",
            "access": "non-rpi-or-explicit-rpi",
            "band_bps": "explicit",
            "replay": "snapshot-only",
        },
        "Visible-book walk at an explicit size and access basis, without a fill promise.",
    ),
    (
        "spot_visible_band_depth",
        "volume_quote",
        {
            "band_bps": "explicit",
            "coverage": "both-observed-sides-reach-boundary",
            "rpi": "total-includes-organic-subset",
            "max_age_ms": 5000,
        },
        "Covered visible notional; full-band imbalance requires full boundary coverage.",
    ),
    (
        "spot_liquidity_percentile_7d",
        "share_fraction",
        {
            "completed_minutes": 10080,
            "minimum_coverage": 0.9,
            "ties": "midrank",
            "current_excluded": True,
            "comparison": "same-instrument-definition-side-size-unit-size-access",
        },
        "Comparable past-minute liquidity percentile with seven-day coverage.",
    ),
):
    DEFINITIONS[_micro_id] = _definition(_micro_id, _micro_unit, _micro_parameters, _micro_description)

REGISTRY_HASH = sha256(
    canonical_json({key: value.definition_hash for key, value in sorted(DEFINITIONS.items())}).encode()
).hexdigest()


def get_definition(definition_id: str) -> MetricDefinition:
    try:
        return DEFINITIONS[definition_id]
    except KeyError as exc:
        raise ValueError(f"unknown market-picture definition: {definition_id}") from exc


def correlation_definition_id(window_hours: int) -> str:
    """Definition id for one allowed hourly-return window; 2160 keeps the legacy 90D id."""
    if window_hours not in CORRELATION_WINDOW_HOURS:
        raise ValueError("unsupported correlation window")
    return "pearson_log_1h_90d" if window_hours == 2160 else f"pearson_log_1h_{window_hours}h"


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
        "rvol_1h": "rvol1h",
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
