from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Sequence

import pandas as pd

PAIR_SIGNAL_SCHEMA = "rsibot.hyperliquid_portfolio_rsi.pair_signal.v1"
ADVISORY_SIGNAL_SCHEMA = "rsibot.hyperliquid_portfolio_rsi.advisory_signal.v1"
_INTERVAL_PATTERN = re.compile(r"^(\d+)(s|m|h|d)$")


@dataclass(frozen=True)
class PairFeatureConfig:
    interval: str = "1m"
    rsi_length: int = 10
    rsi_buy_threshold: float = 31.0
    rsi_sell_threshold: float = 75.0
    rsi_reversal_delta: float = 0.5
    bb_length: int = 20
    bb_std: float = 2.0
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    ema_fast: int = 9
    ema_slow: int = 21
    atr_length: int = 14
    min_atr_pct: float = 0.0002
    max_atr_pct: float = 0.03
    flash_crash_lookback: int = 5
    flash_crash_drop_pct: float = 0.03
    max_candle_age_intervals: float = 2.5
    max_book_age_seconds: float = 15.0
    max_clock_skew_seconds: float = 5.0
    max_spread_bps: float = 30.0
    min_depth_quote: float = 1_000.0
    target_notional_quote: float = 30.0
    max_slippage_bps: float = 25.0
    linear_impact_coefficient: float = 0.10
    estimated_fee_bps_per_side: float = 10.0
    max_total_cost_bps_per_side: float = 50.0
    advisory_required: bool = False
    advisory_max_age_seconds: float = 120.0
    advisory_weight: float = 0.0

    def __post_init__(self):
        if _interval_seconds(self.interval) is None:
            raise ValueError(f"Unsupported candle interval: {self.interval}")
        if self.macd_fast >= self.macd_slow:
            raise ValueError("macd_fast must be below macd_slow")
        if not 0.0 <= self.advisory_weight <= 1.0:
            raise ValueError("advisory_weight must be between 0 and 1")
        if self.rsi_buy_threshold >= self.rsi_sell_threshold:
            raise ValueError("rsi_buy_threshold must be below rsi_sell_threshold")
        if self.estimated_fee_bps_per_side < 0 or self.max_total_cost_bps_per_side < 0:
            raise ValueError("fee and cost limits must be non-negative")


def _interval_seconds(interval: str) -> Optional[int]:
    match = _INTERVAL_PATTERN.fullmatch(str(interval or "").strip().lower())
    if match is None:
        return None
    amount = int(match.group(1))
    multiplier = {"s": 1, "m": 60, "h": 3_600, "d": 86_400}[match.group(2)]
    return amount * multiplier


def _epoch_seconds(value: Any) -> Optional[float]:
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        try:
            text = str(value or "").strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (TypeError, ValueError):
            return None
    if not math.isfinite(numeric):
        return None
    while abs(numeric) >= 100_000_000_000:
        numeric /= 1_000.0
    return numeric


def _iso_timestamp(value: Optional[float]) -> Optional[str]:
    if value is None or not math.isfinite(value):
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _raw_value(source: Any, *names: str) -> Any:
    for name in names:
        if isinstance(source, Mapping) and name in source:
            return source.get(name)
        if hasattr(source, name):
            return getattr(source, name)
    return None


def _finite_float(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError, InvalidOperation):
        return None
    return result if math.isfinite(result) else None


def _rounded(value: Optional[float], digits: int = 8) -> Optional[float]:
    if value is None or not math.isfinite(value):
        return None
    return round(float(value), digits)


def _decimal_string(value: Optional[float]) -> str:
    if value is None or not math.isfinite(value) or value == 0:
        return "0"
    normalized = Decimal(str(round(float(value), 8))).normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def _append(blockers: list[str], reason: str):
    if reason not in blockers:
        blockers.append(reason)


def _base_snapshot(pair_id: str, as_of: Optional[float], symbol_mapping_status: str) -> Dict[str, Any]:
    return {
        "schema": PAIR_SIGNAL_SCHEMA,
        "pair_id": pair_id,
        "as_of": _iso_timestamp(as_of),
        "source_timestamp": None,
        "feature_cutoff": None,
        "freshness_status": "invalid",
        "symbol_mapping_status": symbol_mapping_status,
        "actionable": False,
        "closed_candle_count": 0,
        "excluded_open_candle_count": 0,
        "price_quote": "0",
        "buy_score": "0",
        "sell_score": "0",
        "blockers": [],
        "signal_reasons": {"buy": [], "sell": []},
        "indicators": {},
        "market": {},
        "trading_rule": {},
        "advisory": {"status": "not_provided"},
    }


def _finalize(
    snapshot: Dict[str, Any],
    blockers: Sequence[str],
    buy_score: float = 0.0,
    sell_score: float = 0.0,
) -> Dict[str, Any]:
    unique_blockers = list(dict.fromkeys(blockers))
    snapshot["blockers"] = unique_blockers
    snapshot["actionable"] = len(unique_blockers) == 0
    if snapshot["actionable"]:
        snapshot["freshness_status"] = "fresh"
        snapshot["buy_score"] = _decimal_string(max(0.0, min(1.0, buy_score)))
        snapshot["sell_score"] = _decimal_string(max(0.0, min(1.0, sell_score)))
    else:
        snapshot["freshness_status"] = (
            "stale" if any("stale" in reason for reason in unique_blockers) else "invalid"
        )
        snapshot["buy_score"] = "0"
        snapshot["sell_score"] = "0"
    return snapshot


def _rsi(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    average_gain = gains.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    average_loss = losses.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    relative_strength = average_gain / average_loss.replace(0.0, float("nan"))
    result = 100.0 - (100.0 / (1.0 + relative_strength))
    result = result.mask((average_loss == 0.0) & (average_gain > 0.0), 100.0)
    result = result.mask((average_loss == 0.0) & (average_gain == 0.0), 50.0)
    return result


def _atr(frame: pd.DataFrame, length: int) -> pd.Series:
    previous_close = frame["close"].shift(1)
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def _prepare_closed_candles(
    *,
    candles: pd.DataFrame,
    as_of: float,
    config: PairFeatureConfig,
    snapshot: Dict[str, Any],
    blockers: list[str],
) -> Optional[pd.DataFrame]:
    if not isinstance(candles, pd.DataFrame) or candles.empty:
        _append(blockers, "candles_missing")
        return None
    required_columns = ("timestamp", "open", "high", "low", "close")
    for column in required_columns:
        if column not in candles.columns:
            _append(blockers, f"candle_{column}_missing")
    if blockers:
        return None

    frame = candles.copy()
    normalized_timestamps = frame["timestamp"].map(_epoch_seconds)
    if normalized_timestamps.isna().any():
        _append(blockers, "candle_timestamp_invalid")
        return None
    if normalized_timestamps.duplicated().any():
        _append(blockers, "candle_timestamp_duplicate")
        return None
    if not normalized_timestamps.is_monotonic_increasing:
        _append(blockers, "candle_timestamp_out_of_order")
        return None
    frame["_open_timestamp"] = normalized_timestamps.astype(float)

    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if frame[column].isna().any() or (~frame[column].map(math.isfinite)).any() or (frame[column] <= 0).any():
            _append(blockers, f"candle_{column}_invalid")
    if blockers:
        return None
    if ((frame["high"] < frame[["open", "close"]].max(axis=1))
            | (frame["low"] > frame[["open", "close"]].min(axis=1))
            | (frame["high"] < frame["low"])).any():
        _append(blockers, "candle_ohlc_invalid")
        return None

    interval_seconds = _interval_seconds(config.interval)
    assert interval_seconds is not None
    closed_mask = frame["_open_timestamp"] + interval_seconds <= as_of + 1e-6
    closed = frame.loc[closed_mask].copy()
    snapshot["closed_candle_count"] = int(len(closed))
    snapshot["excluded_open_candle_count"] = int(len(frame) - len(closed))
    if closed.empty:
        _append(blockers, "closed_candles_missing")
        return None

    feature_cutoff = float(closed["_open_timestamp"].iloc[-1]) + interval_seconds
    snapshot["feature_cutoff"] = _iso_timestamp(feature_cutoff)
    candle_age = as_of - feature_cutoff
    if candle_age < -config.max_clock_skew_seconds:
        _append(blockers, "candle_cutoff_future")
    elif candle_age > interval_seconds * config.max_candle_age_intervals:
        _append(blockers, "candle_stale")

    required_history = max(
        config.rsi_length + 2,
        config.bb_length + 1,
        config.macd_slow + config.macd_signal + 2,
        config.ema_slow + 1,
        config.atr_length + 1,
        config.flash_crash_lookback + 1,
    )
    if len(closed) < required_history:
        _append(blockers, "indicator_history_insufficient")
        return None
    return closed


def _build_indicators(
    frame: pd.DataFrame,
    config: PairFeatureConfig,
    snapshot: Dict[str, Any],
    blockers: list[str],
) -> tuple[float, float]:
    close = frame["close"].astype(float)
    rsi = _rsi(close, config.rsi_length)
    bb_mid = close.rolling(config.bb_length, min_periods=config.bb_length).mean()
    bb_std = close.rolling(config.bb_length, min_periods=config.bb_length).std(ddof=0)
    bb_lower = bb_mid - (bb_std * config.bb_std)
    bb_upper = bb_mid + (bb_std * config.bb_std)
    ema_fast = close.ewm(span=config.ema_fast, adjust=False, min_periods=config.ema_fast).mean()
    ema_slow = close.ewm(span=config.ema_slow, adjust=False, min_periods=config.ema_slow).mean()
    macd_fast = close.ewm(span=config.macd_fast, adjust=False, min_periods=config.macd_fast).mean()
    macd_slow = close.ewm(span=config.macd_slow, adjust=False, min_periods=config.macd_slow).mean()
    macd = macd_fast - macd_slow
    macd_signal = macd.ewm(span=config.macd_signal, adjust=False, min_periods=config.macd_signal).mean()
    macd_histogram = macd - macd_signal
    atr = _atr(frame, config.atr_length)

    values = {
        "rsi": _finite_float(rsi.iloc[-1]),
        "rsi_previous": _finite_float(rsi.iloc[-2]),
        "bb_lower": _finite_float(bb_lower.iloc[-1]),
        "bb_mid": _finite_float(bb_mid.iloc[-1]),
        "bb_upper": _finite_float(bb_upper.iloc[-1]),
        "ema_fast": _finite_float(ema_fast.iloc[-1]),
        "ema_slow": _finite_float(ema_slow.iloc[-1]),
        "macd_histogram": _finite_float(macd_histogram.iloc[-1]),
        "macd_histogram_previous": _finite_float(macd_histogram.iloc[-2]),
        "atr": _finite_float(atr.iloc[-1]),
    }
    if any(value is None for value in values.values()):
        _append(blockers, "indicator_value_invalid")
        snapshot["indicators"] = {key: _rounded(value) for key, value in values.items()}
        return 0.0, 0.0

    latest_close = float(close.iloc[-1])
    previous_close = float(close.iloc[-2])
    atr_pct = values["atr"] / latest_close
    one_candle_return = (latest_close / previous_close) - 1.0
    lookback_close = float(close.iloc[-(config.flash_crash_lookback + 1)])
    lookback_return = (latest_close / lookback_close) - 1.0
    volume_ratio = None
    if "volume" in frame.columns:
        volume = pd.to_numeric(frame["volume"], errors="coerce")
        if not volume.isna().any() and (volume >= 0).all():
            volume_mean = _finite_float(volume.tail(20).mean())
            if volume_mean is not None and volume_mean > 0:
                volume_ratio = float(volume.iloc[-1]) / volume_mean

    if atr_pct < config.min_atr_pct:
        _append(blockers, "volatility_below_minimum")
    if atr_pct > config.max_atr_pct:
        _append(blockers, "volatility_above_maximum")
    if lookback_return <= -abs(config.flash_crash_drop_pct):
        _append(blockers, "flash_crash_guard")

    if values["ema_fast"] > values["ema_slow"] and latest_close >= values["ema_fast"]:
        regime = "bullish"
    elif values["ema_fast"] < values["ema_slow"] and latest_close <= values["ema_fast"]:
        regime = "bearish"
    else:
        regime = "range"
    if atr_pct > config.max_atr_pct:
        regime = "high_volatility"

    bb_width = values["bb_upper"] - values["bb_lower"]
    bb_position = None if bb_width <= 0 else (latest_close - values["bb_lower"]) / bb_width
    snapshot["indicators"] = {
        **{key: _rounded(value) for key, value in values.items()},
        "close": _rounded(latest_close),
        "return_1": _rounded(one_candle_return),
        "return_lookback": _rounded(lookback_return),
        "atr_pct": _rounded(atr_pct),
        "bb_position": _rounded(bb_position),
        "volume_ratio": _rounded(volume_ratio),
        "market_regime": regime,
    }

    buy_reasons: list[str] = []
    sell_reasons: list[str] = []
    rsi_oversold = values["rsi"] <= config.rsi_buy_threshold
    rsi_overbought = values["rsi"] >= config.rsi_sell_threshold
    buy_components = 0
    sell_components = 0
    if rsi_oversold:
        buy_components += 1
        buy_reasons.append("rsi_oversold")
        if latest_close <= values["bb_lower"]:
            buy_components += 1
            buy_reasons.append("bollinger_lower_touch")
        if (
            values["macd_histogram"] > values["macd_histogram_previous"]
            and (values["macd_histogram"] > 0 or values["macd_histogram_previous"] < 0)
        ):
            buy_components += 1
            buy_reasons.append("macd_turn_up")
        if values["rsi"] - values["rsi_previous"] >= config.rsi_reversal_delta:
            buy_components += 1
            buy_reasons.append("rsi_rebound")
    if rsi_overbought:
        sell_components += 1
        sell_reasons.append("rsi_overbought")
        if latest_close >= values["bb_upper"]:
            sell_components += 1
            sell_reasons.append("bollinger_upper_touch")
        if (
            values["macd_histogram"] < values["macd_histogram_previous"]
            and (values["macd_histogram"] < 0 or values["macd_histogram_previous"] > 0)
        ):
            sell_components += 1
            sell_reasons.append("macd_turn_down")
        if values["rsi_previous"] - values["rsi"] >= config.rsi_reversal_delta:
            sell_components += 1
            sell_reasons.append("rsi_rollover")

    snapshot["signal_reasons"] = {"buy": buy_reasons, "sell": sell_reasons}
    return buy_components / 4.0, sell_components / 4.0


def _build_market_snapshot(
    *,
    market_snapshot: Mapping[str, Any],
    as_of: float,
    config: PairFeatureConfig,
    snapshot: Dict[str, Any],
    blockers: list[str],
):
    if not isinstance(market_snapshot, Mapping):
        _append(blockers, "market_snapshot_missing")
        return
    source_timestamp = _epoch_seconds(
        _raw_value(market_snapshot, "source_timestamp", "timestamp", "as_of")
    )
    snapshot["source_timestamp"] = _iso_timestamp(source_timestamp)
    if source_timestamp is None:
        _append(blockers, "book_timestamp_missing")
    else:
        book_age = as_of - source_timestamp
        if book_age < -config.max_clock_skew_seconds:
            _append(blockers, "book_timestamp_future")
        elif book_age > config.max_book_age_seconds:
            _append(blockers, "book_stale")

    bid = _finite_float(_raw_value(market_snapshot, "bid_price", "best_bid", "bid"))
    ask = _finite_float(_raw_value(market_snapshot, "ask_price", "best_ask", "ask"))
    bid_depth = _finite_float(_raw_value(market_snapshot, "bid_depth_quote", "bid_liquidity_quote"))
    ask_depth = _finite_float(_raw_value(market_snapshot, "ask_depth_quote", "ask_liquidity_quote"))
    if bid is None or bid <= 0:
        _append(blockers, "best_bid_missing")
    if ask is None or ask <= 0:
        _append(blockers, "best_ask_missing")
    if bid_depth is None or bid_depth < 0:
        _append(blockers, "bid_liquidity_missing")
    elif bid_depth < config.min_depth_quote:
        _append(blockers, "bid_liquidity_below_minimum")
    if ask_depth is None or ask_depth < 0:
        _append(blockers, "ask_liquidity_missing")
    elif ask_depth < config.min_depth_quote:
        _append(blockers, "ask_liquidity_below_minimum")

    mid = None
    spread_bps = None
    buy_slippage_bps = None
    sell_slippage_bps = None
    buy_total_cost_bps = None
    sell_total_cost_bps = None
    round_trip_cost_bps = None
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        if bid >= ask:
            _append(blockers, "book_crossed")
        mid = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / mid) * 10_000.0
        if spread_bps > config.max_spread_bps:
            _append(blockers, "spread_above_limit")
        half_spread_bps = max(spread_bps, 0.0) / 2.0
        if ask_depth is not None and ask_depth > 0:
            buy_slippage_bps = half_spread_bps + (
                config.target_notional_quote / ask_depth
            ) * 10_000.0 * config.linear_impact_coefficient
            if buy_slippage_bps > config.max_slippage_bps:
                _append(blockers, "buy_slippage_above_limit")
            buy_total_cost_bps = buy_slippage_bps + config.estimated_fee_bps_per_side
            if buy_total_cost_bps > config.max_total_cost_bps_per_side:
                _append(blockers, "buy_total_cost_above_limit")
        if bid_depth is not None and bid_depth > 0:
            sell_slippage_bps = half_spread_bps + (
                config.target_notional_quote / bid_depth
            ) * 10_000.0 * config.linear_impact_coefficient
            if sell_slippage_bps > config.max_slippage_bps:
                _append(blockers, "sell_slippage_above_limit")
            sell_total_cost_bps = sell_slippage_bps + config.estimated_fee_bps_per_side
            if sell_total_cost_bps > config.max_total_cost_bps_per_side:
                _append(blockers, "sell_total_cost_above_limit")
        if buy_slippage_bps is not None and sell_slippage_bps is not None:
            round_trip_cost_bps = (
                buy_slippage_bps
                + sell_slippage_bps
                + (2.0 * config.estimated_fee_bps_per_side)
            )

    snapshot["price_quote"] = _decimal_string(mid)
    snapshot["market"] = {
        "bid_price": _rounded(bid),
        "ask_price": _rounded(ask),
        "mid_price": _rounded(mid),
        "spread_bps": _rounded(spread_bps),
        "bid_depth_quote": _rounded(bid_depth),
        "ask_depth_quote": _rounded(ask_depth),
        "estimated_buy_slippage_bps": _rounded(buy_slippage_bps),
        "estimated_sell_slippage_bps": _rounded(sell_slippage_bps),
        "estimated_fee_bps_per_side": _rounded(config.estimated_fee_bps_per_side),
        "estimated_buy_total_cost_bps": _rounded(buy_total_cost_bps),
        "estimated_sell_total_cost_bps": _rounded(sell_total_cost_bps),
        "estimated_round_trip_cost_bps": _rounded(round_trip_cost_bps),
    }


def _build_trading_rule(
    *,
    trading_rule: Any,
    config: PairFeatureConfig,
    snapshot: Dict[str, Any],
    blockers: list[str],
):
    min_notional = _finite_float(_raw_value(trading_rule, "min_notional_size", "min_notional"))
    amount_increment = _finite_float(
        _raw_value(trading_rule, "min_base_amount_increment", "amount_step")
    )
    price_increment = _finite_float(_raw_value(trading_rule, "min_price_increment", "price_step"))
    significant_digits = _finite_float(
        _raw_value(trading_rule, "max_price_significant_digits", "max_significant_digits")
    )
    if min_notional is None or min_notional <= 0:
        _append(blockers, "minimum_notional_missing")
    elif config.target_notional_quote < min_notional:
        _append(blockers, "target_notional_below_exchange_minimum")
    if amount_increment is None or amount_increment <= 0:
        _append(blockers, "amount_precision_missing")
    if price_increment is None or price_increment <= 0:
        _append(blockers, "price_precision_missing")
    if significant_digits is None or significant_digits <= 0:
        _append(blockers, "price_significant_digits_missing")
    snapshot["trading_rule"] = {
        "min_notional_size": _rounded(min_notional),
        "min_base_amount_increment": _rounded(amount_increment),
        "min_price_increment": _rounded(price_increment),
        "max_price_significant_digits": _rounded(significant_digits),
    }


def _apply_advisory(
    *,
    advisory: Optional[Mapping[str, Any]],
    pair_id: str,
    as_of: float,
    local_cutoff: Optional[float],
    config: PairFeatureConfig,
    snapshot: Dict[str, Any],
    blockers: list[str],
    buy_score: float,
    sell_score: float,
) -> tuple[float, float]:
    if advisory is None:
        if config.advisory_required:
            _append(blockers, "advisory_missing")
            snapshot["advisory"] = {"status": "missing"}
        return buy_score, sell_score
    if not isinstance(advisory, Mapping):
        _append(blockers, "advisory_malformed")
        snapshot["advisory"] = {"status": "invalid"}
        return buy_score, sell_score

    advisory_blockers: list[str] = []
    if str(advisory.get("schema") or "") != ADVISORY_SIGNAL_SCHEMA:
        _append(advisory_blockers, "advisory_schema_invalid")
    if str(advisory.get("pair_id") or "").upper() != pair_id.upper():
        _append(advisory_blockers, "advisory_pair_mismatch")
    source_timestamp = _epoch_seconds(advisory.get("source_timestamp") or advisory.get("as_of"))
    advisory_cutoff = _epoch_seconds(advisory.get("feature_cutoff"))
    advisory_buy = _finite_float(advisory.get("buy_score"))
    advisory_sell = _finite_float(advisory.get("sell_score"))
    if source_timestamp is None:
        _append(advisory_blockers, "advisory_timestamp_missing")
    else:
        advisory_age = as_of - source_timestamp
        if advisory_age < -config.max_clock_skew_seconds:
            _append(advisory_blockers, "advisory_timestamp_future")
        elif advisory_age > config.advisory_max_age_seconds:
            _append(advisory_blockers, "advisory_stale")
    if advisory_cutoff is None:
        _append(advisory_blockers, "advisory_cutoff_missing")
    elif local_cutoff is not None and advisory_cutoff > local_cutoff + config.max_clock_skew_seconds:
        _append(advisory_blockers, "advisory_cutoff_after_local")
    if advisory_buy is None or not 0.0 <= advisory_buy <= 1.0:
        _append(advisory_blockers, "advisory_buy_score_invalid")
    if advisory_sell is None or not 0.0 <= advisory_sell <= 1.0:
        _append(advisory_blockers, "advisory_sell_score_invalid")

    snapshot["advisory"] = {
        "status": "invalid" if advisory_blockers else "fresh",
        "source_timestamp": _iso_timestamp(source_timestamp),
        "feature_cutoff": _iso_timestamp(advisory_cutoff),
        "buy_score": _rounded(advisory_buy),
        "sell_score": _rounded(advisory_sell),
        "blockers": advisory_blockers,
    }
    for reason in advisory_blockers:
        _append(blockers, reason)
    if advisory_blockers:
        return buy_score, sell_score

    weight = config.advisory_weight
    if buy_score > 0:
        buy_score = (buy_score * (1.0 - weight)) + (advisory_buy * weight)
    if sell_score > 0:
        sell_score = (sell_score * (1.0 - weight)) + (advisory_sell * weight)
    return buy_score, sell_score


def build_pair_signal_snapshot(
    *,
    pair_id: str,
    candles: pd.DataFrame,
    market_snapshot: Mapping[str, Any],
    trading_rule: Any,
    as_of: Any,
    config: Optional[PairFeatureConfig] = None,
    advisory: Optional[Mapping[str, Any]] = None,
    symbol_mapping_status: str = "resolved",
) -> Dict[str, Any]:
    """Build fail-closed RSI and microstructure evidence for one portfolio pair.

    Candle timestamps are treated as candle-open timestamps. Only rows whose
    full interval has elapsed at ``as_of`` are eligible for indicators.
    """
    config = config or PairFeatureConfig()
    normalized_pair = str(pair_id or "").strip().upper()
    normalized_symbol_status = str(symbol_mapping_status or "unresolved").strip().lower()
    as_of_seconds = _epoch_seconds(as_of)
    snapshot = _base_snapshot(normalized_pair, as_of_seconds, normalized_symbol_status)
    blockers: list[str] = []
    if not normalized_pair or "-" not in normalized_pair:
        _append(blockers, "pair_id_invalid")
    if as_of_seconds is None:
        _append(blockers, "as_of_invalid")
        return _finalize(snapshot, blockers)
    if normalized_symbol_status != "resolved":
        _append(blockers, "symbol_unresolved")

    closed = _prepare_closed_candles(
        candles=candles,
        as_of=as_of_seconds,
        config=config,
        snapshot=snapshot,
        blockers=blockers,
    )
    buy_score = 0.0
    sell_score = 0.0
    if closed is not None:
        buy_score, sell_score = _build_indicators(closed, config, snapshot, blockers)
    _build_market_snapshot(
        market_snapshot=market_snapshot,
        as_of=as_of_seconds,
        config=config,
        snapshot=snapshot,
        blockers=blockers,
    )
    _build_trading_rule(
        trading_rule=trading_rule,
        config=config,
        snapshot=snapshot,
        blockers=blockers,
    )
    local_cutoff = _epoch_seconds(snapshot.get("feature_cutoff"))
    buy_score, sell_score = _apply_advisory(
        advisory=advisory,
        pair_id=normalized_pair,
        as_of=as_of_seconds,
        local_cutoff=local_cutoff,
        config=config,
        snapshot=snapshot,
        blockers=blockers,
        buy_score=buy_score,
        sell_score=sell_score,
    )
    return _finalize(snapshot, blockers, buy_score, sell_score)


__all__ = [
    "ADVISORY_SIGNAL_SCHEMA",
    "PAIR_SIGNAL_SCHEMA",
    "PairFeatureConfig",
    "build_pair_signal_snapshot",
]
