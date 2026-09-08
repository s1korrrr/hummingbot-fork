"""Read-only projections of closed owner candles; never reconstruct trade decisions."""
import hashlib
import math

import pandas as pd

# Covers the owner's 3,660-bar episode feed without unbounded process memory.
MAX_CACHED_CANDLES = 4096
MAX_BATCH_ROWS = 500


def _number(value):
    try:
        result = float(value)
    except (ValueError, TypeError):
        return None
    return result if math.isfinite(result) else None


def _indicator_columns(config):
    return {
        'rsi': f'RSI_{config.rsi_length}', 'atr': f'ATR_{config.atr_length}',
        'ema_fast': f'EMA_{config.ema_fast_length}', 'ema_slow': f'EMA_{config.ema_slow_length}',
        'bb_lower': f'BBL_{config.bb_length}_{config.bb_std}',
        'bb_mid': f'BBM_{config.bb_length}_{config.bb_std}',
        'bb_upper': f'BBU_{config.bb_length}_{config.bb_std}',
        'macd_hist': f'MACDh_{config.macd_fast}_{config.macd_slow}_{config.macd_signal}',
    }


def chart_observation(config, interval, processed, now, interval_seconds):
    timestamp = _number(processed.get('candle_timestamp'))
    observed = _number(processed.get('timestamp'))
    if not (processed.get('data_health', {}).get('ready') is True
            and interval == config.interval and timestamp is not None
            and timestamp == processed.get('candle_timestamp')
            and observed is not None and 0 <= now - observed <= interval_seconds
            and 0 <= now - (timestamp + interval_seconds) < interval_seconds):
        return None, {}
    state = processed.get('signal_state') or {}
    indicators = processed.get('indicators') or {}
    thresholds = processed.get('thresholds') or {}
    trend = processed.get('trend_confirmation') or {}
    row = {key: state.get(key) for key in ('buy_decision', 'sell_decision', 'buy_entry_role', 'buy_context_bias')}
    row.update(signal=_number(processed.get('signal')), signal_score=_number(state.get('signal_score')),
               regime_label=processed.get('regime'), regime_confidence=_number(processed.get('regime_confidence')),
               trend_direction=trend.get('direction'), trend_confidence=_number(trend.get('confidence')),
               rsi_buy_threshold=_number(thresholds.get('rsi_buy')),
               rsi_sell_threshold=_number(thresholds.get('rsi_sell')),
               cost_basis=_number(indicators.get('cost_basis')))
    return timestamp, {key: value for key, value in row.items() if value is not None}


def chart_projection_signature(config, bot_name, interval, frame, processed, now, interval_seconds):
    # Hash only source fields used by chart projection/aggregation. Ignore the
    # forming candle, dataframe index, and unrelated reconstructed signal columns.
    candidate_columns = ('timestamp', 'open', 'high', 'low', 'close', 'volume',
                         'quote_asset_volume', 'n_trades', 'taker_buy_base_volume', 'taker_buy_quote_volume',
                         *_indicator_columns(config).values())
    columns = tuple(column for column in candidate_columns if column in frame.columns)
    numeric = frame.loc[:, columns].apply(pd.to_numeric, errors='coerce')
    digest = hashlib.sha256(numeric.to_numpy(dtype='float64', na_value=float('nan')).tobytes()).digest()
    observation = chart_observation(config, interval, processed, now, interval_seconds)
    return (config.id, config.trading_pair, bot_name, config.interval,
            getattr(config, 'derived_higher_timeframe_max_records', None), columns, digest, observation)


def build_chart_rows(config, bot_name, interval, frame, processed, *, now, interval_seconds):
    if frame is None or frame.empty or not interval_seconds or not math.isfinite(now):
        return []
    columns = _indicator_columns(config)
    observation_timestamp, observation = chart_observation(config, interval, processed, now, interval_seconds)
    rows = []
    for values in frame.itertuples(index=False, name=None):
        candle = dict(zip(frame.columns, values))
        timestamp = _number(candle.get('timestamp'))
        # Owner candle timestamps are epoch seconds, never heuristically guessed units.
        if timestamp is None or timestamp < 0 or timestamp + interval_seconds > now:
            continue
        prices = {f'{key}_price': _number(candle.get(key)) for key in ('open', 'high', 'low', 'close')}
        if any(value is None or value <= 0 for value in prices.values()):
            continue
        if prices['low_price'] > min(prices.values()) or prices['high_price'] < max(prices.values()):
            continue
        row = {'bot_name': bot_name, 'controller_id': config.id, 'pair': config.trading_pair,
               'interval': interval, 'candle_timestamp': int(timestamp * 1000), **prices,
               'volume': _number(candle.get('volume'))}
        row.update({key: _number(candle.get(column)) for key, column in columns.items()})
        row['atr_pct'] = row['atr'] / row['close_price'] if row['atr'] is not None else None
        if timestamp == observation_timestamp:
            row.update(observation)
        # Historical state keys stay absent so restart backfills cannot erase stored observations.
        rows.append({key: value for key, value in row.items() if value is not None})
    return rows


def pending_chart_rows(rows, cache, limit=MAX_BATCH_ROWS):
    """Compare durable projections, allowing older arrivals and indicator enrichment."""
    pending = []
    for row in rows:
        previous = cache.get(row['interval'], {}).get(row['candle_timestamp'], {})
        merged = {**previous, **row}
        if merged != previous:
            pending.append(merged)
            if len(pending) >= limit:
                break
    return pending


def acknowledge_chart_rows(cache, rows):
    """Only the recorder's successful batch receipt advances this bounded cache."""
    for row in rows:
        interval_cache = cache.setdefault(row['interval'], {})
        interval_cache[row['candle_timestamp']] = dict(row)
    for interval, interval_cache in cache.items():
        if len(interval_cache) > MAX_CACHED_CANDLES:
            retained = sorted(interval_cache)[-MAX_CACHED_CANDLES:]
            cache[interval] = {timestamp: interval_cache[timestamp] for timestamp in retained}
