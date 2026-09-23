"""Millisecond recovery-path reducer; opt-in, non-economic and free of I/O."""

import math
from dataclasses import asdict, dataclass, replace

SCHEMA = 'rsibot.nre_path_features.v2'


def _positive(value, name):
    if type(value) not in (int, float):
        raise ValueError(f'{name} must be a finite positive number')
    try:
        valid = math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f'{name} must be a finite positive number')


def _integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f'{name} must be an integer in [{minimum}, {maximum}]')


@dataclass(frozen=True)
class Policy:
    lookback_bars: int
    rebound_atr: float
    invalidation_atr: float
    max_bars: int

    def __post_init__(self):
        _integer(self.lookback_bars, 'lookback_bars', 1, 10000)
        _integer(self.max_bars, 'max_bars', 2, 100000)
        if self.lookback_bars >= self.max_bars:
            raise ValueError('lookback_bars must be below max_bars')
        _positive(self.rebound_atr, 'rebound_atr')
        _positive(self.invalidation_atr, 'invalidation_atr')


@dataclass(frozen=True)
class Candle:
    timestamp_ms: int
    available_at_ms: int
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self):
        _integer(self.timestamp_ms, 'timestamp_ms', 0, 2 ** 53 - 120000)
        _integer(self.available_at_ms, 'available_at_ms', self.timestamp_ms + 60000, 2 ** 53)
        if self.timestamp_ms % 60000:
            raise ValueError('timestamp_ms must be UTC minute aligned milliseconds')
        for key in ('open', 'high', 'low', 'close'):
            _positive(getattr(self, key), key)
        if self.high < max(self.open, self.low, self.close) or self.low > min(self.open, self.high, self.close):
            raise ValueError('invalid OHLC envelope')


@dataclass(frozen=True)
class PathState:
    setup_id: str
    instrument: str
    scale: float
    policy: Policy
    last: Candle
    count: int
    running_low: float
    lows: tuple[float, ...]
    bars_since_low: int
    status: str = 'DECLINING'
    confirmed_trough: float | None = None
    confirmed_at_ms: int | None = None
    peak_close: float | None = None


def _clock(candle, as_of):
    if not isinstance(candle, Candle):
        raise ValueError('a validated Candle is required')
    _integer(as_of, 'as_of', 0, 2 ** 53)
    if candle.available_at_ms > as_of or not candle.timestamp_ms + 60000 <= as_of < candle.timestamp_ms + 120000:
        raise ValueError('candle is not closed, available and fresh at as_of')


def _transition(state):
    if state.confirmed_trough is not None:
        floor = state.confirmed_trough - state.policy.invalidation_atr * state.scale
        if state.last.low <= floor:
            return replace(state, status='FAILED')
    if state.count >= state.policy.max_bars:
        return replace(state, status='EXPIRED')
    if state.confirmed_trough is None:
        target = state.running_low + state.policy.rebound_atr * state.scale
        floor = state.running_low - state.policy.invalidation_atr * state.scale
        if not (math.isfinite(target) and math.isfinite(floor) and target > state.running_low > floor):
            raise ValueError('thresholds are nonfinite or below numeric resolution')
        if state.last.close >= target:
            return replace(state, status='RECOVERING', confirmed_trough=state.running_low,
                           confirmed_at_ms=state.last.available_at_ms, peak_close=state.last.close)
    else:
        return replace(state, peak_close=max(state.peak_close, state.last.close))
    return state


def start(setup_id: str, instrument: str, scale: float, candle: Candle, as_of: int, policy: Policy) -> PathState:
    """Attach to an externally identified setup; do not infer an entry or a bottom."""
    if any(not isinstance(v, str) or not v.strip() for v in (setup_id, instrument)):
        raise ValueError('setup_id and instrument are required')
    if not isinstance(policy, Policy):
        raise ValueError('a validated Policy is required')
    _positive(scale, 'scale')
    _clock(candle, as_of)
    state = _transition(PathState(setup_id, instrument, scale, policy, candle, 1, candle.low, (candle.low,), 0))
    view(state, as_of)
    return state


def step(state: PathState, candle: Candle, as_of: int) -> PathState:
    """Process one consecutive closed candle; rejected input never mutates state."""
    _clock(candle, as_of)
    if candle == state.last:
        return state
    if candle.timestamp_ms == state.last.timestamp_ms:
        raise ValueError('candle revision requires explicit reconstruction')
    if state.status in ('FAILED', 'EXPIRED'):
        raise ValueError('terminal setup requires a new externally justified setup identity')
    if candle.timestamp_ms != state.last.timestamp_ms + 60000:
        raise ValueError('gap or out-of-order candle requires reconstruction')
    low = min(state.running_low, candle.low)
    state = _transition(replace(state, last=candle, count=state.count + 1, running_low=low,
                                lows=(*state.lows, low)[-(state.policy.lookback_bars + 1):],
                                bars_since_low=0 if low < state.running_low else state.bars_since_low + 1))
    view(state, as_of)
    return state


def view(state: PathState, as_of: int) -> dict:
    """Return timestamp_ms-bound descriptive features, never an NRE/probability forecast."""
    _clock(state.last, as_of)
    extension = None
    if len(state.lows) == state.policy.lookback_bars + 1:
        extension = (state.lows[0] - state.running_low) / state.scale
    rebound = (state.last.close - state.running_low) / state.scale
    retention = None
    if state.status == 'RECOVERING':
        retention = (state.last.close - state.confirmed_trough) / (state.peak_close - state.confirmed_trough)
    if any(v is not None and not math.isfinite(v) for v in (extension, rebound, retention)):
        raise ValueError('nonfinite feature calculation')
    return {
        'schema': SCHEMA, 'setup_id': state.setup_id, 'instrument': state.instrument,
        'status': state.status, 'feature_valid': state.status not in ('FAILED', 'EXPIRED') and extension is not None,
        'bar_close_ms': state.last.timestamp_ms + 60000, 'available_at_ms': state.last.available_at_ms,
        'evaluated_at_ms': as_of, 'policy': asdict(state.policy),
        'valid_until_ms': state.last.timestamp_ms + 120000, 'setup_age_bars': state.count - 1,
        'bars_since_low': state.bars_since_low, 'running_low': state.running_low,
        'confirmed_trough': state.confirmed_trough, 'confirmed_at_ms': state.confirmed_at_ms,
        'peak_close': state.peak_close, 'frozen_scale': state.scale,
        'extension_atr': extension, 'rebound_atr': rebound, 'retention': retention,
    }
