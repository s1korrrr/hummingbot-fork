"""Closed-candle stress evidence only; no I/O, order, budget or risk authority."""
from decimal import Decimal
from fractions import Fraction
from typing import Literal

from pydantic import Field, StrictBool, field_validator, model_validator

from hummingbot.strategy_v2.components.candle_data import CandleSnapshot
from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME, ObservationRef
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.recipe_contract import digest

MAX_CLOCK_MS = 2**63 - 1


def checked(model, value):
    if type(value) is not model:
        raise PolicyError(f'exact {model.__name__} required')
    return model.model_validate(value.model_dump(mode='python', warnings='error'))


def clock(value):
    if type(value) is not int or not 0 <= value <= MAX_CLOCK_MS:
        raise PolicyError('bounded integer milliseconds required')
    return value


def bounded_decimal(value):
    if type(value) not in (str, int, float, Decimal):
        raise ValueError('numeric value required, not a boolean')
    result = Decimal(str(value))
    if (not result.is_finite() or len(result.as_tuple().digits) > 40
            or abs(result.as_tuple().exponent) > 40):
        raise ValueError('numeric representation exceeds evidence bounds')
    return result


def publication_ms(value):
    if type(value) not in (float, int):
        raise PolicyError('explicit publication seconds required')
    milliseconds = Fraction(bounded_decimal(value)) * 1000
    return clock(-(-milliseconds.numerator // milliseconds.denominator))


class StressParameters(PolicyModel):
    method: Literal['closed_drawdown_v1']
    lookback_bars: int = Field(ge=1, le=512, strict=True)
    downside_fraction: Decimal = Field(gt=0, lt=1)
    max_age_ms: int = Field(ge=1, le=86_400_000, strict=True)
    max_transport_age_ms: int = Field(ge=1, le=86_400_000, strict=True)
    max_history_rows: int = Field(ge=60, le=10_000, strict=True)

    _amount = field_validator('downside_fraction', mode='before')(bounded_decimal)

    @model_validator(mode='after')
    def capacity(self):
        if self.lookback_bars + 1 > self.max_history_rows:
            raise ValueError('lookback exceeds snapshot capacity')
        return self

    @property
    def fingerprint(self):
        return digest(self.model_dump(mode='json'))


class ReceivedCandles(PolicyModel):
    snapshot: CandleSnapshot
    received_at_ms: int = Field(ge=0, le=MAX_CLOCK_MS, strict=True)

    @field_validator('snapshot', mode='before')
    @classmethod
    def validate_snapshot(cls, value):
        if type(value) is CandleSnapshot:
            value = value.model_dump(mode='python', warnings='error')
        if not isinstance(value, dict):
            raise ValueError('canonical candle snapshot required')
        rows = value.get('rows')
        if not isinstance(rows, (tuple, list)) or not 2 <= len(rows) <= 10_000:
            raise ValueError('bounded canonical candle rows required')
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('canonical candle row required')
            for name in ('open', 'high', 'low', 'close', 'volume', 'quote_volume'):
                if row.get(name) is not None:
                    bounded_decimal(row[name])
        publication_ms(value.get('published_at'))
        snapshot = CandleSnapshot.model_validate(value)
        if snapshot.dataset.interval != '1m':
            raise ValueError('v1 evidence supports confirmed one-minute snapshots only')
        return snapshot

    @model_validator(mode='after')
    def receipt_is_causal(self):
        if self.received_at_ms < publication_ms(self.snapshot.published_at):
            raise ValueError('receipt precedes publication')
        return self

    @property
    def observation(self):
        snap = self.snapshot
        return ObservationRef(dataset_sha256=snap.dataset.key, epoch=snap.epoch,
                              revision=snap.sequence, content_sha256=snap.digest,
                              event_time_ms=(snap.rows[-1].open_time + snap.dataset.seconds) * 1000,
                              available_time_ms=self.received_at_ms)


class RowIdentity(PolicyModel):
    open_time: int = Field(ge=0, le=MAX_CLOCK_MS // 1000, strict=True)
    sha256: str = Field(pattern=HASH)


class StressState(PolicyModel):
    parameters_sha256: str = Field(pattern=HASH)
    observation: ObservationRef
    clock_ms: int = Field(ge=0, le=MAX_CLOCK_MS, strict=True)
    shocked: StrictBool
    window: tuple[RowIdentity, ...] = Field(min_length=2, max_length=10_000)

    @model_validator(mode='after')
    def consistent(self):
        if self.clock_ms < self.observation.available_time_ms:
            raise ValueError('checkpoint clock precedes availability')
        times = [row.open_time for row in self.window]
        if (any(t % 60 for t in times) or any(b - a != 60 for a, b in zip(times, times[1:]))
                or (times[-1] + 60) * 1000 != self.observation.event_time_ms):
            raise ValueError('checkpoint window/cursor mismatch')
        return self


class StressEvaluation(PolicyModel):
    state: StressState | None
    observation: ObservationRef | None
    evaluated_at_ms: int = Field(ge=0, le=MAX_CLOCK_MS, strict=True)
    status: Literal['PASS', 'HOLD', 'UNAVAILABLE', 'STALE']
    reason: str = Field(pattern=NAME)
    fresh: StrictBool
    shocked: StrictBool
    close: Decimal | None = Field(default=None, gt=0)
    low: Decimal | None = Field(default=None, gt=0)

    @model_validator(mode='after')
    def consistent(self):
        if self.state is not None and (self.observation != self.state.observation
                                       or self.state.clock_ms != self.evaluated_at_ms
                                       or self.shocked != self.state.shocked):
            raise ValueError('stress evidence differs from checkpoint')
        if (self.state is None) != (self.observation is None):
            raise ValueError('incomplete stress checkpoint reference')
        if self.fresh and (self.state is None or self.close is None or self.low is None):
            raise ValueError('fresh stress evidence requires candle prices')
        if self.status == 'PASS' and (not self.fresh or self.shocked or self.state is None):
            raise ValueError('passing stress evidence must be fresh and clear')
        if (self.close is None) != (self.low is None):
            raise ValueError('incomplete candle evidence')
        if self.close is not None:
            bounded_decimal(self.close)
            bounded_decimal(self.low)
            if self.low > self.close:
                raise ValueError('invalid close/low evidence')
        return self


def evaluate_stress(state: StressState | None, snapshot: ReceivedCandles | None,
                    parameters: StressParameters, now_ms: int) -> StressEvaluation:
    now_ms = clock(now_ms)
    parameters = checked(StressParameters, parameters)
    if state is not None:
        state = checked(StressState, state)
        if state.parameters_sha256 != parameters.fingerprint or now_ms < state.clock_ms:
            raise PolicyError('stress policy or clock changed')
        if len(state.window) > parameters.max_history_rows:
            raise PolicyError('checkpoint exceeds configured capacity')
        state = StressState(**{**state.model_dump(), 'clock_ms': now_ms})

    def result(status, reason, *, fresh=False, close=None, low=None):
        return StressEvaluation(state=state, observation=None if state is None else state.observation,
                                evaluated_at_ms=now_ms, status=status, reason=reason, fresh=fresh,
                                shocked=False if state is None else state.shocked, close=close, low=low)

    if snapshot is None:
        return result('UNAVAILABLE', 'data_unavailable')
    snapshot = checked(ReceivedCandles, snapshot)
    snap, ref = snapshot.snapshot, snapshot.observation
    if snapshot.received_at_ms > now_ms:
        raise PolicyError('observation is not yet available')
    if state is not None and ref.dataset_sha256 != state.observation.dataset_sha256:
        raise PolicyError('dataset differs from checkpoint')
    if len(snap.rows) > parameters.max_history_rows or snap.dataset.history > parameters.max_history_rows:
        raise PolicyError('snapshot exceeds configured capacity')
    if now_ms - ref.event_time_ms > parameters.max_age_ms:
        return result('STALE', 'candle_stale')
    if (now_ms - ref.available_time_ms > parameters.max_transport_age_ms
            or now_ms - publication_ms(snap.published_at) > parameters.max_transport_age_ms):
        return result('STALE', 'transport_stale')
    if len(snap.rows) < max(snap.dataset.history, parameters.lookback_bars + 1):
        return result('UNAVAILABLE', 'warmup_incomplete')
    prior = max(row.close for row in snap.rows[-parameters.lookback_bars - 1:-1])
    last = snap.rows[-1]
    shocked = Fraction(last.close) <= Fraction(prior) * (1 - Fraction(parameters.downside_fraction))
    window = tuple(RowIdentity(open_time=row.open_time, sha256=digest(row.model_dump(mode='json')))
                   for row in snap.rows)
    reason, fresh = 'seeded', False
    if state is not None:
        old = state.observation
        if ref.event_time_ms < old.event_time_ms or ref.available_time_ms < old.available_time_ms:
            raise PolicyError('observation event/receipt regressed')
        same_epoch = ref.epoch == old.epoch
        if same_epoch and ref.revision < old.revision:
            raise PolicyError('observation revision regressed')
        if same_epoch and ref.revision == old.revision:
            if ref.content_sha256 != old.content_sha256 or ref.event_time_ms != old.event_time_ms:
                raise PolicyError('conflicting observation revision')
        same_event = ref.event_time_ms == old.event_time_ms
        if same_epoch and same_event and ref.content_sha256 == old.content_sha256:
            # Heartbeats cannot move the original availability or renew eligibility.
            return result('HOLD', 'already_observed')
        if not same_epoch or ref.event_time_ms - old.event_time_ms not in (0, 60000):
            reason = 'continuity_reseed'
        elif same_event:
            reason = 'revised_observation'
        else:
            known = {row.open_time: row.sha256 for row in state.window}
            revised = any(row.open_time in known and known[row.open_time] != row.sha256 for row in window)
            reason, fresh = ('revised_history', False) if revised else ('clear', True)
        if not fresh:
            shocked = shocked or state.shocked
    state = StressState(parameters_sha256=parameters.fingerprint, observation=ref,
                        clock_ms=now_ms, shocked=shocked, window=window)
    if fresh and shocked:
        reason = 'stress_downside_move'
    return result('PASS' if fresh and not shocked else 'HOLD', reason,
                  fresh=fresh, close=last.close, low=last.low)
