"""Stateless causal features on existing confirmed snapshot/registry contracts."""
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from pydantic import Field, field_validator, model_validator

from hummingbot.strategy_v2.components.candle_data import CandleSnapshot
from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import ObservationRef
from hummingbot.strategy_v2.components.domain import DataUnavailable, PolicyError, decimal
from hummingbot.strategy_v2.components.feature_registry import FeatureRegistry, FeatureSpec
from hummingbot.strategy_v2.components.recipe_contract import canonical_json, digest

from .recipe import HASH, MomentumRecipe, normalized

MAX_CLOCK_MS = 2**53 - 1


def millis(seconds) -> int:
    value = decimal(seconds, 'seconds') * 1000
    if value != value.to_integral_value() or value > MAX_CLOCK_MS:
        raise PolicyError('timestamp cannot be represented exactly in integer milliseconds')
    return int(value)


def clock_ms(value) -> int:
    if type(value) is not int or not 0 <= value <= MAX_CLOCK_MS:
        raise PolicyError('bounded integer millisecond clock required')
    return value


def trend_features(frame, dependencies, parameters):
    period = parameters['period']
    current = frame.close.iloc[-period:].mean()
    previous = frame.close.iloc[-period-1:-1].mean()
    last = frame.close.iloc[-1]
    return {'trend': int(last > current and current > previous) - int(last < current and current < previous)}


def breakout_features(frame, dependencies, parameters):
    p = parameters
    last, previous = frame.iloc[-1], frame.iloc[-2]
    prior = frame.iloc[-p['atr_period']-1:-1]
    prior_close = frame.close.shift(1).loc[prior.index]
    import pandas as pd
    ranges = pd.concat([prior.high-prior.low, (prior.high-prior_close).abs(),
                        (prior.low-prior_close).abs()], axis=1).max(axis=1)
    atr = float(ranges.mean())
    volume_reference = float(frame.volume.iloc[-p['volume_period']-1:-1].median())
    if atr <= 0 or volume_reference <= 0:
        raise DataUnavailable('positive prior ATR and volume reference required')
    return {'open': last.open, 'high': last.high, 'low': last.low, 'close': last.close,
            'volume': last.volume, 'prior_high': frame.high.iloc[-p['lookback']-1:-1].max(),
            'atr': atr, 'return_1m': last.close/previous.close-1,
            'relative_volume': last.volume/volume_reference,
            'trend_5m': dependencies[p['trend5']].as_dict()['trend'],
            'trend_15m': dependencies[p['trend15']].as_dict()['trend']}


class Measurements(PolicyModel):
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    prior_high: Decimal = Field(gt=0)
    atr: Decimal = Field(gt=0)
    return_1m: Decimal = Field(gt=-1)
    relative_volume: Decimal = Field(ge=0)
    trend_5m: Decimal
    trend_15m: Decimal

    @field_validator('*', mode='before')
    @classmethod
    def finite_numeric(cls, value):
        return decimal(value, 'measurement', minimum=None)

    @model_validator(mode='after')
    def geometry(self):
        if (not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high
                or self.trend_5m not in (-1, 0, 1) or self.trend_15m not in (-1, 0, 1)):
            raise PolicyError('invalid OHLC or trend measurement')
        return self


class ObservedFeatures(PolicyModel):
    recipe_sha256: str = Field(pattern=HASH)
    contract_sha256: str = Field(pattern=HASH)
    result_sha256: str = Field(pattern=HASH)
    primary: ObservationRef
    dependencies: tuple[ObservationRef, ObservationRef]
    values: Measurements

    @model_validator(mode='after')
    def unique_inputs(self):
        if len({r.dataset_sha256 for r in (self.primary, *self.dependencies)}) != 3:
            raise PolicyError('three distinct required datasets are needed')
        return self


class FeaturePipeline:
    """Registry adapter only. Caller supplies finalized 1m/5m/15m streams; no feeds start."""

    def __init__(self, recipe: MomentumRecipe):
        self.recipe = normalized(recipe)
        r, p = self.recipe, self.recipe.features
        implementation = sha256(Path(__file__).read_bytes()).hexdigest()
        suffix = digest([p.model_dump(mode='json'), [d.key for d in r.datasets], r.initialization_sha256])[:12]
        names = (f'mfv2.base.{suffix}', f'mfv2.trend5.{suffix}', f'mfv2.trend15.{suffix}')
        self.name = names[0]
        self.registry = FeatureRegistry(32)
        definitions = []
        for d, name in zip(r.datasets[1:], names[1:]):
            definitions.append((d, name, trend_features, {'period': p.trend_period}, (), ('trend',)))
        params = {**p.model_dump(mode='json'), 'trend5': names[1], 'trend15': names[2]}
        definitions.append((r.datasets[0], names[0], breakout_features, params, names[1:],
                            tuple(Measurements.model_fields)))
        specs = []
        for d, name, compute, parameters, deps, outputs in definitions:
            spec = FeatureSpec(name=name, version=1, dataset_key=d.key,
                implementation_sha256=implementation,
                implementation_symbol=compute.__module__ + '.' + compute.__qualname__,
                initialization_sha256=r.initialization_sha256, parameters_json=canonical_json(parameters),
                required_fields=('open', 'high', 'low', 'close', 'volume'), output_fields=outputs,
                dependencies=deps, min_records=d.history,
                max_candle_age=r.max_candle_age_ms/1000 + (d.seconds if d.interval != '1m' else 0),
                max_transport_age=r.max_transport_age_ms/1000)
            self.registry.register(spec, compute)
            specs.append(spec.model_dump(mode='json'))
        self.registry.seal()
        self.contract_sha256 = digest(specs)

    def evaluate(self, snapshots, *, now_ms: int) -> ObservedFeatures:
        now_ms = clock_ms(now_ms)
        r = self.recipe
        if set(snapshots) != {d.key for d in r.datasets}:
            raise DataUnavailable('exactly three recipe-bound snapshot streams are required')
        clean, refs = {}, []
        for d in r.datasets:
            supplied = snapshots[d.key]
            if isinstance(supplied.published_at, bool):
                raise PolicyError('publication time must not be boolean')
            value = CandleSnapshot.model_validate(supplied.model_dump(mode='json'))
            if value.dataset != d:
                raise PolicyError('snapshot differs from recipe dataset/history')
            published = millis(value.published_at)
            event = (value.rows[-1].open_time + d.seconds)*1000
            clock_ms(event)
            if event > now_ms or not 0 <= now_ms-published <= r.max_transport_age_ms:
                raise DataUnavailable('future or stale publication')
            clean[d.key] = value
            refs.append(ObservationRef(dataset_sha256=d.key, epoch=value.epoch, revision=value.sequence,
                content_sha256=value.digest, event_time_ms=event, available_time_ms=published))
        if now_ms-refs[0].event_time_ms > r.max_candle_age_ms:
            raise DataUnavailable('base candle is stale')
        for d, ref in zip(r.datasets[1:], refs[1:]):
            expected = (refs[0].event_time_ms // (d.seconds*1000))*(d.seconds*1000)
            if ref.event_time_ms != expected:
                raise DataUnavailable('higher-timeframe snapshot is not the latest completed bucket')
        result = self.registry.evaluate(self.name, clean, now=now_ms/1000)
        return ObservedFeatures(recipe_sha256=r.digest, contract_sha256=self.contract_sha256,
            result_sha256=result.result_sha256, primary=refs[0], dependencies=tuple(refs[1:]),
            values=Measurements.model_validate(result.as_dict()))
