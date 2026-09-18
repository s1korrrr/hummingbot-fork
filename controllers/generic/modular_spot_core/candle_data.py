"""Immutable, finalized candle snapshots; no exchange orders or strategy state."""
from decimal import Decimal
import math
from typing import Callable, Literal

import pandas as pd
from pydantic import Field, StrictBool, model_validator

from .config import PolicyModel
from .domain import DataUnavailable, PolicyError, decimal, identity

INTERVALS = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400}
PAIR_PATTERN = r'^[A-Z0-9]+-[A-Z0-9]+$'


class Dataset(PolicyModel):
    connector: str = Field(pattern=r'^[a-z0-9_]+$', max_length=80)
    trading_pair: str = Field(pattern=PAIR_PATTERN, max_length=80)
    interval: Literal['1m', '5m', '15m', '1h', '4h'] = '1m'
    market_type: Literal['spot'] = 'spot'
    source_profile: str = Field(default='native', min_length=1, max_length=80)
    history: int = Field(default=300, ge=60, le=5000, strict=True)

    @property
    def key(self) -> str:
        return identity(self.model_dump(mode='json', exclude={'history'}))

    @property
    def seconds(self) -> int:
        return INTERVALS[self.interval]


class Candle(PolicyModel):
    open_time: int = Field(ge=0, strict=True)
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    quote_volume: Decimal | None = Field(default=None, ge=0)
    confirmed: StrictBool = True

    @model_validator(mode='after')
    def geometry(self):
        if not self.confirmed or not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError('unconfirmed candle or invalid OHLC geometry')
        return self


class CandleSnapshot(PolicyModel):
    schema_version: Literal[1] = 1
    dataset: Dataset
    epoch: str = Field(pattern=r'^[A-Za-z0-9_-]{1,128}$')
    sequence: int = Field(ge=1, strict=True)
    published_at: float = Field(ge=0)
    rows: tuple[Candle, ...] = Field(min_length=2, max_length=5000)

    @model_validator(mode='after')
    def continuity(self):
        if type(self.schema_version) is not int:
            raise ValueError('invalid schema version')
        times = [row.open_time for row in self.rows]
        step = self.dataset.seconds
        if len(times) > self.dataset.history or any(t % step for t in times):
            raise ValueError('invalid snapshot capacity or candle alignment')
        if any(b - a != step for a, b in zip(times, times[1:])):
            raise ValueError('unordered, duplicated or gapped finalized candles')
        if times[-1] + step > self.published_at:
            raise ValueError('publication precedes finalized candle close')
        return self

    @property
    def digest(self) -> str:
        return identity([self.dataset.key, [row.model_dump(mode='json') for row in self.rows]])

    def frame(self, *, now: float, max_age: float) -> pd.DataFrame:
        now = float(decimal(now, 'observation time'))
        age = float(decimal(max_age, 'maximum candle age', positive=True))
        if now < self.published_at or now - (self.rows[-1].open_time + self.dataset.seconds) > age:
            raise DataUnavailable('future or stale candle snapshot')
        if len(self.rows) < self.dataset.history:
            raise DataUnavailable('candle history is warming')
        values = [{'timestamp': r.open_time, 'open': float(r.open), 'high': float(r.high),
                   'low': float(r.low), 'close': float(r.close), 'volume': float(r.volume)} for r in self.rows]
        if all(r.quote_volume is not None for r in self.rows):
            for value, row in zip(values, self.rows):
                value['quote_asset_volume'] = float(row.quote_volume)
        if any(not math.isfinite(value) for row in values for value in row.values()):
            raise PolicyError('candle values exceed the native feature representation')
        return pd.DataFrame(values)


class Replica:
    """One consumer's full-snapshot cursor; gaps never replay historical signals."""

    def __init__(self, datasets: tuple[Dataset, ...], max_transport_age: float = 15):
        if not datasets or len({d.key for d in datasets}) != len(datasets):
            raise ValueError('consumer datasets must be nonempty and unique')
        self.datasets = {d.key: d for d in datasets}
        self.max_transport_age = float(decimal(max_transport_age, 'transport age', positive=True))
        self.snapshots: dict[str, CandleSnapshot] = {}
        self.seed_bars: dict[str, int] = {}
        self.connected = False
        self.epoch = None
        self.revision = 0

    def begin_session(self):
        self.revision += 1
        self.connected = True
        self.epoch = None
        self.snapshots.clear()
        self.seed_bars.clear()

    def disconnect(self):
        self.revision += 1
        self.connected = False

    def accept(self, snapshot: CandleSnapshot, *, now: float) -> bool:
        try:
            self.revision += 1
            now = float(decimal(now, 'receipt time'))
            key = snapshot.dataset.key
            requested = self.datasets.get(key)
            if not self.connected or requested is None:
                raise PolicyError('unsubscribed or disconnected candle source')
            if not 0 <= now - snapshot.published_at <= self.max_transport_age:
                raise PolicyError('stale or future candle publication')
            if snapshot.dataset.history < requested.history:
                raise PolicyError('producer history is smaller than admitted request')
            if self.epoch is not None and self.epoch != snapshot.epoch:
                raise PolicyError('producer epoch changed inside a session')
            self.epoch = snapshot.epoch
            old = self.snapshots.get(key)
            latest = snapshot.rows[-1].open_time
            if old is not None:
                if snapshot.sequence < old.sequence or latest < old.rows[-1].open_time:
                    raise PolicyError('candle stream regressed')
                if snapshot.published_at < old.published_at:
                    raise PolicyError('publication time regressed')
                if snapshot.sequence == old.sequence:
                    if snapshot.digest != old.digest:
                        raise PolicyError('conflicting candle snapshot at the same sequence')
                    self.snapshots[key] = snapshot
                    return False
            if old is None or snapshot.sequence != old.sequence + 1 or latest != old.rows[-1].open_time + snapshot.dataset.seconds:
                self.seed_bars[key] = latest
            self.snapshots[key] = snapshot
            return True
        except (PolicyError, ValueError, AttributeError):
            self.disconnect()
            raise

    def touch(self, key: str, *, epoch: str, sequence: int, digest: str, published_at: float, now: float):
        try:
            current = self.snapshots.get(key)
            now = float(decimal(now, 'heartbeat receipt'))
            published_at = float(decimal(published_at, 'heartbeat observation'))
            if (not self.connected or current is None or epoch != self.epoch
                    or type(sequence) is not int or sequence != current.sequence or digest != current.digest
                    or not current.published_at <= published_at <= now
                    or now - published_at > self.max_transport_age):
                raise PolicyError('heartbeat does not match a current candle revision')
            self.snapshots[key] = current.model_copy(update={'published_at': published_at})
            self.revision += 1
        except (PolicyError, ValueError, TypeError):
            self.disconnect()
            raise

    def entry_ready(self, key: str) -> bool:
        value = self.snapshots.get(key)
        return self.connected and value is not None and value.rows[-1].open_time > self.seed_bars.get(key, value.rows[-1].open_time)

    def frame(self, key: str, *, now: float, max_age: float) -> pd.DataFrame:
        if not self.connected or key not in self.snapshots:
            raise DataUnavailable('candle reader has not synchronized')
        value = self.snapshots[key]
        if not 0 <= now - value.published_at <= self.max_transport_age:
            raise DataUnavailable('candle transport is stale')
        frame = value.frame(now=now, max_age=max_age)
        return frame.iloc[-self.datasets[key].history:].copy(deep=True)


class SnapshotHub:
    """One native feed per dataset, sized before startup; readers never stop feeds."""

    def __init__(self, datasets: tuple[Dataset, ...], *, factory: Callable, epoch: str, max_age: float = 120):
        if not datasets or len(datasets) > 1000:
            raise ValueError('one to one thousand datasets must be explicitly admitted')
        self.datasets = {}
        for spec in datasets:
            previous = self.datasets.get(spec.key)
            if previous is None or previous.history < spec.history:
                self.datasets[spec.key] = spec
        if sum(spec.history for spec in self.datasets.values()) > 1_000_000:
            raise ValueError('total admitted candle history exceeds one million rows')
        self.factory, self.epoch = factory, epoch
        self.max_age = float(decimal(max_age, 'maximum age', positive=True))
        self.feeds = {}
        self.snapshots: dict[str, CandleSnapshot] = {}
        self.errors: dict[str, str] = {}
        self.started = False

    def start(self):
        if self.started:
            return
        try:
            for key, spec in self.datasets.items():
                feed = self.factory(spec)
                self.feeds[key] = feed
                feed.start()
            self.started = True
        except Exception:
            self.stop()
            raise

    def stop(self):
        feeds, self.feeds = self.feeds, {}
        self.started = False
        errors = []
        for key, feed in feeds.items():
            try:
                feed.stop()
            except Exception as exc:
                errors.append((key, type(exc).__name__))
        self.errors = {key: 'producer stopped' for key in self.datasets}
        if errors:
            raise PolicyError(f'native candle cleanup failed: {errors}')

    def capture(self, *, now: float):
        if not self.started:
            raise DataUnavailable('candle hub not started')
        now = float(decimal(now, 'capture time'))
        for key, feed in self.feeds.items():
            try:
                if getattr(feed, 'closed_candles_only', False) is not True:
                    raise PolicyError('unconfirmed native feed: explicit finality adapter required')
                spec = self.datasets[key]
                data = feed.candles_df.tail(spec.history).copy(deep=True)
                parsed = []
                for row in data.to_dict('records'):
                    stamp = decimal(row['timestamp'], 'candle timestamp')
                    if stamp != int(stamp):
                        raise PolicyError('fractional candle timestamp')
                    parsed.append(Candle(open_time=int(stamp), open=row['open'], high=row['high'], low=row['low'],
                                         close=row['close'], volume=row['volume'],
                                         quote_volume=row.get('quote_asset_volume')))
                old = self.snapshots.get(key)
                candidate = CandleSnapshot(dataset=spec, epoch=self.epoch, sequence=1 if old is None else old.sequence,
                                           published_at=now, rows=tuple(parsed))
                candidate.frame(now=now, max_age=self.max_age)
                if old is not None and candidate.digest != old.digest:
                    candidate = candidate.model_copy(update={'sequence': old.sequence + 1})
                self.snapshots[key] = candidate
                self.errors.pop(key, None)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as exc:
                self.errors[key] = f'{type(exc).__name__}: {exc}'

    def latest(self, key: str) -> CandleSnapshot:
        if not self.started or key in self.errors or key not in self.snapshots:
            raise DataUnavailable(self.errors.get(key, 'candle snapshot unavailable'))
        return self.snapshots[key]
