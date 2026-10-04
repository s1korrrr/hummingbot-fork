"""Immutable, finalized candle snapshots; no exchange orders or strategy state."""
import asyncio
import math
import sqlite3
import weakref
from decimal import Decimal
from pathlib import Path
from typing import Callable, Literal

import pandas as pd
from pydantic import Field, StrictBool, field_validator, model_validator

from .config import PolicyModel
from .domain import DataUnavailable, PolicyError, decimal, identity

SCHEMA_VERSION = 1
MAX_CANDLE_HISTORY = 10_000
# Confirmed 1m OKX spot is the admitted first delivery. Do not silently add 3m/30m or extra venues.
FIRST_DELIVERY = {
    'connector': 'okx',
    'source_domain': 'okx',
    'market_type': 'spot',
    'interval': '1m',
    'price_type': 'trade',
    'boundary': 'utc_epoch',
    'finality': 'confirmed',
}
# Measured local-fixture bound for first delivery. Not the hub's configured 1000-dataset cap.
FIRST_DELIVERY_LOAD = {
    'connector': 'okx',
    'interval': '1m',
    'feeds': 2,
    'history': 90,
    'consumers': 2,
}
ADMITTED_NATIVE_CONNECTORS = frozenset({'okx'})
INTERVALS = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400}
PAIR_PATTERN = r'^[A-Z0-9]+-[A-Z0-9]+$'
_EXECUTION_TABLES = frozenset({
    'orders', 'order', 'tradefill', 'trade_fill', 'trade_fills', 'executors', 'positions', 'position',
})


def reject_execution_database(path):
    """Refuse execution/owner sqlite and unidentified files. First delivery is in-memory bounded recovery."""
    target = Path(path)
    if target.exists() and (target.is_symlink() or not target.is_file()):
        raise ValueError('refusing foreign database; use a separate market-data path')
    if not target.exists() or target.stat().st_size == 0:
        raise ValueError('refusing foreign database; use a separate market-data path')
    try:
        db = sqlite3.connect(f'file:{target.resolve()}?mode=ro', uri=True)
        try:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            db.close()
    except sqlite3.Error as exc:
        raise ValueError('refusing foreign database; use a separate market-data path') from exc
    if {name.lower() for name in tables} & _EXECUTION_TABLES:
        raise ValueError('refusing foreign database; use a separate market-data path')
    raise ValueError('refusing foreign database; use a separate market-data path')


class Dataset(PolicyModel):
    connector: str = Field(pattern=r'^[a-z0-9_]+$', max_length=80)
    trading_pair: str = Field(pattern=PAIR_PATTERN, max_length=80)
    interval: Literal['1m', '5m', '15m', '1h', '4h'] = '1m'
    market_type: Literal['spot'] = 'spot'
    source_domain: Literal['okx'] = 'okx'
    source_profile: str = Field(default='native', min_length=1, max_length=80)
    price_type: Literal['trade'] = 'trade'
    boundary: Literal['utc_epoch'] = 'utc_epoch'
    history: int = Field(default=300, ge=60, le=MAX_CANDLE_HISTORY, strict=True)

    @model_validator(mode='after')
    def admitted_native_spot(self):
        base, quote = self.trading_pair.split('-')
        if base == quote:
            raise ValueError('base and quote must differ')
        if self.source_profile == 'native' and (
                self.connector not in ADMITTED_NATIVE_CONNECTORS or self.source_domain != self.connector
                or self.market_type != 'spot' or self.price_type != 'trade' or self.boundary != 'utc_epoch'):
            raise ValueError('native datasets are limited to admitted confirmed OKX spot')
        return self

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
    schema_version: Literal[1] = SCHEMA_VERSION
    dataset: Dataset
    epoch: str = Field(pattern=r'^[A-Za-z0-9_-]{1,128}$')
    sequence: int = Field(ge=1, strict=True)
    published_at: float = Field(ge=0)
    rows: tuple[Candle, ...] = Field(min_length=2, max_length=MAX_CANDLE_HISTORY)

    @field_validator('schema_version', mode='before')
    @classmethod
    def integer_schema_version(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value

    @model_validator(mode='after')
    def continuity(self):
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError('unsupported candle schema version')
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
        return _snapshot_frame(self).copy(deep=True)


_SNAPSHOT_FRAMES: dict[int, pd.DataFrame] = {}


def _snapshot_frame(snapshot: 'CandleSnapshot') -> pd.DataFrame:
    """The float feature frame of one immutable snapshot, converted once.

    A snapshot is frozen, so its conversion never changes; consumers asked for it
    several times per tick and rebuilt thousands of rows each time. Keyed by object
    identity (pydantic equality would include any private cache) and evicted when the
    snapshot is collected. Callers receive copies, so the cached frame is never mutated.
    """
    key = id(snapshot)
    cached = _SNAPSHOT_FRAMES.get(key)
    if cached is not None:
        return cached
    values = [{'timestamp': r.open_time, 'open': float(r.open), 'high': float(r.high),
               'low': float(r.low), 'close': float(r.close), 'volume': float(r.volume)} for r in snapshot.rows]
    if all(r.quote_volume is not None for r in snapshot.rows):
        for value, row in zip(values, snapshot.rows):
            value['quote_asset_volume'] = float(row.quote_volume)
    if any(not math.isfinite(value) for row in values for value in row.values()):
        raise PolicyError('candle values exceed the native feature representation')
    cached = _SNAPSHOT_FRAMES[key] = pd.DataFrame(values)
    weakref.finalize(snapshot, _SNAPSHOT_FRAMES.pop, key, None)
    return cached


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
            if old is None:
                self.seed_bars[key] = latest
            elif latest != old.rows[-1].open_time:
                if (snapshot.sequence != old.sequence + 1
                        or latest != old.rows[-1].open_time + snapshot.dataset.seconds):
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

    def __init__(self, datasets: tuple[Dataset, ...], *, factory: Callable, epoch: str, max_age: float = 120,
                 checkpoint=None):
        if checkpoint is not None:
            reject_execution_database(checkpoint)
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
        self.prior: dict[str, CandleSnapshot] = {}
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
            self.errors = {key: error for key, error in self.errors.items() if error != 'producer stopped'}
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
        for key in self.datasets:
            self.errors.setdefault(key, 'producer stopped')
        if errors:
            raise PolicyError(f'native candle cleanup failed: {errors}')

    async def capture_async(self, *, now: float):
        """Yield between independent datasets so sockets/watchdogs retain event-loop time."""
        for key in tuple(self.feeds):
            self.capture(now=now, keys=(key,))
            await asyncio.sleep(0)

    def capture(self, *, now: float, keys=None):
        if not self.started:
            raise DataUnavailable('candle hub not started')
        now = float(decimal(now, 'capture time'))
        for key in tuple(self.feeds) if keys is None else keys:
            feed = self.feeds[key]
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
                    self.prior[key] = old
                    candidate = candidate.model_copy(update={'sequence': old.sequence + 1})
                self.snapshots[key] = candidate
                self.errors.pop(key, None)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as exc:
                old = self.snapshots.get(key)
                retained = False
                if old is not None:
                    try:
                        old.frame(now=now, max_age=self.max_age)
                        retained = True
                    except PolicyError:
                        retained = False
                if retained:
                    self.errors.pop(key, None)
                    continue
                self.errors[key] = f'{type(exc).__name__}: {exc}'

    def latest(self, key: str) -> CandleSnapshot:
        if not self.started or key in self.errors or key not in self.snapshots:
            raise DataUnavailable(self.errors.get(key, 'candle snapshot unavailable'))
        return self.snapshots[key]
