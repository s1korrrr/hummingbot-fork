"""Opt-in, process-local sharing for modular controllers; native providers are untouched."""
import asyncio
import inspect
import time
from decimal import Decimal
from typing import Literal
from weakref import WeakKeyDictionary

from pydantic import Field, model_validator

from .candle_data import CandleSnapshot, Dataset
from .candle_stream import CandleStreamClient
from .candle_transport import CandleClient, validate_origin
from .config import PolicyModel
from .domain import DataUnavailable, PolicyError, identity
from .feature_cache import FeatureCache
from .universe import pairs


class DataPolicy(PolicyModel):
    mode: Literal['native', 'shared'] = 'native'
    transport: Literal['snapshot', 'stream'] = 'snapshot'
    origin: str | None = None
    token_file: str | None = None
    source_profile: str = Field(default='native', min_length=1, max_length=80)
    symbols: tuple[str, ...] = ()
    minimum_coverage: Decimal = Field(default=Decimal('1'), gt=0, le=1)
    impulse_min_return: Decimal = Field(default=Decimal('.005'), gt=0, lt=1)
    impulse_min_relative_volume: Decimal = Field(default=Decimal('1.5'), gt=0)

    @model_validator(mode='after')
    def validate_transport(self):
        pairs(self.symbols)
        if self.transport == 'stream' and self.mode != 'shared':
            raise ValueError('stream transport requires shared mode')
        if self.mode == 'shared':
            if not self.origin or not self.token_file:
                raise ValueError('shared candles require an authenticated origin and token file')
            validate_origin(self.origin)
            if not self.token_file.startswith('/'):
                raise ValueError('token file must be an absolute path')
        elif self.origin is not None or self.token_file is not None or self.symbols:
            raise ValueError('remote dataset options require shared mode')
        return self


def dataset_for(config, pair=None):
    return Dataset(connector=config.connector_name, trading_pair=pair or config.trading_pair,
                   interval=config.interval, history=config.max_records, source_profile=config.data.source_profile)


class DataPool:
    def __init__(self, policy: DataPolicy, *, clock=time.time):
        self.policy = policy
        self.clock = clock
        self.datasets = {}
        self.references = 0
        self.client = None
        self.features = FeatureCache()
        self.ranking_cache = {}
        self.screen_cache = None

    def attach(self, specs):
        for spec in specs:
            previous = self.datasets.get(spec.key)
            if self.client is not None and (previous is None or spec.history > previous.history):
                raise PolicyError('data subscription expansion requires a controlled reader restart')
        for spec in specs:
            previous = self.datasets.get(spec.key)
            if previous is None or spec.history > previous.history:
                self.datasets[spec.key] = spec
        self.references += 1

    def start(self):
        if self.client is None:
            client_type = CandleStreamClient if self.policy.transport == 'stream' else CandleClient
            self.client = client_type(self.policy.origin, self.policy.token_file, tuple(self.datasets.values()),
                                      clock=self.clock)
            self.client.start()

    async def close(self):
        client, self.client = self.client, None
        if client is not None:
            await client.close()

    def snapshot(self, spec: Dataset, *, now: float, max_age: float) -> CandleSnapshot:
        if self.client is None:
            raise DataUnavailable('shared candle client has not started')
        self.client.replica.frame(spec.key, now=now, max_age=max_age)
        source = self.client.replica.snapshots[spec.key]
        return CandleSnapshot(dataset=spec, epoch=source.epoch, sequence=source.sequence,
                              published_at=source.published_at, rows=source.rows[-spec.history:])

    def entry_ready(self, spec: Dataset) -> bool:
        return self.client is not None and self.client.replica.entry_ready(spec.key)

    def screen(self, config, *, now: float):
        policy = config.universe
        symbols = config.data.symbols if policy.observe_all else policy.symbols
        expected_open = int(now // config.interval_seconds()) * config.interval_seconds() - config.interval_seconds()
        replica = None if self.client is None else self.client.replica
        generation = None if replica is None else replica.revision
        key = (config.universe, config.data, config.connector_name, config.interval,
               config.max_records, config.max_candle_age_seconds, expected_open, generation)
        if self.screen_cache is not None:
            old_key, expires, scores, coverage = self.screen_cache
            if old_key == key and now < expires:
                return dict(scores), coverage
        expires = expected_open + 2 * config.interval_seconds()
        snapshots = {}
        for pair in symbols:
            spec = dataset_for(config, pair)
            try:
                snapshot = self.snapshot(spec, now=now, max_age=config.max_candle_age_seconds)
                if snapshot.rows[-1].open_time == expected_open and self.entry_ready(spec):
                    snapshots[pair] = snapshot
                    expires = min(expires, snapshot.published_at + replica.max_transport_age,
                                  snapshot.rows[-1].open_time + snapshot.dataset.seconds + config.max_candle_age_seconds)
            except DataUnavailable:
                continue
        coverage = Decimal(len(snapshots)) / Decimal(len(symbols)) if symbols else Decimal('0')
        signature = identity([[(pair, snap.digest) for pair, snap in sorted(snapshots.items())],
                              config.universe.model_dump(mode='json'), config.data.model_dump(mode='json')])
        cached = self.ranking_cache.get(signature)
        if cached is None:
            scores = {}
            for pair, snapshot in snapshots.items():
                rows = snapshot.rows
                previous_volume = sum((r.volume for r in rows[-21:-1]), Decimal('0')) / 20
                change = rows[-1].close / rows[-2].close - 1
                relative = rows[-1].volume / previous_volume if previous_volume > 0 else Decimal('0')
                breakout = rows[-1].close > max(r.high for r in rows[-21:-1])
                if breakout and change >= config.data.impulse_min_return and relative >= config.data.impulse_min_relative_volume:
                    scores[pair] = change * relative
            cached = scores
            self.ranking_cache = {signature: scores}
        result = dict(cached)
        self.screen_cache = (key, min(expires, now + 1) if not snapshots else expires, result, coverage)
        return dict(result), coverage


_POOLS = WeakKeyDictionary()


def attach_pool(provider, config):
    policy = config.data
    if policy.mode != 'shared':
        return None
    key = (validate_origin(policy.origin), policy.token_file, policy.transport)
    peers = _POOLS.setdefault(provider, {})
    pool = peers.get(key)
    if pool is None:
        pool = DataPool(policy)
        peers[key] = pool
    symbols = {config.trading_pair, *config.universe.benchmarks}
    if config.universe.mode == 'impulse':
        symbols.update(config.data.symbols if config.universe.observe_all else config.universe.symbols)
    pool.attach(tuple(dataset_for(config, pair) for pair in sorted(symbols)))
    return pool


def release_pool(provider, pool):
    if pool is None:
        return None
    remaining = pool.references - 1
    if remaining < 0:
        raise PolicyError('shared reader released twice')
    if remaining:
        pool.references = remaining
        return None
    task = None
    if pool.client is not None:
        closing = pool.close()
        try:
            task = asyncio.create_task(closing, name='modular-candle-reader-close')
        except BaseException:
            if inspect.getcoroutinestate(closing) == inspect.CORO_CREATED:
                closing.close()
            raise
    # Keep the last owner reachable unless the loop accepted cleanup ownership.
    pool.references = remaining
    peers = _POOLS.get(provider, {})
    for key, value in tuple(peers.items()):
        if value is pool:
            del peers[key]
    return task
