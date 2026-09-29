"""Causal event-time alignment of confirmed candles. Late, unconfirmed or conflicting inputs never backfill."""
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Literal, Sequence

import numpy as np
from pydantic import Field, StrictBool, model_validator

from hummingbot.strategy_v2.components.config import PolicyModel

from .contracts import (
    ASSET_PATTERN,
    EPOCH_PATTERN,
    INSTRUMENT_PATTERN,
    MAX_TIMESTAMP_MS,
    MINUTE_MS,
    SHA256_PATTERN,
    VENUE_PATTERN,
    ClockView,
    ContextError,
)
from .universe import UniverseBinding

MAX_HISTORY_MINUTES = 20_160
MAX_BLOCK_ROWS = MAX_HISTORY_MINUTES + 1
MAX_BATCH_ROWS = 3_000_000
MAX_BLOCKS = 16_384
MAX_SOURCE_EPOCHS = 64
AvailabilityKind = Literal['observed', 'modeled']


class Instrument(PolicyModel):
    dataset_id: str = Field(min_length=1, max_length=128)
    venue: str = Field(pattern=VENUE_PATTERN)
    instrument_id: str = Field(pattern=INSTRUMENT_PATTERN)
    asset_id: str = Field(pattern=ASSET_PATTERN)
    numeraire: str = Field(pattern=ASSET_PATTERN)
    interval_ms: Literal[60_000]


class CandleObservation(PolicyModel):
    """One confirmed-or-not source candle with explicit availability, revision and raw lineage."""
    instrument: Instrument
    open_time_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)
    close_time_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    base_volume: Decimal = Field(ge=0)
    quote_volume: Decimal | None = Field(ge=0)
    confirmed: StrictBool
    source_epoch: str = Field(pattern=EPOCH_PATTERN)
    source_sequence: int | None = Field(ge=0, strict=True)
    received_at_ms: int | None = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)
    available_at_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS, strict=True)
    availability_kind: AvailabilityKind
    revision: int = Field(ge=0, le=1_000_000, strict=True)
    raw_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode='after')
    def causal_candle(self):
        prices = (self.open, self.high, self.low, self.close, self.base_volume)
        if any(not v.is_finite() for v in prices) or (self.quote_volume is not None and not self.quote_volume.is_finite()):
            raise ValueError('nonfinite candle value')
        if self.open_time_ms % self.instrument.interval_ms or self.close_time_ms != self.open_time_ms + self.instrument.interval_ms:
            raise ValueError('candle is not aligned to its interval')
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError('invalid OHLC bounds')
        if self.available_at_ms < self.close_time_ms or (
                self.received_at_ms is not None and self.available_at_ms < self.received_at_ms):
            raise ValueError('a candle cannot be available before it closed or was received')
        return self


def _frozen(values, dtype, name, length):
    array = np.array(values, dtype=dtype, copy=True)
    if array.ndim != 1 or len(array) != length:
        raise ContextError(('INPUT_INVALID',), f'{name} must be a one-dimensional aligned column')
    array.setflags(write=False)
    return array


@dataclass(frozen=True)
class CandleBlock:
    """Columnar candles for one instrument and one availability kind; open times strictly increase."""
    instrument: Instrument
    availability_kind: AvailabilityKind
    source_epochs: tuple[str, ...]
    open_time_ms: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    base_volume: np.ndarray
    quote_volume: np.ndarray | None
    confirmed: np.ndarray
    available_at_ms: np.ndarray
    revision: np.ndarray
    epoch_index: np.ndarray
    source_sequence: np.ndarray
    raw_sha256: np.ndarray

    def __post_init__(self):
        if not isinstance(self.instrument, Instrument) or self.availability_kind not in ('observed', 'modeled'):
            raise ContextError(('INPUT_INVALID',), 'block identity is invalid')
        if not 1 <= len(self.source_epochs) <= MAX_SOURCE_EPOCHS or len(set(self.source_epochs)) != len(self.source_epochs):
            raise ContextError(('INPUT_INVALID',), 'source epochs must be bounded and unique')
        n = len(np.asarray(self.open_time_ms))
        if not 1 <= n <= MAX_BLOCK_ROWS:
            raise ContextError(('CAPACITY_EXCEEDED',), 'block row count is out of bounds')
        columns = {'open_time_ms': '<i8', 'open': '<f8', 'high': '<f8', 'low': '<f8', 'close': '<f8',
                   'base_volume': '<f8', 'confirmed': bool, 'available_at_ms': '<i8', 'revision': '<i8',
                   'epoch_index': '<i8', 'source_sequence': '<i8', 'raw_sha256': 'S32'}
        for name, dtype in columns.items():
            object.__setattr__(self, name, _frozen(getattr(self, name), dtype, name, n))
        if self.quote_volume is not None:
            object.__setattr__(self, 'quote_volume', _frozen(self.quote_volume, '<f8', 'quote_volume', n))
        floats = [self.open, self.high, self.low, self.close, self.base_volume]
        if self.quote_volume is not None:
            floats.append(self.quote_volume)
        if not all(np.isfinite(column).all() for column in floats):
            raise ContextError(('INPUT_NONFINITE',))
        interval = self.instrument.interval_ms
        times = self.open_time_ms
        if ((times % interval) != 0).any() or (np.diff(times) <= 0).any() or times[0] < 0:
            raise ContextError(('INPUT_INVALID',), 'open times must be aligned and strictly increasing')
        if ((self.open <= 0) | (self.high <= 0) | (self.low <= 0) | (self.close <= 0)).any() \
                or (self.base_volume < 0).any() or (self.quote_volume is not None and (self.quote_volume < 0).any()):
            raise ContextError(('INPUT_INVALID',), 'nonpositive price or negative volume')
        lower = np.minimum(self.open, self.close)
        upper = np.maximum(self.open, self.close)
        if ((self.low > lower) | (upper > self.high)).any():
            raise ContextError(('INPUT_INVALID',), 'invalid OHLC bounds')
        if (self.available_at_ms < times + interval).any() or (self.revision < 0).any() or (self.source_sequence < 0).any():
            raise ContextError(('INPUT_INVALID',), 'availability precedes close or negative lineage')
        if (self.epoch_index < 0).any() or (self.epoch_index >= len(self.source_epochs)).any():
            raise ContextError(('INPUT_INVALID',), 'unknown source epoch index')

    @property
    def rows(self) -> int:
        return len(self.open_time_ms)


@dataclass(frozen=True)
class CandleBatch:
    cutoff_ms: int
    blocks: tuple[CandleBlock, ...]

    def __post_init__(self):
        if type(self.cutoff_ms) is not int or self.cutoff_ms % MINUTE_MS or not 0 <= self.cutoff_ms <= MAX_TIMESTAMP_MS:
            raise ContextError(('INPUT_INVALID',), 'batch cutoff must lie on a UTC minute boundary')
        object.__setattr__(self, 'blocks', tuple(self.blocks))
        if len(self.blocks) > MAX_BLOCKS or sum(b.rows for b in self.blocks) > MAX_BATCH_ROWS:
            raise ContextError(('CAPACITY_EXCEEDED',), 'candle batch exceeds its bound')
        if any(not isinstance(b, CandleBlock) for b in self.blocks):
            raise ContextError(('INPUT_INVALID',), 'candle batch holds only candle blocks')


def batch_from_observations(cutoff_ms: int, observations: Sequence[CandleObservation]) -> CandleBatch:
    """Group row contracts into columnar blocks; duplicate keys become separate revision layers."""
    layers: dict[tuple, list[list[CandleObservation]]] = {}
    for row in sorted(observations, key=lambda r: (r.instrument.instrument_id, r.availability_kind, r.open_time_ms,
                                                   r.revision, r.available_at_ms, r.source_sequence or 0)):
        groups = layers.setdefault((row.instrument, row.availability_kind), [])
        for group in groups:
            if group[-1].open_time_ms < row.open_time_ms:
                group.append(row)
                break
        else:
            groups.append([row])
    blocks = []
    for (instrument, kind), groups in sorted(layers.items(), key=lambda item: (item[0][0].instrument_id, item[0][1])):
        for rows in groups:
            epochs = tuple(sorted({r.source_epoch for r in rows}))
            quote = None if any(r.quote_volume is None for r in rows) else [float(r.quote_volume) for r in rows]
            blocks.append(CandleBlock(
                instrument=instrument, availability_kind=kind, source_epochs=epochs,
                open_time_ms=[r.open_time_ms for r in rows], open=[float(r.open) for r in rows],
                high=[float(r.high) for r in rows], low=[float(r.low) for r in rows],
                close=[float(r.close) for r in rows], base_volume=[float(r.base_volume) for r in rows],
                quote_volume=quote, confirmed=[r.confirmed for r in rows],
                available_at_ms=[r.available_at_ms for r in rows], revision=[r.revision for r in rows],
                epoch_index=[epochs.index(r.source_epoch) for r in rows],
                source_sequence=[r.source_sequence if r.source_sequence is not None else 0 for r in rows],
                raw_sha256=[bytes.fromhex(r.raw_sha256) for r in rows]))
    return CandleBatch(cutoff_ms=cutoff_ms, blocks=tuple(blocks))


class AlignmentConfig(PolicyModel):
    interval_ms: Literal[60_000]
    history_minutes: int = Field(ge=1, le=MAX_HISTORY_MINUTES, strict=True)


@dataclass(frozen=True)
class AlignedPanel:
    """Close-time panel ending at the cutoff. Masked slots hold 0.0 (never a valid price) and are never filled."""
    cutoff_ms: int
    decision_ms: int
    interval_ms: int
    times: np.ndarray
    asset_ids: tuple[str, ...]
    instrument_ids: tuple[str, ...]
    closes: np.ndarray
    valid: np.ndarray
    asset_reasons: tuple[tuple[str, tuple[str, ...]], ...]
    universe: UniverseBinding
    max_input_available_at_ms: int | None
    input_manifest_digest: str
    availability_kind: AvailabilityKind


def _asset_reasons(valid_row, conflict, late, unconfirmed) -> tuple[str, ...]:
    """Stable per-member reasons: causes of invalid slots, warmup prefix, interior gaps and a missing latest bar."""
    reasons = {'INPUT_REVISION_CONFLICT'} if conflict.any() else set()
    if (late & ~valid_row).any():
        reasons.add('INPUT_STALE')
    if (unconfirmed & ~valid_row).any():
        reasons.add('INPUT_UNCONFIRMED')
    if not valid_row.any():
        return tuple(sorted(reasons or {'INPUT_MISSING'}))
    first = int(np.argmax(valid_row))
    if first > 0:
        reasons.add('WARMUP_INCOMPLETE')
    if not valid_row[first:-1].all():
        reasons.add('INPUT_GAP')
    if not valid_row[-1] and not (conflict[-1] or late[-1] or unconfirmed[-1]):
        reasons.add('INPUT_MISSING')
    return tuple(sorted(reasons))


def align_panel(inputs: CandleBatch, universe: UniverseBinding, config: AlignmentConfig,
                clock: ClockView) -> AlignedPanel:
    """Select, per member and close time, the highest revision confirmed and available by the decision."""
    cutoff, decision, interval = inputs.cutoff_ms, clock.decision_ms, config.interval_ms
    if decision < cutoff:
        raise ContextError(('CONTEXT_FUTURE',), 'decision precedes the cutoff')
    length = config.history_minutes + 1
    start = cutoff - (length - 1) * interval
    members = {asset: i for i, asset in enumerate(universe.member_asset_ids)}
    n = len(members)
    quote_mismatch = np.zeros(n, dtype=bool)
    parts = []
    lineage = []
    for block in inputs.blocks:
        i = members.get(block.instrument.asset_id)
        if i is None:
            continue
        if block.instrument.interval_ms != interval:
            raise ContextError(('INPUT_INVALID',), 'candle interval differs from the alignment interval')
        if block.instrument.numeraire != universe.numeraire or block.instrument.venue != universe.venue:
            quote_mismatch[i] = True
            continue
        if block.instrument.instrument_id != universe.member_instrument_ids[i]:
            continue  # a non-canonical instrument for a member underlying never votes
        close_time = block.open_time_ms + interval
        inside = (close_time >= start) & (close_time <= cutoff)
        if not inside.any():
            continue
        epochs = np.array(block.source_epochs, dtype=object)[block.epoch_index[inside]]
        lineage.append((i, block.availability_kind, close_time[inside], block.revision[inside],
                        epochs, block.source_sequence[inside], block.raw_sha256[inside]))
        parts.append((np.full(int(inside.sum()), i, dtype='<i8'), (close_time[inside] - start) // interval,
                      block.revision[inside], block.available_at_ms[inside], block.confirmed[inside],
                      np.column_stack([block.open[inside], block.high[inside], block.low[inside], block.close[inside],
                                       block.base_volume[inside],
                                       block.quote_volume[inside] if block.quote_volume is not None
                                       else np.full(int(inside.sum()), -1.0)]),
                      np.full(int(inside.sum()), len(lineage) - 1, dtype='<i8'),
                      np.arange(int(inside.sum()), dtype='<i8'),
                      np.full(int(inside.sum()), block.availability_kind == 'modeled')))
    closes = np.zeros((n, length), dtype='<f8')
    valid = np.zeros((n, length), dtype=bool)
    conflict = np.zeros((n, length), dtype=bool)
    late = np.zeros((n, length), dtype=bool)
    unconfirmed = np.zeros((n, length), dtype=bool)
    max_available, modeled, chosen_lineage = None, False, []
    if parts:
        idx, slot, revision, available, confirmed, values, source, offset, is_modeled = (
            np.concatenate([p[k] for p in parts]) for k in range(9))
        admissible = confirmed & (available <= decision)
        late[idx[confirmed & (available > decision)], slot[confirmed & (available > decision)]] = True
        unconfirmed[idx[~confirmed], slot[~confirmed]] = True
        keep = np.flatnonzero(admissible)
        if len(keep):
            key = idx[keep] * length + slot[keep]
            order = keep[np.lexsort((revision[keep], key))]
            key = idx[order] * length + slot[order]
            last = np.r_[key[1:] != key[:-1], True]
            same = (key[1:] == key[:-1]) & (revision[order][1:] == revision[order][:-1])
            differs = same & (values[order][1:] != values[order][:-1]).any(axis=1)
            top_conflict = np.zeros(len(order), dtype=bool)
            top_conflict[1:] |= differs  # the later row of a same-revision pair
            # A group's top revision conflicts when any same-revision disagreement touches that revision.
            group_start = np.r_[True, key[1:] != key[:-1]]
            group_id = np.cumsum(group_start) - 1
            top_revision = np.zeros(group_id[-1] + 1, dtype='<i8')
            top_revision[group_id[last]] = revision[order][last]
            conflicted_group = np.zeros(group_id[-1] + 1, dtype=bool)
            hits = top_conflict & (revision[order] == top_revision[group_id])
            conflicted_group[group_id[hits]] = True
            chosen = order[last & ~conflicted_group[group_id]]
            bad = order[last & conflicted_group[group_id]]
            conflict[idx[bad], slot[bad]] = True
            closes[idx[chosen], slot[chosen]] = values[chosen, 3]
            valid[idx[chosen], slot[chosen]] = True
            if len(chosen):
                max_available = int(available[chosen].max())
                modeled = bool(is_modeled[chosen].any())
                chosen_lineage = sorted(zip(idx[chosen].tolist(), slot[chosen].tolist(), source[chosen].tolist(),
                                            offset[chosen].tolist()))
    digest = sha256(b'mc-manifest-1\n')
    for i, s, src, off in chosen_lineage:
        _, kind, close_time, revision_values, epochs, sequences, raw = lineage[src]
        digest.update(f'{universe.member_instrument_ids[i]}|{kind}|{int(close_time[off])}|{int(revision_values[off])}|'
                      f'{epochs[off]}|{int(sequences[off])}|'.encode())
        digest.update(bytes(raw[off]))
        digest.update(b'\n')
    reasons = []
    for i, asset in enumerate(universe.member_asset_ids):
        if quote_mismatch[i]:
            valid[i] = False
            closes[i] = 0.0
            reasons.append((asset, ('QUOTE_MISMATCH',)))
            continue
        reasons.append((asset, _asset_reasons(valid[i], conflict[i], late[i], unconfirmed[i])))
    for array in (closes, valid):
        array.setflags(write=False)
    times = start + np.arange(length, dtype='<i8') * interval
    times.setflags(write=False)
    return AlignedPanel(cutoff_ms=cutoff, decision_ms=decision, interval_ms=interval, times=times,
                        asset_ids=universe.member_asset_ids, instrument_ids=universe.member_instrument_ids,
                        closes=closes, valid=valid, asset_reasons=tuple(reasons), universe=universe,
                        max_input_available_at_ms=max_available, input_manifest_digest=digest.hexdigest(),
                        availability_kind='modeled' if modeled else 'observed')


class CoverageGate(PolicyModel):
    min_count_fraction: Decimal = Field(ge=0, le=1)
    min_weight_fraction: Decimal = Field(ge=0, le=1)
    min_valid_assets: int = Field(ge=1, le=500, strict=True)


@dataclass(frozen=True)
class CoverageResult:
    passed: bool
    valid_count: int
    expected_count: int
    valid_weight_fraction: float
    reasons: tuple[str, ...]


def nominal_weights(universe: UniverseBinding, scheme: Literal['equal', 'capped']) -> np.ndarray:
    if scheme not in ('equal', 'capped'):
        raise ValueError('unknown weighting scheme')
    weights = universe.equal_weights if scheme == 'equal' else universe.capped_weights
    return np.array([float(w) for w in weights], dtype='<f8')


def coverage(universe: UniverseBinding, valid, scheme: Literal['equal', 'capped'], gate: CoverageGate) -> CoverageResult:
    """Count and nominal-weight coverage against the full expected cohort, plus mandatory benchmarks."""
    valid = np.asarray(valid, dtype=bool)
    if valid.shape != (len(universe.member_asset_ids),):
        raise ValueError('coverage mask must align with the cohort')
    weights = nominal_weights(universe, scheme)
    expected = len(valid)
    count = int(valid.sum())
    weight_fraction = float(weights[valid].sum() / weights.sum())
    reasons = set()
    if (universe.mode == 'basket_5' and count != expected) or count < gate.min_valid_assets \
            or Decimal(count) < gate.min_count_fraction * expected \
            or Decimal(repr(weight_fraction)) < gate.min_weight_fraction:
        reasons.add('COVERAGE_LOW')
    position = {asset: i for i, asset in enumerate(universe.member_asset_ids)}
    if any(not valid[position[b]] for b in universe.benchmarks):
        reasons.add('BENCHMARK_MISSING')
    return CoverageResult(passed=not reasons, valid_count=count, expected_count=expected,
                          valid_weight_fraction=min(1.0, weight_fraction), reasons=tuple(sorted(reasons)))
