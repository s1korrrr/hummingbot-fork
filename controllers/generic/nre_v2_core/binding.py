"""Bind explicit path state to native immutable snapshots; no feed, ledger or orders."""
import math
import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path

from hummingbot.strategy_v2.components.candle_data import CandleSnapshot, Dataset
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, ObservationRef
from hummingbot.strategy_v2.components.domain import DataUnavailable, PolicyError
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, digest
from hummingbot.strategy_v2.components.source_identity import checked_source

from . import recovery_path as path

MAX_MS = 2 ** 53


def milliseconds(value: int) -> int:
    if type(value) is not int or not 0 <= value <= MAX_MS:
        raise PolicyError('time must be bounded integer milliseconds')
    return value


def seconds_to_ms(value: int | float | Decimal) -> int:
    """Exact conversion of the native serialized decimal-second value; never truncate."""
    if type(value) not in (int, float, Decimal):
        raise PolicyError('seconds require a numeric value, not a string or boolean')
    try:
        d = Decimal(str(value))
        if not d.is_finite() or d < 0 or d > MAX_MS or len(d.as_tuple().digits) > 32 or d.as_tuple().exponent < -18:
            raise ValueError('unsupported time magnitude or precision')
        numerator, denominator = d.as_integer_ratio()
        result, remainder = divmod(numerator * 1000, denominator)
        if remainder:
            raise ValueError('submillisecond timestamps are unsupported')
        return milliseconds(result)
    except (ArithmeticError, InvalidOperation, ValueError) as exc:
        raise PolicyError('seconds cannot be represented exactly in milliseconds') from exc


def _hash(value: str) -> None:
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise PolicyError('a lowercase SHA256 identity is required')


def _native(value, cls):
    if type(value) is not cls:
        raise PolicyError(f'{cls.__name__} from the native contract is required')
    return cls.model_validate(value.model_dump(mode='python'))


def observation_ref(snapshot: CandleSnapshot) -> ObservationRef:
    snapshot = _native(snapshot, CandleSnapshot)
    return ObservationRef(dataset_sha256=snapshot.dataset.key, epoch=snapshot.epoch, revision=snapshot.sequence,
                          content_sha256=snapshot.digest,
                          event_time_ms=milliseconds((snapshot.rows[-1].open_time + snapshot.dataset.seconds) * 1000),
                          available_time_ms=seconds_to_ms(snapshot.published_at))


def scale_from_snapshot(snapshot: CandleSnapshot, *, length: int) -> tuple[float, ObservationRef, str, str]:
    if type(length) is not int or not 2 <= length <= 60:
        raise PolicyError('ATR length must be an integer in [2, 60]')
    snapshot = _native(snapshot, CandleSnapshot)
    if any(not row.confirmed for row in snapshot.rows):
        raise PolicyError('ATR requires confirmed completed rows')
    rows = snapshot.rows
    if len(rows) < length + 1:
        raise PolicyError('insufficient confirmed history for ATR')
    trs = []
    for prev, row in zip(rows, rows[1:]):
        high, low, close = float(row.high), float(row.low), float(prev.close)
        tr = max(high - low, abs(high - close), abs(low - close))
        if not math.isfinite(tr) or tr < 0:
            raise PolicyError('true range is not a finite number')
        trs.append(tr)
    if len(trs) < length:
        raise PolicyError('insufficient confirmed history for ATR')
    atr = sum(trs[:length]) / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    if not math.isfinite(atr) or atr <= 0:
        raise PolicyError('ATR scale must be a finite positive number')
    parent = observation_ref(snapshot)
    return atr, parent, sha256(checked_source(Path(__file__))).hexdigest(), digest({
        'length': length,
        'dataset_key': snapshot.dataset.key,
        'parent_content_sha256': parent.content_sha256,
    })


@dataclass(frozen=True)
class PathContract:
    scope: DecisionScope
    recipe: RecipeBinding
    dataset: Dataset
    setup: str
    setup_event_ms: int
    setup_available_ms: int
    setup_evidence_sha256: str
    frozen_scale: float
    scale_parent: ObservationRef
    scale_implementation_sha256: str
    initialization_sha256: str
    policy: path.Policy
    max_transport_age_ms: int

    def __post_init__(self):
        for name, cls in (('scope', DecisionScope), ('recipe', RecipeBinding), ('dataset', Dataset), ('scale_parent', ObservationRef)):
            object.__setattr__(self, name, _native(getattr(self, name), cls))
        if type(self.setup) is not str or re.fullmatch('[A-Za-z0-9_.:-]{1,128}', self.setup) is None:
            raise PolicyError('a bounded native setup identity is required')
        if self.scope.recipe_sha256 != self.recipe.recipe_sha256:
            raise PolicyError('scope and recipe identities differ')
        if (self.dataset.connector != 'okx' or self.dataset.source_profile != 'native' or self.dataset.interval != '1m'
                or self.scope.connector not in ('okx', 'okx_paper_trade') or self.scope.trading_pair != self.dataset.trading_pair):
            raise PolicyError('adapter supports declared native OKX spot one-minute input only')
        milliseconds(self.setup_event_ms)
        milliseconds(self.setup_available_ms)
        if self.setup_event_ms % 60000 or self.setup_available_ms < self.setup_event_ms:
            raise PolicyError('invalid setup clock')
        if (self.scale_parent.dataset_sha256 != self.dataset.key or self.scale_parent.event_time_ms > self.setup_event_ms
                or self.scale_parent.available_time_ms > self.setup_available_ms):
            raise PolicyError('ATR parent is incompatible or unavailable at setup')
        for value in (self.setup_evidence_sha256, self.scale_implementation_sha256, self.initialization_sha256):
            _hash(value)
        path._positive(self.frozen_scale, 'frozen scale')
        if type(self.policy) is not path.Policy:
            raise PolicyError('explicit path policy is required')
        self.policy.__post_init__()
        if type(self.max_transport_age_ms) is not int or not 1 <= self.max_transport_age_ms <= 60000:
            raise PolicyError('transport age must be explicit and at most one minute')

    @property
    def contract_sha256(self) -> str:
        return digest(asdict(self))


def _bar(contract: PathContract, snapshot: CandleSnapshot) -> path.Candle:
    row = snapshot.rows[-1]
    availability = max(seconds_to_ms(snapshot.published_at), contract.setup_available_ms,
                       contract.scale_parent.available_time_ms)
    return path.Candle(milliseconds(row.open_time * 1000), availability,
                       float(row.open), float(row.high), float(row.low), float(row.close))


@dataclass(frozen=True)
class BoundPath:
    contract: PathContract
    snapshot: CandleSnapshot
    path_state: path.PathState
    previous_state_sha256: str | None
    evaluated_at_ms: int

    def __post_init__(self):
        if type(self.contract) is not PathContract or type(self.path_state) is not path.PathState:
            raise PolicyError('validated in-memory bound state is required')
        milliseconds(self.evaluated_at_ms)
        if self.previous_state_sha256 is not None:
            _hash(self.previous_state_sha256)
        if (self.path_state.setup_id != self.contract.setup
                or self.path_state.instrument != f'okx:{self.contract.dataset.trading_pair}'
                or self.path_state.scale != self.contract.frozen_scale or self.path_state.policy != self.contract.policy
                or self.path_state.last != _bar(self.contract, self.snapshot)):
            raise PolicyError('path state is inconsistent with its contract or observation')
        path.view(self.path_state, self.evaluated_at_ms)

    @property
    def state_sha256(self) -> str:
        return digest({'schema': 'rsibot.nre_bound_path.v1', 'contract': self.contract.contract_sha256,
                       'primary': observation_ref(self.snapshot), 'path': asdict(self.path_state),
                       'previous_state_sha256': self.previous_state_sha256, 'evaluated_at_ms': self.evaluated_at_ms})


def evidence(state: BoundPath, *, now_ms: int) -> dict:
    """Detached diagnostic payload, with nullable features and an exclusive expiry."""
    if type(state) is not BoundPath:
        raise PolicyError('bound path required; restore by verified replay, not deserialization')
    milliseconds(now_ms)
    if now_ms < state.evaluated_at_ms:
        raise PolicyError('decision clock regressed')
    primary = observation_ref(state.snapshot)
    if not 0 <= now_ms - primary.available_time_ms <= state.contract.max_transport_age_ms:
        raise DataUnavailable('path transport evidence is stale or future-dated')
    values = path.view(state.path_state, now_ms)
    missing = []
    if values['extension_atr'] is None:
        missing.append('extension_warmup')
    if values['retention'] is None:
        missing.append('retention_unconfirmed' if state.path_state.status == 'DECLINING' else 'retention_unavailable')
    if state.path_state.status in ('FAILED', 'EXPIRED'):
        missing.append('terminal_setup')
    payload = {'schema': 'rsibot.nre_path_evidence.v1', 'economic_backtest': False, 'execution_authorized': False,
               'contract_sha256': state.contract.contract_sha256, 'state_sha256': state.state_sha256,
               'scope': state.contract.scope.model_dump(mode='json'), 'setup': state.contract.setup,
               'recipe_binding': state.contract.recipe.model_dump(mode='json'),
               'primary': primary.model_dump(mode='json'), 'features': values, 'missingness': missing,
               'available_at_ms': values['available_at_ms'], 'valid_until_ms': values['valid_until_ms']}
    return {**payload, 'evidence_sha256': digest(payload)}


def observe(contract: PathContract, previous: BoundPath | None, snapshot: CandleSnapshot, *, now_ms: int) -> BoundPath:
    """Consume an actual native snapshot, with caller-owned state and no automatic recovery."""
    if type(contract) is not PathContract:
        raise PolicyError('explicit bound contract is required')
    milliseconds(now_ms)
    snapshot = _native(snapshot, CandleSnapshot)
    primary = observation_ref(snapshot)
    if snapshot.dataset != contract.dataset:
        raise PolicyError('snapshot dataset differs from frozen contract')
    if not 0 <= now_ms - primary.available_time_ms <= contract.max_transport_age_ms:
        raise DataUnavailable('snapshot transport is stale or unavailable')
    if len(snapshot.rows) != contract.dataset.history:
        raise DataUnavailable('complete declared native warmup history is required')
    snapshot.frame(now=now_ms / 1000, max_age=60)
    bar = _bar(contract, snapshot)
    if previous is None:
        if primary.event_time_ms != contract.setup_event_ms:
            raise PolicyError('seed must bind the original setup event, not a reconnect event')
        if primary != contract.scale_parent:
            raise PolicyError('first adapter requires ATR provenance bound to the exact seed snapshot')
        reduced = path.start(contract.setup, f'okx:{contract.dataset.trading_pair}', contract.frozen_scale,
                             bar, now_ms, contract.policy)
        result = BoundPath(contract, snapshot, reduced, None, now_ms)
    else:
        if type(previous) is not BoundPath or previous.contract != contract:
            raise PolicyError('path contract changed; explicit new revision is required')
        if now_ms < previous.evaluated_at_ms or snapshot.published_at < previous.snapshot.published_at:
            raise PolicyError('observation clock regressed')
        old = previous.snapshot
        if snapshot.epoch != old.epoch:
            raise DataUnavailable('changed producer epoch requires explicit verified reconstruction')
        if snapshot.sequence == old.sequence:
            if snapshot.digest != old.digest:
                raise PolicyError('snapshot revision cannot overwrite path history')
            # A heartbeat is not a new feature event and cannot renew original evidence.
            evidence(previous, now_ms=now_ms)
            return previous
        if snapshot.sequence != old.sequence + 1 or snapshot.rows[-1].open_time != old.rows[-1].open_time + 60:
            raise DataUnavailable('gapped or reordered snapshot requires reconstruction')
        if old.rows[1:] != snapshot.rows[:-1]:
            raise PolicyError('overlapping finalized candle revision requires reconstruction')
        reduced = path.step(previous.path_state, bar, now_ms)
        result = BoundPath(contract, snapshot, reduced, previous.state_sha256, now_ms)
    evidence(result, now_ms=now_ms)
    return result
