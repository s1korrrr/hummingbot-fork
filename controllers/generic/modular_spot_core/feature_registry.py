"""Versioned feature DAG over existing candle snapshots and the existing bounded cache."""
import inspect
from collections.abc import Callable, Mapping
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType

from pydantic import Field, field_validator, model_validator

from .candle_data import CandleSnapshot
from .config import PolicyModel
from .domain import DataUnavailable, PolicyError, decimal
from .feature_cache import FeatureCache
from .recipe_compiler import strict_json
from .recipe_contract import canonical_json, digest


class FeatureSpec(PolicyModel):
    name: str = Field(pattern=r'^[A-Za-z0-9_.-]{1,80}$')
    version: int = Field(ge=1, strict=True)
    dataset_key: str = Field(pattern=r'^[0-9a-f]{64}$')
    implementation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    implementation_symbol: str = Field(min_length=1, max_length=512)
    initialization_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    parameters_json: str = Field(default='{}', max_length=8192)
    required_fields: tuple[str, ...] = Field(default=('close',), min_length=1, max_length=32)
    output_fields: tuple[str, ...] = Field(default=('value',), min_length=1, max_length=128)
    dependencies: tuple[str, ...] = Field(default=(), max_length=128)
    min_records: int = Field(default=60, ge=2, le=5000, strict=True)
    max_candle_age: float = Field(default=120, gt=0, le=86400)
    max_transport_age: float = Field(default=15, gt=0, le=3600)
    model_sha256: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    scaler_sha256: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    training_cutoff: float | None = Field(default=None, ge=0)
    artifact_available_at: float | None = Field(default=None, ge=0)

    @field_validator('parameters_json')
    @classmethod
    def immutable_parameters(cls, value):
        parsed = strict_json(value)
        if not isinstance(parsed, dict):
            raise ValueError('feature parameters must be a JSON object')
        return canonical_json(parsed)

    @field_validator('max_candle_age', 'max_transport_age', 'training_cutoff', 'artifact_available_at', mode='before')
    @classmethod
    def numeric_time(cls, value):
        if isinstance(value, bool):
            raise ValueError('a boolean is not a time or duration')
        return value

    @model_validator(mode='after')
    def complete(self):
        for values in (self.required_fields, self.output_fields, self.dependencies):
            if len(values) != len(set(values)) or any(not item or len(item) > 128 for item in values):
                raise ValueError('feature schema/dependency names must be nonempty and unique')
        learned = (self.model_sha256, self.training_cutoff, self.artifact_available_at)
        if any(value is not None for value in learned) and any(value is None for value in learned):
            raise ValueError('learned features require model, training cutoff and availability provenance')
        if self.scaler_sha256 is not None and self.model_sha256 is None:
            raise ValueError('a scaler requires a declared learned feature')
        if self.model_sha256 is not None and self.artifact_available_at < self.training_cutoff:
            raise ValueError('model availability precedes training cutoff')
        return self


class FeatureValue(PolicyModel):
    name: str
    value: Decimal

    @field_validator('value', mode='before')
    @classmethod
    def finite_numeric(cls, value):
        return decimal(value, 'feature result', minimum=None)


class FeatureResult(PolicyModel):
    name: str
    contract_sha256: str
    input_sha256: str
    dependency_sha256: tuple[str, ...]
    available_at: float = Field(ge=0)
    values: tuple[FeatureValue, ...]

    def as_dict(self) -> dict[str, Decimal]:
        return {item.name: item.value for item in self.values}

    @property
    def result_sha256(self) -> str:
        return digest(self.model_dump(mode='json'))


def _callable_identity(function: Callable) -> tuple[str, str]:
    if (not inspect.isfunction(function) or function.__closure__ is not None
            or function.__defaults__ or function.__kwdefaults__):
        raise PolicyError('feature implementations must be stateless functions with explicit parameters')
    path = inspect.getsourcefile(function)
    if path is None:
        raise PolicyError('this registry requires an inspectable Python implementation')
    source = Path(path)
    if any(item.is_symlink() for item in (source, *source.parents)):
        raise PolicyError('feature source cannot use a symlink')
    with source.open('rb') as stream:
        data = stream.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise PolicyError('feature implementation source exceeds the size bound')
    return sha256(data).hexdigest(), function.__module__ + '.' + function.__qualname__


class FeatureRegistry:
    def __init__(self, capacity: int = 2048):
        self.cache = FeatureCache(capacity)
        self._definitions = {}
        self._sealed = False

    def register(self, spec: FeatureSpec, compute: Callable) -> None:
        spec = FeatureSpec.model_validate(spec.model_dump(mode='json'))
        if self._sealed or spec.name in self._definitions or len(self._definitions) >= 128:
            raise PolicyError('feature registry is sealed, duplicated or at capacity')
        if not callable(compute) or _callable_identity(compute) != (spec.implementation_sha256, spec.implementation_symbol):
            raise PolicyError('feature callable differs from its pinned implementation')
        self._definitions[spec.name] = (spec, compute)

    def seal(self) -> None:
        visited, active = set(), set()

        def visit(name):
            if name in active:
                raise PolicyError('cyclic feature dependency')
            if name not in self._definitions:
                raise DataUnavailable('required feature dependency is not registered')
            if name in visited:
                return
            active.add(name)
            for dependency in self._definitions[name][0].dependencies:
                visit(dependency)
            active.remove(name)
            visited.add(name)

        for name in self._definitions:
            visit(name)
        self._sealed = True

    def evaluate(self, name: str, snapshots: Mapping[str, CandleSnapshot], *, now: float) -> FeatureResult:
        now = float(decimal(now, 'feature decision time'))
        self.seal()
        if name not in self._definitions:
            raise DataUnavailable('feature is not registered')
        completed = {}

        def evaluate_one(key):
            if key in completed:
                return completed[key]
            spec, compute = self._definitions[key]
            if _callable_identity(compute) != (spec.implementation_sha256, spec.implementation_symbol):
                raise PolicyError('feature source changed after registration')
            supplied = snapshots.get(spec.dataset_key)
            if supplied is None:
                raise DataUnavailable('required dataset is unavailable')
            snapshot = CandleSnapshot.model_validate(supplied.model_dump(mode='json'))
            if snapshot.dataset.key != spec.dataset_key:
                raise PolicyError('feature dataset identity differs from the supplied observation')
            if not 0 <= now - snapshot.published_at <= spec.max_transport_age:
                raise DataUnavailable('feature transport is stale or future-dated')
            frame = snapshot.frame(now=now, max_age=spec.max_candle_age)
            if len(frame) < spec.min_records or not set(spec.required_fields) <= set(frame.columns):
                raise DataUnavailable('feature warmup or required fields are incomplete')
            if spec.model_sha256 is not None and (
                    spec.training_cutoff >= snapshot.rows[-1].open_time or spec.artifact_available_at > now):
                raise DataUnavailable('learned feature is not available before this decision bar')
            dependencies = {dependency: evaluate_one(dependency) for dependency in spec.dependencies}
            dependency_hashes = tuple(dependencies[item].result_sha256 for item in spec.dependencies)
            input_hash = digest([snapshot.dataset.key, snapshot.epoch, snapshot.sequence, snapshot.digest])
            cache_spec = {'contract': spec.model_dump(mode='json'), 'input_sha256': input_hash,
                          'dependency_sha256': dependency_hashes}

            def calculate(native_frame):
                result = compute(native_frame, MappingProxyType(dependencies), strict_json(spec.parameters_json))
                if not isinstance(result, Mapping) or set(result) != set(spec.output_fields):
                    raise PolicyError('feature result does not match the declared output schema')
                values = tuple(FeatureValue(name=field, value=result[field]) for field in spec.output_fields)
                available = max([snapshot.published_at, *[item.available_at for item in dependencies.values()],
                                 *([] if spec.artifact_available_at is None else [spec.artifact_available_at])])
                return FeatureResult(name=spec.name, contract_sha256=digest(spec.model_dump(mode='json')),
                                     input_sha256=input_hash, dependency_sha256=dependency_hashes,
                                     available_at=available, values=values)

            result = self.cache.get(snapshot, spec=cache_spec, implementation=spec.implementation_sha256,
                                    now=now, max_age=spec.max_candle_age, compute=calculate)
            if result.available_at > now:
                raise DataUnavailable('cached feature was not yet available at decision time')
            completed[key] = result
            return result

        return evaluate_one(name)
