"""Immutable recipe identities. These records never grant execution authority."""
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Literal, Mapping

from pydantic import Field, field_validator, model_validator

from .config import AcquisitionPolicy, PolicyModel, RiskPolicy, SignalPolicy, TrailingPolicy
from .domain import PolicyError

PAIR = re.compile(r'^[A-Z0-9]+-[A-Z0-9]+$')
CONTROL_FIELDS = frozenset({'recipe_binding', 'execution_enabled', 'manual_kill_switch', 'acquisition_enabled'})


def canonical_json(value) -> str:
    def encode(obj):
        if isinstance(obj, Decimal):
            if not obj.is_finite():
                raise ValueError('nonfinite values cannot be sealed')
            return str(obj)
        if isinstance(obj, PolicyModel):
            return obj.model_dump(mode='json')
        raise TypeError(f'unsupported contract value: {type(obj).__name__}')
    return json.dumps(value, default=encode, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value) -> str:
    return sha256(canonical_json(value).encode('utf-8')).hexdigest()


def configuration_digest(payload: Mapping) -> str:
    if not isinstance(payload, Mapping) or any(not isinstance(key, str) for key in payload):
        raise PolicyError('a string-keyed native configuration is required')
    return digest({key: value for key, value in payload.items() if key not in CONTROL_FIELDS})


def source_digest(root: Path, files: tuple[Path, ...] | list[Path]) -> str:
    root = Path(root).resolve(strict=True)
    if not files or len(files) > 1000:
        raise PolicyError('bounded nonempty implementation source list required')
    entries, seen, total = [], set(), 0
    for item in files:
        path = Path(item)
        if any(parent.is_symlink() for parent in (path, *path.parents)):
            raise PolicyError('implementation source cannot use symlinks')
        path = path.resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file():
            raise PolicyError('implementation source is outside its owner')
        relative = path.relative_to(root).as_posix()
        if relative in seen:
            raise PolicyError('duplicate implementation source')
        seen.add(relative)
        with path.open('rb') as stream:
            data = stream.read(2 * 1024 * 1024 + 1)
        total += len(data)
        if len(data) > 2 * 1024 * 1024 or total > 16 * 1024 * 1024:
            raise PolicyError('implementation source exceeds the admitted size')
        entries.append((relative, sha256(data).hexdigest()))
    return digest(sorted(entries))


class RecipeBinding(PolicyModel):
    schema_version: Literal[1] = 1
    recipe_id: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    recipe_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    source_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    configuration_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value


def verify_binding(binding: RecipeBinding, payload: Mapping, *, source_sha256: str) -> None:
    binding = RecipeBinding.model_validate(binding.model_dump(mode='json'))
    if binding.source_sha256 != source_sha256:
        raise PolicyError('recipe implementation source changed; explicit revision required')
    if binding.configuration_sha256 != configuration_digest(payload):
        raise PolicyError('recipe behavioural configuration changed; explicit revision required')


class StrategyRecipe(PolicyModel):
    schema_version: Literal[1] = 1
    name: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    family: Literal['rebound', 'impulse', 'sell_only']
    connector: Literal['okx'] = 'okx'
    capital_model: Literal['spot_cash_inventory_1x'] = 'spot_cash_inventory_1x'
    account_topology: Literal['single_native_recorder', 'segregated_account'] = 'single_native_recorder'
    symbols: tuple[str, ...] = Field(min_length=1, max_length=1000)
    observation_symbols: tuple[str, ...] = Field(default=(), max_length=1000)
    quote_budget: Decimal = Field(gt=0)
    max_positions: int = Field(ge=1, le=1000, strict=True)
    history: int = Field(default=300, ge=61, le=3000, strict=True)
    data_origin: str = 'http://127.0.0.1:8100'
    token_file: str = '/run/secrets/modular_candle_reader'
    signal: SignalPolicy | None = None
    acquisition: AcquisitionPolicy = Field(default_factory=AcquisitionPolicy)
    trailing: TrailingPolicy = Field(default_factory=TrailingPolicy)
    risk: RiskPolicy = Field(default_factory=RiskPolicy)

    @model_validator(mode='before')
    @classmethod
    def choose_reference_signal(cls, value):
        if isinstance(value, dict):
            value = dict(value)
            if 'signal' not in value:
                family = value.get('family')
                if family in {'rebound', 'impulse'}:
                    value['signal'] = {'kind': 'breakout' if family == 'impulse' else 'rsi_rebound'}
        return value

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value

    @field_validator('quote_budget', mode='before')
    @classmethod
    def amount_is_not_boolean(cls, value):
        if isinstance(value, bool):
            raise ValueError('a boolean is not capital')
        return value

    @model_validator(mode='after')
    def compatible(self):
        for values in (self.symbols, self.observation_symbols):
            if len(set(values)) != len(values) or any(PAIR.fullmatch(item) is None for item in values):
                raise ValueError('unique native instrument identities required')
        if len({item.split('-')[1] for item in self.symbols}) != 1:
            raise ValueError('one quote currency per native budget program is required')
        if self.max_positions > len(self.symbols):
            raise ValueError('position bound exceeds the explicit execution universe')
        if self.observation_symbols and not set(self.symbols) <= set(self.observation_symbols):
            raise ValueError('execution universe is not covered by observation inputs')
        expected = {'rebound': 'rsi_rebound', 'impulse': 'breakout'}
        if self.family == 'sell_only':
            if self.signal is not None or len(self.acquisition.fractions) != 1:
                raise ValueError('sell-only cannot select an entry signal or DCA schedule')
        elif self.signal is None or self.signal.kind != expected[self.family]:
            raise ValueError('entry signal does not match the reference recipe')
        if self.family == 'impulse' and len(self.acquisition.fractions) != 1:
            raise ValueError('initial impulse recipe supports one-shot acquisition only')
        if self.signal is not None:
            needed = max(60, 2 * self.signal.rsi_length + 2, self.signal.breakout_lookback + 2) + 1
            if self.history < needed:
                raise ValueError('feature history is insufficient for the selected signal')
        return self

    @property
    def acquisition_allowed(self) -> bool:
        return self.family != 'sell_only'

    @property
    def digest(self) -> str:
        return digest(self.model_dump(mode='json'))
