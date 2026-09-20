"""Strategy recipes with compatible exports of canonical neutral source bindings."""
import re
from decimal import Decimal
from typing import Literal

from pydantic import Field, field_validator, model_validator

from hummingbot.strategy_v2.components.recipe_contract import (
    CONTROL_FIELDS,
    RecipeBinding,
    canonical_json,
    configuration_digest,
    digest,
    source_digest,
    source_records,
    verify_binding,
)

from .config import AcquisitionPolicy, PolicyModel, RiskPolicy, SignalPolicy, TrailingPolicy

__all__ = ['CONTROL_FIELDS', 'RecipeBinding', 'canonical_json', 'configuration_digest', 'digest',
           'source_digest', 'source_records', 'verify_binding', 'StrategyRecipe']
PAIR = re.compile(r'^[A-Z0-9]+-[A-Z0-9]+$')


class StrategyRecipe(PolicyModel):
    schema_version: Literal[1] = 1
    name: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    family: Literal['rebound', 'impulse', 'sell_only']
    connector: Literal['okx'] = 'okx'
    capital_model: Literal['spot_cash_inventory_1x'] = 'spot_cash_inventory_1x'
    account_topology: Literal['single_native_recorder', 'segregated_account'] = 'single_native_recorder'
    symbols: tuple[str, ...] = Field(min_length=1, max_length=1000)
    observation_symbols: tuple[str, ...] = Field(default=(), max_length=1000)
    benchmarks: tuple[str, ...] = Field(default=(), max_length=1000)
    native_owners: tuple[str, ...] = Field(default=(), max_length=1000)
    account: str = Field(default='unassigned', pattern=r'^[A-Za-z0-9_-]{1,80}$')
    program_revision: str = Field(default='0', pattern=r'^[A-Za-z0-9._-]{1,64}$')
    policy_revision: str = Field(default='0', pattern=r'^[A-Za-z0-9._-]{1,64}$')
    quote_budget: Decimal = Field(gt=0)
    max_positions: int = Field(ge=1, le=1000, strict=True)
    history: int = Field(default=300, ge=61, le=3000, strict=True)
    data_transport: Literal['snapshot', 'stream'] = 'snapshot'
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
        for values in (self.symbols, self.observation_symbols, self.benchmarks):
            if len(set(values)) != len(values) or any(PAIR.fullmatch(item) is None for item in values):
                raise ValueError('unique native instrument identities required')
        if self.native_owners and (
                len(self.native_owners) != len(self.symbols)
                or len(set(self.native_owners)) != len(self.native_owners)
                or any(re.fullmatch(r'modular_spot_[A-Za-z0-9_-]+', owner) is None for owner in self.native_owners)):
            raise ValueError('unique native owner references must match execution symbols')
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
