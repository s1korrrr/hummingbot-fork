"""Source-bound B0/C1 recipes, not native execution or research admission."""
from decimal import Decimal
from typing import Literal

from pydantic import Field, field_validator, model_validator

from hummingbot.strategy_v2.components.candle_data import Dataset
from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope
from hummingbot.strategy_v2.components.domain import PolicyError, decimal
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding, configuration_digest, digest, verify_binding,
)

HASH = r'^[0-9a-f]{64}$'


class FeatureParameters(PolicyModel):
    lookback: int = Field(default=20, ge=2, le=500, strict=True)
    atr_period: int = Field(default=14, ge=2, le=500, strict=True)
    volume_period: int = Field(default=20, ge=2, le=500, strict=True)
    trend_period: int = Field(default=20, ge=2, le=500, strict=True)
    breakout_buffer_atr: Decimal = Field(default=Decimal('.1'), ge=0, le=2)
    min_return: Decimal = Field(default=Decimal('0'), ge=0, lt=1)
    min_relative_volume: Decimal = Field(default=Decimal('1.5'), gt=0, le=100)

    @field_validator('breakout_buffer_atr', 'min_return', 'min_relative_volume', mode='before')
    @classmethod
    def finite_amount(cls, value):
        return decimal(value, 'feature parameter')


class ShelfParameters(PolicyModel):
    min_bars: int = Field(default=2, ge=1, le=10, strict=True)
    max_bars: int = Field(default=6, ge=1, le=10, strict=True)
    ttl_ms: int = Field(default=600000, ge=60000, le=3600000, strict=True)
    max_pullback: Decimal = Field(default=Decimal('.4'), gt=0, lt=1)
    range_ratio: Decimal = Field(default=Decimal('.7'), gt=0, le=1)
    max_avwap_extension: Decimal = Field(default=Decimal('.08'), gt=0, lt=1)

    @field_validator('max_pullback', 'range_ratio', 'max_avwap_extension', mode='before')
    @classmethod
    def finite_amount(cls, value):
        return decimal(value, 'shelf parameter')

    @model_validator(mode='after')
    def ordered_bars(self):
        if self.min_bars > self.max_bars:
            raise ValueError('minimum shelf bars exceeds maximum')
        return self


class MomentumRecipe(PolicyModel):
    schema_version: Literal[1] = 1
    policy_version: Literal[1] = 1
    recipe_id: str = Field(pattern=r'^momentum_v2_[A-Za-z0-9_-]{1,20}$')
    variant: Literal['B0', 'C1']
    symbol: str = Field(pattern=r'^[A-Z0-9]+-USDC$', max_length=80)
    datasets: tuple[Dataset, Dataset, Dataset]
    initialization_sha256: str = Field(pattern=HASH)
    loss_policy_sha256: str = Field(pattern=HASH)
    exit_policy_sha256: str = Field(pattern=HASH)
    sizing_policy_sha256: str = Field(pattern=HASH)
    capital_policy_sha256: str = Field(pattern=HASH)
    features: FeatureParameters = Field(default_factory=FeatureParameters)
    shelf: ShelfParameters = Field(default_factory=ShelfParameters)
    opportunity_lifetime_ms: int = Field(default=10000, ge=1, le=10000, strict=True)
    max_candle_age_ms: int = Field(default=10000, ge=1, le=10000, strict=True)
    max_transport_age_ms: int = Field(default=10000, ge=1, le=15000, strict=True)
    execution_enabled: Literal[False] = False

    @field_validator('schema_version', 'policy_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('version must be an integer')
        return value

    @field_validator('execution_enabled', mode='before')
    @classmethod
    def disabled_only(cls, value):
        if value is not False:
            raise ValueError('this recipe has no execution adapter')
        return value

    @model_validator(mode='after')
    def supported_data(self):
        if self.symbol == 'USDC-USDC':
            raise ValueError('distinct base and quote required')
        if tuple(d.interval for d in self.datasets) != ('1m', '5m', '15m'):
            raise ValueError('ordered 1m, 5m and 15m snapshots are required')
        if any(d.connector != 'okx' or d.source_profile != 'native' or d.trading_pair != self.symbol
               or d.history > 5000 for d in self.datasets):
            raise ValueError('only bound native OKX spot snapshots are supported')
        p = self.features
        required = max(p.lookback + 1, p.atr_period + 2, p.volume_period + 1, 60)
        if self.datasets[0].history < required or any(d.history < p.trend_period + 1 for d in self.datasets[1:]):
            raise ValueError('dataset history is shorter than causal feature warmup')
        return self

    @property
    def digest(self):
        return digest(self.model_dump(mode='json'))


def normalized(recipe: MomentumRecipe) -> MomentumRecipe:
    return MomentumRecipe.model_validate(recipe.model_dump(mode='json'))


def bind_recipe(recipe: MomentumRecipe, *, source_sha256: str) -> RecipeBinding:
    recipe = normalized(recipe)
    return RecipeBinding(recipe_id=recipe.recipe_id, recipe_sha256=recipe.digest,
                         source_sha256=source_sha256,
                         configuration_sha256=configuration_digest(recipe.model_dump(mode='json')))


def verify_recipe(recipe: MomentumRecipe, binding: RecipeBinding, *, source_sha256: str) -> None:
    recipe = normalized(recipe)
    binding = RecipeBinding.model_validate(binding.model_dump(mode='json'))
    if binding.recipe_id != recipe.recipe_id or binding.recipe_sha256 != recipe.digest:
        raise PolicyError('momentum recipe identity differs from its binding')
    verify_binding(binding, recipe.model_dump(mode='json'), source_sha256=source_sha256)


def assert_single_treatment(baseline: MomentumRecipe, candidate: MomentumRecipe) -> None:
    baseline, candidate = normalized(baseline), normalized(candidate)
    if (baseline.variant != 'B0' or candidate.variant != 'C1'
            or baseline.model_dump(exclude={'variant'}) != candidate.model_dump(exclude={'variant'})):
        raise PolicyError('B0/C1 may differ only in the confirmation variant')


def scope_for(recipe: MomentumRecipe, *, program_id: str, owner_id: str, mode: str) -> DecisionScope:
    recipe = normalized(recipe)
    if (mode not in {'observation', 'shadow'} or not isinstance(owner_id, str)
            or not isinstance(program_id, str) or not owner_id.startswith('momentum_v2_')
            or not program_id.startswith('momentum_v2_') or len(owner_id) > 90):
        raise PolicyError('separate momentum v2 observation/shadow scope required')
    return DecisionScope(program_id=program_id, owner_id=f'{owner_id}:{mode}', connector='okx',
                         trading_pair=recipe.symbol, recipe_sha256=recipe.digest)
