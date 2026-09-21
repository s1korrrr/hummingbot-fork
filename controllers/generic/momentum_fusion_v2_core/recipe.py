"""Versioned momentum recipe. Eligibility is not execution authority."""
from typing import Literal

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, digest
from pydantic import Field, model_validator

from controllers.generic.momentum_fusion_v2_core import setups

RECIPE_ID = 'mom_v2'
PARAMETER_REVISION = 'v1-reference-unoptimized'
PRECEDENCE = ('c1_shelf', 'b0_breakout', 'c2_compression', 'c3_reclaim')
FIRST_SLICE = frozenset({'b0_breakout', 'c1_shelf'})

# Frozen reference magnitudes copied from the v1 packet intent. Not a fitted optimum.
PARAMETERS = {
    'lookback': 20,
    'atr_period': 14,
    'min_return': '0.012',
    'min_range_ratio': '1.4',
    'min_volume_ratio': '1.5',
    'shelf_max_pullback': '0.006',
    'shelf_min_bars': 3,
    'shelf_max_bandwidth': '0.55',
    'compression_max': '0.35',
    'expansion_min': '1.6',
    'htf_trend_bars': 8,
    'c3_pullback_max': '0.02',
    'lifetime_ms': 600000,
    'cooldown_ms': 600000,
}


class RecipeSpec(PolicyModel):
    recipe_id: Literal['mom_v2'] = RECIPE_ID
    parameter_revision: str = PARAMETER_REVISION
    enabled: tuple[str, ...] = Field(default=('b0_breakout', 'c1_shelf'))
    loss_policy: Literal['profit_only', 'explicit_stop'] = 'profit_only'
    acquisition: Literal['single_entry'] = 'single_entry'
    exit_split: Literal['single'] = 'single'
    execution_enabled: Literal[False] = False

    @model_validator(mode='after')
    def known_modules(self):
        self.modules()
        return self

    def modules(self) -> frozenset[str]:
        enabled = frozenset(self.enabled)
        unknown = enabled - set(PRECEDENCE)
        if unknown:
            raise ValueError('unknown momentum modules: ' + ','.join(sorted(unknown)))
        return enabled


def initialization_sha256():
    return digest({'parameters': PARAMETERS, 'revision': PARAMETER_REVISION})


def bind(source_sha256: str, configuration_sha256: str, spec: RecipeSpec | None = None,
         recipe_sha256: str | None = None) -> RecipeBinding:
    body = (spec or RecipeSpec()).model_dump(mode='json')
    return RecipeBinding(
        recipe_id=RECIPE_ID,
        recipe_sha256=recipe_sha256 or digest(body),
        source_sha256=source_sha256,
        configuration_sha256=configuration_sha256,
    )


def select(proposals: tuple[setups.SetupState, ...], prerequisites: tuple[Prerequisite, ...],
           enabled: frozenset[str]) -> setups.SetupState | None:
    blockers = tuple(item for item in prerequisites if item.status != 'PASS')
    if blockers:
        return None
    proposed = [item for item in proposals if item.setup in enabled and item.phase == 'PROPOSED']
    for name in PRECEDENCE:
        for item in proposed:
            if item.setup == name:
                return item
    return None
