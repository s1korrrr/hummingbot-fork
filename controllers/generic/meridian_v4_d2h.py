"""D2h: V4 with native depth-scaled sizing, as a separate stand-alone paper controller.

The same per-pair composition, gates and paper-only execution as ``meridian_v4``; only the
recipe (``meridian_v4_d2h_recipe``), the program identity and the depth budget differ. It runs
beside V4 as a forward paper A/B. Live activation is not supported.
"""
from pathlib import Path
from typing import ClassVar, Literal

from pydantic import Field

from controllers.generic import meridian_v4, meridian_v4_d2h_recipe as recipe
from controllers.generic.meridian_v4 import MeridianV4Config, MeridianV4Controller
from controllers.generic.modular_spot_core.bag import BagConfig, DepthBudgetConfig
from hummingbot.strategy_v2.components.recipe_contract import source_records
from hummingbot.strategy_v2.components.source_identity import component_source_files

CONFIG_CLASS = 'MeridianV4D2hConfig'
PROGRAM = 'meridian_v4_d2h'


def source_files():
    return [Path(__file__), Path(recipe.__file__), *meridian_v4.source_files()]


class MeridianV4D2hConfig(MeridianV4Config):
    RECIPE: ClassVar = recipe
    PROGRAM: ClassVar[str] = PROGRAM

    controller_name: Literal['meridian_v4_d2h'] = 'meridian_v4_d2h'
    recipe_version: Literal['meridian_v4_d2h_v1'] = recipe.RECIPE_ID
    bag: BagConfig = Field(default_factory=lambda: recipe.strategy_parameters()['bag'])
    # Derived from the per-pair budget by the recipe; any other value is refused.
    depth_budget: DepthBudgetConfig | None = None
    program_revision: Literal['meridian_v4_d2h'] = PROGRAM
    policy_revision: Literal['meridian_v4_d2h_v1'] = recipe.RECIPE_ID

    def expected_depth_budget(self):
        return recipe.depth_budget(self.total_amount_quote)

    def source_manifest(self):
        return source_records(Path(__file__).parent, source_files())

    def get_controller_class(self):
        return MeridianV4D2hController


class MeridianV4D2hController(MeridianV4Controller):
    CONFIG = MeridianV4D2hConfig
    STRATEGY_NAME = 'V4 D2h'

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]
