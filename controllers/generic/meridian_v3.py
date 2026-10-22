"""Meridian v3: BNB-evidence recipe on the generic native modular host.

No legacy controller inheritance, order engine, fill ledger or account allocator.
This release supports isolated native paper and disabled shared-data shadow.
Live activation needs a separately admitted account-wide ownership path.
"""
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from controllers.generic import meridian_v3_recipe as recipe, modular_spot
from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.bag import (
    AdaptiveTrailingConfig,
    BagConfig,
    BuyGapConfig,
    DepthBudgetConfig,
    FlashCrashConfig,
    LiveCapitalConfig,
)
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy, TrailingPolicy
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.program_budget import BudgetPolicy
from hummingbot.strategy_v2.components.recipe_contract import digest, source_records, verify_binding
from hummingbot.strategy_v2.components.source_identity import component_source_files

CONFIG_CLASS = 'MeridianV3Config'


def source_files():
    return [Path(__file__), Path(recipe.__file__), *modular_spot.source_files()]


class MeridianV3Config(ModularSpotControllerConfig):
    controller_name: Literal['meridian_v3'] = 'meridian_v3'
    recipe_version: Literal['meridian_v3_bnb_p23_v1'] = recipe.RECIPE_ID
    execution_mode: Literal['paper', 'shadow']
    connector_name: Literal['okx', 'okx_paper_trade']
    candles_connector: Literal['okx'] = 'okx'
    trading_pair: Literal['BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC']
    total_amount_quote: Decimal = Field(default=Decimal('100'), gt=0, le=100)
    max_records: Literal[1500] = 1500
    risk: RiskPolicy = Field(...)  # p23 does not establish native loss/fee rails
    signal: SignalPolicy = Field(default_factory=lambda: recipe.strategy_parameters()['signal'])
    acquisition: AcquisitionPolicy = Field(default_factory=lambda: recipe.strategy_parameters()['acquisition'])
    trailing: TrailingPolicy = Field(default_factory=lambda: recipe.strategy_parameters()['trailing'])
    bag: BagConfig = Field(default_factory=lambda: recipe.strategy_parameters()['bag'])
    adaptive_trailing: AdaptiveTrailingConfig = Field(default_factory=lambda: recipe.strategy_parameters()['adaptive_trailing'])
    buy_gap: BuyGapConfig = Field(default_factory=lambda: recipe.strategy_parameters()['buy_gap'])
    depth_budget: DepthBudgetConfig = Field(default_factory=lambda: recipe.strategy_parameters()['depth_budget'])
    flash_crash: FlashCrashConfig = Field(default_factory=lambda: recipe.strategy_parameters()['flash_crash'])
    capital: LiveCapitalConfig | None = None
    budget: BudgetPolicy = Field(default_factory=lambda: BudgetPolicy(group='meridian_v3', max_quote='500', max_assets=5))
    exit_floor: Literal['protected'] = 'protected'
    program_revision: Literal['meridian_v3'] = 'meridian_v3'
    policy_revision: Literal['bnb_p23_v1'] = 'bnb_p23_v1'

    @model_validator(mode='after')
    def validate_composition(self):
        # Reuse the generic host's full validation without modifying its source,
        # defaults, binding scope or legacy consumers. Validate our own binding below.
        expected_capital = recipe.capital_policy(self.risk.fee_reserve_fraction)
        if self.capital is None:
            self.capital = expected_capital
        if self.capital != expected_capital or self.total_amount_quote != 100:
            raise ValueError('Meridian v3 requires the versioned 20/100 fee-inclusive capital recipe')
        for name, expected in recipe.strategy_parameters().items():
            if getattr(self, name) != expected:
                raise ValueError(f'Meridian v3 {name} differs from the versioned recipe; new version required')
        if self.budget != BudgetPolicy(group='meridian_v3', max_quote='500', max_assets=5):
            raise ValueError('Meridian v3 requires its dedicated 500 quote / five asset program')
        if self.universe.mode != 'fixed' or self.universe.symbols or self.universe.benchmarks:
            raise ValueError('Meridian v3 keeps a fixed pair per controller')
        expected_id = f'modular_spot_meridian_v3_{self.execution_mode}_{self.trading_pair.split("-")[0].lower()}'
        if self.id != expected_id or self.account != f'meridian_v3_{self.execution_mode}':
            raise ValueError('Meridian v3 requires distinct mode, pair and account identities')
        if self.execution_mode == 'paper':
            if self.connector_name != 'okx_paper_trade' or self.data.mode != 'native':
                raise ValueError('Meridian paper requires the isolated native paper connector and injected candles')
            if self.execution_enabled and self.recipe_binding is None:
                raise ValueError('Executable Meridian paper requires a sealed recipe binding')
        elif self.connector_name != 'okx' or self.execution_enabled or self.data.mode != 'shared':
            raise ValueError('Meridian shadow requires shared data and cannot dispatch execution')
        base = self.model_dump(mode='json', exclude={'recipe_version', 'execution_mode', 'candles_connector'})
        base.update(controller_name='modular_spot', recipe_binding=None)
        ModularSpotControllerConfig.model_validate(base)
        if self.recipe_binding is not None:
            if (self.recipe_binding.recipe_id != recipe.RECIPE_ID
                    or self.recipe_binding.recipe_sha256 != self.recipe_digest()):
                raise ValueError('Meridian recipe binding differs')
            verify_binding(self.recipe_binding, self.model_dump(mode='json'),
                           source_sha256=digest(self.source_manifest()))
        return self

    def source_manifest(self):
        return source_records(Path(__file__).parent, source_files())

    def recipe_digest(self):
        return digest({'recipe_id': recipe.RECIPE_ID, 'candidate_sha256': recipe.CANDIDATE_SHA256,
                       'result_sha256': recipe.RESULT_SHA256,
                       'modules': {k: v.model_dump(mode='json') for k, v in recipe.strategy_parameters().items()},
                       'capital': self.capital.model_dump(mode='json'), 'exit_floor': self.exit_floor})

    def get_controller_class(self):
        return MeridianV3Controller


class _MeridianPaperBindings(NativeBindings):
    @staticmethod
    def spot_connector(controller):
        connector = controller.market_data_provider.connectors.get(controller.config.connector_name)
        if not is_native_paper_connector(connector):
            raise ValueError('Meridian requires the exact native paper connector')
        return connector


class MeridianV3Controller(ModularSpotController):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        if type(config) is not MeridianV3Config:
            raise ValueError('Exact Meridian v3 configuration required')
        # Revalidate even model_copy/model_construct input before creating runtime state.
        config = MeridianV3Config.model_validate(config.model_dump(mode='json'))
        if config.execution_mode == 'paper':
            connector = market_data_provider.connectors.get(config.connector_name)
            if not is_native_paper_connector(connector):
                raise ValueError('Meridian requires the exact native paper connector')
        super().__init__(config, market_data_provider, actions_queue, **kwargs)

    def _runtime_bindings(self):
        return _MeridianPaperBindings() if self.config.execution_mode == 'paper' else NativeBindings()

    def get_candles_config(self):
        if self.config.data.mode == 'shared':
            return []
        return [CandlesConfig(connector='okx', trading_pair=self.config.trading_pair,
                              interval='1m', max_records=self.config.max_records)]

    def determine_executor_actions(self):
        # Execution is an ordinary mutable operator flag on the generic host.
        # It must never turn a Meridian shadow instance into a live controller.
        if self.config.execution_mode != 'paper':
            self.runtime.reason = 'meridian_shadow_execution_not_supported'
            return []
        if self.config.recipe_binding is None:
            self.runtime.reason = 'meridian_recipe_binding_required'
            return []
        return super().determine_executor_actions()

    def validate_executor_config(self, config):
        return (self.config.execution_mode == 'paper' and self.config.recipe_binding is not None
                and super().validate_executor_config(config))

    def entry_permission(self, config, *, check_funds=True):
        return (self.config.execution_mode == 'paper' and self.config.recipe_binding is not None
                and super().entry_permission(config, check_funds=check_funds))

    def get_executor_class_for_config(self, config):
        if self.config.execution_mode != 'paper' or self.config.recipe_binding is None:
            raise ValueError('Only sealed Meridian paper may dispatch execution')
        return super().get_executor_class_for_config(config)

    def get_custom_info(self):
        return {**super().get_custom_info(), 'strategy_name': 'Meridian v3',
                'execution_mode': self.config.execution_mode, 'recipe_version': recipe.RECIPE_ID,
                'recipe_sha256': self.config.recipe_digest(),
                'evidence_candidate_sha256': recipe.CANDIDATE_SHA256,
                'economic_validation': 'BNB_PROXY_ORIGINAL_SIZING_ONLY',
                'capital_scope': 'RECORDER_SCOPED',
                'limits': {'entry_quote_including_reserve': '20', 'pair_quote': '100', 'program_quote': '500'}}

    def to_format_status(self):
        return [f'Meridian v3 | {recipe.RECIPE_ID} | {self.config.execution_mode}',
                *super().to_format_status()]
