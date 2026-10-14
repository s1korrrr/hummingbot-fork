"""V4: the backtested v3c recipe as a stand-alone controller on the generic native modular host.

One controller per pair, each with its own explicit budget; no shared portfolio writer, P0
target, tide gate or runner, and no Meridian V3 or Tidewater code (`meridian_v4_recipe.py`).
Same pattern as the per-pair `meridian_v3.py`: native paper and disabled shared-data shadow
only. Live activation needs a separately admitted ownership path.
"""
from decimal import Decimal
from pathlib import Path
from typing import ClassVar, Literal

from pydantic import Field, model_validator

from controllers.generic import meridian_v4_recipe as recipe, modular_spot
from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.bag import (
    AdaptiveTrailingConfig,
    BagConfig,
    BuyGapConfig,
    FlashCrashConfig,
    LiveCapitalConfig,
)
from controllers.generic.modular_spot_core.config import AcquisitionPolicy, RiskPolicy, SignalPolicy, TrailingPolicy
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.program_budget import BudgetPolicy
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    digest,
    source_records,
    verify_binding,
)
from hummingbot.strategy_v2.components.source_identity import component_source_files

CONFIG_CLASS = 'MeridianV4Config'
PROGRAM = 'meridian_v4'


def source_files():
    return [Path(__file__), Path(recipe.__file__), *modular_spot.source_files()]


class MeridianV4Config(ModularSpotControllerConfig):
    controller_name: Literal['meridian_v4'] = 'meridian_v4'
    recipe_version: Literal['meridian_v4_v3c_v1'] = recipe.RECIPE_ID
    execution_mode: Literal['paper', 'shadow']
    connector_name: Literal['okx', 'okx_paper_trade']
    candles_connector: Literal['okx'] = 'okx'
    trading_pair: Literal['BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC']
    # The per-pair budget (position and funded cap). The backtests had none; never defaulted.
    total_amount_quote: Decimal = Field(..., gt=0, json_schema_extra={'is_updatable': False})
    max_records: Literal[1500] = 1500
    risk: RiskPolicy = Field(...)  # the backtests establish no native loss/fee rails
    signal: SignalPolicy = Field(default_factory=lambda: recipe.strategy_parameters()['signal'])
    acquisition: AcquisitionPolicy = Field(default_factory=lambda: recipe.strategy_parameters()['acquisition'])
    trailing: TrailingPolicy = Field(default_factory=lambda: recipe.strategy_parameters()['trailing'])
    bag: BagConfig = Field(default_factory=lambda: recipe.strategy_parameters()['bag'])
    adaptive_trailing: AdaptiveTrailingConfig = Field(default_factory=lambda: recipe.strategy_parameters()['adaptive_trailing'])
    buy_gap: BuyGapConfig = Field(default_factory=lambda: recipe.strategy_parameters()['buy_gap'])
    flash_crash: FlashCrashConfig = Field(default_factory=lambda: recipe.strategy_parameters()['flash_crash'])
    depth_budget: None = None  # V4 has no depth budget (the backtests ran without one)
    capital: LiveCapitalConfig | None = None
    # The program allocation across the five V4 controllers; explicit, never defaulted.
    budget: BudgetPolicy = Field(...)
    exit_floor: Literal['protected'] = 'protected'
    program_revision: Literal['meridian_v4'] = PROGRAM
    policy_revision: Literal['meridian_v4_v3c_v1'] = recipe.RECIPE_ID

    # The versioned recipe this class binds and its program identity; a variant (meridian_v4_d2h)
    # rebinds these together with its Literal identity fields, never V4's values.
    RECIPE: ClassVar = recipe
    PROGRAM: ClassVar[str] = PROGRAM

    def expected_depth_budget(self):
        return None

    @model_validator(mode='after')
    def validate_composition(self):
        # Reuse the generic host's full validation without modifying its source,
        # defaults, binding scope or legacy consumers. Validate our own binding below.
        r, program = self.RECIPE, self.PROGRAM
        expected_capital = r.capital_policy(self.risk.fee_reserve_fraction, self.total_amount_quote)
        if self.capital is None:
            self.capital = expected_capital
        if self.capital != expected_capital:
            raise ValueError(f'{program} requires the versioned {r.ORDER_QUOTE}-quote capital recipe at its per-pair budget')
        for name, expected in r.strategy_parameters().items():
            if getattr(self, name) != expected:
                raise ValueError(f'{program} {name} differs from the versioned recipe; new version required')
        expected_depth = self.expected_depth_budget()
        if self.depth_budget is None and expected_depth is not None:
            self.depth_budget = expected_depth
        if self.depth_budget != expected_depth:
            raise ValueError(f'{program} depth_budget differs from the versioned recipe at its per-pair budget')
        if self.budget.group != program or self.budget.max_assets != len(r.PAIRS):
            raise ValueError(f'{program} requires its dedicated five-asset {program} program budget')
        if self.universe.mode != 'fixed' or self.universe.symbols or self.universe.benchmarks:
            raise ValueError(f'{program} keeps a fixed pair per controller')
        expected_id = f'modular_spot_{program}_{self.execution_mode}_{self.trading_pair.split("-")[0].lower()}'
        if self.id != expected_id or self.account != f'{program}_{self.execution_mode}':
            raise ValueError(f'{program} requires distinct mode, pair and account identities')
        if self.execution_mode == 'paper':
            if self.connector_name != 'okx_paper_trade' or self.data.mode != 'native':
                raise ValueError('V4 paper requires the isolated native paper connector and injected candles')
            if self.execution_enabled and self.recipe_binding is None:
                raise ValueError('Executable V4 paper requires a sealed recipe binding')
        elif self.connector_name != 'okx' or self.execution_enabled or self.data.mode != 'shared':
            raise ValueError('V4 shadow requires shared data and cannot dispatch execution')
        base = self.model_dump(mode='json', exclude={'recipe_version', 'execution_mode', 'candles_connector'})
        base.update(controller_name='modular_spot', recipe_binding=None)
        ModularSpotControllerConfig.model_validate(base)
        if self.recipe_binding is not None:
            if (self.recipe_binding.recipe_id != r.RECIPE_ID
                    or self.recipe_binding.recipe_sha256 != self.recipe_digest()):
                raise ValueError('V4 recipe binding differs')
            verify_binding(self.recipe_binding, self.model_dump(mode='json'),
                           source_sha256=digest(self.source_manifest()))
        return self

    def source_manifest(self):
        return source_records(Path(__file__).parent, source_files())

    def seal(self):
        """The recipe binding of this exact configuration and source; it grants no execution authority."""
        return RecipeBinding(recipe_id=self.RECIPE.RECIPE_ID, recipe_sha256=self.recipe_digest(),
                             source_sha256=digest(self.source_manifest()),
                             configuration_sha256=configuration_digest(self.model_dump(mode='json')))

    def recipe_digest(self):
        r = self.RECIPE
        modules = {k: v.model_dump(mode='json') for k, v in r.strategy_parameters().items()}
        if self.depth_budget is not None:  # absent for V4, so V4's digest is unchanged
            modules['depth_budget'] = self.depth_budget.model_dump(mode='json')
        return digest({'recipe_id': r.RECIPE_ID, 'candidate_sha256': r.CANDIDATE_SHA256,
                       'prepared_sha256': r.PREPARED_SHA256, 'modules': modules,
                       'capital': self.capital.model_dump(mode='json'), 'exit_floor': self.exit_floor})

    def get_controller_class(self):
        return MeridianV4Controller


class _V4PaperBindings(NativeBindings):
    @staticmethod
    def spot_connector(controller):
        connector = controller.market_data_provider.connectors.get(controller.config.connector_name)
        if not is_native_paper_connector(connector):
            raise ValueError('V4 requires the exact native paper connector')
        return connector


class MeridianV4Controller(ModularSpotController):
    allow_implementation_hot_reload = False
    CONFIG = MeridianV4Config
    STRATEGY_NAME = 'V4'

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        if type(config) is not self.CONFIG:
            raise ValueError(f'Exact {self.STRATEGY_NAME} configuration required')
        # Revalidate even model_copy/model_construct input before creating runtime state.
        config = self.CONFIG.model_validate(config.model_dump(mode='json'))
        if config.execution_mode == 'paper':
            connector = market_data_provider.connectors.get(config.connector_name)
            if not is_native_paper_connector(connector):
                raise ValueError(f'{self.STRATEGY_NAME} requires the exact native paper connector')
        super().__init__(config, market_data_provider, actions_queue, **kwargs)

    def _runtime_bindings(self):
        return _V4PaperBindings() if self.config.execution_mode == 'paper' else NativeBindings()

    def get_candles_config(self):
        if self.config.data.mode == 'shared':
            return []
        return [CandlesConfig(connector='okx', trading_pair=self.config.trading_pair,
                              interval='1m', max_records=self.config.max_records)]

    def determine_executor_actions(self):
        # Execution is an ordinary mutable operator flag on the generic host.
        # It must never turn a V4 shadow instance into a live controller.
        if self.config.execution_mode != 'paper':
            self.runtime.reason = f'{self.config.PROGRAM}_shadow_execution_not_supported'
            return []
        if self.config.recipe_binding is None:
            self.runtime.reason = f'{self.config.PROGRAM}_recipe_binding_required'
            return []
        return super().determine_executor_actions()

    def validate_executor_config(self, config):
        return (self.config.execution_mode == 'paper' and self.config.recipe_binding is not None
                and super().validate_executor_config(config))

    def entry_permission(self, config, *, check_funds=True):
        if self.config.execution_mode != 'paper' or self.config.recipe_binding is None:
            return False
        if super().entry_permission(config, check_funds=check_funds):
            return True
        # A submitted entry (the executor re-checks with check_funds=False) rests until it fills or reaches
        # its entry timeout, as in the backtested recipe. Transient market holds (a stale book on any pair of
        # the shared paper connector, a momentarily wide spread) must not cancel it; definitive revocations do.
        return not check_funds and self._resting_entry_retained(config)

    def _resting_entry_retained(self, config):
        c, runtime = self.config, self.runtime
        if (not c.execution_enabled or c.manual_kill_switch or not c.acquisition_enabled or runtime.fault
                or config is None or runtime.pending.get(config.id) != config):
            return False
        now = self.market_data_provider.time()
        return 0 <= now - config.timestamp <= c.acquisition.entry_timeout_seconds

    def get_executor_class_for_config(self, config):
        if self.config.execution_mode != 'paper' or self.config.recipe_binding is None:
            raise ValueError(f'Only sealed {self.STRATEGY_NAME} paper may dispatch execution')
        return super().get_executor_class_for_config(config)

    def get_custom_info(self):
        r = self.config.RECIPE
        return {**super().get_custom_info(), 'strategy_name': self.STRATEGY_NAME,
                'execution_mode': self.config.execution_mode, 'recipe_version': r.RECIPE_ID,
                'recipe_sha256': self.config.recipe_digest(),
                'evidence_candidate_sha256': r.CANDIDATE_SHA256,
                'economic_validation': r.ECONOMIC_VALIDATION,
                'capital_scope': 'RECORDER_SCOPED',
                'limits': {'entry_quote': str(r.ORDER_QUOTE), 'pair_quote': str(self.config.total_amount_quote),
                           'program_quote': str(self.config.budget.max_quote)}}

    def to_format_status(self):
        return [f'{self.STRATEGY_NAME} | {self.config.RECIPE.RECIPE_ID} | {self.config.execution_mode}',
                *super().to_format_status()]
