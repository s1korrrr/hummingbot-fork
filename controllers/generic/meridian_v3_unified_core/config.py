"""One portfolio configuration with five symbol bindings. No per-symbol wallet or owner.

Symbol parameters are the frozen p23 recipe for every pair in this version. A
per-symbol parameter override is a new recipe version with its own evidence; the
P0 comparator evaluates p23 eligibility, so a symbol running different signal
parameters would silently disagree with the portfolio decision.
"""
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, StrictBool, field_validator, model_validator

from controllers.generic import meridian_v3_portfolio, meridian_v3_recipe as recipe, modular_spot
from controllers.generic.meridian_v3_unified_core.market_context import MarketContextPolicy
from controllers.generic.modular_spot import ModularSpotControllerConfig
from controllers.generic.modular_spot_core.data_reader import DataPolicy
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy
from hummingbot.strategy_v2.components.config import PolicyModel, RiskPolicy
from hummingbot.strategy_v2.components.decision_protocol import NAME
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    digest,
    source_records,
    verify_binding,
)
from hummingbot.strategy_v2.controllers.controller_base import ControllerConfigBase
from hummingbot.strategy_v2.portfolio_v3.contracts import P0Policy, PortfolioMandate
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import LiveCapitalBinding

RECIPE_ID = 'meridian_v3_unified_runtime_v1'
PORTFOLIO_ID = 'meridian_v3'
PRODUCER = 'bnb_p23_mr'
PAIRS = recipe.PAIRS
ORDER_QUOTE, ASSET_QUOTE, ENVELOPE_QUOTE = Decimal('20'), Decimal('100'), Decimal('500')
# Meridian is a DCA controller with no loss rail. The shared p23 runtime still takes a
# RiskPolicy, so it receives no PnL signal (see SymbolBindings.snapshot) and these
# fractions are never reached. They exist only to satisfy the shared policy type.
UNREACHABLE_LOSS_FRACTION = Decimal('.99')


def source_files():
    core = Path(__file__).parent
    return [Path(__file__).parents[1] / 'meridian_v3_unified.py', *sorted(core.glob('*.py')),
            Path(recipe.__file__), Path(meridian_v3_portfolio.__file__),
            *[Path(p) for p in modular_spot.source_files()]]


_SOURCE_MANIFEST = {}


class SymbolRuntimeConfig(ModularSpotControllerConfig):
    """Effective native configuration of one symbol context. Candles are OKX market
    data even when orders execute on the isolated paper connector."""
    candles_connector: Literal['okx'] = 'okx'

    def source_manifest(self):
        # The p23 runtime re-checks its fingerprint on every tick. Its engine sources are
        # fixed for the life of the process (read-only image, hot reload disabled), so they
        # are hashed once; configuration values are still compared on every call.
        if 'modular_spot' not in _SOURCE_MANIFEST:
            _SOURCE_MANIFEST['modular_spot'] = super().source_manifest()
        return _SOURCE_MANIFEST['modular_spot']


class MeridianRiskPolicy(PolicyModel):
    """Execution guards only. There is deliberately no drawdown or daily-loss limit."""
    max_spread_fraction: Decimal = Field(gt=0, lt=1)
    fee_reserve_fraction: Decimal = Field(gt=0, lt=1)
    cash_reserve_quote: Decimal = Field(ge=0)

    def native(self):
        return RiskPolicy(max_drawdown_fraction=UNREACHABLE_LOSS_FRACTION,
                          max_daily_loss_fraction=UNREACHABLE_LOSS_FRACTION, **self.model_dump())


class SymbolPolicyBinding(PolicyModel):
    """One symbol context. `entry_enabled=false` stops new acquisitions only.

    Management membership is derived from native obligations, never from this flag:
    a disabled symbol keeps its positions, orders, bag and exits.
    """
    trading_pair: Literal['BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC']
    entry_enabled: StrictBool = True
    policy_version: Literal['bnb_p23_v1'] = 'bnb_p23_v1'


class MeridianUnifiedConfig(ControllerConfigBase):
    controller_name: Literal['meridian_v3_unified'] = 'meridian_v3_unified'
    controller_type: Literal['generic'] = 'generic'
    recipe_version: Literal['meridian_v3_unified_runtime_v1'] = RECIPE_ID
    id: str = Field(pattern=r'^modular_spot_meridian_v3_unified_(observation|paper|live)$')
    execution_mode: Literal['observation', 'paper', 'live']
    connector_name: Literal['okx', 'okx_paper_trade']
    account_id: str = Field(pattern=NAME)
    symbols: tuple[SymbolPolicyBinding, ...] = Field(min_length=5, max_length=5, json_schema_extra={'is_updatable': True})
    # Explicit P0 competition order for scarce capital. Never inferred or defaulted.
    priority: tuple[str, ...] = Field(min_length=5, max_length=5)
    risk: MeridianRiskPolicy = Field(...)
    data: DataPolicy = Field(default_factory=DataPolicy)
    execution_enabled: StrictBool = False
    live_capital: LiveCapitalBinding | None = None
    recipe_binding: RecipeBinding | None = None
    # Shared Market Context is observe-only: recorded and reported, never read by a decision.
    market_context: MarketContextPolicy = Field(default_factory=MarketContextPolicy)
    max_candle_age_seconds: int = Field(default=120, ge=1, le=600, strict=True)
    max_native_report_age_seconds: int = Field(default=5, ge=1, le=60, strict=True)
    max_snapshot_age_ms: int = Field(default=5000, ge=100, le=60000, strict=True)
    target_lifetime_ms: int = Field(default=30000, ge=1000, le=60000, strict=True)
    total_amount_quote: Decimal = Field(default=ENVELOPE_QUOTE, json_schema_extra={'is_updatable': False})
    # Read by ModularPositionExecutor from its owning controller: the protected no-loss floor
    # (after-fee breakeven raised by activation - delta), never the plain after-fee price.
    exit_floor: Literal['protected'] = 'protected'
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False, validate_default=True)

    @field_validator('symbols')
    @classmethod
    def five_distinct_symbols(cls, values):
        if {s.trading_pair for s in values} != set(PAIRS) or len(values) != len(PAIRS):
            raise ValueError('Meridian unified requires exactly one binding for each of its five pairs')
        return tuple(sorted(values, key=lambda s: s.trading_pair))

    @model_validator(mode='after')
    def validate_runtime(self):
        if tuple(sorted(self.priority)) != tuple(sorted(PAIRS)) or len(set(self.priority)) != 5:
            raise ValueError('explicit priority over exactly the five Meridian pairs is required')
        if self.initial_positions:
            raise ValueError('unverified inventory adoption is not supported')
        if self.total_amount_quote != ENVELOPE_QUOTE:
            raise ValueError('the Meridian envelope is the versioned 500 quote recipe')
        if self.id != f'modular_spot_meridian_v3_unified_{self.execution_mode}':
            raise ValueError('controller identity must name its execution mode')
        if self.execution_mode == 'paper':
            if self.connector_name != 'okx_paper_trade' or self.data.mode != 'native':
                raise ValueError('Meridian paper requires the isolated native paper connector and injected candles')
            if self.live_capital is not None:
                raise ValueError('the native paper wallet needs no live capital binding')
        else:
            if self.connector_name != 'okx' or self.data.mode != 'shared':
                raise ValueError('observation and live read shared OKX candles through the native connector')
            if self.live_capital is not None and (self.execution_mode != 'live'
                                                  or self.live_capital.account_id != self.account_id
                                                  or self.live_capital.connector != self.connector_name
                                                  or self.live_capital.envelope_quote != ENVELOPE_QUOTE):
                raise ValueError('live capital must bind this live account, connector and the 500 envelope')
        if self.execution_mode == 'observation' and self.execution_enabled:
            raise ValueError('observation mode cannot dispatch execution')
        if self.execution_enabled and self.recipe_binding is None:
            raise ValueError('executable Meridian requires a sealed recipe binding')
        self.symbol_configs()  # Every symbol must satisfy the native generic host validation.
        if self.recipe_binding is not None:
            if self.recipe_binding.recipe_id != RECIPE_ID or self.recipe_binding.recipe_sha256 != self.recipe_digest():
                raise ValueError('Meridian unified recipe binding differs')
            verify_binding(self.recipe_binding, self.sealed_payload(), source_sha256=digest(self.source_manifest()))
        return self

    # ------------------------------------------------------------------ bindings
    def owned_trading_pairs(self):
        """Every instrument this one native owner manages; read by the engine for attribution."""
        return tuple(s.trading_pair for s in self.symbols)

    def symbol_configs(self):
        """Effective native per-symbol configuration; one owner id, five instruments."""
        modules = recipe.strategy_parameters()
        result = {}
        for binding in self.symbols:
            values = dict(
                controller_name='modular_spot', id=self.id, connector_name=self.connector_name,
                trading_pair=binding.trading_pair, total_amount_quote=ASSET_QUOTE,
                execution_enabled=self.execution_enabled, acquisition_enabled=binding.entry_enabled,
                max_records=1500, max_candle_age_seconds=self.max_candle_age_seconds,
                max_native_report_age_seconds=self.max_native_report_age_seconds,
                risk=self.risk.native(), data=self.data, account=self.account_id,
                budget=BudgetPolicy(group='meridian_v3_unified', max_quote=ENVELOPE_QUOTE, max_assets=5),
                program_revision='meridian_v3_unified', policy_revision=binding.policy_version,
                capital=recipe.capital_policy(self.risk.fee_reserve_fraction), exit_floor=self.exit_floor,
                manual_kill_switch=self.manual_kill_switch, **modules)
            result[binding.trading_pair] = SymbolRuntimeConfig(**values)
        return result

    def mandate(self):
        return PortfolioMandate(
            portfolio_id=PORTFOLIO_ID, account_id=self.account_id, version=RECIPE_ID, mode='observation',
            reference_quote=ENVELOPE_QUOTE, funding_envelope_quote=ENVELOPE_QUOTE, per_asset_quote=ASSET_QUOTE,
            order_quote=ORDER_QUOTE, gross_reference_ratio=1, gross_nav_ratio=1,
            cash_reserve_quote=self.risk.cash_reserve_quote, fee_reserve_fraction=self.risk.fee_reserve_fraction,
            pairs=PAIRS, loss_policy_sha256=digest({'loss_rails': 'none', 'guards': self.risk.model_dump(mode='json'),
                                                    'exit_floor': 'protected', 'no_loss_sell': True}),
            max_age_ms=self.max_snapshot_age_ms, target_lifetime_ms=self.target_lifetime_ms)

    def p0_policy(self):
        return P0Policy(version=RECIPE_ID, horizon_ms=60000, pair_priority=self.priority, producers=(PRODUCER,))

    def source_manifest(self):
        return source_records(Path(__file__).parents[2], source_files())

    def recipe_digest(self):
        return digest({'recipe_id': RECIPE_ID, 'candidate_sha256': recipe.CANDIDATE_SHA256,
                       'result_sha256': recipe.RESULT_SHA256, 'mandate_sha256': self.mandate().identity,
                       'policy_sha256': self.p0_policy().identity,
                       'modules': {k: v.model_dump(mode='json') for k, v in recipe.strategy_parameters().items()},
                       'capital': recipe.capital_policy(self.risk.fee_reserve_fraction).model_dump(mode='json')})

    def sealed_payload(self):
        """Behavioural configuration. Per-symbol entry switches are operator controls,
        like `execution_enabled`: pausing one symbol must not require a new seal."""
        payload = self.model_dump(mode='json')
        # Market context is observe-only: recorded, never read by a decision (proved by paired runs).
        # No gating mode exists; one would be behavioural and must be bound here before it is accepted.
        payload.pop('market_context')
        payload['symbols'] = [{k: v for k, v in s.items() if k != 'entry_enabled'} for s in payload['symbols']]
        return payload

    def seal(self):
        """The binding an operator reviews before enabling execution."""
        return RecipeBinding(recipe_id=RECIPE_ID, recipe_sha256=self.recipe_digest(),
                             source_sha256=digest(self.source_manifest()),
                             configuration_sha256=configuration_digest(self.sealed_payload()))

    def update_markets(self, markets):
        for pair in PAIRS:
            markets = markets.add_or_update(self.connector_name, pair)
        return markets

    def get_controller_class(self):
        from controllers.generic.meridian_v3_unified import MeridianUnifiedController
        return MeridianUnifiedController
