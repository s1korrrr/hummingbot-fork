"""Tidewater (V4) configuration: Meridian V3's unified runtime with paced capital.

Tidewater reuses Meridian V3's symbol runtimes, portfolio writer, grants and exits
unchanged, and binds its own identity (controller, recipe, portfolio, stage table). It
adds the tide gate (occupancy-paced spacing between buys of a pair) and a per-pair
concentration limit, and an opt-in resting-entry policy. Meridian V3's files are imported,
never edited, so V3's sealed source closure does not move.

The per-pair limit lives in the symbol runtimes' native live capital (position cap, funded
cap, depth budget) and in Tidewater's pre-admission check. The P0 mandate keeps its
per-asset limit at the envelope: P0 marks the whole target infeasible when any one pair
exceeds its per-asset limit (`portfolio_v3/allocation.py`), which would hold every pair.
"""
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Literal

from pydantic import Field, model_validator

from controllers.generic import meridian_v3_recipe as recipe
from controllers.generic.meridian_v3_portfolio import CapitalStage
from controllers.generic.meridian_v3_unified_core import config as meridian
from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig, SymbolRuntimeConfig
from controllers.generic.modular_spot_core.program_budget import BudgetPolicy
from controllers.generic.tidewater_v4_core.capitulation import CapitulationOverride
from controllers.generic.tidewater_v4_core.entry_veto import ENTRY_VETOES
from controllers.generic.tidewater_v4_core.tide_gate import TideGateConfig
from hummingbot.strategy_v2.components.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    digest,
    source_records,
    verify_binding,
)
from hummingbot.strategy_v2.portfolio_v3.contracts import P0Policy, PortfolioMandate

RECIPE_ID = 'tidewater_v4_runtime_v1'
PORTFOLIO_ID = 'tidewater_v4'
CONTROLLER_NAME = 'tidewater_v4'
PRODUCER = meridian.PRODUCER
PAIRS = meridian.PAIRS
ID_PREFIX = 'modular_spot_tidewater_v4_'

# envelope, per-pair limit (symbol runtimes and Tidewater admission), order, concurrent tranches.
# Each tranche reserves order x (1 + fee reserve) = 50.15, so six fit under 350 and a seventh does
# not. At zero depth the depth budget (0.75 x 350 = 262.5) allows five; it grows as
# 262.5 + 500 x depth, so the sixth needs about a 7.5% drop from the 24 h high.
CAPITAL_STAGES = MappingProxyType({
    'tidewater_1000': CapitalStage(Decimal('1000'), Decimal('350'), Decimal('50'), 6),
    # Operator 2026-10-06 (via the regime campaign session): one pair uses the whole envelope, so
    # the tide gate is exercised on a single pair. Only that pair may have entries enabled.
    # Nineteen 50.15 tranches fit under 1,000; a twentieth does not.
    'tidewater_bnb_1000': CapitalStage(Decimal('1000'), Decimal('1000'), Decimal('50'), 19),
})
# Single-pair stages: the only pair allowed to acquire. The others stay managed, entries off.
SINGLE_PAIR_STAGES = MappingProxyType({'tidewater_bnb_1000': 'BNB-USDC'})

# Frozen priors (operator preference, not fitted). See the design doc, section T1.
TIDE_GATE_V1 = TideGateConfig(low_occupancy='0.30', high_occupancy='0.90', max_multiplier='4',
                              upward_max_occupancy='0.50')
# Research candidate fields. At None each is left out of the sealed payload and the recipe digest,
# so a configuration that does not use them keeps its pre-candidate recipe and configuration digests.
VARIANT_FIELDS = ('entry_veto', 'capitulation_override')
# Likewise, `census` is left out of the sealed payload and the recipe digest while False.
INERT_FALSE_FIELDS = ('census',)


def source_files():
    """Meridian V3's sealed closure plus every Tidewater file. Tidewater code is bound by its
    own seal; Meridian's seal is computed from Meridian's list only and does not change."""
    core = Path(__file__).resolve().parent
    files = [*meridian.source_files(), core.parent / 'tidewater_v4.py', *sorted(core.glob('*.py'))]
    return list(dict.fromkeys(files))


class TidewaterConfig(MeridianUnifiedConfig):
    controller_name: Literal['tidewater_v4'] = CONTROLLER_NAME
    recipe_version: Literal['tidewater_v4_runtime_v1'] = RECIPE_ID
    id: str = Field(pattern=r'^modular_spot_tidewater_v4_(observation|paper|live)$')
    capital_stage: Literal['tidewater_1000', 'tidewater_bnb_1000'] = Field(json_schema_extra={'is_updatable': False})
    # None disables the tide gate (ablation arm); the per-pair limit still applies.
    tide_gate: TideGateConfig | None = TIDE_GATE_V1
    # 'strict' re-checks a submitted entry with the full submit-time permission, as V3 does.
    # 'stable' revokes a submitted entry only for hard reasons (design T7).
    resting_entry: Literal['strict', 'stable'] = 'strict'
    # Research candidates (opt-in, sealed). 'e0' refuses a first-entry BUY when the frozen volatility
    # model's probability exceeds its threshold; 'x0' is its seeded-hash placebo (entry_veto.py). CP lifts the native flash pause for a
    # DCA buy in a deep capitulation (capitulation.py). Neither ever sells, cancels or demotes.
    entry_veto: Literal['e0', 'x0'] | None = None
    capitulation_override: CapitulationOverride | None = None
    # Signal census (census.py): records every raw signal and its outcomes, with no decision effect.
    # Every campaign arm sets it, the baseline included; every candidate requires it.
    census: bool = False

    @model_validator(mode='before')
    @classmethod
    def stage_envelope(cls, values):
        if isinstance(values, dict) and 'total_amount_quote' not in values and values.get('capital_stage') in CAPITAL_STAGES:
            values = {**values, 'total_amount_quote': CAPITAL_STAGES[values['capital_stage']].envelope_quote}
        return values

    @model_validator(mode='after')
    def validate_runtime(self):
        if tuple(sorted(self.priority)) != tuple(sorted(PAIRS)) or len(set(self.priority)) != 5:
            raise ValueError('explicit priority over exactly the five Tidewater pairs is required')
        if self.initial_positions:
            raise ValueError('unverified inventory adoption is not supported')
        if self.total_amount_quote != self.stage().envelope_quote:
            raise ValueError(f'the Tidewater {self.capital_stage} envelope is {self.stage().envelope_quote} quote')
        only = SINGLE_PAIR_STAGES.get(self.capital_stage)
        if only is not None and {s.trading_pair for s in self.symbols if s.entry_enabled} != {only}:
            raise ValueError(f'the {self.capital_stage} stage allows entries on {only} only')
        if self.id != f'{ID_PREFIX}{self.execution_mode}':
            raise ValueError('controller identity must name its execution mode')
        if self.execution_mode == 'paper':
            if self.connector_name != 'okx_paper_trade' or self.data.mode != 'native':
                raise ValueError('Tidewater paper requires the isolated native paper connector and injected candles')
            if self.live_capital is not None:
                raise ValueError('the native paper wallet needs no live capital binding')
        else:
            if self.connector_name != 'okx' or self.data.mode != 'shared':
                raise ValueError('observation and live read shared OKX candles through the native connector')
            if self.live_capital is not None and (self.execution_mode != 'live'
                                                  or self.live_capital.account_id != self.account_id
                                                  or self.live_capital.connector != self.connector_name
                                                  or self.live_capital.envelope_quote != self.stage().envelope_quote):
                raise ValueError('live capital must bind this live account, connector and the stage envelope')
        if self.execution_mode == 'observation' and self.execution_enabled:
            raise ValueError('observation mode cannot dispatch execution')
        if self.execution_enabled and self.recipe_binding is None:
            raise ValueError('executable Tidewater requires a sealed recipe binding')
        symbols = self.symbol_configs()
        if (self.entry_veto is not None or self.capitulation_override is not None) and not self.census:
            raise ValueError('a research candidate requires census=True (its population must be recorded)')
        cp = self.capitulation_override
        if cp is not None and any(s.flash_crash is None or s.depth_budget is None
                                  or s.depth_budget.lookback_minutes != cp.lookback_minutes for s in symbols.values()):
            raise ValueError('the capitulation override needs the flash pause and a 1,440-minute depth-budget high')
        if self.recipe_binding is not None:
            if self.recipe_binding.recipe_id != RECIPE_ID or self.recipe_binding.recipe_sha256 != self.recipe_digest():
                raise ValueError('Tidewater recipe binding differs')
            verify_binding(self.recipe_binding, self.sealed_payload(), source_sha256=digest(self.source_manifest()))
        return self

    # ------------------------------------------------------------------ bindings
    def stage(self):
        return CAPITAL_STAGES[self.capital_stage]

    def pair_limit_quote(self):
        return self.stage().per_asset_quote

    def entry_veto_model(self):
        return None if self.entry_veto is None else ENTRY_VETOES[self.entry_veto]

    def variants(self):
        """The research candidates this configuration enables, for receipts and reporting."""
        return {'census': self.census, 'entry_veto': self.entry_veto,
                'capitulation_override': None if self.capitulation_override is None
                else self.capitulation_override.model_dump(mode='json')}

    def symbol_configs(self):
        """Meridian's per-symbol native configuration at Tidewater's stage, under Tidewater's
        budget group and program revision."""
        result = {}
        for pair, config in super().symbol_configs().items():
            values = config.model_dump()
            values.update(budget=BudgetPolicy(group=CONTROLLER_NAME, max_quote=self.stage().envelope_quote, max_assets=5),
                          program_revision=CONTROLLER_NAME)
            result[pair] = SymbolRuntimeConfig(**values)
        return result

    def mandate(self):
        stage = self.stage()
        return PortfolioMandate(
            portfolio_id=PORTFOLIO_ID, account_id=self.account_id, version=RECIPE_ID, mode='observation',
            reference_quote=stage.envelope_quote, funding_envelope_quote=stage.envelope_quote,
            per_asset_quote=stage.envelope_quote, order_quote=stage.order_quote,
            gross_reference_ratio=1, gross_nav_ratio=1,
            cash_reserve_quote=self.risk.cash_reserve_quote, fee_reserve_fraction=self.risk.fee_reserve_fraction,
            pairs=PAIRS, loss_policy_sha256=digest({'loss_rails': 'none', 'guards': self.risk.model_dump(mode='json'),
                                                    'exit_floor': 'protected', 'no_loss_sell': True}),
            max_age_ms=self.max_snapshot_age_ms, target_lifetime_ms=self.target_lifetime_ms)

    def p0_policy(self):
        return P0Policy(version=RECIPE_ID, horizon_ms=60000, pair_priority=self.priority, producers=(PRODUCER,))

    def source_manifest(self):
        return source_records(meridian.source_root(), source_files())

    def sealed_payload(self):
        payload = super().sealed_payload()
        for name in VARIANT_FIELDS:
            if payload.get(name, False) is None:
                del payload[name]
        for name in INERT_FALSE_FIELDS:
            if payload.get(name) is False:
                del payload[name]
        return payload

    def recipe_digest(self):
        values = {'recipe_id': RECIPE_ID, 'candidate_sha256': recipe.CANDIDATE_SHA256,
                  'result_sha256': recipe.RESULT_SHA256, 'mandate_sha256': self.mandate().identity,
                  'policy_sha256': self.p0_policy().identity,
                  'modules': {k: v.model_dump(mode='json') for k, v in self.recipe_modules().items()},
                  'capital': self.recipe_capital().model_dump(mode='json'),
                  'pair_limit_quote': str(self.pair_limit_quote()),
                  'tide_gate': None if self.tide_gate is None else self.tide_gate.model_dump(mode='json'),
                  'resting_entry': self.resting_entry}
        if self.entry_veto is not None:  # the frozen model constants are part of the recipe
            values['entry_veto'] = {'id': self.entry_veto, 'model': self.entry_veto_model().model_dump(mode='json')}
        if self.capitulation_override is not None:
            values['capitulation_override'] = self.capitulation_override.model_dump(mode='json')
        if self.census:
            values['census'] = True
        return digest(values)

    def seal(self):
        return RecipeBinding(recipe_id=RECIPE_ID, recipe_sha256=self.recipe_digest(),
                             source_sha256=digest(self.source_manifest()),
                             configuration_sha256=configuration_digest(self.sealed_payload()))

    def get_controller_class(self):
        from controllers.generic.tidewater_v4 import TidewaterController
        return TidewaterController
