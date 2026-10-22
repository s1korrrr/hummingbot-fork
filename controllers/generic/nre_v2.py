"""Isolated NRE v2 recipe host. Optional initial-entry gate over ModularSpotController."""
import threading
from contextvars import ContextVar
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator

import controllers.generic.modular_spot_core.runtime as modular_runtime
from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.data_reader import dataset_for
from controllers.generic.modular_spot_core.domain import PolicyError, identity
from controllers.generic.modular_spot_core.recipe_contract import (
    RecipeBinding,
    configuration_digest,
    digest,
    source_digest,
    source_records,
    verify_binding,
)
from controllers.generic.modular_spot_core.runtime import ModularRuntime
from controllers.generic.nre_v2_core import admission, recovery_path
from controllers.generic.nre_v2_core.admission import FEATURES, ActionRole, GatePolicy, GateResult
from controllers.generic.nre_v2_core.binding import (
    PathContract,
    observation_ref,
    observe,
    scale_from_snapshot,
    seconds_to_ms,
)
from controllers.generic.nre_v2_core.source_manifest import MODULES
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope, Opportunity, Prerequisite
from hummingbot.strategy_v2.components.domain import DataUnavailable
from hummingbot.strategy_v2.components.source_identity import component_source_files

CONFIG_CLASS = 'NREModularControllerConfig'
_PLAN_GATE = threading.Lock()
_NRE_PLAN_ACTIVE = ContextVar('nre_v2_plan_active', default=False)


def source_root():
    return Path(__file__).parent


def source_files():
    package = Path(__file__).parent
    core = package / 'nre_v2_core'
    return [
        Path(__file__),
        *[core / name for name in sorted(MODULES)],
        package / 'modular_spot.py',
        *sorted((package / 'modular_spot_core').rglob('*.py')),
    ]


class NREPathPolicy(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)
    lookback_bars: int = Field(ge=1, le=10000, strict=True)
    rebound_atr: float = Field(gt=0)
    invalidation_atr: float = Field(gt=0)
    max_bars: int = Field(ge=2, le=100000, strict=True)
    max_transport_age_ms: int = Field(ge=1, le=60000, strict=True)

    @model_validator(mode='after')
    def lookback_below_horizon(self):
        if self.lookback_bars >= self.max_bars:
            raise ValueError('lookback_bars must be below max_bars')
        return self


class NREModularControllerConfig(ModularSpotControllerConfig):
    controller_name: Literal['nre_v2'] = 'nre_v2'
    execution_enabled: StrictBool = False
    nre_model_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    nre_scaler_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    nre_horizon_ms: int = Field(ge=1, le=86400000, strict=True)
    nre_minimum_net_bps: Decimal
    nre_atr_length: int = Field(ge=2, le=60, strict=True)
    nre_required_features: tuple[str, ...]
    nre_path: NREPathPolicy

    @model_validator(mode='after')
    def validate_composition(self):
        binding = self.recipe_binding
        object.__setattr__(self, 'recipe_binding', None)
        super().validate_composition()
        if (not self.nre_required_features or len(self.nre_required_features) != len(set(self.nre_required_features))
                or not set(self.nre_required_features) <= FEATURES):
            raise ValueError('explicit supported feature requirements needed')
        if binding is not None:
            object.__setattr__(self, 'recipe_binding', binding)
            verify_binding(binding, self.model_dump(mode='json'),
                           source_sha256=source_digest(source_root(), source_files()))
        return self

    def fingerprint(self):
        values = self.model_dump(mode='json', exclude={'manual_kill_switch', 'execution_enabled', 'acquisition_enabled'})
        universe = dict(values['universe'])
        universe['symbols'] = []
        universe['benchmarks'] = []
        values['universe'] = universe
        data = dict(values['data'])
        data['symbols'] = []
        values['data'] = data
        sources = source_records(source_root(), source_files())
        return identity({'configuration': values, 'sources': sources, 'policy_version': 2})

    def get_controller_class(self):
        return NREModularController


class NRERuntime(ModularRuntime):
    def __init__(self, controller, bindings=None):
        super().__init__(controller, bindings)
        self.nre_snapshot = None
        self.nre_path = None
        self.nre_prediction = None

    def _snapshot_for_gate(self):
        owned = getattr(self, 'nre_snapshot', None)
        if owned is not None:
            return owned
        c = self.controller.config
        if c.data.mode == 'shared' and self.pool is not None:
            now = self.controller.market_data_provider.time()
            return self.pool.snapshot(dataset_for(c), now=now, max_age=c.max_candle_age_seconds)
        return None

    def _recipe(self):
        c = self.controller.config
        if c.recipe_binding is not None:
            return c.recipe_binding
        dumped = c.model_dump(mode='json')
        return RecipeBinding(
            recipe_id='nre_v2', recipe_sha256=digest('nre-unbound'),
            source_sha256=source_digest(source_root(), source_files()),
            configuration_sha256=configuration_digest(dumped),
        )

    def _bind_path(self, snapshot, now_ms, *, seed: bool):
        c = self.controller.config
        atr, parent, impl, init = scale_from_snapshot(snapshot, length=c.nre_atr_length)
        if seed or self.nre_path is None:
            recipe = self._recipe()
            contract = PathContract(
                scope=DecisionScope(program_id=c.id, owner_id=c.id, connector=c.connector_name,
                                    trading_pair=c.trading_pair, recipe_sha256=recipe.recipe_sha256),
                recipe=recipe, dataset=snapshot.dataset, setup=c.id,
                setup_event_ms=parent.event_time_ms, setup_available_ms=parent.available_time_ms,
                setup_evidence_sha256=digest({'setup': c.id, 'primary': parent.content_sha256}),
                frozen_scale=atr, scale_parent=parent, scale_implementation_sha256=impl,
                initialization_sha256=init,
                policy=recovery_path.Policy(
                    c.nre_path.lookback_bars, float(c.nre_path.rebound_atr),
                    float(c.nre_path.invalidation_atr), c.nre_path.max_bars),
                max_transport_age_ms=c.nre_path.max_transport_age_ms)
            self.nre_path = observe(contract, None, snapshot, now_ms=now_ms)
            return
        self.nre_path = observe(self.nre_path.contract, self.nre_path, snapshot, now_ms=now_ms)

    def _evaluate_gate(self, role_name):
        c = self.controller.config
        snapshot = self._snapshot_for_gate()
        now_ms = seconds_to_ms(self.controller.market_data_provider.time())
        if snapshot is None:
            return GateResult(True, 'path_model_or_policy_unavailable',
                              Prerequisite(name='nre_initial_entry', status='UNAVAILABLE'))
        if role_name == 'INITIAL_ENTRY':
            try:
                self._bind_path(snapshot, now_ms, seed=self.nre_path is None)
            except (PolicyError, DataUnavailable, ValueError):
                self.nre_path = None
        recipe = self._recipe()
        opportunity = Opportunity(
            scope=DecisionScope(program_id=c.id, owner_id=c.id, connector=c.connector_name,
                                trading_pair=c.trading_pair, recipe_sha256=recipe.recipe_sha256),
            setup=c.id, action='acquire', primary=observation_ref(snapshot),
            created_at_ms=now_ms, lifetime_ms=c.nre_horizon_ms)
        role = ActionRole(opportunity.opportunity_id, role_name,
                          digest({'role': role_name, 'opportunity_id': opportunity.opportunity_id}),
                          opportunity.created_at_ms)
        policy = None
        if c.recipe_binding is not None:
            policy = GatePolicy(
                recipe=c.recipe_binding, model_sha256=c.nre_model_sha256, scaler_sha256=c.nre_scaler_sha256,
                horizon_ms=c.nre_horizon_ms, required_features=tuple(c.nre_required_features),
                minimum_net_bps=c.nre_minimum_net_bps)
        return admission.evaluate(opportunity=opportunity, role=role, state=self.nre_path,
                                  prediction=self.nre_prediction, policy=policy, now_ms=now_ms)

    def _acquire_allowed(self, proposal):
        role = admission.classify_role(action='acquire', stage=proposal.stage)
        if role in ('SCALE_IN', 'PYRAMID', 'REDUCE'):
            return True
        return admission.acquisition_dispatch_allowed(self._evaluate_gate(role))

    def _without_new_claim(self, state, revised, proposal):
        claims = tuple(item for item in revised.claims if item != proposal.identifier)
        committed = Decimal('0') if state is None else state.committed_quote
        last_entry = Decimal('0') if state is None else state.last_entry_time
        return revised.model_copy(update={'claims': claims, 'committed_quote': committed, 'last_entry_time': last_entry})

    def actions(self):
        c = self.controller.config
        if not c.execution_enabled or c.manual_kill_switch or self.fault or c.universe.mode == 'sell_only':
            return super().actions()
        snapshot = self._snapshot_for_gate()
        if snapshot is not None and self.nre_path is not None:
            try:
                self._bind_path(snapshot, seconds_to_ms(self.controller.market_data_provider.time()), seed=False)
            except (PolicyError, DataUnavailable, ValueError):
                self.nre_path = None
        original = modular_runtime.plan

        def gated_plan(state, inventory, *args, **kwargs):
            revised, proposal, reason = original(state, inventory, *args, **kwargs)
            if not _NRE_PLAN_ACTIVE.get():
                return revised, proposal, reason
            if proposal is None or self._acquire_allowed(proposal):
                return revised, proposal, reason
            return self._without_new_claim(state, revised, proposal), None, 'nre_initial_entry_hold'

        # ponytail: module-global plan hook; ModularRuntime.actions binds `plan` at import.
        # Upgrade: inject a plan callable on ModularRuntime (host source-seal change).
        with _PLAN_GATE:
            modular_runtime.plan = gated_plan
            token = _NRE_PLAN_ACTIVE.set(True)
            try:
                return super().actions()
            finally:
                _NRE_PLAN_ACTIVE.reset(token)
                modular_runtime.plan = original


class NREModularController(ModularSpotController):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    def __init__(self, config: NREModularControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        previous = self.runtime
        previous.close_data()
        self.runtime = NRERuntime(self)


def render_nre_recipe(payload):
    values = {key: value for key, value in payload.items() if key != 'recipe_binding'}
    values['execution_enabled'] = False
    values['controller_name'] = 'nre_v2'
    dumped = NREModularControllerConfig.model_validate(values).model_dump(mode='json')
    dumped['recipe_binding'] = RecipeBinding(
        recipe_id='nre_v2', recipe_sha256=digest(dumped),
        source_sha256=source_digest(source_root(), source_files()),
        configuration_sha256=configuration_digest(dumped),
    ).model_dump(mode='json')
    sealed = NREModularControllerConfig.model_validate(dumped)
    if sealed.execution_enabled or sealed.initial_positions:
        raise PolicyError('compiler must never activate execution or adopt inventory')
    return {'controllers': [sealed.model_dump(mode='json')], 'execution_authorized': False,
            'source_sha256': source_digest(source_root(), source_files()),
            'native_validation': 'CONFIGURATION_PASSED'}
