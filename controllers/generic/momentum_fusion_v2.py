"""Isolated v2 momentum controller. Does not inherit rsi_modular, ok_rsi or rsi_v5."""
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, model_validator

from controllers.generic.momentum_fusion_v2_core import features, recipe, runtime, setups
from hummingbot.core.data_type.common import PriceType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot, Dataset
from hummingbot.strategy_v2.components.data_reader import DataPolicy
from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionCursor,
    DecisionScope,
    ObservationRef,
    Opportunity,
    Prerequisite,
    decide,
)
from hummingbot.strategy_v2.components.domain import DataUnavailable, PolicyError, identity
from hummingbot.strategy_v2.components.exit_policy import ExitProposal, NativeExitView
from hummingbot.strategy_v2.components.feature_cache import FeatureCache
from hummingbot.strategy_v2.components.recipe_contract import configuration_digest, source_digest
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig


def source_root():
    return Path(__file__).resolve().parents[1]


def source_files():
    core = Path(__file__).resolve().parent / 'momentum_fusion_v2_core'
    return [Path(__file__).resolve(), *[path for path in sorted(core.glob('*.py'))]]


class MomentumFusionV2Config(ControllerConfigBase):
    model_config = ConfigDict(extra='forbid', validate_assignment=True, arbitrary_types_allowed=True)
    controller_name: Literal['momentum_fusion_v2'] = 'momentum_fusion_v2'
    controller_type: Literal['generic'] = 'generic'
    connector_name: str = 'okx'
    candles_connector: Literal['okx'] = 'okx'
    execution_mode: Literal['observation', 'shadow', 'paper', 'live'] = 'observation'
    execution_enabled: bool = Field(default=False, strict=True)
    trading_pair: str = Field(default='BNB-USDC', pattern=r'^[A-Z0-9]+-USDC$')
    interval: Literal['1m'] = '1m'
    data: DataPolicy = Field(default_factory=DataPolicy)
    enabled_modules: tuple[str, ...] = Field(default=('b0_breakout', 'c1_shelf'))
    total_amount_quote: Decimal = Field(default=Decimal('50'), gt=0, le=5000)
    trade_quote: Decimal = Field(default=Decimal('10'), gt=0, le=5000)
    risk_fraction: Decimal = Field(default=Decimal('.01'), gt=0, le=Decimal('.05'))
    atr_stop_multiple: Decimal = Field(default=Decimal('2'), gt=0, le=10)
    maximum_stop_fraction: Decimal = Field(default=Decimal('.10'), gt=0, le=Decimal('.25'))
    take_profit: Decimal = Field(default=Decimal('.04'), gt=0, le=Decimal('.5'))
    time_limit: int = Field(default=3600, ge=60, le=86400, strict=True)
    max_candle_age_seconds: int = Field(default=10, ge=0, le=60, strict=True)
    fee_buffer: Decimal = Field(default=Decimal('.003'), ge=0, le=Decimal('.05'))
    topology: Literal['single_recorder', 'shared_multi_process'] = 'single_recorder'
    claim_store_path: str | None = None

    @model_validator(mode='after')
    def disabled_until_admitted(self):
        if self.execution_enabled and self.execution_mode == 'live':
            raise ValueError('live execution is not admitted by this package')
        if self.execution_enabled and not self.claim_store_path:
            raise ValueError('claim_store_path is required when execution is enabled')
        if self.trade_quote > self.total_amount_quote:
            raise ValueError('trade quote cannot exceed assigned capital')
        recipe.RecipeSpec(enabled=self.enabled_modules)
        return self

    def get_controller_class(self):
        return MomentumFusionV2Controller

    @property
    def max_records(self):
        return max(80, int(recipe.PARAMETERS['lookback']) + int(recipe.PARAMETERS['atr_period']) + 5)


class MomentumFusionV2Controller(ControllerBase):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        from hummingbot.strategy_v2.components.source_identity import component_source_files

        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        if type(config) is not MomentumFusionV2Config:
            raise ValueError('Exact momentum v2 configuration required')
        super().__init__(config, market_data_provider, actions_queue, **kwargs)
        self._config_identity = identity(config.model_dump(mode='json', exclude={'manual_kill_switch'}))
        self._cache = FeatureCache(8)
        self._source_sha256 = source_digest(source_root(), source_files())
        spec = recipe.RecipeSpec(enabled=config.enabled_modules)
        self._binding = recipe.bind(self._source_sha256, configuration_digest(config.model_dump(mode='json')), spec)
        self._states = {name: setups.idle(name) for name in recipe.PRECEDENCE}
        self._cursors: dict[tuple[str, str, str], DecisionCursor] = {}
        self._pending = None
        self._pending_identity = None
        self._last_decision = None
        self.recovery_hold = ''
        path = Path(config.claim_store_path) if config.claim_store_path else None
        self._claims = runtime.ClaimStore(path) if path else None

    def get_candles_config(self):
        return [CandlesConfig(connector=self.config.candles_connector, trading_pair=self.config.trading_pair,
                              interval='1m', max_records=self.config.max_records)]

    def _scope(self):
        return DecisionScope(program_id='okx_spot_momentum_v2', owner_id=self.config.id,
                             connector=self.config.connector_name, trading_pair=self.config.trading_pair,
                             recipe_sha256=self._binding.recipe_sha256,
                             acquisition_allowed=not self.config.manual_kill_switch)

    async def update_processed_data(self):
        self.processed_data = {'signal': 0, 'data_health': {'ready': False}, 'setup': None}
        try:
            runtime.refuse_unsupported_topology(self.config.topology)
            now = self.market_data_provider.time()
            frame = self.market_data_provider.get_candles_df(
                self.config.candles_connector, self.config.trading_pair, '1m', self.config.max_records)
            now_s = int(now)
            rows = []
            for row in frame.itertuples():
                if not features.is_explicitly_confirmed(row):
                    continue
                open_time = int(row.timestamp) if float(row.timestamp).is_integer() else -1
                if open_time < 0 or not features.is_closed_minute(open_time, now_s):
                    continue
                rows.append(Candle(open_time=open_time, open=row.open, high=row.high, low=row.low,
                                   close=row.close, volume=row.volume, confirmed=True))
            if not rows:
                raise DataUnavailable('no confirmed candles')
            snapshot = CandleSnapshot(
                dataset=Dataset(connector='okx', trading_pair=self.config.trading_pair, history=self.config.max_records),
                epoch='native', sequence=1, published_at=now, rows=rows)
            values = self._cache.get(
                snapshot, spec=recipe.PARAMETERS, implementation='momentum-fusion-v2-features',
                now=now, max_age=max(1, self.config.max_candle_age_seconds),
                compute=_feature_bundle)
            event_ms = int(values['decision_time']) * 1000
            now_ms = int(now * 1000)
            params = recipe.PARAMETERS
            self._states['b0_breakout'] = setups.step_b0(
                self._states['b0_breakout'], close=values['close'], prior_high=values['prior_high'],
                event_ms=event_ms, now_ms=now_ms, lifetime_ms=int(params['lifetime_ms']),
                cooldown_ms=int(params['cooldown_ms']))
            self._states['c1_shelf'] = setups.step_c1(
                self._states['c1_shelf'], close=values['close'], high=values['high'], low=values['low'],
                impulse=values['impulse'], bandwidth=values['bandwidth'], event_ms=event_ms, now_ms=now_ms,
                max_pullback=params['shelf_max_pullback'], max_bandwidth=params['shelf_max_bandwidth'],
                min_shelf_bars=int(params['shelf_min_bars']), lifetime_ms=int(params['lifetime_ms']),
                cooldown_ms=int(params['cooldown_ms']))
            self._states['c2_compression'] = setups.step_c2(
                self._states['c2_compression'], close=values['close'], prior_high=values['prior_high'],
                bandwidth=values['bandwidth'], expansion_ratio=values['range_ratio'], event_ms=event_ms,
                now_ms=now_ms, compression_max=params['compression_max'], expand_min=params['expansion_min'],
                lifetime_ms=int(params['lifetime_ms']), cooldown_ms=int(params['cooldown_ms']))
            self._states['c3_reclaim'] = setups.step_c3(
                self._states['c3_reclaim'], close=values['close'], high=values['high'], low=values['low'],
                uptrend=values['uptrend'], event_ms=event_ms, now_ms=now_ms,
                pullback_max=params['c3_pullback_max'], lifetime_ms=int(params['lifetime_ms']),
                cooldown_ms=int(params['cooldown_ms']))
            evidence = identity({'decision': values['decision_time'], 'close': str(values['close'])})
            vetoes = ()
            if not values.get('atr'):
                vetoes = (Prerequisite(name='atr', status='HOLD'),)
            checks = recipe.runtime_prerequisites(evidence, vetoes=vetoes)
            selected = recipe.select(tuple(self._states.values()), checks, frozenset(self.config.enabled_modules))
            self.processed_data = {
                **values, 'data_health': {'ready': True}, 'setup': None if selected is None else selected.setup,
                'phase': None if selected is None else selected.phase, 'signal': int(selected is not None),
                'features': frame.copy(deep=True),
            }
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, DataUnavailable, PolicyError) as error:
            self.processed_data['data_health']['reason'] = str(error)

    def determine_executor_actions(self):
        c, data = self.config, self.processed_data
        now = self.market_data_provider.time()
        held = bool(self.positions_held) or any(item.is_active or not item.is_done for item in self.executors_info)
        try:
            if held:
                view, proposals, extras = self._management_inputs(now)
                plan = runtime.plan_management(
                    claims=self._claims, acquisition_allowed=self._scope().acquisition_allowed,
                    view=view, proposals=proposals, **extras)
            else:
                pending_inflight = False
                if self._pending is not None:
                    prior = next((item for item in self.executors_info if item.id == self._pending.id), None)
                    pending_inflight = prior is None or prior.is_active or not prior.is_done
                    if not pending_inflight:
                        self._pending = None
                rules = self.market_data_provider.get_trading_rules(c.connector_name, c.trading_pair)
                available = self.market_data_provider.get_available_balance(c.connector_name, 'USDC')
                if data.get('signal') == 1 and data.get('data_health', {}).get('ready'):
                    ref = ObservationRef(
                        dataset_sha256=identity({'pair': c.trading_pair, 'interval': '1m'}),
                        epoch='native', revision=1,
                        content_sha256=identity({'decision': data['decision_time'], 'close': str(data['close'])}),
                        event_time_ms=int(data['decision_time'] * 1000),
                        available_time_ms=int(data['decision_time'] * 1000),
                    )
                    opportunity = Opportunity(
                        scope=self._scope(), setup=data.get('setup') or 'b0_breakout', action='acquire',
                        primary=ref, created_at_ms=int(now * 1000),
                        lifetime_ms=int(recipe.PARAMETERS['lifetime_ms']))
                    cursor_key = (opportunity.setup, opportunity.action, opportunity.primary.dataset_sha256)
                    cursor, trace = decide(
                        self._cursors.get(cursor_key), opportunity, now_ms=int(now * 1000),
                        prerequisites=recipe.runtime_prerequisites(ref.content_sha256))
                    self._cursors[cursor_key] = cursor
                    if trace.status != 'ELIGIBLE_FOR_NATIVE_ADMISSION' or trace.execution_authorized:
                        plan = runtime.ActionPlan((), trace.reason)
                    else:
                        plan = None
                else:
                    plan = None
                if plan is None:
                    plan = runtime.plan_acquisition(
                        claims=self._claims, execution_enabled=c.execution_enabled,
                        acquisition_allowed=self._scope().acquisition_allowed,
                        recovery_hold=self.recovery_hold,
                        data_ready=bool(data.get('data_health', {}).get('ready')),
                        signal=int(data.get('signal') or 0), last_decision=self._last_decision,
                        decision_time=data.get('decision_time') or 0, now=now,
                        max_candle_age_seconds=c.max_candle_age_seconds, pending_inflight=pending_inflight,
                        controller_id=c.id, connector_name=c.connector_name, trading_pair=c.trading_pair,
                        available_quote=available, total_amount_quote=c.total_amount_quote,
                        trade_quote=c.trade_quote, risk_fraction=c.risk_fraction,
                        price=data.get('close') or 0, atr=data.get('atr') or 0,
                        atr_stop_multiple=c.atr_stop_multiple, maximum_stop_fraction=c.maximum_stop_fraction,
                        take_profit=c.take_profit, time_limit=c.time_limit, fee_buffer=c.fee_buffer,
                        amount_step=rules.min_base_amount_increment, minimum_amount=rules.min_order_size,
                        minimum_notional=rules.min_notional_size)
        except (PolicyError, ValueError, TypeError, KeyError, AttributeError) as error:
            plan = runtime.ActionPlan((), str(error) or 'emit_failed')
        data['action_reason'] = plan.reason
        if plan.actions and not held:
            self._pending = plan.actions[0].executor_config
            self._last_decision = data.get('decision_time')
            self._pending_identity = identity(self._pending.model_dump(mode='json'))
        return list(plan.actions)

    def _management_inputs(self, now):
        c = self.config
        active = next((item for item in self.executors_info if item.is_active or not item.is_done), None)
        info = dict(getattr(active, 'custom_info', None) or {})
        remaining = info.get('remaining_position_amount_base') or info.get('remaining_owned_base')
        if remaining is None and self.positions_held:
            pos = self.positions_held[0]
            remaining = getattr(pos, 'amount', None) or getattr(pos, 'base_amount', None)
        if remaining is None:
            raise PolicyError('native_exit_view_unavailable')
        rules = self.market_data_provider.get_trading_rules(c.connector_name, c.trading_pair)
        bid = self.market_data_provider.get_price_by_type(
            c.connector_name, c.trading_pair, PriceType.BestBid)
        receipt = identity({'executor': getattr(active, 'id', c.id), 'pair': c.trading_pair})
        owner = c.id
        executor_id = str(getattr(active, 'id', None) or 'held-position')
        view = NativeExitView(
            owner_id=owner, executor_id=executor_id, connector=c.connector_name,
            trading_pair=c.trading_pair, receipt_sha256=receipt, observed_at_ms=int(now * 1000),
            reconciled=True, fees_complete=getattr(active, 'cum_fees_quote', None) is not None,
            remaining_base=str(remaining), reserved_base='0',
            base_fee_fraction=str(c.fee_buffer),
            amount_step=str(rules.min_base_amount_increment),
            price_step=str(getattr(rules, 'min_price_increment', '0.01')),
            minimum_amount=str(rules.min_order_size),
            minimum_notional=str(rules.min_notional_size), bid=str(bid))
        harvest = remaining
        proposals = (ExitProposal(
            policy_id='trail', owner_id=owner, executor_id=executor_id, receipt_sha256=receipt,
            requested_base=str(harvest), minimum_price='0', expires_at_ms=int(now * 1000) + 1000),)
        entry = info.get('entry_price') or info.get('current_position_average_price')
        if entry is None:
            raise PolicyError('unknown_cost')
        cost = Decimal(str(entry)) * Decimal(str(remaining))
        extras = dict(
            cost_quote=str(cost), harvest_base=str(harvest), fee_fraction=str(c.fee_buffer),
            margin='0', buffer='0', now_ms=int(now * 1000), controller_id=c.id,
            connector_name=c.connector_name, trading_pair=c.trading_pair, time_limit=c.time_limit)
        return view, proposals, extras

    def get_status_summary(self):
        data = self.processed_data
        return {'controller_name': self.config.controller_name, 'pair': self.config.trading_pair,
                'state': 'recovery_hold' if self.recovery_hold else
                         'ready' if data.get('data_health', {}).get('ready') else 'unavailable',
                'signal': data.get('signal', 0), 'setup': data.get('setup'),
                'execution_mode': self.config.execution_mode, 'execution_enabled': self.config.execution_enabled}

    def get_custom_info(self):
        return {'momentum_fusion_v2': {
            'controller_name': self.config.controller_name, 'controller_id': self.config.id,
            'execution_mode': self.config.execution_mode, 'execution_enabled': self.config.execution_enabled,
            'recipe_sha256': self._binding.recipe_sha256, 'source_sha256': self._binding.source_sha256,
            'enabled_modules': list(self.config.enabled_modules),
            'data_ready': self.processed_data.get('data_health', {}).get('ready', False),
            'data_reason': self.processed_data.get('data_health', {}).get('reason'),
            'setup': self.processed_data.get('setup'), 'signal': self.processed_data.get('signal', 0),
            'action_reason': self.processed_data.get('action_reason'),
            'recovery_hold': self.recovery_hold,
            'component_ids': ['candle_snapshot', 'feature_cache', 'quote_allocation', 'native_position_executor',
                              'exit_arbitration', 'decision_protocol'],
        }}

    def validate_executor_config(self, config):
        return (super().validate_executor_config(config) and not self.recovery_hold
                and self.config.execution_enabled and self._pending is not None
                and self.processed_data.get('data_health', {}).get('ready') is True
                and self.processed_data.get('signal') == 1
                and self.processed_data.get('decision_time') == self._last_decision
                and type(config) is PositionExecutorConfig
                and identity(config.model_dump(mode='json')) == self._pending_identity
                and 0 <= self.market_data_provider.time() - self._last_decision <= self.config.max_candle_age_seconds)


def _feature_bundle(frame):
    params = recipe.PARAMETERS
    empty = {}
    channel = features.prior_channel_break(frame, empty, {'lookback': params['lookback']})
    impulse = features.impulse_move(frame, empty, {
        'lookback': params['lookback'], 'min_return': params['min_return'],
        'min_range_ratio': params['min_range_ratio'], 'min_volume_ratio': params['min_volume_ratio']})
    atr = features.atr_value(frame, empty, {'period': params['atr_period']})
    width = features.bandwidth_ratio(frame, empty, {'lookback': params['lookback']})
    last = frame.iloc[-1]
    return {
        'close': channel['close'], 'prior_high': channel['prior_high'], 'break': channel['break'],
        'impulse': impulse['armed'], 'high': impulse['high'], 'low': impulse['low'],
        'range_ratio': impulse['range_ratio'], 'bandwidth': width['bandwidth'],
        'atr': atr['atr'], 'uptrend': int(frame['close'].iloc[-1] > frame['close'].iloc[-int(params['htf_trend_bars'])]),
        'decision_time': int(last.timestamp) + 60,
    }
