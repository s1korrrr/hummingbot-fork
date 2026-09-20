"""Paper-only spot breakout reference composed from strategy-neutral components.

This is an engineering reference, not an admitted economic strategy. Native
PositionExecutor owns orders, partial fills and protective exits. Restart holds
entries until native inventory reconciliation is separately qualified.
"""
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, model_validator

from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot, Dataset
from hummingbot.strategy_v2.components.data_reader import DataPolicy
from hummingbot.strategy_v2.components.domain import DataUnavailable, PolicyError, identity
from hummingbot.strategy_v2.components.feature_cache import FeatureCache
from hummingbot.strategy_v2.components.portfolio import allocate
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction


def source_root():
    return Path(__file__).resolve().parents[1]


def source_files():
    return [Path(__file__).resolve()]


def breakout_features(frame, lookback, atr_period):
    """Prior-high breakout and simple rolling true range; last closed bar only."""
    last = frame.iloc[-1]
    prior_high = float(frame['high'].iloc[-lookback - 1:-1].max())
    previous = frame['close'].shift(1)
    import pandas as pd
    true_range = pd.concat([frame.high - frame.low, (frame.high - previous).abs(),
                            (frame.low - previous).abs()], axis=1).max(axis=1)
    return {'signal': int(float(last.close) > prior_high), 'prior_high': prior_high,
            'atr': float(true_range.iloc[-atr_period:].mean()), 'close': float(last.close),
            'decision_time': int(last.timestamp) + 60}


class ComponentBreakoutConfig(ControllerConfigBase):
    model_config = ConfigDict(extra='forbid', validate_assignment=True, arbitrary_types_allowed=True)
    controller_name: Literal['component_breakout'] = 'component_breakout'
    controller_type: Literal['generic'] = 'generic'
    connector_name: Literal['okx_paper_trade'] = 'okx_paper_trade'
    candles_connector: Literal['okx'] = 'okx'
    execution_mode: Literal['paper'] = 'paper'
    execution_enabled: bool = Field(default=False, strict=True)
    trading_pair: str = Field(default='BTC-USDC', pattern=r'^[A-Z0-9]+-USDC$')
    interval: Literal['1m'] = '1m'
    data: DataPolicy = Field(default_factory=DataPolicy)
    breakout_lookback: int = Field(default=20, ge=2, le=500, strict=True)
    atr_period: int = Field(default=14, ge=2, le=500, strict=True)
    total_amount_quote: Decimal = Field(default=Decimal('50'), gt=0, le=50)
    trade_quote: Decimal = Field(default=Decimal('10'), gt=0, le=50)
    risk_fraction: Decimal = Field(default=Decimal('.01'), gt=0, le=Decimal('.05'))
    atr_stop_multiple: Decimal = Field(default=Decimal('2'), gt=0, le=10)
    maximum_stop_fraction: Decimal = Field(default=Decimal('.10'), gt=0, le=Decimal('.25'))
    take_profit: Decimal = Field(default=Decimal('.04'), gt=0, le=Decimal('.5'))
    time_limit: int = Field(default=3600, ge=60, le=86400, strict=True)
    max_candle_age_seconds: int = Field(default=10, ge=0, le=60, strict=True)
    fee_buffer: Decimal = Field(default=Decimal('.003'), ge=0, le=Decimal('.05'))

    @model_validator(mode='after')
    def bounded_reference(self):
        if self.initial_positions or self.data.mode != 'native' or self.trade_quote > self.total_amount_quote:
            raise ValueError('Reference requires empty native-paper inventory and bounded trade capital')
        if self.trading_pair == 'USDC-USDC':
            raise ValueError('Distinct base and quote required')
        return self

    def get_controller_class(self):
        return ComponentBreakoutController

    @property
    def max_records(self):
        return max(60, self.breakout_lookback + 1, self.atr_period + 1)


class ComponentBreakoutController(ControllerBase):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        from hummingbot.strategy_v2.components.source_identity import component_source_files

        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        if type(config) is not ComponentBreakoutConfig:
            raise ValueError('Exact reference configuration required')
        normalized = ComponentBreakoutConfig.model_validate(config.model_dump(mode='json'))
        if normalized.model_dump(mode='json') != config.model_dump(mode='json'):
            raise ValueError('Reference configuration must be normalized')
        super().__init__(normalized, market_data_provider, actions_queue, **kwargs)
        self._config_identity = identity(config.model_dump(mode='json', exclude={'manual_kill_switch'}))
        self._cache = FeatureCache(8)
        self._pending = None
        self._pending_identity = None
        self._last_decision = None
        self.recovery_hold = ''

    def get_candles_config(self):
        return [CandlesConfig(connector=self.config.candles_connector, trading_pair=self.config.trading_pair,
                              interval='1m', max_records=self.config.max_records)]

    def _paper_ready(self):
        connector = self.market_data_provider.connectors.get(self.config.connector_name)
        return (is_native_paper_connector(connector) and connector.ready
                and identity(self.config.model_dump(mode='json', exclude={'manual_kill_switch'})) == self._config_identity)

    async def update_processed_data(self):
        self.processed_data = {'signal': 0, 'data_health': {'ready': False}}
        try:
            now = self.market_data_provider.time()
            frame = self.market_data_provider.get_candles_df(self.config.candles_connector,
                                                             self.config.trading_pair, '1m', self.config.max_records)
            rows = tuple(Candle(open_time=int(row.timestamp) if float(row.timestamp).is_integer() else -1,
                                open=row.open, high=row.high, low=row.low, close=row.close, volume=row.volume,
                                confirmed=getattr(row, 'confirmed', True)) for row in frame.itertuples())
            snapshot = CandleSnapshot(dataset=Dataset(connector='okx', trading_pair=self.config.trading_pair,
                                                      history=self.config.max_records), epoch='injected-paper', sequence=1, published_at=now, rows=rows)
            values = self._cache.get(snapshot, spec={'lookback': self.config.breakout_lookback,
                                                     'atr_period': self.config.atr_period}, implementation='prior-high-simple-atr-v1', now=now,
                                     max_age=max(1, self.config.max_candle_age_seconds),
                                     compute=lambda candles: breakout_features(candles, self.config.breakout_lookback, self.config.atr_period))
            if now - values['decision_time'] > self.config.max_candle_age_seconds:
                raise DataUnavailable('closed candle is stale')
            self.processed_data = {**values, 'data_health': {'ready': True}, 'features': frame.copy(deep=True)}
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, DataUnavailable, PolicyError) as error:
            self.processed_data['data_health']['reason'] = str(error)

    def determine_executor_actions(self):
        c, data = self.config, self.processed_data
        if (not c.execution_enabled or c.manual_kill_switch or self.recovery_hold or not self._paper_ready()
                or not data.get('data_health', {}).get('ready') or data.get('signal') != 1):
            return []
        now = self.market_data_provider.time()
        decision = data['decision_time']
        if not 0 <= now - decision <= c.max_candle_age_seconds or (self._last_decision is not None and decision <= self._last_decision):
            return []
        # A queued create remains owned even before the orchestrator reports it.
        if self._pending is not None:
            prior = next((item for item in self.executors_info if item.id == self._pending.id), None)
            if prior is None or prior.is_active or not prior.is_done:
                return []
            self._pending = None
        if self.positions_held or any(item.is_active or not item.is_done for item in self.executors_info):
            return []
        price = Decimal(str(data['close']))
        stop = Decimal(str(data['atr'])) * c.atr_stop_multiple / price
        if not Decimal('0') < stop <= c.maximum_stop_fraction:
            return []
        try:
            rules = self.market_data_provider.get_trading_rules(c.connector_name, c.trading_pair)
            available = self.market_data_provider.get_available_balance(c.connector_name, 'USDC')
            connector = self.market_data_provider.connectors[c.connector_name]
            base, quote = c.trading_pair.split('-')
            fees = [connector.get_fee(base, quote, OrderType.LIMIT, TradeType.BUY, Decimal('1'),
                                      price, is_maker=maker).percent for maker in (True, False)]
            fee_reserve = max(c.fee_buffer, *fees)
            allocation = allocate(stage_quote=min(c.trade_quote, c.total_amount_quote * c.risk_fraction / stop),
                                  remaining_quote=c.total_amount_quote, available_quote=available, price=price,
                                  amount_step=rules.min_base_amount_increment, minimum_amount=rules.min_order_size,
                                  minimum_notional=rules.min_notional_size, fee_buffer=fee_reserve, cash_reserve=0)
        except (ValueError, KeyError, PolicyError):
            return []
        if allocation is None:
            return []
        executor = PositionExecutorConfig(timestamp=now, controller_id=c.id,
                                          connector_name=c.connector_name, trading_pair=c.trading_pair, side=TradeType.BUY,
                                          amount=allocation.amount, entry_price=price, leverage=1, entry_order_policy='single_attempt',
                                          triple_barrier_config=TripleBarrierConfig(stop_loss=stop, take_profit=c.take_profit,
                                                                                    time_limit=c.time_limit, open_order_type=OrderType.LIMIT, open_order_limit_max_age_seconds=60))
        self._pending, self._last_decision = executor, decision
        self._pending_identity = identity(executor.model_dump(mode='json'))
        return [CreateExecutorAction(controller_id=c.id, executor_config=executor)]

    def get_status_summary(self):
        data = self.processed_data
        return {'controller_name': self.config.controller_name, 'pair': self.config.trading_pair,
                'state': 'recovery_hold' if self.recovery_hold else
                         'ready' if data.get('data_health', {}).get('ready') else 'unavailable',
                'signal': data.get('signal', 0), 'decision_time': data.get('decision_time'),
                'execution_mode': self.config.execution_mode}

    def get_custom_info(self):
        return {'strategy_components': {
            'controller_name': self.config.controller_name, 'controller_id': self.config.id,
            'execution_mode': self.config.execution_mode, 'execution_enabled': self.config.execution_enabled,
            'data_ready': self.processed_data.get('data_health', {}).get('ready', False),
            'data_reason': self.processed_data.get('data_health', {}).get('reason'),
            'decision_time': self.processed_data.get('decision_time'), 'signal': self.processed_data.get('signal', 0),
            'recovery_hold': self.recovery_hold,
            'component_ids': ['candle_snapshot', 'feature_cache', 'quote_allocation', 'native_position_executor'],
            'components_version': 'v1',
            'parameters_version': 'prior-high-simple-atr-v1',
            'breakout_lookback': self.config.breakout_lookback, 'atr_period': self.config.atr_period}}

    def validate_executor_config(self, config):
        return (super().validate_executor_config(config) and self._paper_ready() and not self.recovery_hold
                and self.config.execution_enabled and self._pending is not None
                and self.processed_data.get('data_health', {}).get('ready') is True
                and self.processed_data.get('signal') == 1
                and self.processed_data.get('decision_time') == self._last_decision
                and type(config) is PositionExecutorConfig
                and identity(config.model_dump(mode='json')) == self._pending_identity
                and 0 <= self.market_data_provider.time() - self._last_decision <= self.config.max_candle_age_seconds)
