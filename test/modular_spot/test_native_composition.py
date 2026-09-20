"""Differential execution/feature contracts against the real inherited native controllers."""
import asyncio
import json
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from test_native_lifecycle import recorder as _recorder_fixture

from controllers.directional_trading import ok_rsi, rsi_v5
from controllers.generic.modular_ok_rsi import ModularOKRSIController, ModularOKRSIControllerConfig
from controllers.generic.modular_rsi_v5 import ModularRSIV5Controller, ModularRSIV5ControllerConfig
from controllers.generic.modular_spot_core.candle_data import Candle, CandleSnapshot, Replica
from controllers.generic.modular_spot_core.data_reader import DataPolicy
from controllers.generic.modular_spot_core.native_composition import required_shared_history, source_files, source_root
from controllers.generic.modular_spot_core.native_recipe import NativePolicyRecipe
from controllers.generic.modular_spot_core.recipe_compiler import bind_native_bundle, parse_recipe, write_bundle
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus

recorder = _recorder_fixture

FAMILIES = [
    ('ok_rsi', ModularOKRSIControllerConfig, ModularOKRSIController, ok_rsi.OKRSIControllerConfig, ok_rsi.OKRSIController),
    ('rsi_v5', ModularRSIV5ControllerConfig, ModularRSIV5Controller, rsi_v5.RSIv5ControllerConfig, rsi_v5.RSIv5Controller),
]


def provider():
    value = MagicMock()
    value.ready = True
    value.time.return_value = 600000.0
    value.get_price_by_type.return_value = D('100')
    value.get_available_balance.side_effect = lambda connector, asset: D('2') if asset == 'BTC' else D('1000')
    value.get_balance.side_effect = value.get_available_balance.side_effect
    value.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=D('.01'), min_order_size=D('.0001'), min_base_amount_increment=D('.0001'),
        min_notional_size=D('1'), min_order_value=D('1'))
    value.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    return value


def controllers(family, wrapper_config, wrapper, native_config, native, **changes):
    if getattr(wrapper_config, '_public_module', None) == 'rsi_modular':
        changes['profile'] = family
    if changes.get('execution_enabled'):
        changes['funding'] = {'allocation_quote': '50', 'fee_reserve_fraction': '.003'}
    config = wrapper_config(id='modular_spot_parity', trading_pair='BTC-USDC', strict_hmm_mode=False,
                            **changes)
    wrapped = wrapper(config, provider(), asyncio.Queue())
    values = config.model_dump(mode='python', exclude={'profile', 'data', 'shared_history', 'recipe_binding', 'execution_enabled', 'execution_mode', 'funding'})
    values['controller_name'] = family
    values['controller_type'] = 'directional_trading'
    legacy = native(native_config.model_validate(values), provider(), asyncio.Queue())
    for controller in (wrapped, legacy):
        controller._status = RunnableStatus.RUNNING
        controller._latest_atr_pct = .01
        controller.processed_data = {'signal': 1, 'signal_state': {'condition_ok': True, 'size_multiplier': 1.0}}
    return wrapped, legacy


@pytest.mark.parametrize('family,wrapper_config,wrapper,native_config,native', FAMILIES)
def test_native_executor_factory_parameters_match_exactly(family, wrapper_config, wrapper, native_config, native):
    wrapped, legacy = controllers(family, wrapper_config, wrapper, native_config, native, execution_enabled=True)
    created = [owner.get_executor_config(TradeType.BUY, D('100'), D('.3')) for owner in (wrapped, legacy)]
    assert created[0].require_terminal_entry_reconciliation is True
    assert created[1].require_terminal_entry_reconciliation is False
    assert created[0].model_dump(exclude={'id', 'require_terminal_entry_reconciliation'}) == created[1].model_dump(exclude={'id', 'require_terminal_entry_reconciliation'})
    assert created[0].type == 'position_executor'
    assert created[0].leverage == 1


@pytest.mark.parametrize('family,wrapper_config,wrapper,native_config,native', FAMILIES)
def test_disabled_acquisition_preserves_native_stop_action_and_trailing(family, wrapper_config, wrapper, native_config, native):
    wrapped, legacy = controllers(family, wrapper_config, wrapper, native_config, native)
    # The actual inherited stop implementation handles an inert, fully filled native executor report.
    info = SimpleNamespace(id='filled-native', connector_name='okx', trading_pair='BTC-USDC',
                           side=TradeType.BUY, is_active=True, is_trading=True, is_done=False, timestamp=599000,
                           filled_amount_quote=D('30'), net_pnl_pct=D('.02'),
                           config=SimpleNamespace(type='position_executor', side=TradeType.BUY,
                                                  trading_pair='BTC-USDC', connector_name='okx'),
                           custom_info={'trailing_stop_trigger_pct': '.015', 'trailing_move_count': 3})
    for owner in (wrapped, legacy):
        owner.executors_info = [info]
        owner.processed_data['signal'] = 0
    before = dict(info.custom_info)
    assert wrapped.can_create_executor(1) is False
    assert wrapped.stop_actions_proposal() == legacy.stop_actions_proposal()
    assert info.custom_info == before


@pytest.mark.parametrize('family,wrapper_config,wrapper,native_config,native', FAMILIES)
def test_real_native_recipe_export_roundtrip_and_source_closure(
        family, wrapper_config, wrapper, native_config, native, tmp_path):
    recipe = NativePolicyRecipe(name='parity', family=family, controller_id='modular_spot_recipe',
                                parameters={'trading_pair': 'BTC-USDC', 'strict_hmm_mode': False})
    bundle = bind_native_bundle(recipe)
    assert bundle['receipt_kind'] == 'native_policy_composition_v2'
    assert bundle['source_scope'] == 'controllers+strategy_components'
    assert 'directional_trading/rsi_signals.py' in bundle['source_files']
    assert 'directional_trading/pandas_compat.py' in bundle['source_files']
    assert all(path.is_relative_to(source_root()) for path in source_files(family))
    output = tmp_path / family
    write_bundle(bundle, output)
    payload = json.loads((output / 'modular_spot_recipe.yml').read_text())
    from controllers.generic.rsi_modular import get_config_class
    config = get_config_class(family).model_validate(payload)
    assert config.controller_name == 'rsi_modular' and config.profile == family
    assert not config.execution_enabled and config.data.mode == 'native'
    assert parse_recipe(recipe.model_dump(mode='json')) == recipe
    payload['usd_per_entry'] = str(D(str(payload['usd_per_entry'])) + 1)
    with pytest.raises(ValueError, match='configuration changed'):
        get_config_class(family).model_validate(payload)


@pytest.mark.parametrize('family,wrapper_config,wrapper,native_config,native', FAMILIES)
def test_shared_history_admits_complete_native_windows(family, wrapper_config, wrapper, native_config, native):
    data = DataPolicy(mode='shared', transport='stream', origin='http://127.0.0.1:8100', token_file='/unused')
    config = wrapper_config(id='modular_spot_history', data=data)
    required = required_shared_history(config, family)
    assert required == (1500 if family == 'ok_rsi' else 7514)
    with pytest.raises(ValueError, match='required='):
        wrapper_config(id='modular_spot_history', data=data, shared_history=500)
    with pytest.raises(ValueError):
        wrapper_config(id='modular_spot_history', mystery_policy=True)
    with pytest.raises(ValueError, match='adopt'):
        wrapper_config(id='modular_spot_history', initial_positions=[{'connector_name': 'okx',
                                                                      'trading_pair': 'BTC-USDC',
                                                                      'amount': 1, 'side': 'BUY'}])


def base_frame(count=7514, end=599940):
    x = np.arange(count)
    close = 100 + np.sin(x / 20) + x / 10000
    return pd.DataFrame({'timestamp': np.arange(end - (count - 1) * 60, end + 1, 60),
                         'open': close - .02, 'high': close + .1, 'low': close - .1,
                         'close': close, 'volume': np.ones(count) * 10,
                         'quote_asset_volume': close * 10})


def independent_aggregate(frame, interval, count):
    if interval == '1m':
        return frame.tail(count).copy()
    minutes = int(interval[:-1])
    data = frame.copy()
    data.index = pd.to_datetime(data.timestamp, unit='s', utc=True)
    result = data.resample(f'{minutes}min').agg({'timestamp': 'count', 'open': 'first', 'high': 'max',
                                                 'low': 'min', 'close': 'last', 'volume': 'sum',
                                                 'quote_asset_volume': 'sum'})
    result = result[result.timestamp == minutes]
    result['timestamp'] = [int(value.timestamp()) for value in result.index]
    return result.reset_index(drop=True)[frame.columns].tail(count).copy()


def seed_shared(controller, frame):
    reader = controller.shared_candles
    rows = tuple(Candle(open_time=int(item.timestamp), open=str(item.open), high=str(item.high), low=str(item.low),
                        close=str(item.close), volume=str(item.volume), quote_volume=str(item.quote_asset_volume))
                 for item in frame.tail(reader.spec.history).itertuples())
    replica = Replica((reader.spec,))
    replica.begin_session()
    replica.accept(CandleSnapshot(dataset=reader.spec, epoch='parity', sequence=1,
                                  published_at=600000, rows=rows), now=600000)
    reader.pool.client = SimpleNamespace(replica=replica)


@pytest.mark.parametrize('family,wrapper_config,wrapper,native_config,native', FAMILIES)
def test_actual_native_signal_pipeline_matches_shared_candle_composition(
        family, wrapper_config, wrapper, native_config, native):
    async def scenario():
        data = DataPolicy(mode='shared', transport='stream', origin='http://127.0.0.1:8100', token_file='/unused')
        wrapped, legacy = controllers(family, wrapper_config, wrapper, native_config, native,
                                      data=data, execution_enabled=True)
        frame = base_frame()
        seed_shared(wrapped, frame)
        legacy.market_data_provider.get_candles_df.side_effect = (
            lambda connector_name, trading_pair, interval, max_records:
            independent_aggregate(frame, interval, max_records))
        for interval in ('1m', '5m', '15m'):
            # Compare actual provider aggregation to an independent pandas resample.
            count = 500 if family == 'rsi_v5' else min(90, len(frame) // 15)
            if family == 'ok_rsi' and interval != '1m':
                count = 90
            actual = wrapped.shared_candles.get_candles_df('okx', 'BTC-USDC', interval, count)
            expected = independent_aggregate(frame.tail(wrapped.shared_candles.spec.history), interval, count)
            pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
        await legacy.update_processed_data()
        await wrapped.update_processed_data()
        assert wrapped.processed_data['signal'] == legacy.processed_data['signal']
        assert wrapped.processed_data['data_health'] == legacy.processed_data['data_health']
        # Actual native indicators and confirmation context must agree, not only a wrapper call count.
        pd.testing.assert_frame_equal(wrapped.processed_data['features'].reset_index(drop=True),
                                      legacy.processed_data['features'].reset_index(drop=True), check_dtype=False)
        assert wrapped.processed_data.get('signal_state') == legacy.processed_data.get('signal_state')
        assert not wrapped.shared_candles.entry_ready()  # Initial historical seed is never an entry.
    asyncio.run(scenario())


@pytest.mark.parametrize('family,wrapper_config,wrapper,native_config,native', FAMILIES)
def test_actual_native_create_action_path_matches(family, wrapper_config, wrapper, native_config, native, recorder):
    from copy import deepcopy

    class Store:
        value = None

        def load(self):
            return deepcopy(self.value)

        def save(self, previous, current):
            assert previous == self.value
            self.value = deepcopy(current)

    wrapped, legacy = controllers(family, wrapper_config, wrapper, native_config, native, execution_enabled=True)
    results = []
    for owner in (wrapped, legacy):
        if family == 'ok_rsi':
            owner._entry_decision_store = Store()
            owner._last_evaluated_candle_timestamp = 599880
            owner.processed_data = dict(signal=0, candle_timestamp=599880, data_health={'ready': True},
                                        signal_state={'condition_ok': True, 'raw_buy_candidate': False})
            assert owner.determine_executor_actions() == []
        owner._last_evaluated_candle_timestamp = 599940
        owner.processed_data = dict(signal=1, candle_timestamp=599940, data_health={'ready': True},
                                    signal_state={'condition_ok': True, 'raw_buy_candidate': True,
                                                  'size_multiplier': 1.0, 'signal_score': 1})
        actions = owner.determine_executor_actions()
        assert len(actions) == 1
        assert actions[0].executor_config.side == TradeType.BUY
        assert actions[0].executor_config.require_terminal_entry_reconciliation is (owner is wrapped)
        results.append(actions[0].model_dump(exclude={'executor_config': {'id', 'require_terminal_entry_reconciliation'}}))
    assert results[0] == results[1]


@pytest.mark.parametrize('family,wrapper_config,wrapper,native_config,native', FAMILIES)
def test_native_partial_cancel_late_fill_retains_identical_exposure_and_trail(
        family, wrapper_config, wrapper, native_config, native):
    from unittest.mock import patch

    from test_native_lifecycle import apply_fill, cancel, filled_event, tracked

    from hummingbot.connector.trading_rule import TradingRule
    from hummingbot.core.data_type.in_flight_order import OrderState
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor

    wrapped, legacy = controllers(family, wrapper_config, wrapper, native_config, native, execution_enabled=True)
    results = []
    for owner in (wrapped, legacy):
        cfg = owner.get_executor_config(TradeType.BUY, D('100'), D('.3'))
        executor_type = owner.get_executor_class_for_config(cfg) or PositionExecutor
        assert executor_type is PositionExecutor  # Preserve the actual native default selection.
        strategy = MagicMock(spec=StrategyV2Base)
        strategy.current_timestamp = 600000
        connector = MagicMock()
        connector.quantize_order_amount.side_effect = lambda pair, amount: amount
        connector.in_flight_orders = {}
        strategy.connectors = {'okx': connector}
        rule = TradingRule('BTC-USDC', min_order_size=D('.001'), min_price_increment=D('.01'),
                           min_base_amount_increment=D('.001'), min_notional_size=D('1'))
        with patch.object(PositionExecutor, 'get_trading_rules', return_value=rule):
            executor = executor_type(strategy, cfg)
        executor.get_in_flight_order = MagicMock(return_value=None)
        executor.get_price = MagicMock(return_value=D('102'))
        executor.place_order = MagicMock(return_value='inert-replacement')
        executor._status = RunnableStatus.RUNNING
        entry = tracked('entry', TradeType.BUY, '.1', OrderState.PARTIALLY_FILLED, amount=cfg.amount)
        executor._open_order = entry
        executor._trailing_stop_trigger_pct = D('.015')
        executor._trailing_stop_move_count = 3
        cancel(executor, entry)
        assert executor.open_filled_amount == D('.1')
        apply_fill(entry.order, trade_id='late', fill=cfg.amount - D('.1'))
        entry.order.current_state = OrderState.FILLED
        executor.process_order_filled_event(0, None, filled_event(entry, trade_id='late', fill=cfg.amount - D('.1')))
        assert executor.open_filled_amount == cfg.amount
        assert executor._trailing_stop_trigger_pct == D('.015') and executor._trailing_stop_move_count == 3
        executor.place_order.assert_not_called()
        results.append((executor.open_filled_amount, executor.open_filled_amount_quote,
                        executor.amount_to_close, executor._trailing_stop_trigger_pct))
    assert results[0] == results[1]


def test_rsi_v5_held_recovery_high_water_and_sell_action_match_while_entries_disabled(recorder):
    wrapped, legacy = controllers(*FAMILIES[1], use_bag_freeze=False, early_stop_drawdown_pct=D('0'))
    assert wrapped.determine_executor_actions() == []  # Establish the new owner while native inventory is flat.
    persist_position(recorder, wrapped.config.id)
    for owner in (wrapped, legacy):
        owner.processed_data = {
            'signal': 0, 'trend_confirmation': {'direction': 'up', 'confidence': .74, 'alignment_score': .74, 'states': {}},
            'signal_state': {'rsi': 82., 'rsi_prev': 82.4, 'ema_fast': 100.6},
        }
        owner.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='BTC-USDC', side=TradeType.BUY,
                                                amount=D('1'), breakeven_price=D('100'))]
    for price, rsi, previous, ema in [('101', 82., 82.4, 100.6), ('102', 84., 82.4, 100.6),
                                      ('101.6', 81.5, 83., 101.9)]:
        results = []
        for owner in (wrapped, legacy):
            owner.market_data_provider.get_price_by_type.return_value = D(price)
            owner.processed_data['signal_state'].update(rsi=rsi, rsi_prev=previous, ema_fast=ema)
            results.append(owner.determine_executor_actions())
        assert [a.model_dump(exclude={'executor_config': {'id'}}) for a in results[0]] == [
            a.model_dump(exclude={'executor_config': {'id'}}) for a in results[1]]
        assert {k: v.as_dict() for k, v in wrapped._recovery_trails.items()} == {
            k: v.as_dict() for k, v in legacy._recovery_trails.items()}
    assert len(results[0]) == 1 and results[0][0].executor_config.side == TradeType.SELL
    assert next(iter(wrapped._recovery_trails.values())).peak_price == D('101.6')
    assert wrapped._recovery_store.previous.pending_reductions[0].prior_peak == D('102')
    assert wrapped.can_create_executor(1) is False


def persist_position(recorder, owner, *, amount=D('1')):
    from hummingbot.model.position import Position
    with recorder.sql_manager.get_new_session() as session:
        with session.begin():
            session.merge(Position(id='native-held', controller_id=owner, connector_name='okx', side='BUY',
                                   trading_pair='BTC-USDC', timestamp=599000, volume_traded_quote=D('100'),
                                   amount=amount, breakeven_price=D('100'), unrealized_pnl_quote=D('0'),
                                   realized_pnl_quote=D('0'), cum_fees_quote=D('0')))
