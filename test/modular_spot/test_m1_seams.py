"""Integration seams required by M1: native config, report age, adapters, shared candles."""
import asyncio
import time
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch

import pytest

from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.candle_data import FIRST_DELIVERY, SCHEMA_VERSION
from controllers.generic.modular_spot_core.domain import DataUnavailable
from controllers.generic.modular_spot_core.market import MarketFeatures
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from controllers.generic.modular_spot_core.planner import EntryProposal
from controllers.generic.modular_spot_core.profiles import ProgramProfile, build_profile
from controllers.generic.modular_spot_core.state import DecisionState
from hummingbot.client.settings import AllConnectorSettings, ConnectorType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.models.base import RunnableStatus


def config(**overrides):
    return ModularSpotControllerConfig(id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC', **overrides)


def test_build_profile_output_validates_as_native_controller_config():
    result = build_profile(ProgramProfile(
        name='selected', connector='okx', symbols=('BTC-USDC', 'ETH-USDC'),
        mode='fixed', quote_budget='200', max_positions=2))
    assert result['schema_version'] == SCHEMA_VERSION
    assert result['candle_manifest']['schema_version'] == SCHEMA_VERSION
    for payload in result['controllers']:
        value = ModularSpotControllerConfig.model_validate(payload)
        assert value.connector_name == 'okx'
        assert value.interval == FIRST_DELIVERY['interval']
        assert value.execution_enabled is False


def test_unknown_controller_keys_are_rejected():
    with pytest.raises(ValueError):
        ModularSpotControllerConfig(
            id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC', extra_flag=True)


def test_shared_first_delivery_rejects_non_1m_interval():
    with pytest.raises(ValueError):
        ModularSpotControllerConfig(
            id='modular_spot_test', connector_name='okx', trading_pair='BTC-USDC', interval='5m',
            data={'mode': 'shared', 'origin': 'http://127.0.0.1:8100',
                  'token_file': '/run/secrets/modular_candle_reader'})


def test_shared_startup_does_not_initialize_native_duplicate_candles():
    provider = NS(ready=False, initialize_candles_feed=MagicMock())
    value = config(data={'mode': 'shared', 'origin': 'http://127.0.0.1:8100',
                         'token_file': '/run/secrets/modular_candle_reader'})
    with patch('controllers.generic.modular_spot_core.runtime.attach_pool', return_value=MagicMock()):
        controller = ModularSpotController(value, provider, asyncio.Queue())
    assert controller.get_candles_config() == []
    controller.initialize_candles()
    provider.initialize_candles_feed.assert_not_called()


def test_native_mode_still_requests_the_pair_candle_feed():
    value = config()
    controller = ModularSpotController(value, NS(ready=False), asyncio.Queue())
    assert controller.get_candles_config() == [
        CandlesConfig(connector='okx', trading_pair='BTC-USDC', interval='1m', max_records=300)]


def test_native_bindings_report_age_fresh_and_stale(monkeypatch):
    setting = NS(type=ConnectorType.Exchange)
    monkeypatch.setattr(AllConnectorSettings, 'get_connector_settings', classmethod(lambda cls: {'okx': setting}))
    rules = NS(min_price_increment=D('0.01'), min_base_amount_increment=D('0.001'),
               min_order_size=D('0.001'), min_notional_size=D('1'))
    connector = NS(ready=True, supports_terminal_fill_reconciliation=True, trading_rules={'BTC-USDC': rules},
                   get_available_balance=lambda quote: D('100'))
    provider = NS(ready=True, connectors={'okx': connector},
                  get_price_by_type=lambda *args, **kwargs: D('100'))
    controller = ModularSpotController(config(max_native_report_age_seconds=5), provider, asyncio.Queue())
    with pytest.raises(DataUnavailable, match='unavailable'):
        NativeBindings.snapshot(controller)
    controller.performance_report = NS(pnl_available=True, global_pnl_quote=D('0'))
    assert controller.native_report_age() < 5
    snap = NativeBindings.snapshot(controller)
    assert snap['bid'] == D('100') and snap['pnl'] == D('0')
    controller._native_report_received_at = time.monotonic() - 30
    with pytest.raises(DataUnavailable, match='stale'):
        NativeBindings.snapshot(controller)


def test_planner_budget_and_data_adapters_are_invoked_from_runtime():
    called = {}
    shared = config(
        execution_enabled=True,
        budget={'group': 'five', 'max_quote': '100', 'max_assets': 1},
        data={'mode': 'shared', 'origin': 'http://127.0.0.1:8100',
              'token_file': '/run/secrets/modular_candle_reader'},
    )
    pool = MagicMock()
    with patch('controllers.generic.modular_spot_core.runtime.attach_pool', return_value=pool) as attach:
        controller = ModularSpotController(shared, NS(ready=True, time=lambda: 1200), asyncio.Queue())
    assert attach.called and controller.runtime.pool is pool
    assert isinstance(controller.runtime.bindings, NativeBindings)

    market = MarketFeatures(1140, D('100'), D('29'), D('31'), False, False)
    proposal = EntryProposal('claim-1', D('100'), D('1'), 1200.0, 0, 0)
    revised = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1140,
                            claims=('claim-1',), committed_quote=D('100.5'))
    store = MagicMock()
    store.load.return_value = (DecisionState(fingerprint=controller.runtime.fingerprint, candle=1080), 'raw')
    store.compare_and_swap.return_value = True
    store.recorder.get_all_executors.return_value = []
    budget = MagicMock()
    budget.reserve.return_value = True
    native = NS(id='claim-1', amount=D('1'), entry_price=D('100'), timestamp=1200,
                model_copy=lambda deep=True: native)
    rt = controller.runtime
    rt.observe = lambda: market
    rt.acquisition_permitted = lambda: True
    rt.ensure_store = lambda: store
    rt.budget = budget
    rt.selection_store = NS(freeze=lambda **kwargs: None)
    rt.snapshot = lambda: dict(bid=D('100'), ask=D('100.1'), available_quote=D('100'), pnl=D('0'),
                               amount_step=D('0.001'), minimum_amount=D('0.001'), minimum_notional=D('1'))
    rt.bindings.config = lambda cfg, item: native
    rt.bindings.action = lambda cid, item: called.setdefault('action', item)

    with patch('controllers.generic.modular_spot_core.runtime.plan',
               return_value=(revised, proposal, 'entry_proposed')) as planned:
        controller._status = RunnableStatus.RUNNING
        actions = controller.determine_executor_actions()
    planned.assert_called_once()
    budget.reserve.assert_called_once()
    assert actions == [native]
