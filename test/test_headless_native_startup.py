"""Headless MQTT readiness precedes owner strategy construction; no external I/O."""
import asyncio
from contextlib import ExitStack
from test.test_hummingbot_quickstart import quickstart_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.remote_iface.mqtt import EEventQueueFactory


@pytest.mark.asyncio
async def test_headless_start_waits_before_actual_mqtt_dependent_factory():
    app = SimpleNamespace(_mqtt=None)

    async def prepare():
        app._mqtt = SimpleNamespace(add_external_event_listener=Mock())
    app.prepare_headless_startup = AsyncMock(side_effect=prepare)

    async def start_strategy(*args):
        # This is the actual factory that raises 'MQTTGateway is offline!'.
        EEventQueueFactory.create('fixture_event')
        return True
    config = Mock()
    config.paper_trade.paper_trade_exchanges = []
    args = SimpleNamespace(instance_id='ok-rsi-sui-sell-only', auto_set_permissions=None,
                           headless=True, v2_conf='ok_rsi_sui_sell_only.yml', config_file_name=None)
    with ExitStack() as stack:
        for name in ['run_application', 'wait_for_gateway_ready', 'read_system_configs_from_yml', 'create_yml_files_legacy']:
            stack.enter_context(patch.object(quickstart_module, name, new=AsyncMock()))
        stack.enter_context(patch.object(quickstart_module, 'init_logging'))
        stack.enter_context(patch.object(quickstart_module, 'AllConnectorSettings'))
        stack.enter_context(patch.object(quickstart_module, 'load_client_config_map_from_file', return_value=config))
        stack.enter_context(patch.object(quickstart_module.Security, 'login', return_value=True))
        stack.enter_context(patch.object(quickstart_module.Security, 'wait_til_decryption_done', new=AsyncMock()))
        stack.enter_context(patch.object(quickstart_module.HummingbotApplication, 'main_application', return_value=app))
        stack.enter_context(patch.object(quickstart_module, 'load_and_start_strategy', new=AsyncMock(side_effect=start_strategy)))
        stack.enter_context(patch('hummingbot.remote_iface.mqtt.MQTTGateway.main', side_effect=lambda: app._mqtt))
        await quickstart_module.quick_start(args, Mock())
    app.prepare_headless_startup.assert_awaited_once()


@pytest.mark.asyncio
async def test_headless_readiness_failure_is_bounded_and_cleans_mqtt():
    async def never_ready(**kwargs):
        await asyncio.Event().wait()
    app = SimpleNamespace(start_mqtt_async=AsyncMock(side_effect=never_ready), stop_mqtt_async=AsyncMock(), _mqtt=None)
    with pytest.raises(RuntimeError, match='MQTT'):
        await HummingbotApplication.prepare_headless_startup(app, timeout=0.01)
    app.stop_mqtt_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_headless_readiness_requires_healthy_owned_gateway():
    app = SimpleNamespace(start_mqtt_async=AsyncMock(), stop_mqtt_async=AsyncMock(), _mqtt=SimpleNamespace(health=False))
    with pytest.raises(RuntimeError, match='MQTT'):
        await HummingbotApplication.prepare_headless_startup(app)
    app.stop_mqtt_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_headless_readiness_accepts_only_gateway_owned_by_this_application():
    app = SimpleNamespace(start_mqtt_async=AsyncMock(), stop_mqtt_async=AsyncMock())
    gateway = SimpleNamespace(health=True, _hb_app=app)
    app._mqtt = gateway
    with patch('hummingbot.client.hummingbot_application.MQTTGateway.main', return_value=gateway):
        await HummingbotApplication.prepare_headless_startup(app)
    app.start_mqtt_async.assert_awaited_once_with(timeout=35.0)
    app.stop_mqtt_async.assert_not_awaited()
    gateway._hb_app = object()
    with patch('hummingbot.client.hummingbot_application.MQTTGateway.main', return_value=gateway):
        with pytest.raises(RuntimeError, match='MQTT'):
            await HummingbotApplication.prepare_headless_startup(app)
    app.stop_mqtt_async.assert_awaited_once()
