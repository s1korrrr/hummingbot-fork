"""Headless MQTT readiness precedes owner strategy construction; no external I/O."""
import asyncio
from contextlib import ExitStack
from decimal import Decimal
from test.test_hummingbot_quickstart import quickstart_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event import events
from hummingbot.core.pubsub import PubSub
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


@pytest.mark.asyncio
@pytest.mark.parametrize("commands_enabled", [True, False])
async def test_headless_run_delivers_owner_events_and_starts_configured_commands_once(commands_enabled):
    """Headless quickstart bypasses StartCommand but must still wire Telegram."""
    app = HummingbotApplication.__new__(HummingbotApplication)
    app.headless_mode = True
    app.ev_loop = asyncio.get_running_loop()
    app.client_config_map = ClientConfigAdapter(ClientConfigMap())
    config = app.client_config_map.telegram_notifications
    config.telegram_bot_token = "fixture-token"
    config.telegram_chat_id = "fixture-chat"
    config.telegram_enabled = True
    config.telegram_commands_enabled = commands_enabled
    market = PubSub()
    app.trading_core = SimpleNamespace(markets={"okx": market}, notifiers=[])
    app._mqtt = None
    app.run_headless = AsyncMock()
    app._init_telegram_notifier()
    notifier = app._telegram_notifier
    delivered = asyncio.Queue()
    polling_started = asyncio.Event()

    async def capture_message(message):
        delivered.put_nowait(message)

    async def command_loop():
        polling_started.set()
        await asyncio.Event().wait()

    with patch.object(notifier, "_send_message", side_effect=capture_message), patch.object(
        notifier, "_run_command_loop", side_effect=command_loop
    ):
        try:
            await app.run()
            send_task = notifier._send_message_task
            command_task = notifier._command_task
            assert send_task is not None
            assert (command_task is not None) == commands_enabled
            await app.run()
            assert notifier._send_message_task is send_task
            assert notifier._command_task is command_task
            assert len(market.get_listeners(events.MarketEvent.OrderFilled)) == 1
            assert len(market.get_listeners(events.MarketEvent.OrderFailure)) == 1
            market.trigger_event(events.MarketEvent.OrderFilled, events.OrderFilledEvent(
                timestamp=1712745600,
                order_id="fixture-order",
                trading_pair="ETH-USDC",
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("2000"),
                amount=Decimal("0.01"),
                trade_fee=AddedToCostTradeFee(),
            ))
            message = await asyncio.wait_for(delivered.get(), timeout=1)
            assert "BUY FILLED" in message
            assert "ETH-USDC" in message
            assert polling_started.is_set() == commands_enabled
        finally:
            tasks = [task for task in [notifier._send_message_task, notifier._command_task] if task is not None]
            notifier.stop()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert market.get_listeners(events.MarketEvent.OrderFilled) == []


@pytest.mark.asyncio
async def test_headless_run_keeps_disabled_telegram_without_event_listeners():
    app = HummingbotApplication.__new__(HummingbotApplication)
    app.headless_mode = True
    app.client_config_map = ClientConfigAdapter(ClientConfigMap())
    market = PubSub()
    app.trading_core = SimpleNamespace(markets={"okx": market}, notifiers=[])
    app._mqtt = None
    app._telegram_notifier = None
    app.run_headless = AsyncMock()
    app._init_telegram_notifier()
    await app.run()
    assert app._telegram_notifier is None
    assert market.get_listeners(events.MarketEvent.OrderFilled) == []
    assert market.get_listeners(events.MarketEvent.OrderFailure) == []
