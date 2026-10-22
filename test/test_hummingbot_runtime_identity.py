import unittest
from test.test_hummingbot_quickstart import CmdlineParser, quick_start, quickstart_module
from unittest.mock import Mock, patch

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.remote_iface.mqtt import MQTTCommands, MQTTGateway


class RuntimeIdentityTest(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_override_fails_before_loading_config(self):
        for value in ('', 'bad/name', 'bad+', 'bad#', 'white space', '\n', 'a' * 129):
            with self.subTest(value=value), patch.object(quickstart_module, 'load_client_config_map_from_file') as loader:
                with self.assertRaises(ValueError):
                    await quick_start(Mock(instance_id=value), Mock())
                loader.assert_not_called()

    async def test_runtime_identity_is_available_at_mqtt_start_without_mutating_config(self):
        config = ClientConfigAdapter(ClientConfigMap(instance_id='persisted-owner', tables_format='psql'))
        config.mqtt_bridge.mqtt_autostart = True
        before = config.hb_config.model_dump()
        observed = []
        apps = []

        async def connect(app, **kwargs):
            observed.append(app.instance_id)
            app._mqtt = Mock(health=True, _hb_app=app)

        with patch('hummingbot.client.hummingbot_application.load_ssl_config_map_from_file'), \
                patch('hummingbot.client.hummingbot_application.TradingCore'), \
                patch.object(HummingbotApplication, '_init_telegram_notifier'), \
                patch.object(HummingbotApplication, 'start_mqtt_async', connect):
            for identity in ('ok-rsi-main', 'ok-rsi-sui-sell-only', None):
                with patch.object(HummingbotApplication, '_main_app', None):
                    app = HummingbotApplication.main_application(config, headless_mode=True, instance_id=identity)
                    apps.append(app)
                    with patch.object(MQTTGateway, 'main', side_effect=lambda: app._mqtt):
                        await app.prepare_headless_startup()
        self.assertEqual(['ok-rsi-main', 'ok-rsi-sui-sell-only', 'persisted-owner'], observed)
        self.assertEqual(before, config.hb_config.model_dump())
        rpc_names = []
        for app in apps[:2]:
            node = Mock(namespace='hbot')
            MQTTCommands(app, node)
            rpc_names.append({call.kwargs['rpc_name'] for call in node.create_rpc.call_args_list})
            with patch.object(MQTTGateway, '_instance', None), \
                    patch('hummingbot.remote_iface.mqtt.Node.__init__', return_value=None) as node_init:
                gateway = MQTTGateway(app)
                gateway.stop = Mock()  # Node transport was not initialized in this isolated topic test.
                self.assertEqual(f'hbot/{app.instance_id}/hb', node_init.call_args.kwargs['heartbeat_uri'])
        self.assertIn('hbot/ok-rsi-main/stop', rpc_names[0])
        self.assertIn('hbot/ok-rsi-sui-sell-only/stop', rpc_names[1])
        self.assertFalse(rpc_names[0] & rpc_names[1])

    async def test_existing_application_cannot_silently_ignore_an_explicit_identity(self):
        existing = Mock(instance_id='ok-rsi-main')
        with patch.object(HummingbotApplication, '_main_app', existing):
            self.assertIs(existing, HummingbotApplication.main_application())
            self.assertIs(existing, HummingbotApplication.main_application(instance_id='ok-rsi-main'))
            for value in ('other-bot', 'bad/topic'):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    HummingbotApplication.main_application(instance_id=value)

    async def test_cli_accepts_explicit_ids_and_rejects_invalid_id(self):
        for value in ('ok-rsi-main', 'ok-rsi-sui-sell-only'):
            self.assertEqual(value, CmdlineParser().parse_args(['--instance-id', value]).instance_id)
        with self.assertRaises(SystemExit):
            CmdlineParser().parse_args(['--instance-id', 'bad/topic'])
