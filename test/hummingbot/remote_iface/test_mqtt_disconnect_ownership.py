"""Gateway recovery must not compete with commlib's synchronous retry callback."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from commlib.transports.mqtt import MQTTTransport

from hummingbot.remote_iface.mqtt import MQTTGateway


@pytest.mark.parametrize('reason', [0, 1, 5])
def test_native_gateway_owns_disconnect_without_inline_transport_retry(reason):
    gateway = object.__new__(MQTTGateway)
    gateway.stop = Mock()
    gateway._health = True
    gateway._transport_disconnect_errors = 0
    transport = object.__new__(MQTTTransport)
    transport._connected = True
    transport._reconnect_attempts = 0
    transport._conn_params = SimpleNamespace(reconnect_attempts=5)
    transport._reconnect = Mock()
    transport._client = Mock()
    transport._client.on_disconnect = transport.on_disconnect
    gateway._patch_endpoint_disconnect_handler(SimpleNamespace(_transport=transport))

    transport._client.on_disconnect(transport._client, None, reason)

    assert not transport._connected
    assert not gateway._health
    transport._reconnect.assert_not_called()
    transport._client.loop_stop.assert_not_called()
    assert gateway._transport_disconnect_errors == (0 if reason == 0 else 1)
