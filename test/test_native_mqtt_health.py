import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from hummingbot.remote_iface.mqtt import MQTTGateway


async def health_samples(samples):
    gateway = object.__new__(MQTTGateway)
    gateway.stop = Mock()  # No native node/network was constructed.
    gateway._stop_event_async = asyncio.Event()
    gateway._initial_connection_succeeded = True
    gateway._restarting = False
    gateway._health = True
    gateway._unhealthy_since = None
    gateway._hb_app = SimpleNamespace(logger=Mock())
    publisher = SimpleNamespace(_transport=SimpleNamespace(is_connected=samples[0]))
    gateway._publishers = [publisher]
    gateway._rpc_services = []
    gateway._subscribers = []
    clock = {'second': 0}
    observed_health = []

    checks = {'count': 0}

    async def run_in_executor(_, fn):
        index = checks['count']
        checks['count'] += 1
        clock['second'] = index
        publisher._transport.is_connected = samples[index]
        if checks['count'] == len(samples):
            gateway._stop_event_async.set()
        return fn()

    async def sleep(_):
        observed_health.append(gateway.health)

    gateway._ev_loop = SimpleNamespace(run_in_executor=run_in_executor, time=lambda: clock['second'])
    gateway._restart_gateway = AsyncMock()
    with patch('hummingbot.remote_iface.mqtt.asyncio.sleep', side_effect=sleep):
        await gateway._monitor_health_loop()
    return gateway, observed_health


@pytest.mark.asyncio
async def test_new_publisher_handshake_reports_unhealthy_without_restarting_gateway():
    gateway, observed = await health_samples([False, True, True])
    gateway._restart_gateway.assert_not_awaited()
    assert observed == [False, True, True]


@pytest.mark.asyncio
async def test_sustained_endpoint_failure_still_restarts_gateway():
    gateway, observed = await health_samples([False] * 7)
    gateway._restart_gateway.assert_awaited_once()
    assert not any(observed)


@pytest.mark.asyncio
async def test_healthy_sample_resets_disconnection_grace():
    gateway, _ = await health_samples([False] * 4 + [True] + [False] * 4 + [True])
    gateway._restart_gateway.assert_not_awaited()


def test_reconnect_joins_transport_when_connection_disappears_between_health_checks():
    from commlib.endpoints import BaseEndpoint, EndpointState

    class FlappingTransport:
        def __init__(self):
            self.reads = 0
            self.stop = Mock()

        @property
        def is_connected(self):
            self.reads += 1
            return self.reads == 1

    endpoint = BaseEndpoint()
    endpoint._state = EndpointState.CONNECTED
    endpoint._transport = FlappingTransport()
    gateway = object.__new__(MQTTGateway)
    gateway.stop = Mock()
    gateway._publishers = [endpoint]
    gateway._subscribers = []
    gateway._rpc_services = []
    gateway._rpc_clients = []
    gateway._action_services = []
    gateway._action_clients = []
    gateway._hb_thread = None
    gateway._executor = None
    gateway._stop_node_transports()
    endpoint._transport.stop.assert_called_once_with()
    assert endpoint._state == EndpointState.DISCONNECTED
