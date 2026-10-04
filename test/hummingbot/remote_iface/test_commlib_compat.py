import threading
import time
from unittest.mock import MagicMock, patch

from commlib.transports import mqtt as commlib_mqtt

from hummingbot.remote_iface import commlib_compat


class _Endpoint:
    def __init__(self, event=None):
        self._transport = MagicMock()
        self._rpc_name = 'rsibot/test'
        self._on_request_handle = object()
        self._t_stop_event = event


def test_engine_import_replaces_the_polling_rpc_loops():
    import hummingbot.remote_iface.mqtt  # noqa: F401  (installs on import)

    assert commlib_mqtt.RPCService.run_forever is commlib_compat._rpc_service_run_forever
    assert commlib_mqtt.RPCServer.run_forever is commlib_compat._rpc_server_run_forever
    assert commlib_compat.install() == []  # already replaced; idempotent


def test_only_the_exact_polling_loop_is_recognised():
    def polling(self):
        while True:
            time.sleep(0.001)

    def blocking(self):
        self._t_stop_event.wait()

    assert commlib_compat._polls(polling)
    assert not commlib_compat._polls(blocking)
    assert not commlib_compat._polls(None)


def test_rpc_service_subscribes_starts_blocks_and_stops_without_spinning():
    endpoint = _Endpoint(threading.Event())
    sleeps = []
    with patch.object(commlib_compat.time, 'sleep', side_effect=sleeps.append):
        thread = threading.Thread(target=commlib_compat._rpc_service_run_forever, args=(endpoint,))
        thread.start()
        threading.Event().wait(0.2)
        assert thread.is_alive()
        endpoint._t_stop_event.set()
        thread.join(1)
    assert not thread.is_alive()
    assert sleeps == []
    endpoint._transport.subscribe.assert_called_once_with(
        'rsibot/test', endpoint._on_request_handle, qos=commlib_mqtt.MQTTQoS.L1)
    endpoint._transport.start.assert_called_once_with()
    endpoint._transport.stop.assert_called_once_with()


def test_stop_event_assigned_after_thread_start_is_still_honoured():
    endpoint = _Endpoint(None)
    thread = threading.Thread(target=commlib_compat._rpc_server_run_forever, args=(endpoint,))
    thread.start()
    time.sleep(0.1)
    endpoint._t_stop_event = threading.Event()
    endpoint._t_stop_event.set()
    thread.join(2)
    assert not thread.is_alive()
    endpoint._transport.start.assert_called_once_with()
    endpoint._transport.stop.assert_called_once_with()
