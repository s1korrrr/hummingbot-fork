"""Blocking stop wait for commlib MQTT RPC endpoint threads.

commlib 0.11.5 keeps each RPC endpoint thread alive with ``time.sleep(0.001)``
polling of its stop event. With one thread per registered command that is
thousands of wake-ups per second, each taking the GIL from the strategy event
loop. The replacement blocks on the same event, so stop semantics are unchanged
apart from no longer spinning. It installs only over the exact polling loop it
replaces; a commlib release with a different loop is left untouched.
"""
import time

from commlib.transports import mqtt as _commlib_mqtt

_POLL_INTERVAL = 0.001
_STOP_WAIT_SECONDS = 0.5
_UNSET_EVENT_SLEEP_SECONDS = 0.05


def _wait_for_stop(endpoint) -> None:
    while True:
        # commlib starts the thread before assigning the event, so re-read it.
        stop = endpoint._t_stop_event
        if stop is None:
            time.sleep(_UNSET_EVENT_SLEEP_SECONDS)
        elif stop.wait(_STOP_WAIT_SECONDS):
            return


def _rpc_service_run_forever(self):
    self._transport.subscribe(self._rpc_name, self._on_request_handle, qos=_commlib_mqtt.MQTTQoS.L1)
    self._transport.start()
    _wait_for_stop(self)
    self._transport.stop()


def _rpc_server_run_forever(self):
    self._transport.start()
    _wait_for_stop(self)
    self._transport.stop()


def _polls(function) -> bool:
    code = getattr(function, '__code__', None)
    return code is not None and _POLL_INTERVAL in code.co_consts and 'sleep' in code.co_names


def install() -> list:
    """Replace the polling loops that are present; returns the patched class names."""
    patched = []
    for name, replacement in (('RPCService', _rpc_service_run_forever), ('RPCServer', _rpc_server_run_forever)):
        cls = getattr(_commlib_mqtt, name, None)
        if cls is not None and _polls(cls.__dict__.get('run_forever')):
            cls.run_forever = replacement
            patched.append(name)
    return patched
