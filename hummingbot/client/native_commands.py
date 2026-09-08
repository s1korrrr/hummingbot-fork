"""Opt-in native commands validate process ownership at consumption, before I/O."""
import asyncio
import uuid
from collections import OrderedDict
from contextvars import ContextVar
from functools import wraps

from hummingbot.client.native_lifecycle import ShutdownResult
from hummingbot.client.native_sources import NativeSourceGuard, capture_sources, native_sources

native_command_context = ContextVar('native_command_context', default=None)


def native_command_boundary(action):
    """Route native terminal/Telegram start/stop through the same guarded owner path."""
    def decorate(function):
        @wraps(function)
        async def wrapped(app, *args, **kwargs):
            commands = getattr(app, 'native_commands', None)
            if not isinstance(commands, NativeCommands) or native_command_context.get() == (commands, asyncio.current_task()):
                return await function(app, *args, **kwargs)
            async with commands.lock:
                packet = app.native_lifecycle.snapshot(app.trading_core)
                config = kwargs.get('v2_conf', args[1] if len(args) > 1 else None)
                if packet is None:
                    if action != 'start' or not config or not app.trading_core.strategy_name:
                        app.notify('Native loaded identity is unavailable; start with an explicit V2 configuration')
                        return
                    sources = capture_sources(config, app.trading_core.strategy_name.removesuffix('.py') + '.py', None)
                    source_guard = NativeSourceGuard(sources)
                    source_token = native_sources.set(source_guard)
                    command_token = native_command_context.set((commands, asyncio.current_task()))
                    try:
                        result = await function(app, *args, **kwargs)
                        app.native_lifecycle.capture(app.trading_core)
                        if app.trading_core._strategy_running is True:
                            app.native_lifecycle.loaded_sources = dict(sources)
                        return result
                    finally:
                        source_guard.active = False
                        native_sources.reset(source_token)
                        native_command_context.reset(command_token)
                if action == 'start' and config not in (None, packet['config_file']):
                    app.notify('Native start cannot change the frozen configuration identity')
                    return
                request = {'schema_version': 1, 'request_id': str(uuid.uuid4()), 'action': action,
                           'expected_boot_id': packet['boot_id'], 'expected_instance_id': packet['instance_id'],
                           'expected_script_file': packet['script_file'], 'expected_config_file': packet['config_file'],
                           'expected_controller_ids': packet['controller_ids'], 'observed_sequence': packet['sequence']}
                response = await commands._execute_locked(request)
                app.notify(response['error'] or f"Native {action} completed")
                return response
        return wrapped
    return decorate


async def strict_stop(app):
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    core = app.trading_core
    strategy = core.strategy
    reconciliation_connectors = tuple(core.connectors.values())
    if not isinstance(strategy, StrategyV2Base):
        return ShutdownResult(False, 'Native reconciliation requires a V2 strategy')
    # Keep the original risk latch across failed shutdown retries. on_stop's latch
    # is administrative, and the still-running clock can persist it while we await.
    if not hasattr(strategy, '_native_shutdown_initial_stop_latch'):
        strategy._native_shutdown_initial_stop_latch = strategy._is_stop_triggered
    result = await strategy.on_stop(strict=True)
    if not isinstance(result, ShutdownResult) or not result.reconciliation_complete:
        return result if isinstance(result, ShutdownResult) else ShutdownResult(False, 'Missing executor shutdown receipt')
    if not await core.cancel_outstanding_orders(strict=True):
        return ShutdownResult(False, 'Outstanding orders are not reconciled; runtime state retained')
    persist_risk = getattr(strategy, '_persist_risk_state', None)
    if callable(persist_risk):
        # Only restore the pre-shutdown value AFTER complete executor/venue proof.
        # Persistence is synchronous: no clock tick can run with the temporary latch.
        strategy._is_stop_triggered = strategy._native_shutdown_initial_stop_latch
        try:
            persist_risk()
        finally:
            strategy._is_stop_triggered = True
    if not await core.stop_strategy():
        return ShutdownResult(False, 'Strategy stop did not complete')
    app.stop_market_event_notifiers()
    if app._mqtt:
        app._mqtt._remove_market_event_listeners()
    for name in list(core.connectors):
        core.remove_connector(name)
    if core._is_running and not await core.stop_clock():
        return ShutdownResult(False, 'Clock stop did not complete')
    if core.markets_recorder:
        core.markets_recorder.stop()
        core.markets_recorder = None
    core.strategy_name = None
    core.strategy_config_map = None
    core._strategy_file_name = None
    core._config_source = None
    core._config_data = None
    strategy.executor_orchestrator.active_executors.clear()
    strategy.executor_orchestrator.positions_held.clear()
    app.native_lifecycle.reconciliation_connectors = reconciliation_connectors
    return ShutdownResult(True)


class NativeCommands:
    def __init__(self, app):
        self.app = app
        self.lock = asyncio.Lock()
        self.responses = OrderedDict()

    async def execute(self, request):
        async with self.lock:
            return await self._execute_locked(request)

    async def _execute_locked(self, request):
        observer = self.app.native_lifecycle
        core = self.app.trading_core
        request_id = request.get('request_id', '')
        if request_id in self.responses:
            previous_request, response = self.responses[request_id]
            if previous_request == request:
                return dict(response)
            return self._response(request, False, False, 'Request ID reused with different content')
        packet = observer.snapshot(core, advance=False)
        try:
            uuid.UUID(request_id)
        except (ValueError, TypeError, AttributeError):
            return self._response(request, False, False, 'Invalid request ID')
        expected_keys = {'schema_version', 'request_id', 'action', 'expected_boot_id', 'expected_instance_id',
                         'expected_script_file', 'expected_config_file', 'expected_controller_ids', 'observed_sequence'}
        if set(request) != expected_keys or request.get('schema_version') != 1 or request.get('action') not in {'start', 'stop'}:
            return self._response(request, False, False, 'Unsupported native command schema')
        if packet is None:
            return self._response(request, False, False, 'Loaded process identity unavailable')
        match = (request['expected_boot_id'] == packet['boot_id']
                 and request['expected_instance_id'] == packet['instance_id']
                 and request['expected_script_file'] == packet['script_file']
                 and request['expected_config_file'] == packet['config_file']
                 and request['expected_controller_ids'] == packet['controller_ids']
                 and type(request['observed_sequence']) is int
                 and request['observed_sequence'] == packet['sequence'])
        if not match:
            return self._response(request, False, False, 'Process identity or observed sequence changed')
        action = request['action']
        if action == 'start' and (packet['state'] != 'stopped' or not packet['reconciliation_complete'] or core.strategy is not None):
            return self._response(request, False, False, 'Start requires a reconciled stopped process')
        if core.strategy is not None and (
                core.strategy_name.removesuffix('.py') + '.py' != packet['script_file']
                or core._config_source != packet['config_file']
                or sorted(core.strategy.controllers) != packet['controller_ids']):
            return self._response(request, False, False, 'Loaded strategy identity changed')
        if action == 'stop' and (packet['state'] not in {'running', 'unknown'} or core.strategy is None):
            return self._response(request, False, False, 'Stop requires a running strategy')
        observer.transition('starting' if action == 'start' else 'stopping')
        error = None
        completed = False
        context_token = native_command_context.set((self, asyncio.current_task()))
        try:
            if action == 'stop':
                async with asyncio.timeout(40):
                    result = await strict_stop(self.app)
                completed = result.reconciliation_complete
                error = result.error
                observer.transition('stopped' if completed else 'unknown', completed)
            else:
                async with asyncio.timeout(40):
                    connectors = getattr(observer, 'reconciliation_connectors', ())
                    if not connectors:
                        raise ValueError('No retained venue reconciliation source')
                    for connector in connectors:
                        if await connector.native_orders_reconciled() is not True:
                            raise ValueError('Venue orders changed after shutdown')
                    sources = capture_sources(packet['config_file'], packet['script_file'], packet['controller_ids'])
                    if sources != getattr(observer, 'loaded_sources', None):
                        raise ValueError('Native configuration differs from loaded source bytes')
                    source_guard = NativeSourceGuard(sources)
                    token = native_sources.set(source_guard)
                    try:
                        await self.app.start_check(v2_conf=packet['config_file'])
                    finally:
                        source_guard.active = False
                        native_sources.reset(token)
                completed = (core._strategy_running is True and core.strategy is not None
                             and not getattr(core.strategy, '_is_stop_triggered', False)
                             and core.strategy_name.removesuffix('.py') + '.py' == packet['script_file']
                             and core._config_source == packet['config_file']
                             and sorted(core.strategy.controllers) == packet['controller_ids'])
                observer.transition('running' if completed else 'unknown')
                if not completed:
                    error = 'Native strategy start did not complete'
        except asyncio.CancelledError:
            # The operation may have made partial progress; cancellation is not
            # shutdown proof and must not leave a permanent transition in flight.
            observer.transition('unknown')
            raise
        except Exception as exc:
            observer.transition('unknown')
            error = f'Native {action} failed: {type(exc).__name__}'
            self.app.logger().exception('Native command failed')
        finally:
            native_command_context.reset(context_token)
        response = self._response(request, True, completed, error)
        self.responses[request_id] = (dict(request), response)
        if len(self.responses) > 100:
            self.responses.popitem(last=False)
        return response

    def _response(self, request, accepted, completed, error):
        observer = self.app.native_lifecycle
        return {'schema_version': 1, 'request_id': request.get('request_id', ''),
                'boot_id': observer.boot_id, 'instance_id': observer.instance_id,
                'action': request.get('action', ''), 'accepted': accepted, 'execution_completed': completed,
                'state': observer.state, 'sequence': observer.sequence,
                'reconciliation_complete': observer.reconciliation_complete, 'error': error}
