from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from hummingbot.client.native_lifecycle import NativeLifecycle
from hummingbot.core.trading_core import TradingCore


def core():
    return SimpleNamespace(strategy=SimpleNamespace(controllers={'a': object()}, _is_stop_triggered=False),
                           strategy_name='v2_with_controllers', _config_source='ok_rsi.yml', _strategy_running=True)


def test_lifecycle_freezes_loaded_identity_after_stop_without_inventing_reconciliation():
    owner = core()
    observer = NativeLifecycle('ok-rsi-main')
    first = observer.snapshot(owner)
    owner.strategy = None
    owner._config_source = None
    owner.strategy_name = None
    second = observer.snapshot(owner)
    assert first['state'] == 'running'
    assert second['state'] == 'unknown'
    assert second['reconciliation_complete'] is False
    assert second['controller_ids'] == ['a']
    assert second['config_file'] == 'ok_rsi.yml'
    assert second['boot_id'] == first['boot_id']
    assert second['sequence'] > first['sequence']


@pytest.mark.asyncio
async def test_strict_cancellation_propagates_failure_and_does_not_discard_connectors():
    connector = SimpleNamespace(limit_orders=[SimpleNamespace(client_order_id='order')],
                                in_flight_orders={}, cancel_all=AsyncMock(return_value=[SimpleNamespace(order_id='order', success=False)]))
    owner = SimpleNamespace(connector_manager=SimpleNamespace(connectors={'test': connector}), KILL_TIMEOUT=1, logger=Mock())
    assert await TradingCore.cancel_outstanding_orders(owner, strict=True) is False
    assert owner.connector_manager.connectors['test'] is connector
    connector.cancel_all.side_effect = RuntimeError('cancel failure')
    assert await TradingCore.cancel_outstanding_orders(owner, strict=True) is False


@pytest.mark.asyncio
async def test_strict_executor_timeout_retains_collections_and_skips_persistence():
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    executor = SimpleNamespace(is_closed=False, early_stop=Mock(), config=SimpleNamespace(id='pending'),
                               executor_info=SimpleNamespace(is_done=False))
    owner = SimpleNamespace(active_executors={'a': [executor]}, _planned_resume_pending=False, logger=Mock(),
                            store_all_positions=Mock(), store_all_executors=Mock())
    result = await ExecutorOrchestrator.stop(owner, 0, strict=True)
    assert result.reconciliation_complete is False
    assert result.error == 'Executor shutdown timed out; runtime collections retained'
    executor.early_stop.assert_called_once_with(keep_position=False)
    assert owner.active_executors['a'] == [executor]
    owner.store_all_positions.assert_not_called()
    owner.store_all_executors.assert_not_called()


@pytest.mark.asyncio
async def test_strict_executor_persistence_failure_retains_runtime_state():
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    executor = SimpleNamespace(is_closed=True, executor_info=SimpleNamespace(is_done=True))
    owner = SimpleNamespace(active_executors={'a': [executor]}, _planned_resume_pending=False, logger=Mock(),
                            store_all_positions=Mock(side_effect=RuntimeError('database failed')), store_all_executors=Mock())
    result = await ExecutorOrchestrator.stop(owner, 1, strict=True)
    assert result.reconciliation_complete is False
    assert result.error == 'Executor persistence/shutdown failed: RuntimeError'
    owner.store_all_positions.assert_called_once_with(clear=False, strict=True)
    assert owner.active_executors['a'] == [executor]
    owner.store_all_executors.assert_not_called()


@pytest.mark.asyncio
async def test_strict_stop_preserves_pending_resume_without_stopping_or_persisting():
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    executor = SimpleNamespace(is_closed=False, early_stop=Mock(), executor_info=SimpleNamespace(is_done=False))
    owner = SimpleNamespace(active_executors={'a': [executor]}, _planned_resume_pending=True,
                            store_all_positions=Mock(), store_all_executors=Mock())
    result = await ExecutorOrchestrator.stop(owner, 0, strict=True)
    assert result.reconciliation_complete is False
    assert result.error == 'Planned position resume unresolved; durable ownership requires reconciliation'
    assert owner._planned_resume_pending is True
    assert owner.active_executors['a'] == [executor]
    executor.early_stop.assert_not_called()
    owner.store_all_positions.assert_not_called()
    owner.store_all_executors.assert_not_called()


def test_strict_positions_reject_unmapped_records_without_clearing():
    from unittest.mock import patch

    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    positions = {None: [SimpleNamespace(connector_name='test', trading_pair='BTC-USDC')]}
    owner = SimpleNamespace(positions_held=positions, _update_positions_from_done_executors=Mock(), logger=Mock())
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance'):
        with pytest.raises(ValueError):
            ExecutorOrchestrator.store_all_positions(owner, clear=False, strict=True)
    assert owner.positions_held == positions


@pytest.mark.asyncio
async def test_native_command_rejects_old_sequence_and_identity_before_actions():
    import uuid

    from hummingbot.client.native_commands import NativeCommands
    owner = core()
    app = SimpleNamespace(trading_core=owner, native_lifecycle=NativeLifecycle('ok-rsi-main'))
    packet = app.native_lifecycle.snapshot(owner)
    commands = NativeCommands(app)
    request = dict(schema_version=1, request_id=str(uuid.uuid4()), action='stop', expected_boot_id=packet['boot_id'],
                   expected_instance_id='ok-rsi-main', expected_script_file=packet['script_file'],
                   expected_config_file=packet['config_file'], expected_controller_ids=['a'], observed_sequence=packet['sequence'])
    app.native_lifecycle.snapshot(owner)
    response = await commands.execute(request)
    assert response['accepted'] is False
    assert response['execution_completed'] is False
    request['request_id'] = str(uuid.uuid4())
    request['observed_sequence'] = app.native_lifecycle.sequence
    request['expected_boot_id'] = str(uuid.uuid4())
    response = await commands.execute(request)
    assert response['accepted'] is False


@pytest.mark.asyncio
async def test_native_start_cannot_infer_reconciliation_from_no_strategy():
    import uuid

    from hummingbot.client.native_commands import NativeCommands
    owner = core()
    app = SimpleNamespace(trading_core=owner, native_lifecycle=NativeLifecycle('ok-rsi-main'))
    app.native_lifecycle.snapshot(owner)
    owner.strategy = None
    packet = app.native_lifecycle.snapshot(owner)
    request = dict(schema_version=1, request_id=str(uuid.uuid4()), action='start', expected_boot_id=packet['boot_id'],
                   expected_instance_id='ok-rsi-main', expected_script_file=packet['script_file'],
                   expected_config_file=packet['config_file'], expected_controller_ids=['a'], observed_sequence=packet['sequence'])
    response = await NativeCommands(app).execute(request)
    assert response['accepted'] is False
    assert response['reconciliation_complete'] is False


@pytest.mark.asyncio
async def test_strict_success_receipts_retain_collections_until_outer_reconciliation():
    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    executor = SimpleNamespace(is_closed=True, executor_info=SimpleNamespace(is_done=True))
    owner = SimpleNamespace(active_executors={'a': [executor]}, _planned_resume_pending=False, logger=Mock(),
                            store_all_positions=Mock(), store_all_executors=Mock())
    result = await ExecutorOrchestrator.stop(owner, 1, strict=True)
    assert result.reconciliation_complete is True
    assert result.error is None
    assert owner.active_executors['a'] == [executor]
    owner.store_all_positions.assert_called_once_with(clear=False, strict=True)
    owner.store_all_executors.assert_called_once_with(clear=False)


@pytest.mark.asyncio
async def test_no_local_orders_without_venue_proof_is_not_reconciled():
    connector = SimpleNamespace(limit_orders=[], in_flight_orders={})
    owner = SimpleNamespace(connector_manager=SimpleNamespace(connectors={'test': connector}), KILL_TIMEOUT=1, logger=Mock())
    assert await TradingCore.cancel_outstanding_orders(owner, strict=True) is False
    connector.native_orders_reconciled = AsyncMock(return_value=False)
    assert await TradingCore.cancel_outstanding_orders(owner, strict=True) is False
    connector.native_orders_reconciled.return_value = True
    assert await TradingCore.cancel_outstanding_orders(owner, strict=True) is True


def native_request(app, action):
    import uuid
    packet = app.native_lifecycle.snapshot(app.trading_core)
    return dict(schema_version=1, request_id=str(uuid.uuid4()), action=action, expected_boot_id=packet['boot_id'],
                expected_instance_id=packet['instance_id'], expected_script_file=packet['script_file'],
                expected_config_file=packet['config_file'], expected_controller_ids=packet['controller_ids'],
                observed_sequence=packet['sequence'])


@pytest.mark.asyncio
async def test_stop_start_uses_owner_hooks_and_same_request_never_executes_twice():
    from hummingbot.client.native_commands import NativeCommands
    from hummingbot.client.native_lifecycle import ShutdownResult
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    strategy = Mock(spec=StrategyV2Base)
    strategy.controllers = {'a': object()}
    strategy._is_stop_triggered = False
    strategy.on_stop = AsyncMock(return_value=ShutdownResult(True))
    strategy.executor_orchestrator = SimpleNamespace(active_executors={'a': []}, positions_held={})
    connector = SimpleNamespace(limit_orders=[], in_flight_orders={}, native_orders_reconciled=AsyncMock(return_value=True))
    connectors = {'okx': connector}
    owner = core()
    owner.strategy = strategy
    owner.connector_manager = SimpleNamespace(connectors=connectors)
    owner.connectors = connectors
    owner.KILL_TIMEOUT = 1
    owner.logger = Mock()
    owner.cancel_outstanding_orders = lambda strict: TradingCore.cancel_outstanding_orders(owner, strict=strict)

    async def stop_strategy():
        owner.strategy = None
        owner._strategy_running = False
        return True
    owner.stop_strategy = AsyncMock(side_effect=stop_strategy)
    owner.remove_connector = Mock(side_effect=lambda name: connectors.pop(name))
    owner._is_running = False
    owner.markets_recorder = Mock()
    app = SimpleNamespace(trading_core=owner, native_lifecycle=NativeLifecycle('ok-rsi-main'),
                          stop_market_event_notifiers=Mock(), _mqtt=Mock(), logger=Mock(),
                          _peek_config=Mock(return_value={'script_file_name': 'v2_with_controllers.py'}))

    async def start_check(*, v2_conf):
        owner.strategy = strategy
        owner.strategy_name = 'v2_with_controllers'
        owner._config_source = v2_conf
        owner._strategy_running = True
    app.start_check = AsyncMock(side_effect=start_check)
    commands = NativeCommands(app)
    request = native_request(app, 'stop')
    result = await commands.execute(request)
    assert result['accepted'] is True
    assert result['execution_completed'] is True
    assert result['state'] == 'stopped'
    assert result['reconciliation_complete'] is True
    assert await commands.execute(request) == result
    strategy.on_stop.assert_awaited_once_with(strict=True)
    app._mqtt._remove_market_event_listeners.assert_called_once()
    from unittest.mock import patch
    app.native_lifecycle.loaded_sources = {}
    with patch('hummingbot.client.native_commands.capture_sources', return_value={}):
        restart = await commands.execute(native_request(app, 'start'))
    assert restart['execution_completed'] is True
    assert restart['state'] == 'running'
    assert restart['reconciliation_complete'] is False
    app.start_check.assert_awaited_once_with(v2_conf='ok_rsi.yml')


@pytest.mark.asyncio
async def test_explicit_native_mode_blocks_legacy_mqtt_mutations():
    import asyncio

    from hummingbot.client.native_commands import NativeCommands
    from hummingbot.remote_iface.mqtt import MQTTCommands
    app = SimpleNamespace(instance_id='ok-rsi-main', ev_loop=asyncio.get_running_loop(), logger=Mock())
    app.native_commands = NativeCommands(app)
    commands = MQTTCommands(app, Mock(namespace='hbot'))
    for name in ('start', 'stop', 'config', 'import', 'balance_limit', 'balance_paper'):
        response = getattr(commands, '_on_cmd_' + name)(Mock())
        assert response.status == 400
        assert 'native_command' in response.msg


@pytest.mark.asyncio
async def test_native_terminal_boundary_serializes_and_clears_context_after_failure():
    import asyncio
    from unittest.mock import patch

    from hummingbot.client.native_commands import NativeCommands, native_command_boundary, native_command_context
    app = SimpleNamespace(trading_core=core(), native_lifecycle=NativeLifecycle('ok-rsi-main'),
                          notify=Mock(), logger=Mock())
    app.native_commands = NativeCommands(app)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def fail_stop(_):
        assert native_command_context.get() == (app.native_commands, asyncio.current_task())
        entered.set()
        await release.wait()
        raise TimeoutError('bounded shutdown failed')

    @native_command_boundary('stop')
    async def terminal_stop(_):
        raise AssertionError('unguarded legacy stop must not execute')

    with patch('hummingbot.client.native_commands.strict_stop', side_effect=fail_stop):
        first = asyncio.create_task(terminal_stop(app))
        await entered.wait()
        request = native_request(app, 'stop')
        second = asyncio.create_task(app.native_commands.execute(request))
        await asyncio.sleep(0)
        assert not second.done()
        release.set()
        failed = await first
        rejected = await second
    assert failed['accepted'] is True
    assert failed['execution_completed'] is False
    assert failed['state'] == 'unknown'
    assert rejected['accepted'] is False
    assert app.trading_core.strategy is not None
    assert native_command_context.get() is None
    assert not app.native_commands.lock.locked()


@pytest.mark.asyncio
async def test_initial_explicit_gui_start_captures_identity_under_native_lock():
    from unittest.mock import patch

    from hummingbot.client.native_commands import NativeCommands, native_command_boundary
    from hummingbot.client.native_sources import native_sources
    owner = core()
    owner.strategy = None
    owner._strategy_running = False
    app = SimpleNamespace(trading_core=owner, native_lifecycle=NativeLifecycle('ok-rsi-main'), notify=Mock())
    app.native_commands = NativeCommands(app)
    guards = []

    @native_command_boundary('start')
    async def start_check(app, *, v2_conf):
        assert app.native_commands.lock.locked()
        guards.append(native_sources.get())
        owner.strategy = core().strategy
        owner._strategy_running = True
        owner._config_source = v2_conf

    with patch('hummingbot.client.native_commands.capture_sources', return_value={}) as capture:
        await start_check(app, v2_conf='ok_rsi.yml')
    capture.assert_called_once_with('ok_rsi.yml', 'v2_with_controllers.py', None)
    assert app.native_lifecycle.snapshot(owner)['state'] == 'running'
    assert guards[0].active is False
    assert native_sources.get() is None


@pytest.mark.asyncio
async def test_restart_rejects_same_identity_parameter_drift_before_start():
    from unittest.mock import patch

    from hummingbot.client.native_commands import NativeCommands
    owner = core()
    observer = NativeLifecycle('ok-rsi-main')
    observer.snapshot(owner)
    owner.strategy = None
    owner._strategy_running = False
    observer.transition('stopped', True)
    observer.reconciliation_connectors = (SimpleNamespace(native_orders_reconciled=AsyncMock(return_value=True)),)
    observer.loaded_sources = {'source': b'id: a\namount: 1\n'}
    app = SimpleNamespace(trading_core=owner, native_lifecycle=observer, start_check=AsyncMock(), logger=Mock())
    with patch('hummingbot.client.native_commands.capture_sources', return_value={'source': b'id: a\namount: 2\n'}):
        response = await NativeCommands(app).execute(native_request(app, 'start'))
    assert response['execution_completed'] is False
    app.start_check.assert_not_awaited()


def test_retained_executor_persistence_retry_does_not_credit_performance_twice():
    from unittest.mock import patch

    from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
    info = SimpleNamespace(net_pnl_quote=10)
    executor = SimpleNamespace(executor_info=info)
    owner = SimpleNamespace(
        active_executors={'a': [executor]}, _persist_position_hold_entries_from_executor_info=Mock(),
        _update_cached_performance=Mock(), _record_stored_executor=Mock())
    with patch('hummingbot.strategy_v2.executors.executor_orchestrator.MarketsRecorder.get_instance') as recorder:
        ExecutorOrchestrator.store_all_executors(owner, clear=False)
        ExecutorOrchestrator.store_all_executors(owner, clear=False)
        assert recorder().store_or_update_executor.call_count == 2
        owner._update_cached_performance.assert_not_called()
        owner._record_stored_executor.assert_not_called()
        assert owner.active_executors['a'] == [executor]
        ExecutorOrchestrator.store_all_executors(owner)
    owner._update_cached_performance.assert_called_once_with('a', info)
    assert owner.active_executors == {}


@pytest.mark.asyncio
@pytest.mark.parametrize('already_latched', [False, True])
async def test_native_shutdown_preserves_preexisting_risk_latch_after_reconciliation(already_latched):
    from hummingbot.client.native_commands import strict_stop
    from hummingbot.client.native_lifecycle import ShutdownResult
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    strategy = Mock(spec=StrategyV2Base)
    strategy._is_stop_triggered = already_latched
    strategy.executor_orchestrator = SimpleNamespace(active_executors={}, positions_held={})
    persisted = []
    strategy._persist_risk_state = Mock(side_effect=lambda: persisted.append(strategy._is_stop_triggered))

    async def on_stop(strict):
        strategy._is_stop_triggered = True
        strategy._persist_risk_state()  # The still-running clock can persist during awaited shutdown.
        return ShutdownResult(True)
    strategy.on_stop = AsyncMock(side_effect=on_stop)
    owner = SimpleNamespace(strategy=strategy, connectors={}, cancel_outstanding_orders=AsyncMock(return_value=True),
                            stop_strategy=AsyncMock(return_value=True), _is_running=False, markets_recorder=None)
    app = SimpleNamespace(trading_core=owner, stop_market_event_notifiers=Mock(), _mqtt=None,
                          native_lifecycle=NativeLifecycle('test'))
    result = await strict_stop(app)
    assert result.reconciliation_complete is True
    assert persisted[-1] is already_latched
    assert strategy._is_stop_triggered is True


@pytest.mark.asyncio
async def test_native_start_never_acknowledges_restored_stop_latch_as_running():
    from unittest.mock import patch

    from hummingbot.client.native_commands import NativeCommands
    owner = core()
    observer = NativeLifecycle('test')
    observer.capture(owner)
    observer.loaded_sources = {}
    observer.reconciliation_connectors = [SimpleNamespace(native_orders_reconciled=AsyncMock(return_value=True))]
    owner.strategy = None
    owner._strategy_running = False
    observer.transition('stopped', True)

    async def start_check(v2_conf):
        owner.strategy = core().strategy
        owner.strategy._is_stop_triggered = True
        owner._strategy_running = True
    app = SimpleNamespace(trading_core=owner, native_lifecycle=observer, start_check=AsyncMock(side_effect=start_check), logger=Mock())
    with patch('hummingbot.client.native_commands.capture_sources', return_value={}):
        result = await NativeCommands(app).execute(native_request(app, 'start'))
    assert result['accepted'] is True
    assert result['execution_completed'] is False
    assert result['state'] == 'unknown'
    assert observer.snapshot(owner)['state'] == 'unknown'


@pytest.mark.asyncio
async def test_failed_reconciliation_does_not_clear_stop_latch_and_retry_preserves_original():
    from hummingbot.client.native_commands import strict_stop
    from hummingbot.client.native_lifecycle import ShutdownResult
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    strategy = Mock(spec=StrategyV2Base)
    strategy._is_stop_triggered = False
    strategy.executor_orchestrator = SimpleNamespace(active_executors={}, positions_held={})
    persisted = []
    strategy._persist_risk_state = Mock(side_effect=lambda: persisted.append(strategy._is_stop_triggered))

    async def on_stop(strict):
        strategy._is_stop_triggered = True
        return ShutdownResult(True)
    strategy.on_stop = AsyncMock(side_effect=on_stop)
    owner = SimpleNamespace(strategy=strategy, connectors={}, cancel_outstanding_orders=AsyncMock(side_effect=[False, True]),
                            stop_strategy=AsyncMock(return_value=True), _is_running=False, markets_recorder=None)
    app = SimpleNamespace(trading_core=owner, stop_market_event_notifiers=Mock(), _mqtt=None,
                          native_lifecycle=NativeLifecycle('test'))
    assert not (await strict_stop(app)).reconciliation_complete
    assert persisted == []
    assert strategy._is_stop_triggered is True
    owner.stop_strategy.assert_not_awaited()
    assert (await strict_stop(app)).reconciliation_complete
    assert persisted == [False]
    assert strategy._is_stop_triggered is True


@pytest.mark.asyncio
async def test_risk_persistence_failure_retains_runtime_and_stop_latch():
    from hummingbot.client.native_commands import strict_stop
    from hummingbot.client.native_lifecycle import ShutdownResult
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    strategy = Mock(spec=StrategyV2Base)
    strategy._is_stop_triggered = False
    strategy.on_stop = AsyncMock(return_value=ShutdownResult(True))
    strategy._persist_risk_state = Mock(side_effect=RuntimeError('disk error'))
    owner = SimpleNamespace(strategy=strategy, connectors={}, cancel_outstanding_orders=AsyncMock(return_value=True),
                            stop_strategy=AsyncMock())
    with pytest.raises(RuntimeError, match='disk error'):
        await strict_stop(SimpleNamespace(trading_core=owner))
    owner.stop_strategy.assert_not_awaited()
    assert owner.strategy is strategy
    assert strategy._is_stop_triggered is True


@pytest.mark.asyncio
@pytest.mark.parametrize('action', ['start', 'stop'])
async def test_cancelled_native_command_releases_transition_without_claiming_reconciliation(action):
    import asyncio
    from unittest.mock import patch

    from hummingbot.client.native_commands import NativeCommands, native_command_context

    owner = core()
    observer = NativeLifecycle('ok-rsi-main')
    observer.snapshot(owner)
    entered = asyncio.Event()

    async def pending(*args, **kwargs):
        entered.set()
        await asyncio.Future()

    app = SimpleNamespace(trading_core=owner, native_lifecycle=observer, start_check=AsyncMock(side_effect=pending))
    if action == 'start':
        owner.strategy = None
        owner._strategy_running = False
        observer.transition('stopped', True)
        observer.reconciliation_connectors = [SimpleNamespace(native_orders_reconciled=AsyncMock(return_value=True))]
        observer.loaded_sources = {}
    retained_strategy = owner.strategy
    commands = NativeCommands(app)
    request = native_request(app, action)
    with patch('hummingbot.client.native_commands.strict_stop', side_effect=pending), \
            patch('hummingbot.client.native_commands.capture_sources', return_value={}):
        operation = asyncio.create_task(commands.execute(request))
        await asyncio.wait_for(entered.wait(), 1)
        assert observer.state == ('starting' if action == 'start' else 'stopping')
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    assert observer.state == 'unknown'
    assert observer.reconciliation_complete is False
    assert owner.strategy is retained_strategy
    assert not commands.lock.locked()
    assert request['request_id'] not in commands.responses
    assert native_command_context.get() is None
    # Cancellation must not leave an immortal transitional state suppressing observations.
    assert observer.snapshot(owner)['state'] not in {'starting', 'stopping'}
