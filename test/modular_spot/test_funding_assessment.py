"""Readiness inspects real native funding without consuming or releasing capital."""
import asyncio
from test.hummingbot.strategy_v2.paper import test_paper_funding as fixtures
from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper
from threading import Event

import pytest

from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.paper_session import NativePaperSession


class _RevisionCheckedSnapshot:
    def assess_with_revision_check(self):
        status = self.assess()
        status['revision_verified'] = True
        return status


@pytest.mark.asyncio
async def test_assessment_is_read_only_and_exposes_binding_failure_before_signal(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'assessment.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        store = NativeFundingStore(owner, session.recorder)
        with session.recorder.sql_manager.get_new_session() as sql:
            before = [(row.key, row.value) for row in sql.query(Metadata).all()]
        result = store.assess()
        assert result['available_quote'] == '50'
        assert result['pending_quote'] == '0'
        assert result['assessment'] == 'read_only_native_evidence'
        owner._funding_hold = 'Native minimum exceeds funded per-entry cap'
        owner._refresh_funding_assessment()
        assert owner._funding_hold == 'Native minimum exceeds funded per-entry cap'
        assert owner.get_custom_info()['entry_admission']['state'] == 'blocked'
        owner._funding_hold = None
        owner._funding_assessed_at = None
        with session.recorder.sql_manager.get_new_session() as sql:
            assert [(row.key, row.value) for row in sql.query(Metadata).all()] == before
        raw, state = store._load()
        assert store._write(raw, state)
        owner._composition_digest = 'e' * 64  # a trading-configuration change is never carried forward
        owner._funding_store = None
        owner._refresh_funding_assessment()
        assert owner._funding_assessment_hold  # configuration change: refused, never carried forward
        assert owner.get_custom_info()['entry_admission']['state'] == 'blocked'
        assert session.recorder.get_all_executors() == []
        assert session._connector.in_flight_orders == {}
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_assessment_snapshot_uses_fresh_recorder_reads_and_copies_owner_state(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'snapshot.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        store = NativeFundingStore(owner, session.recorder)
        baseline = store.assess()
        monkeypatch.setattr(session.recorder, 'get_all_executors',
                            lambda: (_ for _ in ()).throw(AssertionError('shared recorder read used')))

        snapshot = store.assessment_snapshot()
        assert snapshot.policy is not store.policy
        owner._native_source_hash = 'f' * 64

        result = snapshot.assess()
        assert result['assessment'] == 'read_only_native_evidence'
        assert result['available_quote'] == '50'
        assert result['binding'] == store.binding
        assert result == baseline
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_fixed_allocation_snapshot_does_not_read_wallet_balance(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'fixed-allocation-snapshot.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        store = NativeFundingStore(owner, session.recorder)
        connector = owner.market_data_provider.connectors[owner.config.connector_name]
        monkeypatch.setattr(connector, 'get_available_balance',
                            lambda _asset: (_ for _ in ()).throw(AssertionError('unneeded wallet read')))

        snapshot = store.assessment_snapshot()
        assert snapshot.assess() == store.assess()
        assert snapshot.owner.market_data_provider.connectors[owner.config.connector_name].get_available_balance(
            'USDC') == 0
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_released_terminal_order_snapshot_matches_synchronous_assessment(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'released-order.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        for index, frame in enumerate(fixtures.causal_reversal_frames()):
            now = 600000 + index * 60
            owner._funding_assessed_at = None
            owner._refresh_funding_assessment()
            pending = owner._funding_assessment_task
            if pending is not None:
                await pending
            owner._funding_assessed_at = now
            await session.step(now, books=[dict(trading_pair=fixtures.PAIR, sequence=2 + index,
                                                timestamp=now, bids=[(90, 10)], asks=[(90.1, 10)])],
                               candles=[dict(connector_name='okx', trading_pair=fixtures.PAIR, interval='1m',
                                             frame=frame, available_at=now)])
        executor = session.executor_orchestrator.active_executors[owner.config.id][0]
        session._connector.cancel(fixtures.PAIR, executor._open_order.order.client_order_id)
        await session._connector.drain()
        owner.executors_info = [executor.executor_info]
        store = owner._funding_store
        await executor._request_expired_entry_reconciliation(executor._entry_orders())
        owner.executors_info = [executor.executor_info]
        released = store.reconcile()
        assert released['released_intents'] == 1
        session.recorder.store_or_update_executor(executor)

        synchronous = store.assess()
        snapshot = store.assessment_snapshot()
        order_snapshot = snapshot.owner.market_data_provider.connectors[owner.config.connector_name]._order_tracker.all_orders[
            executor._open_order.order.client_order_id]
        tracked_order = session._connector._order_tracker.all_orders[executor._open_order.order.client_order_id]
        assert order_snapshot.is_done is True
        assert order_snapshot.executed_amount_base == tracked_order.executed_amount_base
        assert order_snapshot.exchange_order_id == tracked_order.exchange_order_id
        assert snapshot.assess() == synchronous
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_blocked_read_assessment_keeps_event_loop_live_and_pending_until_publish(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'async-assessment.sqlite')
    started, release = Event(), Event()
    try:
        owner = next(iter(session.controllers.values()))

        class BlockedSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                started.set()
                if not release.wait(2):
                    raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BlockedSnapshot(store.binding))
        beats = 0

        def beat():
            nonlocal beats
            beats += 1

        owner._refresh_funding_assessment()
        assert owner._funding_assessment_hold == 'Native funding assessment pending'
        assert await asyncio.to_thread(started.wait, 1)
        beat()
        await asyncio.sleep(0)
        assert beats == 1
        assert owner._funding_assessment_hold == 'Native funding assessment pending'

        release.set()
        await asyncio.wait_for(owner._funding_assessment_task, 1)
        assert owner._funding_assessment_hold is None
        assert owner._funding_store.status['binding'] == owner._funding_store.binding
        assert owner._funding_store.status['completed_at'] >= owner._funding_store.status['observed_at']
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_pending_assessment_blocks_buy_until_assessment_is_ready(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'pending-buy.sqlite')
    started, release = Event(), Event()
    try:
        owner = next(iter(session.controllers.values()))
        prior = owner._funding_assessment_task
        if prior is not None:
            await prior

        class BlockedSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                started.set()
                if not release.wait(2):
                    raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BlockedSnapshot(store.binding))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        assert await asyncio.to_thread(started.wait, 1)
        owner._latest_atr_pct = .01
        owner.processed_data = {'signal_state': {}}
        proposed = owner.get_executor_config(TradeType.BUY, 90, 0.3)
        assert owner.validate_executor_config(proposed) is False
        assert owner._funding_assessment_hold == 'Native funding assessment pending'
        assert session.recorder.get_all_executors() == []

        release.set()
        await asyncio.wait_for(owner._funding_assessment_task, 1)
        assert owner._funding_assessment_hold is None
        assert owner._funding_store.status['available_quote'] == '50'
        assert session.recorder.get_all_executors() == []
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_periodic_pending_refresh_does_not_revoke_fresh_native_admission(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'periodic-pending.sqlite')
    started, release = Event(), Event()
    try:
        owner = next(iter(session.controllers.values()))
        if owner._funding_store is None or owner._funding_assessment_context is None:
            owner._funding_assessed_at = None
            owner._refresh_funding_assessment()
            prior = owner._funding_assessment_task
            if prior is not None:
                await prior
        assert owner._funding_assessment_context is not None

        class BlockedSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                started.set()
                if not release.wait(2):
                    raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BlockedSnapshot(store.binding))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        assert await asyncio.to_thread(started.wait, 1)
        assert owner._funding_assessment_refresh_pending is True
        assert owner._funding_assessment_hold is None

        owner._latest_atr_pct = .01
        owner.processed_data = {'signal_state': {}}
        proposal = owner.get_executor_config(TradeType.BUY, 90, .3)
        assert owner._funding_store.admit(proposal) is True
        _, state = owner._funding_store._load()
        assert proposal.id in state['claims']  # fresh durable admission remained authoritative

        release.set()
        await asyncio.wait_for(owner._funding_assessment_task, 1)
        assert owner._funding_assessment_hold is None
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_stale_assessment_result_cannot_clear_hold_after_source_change(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'stale-assessment.sqlite')
    started, release = Event(), Event()
    try:
        owner = next(iter(session.controllers.values()))
        prior = owner._funding_assessment_task
        if prior is not None:
            await prior

        class BlockedSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                started.set()
                if not release.wait(2):
                    raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BlockedSnapshot(store.binding))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        assert await asyncio.to_thread(started.wait, 1)
        prior_status = dict(owner._funding_store.status)
        owner._native_source_hash = 'f' * 64
        release.set()
        await asyncio.wait_for(owner._funding_assessment_task, 1)
        assert owner._funding_store.status == prior_status
        assert owner._funding_assessment_hold == 'Native funding assessment became stale; refresh required'
        assert owner._funding_assessed_at is None
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_restart_requires_new_lifecycle_assessment_before_buy(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'restart-assessment.sqlite')
    started, release = Event(), Event()
    try:
        owner = next(iter(session.controllers.values()))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        initial = owner._funding_assessment_task
        if initial is not None:
            await initial
        context = owner._funding_assessment_context
        assert context is not None

        from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
        monkeypatch.setattr(ControllerBase, 'start', lambda _controller: None)
        owner._status = owner.status.__class__.TERMINATED
        owner._control_loop_task = None
        owner.start()
        assert owner._funding_assessment_generation != context[-1]

        class BlockedSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                started.set()
                if not release.wait(2):
                    raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BlockedSnapshot(store.binding))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        assert owner._funding_assessment_hold == 'Native funding assessment pending'
        assert await asyncio.to_thread(started.wait, 1)
        release.set()
        await asyncio.wait_for(owner._funding_assessment_task, 1)
        assert owner._funding_assessment_hold is None
        assert owner._funding_assessment_context[-1] == owner._funding_assessment_generation
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_periodic_assessment_error_restores_actionable_hold(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'assessment-error.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        if owner._funding_store is None or owner._funding_assessment_context is None:
            owner._funding_assessed_at = None
            owner._refresh_funding_assessment()
            initial = owner._funding_assessment_task
            if initial is not None:
                await initial
        assert owner._funding_assessment_hold is None

        class BrokenSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                raise RuntimeError('fixture read failed')

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BrokenSnapshot(store.binding))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        assert owner._funding_assessment_refresh_pending is True
        assert owner._funding_assessment_hold is None
        await asyncio.wait_for(owner._funding_assessment_task, 1)
        assert owner._funding_assessment_hold == 'fixture read failed'
        assert owner._funding_assessment_error == 'fixture read failed'
        assert owner._funding_assessment_refresh_pending is False
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_missing_assessment_revision_proof_fails_closed(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'missing-revision-proof.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        if owner._funding_assessment_task is not None:
            await owner._funding_assessment_task

        class MissingRevisionProof:
            def __init__(self, binding):
                self.binding = binding

            def assess_with_revision_check(self):
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: MissingRevisionProof(store.binding))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        await asyncio.wait_for(owner._funding_assessment_task, 1)
        assert owner._funding_assessment_hold == 'Funding assessment revision proof is missing'
        assert owner._funding_assessment_error == owner._funding_assessment_hold
        assert owner._funding_assessment_refresh_pending is False
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_late_assessment_publishes_truthful_age_and_preserves_cadence(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'old-assessment.sqlite')
    started, release = Event(), Event()
    clock = {'now': 100}
    try:
        owner = next(iter(session.controllers.values()))
        prior = owner._funding_assessment_task
        if prior is not None:
            await prior
        if owner._funding_store is None:
            owner._funding_store = NativeFundingStore(owner, session.recorder)
            owner._funding_store.status = owner._funding_store.assess()
        monkeypatch.setattr(owner.market_data_provider, 'time', lambda: clock['now'])

        class BlockedSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                started.set()
                if not release.wait(2):
                    raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BlockedSnapshot(store.binding))
        previous_status = dict(owner._funding_store.status)
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        assert await asyncio.to_thread(started.wait, 1)
        clock['now'] = 105
        release.set()
        await asyncio.wait_for(owner._funding_assessment_task, 1)

        assert owner._funding_store.status['assessment'] == 'read_only_native_evidence'
        assert owner._funding_store.status['observed_at'] == 100
        assert owner._funding_store.status['completed_at'] == 105
        assert owner._funding_store.status['assessment_age_seconds'] == 5
        assert owner._funding_store.status['freshness'] == 'stale'
        assert owner._funding_assessment_hold is None
        assert owner._funding_assessed_at == 105
        assert owner._funding_store.status != previous_status

        owner._refresh_funding_assessment()
        assert owner._funding_assessment_task is None  # completion anchors the five-second interval
        clock['now'] = 110
        owner._refresh_funding_assessment()
        assert owner._funding_assessment_refresh_pending is True
        assert owner._funding_assessment_hold is None
        refresh = owner._funding_assessment_task
        assert refresh is not None
        await refresh
        assert owner._funding_assessment_hold is None
        assert owner._funding_store.status['freshness'] == 'fresh'
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_stop_timeout_retains_running_reader_and_refuses_restart_until_done(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'stop-assessment.sqlite')
    started, release = Event(), Event()
    try:
        owner = next(iter(session.controllers.values()))
        prior = owner._funding_assessment_task
        if prior is not None:
            await prior

        class BlockedSnapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding):
                self.binding = binding

            def assess(self):
                started.set()
                if not release.wait(3):
                    raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: BlockedSnapshot(store.binding))
        owner._funding_assessed_at = None
        owner._refresh_funding_assessment()
        assert await asyncio.to_thread(started.wait, 1)
        future = owner._funding_assessment_future
        owner.stop()
        from hummingbot.strategy_v2.runnable_base import RunnableBase

        async def stopped_base_loop(_controller):
            return None
        monkeypatch.setattr(RunnableBase, 'control_loop', stopped_base_loop)
        await owner.control_loop()
        owner._status = owner.status.__class__.TERMINATED
        owner._control_loop_task = None
        owner._funding_assessment_generation += 1
        assert future is not None and future.running()
        with pytest.raises(RuntimeError, match='funding assessment is still stopping'):
            owner.start()

        release.set()
        await asyncio.wrap_future(future)
        task = owner._funding_assessment_task
        if task is not None:
            await task
        from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
        monkeypatch.setattr(ControllerBase, 'start', lambda _controller: None)
        owner.start()
        assert owner._funding_assessment_future is None
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_five_funding_owners_all_receive_fair_assessment_progress(tmp_path, monkeypatch):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    configs = [funded_config().model_copy(update={'id': f'modular_spot_owner_{index}'}) for index in range(5)]
    session = NativePaperSession(paper(), configs, tmp_path / 'five-owner-assessment.sqlite')
    started, release = Event(), Event()
    clock = {'now': 100}
    start_order = []
    try:
        owners = list(session.controllers.values())
        for owner in owners:
            monkeypatch.setattr(owner.market_data_provider, 'time', lambda clock=clock: clock['now'])
        old_tasks = [owner._funding_assessment_task for owner in owners if owner._funding_assessment_task is not None]
        if old_tasks:
            await asyncio.gather(*old_tasks)

        class Snapshot(_RevisionCheckedSnapshot):
            def __init__(self, binding, controller_id):
                self.binding = binding
                self.controller_id = controller_id

            def assess(self):
                start_order.append(self.controller_id)
                if self.controller_id == owners[0].config.id:
                    started.set()
                    if not release.wait(2):
                        raise RuntimeError('test reader was not released')
                return {'assessment': 'read_only_native_evidence', 'binding': self.binding,
                        'revision': 0, 'pending_quote': '0', 'available_quote': '50'}

        monkeypatch.setattr(NativeFundingStore, 'assessment_snapshot',
                            lambda store: Snapshot(store.binding, store.owner.config.id))
        for owner in owners:
            owner._funding_assessed_at = None
            owner._refresh_funding_assessment()
        assert await asyncio.to_thread(started.wait, 1)
        assert [owner._funding_assessment_hold for owner in owners] == [
            'Native funding assessment pending'] * len(owners)
        release.set()
        await asyncio.gather(*(owner._funding_assessment_task for owner in owners))
        assert start_order == [owner.config.id for owner in owners]
        assert all(owner._funding_assessment_hold is None for owner in owners)
        assert all(owner._funding_store.status['assessment'] == 'read_only_native_evidence'
                   for owner in owners)

        clock['now'] = 105
        started.clear()
        release = Event()
        for owner in owners:
            owner._refresh_funding_assessment()
        assert await asyncio.to_thread(started.wait, 1)
        for owner in owners:
            owner._refresh_funding_assessment()  # a control tick cannot enqueue duplicate work or revoke readiness
            assert owner._funding_assessment_refresh_pending is True
            assert owner._funding_assessment_hold is None
        release.set()
        await asyncio.gather(*(owner._funding_assessment_task for owner in owners))
        assert start_order == [owner.config.id for owner in owners] * 2
        assert all(owner._funding_assessment_hold is None for owner in owners)
        assert all(owner._funding_assessment_context[-1] == owner._funding_assessment_generation
                   for owner in owners)

        clock['now'] = 110
        release = Event()
        for owner in owners:
            owner._refresh_funding_assessment()
        release.set()
        await asyncio.gather(*(owner._funding_assessment_task for owner in owners))
        assert start_order == [owner.config.id for owner in owners] * 3
        assert all(owner._funding_assessment_hold is None for owner in owners)
    finally:
        release.set()
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_runtime_prepares_proven_code_upgrade_before_pure_assessment(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'runtime-upgrade.sqlite')
    try:
        await fixtures.NativePaperFundingTests.seed(None, session)
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        assert old._write(raw, state)
        pending = owner._funding_assessment_task
        if pending is not None:
            await pending
        owner._native_source_hash = 'a' * 64
        owner._funding_store = None
        owner._funding_assessed_at = None

        owner._refresh_funding_assessment()
        if owner._funding_assessment_task is not None:
            await owner._funding_assessment_task

        assert owner._funding_assessment_hold is None
        assert owner._funding_store.status['binding'] != old.binding
        assert owner._funding_store.status['assessment'] == 'read_only_native_evidence'
        assert owner._funding_store.entry_notional_ceiling() > 0
        with session.recorder.sql_manager.get_new_session() as sql:
            before = sorted((row.key, row.value) for row in sql.query(Metadata).all())
        owner._funding_store.assess()
        with session.recorder.sql_manager.get_new_session() as sql:
            assert sorted((row.key, row.value) for row in sql.query(Metadata).all()) == before
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
@pytest.mark.parametrize('method', ['assess', 'entry_notional_ceiling'])
async def test_read_only_funding_checks_do_not_rebind_a_code_upgrade(tmp_path, method):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'assessment.sqlite')
    try:
        await fixtures.NativePaperFundingTests.seed(None, session)
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        assert old._write(raw, state)  # retain genuine source proof for the upgrade
        with session.recorder.sql_manager.get_new_session() as sql:
            before = sorted((row.key, row.value) for row in sql.query(Metadata).all())
        owner._native_source_hash = 'a' * 64
        new = NativeFundingStore(owner, session.recorder)

        with pytest.raises(ValueError, match='FUNDING_BINDING_MISMATCH'):
            getattr(new, method)()
        assert new._code_upgrade_checked is False
        with session.recorder.sql_manager.get_new_session() as sql:
            assert sorted((row.key, row.value) for row in sql.query(Metadata).all()) == before

        # The runtime's mutating reconciliation retains the established upgrade
        # policy; status and sizing checks cannot perform that maintenance.
        new.reconcile()
        assert new.assess()['binding'] == new.binding
        with session.recorder.sql_manager.get_new_session() as sql:
            receipts = sql.query(Metadata).filter(
                Metadata.key.like('modular-funding-code-upgrade:v1:%')).all()
            assert len(receipts) == 1
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous
