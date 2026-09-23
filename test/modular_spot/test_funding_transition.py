"""A source upgrade must preserve real native fills and terminal admission proof."""
from decimal import Decimal as D
from test.hummingbot.strategy_v2.paper import test_paper_funding as fixtures
from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper
from unittest.mock import patch

import pytest

from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.paper_session import NativePaperSession


@pytest.mark.asyncio
async def test_explicit_transition_preserves_partial_fill_and_admits_next_buy(tmp_path):
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        session = NativePaperSession(paper(), [funded_config()], tmp_path / 'funding.sqlite')
        try:
            await fixtures.NativePaperFundingTests.seed(None, session)
            owner = next(iter(session.controllers.values()))
            old = owner._funding_store
            executor = session.executor_orchestrator.active_executors[owner.config.id][0]
            await session.step(600061, trades=[dict(trading_pair='BTC-USDC', sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=executor.config.entry_price, amount=D('.1'))])
            old_binding = old.binding
            owner._native_source_hash = 'a' * 64
            new = NativeFundingStore(owner, session.recorder)
            with pytest.raises(ValueError, match='identity'):
                new.reconcile()
            with pytest.raises(ValueError, match='active|quiescent'):
                new.plan_binding_transition(old_binding)
            await session.close()
            owner.executors_info = session.recorder.get_all_executors()
            plan = new.plan_binding_transition(old_binding)
            assert plan['inventory_cost'] != '0'
            assert plan['pending_quote'] == '0'
            before = session.recorder.get_trades_for_config(session.recorder.config_file_path)
            receipt = new.apply_binding_transition(plan['transition_id'])
            assert receipt['from_binding'] == old_binding
            assert receipt['to_binding'] == new.binding
            assert new.apply_binding_transition(plan['transition_id']) == receipt
            assert len(session.recorder.get_trades_for_config(session.recorder.config_file_path)) == len(before) == 1
            assert new.reconcile()['available_quote'] == plan['available_quote']
            session._connector.set_order_book('BTC-USDC', 4, 600061, [(90, 10)], [(90.1, 10)])
            cfg = owner.get_executor_config(TradeType.BUY, D('90'), D('.3'))
            assert new.admit(cfg) is True
            _, state = new._load()
            assert len(state['claims']) == 2
            assert executor.config.id in state['releases']
        finally:
            await session.close()
            MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
async def test_stale_plan_and_unproved_orphan_intent_cannot_rebind(tmp_path):
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'funding.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        assert old._write(raw, state)
        owner._native_source_hash = 'b' * 64
        new = NativeFundingStore(owner, session.recorder)
        plan = new.plan_binding_transition(old.binding)
        raw, state = old._load()
        assert old._write(raw, state)
        with pytest.raises(ValueError, match='stale|changed'):
            new.apply_binding_transition(plan['transition_id'])
        raw, state = old._load()
        state['claims']['orphan'] = {'config': 'c' * 64, 'amount': '1', 'price': '20', 'fee_fraction': '.003'}
        assert old._write(raw, state)
        with pytest.raises(ValueError, match='Unresolved funded intent'):
            new.plan_binding_transition(old.binding)
        with session.recorder.sql_manager.get_new_session() as sql:
            assert old.binding in sql.get(Metadata, old.key).value
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
async def test_legacy_absence_marker_keeps_full_uncertain_reserve_across_upgrade(tmp_path):
    from controllers.generic.modular_spot_core.native_funding import LEGACY_NEVER_STARTED
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'legacy.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        state['claims']['legacy-orphan'] = {'config': 'c' * 64, 'amount': '1', 'price': '20', 'fee_fraction': '.003'}
        state['releases']['legacy-orphan'] = dict(LEGACY_NEVER_STARTED)
        assert old._write(raw, state)
        owner._native_source_hash = 'c' * 64
        new = NativeFundingStore(owner, session.recorder)
        plan = new.plan_binding_transition(old.binding)
        assert D(plan['legacy_uncertain_quote']) == D('20.06')
        assert D(plan['available_quote']) == D('29.94')
        new.apply_binding_transition(plan['transition_id'])
        assert new.reconcile()['released_intents'] == 0
        session._connector.set_order_book('BTC-USDC', session._connector._book_sequence['BTC-USDC'] + 1, session._connector.current_timestamp, [(90, 10)], [(90.1, 10)])
        owner._latest_atr_pct = .01
        too_large = owner.get_executor_config(TradeType.BUY, D('90'), D('.4'))
        too_large.amount = D('.4')
        with pytest.raises(ValueError, match='allocation exhausted'):
            new.admit(too_large)
        assert D(new.reconcile()['available_quote']) == D('29.94')
        smaller = owner.get_executor_config(TradeType.BUY, D('90'), D('.3'))
        smaller.amount = D('.3')
        assert new.admit(smaller)
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
async def test_target_drift_and_tampered_archive_are_rejected(tmp_path):
    import json

    from controllers.generic.modular_spot_core.domain import identity
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'drift.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        assert old._write(raw, state)
        owner._native_source_hash = 'd' * 64
        new = NativeFundingStore(owner, session.recorder)
        plan = new.plan_binding_transition(old.binding)
        original = owner.config.usd_per_entry
        owner.config.usd_per_entry = original + 1
        with pytest.raises(ValueError, match='configuration|identity'):
            new.apply_binding_transition(plan['transition_id'])
        owner.config.usd_per_entry = original
        new.apply_binding_transition(plan['transition_id'])
        receipt_key = 'modular-funding-transition:v1:' + identity([new.key, plan['transition_id']])
        with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
            row = sql.get(Metadata, receipt_key)
            value = json.loads(row.value)
            value['before'] = {}
            row.value = json.dumps(value)
        with pytest.raises(ValueError, match='receipt'):
            new.apply_binding_transition(plan['transition_id'])
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
async def test_apply_failure_rolls_back_every_funding_row(tmp_path):
    from sqlalchemy.orm import Session

    from controllers.generic.modular_spot_core.native_funding_transition import _native_digest
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'atomic.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        assert old._write(raw, state)
        owner._native_source_hash = 'e' * 64
        new = NativeFundingStore(owner, session.recorder)
        plan = new.plan_binding_transition(old.binding)
        add = Session.add

        def fail_receipt(sql, instance, *args, **kwargs):
            if isinstance(instance, Metadata) and instance.key.startswith('modular-funding-transition:'):
                raise OSError('simulated receipt disk failure')
            return add(sql, instance, *args, **kwargs)
        with patch.object(Session, 'add', fail_receipt), pytest.raises(OSError, match='disk failure'):
            new.apply_binding_transition(plan['transition_id'])
        with session.recorder.sql_manager.get_new_session() as sql:
            assert sql.get(Metadata, old.key).value == plan['before'][old.key]
            assert _native_digest(sql) == plan['native_sha256']
        assert new.apply_binding_transition(plan['transition_id'])['to_binding'] == new.binding
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
@pytest.mark.parametrize('surface', ['submission', 'supervised', 'tracker'])
async def test_crash_before_order_row_cannot_pass_quiescence(tmp_path, surface):
    import json

    from hummingbot.core.data_type.common import OrderType
    from hummingbot.core.data_type.in_flight_order import InFlightOrder
    from hummingbot.model.market_state import MarketState
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'pending.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        old = NativeFundingStore(owner, session.recorder)
        raw, state = old._load()
        assert old._write(raw, state)
        pending = InFlightOrder(client_order_id='uncertain-submit', trading_pair='BTC-USDC',
                                order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
                                amount=D('1'), price=D('20'), creation_timestamp=1000).to_json()
        with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
            if surface == 'tracker':
                row = sql.query(MarketState).one_or_none()
                if row is None:
                    row = MarketState(config_file_path=session.recorder.config_file_path, market=session._connector.display_name, timestamp=1000)
                    sql.add(row)
                row.saved_state = {'schema': 'native_spot_paper_v1', 'orders': {'uncertain-submit': pending}}
            else:
                key, value = (('v2-submitted-order:uncertain-submit', {'order': pending}) if surface == 'submission'
                              else ('v2-supervised-orders:fixture', {'orders': [pending]}))
                sql.add(Metadata(key=key, value=json.dumps({'owner': session.recorder.config_file_path, **value})))
        owner._native_source_hash = 'f' * 64
        new = NativeFundingStore(owner, session.recorder)
        with pytest.raises(ValueError, match='unresolved durable submission'):
            new.plan_binding_transition(old.binding)
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['fill_price', 'fill_base_amount'])
async def test_terminal_snapshot_fill_contradiction_blocks_transition(tmp_path, field):
    import copy

    from hummingbot.model.executors import Executors
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'contradiction.sqlite')
    try:
        await fixtures.NativePaperFundingTests.seed(None, session)
        owner = next(iter(session.controllers.values()))
        old = owner._funding_store
        executor = session.executor_orchestrator.active_executors[owner.config.id][0]
        await session.step(600061, trades=[dict(trading_pair='BTC-USDC', sequence=1, timestamp=600061,
                                                side=TradeType.SELL, price=executor.config.entry_price, amount=D('.1'))])
        await session.close()
        owner.executors_info = session.recorder.get_all_executors()
        with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
            row = sql.get(Executors, executor.config.id)
            custom = copy.deepcopy(row.custom_info)
            changed = 0
            for snapshot in custom['execution_order_snapshots']:
                for trade in snapshot['order_fills'].values():
                    trade[field] = str(D(trade[field]) + D('.01'))
                    changed += 1
            assert changed
            row.custom_info = custom
        owner._native_source_hash = '1' * 64
        new = NativeFundingStore(owner, session.recorder)
        with pytest.raises(ValueError, match='fill.*(contradict|differs)|unrecorded fills'):
            new.plan_binding_transition(old.binding)
        with session.recorder.sql_manager.get_new_session() as sql:
            assert old.binding in sql.get(Metadata, old.key).value
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior
