"""Allocation-only held migration preserves native sell state and refuses drift."""
import json
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_native_funding import fill
from test_native_lifecycle import recorder as _recorder_fixture

from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
from controllers.generic.modular_spot_core.domain import PolicyError, identity
from controllers.generic.modular_spot_core.native_recovery import (
    HeldIdentity,
    NativeTrail,
    PendingReduction,
    RecoveryCheckpoint,
    allocation_transition_held,
    decode,
    encode,
)
from controllers.generic.modular_spot_core.recipe_contract import configuration_digest
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.model.position import Position
from hummingbot.model.position_hold_ledger import PositionHoldLedger
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig

recorder = _recorder_fixture


def seeded_held(recorder, monkeypatch):
    old = ModularOKRSIControllerConfig(
        id='modular_spot_held_transition', connector_name='okx',
        trading_pair='BTC-USDC', execution_enabled=True,
        funding={'allocation_quote': '500', 'max_position_quote': '500',
                 'fee_reserve_fraction': '.003'})
    payload = old.model_dump(mode='json')
    payload['allocation_mode'] = 'wallet_funded'
    payload['funding']['allocation_quote'] = None
    payload['funding']['max_position_quote'] = None
    target = type(old).model_validate(payload)
    prior_digest = configuration_digest(old.model_dump(mode='json'))
    target_digest = configuration_digest(target.model_dump(mode='json'))
    prior_source, target_source = 'a' * 64, 'b' * 64
    owner = SimpleNamespace(config=target, _composition_digest=target_digest, _native_source_hash=target_source)
    sell_config = PositionExecutorConfig(id='sell-1', timestamp=2, connector_name='okx',
                                         trading_pair='BTC-USDC', side=TradeType.SELL,
                                         amount=D('.5'), entry_price=D('105'))
    sell_info = SimpleNamespace(id='sell-1', config=sell_config)
    monkeypatch.setattr(recorder, 'get_executors_by_controller', lambda _: [sell_info])
    sell_fingerprint = identity(sell_config.model_dump(mode='json', exclude={'controller_id'}))
    key = 'okx|BTC-USDC|BUY|100.00000000'
    checkpoint = RecoveryCheckpoint(family='ok_rsi', configuration_sha256=prior_digest,
                                    source_sha256=prior_source, sequence=912,
                                    positions=(HeldIdentity(position_key=key, amount=D('1'), breakeven_price=D('100')),),
                                    trails=(NativeTrail(position_key=key, armed_timestamp=10, arm_price=D('101'),
                                                        peak_price=D('108'), tracked_amount=D('1'), peak_rsi=82,
                                                        target_profit_pct=D('.03'), last_reason='peak',
                                                        partial_exit_done=True),),
                                    pending_reductions=(PendingReduction(position_key=key, executor_id='sell-1',
                                                                         configuration_sha256=sell_fingerprint,
                                                                         prior_peak=D('108')),))
    held_key = 'modular-native-held:v1:' + identity([
        recorder.config_file_path, old.id, old.connector_name, old.trading_pair])
    trade = fill('buy-1', '1', 'BUY', '1', '100')
    record = ExecutorOrchestrator._trade_fill_order_record(trade)
    record['client_order_id'] = 'buy-1'
    record['exchange_order_id'] = 'exchange-buy-1'
    receipt = record['order_fills']['1']
    receipt.update(trade_id='1', client_order_id='buy-1', exchange_order_id='exchange-buy-1',
                   trading_pair='BTC-USDC')
    ledger = PositionHoldLedger(id='held-ledger-1', controller_id=old.id, executor_id='buy-executor',
                                connector_name='okx', trading_pair='BTC-USDC', position_side=None,
                                order_side='BUY', timestamp=1000, client_order_id='buy-1',
                                exchange_order_id='exchange-buy-1', price=D('100'), amount=D('1'),
                                executed_amount_base=D('1'), executed_amount_quote=D('100'),
                                cumulative_fee_paid_quote=D('0'),
                                order_payload=json.loads(json.dumps(record, default=str)))
    position = Position(id='held-position-1', controller_id=old.id, connector_name='okx',
                        trading_pair='BTC-USDC', side='BUY', timestamp=1000,
                        volume_traded_quote=D('100'), amount=D('1'), breakeven_price=D('100'),
                        unrealized_pnl_quote=D('0'), realized_pnl_quote=D('0'), cum_fees_quote=D('0'))
    with recorder.sql_manager.get_new_session() as session, session.begin():
        session.add_all([Metadata(key=held_key, value=encode(checkpoint)), ledger, position])
    monkeypatch.setattr(recorder, 'get_trades_for_config', lambda _: [trade])
    return owner, held_key, checkpoint, prior_digest, prior_source, trade


def test_allocation_held_identity_carry_preserves_trail_reduction_and_sequence(recorder, monkeypatch):
    owner, held_key, checkpoint, prior_digest, prior_source, _ = seeded_held(recorder, monkeypatch)
    with recorder.sql_manager.get_new_session() as session:
        evidence = allocation_transition_held(session, owner, recorder, prior_digest, prior_source)
    after = decode(evidence['after_raw'])
    assert evidence['key'] == held_key
    assert decode(evidence['before_raw']) == checkpoint
    assert after.sequence == checkpoint.sequence
    assert after.positions == checkpoint.positions
    assert after.trails == checkpoint.trails
    assert after.pending_reductions == checkpoint.pending_reductions
    assert after.configuration_sha256 == owner._composition_digest
    assert after.source_sha256 == owner._native_source_hash


def test_missing_held_checkpoint_requires_proven_empty_native_scope(recorder, monkeypatch):
    owner, held_key, _, prior_digest, prior_source, _ = seeded_held(recorder, monkeypatch)
    with recorder.sql_manager.get_new_session() as session, session.begin():
        session.delete(session.get(Metadata, held_key))
    with recorder.sql_manager.get_new_session() as session:
        with pytest.raises(PolicyError, match='uncheckpointed native inventory'):
            allocation_transition_held(session, owner, recorder, prior_digest, prior_source)
    with recorder.sql_manager.get_new_session() as session, session.begin():
        session.delete(session.get(Position, 'held-position-1'))
        session.delete(session.get(PositionHoldLedger, 'held-ledger-1'))
    with recorder.sql_manager.get_new_session() as session:
        evidence = allocation_transition_held(session, owner, recorder, prior_digest, prior_source)
    assert evidence['before_raw'] is evidence['after_raw'] is None


@pytest.mark.parametrize('fault', ['missing_executor', 'changed_executor'])
def test_pending_reduction_requires_exact_existing_native_sell_intent(recorder, monkeypatch, fault):
    owner, _, _, prior_digest, prior_source, _ = seeded_held(recorder, monkeypatch)
    if fault == 'missing_executor':
        monkeypatch.setattr(recorder, 'get_executors_by_controller', lambda _: [])
    else:
        changed = PositionExecutorConfig(id='sell-1', timestamp=2, connector_name='okx',
                                         trading_pair='BTC-USDC', side=TradeType.SELL,
                                         amount=D('.6'), entry_price=D('105'))
        monkeypatch.setattr(recorder, 'get_executors_by_controller',
                            lambda _: [SimpleNamespace(id='sell-1', config=changed)])
    with recorder.sql_manager.get_new_session() as session:
        with pytest.raises((PolicyError, ValueError), match='reduction receipt'):
            allocation_transition_held(session, owner, recorder, prior_digest, prior_source)


@pytest.mark.parametrize('drift', ['source', 'configuration', 'family', 'scope', 'inventory', 'fill'])
def test_allocation_held_identity_carry_refuses_unproved_state(recorder, monkeypatch, drift):
    owner, held_key, checkpoint, prior_digest, prior_source, trade = seeded_held(recorder, monkeypatch)
    with recorder.sql_manager.get_new_session() as session, session.begin():
        if drift == 'source':
            session.get(Metadata, held_key).value = encode(checkpoint.model_copy(update={'source_sha256': 'd' * 64}))
        elif drift == 'configuration':
            session.get(Metadata, held_key).value = encode(checkpoint.model_copy(update={'configuration_sha256': 'd' * 64}))
        elif drift == 'family':
            session.get(Metadata, held_key).value = encode(checkpoint.model_copy(update={'family': 'rsi_v5'}))
        elif drift == 'scope':
            changed = checkpoint.model_copy(update={
                'positions': (checkpoint.positions[0].model_copy(update={'position_key': 'foreign'}),),
                'trails': (), 'pending_reductions': ()})
            session.get(Metadata, held_key).value = encode(changed)
        elif drift == 'inventory':
            session.get(Position, 'held-position-1').amount = D('2')
        else:
            trade.exact_price = '101'
    with recorder.sql_manager.get_new_session() as session:
        with pytest.raises((PolicyError, ValueError)):
            allocation_transition_held(session, owner, recorder, prior_digest, prior_source)
