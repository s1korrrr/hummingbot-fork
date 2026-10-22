"""Offline native executor/recorder/shared-funding acceptance, not economic research."""
import asyncio
import json
from decimal import Decimal as D
from test.hummingbot.strategy_v2.paper.test_paper_session import causal_reversal_frames

import pytest

from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from controllers.generic.rsi_modular import get_config_class
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.strategy_v2.paper_session import NativePaperSession


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_transition", [False, True])
async def test_native_replacement_base_fees_shared_reserve_and_next_pair_admission(tmp_path, legacy_transition):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    pairs = ('BTC-USDC', 'XRP-USDC')
    rules = {pair: TradingRule(pair, min_order_size=D('.01'), min_price_increment=D('.01'),
                               min_base_amount_increment=D('.01'), min_notional_size=D('1')) for pair in pairs}
    connector = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
        maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002'),
        buy_percent_fee_deducted_from_returns=True), trade_participation=D('1'))
    connector.set_balance('USDC', D('1000'))
    configs = [get_config_class('ok_rsi')(
        profile='ok_rsi', id='modular_spot_chain_' + pair[:3].lower(), trading_pair=pair,
        connector_name='okx_paper_trade', execution_mode='paper', candles_connector='okx',
        execution_enabled=pair.startswith('BTC'), min_signal_score=1, strict_hmm_mode=False,
        trend_confirmation_enabled=False, dynamic_position_sizing=False, usd_per_entry=D('40'),
        entry_acquisition_policy='down_only_maker_v1', entry_activation_bound_pct=D('.01'), entry_reprice_min_ticks=1, entry_reprice_atr_fraction=D('.01'),
        funding=dict(allocation_quote='100', fee_reserve_fraction='.003', scope='strategy', max_position_quote='70'),
    ) for pair in pairs]
    session = NativePaperSession(connector, configs, tmp_path / 'chain.sqlite')
    try:
        for index, frame in enumerate(causal_reversal_frames()):
            now = 600000 + index * 60
            await session.step(now, books=[dict(trading_pair=p, sequence=2 + index, timestamp=now,
                                                bids=[(90, 10)], asks=[(90.01, 10)]) for p in pairs],
                               candles=[dict(connector_name='okx', trading_pair=p, interval='1m',
                                             frame=frame, available_at=now) for p in pairs])
        owner = session.controllers[configs[0].id]
        executor = session.executor_orchestrator.active_executors[owner.config.id][0]
        original_id = executor._open_order.order_id
        assert executor.config.entry_chain_contract == 'v2_bounded_acquisition_v1'
        saw_reserved_handoff = False
        for timestamp, sequence in [(600066, 4), (600067, 5), (600068, 6)]:
            await session.step(timestamp, books=[dict(trading_pair='BTC-USDC', sequence=sequence,
                                                      timestamp=timestamp, bids=[(89.90, 10)], asks=[(90.01, 10)]),
                                                 dict(trading_pair='XRP-USDC', sequence=sequence, timestamp=timestamp, bids=[(90, 10)], asks=[(90.01, 10)])])
            await asyncio.sleep(0)
            if executor._entry_child_submission_authorized:
                saw_reserved_handoff = True
                assert executor._open_order is None
                assessment = owner._funding_store.assess()
                expected_reserve = executor.config.amount * executor.config.entry_price * D('1.003')
                assert D(assessment['pending_quote']) == expected_reserve
                next_owner = session.controllers[configs[1].id]
                too_large = next_owner.get_executor_config(TradeType.BUY, D('90'), D('.70'))
                too_large.amount = D('.70')
                with pytest.raises(ValueError, match='allocation exhausted'):
                    NativeFundingStore(next_owner, session.recorder).admit(too_large)
        assert saw_reserved_handoff
        assert executor._open_order is not None, executor.get_custom_info()['entry_acquisition']
        assert executor._open_order.order_id != original_id, (executor.get_custom_info(
        )['entry_acquisition'], executor._open_order.order.to_json() if executor._open_order.order else None, executor._entry_lifecycle)
        pending = owner._funding_store.assess()
        assert D(pending['pending_quote']) > 0
        assert not executor.get_custom_info()['acquisition_chain']['membership_closed']
        replacement = executor._open_order
        price = replacement.order.price
        amount = executor.config.amount
        for sequence, quantity in [(1, D('.10')), (2, amount - D('.10'))]:
            await session.step(600069 + sequence, trades=[dict(trading_pair='BTC-USDC', sequence=sequence,
                                                               timestamp=600069 + sequence, side=TradeType.SELL, price=price, amount=quantity)])
        for sequence, timestamp in enumerate((600072, 600073, 600074), 7):
            await session.step(timestamp, books=[dict(trading_pair=p, sequence=sequence, timestamp=timestamp,
                                                      bids=[(89.90 if p.startswith('BTC') else 90, 10)], asks=[(90.01, 10)]) for p in pairs])
            await asyncio.sleep(0)
        assert executor.open_filled_amount == amount * D('.999')
        assert executor._cumulative_entry_filled_base() == amount
        assert len(executor._entry_orders()) == 2
        assert len(replacement.order.order_fills) == 2
        status = owner._funding_store.reconcile()
        assert status['released_intents'] == 1
        assert D(status['pending_quote']) == 0
        if legacy_transition:
            await assert_explicit_legacy_transition_keeps_claim_unreleased(session, owner, executor)
            return
        xrp = session.controllers[configs[1].id]
        store = NativeFundingStore(xrp, session.recorder)
        proposed = xrp.get_executor_config(TradeType.BUY, D('90'), D('.30'))
        assert store.admit(proposed)
        _, state = store._load()
        assert len(state['claims']) == 2
        assert executor.config.id in state['releases']
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_peer_child_awaiting_exchange_ack_does_not_refuse_another_pair(tmp_path):
    """A child is recorded when `place_order` returns its id; its native order object
    arrives only with the created event after the exchange acknowledges it (seconds on
    OKX). During that window another pair's admission must still be decided on capacity
    (2026-10-05 14:36: BTC refused 'native child coverage differs' while SOL/XRP children
    awaited their acks)."""
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    pairs = ('BTC-USDC', 'XRP-USDC')
    rules = {pair: TradingRule(pair, min_order_size=D('.01'), min_price_increment=D('.01'),
                               min_base_amount_increment=D('.01'), min_notional_size=D('1')) for pair in pairs}
    connector = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
        maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002'),
        buy_percent_fee_deducted_from_returns=True), trade_participation=D('1'))
    connector.set_balance('USDC', D('1000'))
    configs = [get_config_class('ok_rsi')(
        profile='ok_rsi', id='modular_spot_ack_' + pair[:3].lower(), trading_pair=pair,
        connector_name='okx_paper_trade', execution_mode='paper', candles_connector='okx',
        execution_enabled=pair.startswith('BTC'), min_signal_score=1, strict_hmm_mode=False,
        trend_confirmation_enabled=False, dynamic_position_sizing=False, usd_per_entry=D('40'),
        entry_acquisition_policy='down_only_maker_v1', entry_activation_bound_pct=D('.01'), entry_reprice_min_ticks=1,
        entry_reprice_atr_fraction=D('.01'),
        funding=dict(allocation_quote='100', fee_reserve_fraction='.003', scope='strategy', max_position_quote='70'),
    ) for pair in pairs]
    session = NativePaperSession(connector, configs, tmp_path / 'ack.sqlite')
    try:
        for index, frame in enumerate(causal_reversal_frames()):
            now = 600000 + index * 60
            await session.step(now, books=[dict(trading_pair=p, sequence=2 + index, timestamp=now,
                                                bids=[(90, 10)], asks=[(90.01, 10)]) for p in pairs],
                               candles=[dict(connector_name='okx', trading_pair=p, interval='1m',
                                             frame=frame, available_at=now) for p in pairs])
        btc = session.executor_orchestrator.active_executors[configs[0].id][0]
        assert btc.config.entry_chain_contract == 'v2_bounded_acquisition_v1'
        assert btc._open_order is not None and btc._open_order.order is not None
        btc._open_order.order = None  # Submitted, id recorded, created event not yet received.
        # The next strategy tick publishes that state in the controller's executor report.
        session.controllers[configs[0].id].executors_info = [btc.executor_info]
        chain = btc.get_custom_info()['acquisition_chain']
        assert [child['order_id'] for child in chain['children']] == [btc._open_order.order_id]
        assert not chain['membership_closed']

        xrp = session.controllers[configs[1].id]
        store = NativeFundingStore(xrp, session.recorder)
        assert D(store.assess()['pending_quote']) == btc.config.amount * btc.config.entry_price * D('1.003')
        too_large = xrp.get_executor_config(TradeType.BUY, D('90'), D('.70'))
        too_large.amount = D('.70')
        with pytest.raises(ValueError, match='allocation exhausted'):
            store.admit(too_large)  # The in-flight parent still holds its whole authorization.
        assert store.admit(xrp.get_executor_config(TradeType.BUY, D('90'), D('.30')))
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous


async def assert_explicit_legacy_transition_keeps_claim_unreleased(session, owner, executor):
    """Construct old-version facts in a disposable DB, never a live migration."""
    from controllers.generic.modular_spot_core.native_funding import _config_identity
    from hummingbot.strategy_v2.executors.position_executor.data_types import parse_position_config

    def row():
        with session.recorder.sql_manager.get_new_session() as sql:
            raw = dict(sql.connection().exec_driver_sql(
                'SELECT * FROM Executors WHERE id=?', (executor.config.id,)).mappings().one())
        for key in ('config', 'custom_info'):
            raw[key] = json.loads(raw[key])
        return raw

    def historical(record):
        record['config'].pop('entry_chain_contract', None)
        custom = record['custom_info']
        custom.pop('acquisition_chain', None)
        custom.pop('entry_child_lifecycles', None)
        custom['execution_order_snapshots'][0]['cancel_source'] = '31'
        custom['entry_lifecycle'].update(cancel_source='31', cancel_event_timestamp=custom['execution_order_snapshots'][0]['last_update_timestamp'], replacement_purpose='post_only_repair',
                                         replacement_of_order_id=custom['order_ids'][0], order_id=custom['order_ids'][1], reprice_count=1)
        return record

    before = historical(row())
    old = owner._funding_store
    old_binding = old.binding
    await session.close()
    stopped = historical(row())
    assert before['custom_info']['execution_order_snapshots'] == stopped['custom_info']['execution_order_snapshots']
    with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
        sql.connection().exec_driver_sql('UPDATE Executors SET config=?, custom_info=? WHERE id=?',
                                         (json.dumps(stopped['config']), json.dumps(stopped['custom_info']), executor.config.id))
    _, state = old._load()
    original_claim = dict(state['claims'][executor.config.id])
    original_claim['config'] = _config_identity(parse_position_config(stopped['config']), owner.config.id)
    state['claims'][executor.config.id] = original_claim
    state['releases'].pop(executor.config.id, None)
    # Build an old-version test fixture from the recorded native session. The
    # production immutable writer correctly refuses rewriting a live claim.
    from hummingbot.model.metadata import Metadata
    with session.recorder.sql_manager.get_new_session() as sql, sql.begin():
        for item in sql.query(Metadata).all():
            if item.key == old.key or item.key.startswith(old.key + ':'):
                sql.delete(item)
    assert old._write(None, state)
    for peer in session.controllers.values():
        peer.executors_info = session.recorder.get_executors_by_controller(peer.config.id)
    owner._native_source_hash = 'a' * 64
    upgraded = NativeFundingStore(owner, session.recorder)
    # Without legacy evidence the plan reconciles normally: the merged
    # single-filled-order rule releases the legacy set on its own.
    plain = upgraded.plan_binding_transition(old_binding)
    assert executor.config.id in plain['state']['releases']
    evidence = dict(executor_id=executor.config.id, order_ids=before['custom_info']['order_ids'], preflight=before)
    proposal = upgraded.plan_binding_transition(old_binding, legacy_evidence=evidence)
    assert proposal['legacy_retained_unreleased']['executor_id'] == executor.config.id
    assert executor.config.id not in proposal['state']['releases']
    upgraded.apply_binding_transition(proposal['transition_id'])
    _, current = upgraded._load()
    assert current['claims'][executor.config.id] == original_claim
    assert executor.config.id not in current['releases']
    # Ordinary reconciliation then releases the legacy set under the merged
    # single-filled-order rule; the transition itself never released it.
    assert upgraded.reconcile()['released_intents'] == 1
    _, released = upgraded._load()
    assert executor.config.id in released['releases']
