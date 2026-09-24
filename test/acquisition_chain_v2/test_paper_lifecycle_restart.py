"""Whole-lifecycle native paper acceptance for one bounded V2 acquisition chain.

Real orchestrator, recorder, funding store, controllers and executors run
against the offline paper venue. Restarts are modelled two ways:

* clean: ``session.close()`` (early stop, keep position) then ``restore=True``;
* crash: a consistent SQLite snapshot taken between steps at every distinct
  chain phase, restored into a fresh session as if the process had died there.

Neither path may create a BUY, lose a fill, or change persisted chain facts.
"""
import asyncio
import json
import shutil
import sqlite3
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper.test_paper_funding import candles_for_second_reversal, synthetic_candles
from test.hummingbot.strategy_v2.paper.test_paper_session import causal_reversal_frames

import pytest

from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from controllers.generic.rsi_modular import get_config_class
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.strategy_v2.executors.acquisition_chain import validate_acquisition_chain
from hummingbot.strategy_v2.paper_session import NativePaperSession

PAIRS = ('BTC-USDC', 'XRP-USDC')
CONTRACT = 'v2_bounded_acquisition_v1'


def two_pair_paper():
    rules = {pair: TradingRule(pair, min_order_size=D('.01'), min_price_increment=D('.01'),
                               min_base_amount_increment=D('.01'), min_notional_size=D('1')) for pair in PAIRS}
    connector = NativePaperTradeExchange(rules, fee_schema=TradeFeeSchema(
        maker_percent_fee_decimal=D('.001'), taker_percent_fee_decimal=D('.002'),
        buy_percent_fee_deducted_from_returns=True), trade_participation=D('1'))
    connector.set_balance('USDC', D('1000'))
    return connector


def chain_configs():
    return [get_config_class('ok_rsi')(
        profile='ok_rsi', id='modular_spot_chain_' + pair[:3].lower(), trading_pair=pair,
        connector_name='okx_paper_trade', execution_mode='paper', candles_connector='okx',
        execution_enabled=pair.startswith('BTC'), min_signal_score=1, strict_hmm_mode=False,
        trend_confirmation_enabled=False, dynamic_position_sizing=False, usd_per_entry=D('40'),
        entry_acquisition_policy='down_only_maker_v1', entry_activation_bound_pct=D('.01'),
        entry_reprice_min_ticks=1, entry_reprice_atr_fraction=D('.01'),
        funding=dict(allocation_quote='100', fee_reserve_fraction='.003', scope='strategy', max_position_quote='70'),
    ) for pair in PAIRS]


def books(timestamp, sequence, *, btc_bid=90, xrp_bid=90):
    def side(bid):
        return dict(bids=[(bid, 10)], asks=[(max(90.01, round(bid + .1, 2)), 10)])
    return [dict(trading_pair='BTC-USDC', sequence=sequence, timestamp=timestamp, **side(btc_bid)),
            dict(trading_pair='XRP-USDC', sequence=sequence, timestamp=timestamp, **side(xrp_bid))]


def executor_row(db, executor_id):
    with sqlite3.connect(db) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute('SELECT * FROM Executors WHERE id=?', (executor_id,)).fetchone()
    result = dict(row)
    for key in ('config', 'custom_info'):
        result[key] = json.loads(result[key])
    return result


def snapshot_database(db, target_dir):
    """Consistent copy of the live session database as a crash would leave it."""
    target_dir.mkdir()
    target = target_dir / db.name
    with sqlite3.connect(db) as source, sqlite3.connect(target) as destination:
        source.backup(destination)
    shutil.copy(db.with_suffix(db.suffix + '.paper-session.json'),
                target.with_suffix(target.suffix + '.paper-session.json'))
    return target


class ChainRun:
    """Drive the BTC executor through zero-fill cancel, repair and two fills."""

    def __init__(self, session, tmp_path):
        self.session = session
        self.tmp_path = tmp_path
        self.snapshots = []  # (label, db_path, persisted executor row)
        self._seen = set()
        self.executor = None
        self.owner = None

    def _label(self):
        custom = self.executor.get_custom_info()
        chain = custom.get('acquisition_chain') or {}
        return (custom['entry_lifecycle'].get('state'), len(chain.get('children', [])),
                chain.get('membership_closed'), chain.get('gross_filled_base'))

    def _snapshot(self):
        label = self._label()
        if label in self._seen:
            return
        self._seen.add(label)
        target = snapshot_database(self.db, self.tmp_path / f'crash-{len(self.snapshots)}')
        self.snapshots.append((label, target, executor_row(target, self.executor.config.id)))

    async def step(self, timestamp, **inputs):
        receipt = await self.session.step(timestamp, **inputs)
        await asyncio.sleep(0)
        if self.executor is not None:
            self._snapshot()
        return receipt

    async def run_to_resting_replacement(self):
        session = self.session
        self.db = Path(session.recorder.sql_manager.db_path)
        for index, frame in enumerate(causal_reversal_frames()):
            now = 600000 + index * 60
            await session.step(now, books=[dict(trading_pair=p, sequence=2 + index, timestamp=now,
                                                bids=[(90, 10)], asks=[(90.01, 10)]) for p in PAIRS],
                               candles=[dict(connector_name='okx', trading_pair=p, interval='1m',
                                             frame=frame, available_at=now) for p in PAIRS])
        self.owner = session.controllers['modular_spot_chain_btc']
        self.executor = session.executor_orchestrator.active_executors[self.owner.config.id][0]
        assert self.executor.config.entry_chain_contract == CONTRACT
        self._snapshot()
        original_id = self.executor._open_order.order_id
        for timestamp, sequence in [(600066, 4), (600067, 5), (600068, 6)]:
            await self.step(timestamp, books=books(timestamp, sequence, btc_bid=89.90))
        replacement = self.executor._open_order
        assert replacement is not None and replacement.order_id != original_id
        assert replacement.order.executed_amount_base == 0
        return original_id, replacement

    async def run_to_closed_chain(self):
        original_id, replacement = await self.run_to_resting_replacement()
        price = replacement.order.price
        amount = self.executor.config.amount
        for sequence, quantity in [(1, D('.10')), (2, amount - D('.10'))]:
            await self.step(600069 + sequence, trades=[dict(
                trading_pair='BTC-USDC', sequence=sequence, timestamp=600069 + sequence,
                side=TradeType.SELL, price=price, amount=quantity)])
        for sequence, timestamp in enumerate((600072, 600073, 600074), 7):
            await self.step(timestamp, books=books(timestamp, sequence, btc_bid=89.90))
        assert self.executor._cumulative_entry_filled_base() == amount
        assert self.executor.get_custom_info()['acquisition_chain']['membership_closed']
        return original_id, replacement.order_id


@pytest.fixture
def isolated_recorder():
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    try:
        yield
    finally:
        MarketsRecorder._shared_instance = previous


@pytest.mark.asyncio
async def test_closed_chain_keeps_managing_position_and_exits_once(tmp_path, isolated_recorder):
    """After the chain closes, trailing arms and exits with exactly one SELL of the net inventory."""
    session = NativePaperSession(two_pair_paper(), chain_configs(), tmp_path / 'chain.sqlite')
    try:
        run = ChainRun(session, tmp_path)
        original_id, replacement_id = await run.run_to_closed_chain()
        executor = run.executor
        custom = executor.get_custom_info()
        assert [child['order_id'] for child in custom['acquisition_chain']['children']] == [original_id, replacement_id]
        assert custom['acquisition_outcome'] == 'fully_acquired'
        assert custom['further_acquisition_permission'] == 'closed'
        validate_acquisition_chain(executor.config, custom, executor._entry_reconciliation)
        net = executor.open_filled_amount
        assert net == executor.config.amount * D('.999')
        assert session._connector.get_balance('BTC') == net
        funding = run.owner._funding_store.reconcile()
        assert funding['released_intents'] == 1 and D(funding['pending_quote']) == 0

        # Persisted row already carries the sealed chain while still RUNNING.
        row = executor_row(run.db, executor.config.id)
        assert row['custom_info']['acquisition_chain'] == custom['acquisition_chain']
        assert len(row['custom_info']['entry_reconciliation']['orders']) == 2
        assert row['status'] == 2 and row['is_active'] == 1

        # Trailing arms on a higher bid, then exits on retracement with one SELL.
        buys_before = len([o for o in session._connector._order_tracker.all_orders.values()
                           if o.trade_type is TradeType.BUY])
        for timestamp, sequence, bid in ((600075, 10, 100), (600076, 11, 91), (600077, 12, 91)):
            await run.step(timestamp, books=books(timestamp, sequence, btc_bid=bid))
        await run.step(600078, books=books(600078, 13, btc_bid=91))
        sells = [o for o in session._connector._order_tracker.all_orders.values() if o.trade_type is TradeType.SELL]
        buys_after = len([o for o in session._connector._order_tracker.all_orders.values()
                          if o.trade_type is TradeType.BUY])
        assert buys_after == buys_before
        # The venue step is 0.01 base, so the exit sells the floored net amount and
        # leaves unsellable dust; that dust must not pin the executor open.
        sellable = (net / D('.01')).to_integral_value(rounding='ROUND_FLOOR') * D('.01')
        assert len(sells) == 1 and sells[0].amount == sellable
        assert session._connector.get_balance('BTC') == net - sellable
        assert executor.is_closed
        # The closed executor still reports the full chain, not only the final child.
        final = executor.get_custom_info()
        assert final['acquisition_chain'] == custom['acquisition_chain']
        assert final['order_ids'][:2] == [original_id, replacement_id]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_clean_restart_after_closed_chain_preserves_orders_and_places_no_buy(tmp_path, isolated_recorder):
    db = tmp_path / 'chain.sqlite'
    session = NativePaperSession(two_pair_paper(), chain_configs(), db)
    run = ChainRun(session, tmp_path)
    try:
        original_id, replacement_id = await run.run_to_closed_chain()
        executor = run.executor
        expected_custom = executor.get_custom_info()
        expected_balances = dict(session._connector.get_all_balances())
        expected_fills = session.recorder.get_trades_for_config('native-paper-session')
        assert len(expected_fills) == 2
    finally:
        await session.close()

    stopped = executor_row(db, executor.config.id)
    assert stopped['status'] == 4 and stopped['is_active'] == 0
    assert stopped['custom_info']['acquisition_chain'] == expected_custom['acquisition_chain']
    assert stopped['custom_info']['order_ids'] == [original_id, replacement_id]
    snapshots = {row['client_order_id']: row for row in stopped['custom_info']['execution_order_snapshots']}
    assert InFlightOrder.from_json(snapshots[original_id]).current_state is OrderState.CANCELED
    assert D(snapshots[original_id]['executed_amount_base']) == 0
    assert InFlightOrder.from_json(snapshots[replacement_id]).current_state is OrderState.FILLED
    assert len(snapshots[replacement_id]['order_fills']) == 2
    assert stopped['custom_info']['entry_reconciliation']['orders'][0]['order_id'] == original_id
    assert stopped['config']['entry_chain_contract'] == CONTRACT

    restored = NativePaperSession(two_pair_paper(), chain_configs(), db, restore=True)
    try:
        assert restored._connector.get_all_balances() == expected_balances
        assert restored._connector.in_flight_orders == {}
        rows = restored.recorder.get_all_executors()
        assert [row.id for row in rows] == [executor.config.id]
        assert rows[0].custom_info['acquisition_chain'] == expected_custom['acquisition_chain']
        assert len(restored.recorder.get_trades_for_config('native-paper-session')) == 2
        owner = restored.controllers['modular_spot_chain_btc']
        owner.executors_info = restored.recorder.get_executors_by_controller(owner.config.id)
        status = NativeFundingStore(owner, restored.recorder).reconcile()
        assert status['released_intents'] == 1
        assert D(status['pending_quote']) == 0
        receipt = await restored.step(600080, books=books(600080, 20))
        assert receipt['actions'] == []
        assert restored._connector.in_flight_orders == {}
        assert restored._connector.get_balance('BTC') == expected_balances['BTC']
        assert len(restored.recorder.get_all_executors()) == 1
    finally:
        await restored.close()


@pytest.mark.asyncio
async def test_crash_at_every_chain_phase_holds_without_buy_or_lost_state(tmp_path, isolated_recorder):
    """Restart from a consistent snapshot at each distinct chain phase.

    A process death between steps must leave the persisted executor row intact,
    place the owner into startup reconciliation, and never create a BUY.
    """
    db = tmp_path / 'chain.sqlite'
    session = NativePaperSession(two_pair_paper(), chain_configs(), db)
    run = ChainRun(session, tmp_path)
    try:
        await run.run_to_closed_chain()
        executor_id = run.executor.config.id
        labels = [label for label, _, _ in run.snapshots]
    finally:
        await session.close()
    assert len(run.snapshots) >= 4, labels
    states = [label[0] for label in labels]
    assert 'child_authorized' in states or any(label[1] == 2 for label in labels), labels
    assert labels[-1][2] is True, labels

    for label, snapshot_db, persisted in run.snapshots:
        restored = NativePaperSession(two_pair_paper(), chain_configs(), snapshot_db, restore=True)
        try:
            rows = restored.recorder.get_all_executors()
            assert [row.id for row in rows] == [executor_id], label
            row = rows[0]
            assert row.custom_info['acquisition_chain'] == persisted['custom_info']['acquisition_chain'], label
            assert row.custom_info.get('execution_order_snapshots') == persisted['custom_info'].get('execution_order_snapshots'), label
            assert row.config.entry_chain_contract == CONTRACT, label
            # Unfinished ownership blocks new execution until reconciled.
            orchestrator = restored.executor_orchestrator
            assert orchestrator.recovery_required.get('modular_spot_chain_btc') == [executor_id], (label, orchestrator.recovery_required)
            assert orchestrator.active_executors.get('modular_spot_chain_btc', []) == [], label
            buys_before = [o for o in restored._connector._order_tracker.all_orders.values() if o.trade_type is TradeType.BUY]
            frame = causal_reversal_frames()[1]
            receipt = await restored.step(600100, books=books(600100, 30), candles=[dict(
                connector_name='okx', trading_pair=p, interval='1m', frame=frame, available_at=600100) for p in PAIRS])
            assert receipt['actions'] == [], label
            assert not receipt['readiness']['ready'], label
            assert any('unfinished_native_executors_require_reconciliation' in reason
                       for reason in receipt['readiness']['reasons']), (label, receipt['readiness'])
            buys_after = [o for o in restored._connector._order_tracker.all_orders.values() if o.trade_type is TradeType.BUY]
            assert len(buys_after) == len(buys_before), label
            assert len(restored.recorder.get_all_executors()) == 1, label
            # The persisted row is never rewritten by a held restart.
            assert executor_row(snapshot_db, executor_id)['custom_info']['acquisition_chain'] == persisted['custom_info']['acquisition_chain'], label
        finally:
            await restored.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('partial', [False, True])
async def test_clean_stop_mid_chain_closes_chain_releases_reserve_and_admits_next_entry(tmp_path, isolated_recorder, partial):
    """A clean stop while the replacement rests (or is partly filled) must not leak the reserve.

    Both children end CANCELED (the replacement with zero or partial fill). The chain
    closes without a successor, funding releases the unused commitment, and after
    restart a fresh causal reversal is admitted again. Partial inventory is retained
    as held inventory, never re-bought and never sold by the restart itself.
    """
    db = tmp_path / 'chain.sqlite'
    session = NativePaperSession(two_pair_paper(), chain_configs(), db)
    run = ChainRun(session, tmp_path)
    try:
        original_id, replacement = await run.run_to_resting_replacement()
        executor = run.executor
        if partial:
            await run.step(600070, trades=[dict(
                trading_pair='BTC-USDC', sequence=1, timestamp=600070,
                side=TradeType.SELL, price=replacement.order.price, amount=D('.10'))])
            assert executor._cumulative_entry_filled_base() == D('.10')
        held_before = session._connector.get_balance('BTC')
        assert held_before == (D('.10') * D('.999') if partial else D('0'))
    finally:
        await session.close()

    stopped = executor_row(db, executor.config.id)
    assert stopped['status'] == 4 and stopped['is_active'] == 0
    chain = stopped['custom_info']['acquisition_chain']
    assert [child['order_id'] for child in chain['children']] == [original_id, replacement.order_id]
    assert chain['membership_closed'] is True and chain['successor_pending'] is None
    assert D(chain['gross_filled_base']) == (D('.10') if partial else D('0'))
    assert stopped['custom_info']['acquisition_outcome'] == ('partially_acquired' if partial else 'no_fill')
    assert stopped['custom_info']['further_acquisition_permission'] == 'closed'
    proofs = stopped['custom_info']['entry_reconciliation']['orders']
    assert [proof['state'] for proof in proofs] == ['CANCELED', 'CANCELED']
    assert [D(proof['filled_base']) for proof in proofs] == [D('0'), D('.10') if partial else D('0')]
    validate_acquisition_chain(executor.config, stopped['custom_info'], stopped['custom_info']['entry_reconciliation'])

    restored = NativePaperSession(two_pair_paper(), chain_configs(), db, restore=True)
    try:
        assert restored._connector.in_flight_orders == {}
        assert restored._connector.get_balance('BTC') == held_before
        owner = restored.controllers['modular_spot_chain_btc']
        owner.executors_info = restored.recorder.get_executors_by_controller(owner.config.id)
        status = NativeFundingStore(owner, restored.recorder).reconcile()
        assert status['released_intents'] == 1, status
        assert D(status['pending_quote']) == 0, status
        # A later causal reversal is admitted again: the closed chain consumed no lingering reserve.
        clear = synthetic_candles(600120)
        await restored.step(600180, books=books(600180, 40), candles=[dict(
            connector_name='okx', trading_pair=p, interval='1m', frame=clear, available_at=600180) for p in PAIRS])
        frame = candles_for_second_reversal(602400)
        receipt = await restored.step(602460, books=books(602460, 41), candles=[dict(
            connector_name='okx', trading_pair=p, interval='1m', frame=frame, available_at=602460) for p in PAIRS])
        new = [e for e in restored.executor_orchestrator.active_executors.get(owner.config.id, [])
               if e.config.id != executor.config.id]
        info = owner.get_custom_info()
        assert receipt['readiness']['ready'], receipt['readiness']
        assert info['entry_admission'] == {'state': 'conditional', 'reason': None}, info['entry_admission']
        if partial:
            # Retained inventory from the partial fill is a held bag above the
            # current mid; the strategy's own average-entry gap refuses a re-buy.
            # Funding and admission are open; only the strategy rule holds.
            assert new == [], receipt['actions']
            assert info['buy_gate'] == 'avg_entry_gap', info['buy_gate']
            assert len(restored.recorder.get_all_executors()) == 1
        else:
            assert len(new) == 1, (receipt['actions'], info.get('buy_gate'), owner._get_entry_decision_gate().store.load())
            assert new[0].config.side is TradeType.BUY
            assert new[0].config.entry_chain_contract == CONTRACT
            assert len(restored.recorder.get_all_executors()) == 2
        # The old chain row is untouched by the new admission.
        assert executor_row(db, executor.config.id)['custom_info']['acquisition_chain'] == chain
    finally:
        await restored.close()
