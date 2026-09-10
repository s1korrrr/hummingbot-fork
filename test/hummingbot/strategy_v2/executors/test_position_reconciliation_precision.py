"""Exact held inventory must survive periodic reports across legacy SQLite receipts."""
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator

D = Decimal


def receipt(order, trade, amount, side, fee='0'):
    return {'trade_id': trade, 'client_order_id': order, 'exchange_order_id': order + '-exchange',
            'trading_pair': 'BTC-USDC', 'fill_base_amount': amount, 'fill_price': '78000',
            'fill_quote_amount': str(D(amount) * D('78000')), 'fee': {
                'percent': '0', 'percent_token': None, 'flat_fees': [{'token': 'BTC', 'amount': fee}]}}


def fixtures():
    receipts = [('buy', 'buy-1', '0.00011', 'BUY', '0.000000088'),
                ('buy', 'buy-2', '0.00002824', 'BUY', '0.000000022592'),
                ('sell', 'sell-1', '0.00013812', 'SELL', '0')]
    fills, entries = [], []
    for order, trade, amount, side, fee in receipts:
        record = receipt(order, trade, amount, side, fee)
        fills.append(TradeFill(config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                               symbol='BTC-USDC', base_asset='BTC', quote_asset='USDC', timestamp=len(fills), order_id=order,
                               exchange_trade_id=trade, trade_type=side, order_type='LIMIT', leverage=1, position='NIL',
                               amount=D(int(D(amount) * 1000000)) / 1000000, price=D('78000'),
                               trade_fee=record['fee'], trade_fee_in_quote=D('0')))
    for order, side in [('buy', 'BUY'), ('sell', 'SELL')]:
        selected = {trade: receipt(o, trade, amount, s, fee) for o, trade, amount, s, fee in receipts if o == order}
        base = sum((D(fill['fill_base_amount']) for fill in selected.values()), D('0'))
        payload = {'client_order_id': order, 'exchange_order_id': order + '-exchange', 'trade_type': side,
                   'executed_amount_base': str(base), 'executed_amount_quote': str(base * D('78000')),
                   'cumulative_fee_paid_quote': '0', 'order_fills': selected}
        entries.append(SimpleNamespace(controller_id='btc', connector_name='okx', trading_pair='BTC-USDC',
                                       position_side=None, order_side=side, timestamp=0 if side == 'BUY' else 2, client_order_id=order, exchange_order_id=order + '-exchange',
                                       order_payload=payload, to_order_record=lambda value=payload: value))
    return entries, fills


def make_orchestrator(entries, fills):
    recorder = MagicMock(spec=MarketsRecorder)
    recorder.config_file_path = 'owner.yml'
    recorder.get_all_executors.return_value = []
    recorder.get_all_positions.return_value = []
    recorder.get_all_position_hold_ledger_entries.return_value = entries
    recorder.get_trades_for_config.return_value = fills
    strategy = MagicMock()
    strategy.controllers = {'btc': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='BTC-USDC'))}
    strategy.markets = {'okx': {'BTC-USDC'}}
    strategy.market_data_provider.get_price_by_type.return_value = D('78000')
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        orchestrator = ExecutorOrchestrator(strategy)
    return orchestrator, recorder


def test_periodic_public_reports_preserve_exact_ledger_dust_side_and_amount():
    entries, fills = fixtures()
    orchestrator, recorder = make_orchestrator(entries, fills)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        for _ in range(2):
            report = orchestrator.get_all_reports()['btc']
            assert report['positions'][0].side == TradeType.BUY
            assert report['positions'][0].amount == D('0.000000009408')


def test_partial_receipts_raise_before_replacing_existing_exact_position():
    entries, fills = fixtures()
    orchestrator, recorder = make_orchestrator(entries, fills)
    previous = orchestrator.positions_held['btc'][0]
    recorder.get_trades_for_config.return_value = fills[:-1]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()
    assert orchestrator.positions_held['btc'][0] is previous


def test_orphan_legacy_fill_is_unavailable_without_inventing_inventory():
    _, fills = fixtures()
    orchestrator, recorder = make_orchestrator([], fills)
    assert orchestrator.positions_held['btc'] == []
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()


def exact_fill(order, side, amount):
    return TradeFill(config_file_path='owner.yml', strategy='v2_with_controllers', market='okx',
                     symbol='BTC-USDC', base_asset='BTC', quote_asset='USDC', timestamp=10, order_id=order,
                     exchange_trade_id=order + '-fill', trade_type=side, order_type='LIMIT', leverage=1, position='NIL',
                     amount=D(int(D(amount) * 1000000)) / 1000000, price=D('78000'),
                     exact_amount=amount, exact_price='78000', exact_trade_fee_in_quote='0',
                     exact_receipt_source='order_filled_event_v1', trade_fee={'percent': '0', 'flat_fees': []},
                     trade_fee_in_quote=D('0'))


def test_complete_exact_fills_support_new_inventory_and_repeated_reports():
    fills = [exact_fill('first-buy', 'BUY', '1.000000009'), exact_fill('later-sell', 'SELL', '1')]
    orchestrator, recorder = make_orchestrator([], fills)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        for _ in range(2):
            summary = orchestrator.get_all_reports()['btc']['positions'][0]
            assert summary.side == TradeType.BUY and summary.amount == D('0.000000009')


def test_exact_new_order_adds_to_ledger_inventory_without_freezing_scope():
    entries, fills = fixtures()
    orchestrator, recorder = make_orchestrator(entries, fills)
    recorder.get_trades_for_config.return_value = fills + [exact_fill('new-buy', 'BUY', '0.01')]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        summary = orchestrator.get_all_reports()['btc']['positions'][0]
    assert summary.side == TradeType.BUY and summary.amount == D('0.010000009408')


def test_missing_exact_buy_cannot_resurrect_short_after_flat_roundtrip():
    fills = [exact_fill('first-buy', 'BUY', '1'), exact_fill('later-sell', 'SELL', '1')]
    orchestrator, recorder = make_orchestrator([], fills)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        assert orchestrator.get_all_reports()['btc']['positions'] == []
        prior = orchestrator.positions_held['btc'][0]
        recorder.get_trades_for_config.return_value = fills[1:]
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()
    assert orchestrator.positions_held['btc'][0] is prior


@pytest.mark.parametrize('change', ['empty', 'duplicate', 'wrong_encoding', 'wrong_fee', 'internal_identity',
                                    'partial_exact', 'orphan_legacy', 'read_failure', 'missing_quote'])
def test_incomplete_or_conflicting_accounting_is_atomic_unavailable(change):
    entries, fills = fixtures()
    orchestrator, recorder = make_orchestrator(entries, fills)
    prior = orchestrator.positions_held['btc'][0]
    if change == 'empty':
        recorder.get_trades_for_config.return_value = []
    elif change == 'duplicate':
        recorder.get_trades_for_config.return_value = fills + [fills[0]]
    elif change == 'wrong_encoding':
        fills[1].amount = D('0.000029')
    elif change == 'wrong_fee':
        fills[0].trade_fee = {'percent': '0', 'flat_fees': []}
    elif change == 'internal_identity':
        entries[0].order_payload['order_fills']['buy-1']['trade_id'] = 'other'
    elif change == 'partial_exact':
        fills[0].exact_amount = '0.00011'
    elif change == 'orphan_legacy':
        orphan = exact_fill('orphan', 'BUY', '1')
        orphan.exact_amount = orphan.exact_price = orphan.exact_trade_fee_in_quote = orphan.exact_receipt_source = None
        recorder.get_trades_for_config.return_value = fills + [orphan]
    elif change == 'read_failure':
        recorder.get_trades_for_config.side_effect = OSError('read failed')
    elif change == 'missing_quote':
        del entries[0].order_payload['order_fills']['buy-1']['fill_quote_amount']
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()
    assert orchestrator.positions_held['btc'][0] is prior


def test_report_validity_gate_recovers_only_after_complete_receipts_return():
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    entries, fills = fixtures()
    orchestrator, recorder = make_orchestrator(entries, fills)
    orchestrator.recovery_required['unrelated-controller'] = ['persisted-unfinished']
    controller = MagicMock()
    harness = SimpleNamespace(executor_orchestrator=orchestrator, controllers={'btc': controller},
                              logger=lambda: MagicMock(), _controller_reports_valid=False)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        StrategyV2Base.update_executors_info(harness)
        assert harness._controller_reports_valid is True
        recorder.get_trades_for_config.return_value = []
        StrategyV2Base.update_executors_info(harness)
        assert harness._controller_reports_valid is False
        recorder.get_trades_for_config.return_value = fills
        StrategyV2Base.update_executors_info(harness)
        assert harness._controller_reports_valid is True
    assert orchestrator.recovery_required == {'unrelated-controller': ['persisted-unfinished']}


def test_ledger_replay_order_preserves_cost_basis_when_native_fills_interleave():
    events = [('A', 'a1', 'BUY', '1', '100', 1000), ('C', 'c1', 'SELL', '1', '250', 2000),
              ('B', 'b1', 'BUY', '1', '300', 2500), ('A', 'a2', 'BUY', '1', '200', 3000)]
    native, payloads = [], {}
    for order, trade, side, amount, price, timestamp in events:
        fill = exact_fill(order, side, amount)
        fill.exchange_trade_id = trade
        fill.price = D(price)
        fill.exact_price = price
        fill.timestamp = timestamp
        native.append(fill)
        payload = payloads.setdefault(order, {'client_order_id': order, 'exchange_order_id': order + '-exchange',
                                              'trade_type': side, 'executed_amount_base': D('0'), 'executed_amount_quote': D('0'),
                                              'cumulative_fee_paid_quote': '0', 'order_fills': {}})
        update = receipt(order, trade, amount, side)
        update.update(fill_price=price, fill_quote_amount=str(D(amount) * D(price)), fee=fill.trade_fee)
        payload['order_fills'][trade] = update
        payload['executed_amount_base'] += D(amount)
        payload['executed_amount_quote'] += D(amount) * D(price)
    entries = [SimpleNamespace(controller_id='btc', connector_name='okx', trading_pair='BTC-USDC',
                               position_side=None, order_side=payloads[order]['trade_type'], timestamp=timestamp, client_order_id=order,
                               exchange_order_id=order + '-exchange', order_payload=payloads[order],
                               to_order_record=lambda value=payloads[order]: value) for order, timestamp in [('C', 2000), ('B', 2500), ('A', 3000)]]
    orchestrator, recorder = make_orchestrator(entries, native)
    baseline = orchestrator.get_positions_report()['btc'][0]
    assert baseline.amount == D('2') and baseline.breakeven_price == D('150')
    assert baseline.realized_pnl_quote == D('-50')
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        for _ in range(2):
            result = orchestrator.get_all_reports()['btc']['positions'][0]
            assert result.side == baseline.side
            assert result.amount == baseline.amount
            assert result.breakeven_price == baseline.breakeven_price
            assert result.realized_pnl_quote == baseline.realized_pnl_quote


def test_nonledger_exact_quote_fee_must_match_legacy_encoding():
    fill = exact_fill('one', 'BUY', '1')
    fill.exact_trade_fee_in_quote = '1'
    orchestrator, recorder = make_orchestrator([], [fill])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()


def test_one_order_cannot_have_opposite_fill_sides():
    buy = exact_fill('one', 'BUY', '1')
    sell = exact_fill('one', 'SELL', '0.5')
    sell.exchange_trade_id = 'different-fill'
    orchestrator, recorder = make_orchestrator([], [buy, sell])
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()


def test_existing_exact_fill_cannot_shrink_under_same_identity():
    orchestrator, recorder = make_orchestrator([], [exact_fill('one', 'BUY', '1')])
    previous = orchestrator.positions_held['btc'][0]
    recorder.get_trades_for_config.return_value = [exact_fill('one', 'BUY', '0.5')]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()
    assert orchestrator.positions_held['btc'][0] is previous


def test_new_distinct_fill_can_extend_existing_exact_order():
    original = exact_fill('one', 'BUY', '1')
    orchestrator, recorder = make_orchestrator([], [original])
    additional = exact_fill('one', 'BUY', '0.5')
    additional.exchange_trade_id = 'additional-fill'
    additional.timestamp = 11
    recorder.get_trades_for_config.return_value = [original, additional]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        assert orchestrator.get_all_reports()['btc']['positions'][0].amount == D('1.5')


def test_known_exact_quote_fee_cannot_change_with_consistent_storage_encoding():
    orchestrator, recorder = make_orchestrator([], [exact_fill('one', 'BUY', '1')])
    previous = orchestrator.positions_held['btc'][0]
    changed = exact_fill('one', 'BUY', '1')
    changed.exact_trade_fee_in_quote = '1'
    changed.trade_fee_in_quote = D('1')
    recorder.get_trades_for_config.return_value = [changed]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unavailable'):
            orchestrator.get_all_reports()
    assert orchestrator.positions_held['btc'][0] is previous
