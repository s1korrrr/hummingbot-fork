from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor, tracked
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


def make_orchestrator():
    strategy = make_executor()._strategy
    strategy.controllers = {'controller': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.get_price_by_type.return_value = D('100')
    recorder = MagicMock(spec=MarketsRecorder)
    recorder.get_all_executors.return_value = []
    recorder.get_all_positions.return_value = []
    recorder.get_all_position_hold_ledger_entries.return_value = []
    recorder.config_file_path = 'audit.yml'
    recorder.get_trades_for_config.return_value = []
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        return ExecutorOrchestrator(strategy)


def fill(oid, side, amount, fee='0'):
    return TradeFill(market='okx', symbol='ETH-USDT', order_id=oid, exchange_trade_id=oid,
                     timestamp=1, trade_type=side, price=D('100'), amount=D(int(D(amount) * 1000000)) / 1000000,
                     trade_fee_in_quote=D(fee), trade_fee={'percent': '0', 'flat_fees': []},
                     exact_amount=amount, exact_price='100', exact_trade_fee_in_quote=fee,
                     exact_receipt_source='order_filled_event_v1', base_asset='ETH', quote_asset='USDT',
                     leverage=1, position='NIL')


def test_partial_exit_does_not_consume_an_unrelated_held_bag():
    orchestrator = make_orchestrator()
    executor = make_executor()
    executor.config.controller_id = 'controller'
    executor._open_order = tracked('active-entry', TradeType.BUY, '1', OrderState.FILLED)
    executor._close_order = tracked('active-exit', TradeType.SELL, '.4', OrderState.PARTIALLY_FILLED)
    orchestrator.active_executors = {'controller': [executor]}
    _, positions = orchestrator._build_reconciled_positions_from_trade_fills([
        fill('held-entry', 'BUY', '1'), fill('active-entry', 'BUY', '1'), fill('active-exit', 'SELL', '.4')])
    position = positions[('controller', 'okx', 'ETH-USDT')].get_position_summary(D('100'))
    assert position.amount == D('1')
    assert position.breakeven_price == D('100')


def test_closed_executor_roundtrip_is_not_counted_again_as_held_pnl():
    orchestrator = make_orchestrator()
    executor = make_executor()
    executor.config.controller_id = 'controller'
    executor._open_order = tracked('entry', TradeType.BUY, '1', OrderState.FILLED)
    executor._close_order = tracked('exit', TradeType.SELL, '1', OrderState.FILLED)
    executor._status = RunnableStatus.TERMINATED
    executor.close_type = CloseType.TRAILING_STOP
    orchestrator._update_cached_performance('controller', executor.executor_info)
    _, positions = orchestrator._build_reconciled_positions_from_trade_fills([
        fill('entry', 'BUY', '1', '1'), fill('exit', 'SELL', '1', '1'), fill('held', 'BUY', '1')])
    assert positions[('controller', 'okx', 'ETH-USDT')].cum_fees_quote == D('0')


def test_late_fill_after_zero_fill_entry_termination_is_not_excluded_from_inventory():
    from test.hummingbot.strategy_v2.executors.position_executor.test_entry_expiry_policy import single_attempt

    orchestrator = make_orchestrator()
    executor = single_attempt()
    executor.config.controller_id = 'controller'
    executor._open_order = tracked('expired-entry', TradeType.BUY, '0', OrderState.CANCELED)
    executor.control_open_order()
    assert executor.is_closed
    # Cover both cached historical ownership and the interval before archival.
    orchestrator._update_cached_performance('controller', executor.executor_info)
    orchestrator.active_executors = {'controller': [executor]}
    _, positions = orchestrator._build_reconciled_positions_from_trade_fills([
        fill('expired-entry', 'BUY', '.00013824'),
    ])
    assert positions[('controller', 'okx', 'ETH-USDT')].get_position_summary(D('100')).amount == D('.00013824')


def test_new_fill_after_closed_partial_position_is_adopted_without_recounting_old_fills():
    from test.hummingbot.strategy_v2.executors.position_executor.test_entry_expiry_policy import single_attempt

    orchestrator = make_orchestrator()
    executor = single_attempt()
    executor.config.controller_id = 'controller'
    executor._open_order = tracked('entry', TradeType.BUY, '.4', OrderState.CANCELED)
    executor._close_order = tracked('exit', TradeType.SELL, '.4', OrderState.FILLED)
    executor._status = RunnableStatus.TERMINATED
    executor.close_type = CloseType.TRAILING_STOP
    orchestrator._update_cached_performance('controller', executor.executor_info)
    orchestrator.active_executors = {'controller': [executor]}
    late = fill('entry', 'BUY', '.6', '.06')
    late.exchange_trade_id = 'late-fill'
    _, positions = orchestrator._build_reconciled_positions_from_trade_fills([
        fill('entry', 'BUY', '.4'), fill('exit', 'SELL', '.4'), late,
    ])
    position = positions[('controller', 'okx', 'ETH-USDT')]
    assert position.get_position_summary(D('100')).amount == D('.6')
    assert position.cum_fees_quote == D('.06')


def test_flat_held_roundtrip_keeps_realized_fees_in_performance():
    orchestrator = make_orchestrator()
    position = PositionHold('okx', 'ETH-USDT', TradeType.BUY)
    for oid, side in [('buy', 'BUY'), ('sell', 'SELL')]:
        position.add_order_record({'client_order_id': oid, 'trade_type': side,
                                   'executed_amount_base': '1', 'executed_amount_quote': '100',
                                   'cumulative_fee_paid_quote': '1'})
    orchestrator.positions_held = {'controller': [position]}
    assert orchestrator.generate_performance_report('controller').global_pnl_quote == D('-2')
    orchestrator._prune_flat_positions('controller')
    assert orchestrator.generate_performance_report('controller').global_pnl_quote == D('-2')
    assert orchestrator.get_positions_report()['controller'] == []


def test_new_position_after_flat_uses_its_own_cost_basis():
    position = PositionHold('okx', 'ETH-USDT', TradeType.BUY)
    for oid, side, quote in [('first-buy', 'BUY', '100'), ('sell', 'SELL', '110'), ('new-buy', 'BUY', '200')]:
        position.add_order_record({'client_order_id': oid, 'trade_type': side,
                                   'executed_amount_base': '1', 'executed_amount_quote': quote})
    summary = position.get_position_summary(D('200'))
    assert summary.breakeven_price == D('200')
    assert summary.realized_pnl_quote == D('10')
    assert summary.unrealized_pnl_quote == D('0')


def test_late_cumulative_order_fill_updates_held_inventory_idempotently():
    position = PositionHold('okx', 'ETH-USDT', TradeType.BUY)
    record = {'client_order_id': 'partial', 'trade_type': 'BUY',
              'executed_amount_base': '.4', 'executed_amount_quote': '40'}
    position.add_order_record(record)
    record.update(executed_amount_base='.6', executed_amount_quote='60')
    position.add_order_record(record)
    position.add_order_record(record)
    assert position.get_position_summary(D('100')).amount == D('.6')


def test_base_fee_reduces_owned_inventory_without_double_charging_pnl():
    position = PositionHold('okx', 'ETH-USDT', TradeType.BUY)
    position.add_order_record({'client_order_id': 'buy', 'trade_type': 'BUY',
                              'executed_amount_base': '1', 'executed_amount_quote': '100',
                               'cumulative_fee_paid_quote': '1',
                               'order_fills': {'f': {'fill_base_amount': '1', 'fill_price': '100',
                                                     'fee': {'flat_fees': [{'token': 'ETH', 'amount': '.01'}]}}}})
    summary = position.get_position_summary(D('100'))
    assert summary.amount == D('.99')
    assert summary.global_pnl_quote == D('-1')


def test_closed_history_fees_do_not_charge_new_inventory_again():
    position = PositionHold('okx', 'ETH-USDT', TradeType.BUY)
    for oid, side, amount, quote, fee in [('a', 'BUY', '1', '100', '1'),
                                          ('b', 'SELL', '1', '110', '1'),
                                          ('c', 'BUY', '2', '400', '4'),
                                          ('d', 'SELL', '1', '210', '1')]:
        position.add_order_record({'client_order_id': oid, 'trade_type': side,
                                  'executed_amount_base': amount, 'executed_amount_quote': quote,
                                   'cumulative_fee_paid_quote': fee})
    summary = position.get_position_summary(D('200'))
    assert summary.remaining_fees_quote == D('2')
    assert summary.cum_fees_quote == D('7')
    assert summary.global_pnl_quote == D('13')
