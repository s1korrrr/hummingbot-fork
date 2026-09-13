from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.test_held_inventory_accounting import fill, make_orchestrator
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import SellOrderCompletedEvent
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StoreExecutorAction
from hummingbot.strategy_v2.models.executors import TrackedOrder
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

AMOUNT, PRICE, FEE = D('39.469'), D('.7243'), D('.02286991736')


def wallet_sale():
    orchestrator = make_orchestrator()
    config = orchestrator.strategy.controllers['controller'].config
    config.trading_pair = 'SUI-USDT'
    config.controller_name = 'ok_rsi'
    config.trade_mode = 'sell_only'
    config.sell_only_strict_fraction = True
    config.sell_only_if_profitable = False
    orchestrator.strategy.markets = {'okx': {'SUI-USDT'}}
    sale_config = OrderExecutorConfig(id='wallet-sale', controller_id='controller', timestamp=1,
                                      connector_name='okx', trading_pair='SUI-USDT', side=TradeType.SELL,
                                      position_action=PositionAction.CLOSE, amount=AMOUNT, price=PRICE,
                                      level_id='signal_exit', execution_strategy=ExecutionStrategy.LIMIT)
    executor = OrderExecutor(orchestrator.strategy, sale_config)
    executor._status = RunnableStatus.RUNNING
    fee = AddedToCostTradeFee(flat_fees=[TokenAmount(token='USDT', amount=FEE)])
    order = InFlightOrder(client_order_id='sale-order', exchange_order_id='exchange-sale',
                          trading_pair='SUI-USDT', order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
                          amount=AMOUNT, price=PRICE, creation_timestamp=1, initial_state=OrderState.FILLED)
    order.update_with_trade_update(TradeUpdate(
        trade_id='sale-fill', client_order_id='sale-order', exchange_order_id='exchange-sale',
        trading_pair='SUI-USDT', fill_price=PRICE, fill_base_amount=AMOUNT,
        fill_quote_amount=AMOUNT * PRICE, fee=fee, fill_timestamp=2))
    executor._order = TrackedOrder(order_id='sale-order')
    executor._order.order = order
    executor.get_in_flight_order = MagicMock(return_value=order)
    receipt = TradeFill(config_file_path='audit.yml', market='okx', symbol='SUI-USDT',
                        order_id='sale-order', exchange_trade_id='sale-fill', timestamp=2000,
                        trade_type='SELL', price=PRICE, amount=AMOUNT,
                        trade_fee_in_quote=D(int(FEE * 1000000)) / 1000000, trade_fee=fee.to_json(),
                        exact_amount=str(AMOUNT), exact_price=str(PRICE), exact_trade_fee_in_quote=str(FEE),
                        exact_receipt_source='order_filled_event_v1', base_asset='SUI', quote_asset='USDT',
                        leverage=1, position='NIL')
    recorder = MagicMock(spec=MarketsRecorder)
    recorder.config_file_path = 'audit.yml'
    recorder.get_all_executors.return_value = []
    recorder.get_all_positions.return_value = []
    recorder.get_all_position_hold_ledger_entries.return_value = []
    recorder.get_trades_for_config.return_value = [receipt]
    orchestrator.active_executors['controller'] = [executor]
    return orchestrator, executor, recorder, receipt


def assert_unknown_sale(report):
    assert report['positions'] == []
    performance = report['performance']
    assert performance.pnl_available is False
    assert performance.pnl_unavailable_reason == 'unknown_wallet_acquisition_cost'
    for field in ('global_pnl_quote', 'global_pnl_pct', 'realized_pnl_quote', 'realized_pnl_pct',
                  'unrealized_pnl_quote', 'unrealized_pnl_pct'):
        assert getattr(performance, field) is None
    assert performance.volume_traded == AMOUNT * PRICE
    assert len(performance.wallet_sales) == 1
    sale = performance.wallet_sales[0]
    assert (sale['amount_base'], sale['proceeds_quote'], sale['fees_quote']) == (AMOUNT, AMOUNT * PRICE, FEE)
    assert sale['fee']['flat_fees'] == [{'token': 'USDT', 'amount': str(FEE)}]
    info = report['executors'][0]
    assert info.net_pnl_quote is None and info.net_pnl_pct is None
    assert info.custom_info['pnl_available'] is False
    assert info.custom_info['pnl_unavailable_reason'] == 'unknown_wallet_acquisition_cost'


def test_wallet_fill_reports_terminal_then_archives_and_restores_without_inventing_cost():
    orchestrator, executor, recorder, _ = wallet_sale()
    executor.process_order_completed_event(0, None, SellOrderCompletedEvent(
        3, 'sale-order', 'SUI', 'USDT', AMOUNT, AMOUNT * PRICE, OrderType.LIMIT, 'exchange-sale'))
    assert executor.is_closed
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        assert_unknown_sale(orchestrator.get_all_reports()['controller'])
        assert executor.executor_info.net_pnl_quote == 0  # Projection must not mutate native execution proof.
        orchestrator.store_executor(StoreExecutorAction(controller_id='controller', executor_id=executor.config.id))
        recorder.store_or_update_executor.assert_called_once_with(executor)
        assert orchestrator.active_executors['controller'] == []
        assert_unknown_sale(orchestrator.get_all_reports()['controller'])
        # Native persistence serializes executor info; round-trip the same contract before fresh startup.
        recorder.get_all_executors.return_value = [ExecutorInfo.model_validate_json(executor.executor_info.model_dump_json())]
        restarted = ExecutorOrchestrator(orchestrator.strategy)
        assert restarted.recovery_required == {}
        assert_unknown_sale(restarted.get_all_reports()['controller'])
        assert_unknown_sale(restarted.get_all_reports()['controller'])
        assert restarted.positions_held['controller'] == []


@pytest.mark.parametrize('field,value', [('sell_only_if_profitable', True), ('sell_only_strict_fraction', False),
                                         ('trade_mode', 'both'), ('controller_name', 'hl_rsi')])
def test_wallet_exception_requires_explicit_policy(field, value):
    orchestrator, _, recorder, _ = wallet_sale()
    setattr(orchestrator.strategy.controllers['controller'].config, field, value)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='unowned negative inventory'):
            orchestrator.get_all_reports()


@pytest.mark.parametrize('conflict', ['missing_owner', 'wrong_order', 'oversized', 'owned_buy', 'ledger'])
def test_wallet_exception_does_not_hide_ownership_or_oversell_conflicts(conflict):
    orchestrator, executor, recorder, receipt = wallet_sale()
    if conflict == 'missing_owner':
        orchestrator.active_executors['controller'] = []
    elif conflict == 'wrong_order':
        receipt.order_id = 'unowned-sale'
    elif conflict == 'oversized':
        executor.config.amount = AMOUNT - 1
    elif conflict == 'owned_buy':
        buy = fill('buy', 'BUY', '1')
        buy.symbol, buy.base_asset, buy.config_file_path = 'SUI-USDT', 'SUI', 'audit.yml'
        recorder.get_trades_for_config.return_value.insert(0, buy)
    elif conflict == 'ledger':
        recorder.get_all_position_hold_ledger_entries.return_value = [SimpleNamespace(
            connector_name='okx', trading_pair='SUI-USDT', controller_id='controller',
            position_side=None, client_order_id='missing-owned-buy')]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='reconciliation unavailable'):
            orchestrator.get_all_reports()


def legacy_sale_ledger(executor):
    from hummingbot.model.position_hold_ledger import PositionHoldLedger
    order = executor._order.order.to_json()
    order['cumulative_fee_paid_quote'] = str(FEE)
    return PositionHoldLedger(
        controller_id='controller', executor_id=executor.config.id, connector_name='okx',
        trading_pair='SUI-USDT', position_side=None, order_side='SELL', timestamp=2000,
        client_order_id='sale-order', exchange_order_id='exchange-sale',
        price=PRICE, amount=AMOUNT, executed_amount_base=AMOUNT,
        executed_amount_quote=D(int(AMOUNT * PRICE * 1000000)) / 1000000,
        cumulative_fee_paid_quote=D(int(FEE * 1000000)) / 1000000, order_payload=order)


def test_previous_failed_refresh_sell_ledger_restores_as_receipt_and_remains_immutable():
    import copy
    orchestrator, executor, recorder, _ = wallet_sale()
    executor._finish_with_fills()
    entry = legacy_sale_ledger(executor)
    original = copy.deepcopy(entry.order_payload)
    recorder.get_all_position_hold_ledger_entries.return_value = [entry]
    recorder.get_all_executors.return_value = [executor.executor_info]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        restarted = ExecutorOrchestrator(orchestrator.strategy)
        assert_unknown_sale(restarted.get_all_reports()['controller'])
    assert entry.order_payload == original
    recorder.store_or_update_position_hold_ledger_entries.assert_not_called()


def test_active_partial_wallet_sale_counts_proceeds_once():
    orchestrator, executor, recorder, _ = wallet_sale()
    executor.config.amount = AMOUNT * 2
    executor._order.order.current_state = OrderState.PARTIALLY_FILLED
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        report = orchestrator.get_all_reports()['controller']
        assert_unknown_sale(report)
        assert report['executors'][0].is_active


@pytest.mark.parametrize('field,value', [('executor_id', 'other-owner'), ('order_side', 'BUY')])
def test_existing_ledger_conflict_is_not_a_wallet_sale(field, value):
    orchestrator, executor, recorder, _ = wallet_sale()
    entry = legacy_sale_ledger(executor)
    setattr(entry, field, value)
    recorder.get_all_position_hold_ledger_entries.return_value = [entry]
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='reconciliation unavailable'):
            orchestrator.get_all_reports()


def incomplete_saved_wallet_sale():
    from hummingbot.model.exact_order_values import OrderExact
    from hummingbot.model.order import Order
    from hummingbot.model.position import Position
    orchestrator, executor, recorder, receipt = wallet_sale()
    executor._finish_with_fills()
    info = executor.executor_info.model_copy(update={'status': RunnableStatus.SHUTTING_DOWN})
    entry = legacy_sale_ledger(executor)
    recorder.get_all_executors.return_value = [info]
    recorder.get_all_position_hold_ledger_entries.return_value = [entry]
    summary = Position(controller_id='controller', connector_name='okx', trading_pair='SUI-USDT', side='SELL',
                       timestamp=3000, amount=AMOUNT, breakeven_price=PRICE,
                       volume_traded_quote=D(int(AMOUNT * PRICE * 1000000)) / 1000000,
                       cum_fees_quote=D(int(FEE * 1000000)) / 1000000,
                       realized_pnl_quote=D('0'), unrealized_pnl_quote=D('0'))
    recorder.get_all_positions.return_value = [summary]
    native_order = Order(id='sale-order', config_file_path='audit.yml', market='okx', symbol='SUI-USDT',
                         base_asset='SUI', quote_asset='USDT', leverage=1, position='NIL',
                         exchange_order_id='exchange-sale', last_status='SellOrderCompleted',
                         amount=AMOUNT, price=PRICE)
    exact_order = OrderExact(order_id='sale-order', amount=str(AMOUNT), price=str(PRICE))
    session = MagicMock()
    session.get.side_effect = lambda model, key: native_order if model is Order else exact_order
    recorder._sql_manager = SimpleNamespace(get_new_session=MagicMock())
    recorder._sql_manager.get_new_session.return_value.__enter__.return_value = session
    return orchestrator, recorder, info, entry, summary, native_order, exact_order


def test_exact_completed_wallet_sale_recovers_stale_shutdown_and_synthetic_sell_summary():
    orchestrator, recorder, original, _, _, _, _ = incomplete_saved_wallet_sale()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        restored = ExecutorOrchestrator(orchestrator.strategy)
        assert_unknown_sale(restored.get_all_reports()['controller'])
        info = restored.get_executors_report()['controller'][0]
        assert info.is_done and not info.is_active
        assert info.custom_info['wallet_sale_terminal_recovery'] == 'exact_completed_order_receipts'
        assert not restored.recovery_required.get('controller')
        assert original.status == RunnableStatus.SHUTTING_DOWN
        recorder.store_or_update_executor.assert_not_called()


@pytest.mark.parametrize('price_change', [D('-0.01'), D('0.01')])
def test_wallet_sale_recovers_native_stop_summary_at_changed_market_price(price_change):
    from hummingbot.strategy_v2.executors.executor_orchestrator import PositionHold
    orchestrator, recorder, original, ledger, summary, _, _ = incomplete_saved_wallet_sale()
    # The deployed pre-repair strict stop persists this native projection using
    # the then-current midpoint, even though wallet acquisition cost is unknown.
    held = PositionHold('okx', 'SUI-USDT', TradeType.SELL)
    held.add_order_record(ledger.order_payload)
    projection = held.get_position_summary(PRICE + price_change)
    summary.unrealized_pnl_quote = D(int(projection.unrealized_pnl_quote * 1000000)) / 1000000
    assert summary.unrealized_pnl_quote != 0
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        restored = ExecutorOrchestrator(orchestrator.strategy)
        assert_unknown_sale(restored.get_all_reports()['controller'])
        assert restored.get_executors_report()['controller'][0].is_done
        assert original.status == RunnableStatus.SHUTTING_DOWN
        assert not restored.recovery_required.get('controller')
        recorder.store_or_update_executor.assert_not_called()


@pytest.mark.parametrize('unrealized', [None, D('NaN'), D('Infinity'), D('-Infinity'), 'invalid'])
def test_wallet_sale_rejects_invalid_synthetic_mark_to_market(unrealized):
    orchestrator, recorder, _, _, summary, _, _ = incomplete_saved_wallet_sale()
    summary.unrealized_pnl_quote = unrealized
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        restored = ExecutorOrchestrator(orchestrator.strategy)
        with pytest.raises(RuntimeError, match='reconciliation unavailable'):
            restored.get_all_reports()


@pytest.mark.parametrize('defect', ['open', 'missing_order', 'missing_exact', 'wrong_exchange',
                                    'partial', 'missing_ledger', 'summary_amount', 'summary_fees'])
def test_terminal_wallet_recovery_rejects_incomplete_or_conflicting_proof(defect):
    orchestrator, recorder, original, _, summary, native, exact = incomplete_saved_wallet_sale()
    if defect == 'open':
        native.last_status = 'OrderFilled'
    elif defect == 'missing_order':
        recorder._sql_manager.get_new_session.return_value.__enter__.return_value.get.side_effect = lambda *args: None
    elif defect == 'missing_exact':
        exact.amount = None
    elif defect == 'wrong_exchange':
        native.exchange_order_id = 'different'
    elif defect == 'partial':
        original.config.amount = AMOUNT + 1
    elif defect == 'missing_ledger':
        recorder.get_all_position_hold_ledger_entries.return_value = []
    elif defect == 'summary_amount':
        summary.amount = AMOUNT + 1
    elif defect == 'summary_fees':
        summary.cum_fees_quote = D('0')
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        restored = ExecutorOrchestrator(orchestrator.strategy)
        with pytest.raises(RuntimeError, match='reconciliation unavailable'):
            restored.get_all_reports()
        assert original.id in restored.recovery_required['controller']
        assert original.status == RunnableStatus.SHUTTING_DOWN


@pytest.mark.parametrize('defect', ['ledger_quote_fee', 'missing_native', 'missing_exact_fee', 'duplicate_fill',
                                    'same_id_fee_changed', 'replacement_total'])
def test_wallet_receipts_remain_complete_exact_and_bounded(defect):
    import copy
    orchestrator, executor, recorder, receipt = wallet_sale()
    executor._finish_with_fills()
    if defect == 'ledger_quote_fee':
        entry = legacy_sale_ledger(executor)
        entry.order_payload['cumulative_fee_paid_quote'] = '0'
        recorder.get_all_position_hold_ledger_entries.return_value = [entry]
    elif defect == 'missing_native':
        recorder.get_trades_for_config.return_value = []
    elif defect == 'missing_exact_fee':
        receipt.exact_trade_fee_in_quote = None
    elif defect == 'duplicate_fill':
        recorder.get_trades_for_config.return_value.append(copy.copy(receipt))
    elif defect == 'same_id_fee_changed':
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
            orchestrator.get_all_reports()
        receipt.exact_trade_fee_in_quote = '0'
        receipt.trade_fee_in_quote = D('0')
    elif defect == 'replacement_total':
        original = executor.executor_info
        original.custom_info['order_ids'].append('replacement')
        orchestrator.active_executors['controller'] = []
        orchestrator.stored_executors['controller'] = [original]
        second = copy.copy(receipt)
        second.order_id, second.exchange_trade_id = 'replacement', 'second-fill'
        recorder.get_trades_for_config.return_value.append(second)
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        with pytest.raises(RuntimeError, match='reconciliation unavailable'):
            orchestrator.get_all_reports()


def test_wallet_native_volume_is_not_added_twice_when_executor_reports_quote_volume():
    from unittest.mock import PropertyMock
    orchestrator, executor, recorder, _ = wallet_sale()
    with patch.object(type(executor), 'filled_amount_quote', new_callable=PropertyMock, return_value=AMOUNT * PRICE):
        with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
            assert_unknown_sale(orchestrator.get_all_reports()['controller'])
