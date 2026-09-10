from decimal import Decimal as D
from test.test_ok_rsi_admission_safety import controller
from types import SimpleNamespace

import pytest

from hummingbot.strategy_v2.models.executors import CloseType


def receipt(order_id, side, base, quote, fees='0', base_fee='0'):
    return {'client_order_id': order_id, 'trade_type': side,
            'executed_amount_base': base, 'executed_amount_quote': quote,
            'cumulative_fee_paid_quote': fees,
            'order_fills': {'fill': {'fill_base_amount': base,
                                     'fee': {'flat_fees': [{'token': 'ETH', 'amount': base_fee}]}}}}


def held(executor_id='dust', orders=None, **custom):
    return SimpleNamespace(id=executor_id, timestamp=990, close_timestamp=999,
                           is_done=True, is_active=False, close_type=CloseType.POSITION_HOLD,
                           net_pnl_quote=D('-999'),
                           custom_info={'held_position_orders': orders or [], **custom})


def test_realized_dust_loss_pauses_new_entries_exactly_once():
    instance = controller()
    instance.config.max_consecutive_losses = 1
    result = held(orders=[receipt('buy', 'BUY', '1', '100', '.1'),
                          receipt('sell', 'SELL', '.999', '98.901', '.098901')])
    instance._ingest_closed_executor_results([result])
    instance._ingest_closed_executor_results([result])
    assert instance._consecutive_losses == 1
    assert instance._check_consecutive_loss_pause()


def test_pure_inventory_hold_does_not_count_executor_fee_only_loss():
    instance = controller()
    instance._ingest_closed_executor_results([held(orders=[receipt('buy', 'BUY', '1', '100', '.1')])])
    assert instance._consecutive_losses == 0


def test_realized_profit_resets_losses_despite_negative_hold_executor_metric():
    instance = controller()
    instance._consecutive_losses = 2
    instance._ingest_closed_executor_results([held(orders=[receipt('buy', 'BUY', '1', '100', '.1'),
                                                           receipt('sell', 'SELL', '.5', '51', '.051')])])
    assert instance._consecutive_losses == 0


def test_matched_result_uses_net_base_and_only_matched_share_of_fees():
    instance = controller()
    orders = [receipt('buy', 'BUY', '1', '100', '1', '.01'),
              receipt('sell', 'SELL', '.495', '50', '.05')]
    assert instance._held_position_realized_pnl(held(orders=orders)) == D('-.05')


def test_cumulative_duplicate_receipts_are_not_double_counted():
    instance = controller()
    orders = [receipt('buy', 'BUY', '.5', '50', '.05'),
              receipt('buy', 'BUY', '1', '100', '.1'),
              receipt('buy', 'BUY', '.5', '50', '.05'),
              receipt('sell', 'SELL', '.5', '50', '.05')]
    assert instance._held_position_realized_pnl(held(orders=orders)) == D('-.1')


def test_invalid_realized_receipts_block_entries_until_repaired():
    instance = controller()
    result = held(orders=[receipt('buy', 'BUY', 'NaN', '100'), receipt('sell', 'SELL', '.5', '49')])
    instance._ingest_closed_executor_results([result])
    assert instance._check_consecutive_loss_pause()
    assert result.id not in instance._tracked_executor_result_ids
    result.custom_info['held_position_orders'][0]['executed_amount_base'] = '1'
    result.custom_info['held_position_orders'][0]['order_fills']['fill']['fill_base_amount'] = '1'
    instance._ingest_closed_executor_results([result])
    assert result.id in instance._tracked_executor_result_ids
    assert instance._consecutive_losses == 1


def test_explicit_base_fee_receipts_keep_gross_cost():
    instance = controller()
    buy = receipt('buy', 'BUY', '1', '100')
    buy['actual_base_fee'] = '.01'
    assert instance._held_position_realized_pnl(held(orders=[buy, receipt('sell', 'SELL', '.495', '50')])) == 0


@pytest.mark.parametrize('field,value', [('executed_amount_base', '-1'), ('executed_amount_quote', 'Infinity')])
def test_invalid_amounts_are_rejected(field, value):
    instance = controller()
    order = receipt('buy', 'BUY', '1', '100')
    order[field] = value
    with pytest.raises(ValueError):
        instance._held_position_realized_pnl(held(orders=[order]))


def test_missing_receipts_for_realized_close_pause_entries():
    instance = controller()
    instance._ingest_closed_executor_results([held(close_order_filled_amount_base='.5')])
    assert instance._check_consecutive_loss_pause()
    assert 'dust' not in instance._tracked_executor_result_ids


def test_negative_explicit_base_fee_is_rejected_like_held_ledger():
    instance = controller()
    order = receipt('buy', 'BUY', '1', '100')
    order['actual_base_fee'] = '-.01'
    with pytest.raises(ValueError):
        instance._held_position_realized_pnl(held(orders=[order]))


def test_sell_base_fee_consumption_is_allocated_to_realized_inventory():
    instance = controller()
    orders = [receipt('buy', 'BUY', '1', '100'),
              receipt('sell', 'SELL', '.49', '49', '1', '.01')]
    assert instance._held_position_realized_pnl(held(orders=orders)) == D('-1')


def test_claimed_close_without_exit_receipt_remains_unresolved():
    instance = controller()
    result = held(orders=[receipt('buy', 'BUY', '1', '100')], close_order_filled_amount_base='.5')
    instance._ingest_closed_executor_results([result])
    assert instance._check_consecutive_loss_pause()
    assert result.id not in instance._tracked_executor_result_ids


def test_repaired_older_profit_cannot_clear_a_newer_realized_loss():
    instance = controller()
    instance.config.max_consecutive_losses = 1
    older = held('older', orders=[receipt('buy-old', 'BUY', 'NaN', '100'),
                                  receipt('sell-old', 'SELL', '.5', '51')])
    newer = held('newer', orders=[receipt('buy-new', 'BUY', '1', '100'),
                                  receipt('sell-new', 'SELL', '.5', '49')])
    older.close_timestamp = 995
    instance._ingest_closed_executor_results([older, newer])
    assert instance._consecutive_losses == 1
    older.custom_info['held_position_orders'][0]['executed_amount_base'] = '1'
    older.custom_info['held_position_orders'][0]['order_fills']['fill']['fill_base_amount'] = '1'
    instance._ingest_closed_executor_results([older])
    assert older.id in instance._tracked_executor_result_ids
    assert older.id not in instance._unresolved_executor_result_ids
    assert instance._consecutive_losses == 1
    assert instance._last_loss_timestamp == 999
    assert instance._check_consecutive_loss_pause()


@pytest.mark.parametrize('malformation', [
    'missing_entry', 'zero_entry_quote', 'zero_exit_quote', 'nan_fill_price',
    'zero_fill_price', 'nan_fill_amount', 'negative_fill_amount', 'infinite_fill_quote',
    'mismatched_fill_amount', 'mismatched_fill_quote', 'missing_fills', 'overclosed',
])
def test_malformed_held_closures_remain_unresolved_and_never_reset_losses(malformation):
    instance = controller()
    instance._consecutive_losses = 2
    buy = receipt('buy', 'BUY', '1', '100')
    sell = receipt('sell', 'SELL', '.5', '51')
    orders = [buy, sell]
    if malformation == 'missing_entry':
        orders = [sell]
    elif malformation == 'zero_entry_quote':
        buy['executed_amount_quote'] = '0'
    elif malformation == 'zero_exit_quote':
        sell['executed_amount_quote'] = '0'
    elif malformation == 'nan_fill_price':
        buy['order_fills']['fill']['fill_price'] = 'NaN'
    elif malformation == 'zero_fill_price':
        buy['order_fills']['fill']['fill_price'] = '0'
    elif malformation == 'nan_fill_amount':
        buy['order_fills']['fill']['fill_base_amount'] = 'NaN'
    elif malformation == 'negative_fill_amount':
        buy['order_fills']['fill']['fill_base_amount'] = '-1'
    elif malformation == 'infinite_fill_quote':
        buy['order_fills']['fill']['fill_quote_amount'] = 'Infinity'
    elif malformation == 'mismatched_fill_amount':
        buy['order_fills']['fill']['fill_base_amount'] = '.9'
    elif malformation == 'mismatched_fill_quote':
        buy['order_fills']['fill']['fill_quote_amount'] = '99'
    elif malformation == 'missing_fills':
        buy['order_fills'] = {}
    elif malformation == 'overclosed':
        buy['executed_amount_base'] = '.4'
        buy['order_fills']['fill']['fill_base_amount'] = '.4'
    result = held(orders=orders, close_order_filled_amount_base='.5')
    instance._ingest_closed_executor_results([result])
    assert result.id not in instance._tracked_executor_result_ids
    assert instance._check_consecutive_loss_pause()
    assert instance._consecutive_losses == 2


def test_market_order_nan_limit_price_does_not_invalidate_executed_price():
    instance = controller()
    buy = receipt('buy', 'BUY', '1', '100')
    sell = receipt('sell', 'SELL', '.5', '49')
    sell['price'] = 'NaN'
    sell['order_type'] = 'MARKET'
    sell['order_fills']['fill'].update(fill_price='98', fill_quote_amount='49')
    result = held(orders=[buy, sell], close_order_filled_amount_base='.5')
    instance._ingest_closed_executor_results([result])
    assert result.id in instance._tracked_executor_result_ids
    assert instance._consecutive_losses == 1
