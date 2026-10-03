"""Terminal native dust remains owned inventory in the unified portfolio snapshot."""
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import pytest

from controllers.generic.meridian_v3_unified_core.portfolio import MeridianPortfolio
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.inventory import inventory_view
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.models.executors import CloseType

PAIR = 'BTC-USDC'


def snapshot_case():
    rows, fills = [], []
    for index, dust in enumerate(('0.000000002902', '0.000000008552')):
        key = f'terminal-{index}'
        buy, sell, price = D('.0006'), D('.0006') - D(dust), D(85000)
        entry, exit_id = f'entry-{index}', f'exit-{index}'
        fills.extend([NS(symbol=PAIR, amount_decimal=buy, price_decimal=price, trade_type='BUY',
                         trade_fee={'flat_fees': []}, order_id=entry, exchange_trade_id=f'buy-{index}'),
                      NS(symbol=PAIR, amount_decimal=sell, price_decimal=price, trade_type='SELL',
                         trade_fee={'flat_fees': []}, order_id=exit_id, exchange_trade_id=f'sell-{index}')])
        rows.append(NS(id=key, controller_id='owner',
                       config=NS(controller_id='owner', connector_name='okx', trading_pair=PAIR,
                                 side=TradeType.BUY, level_id='modular_spot:v1:test', amount=buy, entry_price=price),
                       is_done=True, is_active=False, close_timestamp=1000,
                       close_type=CloseType.TRAILING_STOP,
                       custom_info={'order_ids': [entry, exit_id], 'entry_price': str(price),
                                    'entry_lifecycle': {'state': 'managing_fill'},
                                    'execution_receipt': {'residual_dust_base': dust,
                                                          'remaining_owned_base': '0',
                                                          'entry_gross_base': str(buy),
                                                          'entry_gross_quote': str(buy * price),
                                                          'exit_gross_base': str(sell),
                                                          'exit_gross_quote': str(sell * price),
                                                          'accounting_owner': 'executor'},
                                    'close_quantity': {'quantity_increment': '0.00000001'}}))
    bindings = NS(quote_observation=NS(event_timestamp=1000, applied_timestamp=1000))
    book = dict(available_quote=D(500), amount_step=D('.00000001'), minimum_amount=D('.00001'),
                minimum_notional=D(5), bid=D(85000), ask=D(85000))

    def inventory():
        return inventory_view(rows, [], controller_id='owner', connector='okx', pair=PAIR, bag_mode=True)

    runtime = NS(snapshot=lambda: book, bindings=bindings, inventory=inventory)
    config = NS(risk=NS(fee_reserve_fraction=D('.003')), connector_name='okx', account_id='owner', execution_mode='live',
                symbols=[NS(trading_pair=PAIR, entry_enabled=True)])
    owner = NS(config=config, contexts={PAIR: NS(runtime=runtime, positions_held=[])},
               operator_entry_paused=False)
    portfolio = object.__new__(MeridianPortfolio)
    portfolio.owner, portfolio.clock, portfolio.status = owner, {}, {}
    portfolio.source_sha256 = 'a' * 64
    portfolio.recipe = NS(mandate=NS(pairs=(PAIR,), reference_quote=D(500), identity='b' * 64))
    portfolio.recorder = Mock(return_value=NS(config_file_path='owner',
                                              get_trades_for_config=lambda _: fills))
    portfolio.evidence = Mock(return_value=rows)
    portfolio.store = Mock(return_value=NS(grants=lambda **_: ()))
    portfolio.target_store = Mock(return_value=NS(head=lambda: None))
    return portfolio, rows, fills, inventory


def test_two_completed_sublot_residuals_remain_owned_when_sum_exceeds_one_lot():
    portfolio, _, _, _ = snapshot_case()
    with patch('controllers.generic.meridian_v3_unified_core.portfolio.funding_projection',
               return_value={'cash_used': D(0)}):
        result = portfolio.snapshot(1000000)
    [asset] = result.assets
    assert result.reconciled
    assert portfolio.status['snapshot_holds'] == ()
    assert asset.quantity == D('0.000000011454')
    assert asset.basis_quote == asset.quantity * D(85000)
    assert result.attributed_nav_quote == D(500) + asset.quantity * asset.mark
    assert asset.sellable_quantity == 0


def take_snapshot(portfolio):
    with patch('controllers.generic.meridian_v3_unified_core.portfolio.funding_projection',
               return_value={'cash_used': D(0)}):
        return portfolio.snapshot(1000000)


def test_unexplained_fill_inventory_difference_still_holds_entries():
    portfolio, _, fills, _ = snapshot_case()
    fills.append(NS(symbol=PAIR, amount_decimal=D('.000001'), price_decimal=D(85000), trade_type='BUY',
                    trade_fee={'flat_fees': []}, order_id='unexplained', exchange_trade_id='unexplained'))
    result = take_snapshot(portfolio)
    assert not result.reconciled
    assert portfolio.status['snapshot_holds'] == ('NATIVE_INVENTORY_MISMATCH:BTC-USDC',)


@pytest.mark.parametrize('dust', ['NaN', 'Infinity', '-.000000001', '.00000001', '.001'])
def test_invalid_or_not_sublot_terminal_dust_is_rejected(dust):
    portfolio, rows, _, _ = snapshot_case()
    rows[0].custom_info['execution_receipt']['residual_dust_base'] = dust
    with pytest.raises(PolicyError):
        take_snapshot(portfolio)


@pytest.mark.parametrize('change', ['active', 'not_done', 'held_owner', 'remaining', 'missing_lot', 'missing_close'])
def test_unfinished_or_unproved_residual_cannot_reconcile_inventory(change):
    portfolio, rows, _, _ = snapshot_case()
    row = rows[0]
    if change == 'active':
        row.is_active = True
    elif change == 'not_done':
        row.is_done = False
    elif change == 'held_owner':
        row.custom_info['execution_receipt']['accounting_owner'] = 'held_position'
    elif change == 'remaining':
        row.custom_info['execution_receipt']['remaining_owned_base'] = '.001'
    elif change == 'missing_lot':
        del row.custom_info['close_quantity']
    else:
        row.close_timestamp = None
    with pytest.raises((PolicyError, KeyError)):
        take_snapshot(portfolio)


@pytest.mark.parametrize('change', ['close_type', 'entry_amount', 'exit_amount', 'native_amount',
                                    'native_fee', 'duplicate_order', 'duplicate_trade', 'missing_trade', 'foreign_pair'])
def test_terminal_dust_requires_completed_matching_native_fill_lineage(change):
    portfolio, rows, fills, _ = snapshot_case()
    row, receipt = rows[0], rows[0].custom_info['execution_receipt']
    if change == 'close_type':
        row.close_type = CloseType.EXPIRED
    elif change == 'entry_amount':
        receipt['entry_gross_base'] = '.001'
    elif change == 'exit_amount':
        receipt['exit_gross_quote'] = '1'
    elif change == 'native_amount':
        fills[0].amount_decimal += D('.000000001')
    elif change == 'native_fee':
        fills[0].trade_fee['flat_fees'] = [{'token': 'BTC', 'amount': '.000000001'}]
    elif change == 'duplicate_order':
        row.custom_info['order_ids'].append(row.custom_info['order_ids'][0])
    elif change == 'duplicate_trade':
        fills[1].exchange_trade_id = fills[0].exchange_trade_id
    elif change == 'missing_trade':
        fills[0].exchange_trade_id = None
    else:
        fills[0].symbol = 'ETH-USDC'
    with pytest.raises(PolicyError):
        take_snapshot(portfolio)


def test_recorded_lot_must_match_the_current_native_book_lot():
    portfolio, rows, _, _ = snapshot_case()
    rows[0].custom_info['close_quantity']['quantity_increment'] = '.0000001'
    with pytest.raises(PolicyError):
        take_snapshot(portfolio)


def test_sublot_unexplained_inventory_difference_is_not_tolerated_when_dust_is_reconciled():
    portfolio, _, fills, _ = snapshot_case()
    fills.append(NS(symbol=PAIR, amount_decimal=D('.000000001'), price_decimal=D(85000), trade_type='BUY',
                    trade_fee={'flat_fees': []}, order_id='unexplained', exchange_trade_id='unexplained'))
    assert not take_snapshot(portfolio).reconciled


def test_real_native_inventory_reader_rejects_foreign_executor_ownership():
    portfolio, rows, _, _ = snapshot_case()
    rows[0].config.controller_id = 'foreign'
    with pytest.raises(PolicyError):
        take_snapshot(portfolio)


def test_duplicate_native_trade_identity_across_terminal_executors_is_rejected():
    portfolio, _, fills, _ = snapshot_case()
    fills[2].exchange_trade_id = fills[0].exchange_trade_id
    with pytest.raises(PolicyError):
        take_snapshot(portfolio)
