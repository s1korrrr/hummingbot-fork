"""Terminal native dust remains owned inventory in the unified portfolio snapshot."""
import copy
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from controllers.generic.meridian_v3_unified_core.portfolio import MeridianPortfolio, funding_projection
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.inventory import inventory_view
from hummingbot.core.data_type.common import TradeType
from hummingbot.model import get_declarative_base
from hummingbot.model.order import Order
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy_v2.models.executors import CloseType

PAIR = 'BTC-USDC'


def native_fill(order_id, trade_id, side, amount, price, fee=None):
    amount, price = D(amount), D(price)
    fee = fee or {'percent': '0', 'flat_fees': []}
    base, quote = PAIR.split('-')
    fee_quote = sum((D(row['amount']) * (price if row['token'] == base else D(1))
                     for row in fee['flat_fees'] if row['token'] in {base, quote}), D(0))

    def stored(value):
        return D(int(value * D(1000000))) / D(1000000)

    return NS(config_file_path='owner', strategy='owner', market='okx', symbol=PAIR, base_asset=base,
              quote_asset=quote, timestamp=1000000, order_id=order_id, trade_type=side, order_type='LIMIT',
              amount=stored(amount), price=stored(price), leverage=1, trade_fee=fee,
              trade_fee_in_quote=stored(fee_quote), exact_amount=str(amount), exact_price=str(price),
              exact_trade_fee_in_quote=str(fee_quote), exact_receipt_source='order_filled_event_v1',
              position='NIL', amount_decimal=amount, price_decimal=price,
              trade_fee_in_quote_decimal=fee_quote, has_exact_receipt=True, exchange_trade_id=trade_id,
              order=NS(id=order_id, exchange_order_id=f'exchange-{order_id}', symbol=PAIR, market='okx'))


def snapshot_fill(fill, order_id, exchange_order_id):
    return {'trade_id': fill.exchange_trade_id, 'client_order_id': order_id,
            'exchange_order_id': exchange_order_id, 'trading_pair': PAIR,
            'fill_base_amount': str(fill.amount_decimal), 'fill_price': str(fill.price_decimal),
            'fill_quote_amount': str(fill.amount_decimal * fill.price_decimal), 'fee': fill.trade_fee}


def snapshot_case():
    rows, fills = [], []
    for index, dust in enumerate(('0.000000002902', '0.000000008552')):
        key = f'terminal-{index}'
        buy, sell, price = D('.0006'), D('.0006') - D(dust), D(85000)
        entry, exit_id = f'entry-{index}', f'exit-{index}'
        buy_fill_id, sell_fill_id = f'buy-{index}', f'sell-{index}'
        buy_fill = native_fill(entry, buy_fill_id, 'BUY', buy, price)
        sell_fill = native_fill(exit_id, sell_fill_id, 'SELL', sell, price)
        fills.extend([buy_fill, sell_fill])
        order_snapshots = [
            {'client_order_id': entry, 'exchange_order_id': f'exchange-{entry}', 'trading_pair': PAIR,
             'trade_type': 'BUY', 'last_state': '5', 'amount': str(buy),
             'executed_amount_base': str(buy), 'executed_amount_quote': str(buy * price),
             'order_fills': {buy_fill_id: snapshot_fill(buy_fill, entry, f'exchange-{entry}')}},
            {'client_order_id': exit_id, 'exchange_order_id': f'exchange-{exit_id}', 'trading_pair': PAIR,
             'trade_type': 'SELL', 'last_state': '5', 'amount': str(sell),
             'executed_amount_base': str(sell), 'executed_amount_quote': str(sell * price),
             'order_fills': {sell_fill_id: snapshot_fill(sell_fill, exit_id, f'exchange-{exit_id}')}},
        ]
        rows.append(NS(id=key, controller_id='owner',
                       config=NS(controller_id='owner', connector_name='okx', trading_pair=PAIR,
                                 side=TradeType.BUY, level_id='modular_spot:v1:test', amount=buy, entry_price=price),
                       is_done=True, is_active=False, close_timestamp=1000,
                       close_type=CloseType.TRAILING_STOP,
                       custom_info={'order_ids': [entry, exit_id], 'entry_price': str(price),
                                    'execution_order_snapshots': order_snapshots,
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


def funding_without_quote_cash(fills, connector, pair):
    projection = funding_projection(fills, connector, pair)
    projection['cash_used'] = D(0)
    return projection


def test_two_completed_sublot_residuals_remain_owned_when_sum_exceeds_one_lot():
    portfolio, _, _, _ = snapshot_case()
    with patch('controllers.generic.meridian_v3_unified_core.portfolio.funding_projection',
               side_effect=funding_without_quote_cash):
        result = portfolio.snapshot(1000000)
    [asset] = result.assets
    assert result.reconciled
    assert portfolio.status['snapshot_holds'] == ()
    assert asset.quantity == D('0.000000011454')
    assert asset.basis_quote == asset.quantity * D(85000)
    assert result.attributed_nav_quote == D(500) + asset.quantity * asset.mark
    assert asset.sellable_quantity == 0


def test_current_same_id_overlay_cannot_erase_persisted_sublot_dust():
    portfolio, rows, fills, _ = snapshot_case()
    del rows[1:]
    del fills[2:]
    persisted = rows[0]
    current = copy.deepcopy(persisted)
    current.custom_info['execution_receipt']['residual_dust_base'] = '0'
    portfolio.owner.executors_info = [current]
    portfolio.recorder.return_value.get_all_executors = lambda: [persisted]
    portfolio._tick = None
    portfolio.evidence = MeridianPortfolio.evidence.__get__(portfolio, MeridianPortfolio)
    with pytest.raises(PolicyError, match='terminal dust'):
        take_snapshot(portfolio)


def test_matching_current_terminal_proof_preserves_recorded_dust():
    portfolio, rows, _, _ = snapshot_case()
    portfolio.owner.executors_info = copy.deepcopy(rows)
    portfolio.recorder.return_value.get_all_executors = lambda: rows
    portfolio._tick = None
    portfolio.evidence = MeridianPortfolio.evidence.__get__(portfolio, MeridianPortfolio)
    result = take_snapshot(portfolio)
    assert result.reconciled
    assert result.assets[0].quantity == D('0.000000011454')


def test_matching_current_numeric_projection_can_differ_in_decimal_scale():
    portfolio, rows, _, _ = snapshot_case()
    current = copy.deepcopy(rows)
    current[0].close_timestamp = 1000.0
    current[0].custom_info['execution_receipt']['entry_gross_quote'] = '51.0000'
    portfolio.owner.executors_info = current
    portfolio.recorder.return_value.get_all_executors = lambda: rows
    portfolio._tick = None
    portfolio.evidence = MeridianPortfolio.evidence.__get__(portfolio, MeridianPortfolio)
    assert take_snapshot(portfolio).reconciled


def test_current_same_id_overlay_cannot_change_native_ownership():
    portfolio, rows, _, _ = snapshot_case()
    current = copy.deepcopy(rows)
    current[0].config.trading_pair = 'ETH-USDC'
    portfolio.owner.executors_info = current
    portfolio.recorder.return_value.get_all_executors = lambda: rows
    portfolio._tick = None
    portfolio.evidence = MeridianPortfolio.evidence.__get__(portfolio, MeridianPortfolio)
    with pytest.raises(PolicyError, match='terminal dust proof'):
        take_snapshot(portfolio)


def test_duplicate_current_executor_identity_cannot_overwrite_native_proof():
    portfolio, rows, _, _ = snapshot_case()
    portfolio.owner.executors_info = [copy.deepcopy(rows[0]), copy.deepcopy(rows[0])]
    portfolio.recorder.return_value.get_all_executors = lambda: rows
    portfolio._tick = None
    portfolio.evidence = MeridianPortfolio.evidence.__get__(portfolio, MeridianPortfolio)
    with pytest.raises(PolicyError, match='duplicate'):
        take_snapshot(portfolio)


def test_completed_zero_fill_cancelled_reprice_does_not_hide_proven_terminal_dust():
    portfolio, rows, _, _ = snapshot_case()
    row = rows[0]
    canceled_id = 'entry-canceled-reprice'
    row.custom_info['order_ids'].insert(0, canceled_id)
    row.custom_info['execution_order_snapshots'].insert(0, {
        'client_order_id': canceled_id, 'exchange_order_id': 'exchange-canceled-reprice',
        'trading_pair': PAIR, 'trade_type': 'BUY', 'last_state': '3', 'amount': '0.0006',
        'executed_amount_base': '0', 'executed_amount_quote': '0', 'order_fills': {},
    })
    result = take_snapshot(portfolio)
    assert result.reconciled
    assert result.assets[0].quantity == D('0.000000011454')


def test_completed_partial_fill_cancelled_entry_reprice_is_reconciled_exactly():
    portfolio, rows, fills, _ = snapshot_case()
    row = rows[0]
    canceled_id = 'entry-partial-reprice'
    partial = native_fill(canceled_id, 'partial-entry-trade', 'BUY', '.0001', '85000')
    fills.append(partial)
    row.custom_info['order_ids'].insert(0, canceled_id)
    row.custom_info['execution_order_snapshots'].insert(0, {
        'client_order_id': canceled_id, 'exchange_order_id': f'exchange-{canceled_id}',
        'trading_pair': PAIR, 'trade_type': 'BUY', 'last_state': '3', 'amount': '0.0006',
        'executed_amount_base': '0.0001', 'executed_amount_quote': '8.5',
        'order_fills': {'partial-entry-trade': snapshot_fill(
            partial, canceled_id, f'exchange-{canceled_id}')},
    })
    replacement = native_fill('entry-0', 'buy-0', 'BUY', '.0005', '85000')
    fills[0] = replacement
    replacement_order = row.custom_info['execution_order_snapshots'][1]
    replacement_order.update(
        amount='0.0005', executed_amount_base='0.0005', executed_amount_quote='42.5',
        order_fills={'buy-0': snapshot_fill(replacement, 'entry-0', 'exchange-entry-0')},
    )
    assert take_snapshot(portfolio).reconciled


def test_completed_dust_exit_close_type_is_not_limited_to_trailing_stop():
    portfolio, rows, _, _ = snapshot_case()
    rows[0].close_type = CloseType.EARLY_STOP
    assert take_snapshot(portfolio).reconciled


def test_dust_basis_uses_fee_net_entry_receipt_not_gross_vwap():
    portfolio, rows, fills, _ = snapshot_case()
    row = rows[0]
    # Two BUY fills have different prices and base-denominated fees. The residual's
    # basis must come from the same exact net position projection as native accounting.
    fills[0] = native_fill('entry-0', 'buy-0', 'BUY', '.0003', '80000')
    fills.insert(1, native_fill('entry-0', 'buy-extra-0', 'BUY', '.0003', '90000',
                                {'percent': '0', 'flat_fees': [{'token': 'BTC', 'amount': '0.00000003'}]}))
    sell = D('0.000599967098')
    fills[2] = native_fill('exit-0', 'sell-0', 'SELL', sell, '85000')
    # Update native entry order snapshot and receipt to the two exact fills.
    receipt = row.custom_info['execution_receipt']
    receipt['entry_gross_base'] = D('0.0006')
    receipt['entry_gross_quote'] = D('51')
    receipt['exit_gross_base'] = sell
    receipt['exit_gross_quote'] = sell * D('85000')
    entry_order = row.custom_info['execution_order_snapshots'][0]
    entry_order['executed_amount_base'] = '0.0006'
    entry_order['executed_amount_quote'] = '51'
    entry_order['order_fills'] = {
        'buy-0': snapshot_fill(fills[0], 'entry-0', entry_order['exchange_order_id']),
        'buy-extra-0': snapshot_fill(fills[1], 'entry-0', entry_order['exchange_order_id']),
    }
    exit_order = row.custom_info['execution_order_snapshots'][1]
    exit_order.update(amount=str(sell), executed_amount_base=str(sell),
                      executed_amount_quote=str(sell * D('85000')),
                      order_fills={'sell-0': snapshot_fill(fills[2], 'exit-0', exit_order['exchange_order_id'])})
    result = take_snapshot(portfolio)
    dust = D('0.000000002902')
    expected_basis = dust * D('51') / (D('0.0006') - D('0.00000003'))
    assert result.reconciled
    assert result.assets[0].basis_quote != result.assets[0].quantity * D('85000')
    assert result.assets[0].basis_quote == (D('0.000000008552') * D('85000')) + expected_basis


def test_detached_native_sql_trade_fills_reconcile_without_lazy_order_access():
    portfolio, _, fills, _ = snapshot_case()
    engine = create_engine('sqlite://')
    get_declarative_base().metadata.create_all(engine)
    with Session(engine) as session:
        for fill in fills:
            session.add(Order(
                id=fill.order_id, config_file_path=fill.config_file_path, strategy=fill.strategy,
                market=fill.market, symbol=fill.symbol, base_asset=fill.base_asset, quote_asset=fill.quote_asset,
                creation_timestamp=fill.timestamp, order_type=fill.order_type, amount=fill.amount,
                leverage=fill.leverage, price=fill.price, last_status='FILLED',
                last_update_timestamp=fill.timestamp, exchange_order_id=f'exchange-{fill.order_id}',
                position=fill.position,
            ))
            session.add(TradeFill(
                config_file_path=fill.config_file_path, strategy=fill.strategy, market=fill.market,
                symbol=fill.symbol, base_asset=fill.base_asset, quote_asset=fill.quote_asset,
                timestamp=fill.timestamp, order_id=fill.order_id, trade_type=fill.trade_type,
                order_type=fill.order_type, price=fill.price, amount=fill.amount, leverage=fill.leverage,
                trade_fee=fill.trade_fee, trade_fee_in_quote=fill.trade_fee_in_quote,
                exact_amount=fill.exact_amount, exact_price=fill.exact_price,
                exact_trade_fee_in_quote=fill.exact_trade_fee_in_quote,
                exact_receipt_source=fill.exact_receipt_source, exchange_trade_id=fill.exchange_trade_id,
                position=fill.position,
            ))
        session.commit()
        detached_fills = session.query(TradeFill).all()
        session.expunge_all()
    portfolio.recorder.return_value.get_trades_for_config = lambda _: detached_fills
    assert take_snapshot(portfolio).reconciled


def take_snapshot(portfolio):
    with patch('controllers.generic.meridian_v3_unified_core.portfolio.funding_projection',
               side_effect=funding_without_quote_cash):
        return portfolio.snapshot(1000000)


def test_unexplained_fill_inventory_difference_still_holds_entries():
    portfolio, _, fills, _ = snapshot_case()
    fills.append(native_fill('unexplained', 'unexplained', 'BUY', '.000001', '85000'))
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
                                    'native_fee', 'duplicate_order', 'duplicate_trade', 'missing_trade',
                                    'foreign_pair', 'failed_order'])
def test_terminal_dust_requires_completed_matching_native_fill_lineage(change):
    portfolio, rows, fills, _ = snapshot_case()
    row, receipt = rows[0], rows[0].custom_info['execution_receipt']
    if change == 'close_type':
        row.close_type = CloseType.POSITION_HOLD
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
    elif change == 'failed_order':
        row.custom_info['execution_order_snapshots'][0]['last_state'] = '6'
    else:
        fills[0].symbol = 'ETH-USDC'
    with pytest.raises((PolicyError, ValueError)):
        take_snapshot(portfolio)


def test_recorded_lot_must_match_the_current_native_book_lot():
    portfolio, rows, _, _ = snapshot_case()
    rows[0].custom_info['close_quantity']['quantity_increment'] = '.0000001'
    with pytest.raises(PolicyError):
        take_snapshot(portfolio)


def test_sublot_unexplained_inventory_difference_is_not_tolerated_when_dust_is_reconciled():
    portfolio, _, fills, _ = snapshot_case()
    fills.append(native_fill('unexplained', 'unexplained', 'BUY', '.000000001', '85000'))
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


def test_duplicate_native_exchange_order_identity_across_executors_is_rejected():
    portfolio, rows, _, _ = snapshot_case()
    rows[1].custom_info['execution_order_snapshots'][0]['exchange_order_id'] = 'exchange-entry-0'
    with pytest.raises(PolicyError):
        take_snapshot(portfolio)
