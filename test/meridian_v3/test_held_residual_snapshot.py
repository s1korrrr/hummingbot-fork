"""A held residual is owned inventory before and after the orchestrator absorbs it.

A single-attempt entry that expires with a partial fill below the exchange
minimum ends `held_residual` and stops with keep_position. Its receipt names the
held position as accounting owner at once, but the orchestrator moves the
inventory into `positions_held` only after the executor is done. The snapshot
must count that residual exactly once in both phases.
"""
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import pytest

from controllers.generic.meridian_v3_unified_core.portfolio import MeridianPortfolio, funding_projection
from controllers.generic.modular_spot_core.inventory import inventory_view
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

PAIR = 'ETH-USDC'
LOT = D('0.0001')
PRICE = D(2000)
PLANNED = D('0.01')


def native_fill(order_id, trade_id, amount, price):
    fee = {'percent': '0', 'flat_fees': [{'token': 'USDC', 'amount': str(amount * price * D('0.001'))}]}
    fee_quote = amount * price * D('0.001')
    return NS(config_file_path='owner', strategy='owner', market='okx', symbol=PAIR, base_asset='ETH',
              quote_asset='USDC', timestamp=1000000, order_id=order_id, trade_type='BUY', order_type='LIMIT',
              amount=amount, price=price, leverage=1, trade_fee=fee, trade_fee_in_quote=fee_quote,
              exact_amount=str(amount), exact_price=str(price), exact_trade_fee_in_quote=str(fee_quote),
              exact_receipt_source='order_filled_event_v1', position='NIL', amount_decimal=amount,
              price_decimal=price, trade_fee_in_quote_decimal=fee_quote, has_exact_receipt=True,
              exchange_trade_id=trade_id, order=NS(id=order_id, exchange_order_id=f'exchange-{order_id}',
                                                   symbol=PAIR, market='okx'))


def held_residual_executor(residual, *, done):
    """Native receipt of a single-attempt entry that ended `held_residual` (keep_position)."""
    return NS(id='held-residual', controller_id='owner',
              config=NS(controller_id='owner', connector_name='okx', trading_pair=PAIR, side=TradeType.BUY,
                        level_id='modular_spot:v1:test', amount=PLANNED, entry_price=PRICE),
              is_done=done, is_active=not done,
              status=RunnableStatus.TERMINATED if done else RunnableStatus.SHUTTING_DOWN,
              close_type=CloseType.POSITION_HOLD, close_timestamp=1000 if done else None,
              custom_info={'order_ids': ['entry'], 'entry_price': str(PRICE),
                           'entry_lifecycle': {'policy': 'single_attempt', 'state': 'held_residual'},
                           'execution_receipt': {'residual_dust_base': '0',
                                                 'remaining_owned_base': str(residual),
                                                 'entry_gross_base': str(residual),
                                                 'entry_gross_quote': str(residual * PRICE),
                                                 'exit_gross_base': '0', 'exit_gross_quote': '0',
                                                 'accounting_owner': 'held_position'}})


def held_position(amount):
    return NS(connector_name='okx', trading_pair=PAIR, side=TradeType.BUY, amount=amount,
              breakeven_price=PRICE, remaining_fees_quote=amount * PRICE * D('0.001'))


def portfolio_case(residual, *, done, absorbed):
    rows = [held_residual_executor(residual, done=done)]
    fills = [native_fill('entry', 'buy-1', residual, PRICE)]
    positions = [held_position(residual)] if absorbed else []
    book = dict(available_quote=D(500), amount_step=LOT, minimum_amount=LOT,
                minimum_notional=D(5), bid=PRICE, ask=PRICE)

    def inventory():
        return inventory_view(rows, positions, controller_id='owner', connector='okx', pair=PAIR, bag_mode=True)

    runtime = NS(snapshot=lambda: book, inventory=inventory,
                 bindings=NS(quote_observation=NS(event_timestamp=1000, applied_timestamp=1000)))
    config = NS(risk=NS(fee_reserve_fraction=D('.003')), connector_name='okx', account_id='owner',
                execution_mode='live', symbols=[NS(trading_pair=PAIR, entry_enabled=True)])
    owner = NS(config=config, contexts={PAIR: NS(runtime=runtime, positions_held=positions)},
               operator_entry_paused=False)
    portfolio = object.__new__(MeridianPortfolio)
    portfolio.owner, portfolio.clock, portfolio.status = owner, {}, {}
    portfolio.source_sha256 = 'a' * 64
    portfolio.recipe = NS(mandate=NS(pairs=(PAIR,), reference_quote=D(500), identity='b' * 64))
    portfolio.recorder = Mock(return_value=NS(config_file_path='owner', get_trades_for_config=lambda _: fills))
    portfolio.evidence = Mock(return_value=rows)
    portfolio.store = Mock(return_value=NS(grants=lambda **_: ()))
    portfolio.target_store = Mock(return_value=NS(head=lambda: None))
    return portfolio


def take_snapshot(portfolio):
    def funding_without_quote_cash(fills, connector, pair):
        projection = funding_projection(fills, connector, pair)
        projection['cash_used'] = D(0)
        return projection

    with patch('controllers.generic.meridian_v3_unified_core.portfolio.funding_projection',
               side_effect=funding_without_quote_cash):
        return portfolio.snapshot(1000000)


@pytest.mark.parametrize('residual', [LOT, 12 * LOT], ids=['one_lot', 'multi_lot'])
def test_held_residual_counts_before_the_orchestrator_absorbs_it(residual):
    portfolio = portfolio_case(residual, done=False, absorbed=False)
    result = take_snapshot(portfolio)
    [asset] = result.assets
    assert portfolio.status['snapshot_holds'] == ()
    assert result.reconciled
    assert asset.quantity == residual
    assert asset.basis_quote == residual * PRICE
    # The entry is terminal: its unfilled planned quote is not a live commitment.
    assert result.commitments == ()


@pytest.mark.parametrize('residual', [LOT, 12 * LOT], ids=['one_lot', 'multi_lot'])
def test_absorbed_held_residual_counts_once(residual):
    portfolio = portfolio_case(residual, done=True, absorbed=True)
    result = take_snapshot(portfolio)
    [asset] = result.assets
    assert asset.quantity == residual
    assert asset.basis_quote == residual * PRICE + residual * PRICE * D('0.001')
    assert result.reconciled
    assert portfolio.status['snapshot_holds'] == ()
    assert result.commitments == ()
