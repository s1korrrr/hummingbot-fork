import asyncio
import copy
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType


@pytest.fixture(autouse=True)
def _complete_native_history():
    """Each synthetic controller starts with explicit complete empty native history."""
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = []
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        yield recorder


def _provider(*, mid=D('100')):
    provider = MagicMock()
    provider.initialize_rate_sources.return_value = None
    provider.time.return_value = 1000.0
    provider.get_available_balance.return_value = D('10000')
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=D('.01'), min_order_size=D('.001'),
        min_base_amount_increment=D('.001'), min_notional_size=D('1'), min_order_value=D('1'))
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    provider.get_price_by_type.side_effect = lambda connector, pair, price_type: (
        mid if price_type == PriceType.MidPrice else mid
    )
    return provider


def _controller(**overrides):
    config = HLRSIControllerConfig(
        id='hl-rsi-entry-control',
        connector_name='hyperliquid',
        trading_pair='HYPE-USDC',
        strict_hmm_mode=False,
        executor_price_gap_threshold=D('0'),
        **overrides,
    )
    result = HLRSIController(config, _provider(), asyncio.Queue())
    result.positions_held = []
    result.executors_info = []
    result.processed_data = {
        'timestamp': 1000,
        'signal': 1,
        'data_health': {'ready': True},
        'signal_state': {'condition_ok': True, 'signal_score': 2, 'buy_entry_role': 'full'},
    }
    result._status = RunnableStatus.RUNNING
    return result


def _buy_executor(
    *,
    executor_id='ex',
    amount=D('1'),
    entry_price=D('120'),
    working_price=None,
    average_price=None,
    filled_base=D('0'),
    remaining_base=None,
    lifecycle='open',
    net_pnl_pct=D('0'),
    level_id=None,
    maximum_acceptable_price=None,
    is_active=True,
    is_done=False,
    accounting_owner=None,
    receipt_remaining=None,
    entry_gross_base=None,
    include_filled=True,
    include_remaining=True,
    native_order_state=None,
    terminal_exchange_timestamp=None,
    close_timestamp=None,
    cumulative_filled_base=None,
    order_ids=None,
    held_position_orders=None,
):
    custom = {
        'side': TradeType.BUY,
        'level_id': level_id,
        'entry_lifecycle': {'state': lifecycle},
        'close_order_filled_amount_base': D('0'),
    }
    if include_filled:
        custom['open_order_filled_amount_base'] = filled_base
    if include_remaining:
        custom['remaining_position_amount_base'] = filled_base if remaining_base is None else remaining_base
    if terminal_exchange_timestamp is not None:
        custom['entry_lifecycle']['terminal_exchange_timestamp'] = terminal_exchange_timestamp
    if working_price is not None or native_order_state is not None:
        acquired = filled_base if include_filled else D('0')
        if cumulative_filled_base is not None:
            acquired = cumulative_filled_base
        acquisition = {
            'state': lifecycle,
            'remaining_base': str(max(amount - (filled_base if include_filled else D('0')), D('0'))),
            'cumulative_filled_base': str(acquired),
        }
        if working_price is not None:
            custom['order_price'] = working_price
            acquisition['current_order_price'] = str(working_price)
        if native_order_state is not None:
            acquisition['native_order_state'] = native_order_state
        custom['entry_acquisition'] = acquisition
    if average_price is not None:
        custom['current_position_average_price'] = average_price
        custom['entry_price'] = average_price
    if accounting_owner is not None or receipt_remaining is not None or entry_gross_base is not None:
        receipt = {}
        if accounting_owner is not None:
            receipt['accounting_owner'] = accounting_owner
        if receipt_remaining is not None:
            receipt['remaining_owned_base'] = receipt_remaining
        if entry_gross_base is not None:
            receipt['entry_gross_base'] = entry_gross_base
        custom['execution_receipt'] = receipt
    if order_ids is not None:
        custom['order_ids'] = list(order_ids)
    if held_position_orders is not None:
        custom['held_position_orders'] = list(held_position_orders)
    return SimpleNamespace(
        id=executor_id,
        controller_id='hl-rsi-entry-control',
        is_active=is_active,
        is_done=is_done,
        close_timestamp=close_timestamp,
        connector_name='hyperliquid',
        trading_pair='HYPE-USDC',
        side=TradeType.BUY,
        timestamp=1.0,
        net_pnl_pct=net_pnl_pct,
        config=SimpleNamespace(
            type='position_executor',
            id=executor_id,
            controller_id='hl-rsi-entry-control',
            connector_name='hyperliquid',
            trading_pair='HYPE-USDC',
            side=TradeType.BUY,
            leverage=1,
            amount=amount,
            entry_price=entry_price,
            price=working_price,
            maximum_acceptable_price=maximum_acceptable_price or entry_price,
            level_id=level_id,
        ),
        custom_info=custom,
    )


def _ledger_row(*, executor_id, client_order_id='OID-1', connector_name='hyperliquid',
                trading_pair='HYPE-USDC', position_side=None,
                executed_amount_base='0.4', executed_amount_quote='40',
                cumulative_fee_paid_quote='0'):
    return SimpleNamespace(
        executor_id=executor_id,
        client_order_id=client_order_id,
        connector_name=connector_name,
        trading_pair=trading_pair,
        position_side=position_side,
        executed_amount_base=executed_amount_base,
        executed_amount_quote=executed_amount_quote,
        cumulative_fee_paid_quote=cumulative_fee_paid_quote,
    )


def _use_hold_ledger(rows):
    recorder = MagicMock()
    recorder.get_position_hold_ledger_entries_by_controller.return_value = list(rows)
    recorder.get_executors_by_controller.return_value = []
    return patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder)


def _held_bag(amount):
    return SimpleNamespace(
        connector_name='hyperliquid', trading_pair='HYPE-USDC', side=TradeType.BUY,
        amount=amount, breakeven_price=D('100'), cum_fees_quote=D('0'),
    )


def test_drawdown_and_bag_freeze_use_remaining_position_basis_not_intent_ceiling():
    instance = _controller(
        early_stop_drawdown_pct=0.08,
        bag_freeze_require_full_slots=False,
        bag_freeze_min_age_seconds=60,
        bag_freeze_distance_pct=0.04,
    )
    instance.executors_info = [_buy_executor(
        entry_price=D('120'),
        maximum_acceptable_price=D('120'),
        average_price=D('100'),
        filled_base=D('1'),
        remaining_base=D('1'),
        lifecycle='managing_fill',
        net_pnl_pct=D('-0.07'),
    )]
    assert instance._executor_remaining_position_basis(instance.executors_info[0]) == D('100')
    assert instance._executor_intent_ceiling(instance.executors_info[0]) == D('120')
    assert instance._check_active_executor_drawdown_stop(D('93')) == []
    assert instance._check_bag_freeze_trigger(D('97'), 1000.0) == []

    instance.executors_info = [_buy_executor(
        entry_price=D('120'),
        average_price=D('100'),
        filled_base=D('1'),
        remaining_base=D('1'),
        lifecycle='managing_fill',
        net_pnl_pct=D('-0.12'),
    )]
    drawdown = instance._check_active_executor_drawdown_stop(D('91'))
    assert len(drawdown) == 1
    assert isinstance(drawdown[0], StopExecutorAction)


def test_duplicate_entry_spacing_uses_working_order_then_acquired_price():
    instance = _controller()
    live = _buy_executor(
        entry_price=D('120'),
        working_price=D('99'),
        filled_base=D('0'),
        remaining_base=D('0'),
        lifecycle='open',
    )
    instance.executors_info = [live]
    assert instance._last_same_side_ref_price(TradeType.BUY) == D('99')
    assert instance._executor_working_order_price(live) == D('99')
    assert instance._executor_intent_ceiling(live) == D('120')

    owned = _buy_executor(
        entry_price=D('120'),
        working_price=D('99'),
        average_price=D('95'),
        filled_base=D('1'),
        remaining_base=D('1'),
        lifecycle='managing_fill',
    )
    instance.executors_info = [owned]
    assert instance._last_same_side_ref_price(TradeType.BUY) == D('95')


def test_averaging_down_uses_owned_basis_not_dca_label_for_independent_or_runner_entries():
    instance = _controller(min_signal_score=2, dca_score_boost=1, max_total_position_usd=D('5000'))
    instance.processed_data['signal_state'] = {
        'condition_ok': True,
        'signal_score': 2,
        'buy_entry_role': 'full',
    }

    unfilled = _buy_executor(filled_base=D('0'), remaining_base=D('0'), lifecycle='open', net_pnl_pct=D('0'))
    instance.executors_info = [unfilled]
    assert instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'ready'
    assert 'dca' not in str(instance._last_gate_reason[TradeType.BUY])

    instance.executors_info = []
    instance.positions_held = [SimpleNamespace(
        connector_name='hyperliquid', trading_pair='HYPE-USDC', side=TradeType.BUY,
        amount=D('.1'), breakeven_price=D('100'), cum_fees_quote=D('0'),
    )]
    assert instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'ready'
    assert instance._last_gate_reason[TradeType.BUY] != 'dca_score_low'

    underwater = _buy_executor(
        filled_base=D('1'), remaining_base=D('1'), lifecycle='managing_fill', net_pnl_pct=D('-0.1'),
        average_price=D('100'),
    )
    instance.positions_held = []
    instance.executors_info = [underwater]
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'averaging_score_low'
    assert instance._last_gate_reason[TradeType.BUY] != 'dca_score_low'

    instance.processed_data['signal_state']['buy_entry_role'] = 'runner'
    instance.processed_data['signal_state']['signal_score'] = 2
    scout = _buy_executor(
        executor_id='scout', level_id='scout', filled_base=D('1'), remaining_base=D('1'),
        lifecycle='managing_fill', net_pnl_pct=D('-0.1'), average_price=D('100'),
    )
    instance.executors_info = [scout]
    assert instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'ready'


def test_managing_fill_without_terminal_confirmation_keeps_unresolved_commitment():
    instance = _controller()
    executor = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.1'),
        remaining_base=D('0.1'),
        lifecycle='managing_fill',
    )
    instance.executors_info = [executor]
    assert instance._executor_owned_base(executor) == D('0.1')
    assert instance._executor_outstanding_base(executor) == D('0.9')
    assert instance._get_active_buy_executor_position_value_usd() == D('100')
    assert instance._get_total_position_value_usd() == D('100')


def test_terminated_leftover_owned_stays_reserved_until_hold_transfer():
    instance = _controller()
    leftover = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.4'),
        remaining_base=D('0.4'),
        lifecycle='managing_fill',
        is_active=False,
        is_done=True,
        accounting_owner='executor',
        terminal_exchange_timestamp=1000,
    )
    instance.executors_info = [leftover]
    assert instance._executor_owned_base(leftover) == D('0.4')
    assert instance._executor_outstanding_base(leftover) == D('0')
    assert instance._get_total_position_value_usd() == D('40')

    transferred = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.4'),
        remaining_base=D('0.4'),
        lifecycle='managing_fill',
        is_active=False,
        is_done=True,
        accounting_owner='held_position',
        receipt_remaining=D('0.4'),
        terminal_exchange_timestamp=1000,
        order_ids=['OID-held'],
        held_position_orders=[{
            'client_order_id': 'OID-held',
            'trade_type': 'BUY',
            'executed_amount_base': '0.4',
            'executed_amount_quote': '40',
            'cumulative_fee_paid_quote': '0',
        }],
    )
    instance.executors_info = [transferred]
    instance.positions_held = [_held_bag(D('0.4'))]
    with _use_hold_ledger([_ledger_row(executor_id=transferred.id, client_order_id='OID-held')]):
        assert instance._executor_owned_base(transferred) == D('0')
        assert instance._get_total_position_value_usd() == D('40')


def _completed_dust_exit():
    instance = _controller()
    executor = _buy_executor(
        amount=D('1'), filled_base=D('0.9981'), remaining_base=D('0'),
        is_active=False, is_done=True, accounting_owner='executor',
        receipt_remaining=D('0'), entry_gross_base=D('1'),
        lifecycle='managing_fill', terminal_exchange_timestamp=1000, close_timestamp=1000.0,
        order_ids=['buy-1', 'sell-1'],
    )
    executor.close_type = CloseType.TRAILING_STOP
    executor.custom_info['entry_acquisition'] = {'state': 'managing_fill'}
    executor.custom_info['close_order_filled_amount_base'] = D('0.998')
    executor.custom_info['close_quantity'] = {'quantity_increment': '0.001'}
    executor.custom_info['execution_receipt'].update({
        'entry_gross_quote': D('100'), 'exit_gross_base': D('0.998'),
        'exit_gross_quote': D('99.8'), 'residual_dust_base': D('0.0001'),
    })
    executor.custom_info['execution_order_snapshots'] = [
        {
            'client_order_id': 'buy-1', 'exchange_order_id': 'exchange-buy-1',
            'trading_pair': 'HYPE-USDC',
            'trade_type': 'BUY', 'last_state': '5',
            'executed_amount_base': '1', 'executed_amount_quote': '100',
            'order_fills': {'buy-fill': {
                'trade_id': 'buy-fill', 'client_order_id': 'buy-1',
                'exchange_order_id': 'exchange-buy-1', 'trading_pair': 'HYPE-USDC',
                'fill_base_amount': '1',
                'fill_price': '100', 'fill_quote_amount': '100',
                'fee': {'fee_type': 'AddedToCost', 'percent': '0', 'percent_token': 'HYPE',
                        'flat_fees': [{'token': 'HYPE', 'amount': '0.0019'}]},
            }},
        },
        {
            'client_order_id': 'sell-1', 'exchange_order_id': 'exchange-sell-1',
            'trading_pair': 'HYPE-USDC',
            'trade_type': 'SELL', 'last_state': '5',
            'executed_amount_base': '0.998', 'executed_amount_quote': '99.8',
            'order_fills': {'sell-fill': {
                'trade_id': 'sell-fill', 'client_order_id': 'sell-1',
                'exchange_order_id': 'exchange-sell-1', 'trading_pair': 'HYPE-USDC',
                'fill_base_amount': '0.998',
                'fill_price': '100', 'fill_quote_amount': '99.8',
                'fee': {'fee_type': 'DeductedFromReturns', 'percent': '0',
                        'percent_token': 'USDC', 'flat_fees': []},
            }},
        },
    ]
    instance.executors_info = [executor]
    return instance, executor


def test_completed_trailing_exit_with_native_fee_dust_retains_owned_exposure():
    instance, _ = _completed_dust_exit()

    assert instance._get_total_position_value_usd() == D('0.01')
    held, active, dust, free = instance._inventory_reconciliation_values(D('100'))
    assert held == D('0')
    assert active == D('0')
    assert dust == D('0.01')
    assert free == instance._get_base_balance() * D('100') - dust
    assert instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] != 'exposure_unavailable'


def test_native_persisted_scalar_buy_side_keeps_terminal_dust_and_visible_capital():
    instance, seed = _completed_dust_exit()
    seed.side = TradeType.BUY.value  # Native JSON-backed ExecutorInfo restores this scalar.
    assert instance._get_total_position_value_usd() == D('0.01')
    assert instance._filter_same_side(TradeType.BUY) == [seed]


@pytest.mark.parametrize('side,config_side', [
    (None, TradeType.BUY),
    (TradeType.SELL.value, TradeType.BUY),
    (TradeType.BUY.value, TradeType.SELL),
])
def test_invalid_native_buy_side_with_owned_history_cannot_vanish_from_capital(side, config_side):
    instance = _controller()
    seed = _buy_executor(
        filled_base=D('0.2'), remaining_base=D('0.2'),
        accounting_owner='executor', receipt_remaining=D('0.2'))
    seed.side = side
    seed.config.side = config_side
    instance.executors_info = []
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [seed]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)


def _distinct_dust_executor(seed, number):
    row = copy.deepcopy(seed)
    row.id = f'executor-{number}'
    row.config.id = row.id
    ids = []
    for order in row.custom_info['execution_order_snapshots']:
        order_id = f'{order["client_order_id"]}-{number}'
        order['client_order_id'] = order_id
        fill = next(iter(order['order_fills'].values()))
        fill['client_order_id'] = order_id
        fill['exchange_order_id'] = f'{fill["exchange_order_id"]}-{number}'
        fill['trade_id'] = f'{fill["trade_id"]}-{number}'
        order['exchange_order_id'] = fill['exchange_order_id']
        order['order_fills'] = {fill['trade_id']: fill}
        ids.append(order_id)
    row.custom_info['order_ids'] = ids
    return row


def test_completed_sublot_residues_aggregate_into_owned_capital_without_sell_authority():
    instance, seed = _completed_dust_exit()
    rows = [_distinct_dust_executor(seed, n) for n in range(11)]
    instance.executors_info = rows
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = rows
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert instance._get_total_position_value_usd() == D('0.11')
        assert instance._get_sellable_inventory_amount() == D('0')


def test_native_history_reserves_terminal_dust_omitted_from_recent_executor_report():
    instance, seed = _completed_dust_exit()
    instance.executors_info = []
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [_distinct_dust_executor(seed, n) for n in range(11)]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert instance._get_total_position_value_usd() == D('0.11')


def test_incomplete_older_terminal_receipt_cannot_be_silently_treated_as_flat():
    instance, seed = _completed_dust_exit()
    instance.executors_info = []
    del seed.custom_info['execution_receipt']['residual_dust_base']
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [seed]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)


def test_unseen_unfinished_buy_in_complete_history_blocks_entry():
    instance, seed = _completed_dust_exit()
    instance.executors_info = []
    seed.is_done = False
    seed.is_active = True
    seed.custom_info['execution_receipt']['residual_dust_base'] = D('0')
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [seed]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)


def test_unseen_positive_terminal_without_owner_blocks_entry():
    instance, seed = _completed_dust_exit()
    instance.executors_info = []
    seed.custom_info['execution_receipt'].pop('accounting_owner')
    seed.custom_info['execution_receipt']['residual_dust_base'] = D('0')
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [seed]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)


def test_duplicate_visible_executor_identity_blocks_entry():
    instance, seed = _completed_dust_exit()
    instance.executors_info = [seed, copy.deepcopy(seed)]
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [seed]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)


def test_recent_same_id_overlay_cannot_erase_persisted_terminal_dust():
    instance, seed = _completed_dust_exit()
    recent = copy.deepcopy(seed)
    recent.custom_info['execution_receipt'].pop('residual_dust_base')
    recent.custom_info['execution_receipt'].pop('accounting_owner')
    recent.custom_info['execution_receipt']['remaining_owned_base'] = D('0')
    recent.custom_info['remaining_position_amount_base'] = D('0')
    recent.custom_info['open_order_filled_amount_base'] = D('0')
    recent.custom_info['close_order_filled_amount_base'] = D('0')
    instance.executors_info = [recent]
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [seed]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)
        assert instance.get_status_summary()['gate'] == 'exposure-unavailable'


def test_recent_same_id_overlay_cannot_erase_persisted_unfinished_owned_base():
    instance = _controller()
    persisted = _buy_executor(
        filled_base=D('0.2'), remaining_base=D('0.2'),
        accounting_owner='executor', receipt_remaining=D('0.2'),
        order_ids=['owned-entry'])
    recent = copy.deepcopy(persisted)
    recent.is_done = True
    recent.is_active = False
    recent.custom_info['remaining_position_amount_base'] = D('0')
    recent.custom_info['open_order_filled_amount_base'] = D('0')
    recent.custom_info['execution_receipt'].clear()
    recent.custom_info['order_ids'] = []
    recent.custom_info['entry_lifecycle']['state'] = 'ended_no_fill'
    instance.executors_info = [recent]
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [persisted]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)


def test_recent_same_id_proven_complete_exit_can_release_persisted_owned_base():
    instance, recent = _completed_dust_exit()
    persisted = _buy_executor(
        filled_base=D('0.998'), remaining_base=D('0.998'),
        accounting_owner='executor', receipt_remaining=D('0.998'),
        order_ids=['buy-1'])
    recent.custom_info['open_order_filled_amount_base'] = D('0.998')
    recent.custom_info['execution_receipt']['entry_gross_base'] = D('1')
    recent.custom_info['execution_receipt']['residual_dust_base'] = D('0')
    recent.custom_info['execution_order_snapshots'][0]['order_fills']['buy-fill']['fee']['flat_fees'][0]['amount'] = '0.002'
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = [persisted]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert instance._get_total_position_value_usd() == D('0')
        recent.custom_info['execution_order_snapshots'][1]['order_fills']['sell-fill']['exchange_order_id'] = 'wrong'
        assert not instance._get_total_position_value_usd().is_finite()


def test_composed_status_reads_complete_native_history_once():
    instance, seed = _completed_dust_exit()
    recorder = MagicMock()
    recorder.get_executors_by_controller.side_effect = [[seed], []]
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        summary = instance.get_status_summary()
    assert recorder.get_executors_by_controller.call_count == 1
    assert summary['state'] != 'UNAVAILABLE'
    assert summary['exposure'] != 'unavailable'


def test_unavailable_complete_native_history_blocks_buy_and_is_reported_as_unavailable():
    instance, _ = _completed_dust_exit()
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=None):
        assert not instance.can_create_executor(1)
        summary = instance.get_status_summary()
    assert summary['state'] == 'UNAVAILABLE'
    assert summary['gate'] == 'exposure-unavailable'
    assert summary['exposure'] == 'unavailable'
    assert summary['plan_liquidity'] == 'idle $n/a'


@pytest.mark.parametrize('close_type', [CloseType.TAKE_PROFIT, CloseType.TIME_LIMIT, CloseType.STOP_LOSS])
def test_other_native_completed_spot_exits_retain_proven_dust(close_type):
    instance, row = _completed_dust_exit()
    row.close_type = close_type
    assert instance._get_total_position_value_usd() == D('0.01')


def test_zero_fill_cancelled_entry_predecessor_does_not_invent_a_fill():
    instance, row = _completed_dust_exit()
    row.custom_info['order_ids'].insert(0, 'cancelled-entry')
    row.custom_info['execution_order_snapshots'].insert(0, {
        'client_order_id': 'cancelled-entry', 'exchange_order_id': None,
        'trading_pair': 'HYPE-USDC', 'trade_type': 'BUY', 'last_state': '3',
        'executed_amount_base': '0', 'executed_amount_quote': '0', 'order_fills': {},
    })
    assert instance._get_total_position_value_usd() == D('0.01')


def test_native_base_fee_rebate_can_leave_proven_sublot_residue():
    instance, row = _completed_dust_exit()
    custom = row.custom_info
    custom['open_order_filled_amount_base'] = D('1.0019')
    custom['close_order_filled_amount_base'] = D('1.0018')
    buy, sell = custom['execution_order_snapshots']
    buy['order_fills']['buy-fill']['fee']['flat_fees'][0]['amount'] = '-0.0019'
    sell['executed_amount_base'] = '1.0018'
    sell['executed_amount_quote'] = '100.18'
    sell['order_fills']['sell-fill']['fill_base_amount'] = '1.0018'
    sell['order_fills']['sell-fill']['fill_quote_amount'] = '100.18'
    custom['execution_receipt']['exit_gross_base'] = D('1.0018')
    custom['execution_receipt']['exit_gross_quote'] = D('100.18')
    assert instance._get_total_position_value_usd() == D('0.01')


@pytest.mark.parametrize('sell_base_fee', ['0.00001', '-0.00001'])
def test_sell_base_fee_or_rebate_invalidates_executor_dust_receipt(sell_base_fee):
    instance, row = _completed_dust_exit()
    sell_fill = row.custom_info['execution_order_snapshots'][1]['order_fills']['sell-fill']
    sell_fill['fee']['flat_fees'] = [{'token': 'HYPE', 'amount': sell_base_fee}]

    assert not instance._get_total_position_value_usd().is_finite()


def test_sell_percent_fee_charged_in_base_invalidates_executor_dust_receipt():
    instance, row = _completed_dust_exit()
    sell_fill = row.custom_info['execution_order_snapshots'][1]['order_fills']['sell-fill']
    sell_fill['fee']['percent'] = '0.00001'
    sell_fill['fee']['percent_token'] = 'HYPE'

    assert not instance._get_total_position_value_usd().is_finite()


@pytest.mark.parametrize('sell_quote_fee', ['0.01', '-0.01'])
def test_sell_quote_fee_or_rebate_does_not_change_executor_base_dust(sell_quote_fee):
    instance, row = _completed_dust_exit()
    sell_fill = row.custom_info['execution_order_snapshots'][1]['order_fills']['sell-fill']
    sell_fill['fee']['flat_fees'] = [{'token': 'USDC', 'amount': sell_quote_fee}]

    assert instance._get_total_position_value_usd() == D('0.01')


def test_same_venue_order_claimed_as_buy_and_sell_does_not_release_exposure():
    instance, row = _completed_dust_exit()
    buy, sell = row.custom_info['execution_order_snapshots']
    sell['exchange_order_id'] = buy['exchange_order_id']
    sell['order_fills']['sell-fill']['exchange_order_id'] = buy['exchange_order_id']
    assert not instance._get_total_position_value_usd().is_finite()


@pytest.mark.parametrize('tamper', [
    'missing_snapshots', 'wrong_fill_fee', 'unfilled_exit', 'wrong_pair',
    'missing_receipt', 'active_executor', 'partial_executor', 'held_owner',
    'negative_dust', 'nonfinite_dust', 'excess_dust', 'coordinated_false_dust',
    'wrong_config_connector', 'perpetual_connector', 'wrong_config_pair',
    'wrong_config_side', 'leveraged_config', 'nonfinite_close_time',
    'nonfinite_other_token_fee', 'wrong_lot_receipt',
    'foreign_fill_pair', 'foreign_exchange_id', 'mismatched_trade_id',
    'duplicate_trade_id',
    'reconciliation_required',
])
def test_unverified_closed_exit_dust_keeps_exposure_unavailable(tamper):
    instance, executor = _completed_dust_exit()
    custom = executor.custom_info
    receipt = custom['execution_receipt']
    snapshots = custom['execution_order_snapshots']
    if tamper == 'missing_snapshots':
        del custom['execution_order_snapshots']
    elif tamper == 'wrong_fill_fee':
        snapshots[0]['order_fills']['buy-fill']['fee']['flat_fees'][0]['amount'] = '0.0001'
    elif tamper == 'unfilled_exit':
        snapshots[1]['last_state'] = '4'
    elif tamper == 'wrong_pair':
        snapshots[1]['trading_pair'] = 'OTHER-USDC'
    elif tamper == 'missing_receipt':
        del receipt['remaining_owned_base']
    elif tamper == 'active_executor':
        executor.is_active = True
    elif tamper == 'partial_executor':
        executor.is_done = False
    elif tamper == 'held_owner':
        receipt['accounting_owner'] = 'held_position'
    elif tamper == 'negative_dust':
        receipt['residual_dust_base'] = '-0.0001'
    elif tamper == 'nonfinite_dust':
        receipt['residual_dust_base'] = 'NaN'
    elif tamper == 'excess_dust':
        custom['open_order_filled_amount_base'] = D('0.9995')
        receipt['residual_dust_base'] = D('0.0015')
    elif tamper == 'coordinated_false_dust':
        custom['open_order_filled_amount_base'] = D('0.9982')
        receipt['residual_dust_base'] = D('0.0002')
    elif tamper == 'wrong_config_connector':
        executor.config.connector_name = 'okx_perpetual'
    elif tamper == 'perpetual_connector':
        object.__setattr__(instance.config, 'connector_name', 'okx_perpetual')
        executor.connector_name = 'okx_perpetual'
        executor.config.connector_name = 'okx_perpetual'
    elif tamper == 'wrong_config_pair':
        executor.config.trading_pair = 'OTHER-USDC'
    elif tamper == 'wrong_config_side':
        executor.config.side = TradeType.SELL
    elif tamper == 'leveraged_config':
        executor.config.leverage = 2
    elif tamper == 'nonfinite_close_time':
        executor.close_timestamp = float('nan')
    elif tamper == 'nonfinite_other_token_fee':
        snapshots[0]['order_fills']['buy-fill']['fee']['flat_fees'].append(
            {'token': 'USDC', 'amount': 'NaN'})
    elif tamper == 'wrong_lot_receipt':
        custom['close_quantity']['quantity_increment'] = '0.01'
    elif tamper == 'foreign_fill_pair':
        snapshots[0]['order_fills']['buy-fill']['trading_pair'] = 'OTHER-USDC'
    elif tamper == 'foreign_exchange_id':
        snapshots[0]['order_fills']['buy-fill']['exchange_order_id'] = 'foreign-order'
    elif tamper == 'mismatched_trade_id':
        snapshots[0]['order_fills']['buy-fill']['trade_id'] = 'foreign-fill'
    elif tamper == 'duplicate_trade_id':
        fill = snapshots[1]['order_fills'].pop('sell-fill')
        fill['trade_id'] = 'buy-fill'
        snapshots[1]['order_fills']['buy-fill'] = fill
    elif tamper == 'reconciliation_required':
        custom['entry_lifecycle']['state'] = 'reconciliation_required'
        custom.setdefault('entry_acquisition', {})['state'] = 'reconciliation_required'

    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_position_hold_without_held_bag_refuses_new_buy():
    instance = _controller()
    transferring = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.4'),
        remaining_base=D('0.4'),
        lifecycle='managing_fill',
        is_active=True,
        is_done=False,
        accounting_owner='held_position',
        receipt_remaining=D('0.4'),
        terminal_exchange_timestamp=1000,
    )
    instance.executors_info = [transferring]
    instance.positions_held = []
    assert instance._executor_owned_base(transferring) is None
    assert instance._executor_outstanding_base(transferring) is None
    assert instance._executor_counts_toward_buy_capital(transferring)
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def _held_fill(order_id, base, quote='40', fee='0'):
    return {
        'client_order_id': order_id,
        'trade_type': 'BUY',
        'executed_amount_base': str(base),
        'executed_amount_quote': str(quote),
        'cumulative_fee_paid_quote': str(fee),
    }


def _hold_claimant(*, executor_id, leftover=D('0.4'), is_active=True, is_done=False, close_timestamp=None):
    return _buy_executor(
        executor_id=executor_id,
        amount=D('1'),
        entry_price=D('100'),
        filled_base=leftover,
        remaining_base=leftover,
        lifecycle='managing_fill',
        is_active=is_active,
        is_done=is_done,
        accounting_owner='held_position',
        receipt_remaining=leftover,
        terminal_exchange_timestamp=1000,
        close_timestamp=close_timestamp,
        held_position_orders=[{
            'client_order_id': 'OID-1',
            'trade_type': 'BUY',
            'executed_amount_base': str(leftover),
            'executed_amount_quote': str(leftover * D('100')),
            'cumulative_fee_paid_quote': '0',
        }],
    )


def test_position_hold_undersized_or_prior_bag_refuses_new_buy():
    instance = _controller()
    transferring = _hold_claimant(executor_id='hold-a')
    instance.executors_info = [transferring]
    instance.positions_held = [_held_bag(D('0.1'))]
    assert instance._executor_owned_base(transferring) is None
    assert instance._executor_outstanding_base(transferring) is None
    assert instance._executor_counts_toward_buy_capital(transferring)
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'

    first = _hold_claimant(
        executor_id='hold-a', is_active=False, is_done=True, close_timestamp=100.0,
    )
    second = _hold_claimant(
        executor_id='hold-b', is_active=False, is_done=True, close_timestamp=200.0,
    )
    instance.executors_info = [second, first]
    instance.positions_held = [_held_bag(D('0.4'))]
    with _use_hold_ledger([
        _ledger_row(executor_id='hold-a'),
        _ledger_row(executor_id='hold-b'),
    ]):
        assert instance._executor_owned_base(first) == D('0')
        assert instance._executor_owned_base(second) == D('0')
        assert not instance._executor_counts_toward_buy_capital(first)
        assert not instance._executor_counts_toward_buy_capital(second)
        assert instance._get_total_position_value_usd() == D('40')
        assert instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] != 'exposure_unavailable'


def test_new_active_hold_before_stored_prior_hold_refuses_new_buy():
    instance = _controller()
    new_hold = _hold_claimant(executor_id='new-hold')
    prior = _hold_claimant(
        executor_id='prior-hold', is_active=False, is_done=True, close_timestamp=100.0,
    )
    instance.executors_info = [new_hold, prior]
    instance.positions_held = [_held_bag(D('0.4'))]
    with _use_hold_ledger([_ledger_row(executor_id='prior-hold')]):
        assert instance._executor_owned_base(new_hold) is None
        assert instance._executor_owned_base(prior) == D('0')
        assert instance._executor_counts_toward_buy_capital(new_hold)
        assert not instance._executor_counts_toward_buy_capital(prior)
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_new_hold_without_prior_claimant_refuses_leftover_sized_bag():
    instance = _controller()
    new_hold = _hold_claimant(executor_id='new-hold')
    instance.executors_info = [new_hold]
    instance.positions_held = [_held_bag(D('0.4'))]
    assert instance._executor_owned_base(new_hold) is None
    assert instance._executor_outstanding_base(new_hold) is None
    assert instance._executor_counts_toward_buy_capital(new_hold)
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_hold_transfer_survives_partial_bag_sale():
    instance = _controller()
    transferred = _hold_claimant(
        executor_id='held-ex', leftover=D('0.4'), is_active=False, is_done=True,
    )
    transferred.custom_info['held_position_orders'] = [_held_fill('OID-this', '0.4', '40')]
    instance.executors_info = [transferred]
    instance.positions_held = [_held_bag(D('0.3'))]
    with _use_hold_ledger([_ledger_row(executor_id='held-ex', client_order_id='OID-this')]):
        assert instance._executor_hold_released_to_bag(transferred)
        assert instance._executor_owned_base(transferred) == D('0')
        assert instance._get_total_position_value_usd() == D('30')
        assert instance._get_total_position_value_usd().is_finite()


def test_hold_transfer_survives_full_bag_disposal():
    instance = _controller()
    transferred = _hold_claimant(
        executor_id='held-ex', leftover=D('0.4'), is_active=False, is_done=True,
    )
    transferred.custom_info['held_position_orders'] = [_held_fill('OID-this', '0.4', '40')]
    instance.executors_info = [transferred]
    instance.positions_held = []
    with _use_hold_ledger([_ledger_row(executor_id='held-ex', client_order_id='OID-this')]):
        assert instance._executor_hold_released_to_bag(transferred)
        assert instance._executor_owned_base(transferred) == D('0')
        assert instance._get_total_position_value_usd() == D('0')
        assert instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] != 'exposure_unavailable'


def test_unrelated_equal_bag_does_not_release_new_claimant():
    instance = _controller()
    new_hold = _hold_claimant(
        executor_id='new-leftover', leftover=D('0.4'), is_active=False, is_done=True,
    )
    instance.executors_info = [new_hold]
    instance.positions_held = [_held_bag(D('0.4'))]
    with _use_hold_ledger([_ledger_row(executor_id='other-ex', client_order_id='OID-other')]):
        assert instance._executor_owned_base(new_hold) is None
        assert instance._executor_outstanding_base(new_hold) is None
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_incomplete_hold_ledger_does_not_release_whole_executor():
    instance = _controller()
    transferred = _hold_claimant(
        executor_id='held-ex', leftover=D('0.4'), is_active=False, is_done=True,
    )
    transferred.custom_info['held_position_orders'] = [
        _held_fill('buy-1', '0.1', '10'),
        _held_fill('buy-2', '0.3', '30'),
    ]
    instance.executors_info = [transferred]
    instance.positions_held = [_held_bag(D('0.4'))]
    with _use_hold_ledger([
        _ledger_row(executor_id='held-ex', client_order_id='buy-1',
                    executed_amount_base='0.1', executed_amount_quote='10'),
    ]):
        assert instance._executor_hold_released_to_bag(transferred) is False
        assert instance._executor_owned_base(transferred) is None
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_stale_hold_ledger_quantity_does_not_release_whole_executor():
    instance = _controller()
    transferred = _hold_claimant(
        executor_id='held-ex', leftover=D('0.4'), is_active=False, is_done=True,
    )
    transferred.custom_info['held_position_orders'] = [_held_fill('buy-1', '0.4', '40')]
    instance.executors_info = [transferred]
    instance.positions_held = [_held_bag(D('0.4'))]
    with _use_hold_ledger([
        _ledger_row(executor_id='held-ex', client_order_id='buy-1',
                    executed_amount_base='0.1', executed_amount_quote='10'),
    ]):
        assert instance._executor_hold_released_to_bag(transferred) is False
        assert instance._executor_owned_base(transferred) is None
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_gross_fill_above_authorized_makes_outstanding_unavailable():
    instance = _controller()
    executor = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.4'),
        remaining_base=D('0.4'),
        lifecycle='managing_fill',
        native_order_state='OPEN',
        cumulative_filled_base=D('2'),
    )
    instance.executors_info = [executor]
    assert instance._executor_owned_base(executor) == D('0.4')
    assert instance._executor_outstanding_base(executor) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_fee_dust_gross_vs_net_keeps_finite_outstanding():
    instance = _controller()
    executor = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.4'),
        remaining_base=D('0.4'),
        lifecycle='managing_fill',
        native_order_state='OPEN',
        cumulative_filled_base=D('0.41'),
    )
    instance.executors_info = [executor]
    assert instance._executor_owned_base(executor) == D('0.4')
    assert instance._executor_outstanding_base(executor) == D('0.59')
    assert instance._get_total_position_value_usd().is_finite()


def test_missing_or_failed_hold_ledger_does_not_release():
    instance = _controller()
    transferred = _hold_claimant(executor_id='held-ex', is_active=False, is_done=True)
    instance.executors_info = [transferred]
    instance.positions_held = [_held_bag(D('0.4'))]
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=None):
        assert instance._executor_hold_released_to_bag(transferred) is False
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'
    recorder = MagicMock()
    recorder.get_position_hold_ledger_entries_by_controller.side_effect = RuntimeError('db')
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert instance._executor_hold_released_to_bag(transferred) is False
        assert not instance._get_total_position_value_usd().is_finite()


def test_missing_or_conflicting_owned_receipts_are_unavailable():
    instance = _controller()
    missing = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        include_filled=False,
        include_remaining=False,
        lifecycle='open',
    )
    instance.executors_info = [missing]
    assert instance._executor_owned_base(missing) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'

    conflicting = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.2'),
        remaining_base=D('0.2'),
        receipt_remaining=D('0.1'),
        lifecycle='managing_fill',
        terminal_exchange_timestamp=1000,
    )
    instance.executors_info = [conflicting]
    assert instance._executor_owned_base(conflicting) is None
    assert not instance._get_total_position_value_usd().is_finite()

    over_authorized = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('1.1'),
        remaining_base=D('1.1'),
        lifecycle='managing_fill',
        terminal_exchange_timestamp=1000,
    )
    instance.executors_info = [over_authorized]
    assert instance._executor_owned_base(over_authorized) is None
    assert not instance._get_total_position_value_usd().is_finite()

    non_finite = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.1'),
        remaining_base=D('NaN'),
        lifecycle='managing_fill',
        terminal_exchange_timestamp=1000,
    )
    instance.executors_info = [non_finite]
    assert instance._executor_owned_base(non_finite) is None
    assert not instance._get_total_position_value_usd().is_finite()


def test_invalid_cumulative_gross_makes_outstanding_unavailable():
    instance = _controller()
    for gross in ('NaN', 'Infinity', 'broken'):
        executor = _buy_executor(
            amount=D('1'),
            entry_price=D('100'),
            filled_base=D('0.4'),
            remaining_base=D('0.4'),
            lifecycle='managing_fill',
            native_order_state='OPEN',
            cumulative_filled_base=gross,
        )
        instance.executors_info = [executor]
        assert instance._executor_outstanding_base(executor) is None
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_conflicting_native_gross_receipts_make_outstanding_unavailable():
    instance = _controller()
    executor = _buy_executor(
        amount=D('1'),
        entry_price=D('100'),
        filled_base=D('0.4'),
        remaining_base=D('0.4'),
        lifecycle='managing_fill',
        native_order_state='OPEN',
        cumulative_filled_base=D('0.9'),
        entry_gross_base=D('0.4'),
    )
    instance.executors_info = [executor]
    assert instance._executor_outstanding_base(executor) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_over_authorized_order_executor_executed_amount_is_unavailable():
    instance = _controller()
    executor = SimpleNamespace(
        id='order-ex',
        is_active=True,
        is_done=False,
        connector_name='hyperliquid',
        trading_pair='HYPE-USDC',
        side=TradeType.BUY,
        timestamp=1.0,
        net_pnl_pct=D('0'),
        config=SimpleNamespace(type='order_executor', amount=D('1')),
        custom_info={'executed_amount_base': D('2'), 'side': TradeType.BUY},
    )
    instance.executors_info = [executor]
    assert instance._executor_outstanding_base(executor) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_backtest_signal_frame_replays_two_entry_exit_cycles_chronologically():
    instance = _controller(
        rsi_length=4,
        min_signal_score=1,
        use_split_entries=True,
        buy_confirmation_mode='rebound_confirm',
        scout_entry_fraction=0.35,
        sell_only_if_profitable=True,
        min_profit_pct_for_sell=D('0.002'),
    )
    df = pd.DataFrame({
        'timestamp': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        'close': [100.0, 100.5, 101.0, 100.0, 100.0, 100.5, 101.0],
        'RSI_4': [20.0, 22.0, 80.0, 50.0, 20.0, 22.0, 80.0],
        'BBL_20_2.0': [90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0],
        'MACDh_12_26_9': [0.0, 0.2, 0.1, 0.0, 0.0, 0.2, 0.1],
        'EMA_9': [102.0, 102.0, 102.0, 102.0, 102.0, 102.0, 102.0],
    })
    atr = pd.Series([1.0] * len(df), index=df.index)
    reversal = pd.Series([True, False, False, False, True, False, False], index=df.index)
    with patch.object(instance, '_compute_raw_buy_reversal_series', return_value=reversal), \
            patch.object(instance, '_compute_atr_series', return_value=(atr, 1.0)):
        frame = instance._build_backtest_signal_frame(df, rsi_buy_eff=35.0, rsi_sell_eff=75.0)
    signals = frame['signal'].tolist()
    assert signals[0] == 1
    assert frame['signal_state'].iloc[0]['buy_entry_role'] == 'scout'
    assert signals[2] == -1
    assert signals[4] == 1
    assert frame['signal_state'].iloc[4]['buy_entry_role'] == 'scout'
    assert signals[6] == -1
