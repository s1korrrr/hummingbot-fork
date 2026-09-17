import asyncio
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from controllers.directional_trading.hl_rsi import HLRSIController, HLRSIControllerConfig
from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction


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
    include_filled=True,
    include_remaining=True,
    native_order_state=None,
    terminal_exchange_timestamp=None,
    close_timestamp=None,
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
        acquisition = {
            'state': lifecycle,
            'remaining_base': str(max(amount - (filled_base if include_filled else D('0')), D('0'))),
            'cumulative_filled_base': str(filled_base if include_filled else D('0')),
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
    if accounting_owner is not None or receipt_remaining is not None:
        receipt = {}
        if accounting_owner is not None:
            receipt['accounting_owner'] = accounting_owner
        if receipt_remaining is not None:
            receipt['remaining_owned_base'] = receipt_remaining
        custom['execution_receipt'] = receipt
    return SimpleNamespace(
        id=executor_id,
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
            amount=amount,
            entry_price=entry_price,
            price=working_price,
            maximum_acceptable_price=maximum_acceptable_price or entry_price,
            level_id=level_id,
        ),
        custom_info=custom,
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
    )
    instance.executors_info = [transferred]
    instance.positions_held = [SimpleNamespace(
        connector_name='hyperliquid', trading_pair='HYPE-USDC', side=TradeType.BUY,
        amount=D('0.4'), breakeven_price=D('100'), cum_fees_quote=D('0'),
    )]
    assert instance._executor_owned_base(transferred) == D('0')
    assert instance._get_total_position_value_usd() == D('40')


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
    )


def test_position_hold_undersized_or_prior_bag_refuses_new_buy():
    instance = _controller()
    transferring = _hold_claimant(executor_id='hold-a')
    instance.executors_info = [transferring]
    instance.positions_held = [SimpleNamespace(
        connector_name='hyperliquid', trading_pair='HYPE-USDC', side=TradeType.BUY,
        amount=D('0.1'), breakeven_price=D('100'), cum_fees_quote=D('0'),
    )]
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
    instance.positions_held = [SimpleNamespace(
        connector_name='hyperliquid', trading_pair='HYPE-USDC', side=TradeType.BUY,
        amount=D('0.4'), breakeven_price=D('100'), cum_fees_quote=D('0'),
    )]
    assert instance._executor_owned_base(first) == D('0')
    assert instance._executor_owned_base(second) is None
    assert not instance._executor_counts_toward_buy_capital(first)
    assert instance._executor_counts_toward_buy_capital(second)
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_new_active_hold_before_stored_prior_hold_refuses_new_buy():
    instance = _controller()
    new_hold = _hold_claimant(executor_id='new-hold')
    prior = _hold_claimant(
        executor_id='prior-hold', is_active=False, is_done=True, close_timestamp=100.0,
    )
    instance.executors_info = [new_hold, prior]
    instance.positions_held = [SimpleNamespace(
        connector_name='hyperliquid', trading_pair='HYPE-USDC', side=TradeType.BUY,
        amount=D('0.4'), breakeven_price=D('100'), cum_fees_quote=D('0'),
    )]
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
    instance.positions_held = [SimpleNamespace(
        connector_name='hyperliquid', trading_pair='HYPE-USDC', side=TradeType.BUY,
        amount=D('0.4'), breakeven_price=D('100'), cum_fees_quote=D('0'),
    )]
    assert instance._executor_owned_base(new_hold) is None
    assert instance._executor_outstanding_base(new_hold) is None
    assert instance._executor_counts_toward_buy_capital(new_hold)
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


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
