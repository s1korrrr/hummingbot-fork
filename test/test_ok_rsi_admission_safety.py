import asyncio
from contextlib import ExitStack
from decimal import Decimal as D
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor, tracked
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from controllers.directional_trading.ok_rsi import OKRSIController, OKRSIControllerConfig
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


@pytest.fixture(autouse=True)
def _complete_native_history():
    recorder = MagicMock()
    recorder.get_executors_by_controller.return_value = []
    recorder.get_position_hold_ledger_entries_by_controller.return_value = []
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        yield recorder


def controller():
    provider = MagicMock()
    provider.time.return_value = 1000
    provider.get_price_by_type.return_value = D('100')
    provider.get_available_balance.return_value = D('1000')
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_price_increment=D('.01'), min_order_size=D('.001'),
        min_base_amount_increment=D('.001'), min_notional_size=D('1'), min_order_value=D('1'))
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
    config = OKRSIControllerConfig(id='main', connector_name='okx', trading_pair='ETH-USDT',
                                   use_adaptive_trailing=False, strict_hmm_mode=False,
                                   usd_per_entry=D('25'), max_total_position_usd=D('100'))
    result = OKRSIController(config, provider, asyncio.Queue())
    result.processed_data = {'timestamp': 1000, 'signal': 1, 'data_health': {'ready': True},
                             'signal_state': {'condition_ok': True, 'size_multiplier': 1}}
    result._status = RunnableStatus.RUNNING
    return result


def _ledger_row(*, executor_id, client_order_id=None, connector_name='okx',
                trading_pair='ETH-USDT', position_side=None,
                executed_amount_base='.4', executed_amount_quote='40',
                cumulative_fee_paid_quote='0'):
    return SimpleNamespace(
        executor_id=executor_id,
        client_order_id=client_order_id or f'held-{executor_id}',
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
    return SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                           amount=amount, breakeven_price=D('100'), cum_fees_quote=D('0'))


def test_pending_entry_reserves_its_full_commitment_including_during_shutdown():
    instance = controller()
    instance.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                                               amount=D('.74'), breakeven_price=D('100'), cum_fees_quote=D('0'))]
    executor = make_executor()
    executor.config.amount = D('.25')
    executor._open_order = tracked('pending', TradeType.BUY, '.01', OrderState.PARTIALLY_FILLED)
    instance.executors_info = [executor.executor_info]
    assert instance._get_total_position_value_usd() == D('99')
    assert not instance.can_create_executor(1)
    executor._status = RunnableStatus.SHUTTING_DOWN
    instance.executors_info = [executor.executor_info]
    assert instance._get_total_position_value_usd() == D('99')


def test_terminally_canceled_remainder_releases_only_resolved_commitment():
    instance = controller()
    instance.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                                               amount=D('.74'), breakeven_price=D('100'), cum_fees_quote=D('0'))]
    executor = make_executor()
    executor.config.amount = D('.25')
    executor._open_order = tracked('canceled', TradeType.BUY, '.01', OrderState.CANCELED)
    executor._entry_lifecycle['state'] = 'managing_fill'
    executor._entry_lifecycle['terminal_exchange_timestamp'] = 1000
    instance.executors_info = [executor.executor_info]
    assert instance._get_total_position_value_usd() == D('75')


def test_managing_fill_canceled_open_order_state_releases_remainder_without_timestamp():
    instance = controller()
    instance.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                                               amount=D('.74'), breakeven_price=D('100'), cum_fees_quote=D('0'))]
    executor = make_executor()
    executor.config.amount = D('.25')
    executor._open_order = tracked('canceled', TradeType.BUY, '.01', OrderState.CANCELED)
    executor._entry_lifecycle['state'] = 'managing_fill'
    info = executor.executor_info
    assert info.custom_info['open_order_state'] == 'CANCELED'
    assert 'terminal_exchange_timestamp' not in (info.custom_info.get('entry_lifecycle') or {})
    instance.executors_info = [info]
    assert instance._get_total_position_value_usd() == D('75')


def test_managing_fill_label_without_terminal_confirmation_keeps_unresolved_remainder():
    instance = controller()
    executor = make_executor()
    executor.config.amount = D('1')
    executor._open_order = tracked('working', TradeType.BUY, '.1', OrderState.PARTIALLY_FILLED)
    executor._entry_lifecycle['state'] = 'managing_fill'
    instance.executors_info = [executor.executor_info]
    assert instance._get_total_position_value_usd() == D('100')
    assert not instance.can_create_executor(1)


def test_terminated_leftover_owned_is_reserved_until_hold_transfer():
    instance = controller()
    instance.config.max_total_position_usd = D('1000')
    leftover = make_executor()
    leftover.config.amount = D('.4')
    leftover._open_order = tracked('owned', TradeType.BUY, '.4', OrderState.FILLED)
    leftover._status = RunnableStatus.TERMINATED
    instance.executors_info = [leftover.executor_info]
    assert instance._get_total_position_value_usd() == D('40')

    transferred = make_executor()
    transferred.config.amount = D('.4')
    transferred._open_order = tracked('held', TradeType.BUY, '.4', OrderState.FILLED)
    transferred._status = RunnableStatus.TERMINATED
    transferred.close_type = CloseType.POSITION_HOLD
    transferred._held_position_orders = [transferred._open_order.order.to_json()]
    info = transferred.executor_info
    instance.executors_info = [info]
    instance.positions_held = [_held_bag(D('.4'))]
    with _use_hold_ledger([_ledger_row(executor_id=info.id, client_order_id='held')]):
        assert instance._get_total_position_value_usd() == D('40')


def test_position_hold_without_held_bag_refuses_new_buy():
    instance = controller()
    transferring = make_executor()
    transferring.config.amount = D('.4')
    transferring._open_order = tracked('held', TradeType.BUY, '.4', OrderState.FILLED)
    transferring._status = RunnableStatus.SHUTTING_DOWN
    transferring.close_type = CloseType.POSITION_HOLD
    instance.executors_info = [transferring.executor_info]
    instance.positions_held = []
    assert transferring.executor_info.custom_info['execution_receipt']['accounting_owner'] == 'held_position'
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def _hold_transferring(*, leftover=D('.4')):
    transferring = make_executor()
    transferring.config.amount = leftover
    transferring._open_order = tracked('held', TradeType.BUY, str(leftover), OrderState.FILLED)
    transferring._status = RunnableStatus.SHUTTING_DOWN
    transferring.close_type = CloseType.POSITION_HOLD
    return transferring


def _hold_done(*, leftover=D('.4'), executor_id='hold-done', close_timestamp=1000.0):
    transferring = make_executor()
    transferring.config.amount = leftover
    transferring._open_order = tracked(f'held-{executor_id}', TradeType.BUY, str(leftover), OrderState.FILLED)
    transferring._status = RunnableStatus.TERMINATED
    transferring.close_type = CloseType.POSITION_HOLD
    transferring.close_timestamp = close_timestamp
    transferring._held_position_orders = [transferring._open_order.order.to_json()]
    return transferring.executor_info.model_copy(update={'id': executor_id})


def test_position_hold_undersized_or_prior_bag_refuses_new_buy():
    instance = controller()
    transferring = _hold_transferring()
    transferring_info = transferring.executor_info
    instance.executors_info = [transferring_info]
    instance.positions_held = [_held_bag(D('.1'))]
    assert transferring_info.custom_info['execution_receipt']['accounting_owner'] == 'held_position'
    assert instance._executor_owned_base(transferring_info) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'

    first_info = _hold_done(executor_id='hold-a', close_timestamp=100.0)
    second_info = _hold_done(executor_id='hold-b', close_timestamp=200.0)
    instance.executors_info = [second_info, first_info]
    instance.positions_held = [_held_bag(D('.4'))]
    instance.config.executor_price_gap_threshold = D('0')
    with _use_hold_ledger([
        _ledger_row(executor_id='hold-a'),
        _ledger_row(executor_id='hold-b'),
    ]):
        assert instance._executor_owned_base(first_info) == D('0')
        assert instance._executor_owned_base(second_info) == D('0')
        assert not instance._executor_counts_toward_buy_capital(first_info)
        assert not instance._executor_counts_toward_buy_capital(second_info)
        assert instance._get_total_position_value_usd() == D('40')
        assert instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] != 'exposure_unavailable'


def test_new_shutting_down_hold_before_stored_prior_hold_refuses_new_buy():
    instance = controller()
    new_info = _hold_transferring().executor_info
    prior_info = _hold_done(executor_id='prior-hold', close_timestamp=100.0)
    instance.executors_info = [new_info, prior_info]
    instance.positions_held = [_held_bag(D('.4'))]
    with _use_hold_ledger([_ledger_row(executor_id='prior-hold')]):
        assert instance._executor_owned_base(new_info) is None
        assert instance._executor_owned_base(prior_info) == D('0')
        assert instance._executor_counts_toward_buy_capital(new_info)
        assert not instance._executor_counts_toward_buy_capital(prior_info)
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_new_hold_without_prior_claimant_refuses_leftover_sized_bag():
    instance = controller()
    new_info = _hold_transferring().executor_info
    instance.executors_info = [new_info]
    instance.positions_held = [_held_bag(D('.4'))]
    assert new_info.custom_info['execution_receipt']['accounting_owner'] == 'held_position'
    assert instance._executor_owned_base(new_info) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_hold_transfer_survives_partial_bag_sale():
    instance = controller()
    instance.config.max_total_position_usd = D('1000')
    info = _hold_done(executor_id='held-ex')
    instance.executors_info = [info]
    instance.positions_held = [_held_bag(D('.3'))]
    with _use_hold_ledger([_ledger_row(executor_id='held-ex')]):
        assert instance._executor_hold_released_to_bag(info)
        assert instance._executor_owned_base(info) == D('0')
        assert instance._get_total_position_value_usd() == D('30')
        assert instance._get_total_position_value_usd().is_finite()


def test_hold_transfer_survives_full_bag_disposal():
    instance = controller()
    instance.config.max_total_position_usd = D('1000')
    info = _hold_done(executor_id='held-ex')
    instance.executors_info = [info]
    instance.positions_held = []
    with _use_hold_ledger([_ledger_row(executor_id='held-ex')]):
        assert instance._executor_hold_released_to_bag(info)
        assert instance._executor_owned_base(info) == D('0')
        assert instance._get_total_position_value_usd() == D('0')
        assert instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] != 'exposure_unavailable'


def test_unrelated_equal_bag_does_not_release_new_claimant():
    instance = controller()
    new_info = _hold_done(executor_id='new-leftover')
    instance.executors_info = [new_info]
    instance.positions_held = [_held_bag(D('.4'))]
    with _use_hold_ledger([_ledger_row(executor_id='other-ex', client_order_id='other-order')]):
        assert instance._executor_owned_base(new_info) is None
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


def test_incomplete_hold_ledger_does_not_release_whole_executor():
    instance = controller()
    info = _hold_done(executor_id='held-ex')
    info.custom_info['held_position_orders'] = [
        _held_fill('buy-1', '0.1', '10'),
        _held_fill('buy-2', '0.3', '30'),
    ]
    instance.executors_info = [info]
    instance.positions_held = [_held_bag(D('.4'))]
    with _use_hold_ledger([
        _ledger_row(executor_id='held-ex', client_order_id='buy-1',
                    executed_amount_base='0.1', executed_amount_quote='10'),
    ]):
        assert instance._executor_hold_released_to_bag(info) is False
        assert instance._executor_owned_base(info) is None
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_stale_hold_ledger_quantity_does_not_release_whole_executor():
    instance = controller()
    info = _hold_done(executor_id='held-ex')
    info.custom_info['held_position_orders'] = [_held_fill('buy-1', '0.4', '40')]
    instance.executors_info = [info]
    instance.positions_held = [_held_bag(D('.4'))]
    with _use_hold_ledger([
        _ledger_row(executor_id='held-ex', client_order_id='buy-1',
                    executed_amount_base='0.1', executed_amount_quote='10'),
    ]):
        assert instance._executor_hold_released_to_bag(info) is False
        assert instance._executor_owned_base(info) is None
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_gross_fill_above_authorized_makes_outstanding_unavailable():
    instance = controller()
    executor = make_executor()
    executor.config.amount = D('1')
    executor._open_order = tracked('working', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    executor._entry_lifecycle['state'] = 'managing_fill'
    info = executor.executor_info
    info.custom_info['entry_acquisition'] = {
        'cumulative_filled_base': '2',
        'native_order_state': 'OPEN',
    }
    instance.executors_info = [info]
    assert instance._executor_owned_base(info) == D('.4')
    assert instance._executor_outstanding_base(info) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_fee_dust_gross_vs_net_keeps_finite_outstanding():
    instance = controller()
    executor = make_executor()
    executor.config.amount = D('1')
    executor._open_order = tracked('working', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    executor._entry_lifecycle['state'] = 'managing_fill'
    info = executor.executor_info
    info.custom_info['entry_acquisition'] = {
        'cumulative_filled_base': '0.41',
        'native_order_state': 'OPEN',
    }
    info.custom_info['execution_receipt']['entry_gross_base'] = D('0.41')
    instance.executors_info = [info]
    assert instance._executor_owned_base(info) == D('.4')
    assert instance._executor_outstanding_base(info) == D('0.59')
    assert instance._get_total_position_value_usd().is_finite()


def test_invalid_cumulative_gross_makes_outstanding_unavailable():
    instance = controller()
    for gross in ('NaN', 'Infinity', 'broken'):
        executor = make_executor()
        executor.config.amount = D('1')
        executor._open_order = tracked('working', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
        executor._entry_lifecycle['state'] = 'managing_fill'
        info = executor.executor_info
        info.custom_info['entry_acquisition'] = {
            'cumulative_filled_base': gross,
            'native_order_state': 'OPEN',
        }
        instance.executors_info = [info]
        assert instance._executor_outstanding_base(info) is None
        assert not instance._get_total_position_value_usd().is_finite()
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_conflicting_native_gross_receipts_make_outstanding_unavailable():
    instance = controller()
    executor = make_executor()
    executor.config.amount = D('1')
    executor._open_order = tracked('working', TradeType.BUY, '.4', OrderState.PARTIALLY_FILLED)
    executor._entry_lifecycle['state'] = 'managing_fill'
    info = executor.executor_info
    info.custom_info['entry_acquisition'] = {
        'cumulative_filled_base': '0.9',
        'native_order_state': 'OPEN',
    }
    info.custom_info['execution_receipt']['entry_gross_base'] = D('0.4')
    instance.executors_info = [info]
    assert instance._executor_outstanding_base(info) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_over_authorized_order_executor_executed_amount_is_unavailable():
    instance = controller()
    info = SimpleNamespace(
        id='order-ex',
        is_active=True,
        is_done=False,
        connector_name='okx',
        trading_pair='ETH-USDT',
        side=TradeType.BUY,
        timestamp=1.0,
        net_pnl_pct=D('0'),
        config=SimpleNamespace(type='order_executor', amount=D('1')),
        custom_info={'executed_amount_base': D('2'), 'side': TradeType.BUY},
    )
    instance.executors_info = [info]
    assert instance._executor_outstanding_base(info) is None
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)
    assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'


def test_missing_or_failed_hold_ledger_does_not_release():
    instance = controller()
    info = _hold_done(executor_id='held-ex')
    instance.executors_info = [info]
    instance.positions_held = [_held_bag(D('.4'))]
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=None):
        assert instance._executor_hold_released_to_bag(info) is False
        assert not instance.can_create_executor(1)
        assert instance._last_gate_reason[TradeType.BUY] == 'exposure_unavailable'
    recorder = MagicMock()
    recorder.get_position_hold_ledger_entries_by_controller.side_effect = RuntimeError('db')
    with patch.object(MarketsRecorder, 'get_existing_instance', return_value=recorder):
        assert instance._executor_hold_released_to_bag(info) is False
        assert not instance._get_total_position_value_usd().is_finite()


def test_transferring_owned_inventory_between_native_owners_preserves_buying_capacity():
    instance = controller()
    instance.config.max_total_position_usd = D('1000')
    held = SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                           amount=D('.74'), breakeven_price=D('120'), cum_fees_quote=D('0'))
    instance.positions_held = [held]
    held_capacity = instance._get_total_position_value_usd()
    assert held_capacity == D('74')

    instance.positions_held = []
    executor = make_executor()
    executor.config.amount = D('.74')
    executor.config.entry_price = D('120')
    executor._open_order = tracked('owned', TradeType.BUY, '1', OrderState.FILLED)
    executor._open_order.order.executed_amount_base = D('.74')
    executor._open_order.order.executed_amount_quote = D('88.8')
    executor._entry_lifecycle['state'] = 'managing_fill'
    instance.executors_info = [executor.executor_info]
    assert instance._get_total_position_value_usd() == held_capacity


def test_invalid_inventory_cannot_be_valued_as_zero_exposure():
    instance = controller()
    instance.positions_held = [SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT', side=TradeType.BUY,
                                               amount='bad', breakeven_price=D('100'), cum_fees_quote=D('0'))]
    assert not instance._get_total_position_value_usd().is_finite()
    assert not instance.can_create_executor(1)


def test_wallet_funded_mode_bypasses_accumulated_buy_caps_but_keeps_wallet_cash_check():
    def funded_controller(available_quote):
        instance = controller()
        instance.config.allocation_mode = 'wallet_funded'
        instance.config.max_total_position_usd = D('100')
        instance.config.max_executors_per_side = 1
        instance.config.executor_price_gap_threshold = D('0')
        instance.market_data_provider.get_available_balance.return_value = available_quote
        instance._get_total_position_value_usd = lambda: D('30000')
        active_buy = SimpleNamespace(timestamp=1000)
        gates = (
            patch.object(instance, '_filter_same_side', return_value=[active_buy]),
            patch.object(instance, '_executor_owned_base', return_value=D('0')),
            patch.object(instance, '_has_active_buy_leg', return_value=False),
            patch.object(instance, '_has_sufficient_avg_entry_gap_for_buy', return_value=True),
            patch.object(instance, '_effective_cooldown_time', return_value=0),
            patch.object(instance, '_has_sufficient_price_gap', return_value=True),
            patch.object(instance, '_is_flash_crash_cooldown', return_value=False),
        )
        return instance, gates

    funded, funded_gates = funded_controller(D('25'))
    with ExitStack() as stack:
        for gate in funded_gates:
            stack.enter_context(gate)
        assert funded.can_create_executor(1)
        assert funded._last_gate_reason[TradeType.BUY] == 'ready'
        assert funded._get_position_utilization() == 0
        assert 'allocation=wallet-funded' in funded._position_log_context()
        buy_config = funded.get_executor_config(TradeType.BUY, D('100'), D('.25'))
        decision_gate = SimpleNamespace(dispatch=lambda *args: True, release=lambda: None)
        with patch.object(funded, '_get_entry_decision_gate', return_value=decision_gate):
            assert funded.validate_executor_config(buy_config)

    short_cash, short_cash_gates = funded_controller(D('24.99'))
    with ExitStack() as stack:
        for gate in short_cash_gates:
            stack.enter_context(gate)
        assert not short_cash.can_create_executor(1)
        assert short_cash._last_gate_reason[TradeType.BUY] == 'balance_low'


def test_bounded_allocation_remains_default_and_rejects_entry_over_position_cap():
    with pytest.raises(ValueError, match='usd_per_entry must be less than or equal'):
        OKRSIControllerConfig(
            id='bounded-invalid', connector_name='okx', trading_pair='ETH-USDT',
            usd_per_entry=D('25'), max_total_position_usd=D('24'),
        )
    wallet_funded = OKRSIControllerConfig(
        id='wallet-funded', connector_name='okx', trading_pair='ETH-USDT',
        allocation_mode='wallet_funded', usd_per_entry=D('25'), max_total_position_usd=D('24'),
    )
    default_bounded = OKRSIControllerConfig(
        id='default-bounded', connector_name='okx', trading_pair='ETH-USDT',
        usd_per_entry=D('25'), max_total_position_usd=D('25'),
    )
    assert wallet_funded.allocation_mode == 'wallet_funded'
    assert default_bounded.allocation_mode == 'bounded'
    assert 'allocation_mode' not in default_bounded.model_dump()
    assert wallet_funded.model_dump()['allocation_mode'] == 'wallet_funded'


def test_completed_loss_is_counted_before_any_entry_proposal():
    instance = controller()
    instance.config.max_consecutive_losses = 1
    executor = make_executor()
    executor._status = RunnableStatus.TERMINATED
    executor.close_type = CloseType.STOP_LOSS
    info = executor.executor_info.model_copy(update={'net_pnl_quote': D('-1'), 'close_timestamp': 999})
    instance.executors_info = [info]
    instance.create_actions_proposal = MagicMock(side_effect=lambda: ['unsafe'] if not instance._check_consecutive_loss_pause() else [])
    instance.stop_actions_proposal = MagicMock(return_value=[])
    assert instance.determine_executor_actions() == []


def test_wallet_funding_cannot_silently_leave_an_episode_allocation_ceiling_active():
    with pytest.raises(ValueError, match='wallet_funded.*episode'):
        OKRSIControllerConfig(id='episode-wallet', connector_name='okx', trading_pair='ETH-USDT',
                              allocation_mode='wallet_funded', episode_enabled=True)
