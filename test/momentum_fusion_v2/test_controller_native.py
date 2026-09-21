import asyncio
import json
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

try:
    from hummingbot.connector.connector_base import ConnectorBase  # noqa: F401
except ModuleNotFoundError:
    pytest.skip('compiled hummingbot native package required', allow_module_level=True)

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.components.domain import identity
from hummingbot.strategy_v2.executors.data_types import PositionSummary
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config, MomentumFusionV2Controller
from controllers.generic.momentum_fusion_v2_core import runtime


def test_source_identity_hold_is_visible_on_the_controller():
    assert MomentumFusionV2Controller.allow_implementation_hot_reload is False
    config = MomentumFusionV2Config(id='mom_fixture')
    assert config.execution_mode == 'observation'
    assert config.execution_enabled is False


def _host(tmp_path, *, kill_switch=False, **config_over):
    claims = tmp_path / 'claims.json'
    config = MomentumFusionV2Config(
        id='mom-v2', execution_enabled=True, execution_mode='paper',
        claim_store_path=str(claims), manual_kill_switch=kill_switch, **config_over)
    provider = MagicMock()
    provider.initialize_rate_sources.return_value = None
    provider.time.return_value = 1_700_000_180
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_base_amount_increment=D('0.1'), min_order_size=D('0.1'),
        min_notional_size=D('1'), min_price_increment=D('0.01'))
    provider.get_price_by_type.return_value = D('12')
    provider.get_available_balance.return_value = D('100')
    controller = MomentumFusionV2Controller(
        config=config, market_data_provider=provider, actions_queue=asyncio.Queue())
    controller._status = RunnableStatus.RUNNING
    controller.processed_data = {
        'signal': 0, 'data_health': {'ready': True}, 'decision_time': 1_700_000_120, 'close': D('12')}
    return controller


def _buy_held():
    buy = PositionExecutorConfig(
        timestamp=1, controller_id='mom-v2', connector_name='okx', trading_pair='BNB-USDC',
        side=TradeType.BUY, amount=D('10'), entry_price=D('10'),
        triple_barrier_config=TripleBarrierConfig(time_limit=None))
    return ExecutorInfo(
        id='long-1', timestamp=1, type='position_executor', status=RunnableStatus.RUNNING,
        config=buy, net_pnl_pct=D('0'), net_pnl_quote=D('0'), cum_fees_quote=D('0'),
        filled_amount_quote=D('100'), is_active=True, is_trading=True,
        custom_info={'side': TradeType.BUY, 'remaining_position_amount_base': D('10'),
                     'entry_price': D('10'), 'cum_fees_quote': D('0')})


def _held_bag():
    return PositionSummary(
        connector_name='okx', trading_pair='BNB-USDC', volume_traded_quote=D('100'),
        side=TradeType.BUY, amount=D('10'), breakeven_price=D('10'),
        unrealized_pnl_quote=D('0'), realized_pnl_quote=D('0'), cum_fees_quote=D('0'))


def _sell_pending():
    sell = PositionExecutorConfig(
        timestamp=1, controller_id='mom-v2', connector_name='okx', trading_pair='BNB-USDC',
        side=TradeType.SELL, amount=D('3'), entry_price=D('12'),
        triple_barrier_config=TripleBarrierConfig(time_limit=None))
    return ExecutorInfo(
        id='sell-1', timestamp=1, type='position_executor', status=RunnableStatus.RUNNING,
        config=sell, net_pnl_pct=D('0'), net_pnl_quote=D('0'), cum_fees_quote=D('0'),
        filled_amount_quote=D('36'), is_active=True, is_trading=True,
        custom_info={'side': TradeType.SELL, 'remaining_position_amount_base': D('3')})


def test_host_maps_reserved_base_from_pending_sales(tmp_path):
    controller = _host(tmp_path)
    controller.executors_info = [_buy_held(), _sell_pending()]
    view, proposals, extras = controller._management_inputs(controller.market_data_provider.time())
    assert view.remaining_base == D('10')
    assert view.reserved_base == D('3')
    assert extras['harvest_base'] == '7'
    assert proposals[0].requested_base == D('7')


def test_management_sell_passes_controller_validator_while_entry_paused(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    actions = controller.determine_executor_actions()
    assert actions
    sell = actions[0].executor_config
    assert sell.side == TradeType.SELL
    assert controller.validate_executor_config(sell) is True
    assert identity(sell.model_dump(mode='json')) == controller._pending_identity


def test_management_does_not_double_emit_sell_while_pending(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    assert first and first[0].executor_config.side == TradeType.SELL
    second = controller.determine_executor_actions()
    assert second == []
    assert controller.processed_data['action_reason'] == 'pending_inflight'


def _cash_host(tmp_path):
    return _host(tmp_path, total_amount_quote=D('100'), trade_quote=D('60'),
                 risk_fraction=D('0.05'))


def _arm_buy(controller, decision_time, now):
    controller.market_data_provider.time.return_value = now
    controller.processed_data = {
        'signal': 1, 'data_health': {'ready': True}, 'decision_time': decision_time,
        'close': D('12'), 'atr': D('0.3'), 'setup': 'b0_breakout'}


def _drive_buy(controller, start):
    reasons = []
    for offset in range(0, 180, 20):
        _arm_buy(controller, start + offset, start + offset + 5)
        actions = controller.determine_executor_actions()
        reason = controller.processed_data.get('action_reason')
        reasons.append(reason)
        if actions:
            return actions, reasons
        if reason in {'account_limit', 'program_limit', 'available_quote_limit', 'claim_already_open'}:
            return [], reasons
    raise AssertionError(reasons)


def test_host_second_sixty_holds_via_claims(tmp_path):
    controller = _cash_host(tmp_path)
    first, _ = _drive_buy(controller, 1_700_000_000)
    assert first and first[0].executor_config.side == TradeType.BUY
    controller._pending = None
    controller._pending_identity = None
    second, reasons = _drive_buy(controller, 1_700_000_200)
    assert second == []
    assert reasons[-1] in {'account_limit', 'program_limit', 'available_quote_limit'}


def test_host_buy_persists_submitted_before_create_action(tmp_path, monkeypatch):
    on_disk = []
    original = runtime.CreateExecutorAction

    def wrapped(**kwargs):
        rows = json.loads((tmp_path / 'claims.json').read_text())
        on_disk.append(rows[0]['status'])
        return original(**kwargs)

    monkeypatch.setattr(runtime, 'CreateExecutorAction', wrapped)
    controller = _cash_host(tmp_path)
    first, _ = _drive_buy(controller, 1_700_000_000)
    assert first and first[0].executor_config.side == TradeType.BUY
    assert on_disk == ['SUBMITTED_UNACKNOWLEDGED']
    restored = _cash_host(tmp_path)
    again, reasons = _drive_buy(restored, 1_700_000_200)
    assert again == []
    assert reasons[-1] in {
        'account_limit', 'program_limit', 'available_quote_limit', 'claim_already_open'}


def test_host_sell_persists_and_restore_does_not_rearm(tmp_path, monkeypatch):
    on_disk = []
    original = runtime.CreateExecutorAction

    def wrapped(**kwargs):
        rows = json.loads((tmp_path / 'claims.json').read_text())
        on_disk.append(rows[0]['status'])
        return original(**kwargs)

    monkeypatch.setattr(runtime, 'CreateExecutorAction', wrapped)
    first = _host(tmp_path, kill_switch=True)
    first.executors_info = [_buy_held()]
    actions = first.determine_executor_actions()
    assert actions and actions[0].executor_config.side == TradeType.SELL
    assert on_disk == ['SUBMITTED_UNACKNOWLEDGED']
    restored = _host(tmp_path, kill_switch=True)
    restored.executors_info = [_buy_held()]
    again = restored.determine_executor_actions()
    assert again == []
    assert restored.processed_data['action_reason'] == 'claim_already_open'


def _info(config, status, *, filled='0'):
    active = status not in {RunnableStatus.TERMINATED}
    return ExecutorInfo(
        id=config.id, timestamp=config.timestamp, type='position_executor', status=status,
        config=config, net_pnl_pct=D('0'), net_pnl_quote=D('0'), cum_fees_quote=D('0'),
        filled_amount_quote=D(filled), is_active=active, is_trading=active,
        custom_info={'side': config.side, 'remaining_position_amount_base': config.amount,
                     'entry_price': config.entry_price or D('10')})


def _claim_rows(tmp_path):
    return json.loads((tmp_path / 'claims.json').read_text())


def test_host_open_sell_stays_claimed_when_restart_clears_pending(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.RUNNING)]
    again = controller.determine_executor_actions()
    assert again == []
    assert controller.processed_data['action_reason'] == 'claim_already_open'
    assert _claim_rows(tmp_path)[0]['status'] == 'SUBMITTED_UNACKNOWLEDGED'


def test_host_done_sell_allows_new_sell_with_distinct_opportunity(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    first_oid = _claim_rows(tmp_path)[0]['opportunity_id']
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.TERMINATED, filled='36')]
    second = controller.determine_executor_actions()
    assert second and second[0].executor_config.side == TradeType.SELL
    rows = _claim_rows(tmp_path)
    by_oid = {row['opportunity_id']: row['status'] for row in rows}
    assert by_oid[first_oid] == 'RECONCILED_TERMINAL'
    assert any(oid != first_oid and status == 'SUBMITTED_UNACKNOWLEDGED' for oid, status in by_oid.items())


def test_host_zero_fill_cancelled_sell_allows_new_sell(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.TERMINATED, filled='0')]
    second = controller.determine_executor_actions()
    assert second and second[0].executor_config.side == TradeType.SELL
    assert {row['status'] for row in _claim_rows(tmp_path)} >= {
        'RECONCILED_TERMINAL', 'SUBMITTED_UNACKNOWLEDGED'}


def test_host_filled_sell_gone_from_executors_allows_new_sell(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    first_oid = _claim_rows(tmp_path)[0]['opportunity_id']
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.RUNNING, filled='36')]
    mid = controller.determine_executor_actions()
    assert mid == []
    assert _claim_rows(tmp_path)[0]['status'] == 'PARTIAL'
    controller.executors_info = [_buy_held()]
    third = controller.determine_executor_actions()
    assert third and third[0].executor_config.side == TradeType.SELL
    by_oid = {row['opportunity_id']: row['status'] for row in _claim_rows(tmp_path)}
    assert by_oid[first_oid] == 'RECONCILED_TERMINAL'


def test_host_cancelled_buy_releases_reserved_quote(tmp_path):
    controller = _cash_host(tmp_path)
    first, _ = _drive_buy(controller, 1_700_000_000)
    buy = first[0].executor_config
    controller.executors_info = [_info(buy, RunnableStatus.TERMINATED, filled='0')]
    second, reasons = _drive_buy(controller, 1_700_000_200)
    assert second and second[0].executor_config.side == TradeType.BUY
    statuses = {row['status'] for row in _claim_rows(tmp_path)}
    assert 'RECONCILED_TERMINAL' in statuses
    assert 'SUBMITTED_UNACKNOWLEDGED' in statuses


def test_host_late_fill_after_cancel_pending_is_not_a_new_emit(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.SHUTTING_DOWN, filled='0')]
    pending = controller.determine_executor_actions()
    assert pending == []
    assert _claim_rows(tmp_path)[0]['status'] == 'CANCEL_PENDING'
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.SHUTTING_DOWN, filled='24')]
    late = controller.determine_executor_actions()
    assert late == []
    assert _claim_rows(tmp_path)[0]['status'] == 'PARTIAL'
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.TERMINATED, filled='24')]
    done = controller.determine_executor_actions()
    rows = _claim_rows(tmp_path)
    assert any(row['status'] == 'RECONCILED_TERMINAL' for row in rows)
    assert done == [] or done[0].executor_config.side == TradeType.SELL


def test_host_submitted_never_seen_sell_restore_still_blocks(tmp_path):
    first = _host(tmp_path, kill_switch=True)
    first.executors_info = [_buy_held()]
    actions = first.determine_executor_actions()
    assert actions and actions[0].executor_config.side == TradeType.SELL
    restored = _host(tmp_path, kill_switch=True)
    restored.executors_info = [_buy_held()]
    restored.positions_held = [_held_bag()]
    again = restored.determine_executor_actions()
    assert again == []
    assert restored.processed_data['action_reason'] == 'claim_already_open'
    row = _claim_rows(tmp_path)[0]
    assert row['status'] == 'SUBMITTED_UNACKNOWLEDGED'
    assert not row.get('seen_executor_id')


def test_host_submitted_seen_then_gone_sell_allows_new_sell(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    first_oid = _claim_rows(tmp_path)[0]['opportunity_id']
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.RUNNING, filled='0')]
    seen = controller.determine_executor_actions()
    assert seen == []
    row = _claim_rows(tmp_path)[0]
    assert row['status'] == 'SUBMITTED_UNACKNOWLEDGED'
    assert row.get('seen_executor_id') == sell.id
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = [_buy_held()]
    second = controller.determine_executor_actions()
    assert second and second[0].executor_config.side == TradeType.SELL
    by_oid = {item['opportunity_id']: item['status'] for item in _claim_rows(tmp_path)}
    assert by_oid[first_oid] == 'RECONCILED_TERMINAL'


def test_host_submitted_seen_then_gone_buy_releases_reserved_quote(tmp_path):
    controller = _cash_host(tmp_path)
    first, _ = _drive_buy(controller, 1_700_000_000)
    buy = first[0].executor_config
    working = _info(buy, RunnableStatus.RUNNING, filled='0')
    working.custom_info['remaining_position_amount_base'] = D('0')
    controller.executors_info = [working]
    controller.determine_executor_actions()
    row = _claim_rows(tmp_path)[0]
    assert row['status'] == 'SUBMITTED_UNACKNOWLEDGED'
    assert row.get('seen_executor_id') == buy.id
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = []
    second, _ = _drive_buy(controller, 1_700_000_200)
    assert second and second[0].executor_config.side == TradeType.BUY
    statuses = {item['status'] for item in _claim_rows(tmp_path)}
    assert 'RECONCILED_TERMINAL' in statuses


def test_host_open_sell_survives_buy_executor_drop(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    assert first and first[0].executor_config.side == TradeType.SELL
    first_oid = _claim_rows(tmp_path)[0]['opportunity_id']
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = []
    controller.positions_held = [_held_bag()]
    second = controller.determine_executor_actions()
    assert second == []
    assert controller.processed_data['action_reason'] == 'claim_already_open'
    row = _claim_rows(tmp_path)[0]
    assert row['opportunity_id'] == first_oid
    assert row['status'] == 'SUBMITTED_UNACKNOWLEDGED'


def test_host_seen_sell_matches_by_executor_id_not_config_hash(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    controller.executors_info = [_buy_held(), _info(sell, RunnableStatus.RUNNING, filled='0')]
    controller.determine_executor_actions()
    mutated = sell.model_copy(update={'controller_id': 'mutated-id'})
    assert mutated.id == sell.id
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = [_buy_held(), _info(mutated, RunnableStatus.RUNNING, filled='0')]
    again = controller.determine_executor_actions()
    assert again == []
    assert _claim_rows(tmp_path)[0]['status'] == 'SUBMITTED_UNACKNOWLEDGED'
    assert _claim_rows(tmp_path)[0].get('seen_executor_id') == sell.id


def test_host_first_sighting_stamps_when_controller_id_dump_drifts(tmp_path):
    controller = _host(tmp_path, kill_switch=True)
    controller.executors_info = [_buy_held()]
    first = controller.determine_executor_actions()
    sell = first[0].executor_config
    mutated = sell.model_copy(update={'controller_id': 'mutated-id'})
    assert mutated.id == sell.id
    assert identity(mutated.model_dump(mode='json')) != identity(sell.model_dump(mode='json'))
    controller.executors_info = [_buy_held(), _info(mutated, RunnableStatus.RUNNING, filled='0')]
    seen = controller.determine_executor_actions()
    assert seen == []
    row = _claim_rows(tmp_path)[0]
    assert row['status'] == 'SUBMITTED_UNACKNOWLEDGED'
    assert row.get('seen_executor_id') == sell.id
    controller._pending = None
    controller._pending_identity = None
    controller.executors_info = [_buy_held()]
    gone = controller.determine_executor_actions()
    assert gone and gone[0].executor_config.side == TradeType.SELL
    by_oid = {item['opportunity_id']: item['status'] for item in _claim_rows(tmp_path)}
    assert by_oid[row['opportunity_id']] == 'RECONCILED_TERMINAL'
