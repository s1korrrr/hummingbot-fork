import json
from types import SimpleNamespace
from unittest.mock import Mock

from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from scripts.v2_with_controllers import V2WithControllers


def test_failed_report_cannot_refresh_snapshot_or_publish_stale_performance(tmp_path):
    path = tmp_path / 'test.status.json'
    original = json.dumps({'updated_at': 'old', 'active_executors': [{'id': 'completed-sale'}]})
    path.write_text(original)
    strategy = SimpleNamespace(
        _controller_reports_valid=False, _unattended_restart_hold=False,
        ready_to_trade=True, current_timestamp=100,
        _runtime_status_snapshot_enabled=lambda: True, _last_runtime_status_snapshot_ts=0,
        _runtime_status_snapshot_interval=lambda: 10, _runtime_status_snapshot_dir=lambda: tmp_path,
        _build_runtime_status_snapshot=lambda: {'bot_name': 'test', 'updated_at': 'fresh'},
        _last_performance_report_timestamp=0, performance_report_interval=1,
        _pub=Mock(), controllers={'c': object()}, get_controller_report=lambda _: {'performance': 'stale'},
        logger=Mock(),
    )
    StrategyV2Base._write_runtime_status_snapshot_if_due(strategy)
    V2WithControllers.send_performance_report(strategy)
    assert path.read_text() == original
    strategy._pub.assert_not_called()
    assert strategy._last_runtime_status_snapshot_ts == 0
    strategy._controller_reports_valid = True
    StrategyV2Base._write_runtime_status_snapshot_if_due(strategy)
    V2WithControllers.send_performance_report(strategy)
    assert json.loads(path.read_text())['updated_at'] == 'fresh'
    strategy._pub.assert_called_once()


def test_repeated_report_failure_logs_bounded_reminders_and_recovery():
    logger = Mock()
    controller = SimpleNamespace(executors_update_event=Mock())
    orchestrator = SimpleNamespace(get_all_reports=Mock(side_effect=RuntimeError('unowned inventory')))
    strategy = SimpleNamespace(current_timestamp=100, _controller_reports_valid=True,
                               _controller_report_error=None, _last_controller_report_error_log_ts=None,
                               executor_orchestrator=orchestrator, controllers={'c': controller},
                               controller_reports={'c': {'executors': ['old']}}, logger=lambda: logger)
    for timestamp in [100, 101, 102, 159, 160]:
        strategy.current_timestamp = timestamp
        StrategyV2Base.update_executors_info(strategy)
    assert not strategy._controller_reports_valid
    assert logger.error.call_count == 2
    controller.executors_update_event.set.assert_not_called()
    orchestrator.get_all_reports.side_effect = None
    orchestrator.get_all_reports.return_value = {'c': {'executors': [], 'positions': [], 'performance': []}}
    strategy.current_timestamp = 161
    StrategyV2Base.update_executors_info(strategy)
    assert strategy._controller_reports_valid
    assert controller.executors_info == []
    logger.info.assert_called_once()
    orchestrator.get_all_reports.side_effect = RuntimeError('unowned inventory')
    strategy.current_timestamp = 162
    StrategyV2Base.update_executors_info(strategy)
    assert logger.error.call_count == 3


def test_unknown_wallet_pnl_is_unavailable_without_disabling_known_controller_guard():
    from decimal import Decimal

    from hummingbot.strategy_v2.models.base import RunnableStatus
    unknown = SimpleNamespace(pnl_available=False, pnl_unavailable_reason='unknown_wallet_acquisition_cost', global_pnl_quote=None)
    known = SimpleNamespace(pnl_available=True, global_pnl_quote=Decimal('-60'))
    wallet = SimpleNamespace(status=RunnableStatus.RUNNING, config=SimpleNamespace(
        controller_name='ok_rsi', trade_mode='sell_only', sell_only_strict_fraction=True, sell_only_if_profitable=False))
    main = SimpleNamespace(status=RunnableStatus.RUNNING, config=SimpleNamespace(trade_mode='buy_sell'), stop=Mock())
    reports = {'wallet': unknown, 'main': known}
    strategy = SimpleNamespace(controllers={'wallet': wallet, 'main': main},
                               get_performance_report=reports.get, max_pnl_by_controller={}, max_global_pnl=Decimal('10'),
                               config=SimpleNamespace(max_controller_drawdown_quote=Decimal('50'), max_global_drawdown_quote=Decimal('100')),
                               drawdown_exited_controllers=[], get_executors_by_controller=lambda _: [], filter_executors=lambda **_: [],
                               executor_orchestrator=SimpleNamespace(execute_actions=Mock()), logger=lambda: Mock())
    strategy._drawdown_pnl = lambda cid: V2WithControllers._drawdown_pnl(strategy, cid)
    V2WithControllers.check_max_controller_drawdown(strategy)
    V2WithControllers.check_max_global_drawdown(strategy)
    main.stop.assert_called_once()
    assert strategy.drawdown_exited_controllers == ['main']
    assert strategy.max_global_pnl == Decimal('10')
    assert 'wallet' not in strategy.max_pnl_by_controller
    reports['main'] = unknown
    import pytest
    with pytest.raises(ValueError, match='PnL unavailable'):
        strategy._drawdown_pnl('main')


def test_unknown_cost_summary_is_not_formatted_as_zero_profit():
    report = SimpleNamespace(pnl_available=False, realized_pnl_quote=None, unrealized_pnl_quote=None,
                             unrealized_pnl_pct=None, global_pnl_quote=None, global_pnl_pct=None)
    result = StrategyV2Base._enrich_controller_status_summary(
        summary={'u_pnl_pct': '0%'}, executors=[], positions=[], performance=report)
    assert result['r_pnl'] == 'n/a'
    assert result['u_pnl'] == 'n/a'
    assert result['g_pnl'] == 'n/a'
    assert result['u_pnl_pct'] == 'n/a'


def test_unknown_pnl_survives_native_snapshot_and_money_formatting():
    report = SimpleNamespace(pnl_available=False, wallet_sales=[{'fees_quote': '0.02', 'proceeds_quote': '28.58'}])
    controller = SimpleNamespace(config=SimpleNamespace(connector_name='okx'), get_custom_info=lambda: {})
    entry = {'controller_id': 'c', 'controller': controller, 'summary': {'pair': 'SUI-USDC'},
             'executors': [], 'positions': [], 'performance': report}
    strategy = SimpleNamespace(_build_controller_status_entries=lambda: [entry], _get_balances_snapshot=lambda: [],
                               connectors={}, get_active_orders=lambda _: [], _runtime_status_snapshot_bot_name=lambda: 'sui',
                               _build_runtime_operations_snapshot=lambda: {}, current_timestamp=100,
                               _controller_reports_valid=True, _unattended_restart_hold=False,
                               _runtime_position_resume_observation=lambda: StrategyV2Base._runtime_position_resume_observation(strategy),
                               _safe_decimal=StrategyV2Base._safe_decimal)
    snapshot = StrategyV2Base._build_runtime_status_snapshot(strategy)
    assert snapshot['unattended_restart_hold'] is False
    assert snapshot['position_resume_recovery']['ownership_transfer_pending'] is False
    assert snapshot['summary']['pnl_available'] is False
    assert snapshot['summary']['net_pnl_quote'] is None
    assert snapshot['summary']['realized_pnl_quote'] is None
    assert snapshot['summary']['fees_quote'] == 0.02
    assert snapshot['wallet_sales'] == report.wallet_sales
    assert StrategyV2Base._format_summary_quote(None) == 'n/a'
    assert StrategyV2Base._format_summary_quote_pct(None, None) == 'n/a / n/a'


def test_wallet_sale_report_writes_exact_json_and_refreshes_atomically(tmp_path):
    from decimal import Decimal
    from test.hummingbot.strategy_v2.executors.test_wallet_sale_accounting import wallet_sale
    from unittest.mock import patch

    from hummingbot.connector.markets_recorder import MarketsRecorder

    orchestrator, executor, recorder, _ = wallet_sale()
    executor._finish_with_fills()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        report = orchestrator.get_all_reports()['controller']
    sale = report['performance'].wallet_sales[0]
    assert sale['fees_quote'] == Decimal('0.02286991736')
    controller = SimpleNamespace(config=SimpleNamespace(connector_name='okx'), get_custom_info=lambda: {})
    entry = dict(report, controller_id='controller', controller=controller, summary={'pair': 'SUI-USDT'})
    logger = Mock()
    strategy = SimpleNamespace(
        _build_controller_status_entries=lambda: [entry], _get_balances_snapshot=lambda: [],
        connectors={}, get_active_orders=lambda _: [], _runtime_status_snapshot_bot_name=lambda: 'sui',
        _build_runtime_operations_snapshot=lambda: {}, current_timestamp=100, _controller_reports_valid=True,
        _unattended_restart_hold=False,
        _runtime_position_resume_observation=lambda: StrategyV2Base._runtime_position_resume_observation(strategy),
        _safe_decimal=StrategyV2Base._safe_decimal, ready_to_trade=True, logger=lambda: logger,
        _runtime_status_snapshot_enabled=lambda: True, _last_runtime_status_snapshot_ts=0,
        _runtime_status_snapshot_interval=lambda: 10, _runtime_status_snapshot_dir=lambda: tmp_path,
    )
    strategy._build_runtime_status_snapshot = lambda: StrategyV2Base._build_runtime_status_snapshot(strategy)
    path = tmp_path / 'sui.status.json'
    path.write_text('{"updated_at":"old"}')
    StrategyV2Base._write_runtime_status_snapshot_if_due(strategy)
    logger.warning.assert_not_called()
    snapshot = json.loads(path.read_text())
    assert snapshot['updated_at'] != 'old'
    assert snapshot['wallet_sales'][0]['amount_base'] == '39.469'
    assert snapshot['wallet_sales'][0]['proceeds_quote'] == '28.5873967'
    assert snapshot['wallet_sales'][0]['fees_quote'] == '0.02286991736'
    assert snapshot['summary']['pnl_available'] is False
    assert all(snapshot['summary'][field] is None for field in ('realized_pnl_quote', 'unrealized_pnl_quote', 'net_pnl_quote'))
    assert snapshot['summary']['fees_quote'] == 0.02287
    assert strategy._last_runtime_status_snapshot_ts == 100
    assert not list(tmp_path.glob('*.tmp'))
    assert isinstance(sale['amount_base'], Decimal)  # Export cannot alter accounting inputs.
    strategy.current_timestamp = 111
    StrategyV2Base._write_runtime_status_snapshot_if_due(strategy)
    assert json.loads(path.read_text())['updated_at'] != snapshot['updated_at']
    assert strategy._last_runtime_status_snapshot_ts == 111
    logger.warning.assert_not_called()
