from decimal import Decimal
from types import SimpleNamespace as Obj


def _position_resume_observation(strategy):
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    return StrategyV2Base._runtime_position_resume_observation(strategy)


def test_controller_episode_and_pending_sell_are_read_only_and_allowlisted():
    from hummingbot.strategy.native_observability import controller_observation
    request = Obj(side='SELL', price=Decimal('12'), amount=Decimal('3'))
    progress = Obj(done=False, request=request, filled_base=Decimal('1'))
    custom = {'episode': {'enabled': True, 'base': '10', 'cost': '90', 'cost_known': True, 'phase': 'DISTRIBUTE'},
              'trailing_policy': {'policy': 'episode_aggregate', 'peak': '13', 'floor': '12', 'target_base': '7'},
              'secret': 'not-public'}
    controller = Obj(get_custom_info=lambda: custom, config=Obj(connector_name='okx'),
                     _episode_runtime=Obj(state=Obj(orders={'request1': progress})))
    result = controller_observation({'controller_id': 'c1', 'controller': controller})
    assert result['custom_info']['episode']['base'] == '10'
    assert 'secret' not in str(result)
    assert result['custom_info']['pending_sell_requests'][0]['remaining_amount_base'] == '2'
    assert progress.filled_base == Decimal('1')


def test_event_projection_is_allowlisted_only_for_v2_observer():
    from hummingbot.strategy.native_observability import controller_observation
    projection = {
        'schema': 'cross_venue_event.operator_projection.v2',
        'status': 'NO_ORDER_OBSERVER_V2',
        'recipe_sha256': 'a' * 64,
        'module_graph_sha256': 'c' * 64,
        'source_sha256': 'd' * 64,
        'configuration_sha256': 'e' * 64,
        'snapshot_sha256': 'b' * 64,
        'asof_ms': 1780000000000,
        'lag_ms': 1000,
        'observation_universe': ['eip155:1/erc20:0xabc'],
        'entry_universe': [],
        'management_universe': [],
        'native_ownership_status': 'UNVERIFIED',
        'research_status': 'NOT_RUN',
        'execution_allowed': False,
        'private': 'never publish',
    }
    observer = Obj(config=Obj(controller_name='cross_venue_event_v2', connector_name='okx'),
                   get_custom_info=lambda: {'event_projection': projection})
    result = controller_observation({'controller_id': 'events-v2', 'controller': observer})
    assert result['custom_info']['event_projection']['recipe_sha256'] == 'a' * 64
    assert 'private' not in str(result)
    legacy = Obj(config=Obj(controller_name='ok_rsi', connector_name='okx'),
                 get_custom_info=lambda: {'event_projection': projection})
    assert 'event_projection' not in controller_observation({'controller_id': 'legacy', 'controller': legacy})['custom_info']


def test_event_projection_rejects_malformed_claims():
    from hummingbot.strategy.native_observability import controller_observation
    controller = Obj(config=Obj(controller_name='cross_venue_event_v2', connector_name='okx'),
                     get_custom_info=lambda: {'event_projection': {'schema': 'cross_venue_event.operator_projection.v2',
                                                                   'execution_allowed': True}})
    result = controller_observation({'controller_id': 'events-v2', 'controller': controller})
    assert result['observation_status'] == 'unavailable'
    assert result['custom_info'] == {}


def test_orders_keep_remaining_and_unknown_attribution_and_report_failure():
    from hummingbot.strategy.native_observability import active_order_observations
    order = Obj(client_order_id='o1', exchange_order_id='venue1', trading_pair='BTC-USDT', trade_type=Obj(name='SELL'),
                current_state=Obj(name='PARTIALLY_FILLED'), amount=Decimal('3'), executed_amount_base=Decimal('1'), price=Decimal('12'), is_done=False)
    rows, coverage = active_order_observations({'okx': Obj(in_flight_orders={'o1': order})}, [])
    assert rows[0]['remaining_amount_base'] == '2'
    assert rows[0]['controller_id'] is None
    assert coverage['complete'] is True

    class Broken:
        @property
        def in_flight_orders(self):
            raise RuntimeError('private message')
    rows, coverage = active_order_observations({'okx': Broken()}, [])
    assert rows == [] and coverage['complete'] is False
    assert 'private message' not in str(coverage)


def test_executor_trailing_missing_never_defaults_to_zero():
    from hummingbot.strategy.native_observability import executor_observation
    executor = Obj(id='e1', config=Obj(connector_name='okx', amount=Decimal('3'), price=Decimal('12')),
                   custom_info={'trailing_state': 'armed', 'trailing_trigger_price': Decimal('11'), 'remaining_position_amount_base': Decimal('2')})
    result = executor_observation(executor, 'c1')
    assert result['trailing_trigger_price'] == '11'
    assert result['trailing_activation_price'] is None
    assert result['remaining_position_amount_base'] == '2'


def test_executor_entry_details_are_allowlisted_without_leaking_unknown_fields():
    from hummingbot.strategy.native_observability import executor_observation
    executor = Obj(
        id='e1',
        config=Obj(connector_name='okx', amount=Decimal('3'), price=Decimal('12')),
        custom_info={
            'entry_lifecycle': {
                'policy': 'down_only_maker_v1',
                'state': 'cancel_pending',
                'stop_reason': 'entry_deadline',
                'order_id': 'order-1',
                'max_age_seconds': 30,
                'private_failure': 'do not publish',
            },
            'entry_acquisition': {
                'policy': 'down_only_maker_v1',
                'intent_id': 'intent-1',
                'state': 'cancel_pending_reprice',
                'terminal_reason': None,
                'initial_price': '100',
                'current_order_price': '99.90',
                'maximum_acceptable_price': '100',
                'decision_atr_1m': '1',
                'authorized_base': '1',
                'cumulative_filled_base': '0.2',
                'remaining_base': '0.8',
                'reprice_count': 1,
                'rest_seconds': 30,
                'total_max_age_seconds': 300,
                'order_ids': ['parent', 'child-1'],
                'queue_fill_status': 'UNAVAILABLE',
                'fee_economics_status': 'AVAILABLE',
                'private_failure': 'do not publish',
            },
        },
    )

    result = executor_observation(executor, 'c1')

    assert result['entry_lifecycle'] == {
        'policy': 'down_only_maker_v1',
        'state': 'cancel_pending',
        'stop_reason': 'entry_deadline',
        'order_id': 'order-1',
        'max_age_seconds': 30,
    }
    assert result['entry_acquisition'] == {
        'policy': 'down_only_maker_v1',
        'intent_id': 'intent-1',
        'state': 'cancel_pending_reprice',
        'terminal_reason': None,
        'initial_price': '100',
        'current_order_price': '99.90',
        'maximum_acceptable_price': '100',
        'decision_atr_1m': '1',
        'authorized_base': '1',
        'cumulative_filled_base': '0.2',
        'remaining_base': '0.8',
        'reprice_count': 1,
        'rest_seconds': 30,
        'total_max_age_seconds': 300,
        'order_ids': ['parent', 'child-1'],
        'queue_fill_status': 'UNAVAILABLE',
        'fee_economics_status': 'AVAILABLE',
    }
    assert 'private_failure' not in str(result)


def test_explicit_multiple_order_refs_attribute_without_pair_guessing():
    from hummingbot.strategy.native_observability import active_order_observations
    order = Obj(client_order_id='o1', exchange_order_id=None, trading_pair='BTC-USDT', trade_type=Obj(name='SELL'),
                current_state=Obj(name='OPEN'), amount=Decimal('3'), executed_amount_base=Decimal('0'), price=Decimal('12'), is_done=False)
    executor = Obj(config=Obj(connector_name='okx'), custom_info={'order_ids': ['o1']})
    rows, _ = active_order_observations({'okx': Obj(in_flight_orders={'o1': order})}, [{'controller_id': 'c1', 'executors': [executor]}])
    assert rows[0]['controller_id'] == 'c1'


def test_controller_unavailable_is_explicit_and_original_plan_is_preserved():
    from hummingbot.strategy.native_observability import controller_observation

    def broken():
        raise ValueError('private failure')
    result = controller_observation({'controller_id': 'c1', 'summary': {'plan_next': 'owner reference'},
                                     'controller': Obj(config=Obj(connector_name='okx'), get_custom_info=broken)})
    assert result['observation_status'] == 'unavailable'
    assert result['plan_next'] == 'owner reference'
    assert 'private failure' not in str(result)


def test_actual_snapshot_function_emits_enrichment_and_preserves_legacy_count():
    # Execute the actual source method in isolation from compiled engine imports.
    import ast
    from pathlib import Path
    from typing import Any, Dict, List

    import pandas as pd
    tree = ast.parse(Path('hummingbot/strategy/strategy_v2_base.py').read_text())
    owner = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'StrategyV2Base')
    method = next(node for node in owner.body if isinstance(node, ast.FunctionDef) and node.name == '_build_runtime_status_snapshot')
    namespace = {'Dict': Dict, 'Any': Any, 'List': List, 'Decimal': Decimal, 'pd': pd}
    exec(compile(ast.Module(body=[method], type_ignores=[]), '<actual snapshot method>', 'exec'), namespace)
    controller = Obj(config=Obj(connector_name='okx'), get_custom_info=lambda: {'episode': {'enabled': True, 'base': '2', 'cost_known': False}})
    entry = {'controller_id': 'c1', 'controller': controller, 'summary': {'pair': 'BTC-USDT', 'price': '12'}, 'executors': [], 'positions': []}
    strategy = Obj(_build_controller_status_entries=lambda: [entry], _get_balances_snapshot=lambda: [],
                   connectors={'okx': Obj(in_flight_orders={})}, get_active_orders=lambda name: [],
                   _runtime_status_snapshot_bot_name=lambda: 'test-bot', _build_runtime_operations_snapshot=lambda: {},
                   current_timestamp=100, _controller_reports_valid=True, _unattended_restart_hold=False,
                   _runtime_position_resume_observation=lambda: _position_resume_observation(strategy),
                   _safe_decimal=lambda value: Decimal(str(value)))
    result = namespace['_build_runtime_status_snapshot'](strategy)
    assert result['unattended_restart_hold'] is False
    assert result['position_resume_recovery']['unfinished_claim_controllers'] == []
    assert result['active_orders_count'] == 0
    assert result['active_orders'] == []
    assert result['active_orders_status']['complete'] is True
    assert result['controllers'][0]['controller_id'] == 'c1'
    assert result['controllers'][0]['custom_info']['episode']['base'] == '2'
    assert result['controllers'][0]['custom_info']['episode']['cost_known'] is False


def test_actual_snapshot_keeps_closing_executor_in_lifecycle_projection():
    import ast
    from pathlib import Path
    from typing import Any, Dict, List

    import pandas as pd
    tree = ast.parse(Path('hummingbot/strategy/strategy_v2_base.py').read_text())
    owner = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'StrategyV2Base')
    method = next(node for node in owner.body if isinstance(node, ast.FunctionDef) and node.name == '_build_runtime_status_snapshot')
    namespace = {'Dict': Dict, 'Any': Any, 'List': List, 'Decimal': Decimal, 'pd': pd}
    exec(compile(ast.Module(body=[method], type_ignores=[]), '<actual snapshot method>', 'exec'), namespace)
    controller = Obj(config=Obj(connector_name='okx'), get_custom_info=lambda: {})
    closing = Obj(
        id='closing-1',
        controller_id='c1',
        config=Obj(connector_name='okx', amount=Decimal('3'), price=Decimal('12'), side='BUY'),
        custom_info={'entry_lifecycle': {'policy': 'single_attempt', 'state': 'managing_fill'}},
        side='BUY',
        type='position_executor',
        status=Obj(name='SHUTTING_DOWN'),
        close_type=Obj(name='TRAILING_STOP'),
        timestamp=90,
        net_pnl_quote=Decimal('1.2'),
        net_pnl_pct=Decimal('0.01'),
        filled_amount_quote=Decimal('36'),
        is_active=False,
        is_done=False,
    )
    entry = {
        'controller_id': 'c1',
        'controller': controller,
        'summary': {'pair': 'BTC-USDT', 'price': '12'},
        'executors': [closing],
        'positions': [],
        'performance': None,
    }
    strategy = Obj(
        _build_controller_status_entries=lambda: [entry],
        _get_balances_snapshot=lambda: [],
        connectors={'okx': Obj(in_flight_orders={})},
        get_active_orders=lambda name: [],
        _runtime_status_snapshot_bot_name=lambda: 'test-bot',
        _build_runtime_operations_snapshot=lambda: {},
        _compact_side_label=lambda value: value,
        _compact_executor_type=lambda value: 'position',
        _compact_enum_label=lambda value: value.name.replace('_', '-').lower(),
        _format_executor_age_short=lambda timestamp: '10s',
        current_timestamp=100,
        _controller_reports_valid=True,
        _unattended_restart_hold=False,
        _runtime_position_resume_observation=lambda: _position_resume_observation(strategy),
        _safe_decimal=lambda value: Decimal(str(value)),
    )

    result = namespace['_build_runtime_status_snapshot'](strategy)

    assert result['active_executors'] == []
    assert result['lifecycle_executors'][0]['executor_id'] == 'closing-1'
    assert result['lifecycle_executors'][0]['status'] == 'shutting-down'
    assert result['lifecycle_executors'][0]['close_type'] == 'trailing-stop'
    assert result['summary']['lifecycle_executor_count'] == 1


def test_actual_snapshot_uses_each_native_instrument_for_multisymbol_rows():
    # Meridian can have no controller-level trading pair. Each native row must
    # retain its own instrument rather than inheriting the controller ID.
    controller_id = 'meridian-v3'
    controller = Obj(config=Obj(connector_name='okx'), get_custom_info=lambda: {})

    def executor(executor_id, trading_pair, *, active):
        return Obj(
            id=executor_id,
            controller_id=controller_id,
            config=Obj(connector_name='okx', trading_pair=trading_pair,
                       amount=Decimal('1'), price=Decimal('100'), side='BUY'),
            custom_info={},
            side='BUY',
            type='position_executor',
            status=Obj(name='RUNNING' if active else 'SHUTTING_DOWN'),
            close_type=None,
            timestamp=90,
            net_pnl_quote=Decimal('0'),
            net_pnl_pct=Decimal('0'),
            filled_amount_quote=Decimal('0'),
            is_active=active,
            is_done=False,
        )

    entry = {
        'controller_id': controller_id,
        'controller': controller,
        # This reproduces the prior fallback when get_status_summary() is {}.
        'summary': {'pair': controller_id, 'price': 'n/a'},
        'executors': [
            executor('active-bnb', 'BNB-USDC', active=True),
            executor('active-eth', 'ETH-USDC', active=True),
            executor('closing-sol', 'SOL-USDC', active=False),
            executor('missing-pair', None, active=True),
        ],
        'positions': [Obj(trading_pair='XRP-USDC', side='BUY', amount=Decimal('2'),
                          breakeven_price=Decimal('1'), unrealized_pnl_quote=Decimal('0'),
                          realized_pnl_quote=Decimal('0'), cum_fees_quote=Decimal('0'))],
        'performance': None,
    }
    strategy = Obj(
        _build_controller_status_entries=lambda: [entry],
        _get_balances_snapshot=lambda: [],
        connectors={'okx': Obj(in_flight_orders={})},
        get_active_orders=lambda _: [],
        _runtime_status_snapshot_bot_name=lambda: 'test-bot',
        _build_runtime_operations_snapshot=lambda: {},
        _compact_side_label=lambda value: value,
        _compact_executor_type=lambda value: 'position',
        _compact_enum_label=lambda value: value.name.replace('_', '-').lower() if value else None,
        _format_executor_age_short=lambda timestamp: '10s',
        current_timestamp=100,
        _controller_reports_valid=True,
        _unattended_restart_hold=False,
        _runtime_position_resume_observation=lambda: _position_resume_observation(strategy),
        _safe_decimal=lambda value: Decimal(str(value or '0')),
    )

    from hummingbot.strategy.strategy_v2_base import StrategyV2Base
    result = StrategyV2Base._build_runtime_status_snapshot(strategy)

    active = {row['executor_id']: row for row in result['active_executors']}
    lifecycle = {row['executor_id']: row for row in result['lifecycle_executors']}
    assert active['active-bnb']['pair'] == 'BNB-USDC'
    assert active['active-eth']['pair'] == 'ETH-USDC'
    assert active['missing-pair']['pair'] is None
    assert lifecycle['closing-sol']['pair'] == 'SOL-USDC'
    assert result['positions_held'][0]['pair'] == 'XRP-USDC'
    assert all(row['controller_id'] == controller_id for row in active.values())
    assert all(row['controller_id'] == controller_id for row in lifecycle.values())
    assert result['positions_held'][0]['controller_id'] == controller_id


def test_controller_status_does_not_fabricate_pair_from_controller_id():
    from hummingbot.strategy.strategy_v2_base import StrategyV2Base

    class NoLoggerNeeded:
        pass

    no_pair = Obj(config=Obj(), get_status_summary=lambda: {})
    malformed_pair = Obj(config=Obj(), get_status_summary=lambda: {'pair': 42})
    configured_pair = Obj(config=Obj(trading_pair='BNB-USDC'), get_status_summary=lambda: {})

    assert StrategyV2Base._safe_controller_status_summary(NoLoggerNeeded(), 'meridian-v3', no_pair)['pair'] is None
    assert StrategyV2Base._safe_controller_status_summary(NoLoggerNeeded(), 'meridian-v3', malformed_pair)['pair'] is None
    assert StrategyV2Base._safe_controller_status_summary(NoLoggerNeeded(), 'meridian-v3', configured_pair)['pair'] == 'BNB-USDC'


def test_pending_sell_receipt_preserves_cancellation_and_expiry():
    from hummingbot.strategy.native_observability import controller_observation
    request = Obj(side='SELL', price=Decimal('12'), amount=Decimal('3'), expires_at=1789066790)
    progress = Obj(done=False, request=request, filled_base=Decimal('1'),
                   termination_requested=True, terminal_cause='CANCEL_REQUESTED')
    controller = Obj(config=Obj(connector_name='okx'), get_custom_info=lambda: {'episode': {'enabled': True}},
                     _episode_runtime=Obj(state=Obj(orders={'request1': progress})))
    result = controller_observation({'controller_id': 'c1', 'controller': controller})
    receipt = result['custom_info']['pending_sell_requests'][0]
    assert receipt['termination_requested'] is True
    assert receipt['expires_at'] == 1789066790
    assert receipt['terminal_cause'] == 'CANCEL_REQUESTED'
    assert receipt['remaining_amount_base'] == '2'
    del progress.termination_requested
    del progress.terminal_cause
    del request.expires_at
    receipt = controller_observation({'controller_id': 'c1', 'controller': controller})['custom_info']['pending_sell_requests'][0]
    assert receipt['termination_requested'] is None
    assert receipt['expires_at'] is None
    assert receipt['terminal_cause'] is None


def test_runtime_pnl_requires_nonempty_explicit_performance_evidence():
    import ast
    from pathlib import Path
    from typing import Any, Dict, List

    import pandas as pd
    tree = ast.parse(Path('hummingbot/strategy/strategy_v2_base.py').read_text())
    owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'StrategyV2Base')
    method = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == '_build_runtime_status_snapshot')
    namespace = {'Dict': Dict, 'Any': Any, 'List': List, 'Decimal': Decimal, 'pd': pd}
    exec(compile(ast.Module(body=[method], type_ignores=[]), '<actual snapshot>', 'exec'), namespace)
    controller = Obj(config=Obj(connector_name='okx'), get_custom_info=lambda: {})
    entry = {'controller_id': 'c1', 'controller': controller, 'summary': {'pair': 'SUI-USDC', 'price': '1'}, 'executors': [], 'positions': []}
    for performance, expected in [(None, False), (Obj(), False), (Obj(pnl_available=1), False), (Obj(pnl_available=False), False), (Obj(pnl_available=True), True)]:
        entries = [dict(entry, performance=performance)]
        strategy = Obj(_build_controller_status_entries=lambda: entries, _get_balances_snapshot=lambda: [],
                       connectors={'okx': Obj(in_flight_orders={})}, get_active_orders=lambda _: [],
                       _runtime_status_snapshot_bot_name=lambda: 'test', _build_runtime_operations_snapshot=lambda: {},
                       current_timestamp=100, _controller_reports_valid=True, _unattended_restart_hold=False,
                       _runtime_position_resume_observation=lambda: _position_resume_observation(strategy),
                       _safe_decimal=lambda v: Decimal(str(v)))
        snapshot = namespace['_build_runtime_status_snapshot'](strategy)
        assert snapshot['summary']['pnl_available'] is expected
        if not expected:
            assert snapshot['summary']['net_pnl_quote'] is None
    entries.clear()
    assert namespace['_build_runtime_status_snapshot'](strategy)['summary']['pnl_available'] is False
