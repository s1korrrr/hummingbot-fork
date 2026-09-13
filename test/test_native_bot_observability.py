from decimal import Decimal
from types import SimpleNamespace as Obj


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
                   current_timestamp=100, _controller_reports_valid=True, _safe_decimal=lambda value: Decimal(str(value)))
    result = namespace['_build_runtime_status_snapshot'](strategy)
    assert result['active_orders_count'] == 0
    assert result['active_orders'] == []
    assert result['active_orders_status']['complete'] is True
    assert result['controllers'][0]['controller_id'] == 'c1'
    assert result['controllers'][0]['custom_info']['episode']['base'] == '2'
    assert result['controllers'][0]['custom_info']['episode']['cost_known'] is False


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
