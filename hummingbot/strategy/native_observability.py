"""Bounded projections of owner state. No decisions, connector calls or order actions."""
from decimal import Decimal, InvalidOperation

EPISODE_FIELDS = ('enabled', 'mode', 'phase', 'episode_id', 'reason', 'hold_reason', 'base', 'cost',
                  'cost_known', 'gross_spent', 'sell_proceeds', 'minimum_profit_price', 'target_base',
                  'exit_risk_clear', 'exit_policy', 'exit_target_revision', 'checkpoint_sequence')
TRAILING_FIELDS = ('policy', 'peak', 'floor', 'target_base', 'anchor_activation_pct', 'anchor_delta_pct',
                   'selected_activation_pct', 'selected_delta_pct', 'initial_locked_profit_pct')
EXECUTOR_FIELDS = ('trailing_state', 'trailing_activation_price', 'trailing_trigger_price',
                   'remaining_position_amount_base', 'executed_amount_base', 'trailing_current_price',
                   'distance_to_trailing_activation_pct', 'distance_to_trailing_trigger_pct')


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation):
        return None
    return str(parsed) if parsed.is_finite() else None


def scalar(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    return number(value)


def remaining(total, filled):
    total, filled = number(total), number(filled)
    if total is None or filled is None or Decimal(total) < 0 or not 0 <= Decimal(filled) <= Decimal(total):
        return None
    return str(Decimal(total) - Decimal(filled))


def controller_observation(entry):
    controller = entry['controller']
    result = {'controller_id': entry['controller_id'],
              'connector_name': getattr(controller.config, 'connector_name', None),
              'custom_info': {}, 'observation_status': 'available'}
    summary = entry.get('summary', {})
    for key in ('plan_mode', 'plan_target', 'plan_anchor', 'plan_next', 'plan_slice', 'plan_inventory'):
        if key in summary:
            result[key] = scalar(summary[key])
    try:
        custom = controller.get_custom_info()
        if not isinstance(custom, dict):
            raise ValueError('Invalid controller observation')
        for name, fields in [('episode', EPISODE_FIELDS), ('trailing_policy', TRAILING_FIELDS)]:
            source = custom.get(name)
            if isinstance(source, dict):
                result['custom_info'][name] = {key: scalar(source[key]) for key in fields if key in source}
        runtime = getattr(controller, '_episode_runtime', None)
        requests = []
        if runtime is not None:
            for request_id, progress in runtime.state.orders.items():
                request = progress.request
                if progress.done or request.side != 'SELL':
                    continue
                requests.append({'request_id': str(request_id), 'price_quote': number(request.price),
                                 'amount_base': number(request.amount), 'filled_amount_base': number(progress.filled_base),
                                 'remaining_amount_base': remaining(request.amount, progress.filled_base),
                                 'termination_requested': (
                                     progress.termination_requested
                                     if isinstance(getattr(progress, 'termination_requested', None), bool) else None),
                                 'expires_at': scalar(getattr(request, 'expires_at', None)),
                                 'terminal_cause': (
                                     progress.terminal_cause
                                     if isinstance(getattr(progress, 'terminal_cause', None), str) else None),
                                 'source': 'owner_issued_request'})
                if len(requests) >= 200:
                    break
            result['custom_info']['pending_sell_requests'] = requests
            result['custom_info']['pending_sell_requests_truncated'] = len(requests) >= 200
    except Exception:
        result['custom_info'] = {}
        result['observation_status'] = 'unavailable'
    return result


def executor_observation(executor, controller_id):
    custom = executor.custom_info or {}
    result = {'executor_id': executor.id, 'controller_id': controller_id,
              'connector_name': getattr(executor.config, 'connector_name', None),
              'amount_base': number(getattr(executor.config, 'amount', None)),
              'price_quote': number(getattr(executor.config, 'price', None))}
    result.update({key: scalar(custom.get(key)) for key in EXECUTOR_FIELDS})
    return result


def active_order_observations(connectors, entries):
    # Attribute only explicit executor order references. Matching pair alone is insufficient.
    owners = {}
    for entry in entries:
        for executor in entry['executors']:
            custom = executor.custom_info or {}
            references = list(custom.get('order_ids') or [])
            if custom.get('order_id'):
                references.append(custom['order_id'])
            for order_id in references:
                if isinstance(order_id, str) and order_id:
                    key = (getattr(executor.config, 'connector_name', None), order_id)
                    owners.setdefault(key, set()).add(entry['controller_id'])
    rows, errors = [], []
    for connector_name, connector in connectors.items():
        try:
            orders = connector.in_flight_orders
            if not isinstance(orders, dict):
                raise ValueError('Invalid tracked orders')
            for order in orders.values():
                if order.is_done:
                    continue
                if len(rows) >= 500:
                    errors.append('order_limit_reached')
                    break
                order_id = str(order.client_order_id)
                matches = owners.get((connector_name, order_id), set())
                rows.append({'order_id': order_id, 'exchange_order_id': order.exchange_order_id,
                             'connector_name': connector_name, 'pair': order.trading_pair,
                             'controller_id': next(iter(matches)) if len(matches) == 1 else None,
                             'side': order.trade_type.name.lower(), 'status': order.current_state.name.lower(),
                             'amount_base': number(order.amount), 'filled_amount_base': number(order.executed_amount_base),
                             'remaining_amount_base': remaining(order.amount, order.executed_amount_base),
                             'price_quote': number(order.price)})
        except Exception:
            errors.append(f'{connector_name}:tracked_orders_unavailable')
    return rows, {'complete': not errors, 'source': 'connector_in_flight_orders', 'errors': errors}
