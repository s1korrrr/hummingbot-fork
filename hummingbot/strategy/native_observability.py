"""Bounded projections of owner state. No decisions, connector calls or order actions."""
import math
import re
from decimal import Decimal, InvalidOperation

EPISODE_FIELDS = ('enabled', 'mode', 'phase', 'episode_id', 'reason', 'hold_reason', 'base', 'cost',
                  'cost_known', 'gross_spent', 'sell_proceeds', 'minimum_profit_price', 'target_base',
                  'exit_risk_clear', 'exit_policy', 'exit_target_revision', 'checkpoint_sequence')
TRAILING_FIELDS = ('policy', 'peak', 'floor', 'target_base', 'anchor_activation_pct', 'anchor_delta_pct',
                   'selected_activation_pct', 'selected_delta_pct', 'initial_locked_profit_pct')
EXECUTOR_FIELDS = ('trailing_state', 'trailing_activation_price', 'trailing_trigger_price',
                   'remaining_position_amount_base', 'executed_amount_base', 'trailing_current_price',
                   'distance_to_trailing_activation_pct', 'distance_to_trailing_trigger_pct')
ENTRY_LIFECYCLE_FIELDS = ('policy', 'state', 'stop_reason', 'order_id', 'submission_timestamp',
                          'creation_timestamp', 'deadline_timestamp', 'max_age_seconds',
                          'stop_requested_timestamp', 'stop_observed_wall_timestamp',
                          'cancel_requested_timestamp', 'cancel_event_timestamp',
                          'cancel_event_observed_timestamp', 'cancel_event_observed_wall_timestamp',
                          'terminal_exchange_timestamp', 'terminal_observed_timestamp',
                          'failure_event_timestamp')
ENTRY_ACQUISITION_FIELDS = ('policy', 'intent_id', 'state', 'terminal_reason', 'initial_price',
                            'current_order_price', 'maximum_acceptable_price', 'decision_atr_1m',
                            'authorized_base', 'cumulative_filled_base', 'remaining_base',
                            'reprice_count', 'rest_seconds', 'total_max_age_seconds',
                            'queue_fill_status', 'fee_economics_status')


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


def strategy_components_observation(source):
    """Bounded diagnostics only; identity and economics remain native-owned."""
    if not isinstance(source, dict):
        raise ValueError('Invalid strategy component observation')
    ids = source.get('component_ids')
    if (not isinstance(ids, list) or not 1 <= len(ids) <= 16
            or any(not isinstance(value, str) or re.fullmatch(r'[a-z][a-z0-9_]{0,63}', value) is None for value in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError('Invalid strategy component identifiers')
    result = {'component_ids': list(ids)}
    for name in ('execution_mode', 'execution_enabled', 'data_ready', 'data_reason',
                 'decision_time', 'signal', 'recovery_hold', 'components_version', 'parameters_version'):
        if name not in source:
            continue
        value = source[name]
        if value is None:
            result[name] = None
        elif name in ('execution_enabled', 'data_ready'):
            if type(value) is not bool:
                raise ValueError('Invalid component boolean')
            result[name] = value
        elif name in ('decision_time', 'signal'):
            if type(value) not in (int, float) or not math.isfinite(value) or abs(value) > 10**15:
                raise ValueError('Invalid component number')
            result[name] = value
        elif not isinstance(value, str) or len(value) > 256:
            raise ValueError('Invalid component string')
        else:
            result[name] = value
    return result


def event_projection_observation(source):
    """Allowlist the opt-in observer's compact, explicitly no-order status."""
    if not isinstance(source, dict) or source.get('schema') != 'cross_venue_event.operator_projection.v2':
        raise ValueError('Invalid event projection schema')
    if source.get('status') != 'NO_ORDER_OBSERVER_V2' or source.get('execution_allowed') is not False:
        raise ValueError('Event projection cannot claim execution authority')
    result = {key: source[key] for key in ('schema', 'status', 'execution_allowed')}
    for key in ('recipe_sha256', 'snapshot_sha256'):
        value = source.get(key)
        if not isinstance(value, str) or re.fullmatch(r'[0-9a-f]{64}', value) is None:
            raise ValueError('Invalid event projection identity')
        result[key] = value
    for key in ('asof_ms', 'lag_ms'):
        value = source.get(key)
        if type(value) is not int or value < 0 or value > 10**15:
            raise ValueError('Invalid event projection clock')
        result[key] = value
    if source.get('native_ownership_status') not in ('UNVERIFIED', 'VERIFIED'):
        raise ValueError('Invalid event ownership status')
    if source.get('research_status') not in ('NOT_RUN', 'HELD', 'VALIDATED'):
        raise ValueError('Invalid event research status')
    result['native_ownership_status'] = source['native_ownership_status']
    result['research_status'] = source['research_status']
    for key in ('observation_universe', 'entry_universe', 'management_universe'):
        values = source.get(key)
        if (not isinstance(values, list) or len(values) > 2
                or any(not isinstance(value, str) or not 0 < len(value) <= 128 for value in values)
                or len(set(values)) != len(values)):
            raise ValueError('Invalid event universe projection')
        result[key] = list(values)
        count = source.get(key + '_count', len(values))
        truncated = source.get(key + '_truncated', False)
        if type(count) is not int or count < len(values) or count > 10_000 or type(truncated) is not bool:
            raise ValueError('Invalid event universe count')
        result[key + '_count'] = count
        result[key + '_truncated'] = truncated
    coverage = source.get('source_coverage', [])
    if not isinstance(coverage, list) or len(coverage) > 2:
        raise ValueError('Invalid event source coverage')
    result['source_coverage'] = []
    for row in coverage:
        if (not isinstance(row, dict) or not isinstance(row.get('source_id'), str)
                or not 0 < len(row['source_id']) <= 128
                or row.get('coverage_state') not in ('COMPLETE', 'PARTIAL', 'UNAVAILABLE', 'UNVERIFIED')
                or type(row.get('complete_for_entry')) is not bool
                or type(row.get('complete_for_risk')) is not bool):
            raise ValueError('Invalid event source coverage row')
        result['source_coverage'].append({key: row[key] for key in (
            'source_id', 'coverage_state', 'complete_for_entry', 'complete_for_risk')})
    traces = source.get('traces', [])
    if not isinstance(traces, list) or len(traces) > 2:
        raise ValueError('Invalid event decision traces')
    result['traces'] = []
    for row in traces:
        if (not isinstance(row, dict) or row.get('status') != 'HOLD'
                or not isinstance(row.get('reason'), str) or not 0 < len(row['reason']) <= 128
                or not isinstance(row.get('opportunity_id'), str)
                or re.fullmatch(r'[0-9a-f]{64}', row['opportunity_id']) is None):
            raise ValueError('Invalid event decision trace')
        result['traces'].append({key: row[key] for key in ('opportunity_id', 'status', 'reason')})
    for key, rows in (('source_coverage_count', coverage), ('trace_count', traces)):
        count = source.get(key, len(rows))
        if type(count) is not int or count < len(rows) or count > 10_000:
            raise ValueError('Invalid event projection count')
        result[key] = count
    return result


def controller_observation(entry):
    controller = entry['controller']
    result = {'controller_id': entry['controller_id'],
              'controller_name': getattr(controller.config, 'controller_name', None),
              'profile': getattr(controller.config, 'profile', None),
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
        if 'strategy_components' in custom:
            result['custom_info']['strategy_components'] = strategy_components_observation(custom['strategy_components'])
        if getattr(controller.config, 'controller_name', None) == 'cross_venue_event_v2' and 'event_projection' in custom:
            result['custom_info']['event_projection'] = event_projection_observation(custom['event_projection'])
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
    entry_lifecycle = custom.get('entry_lifecycle')
    if isinstance(entry_lifecycle, dict):
        result['entry_lifecycle'] = {
            key: scalar(entry_lifecycle[key])
            for key in ENTRY_LIFECYCLE_FIELDS
            if key in entry_lifecycle
        }
    entry_acquisition = custom.get('entry_acquisition')
    if isinstance(entry_acquisition, dict):
        result['entry_acquisition'] = {
            key: scalar(entry_acquisition[key])
            for key in ENTRY_ACQUISITION_FIELDS
            if key in entry_acquisition
        }
        order_ids = entry_acquisition.get('order_ids')
        if isinstance(order_ids, list):
            result['entry_acquisition']['order_ids'] = [str(order_id) for order_id in order_ids]
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
