"""Reviewed stopped-owner risk transitions in the existing native Metadata store.

This module has no CLI and never contacts a venue. Its caller must hold the
owner's maintenance lock, prove entries fenced and the process stopped, and
retain a consistent backup. Planning is read-only. Applying rechecks a bounded
financial-state digest inside BEGIN IMMEDIATE, then archives the exact before
and after state atomically. An approval receipt is evidence of operator intent,
not a replacement for those external owner/venue checks.
"""
import copy
import hashlib
import json
import math
import re
from decimal import Decimal

from hummingbot.strategy_v2.models.daily_entry_risk import DailyEntryRisk
from hummingbot.strategy_v2.models.entry_risk_enrollment import EntryRiskEnrollment

TABLES = ('Metadata', 'Controllers', 'Executors', 'Order', 'OrderExact', 'OrderStatus', 'TradeFill', 'TradeFillExact',
          'Position', 'PositionHoldLedger', 'InventoryCost', 'MarketState', 'FundingPayment',
          'RangePositionCollectedFees', 'RangePositionUpdate')


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def financial_state_digest(connection, *, metadata_overrides=None, omitted_metadata_keys=()):
    """Bind all native financial rows, operator receipts and recovery identities."""
    metadata_overrides = metadata_overrides or {}
    omitted_metadata_keys = set(omitted_metadata_keys)
    available = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    _require('Metadata' in available, 'Native Metadata store is required')
    digest = hashlib.sha256()
    for table in TABLES:
        digest.update(table.encode())
        if table not in available:
            digest.update(b'absent')
            continue
        schema = connection.execute('SELECT sql FROM sqlite_master WHERE name=?', (table,)).fetchone()[0]
        digest.update(schema.encode())
        columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')]
        order = ','.join('"' + name.replace('"', '""') + '"' for name in columns)
        for source_row in connection.execute(f'SELECT * FROM "{table}" ORDER BY {order}'):
            row = list(source_row)
            if table == 'Metadata':
                values_by_column = dict(zip(columns, row))
                if values_by_column.get('key') in omitted_metadata_keys:
                    continue
                if values_by_column.get('key') in metadata_overrides:
                    values_by_column['value'] = metadata_overrides[values_by_column['key']]
                    row = [values_by_column[column] for column in columns]
                elif isinstance(values_by_column.get('key'), str) and values_by_column['key'].startswith('strategy-risk:'):
                    try:
                        values_by_column['value'] = _canonical(json.loads(values_by_column['value']))
                        row = [values_by_column[column] for column in columns]
                    except (TypeError, ValueError):
                        pass
            digest.update(_canonical(list(row)).encode())
            digest.update(b'\n')
    return digest.hexdigest()


def financial_state_digest_before_prospective_daily_enrollment(connection, script_config, before_state, archive_key):
    """Reconstruct the exact financial digest immediately before the metadata transition."""
    return financial_state_digest(
        connection,
        metadata_overrides={'strategy-risk:' + script_config: _canonical(before_state)},
        omitted_metadata_keys=(archive_key,))


def _validate_state(state, controllers):
    _require(isinstance(state, dict) and type(state.get('version')) is int and state['version'] in (1, 2),
             'Invalid native risk state version')
    _require(state.get('controllers') == controllers
             and set(state.get('max_pnl_by_controller', {})) == set(controllers)
             and set(state.get('operator_paused', {})) == set(controllers)
             and all(type(v) is bool for v in state['operator_paused'].values())
             and type(state.get('stopped')) is bool
             and isinstance(state.get('drawdown_exited_controllers'), list)
             and set(state['drawdown_exited_controllers']).issubset(controllers),
             'Incomplete native risk state or controller scope mismatch')
    for value in [state.get('max_global_pnl'), *state['max_pnl_by_controller'].values()]:
        _require(not isinstance(value, bool) and Decimal(str(value)).is_finite(),
                 'Native risk peaks are unavailable')
    watermarks = state.get('operator_watermarks', {})
    _require(isinstance(watermarks, dict) and set(watermarks).issubset(controllers), 'Invalid operator scope')
    for value in watermarks.values():
        _require(isinstance(value, dict) and set(value) == {'requested_at', 'command_id'}
                 and type(value['requested_at']) in (float, int) and math.isfinite(value['requested_at'])
                 and value['requested_at'] >= 0 and isinstance(value['command_id'], str)
                 and 1 <= len(value['command_id']) <= 128, 'Invalid durable operator receipt')
    if state['version'] == 2:
        enrollment = EntryRiskEnrollment.model_validate(state.get('entry_risk_enrollment'))
        _require(not enrollment.pending, 'Pending enrollment must complete before another risk transition')
        enrollment.validate_daily_state(DailyEntryRisk.model_validate(state.get('daily_entry_risk')))
    else:
        _require(state.get('entry_risk_enrollment') is None, 'Enrollment requires native risk version 2')


def _prospective_daily_enrollment_after(before, request, controllers):
    """Derive the one allowed pending v1→v2 transition without mutating state."""
    _validate_state(before, controllers)
    _require(before['version'] == 1 and before.get('daily_entry_risk') is None,
             'Prospective enrollment requires preserved legacy V1 risk state')
    enrollment = EntryRiskEnrollment(
        schema_version=1, command_id=request['command_id'], limit_quote=request['limit_quote'],
        requested_at=request['requested_at'], historical_peaks_available=True)
    after = copy.deepcopy(before)
    after.update(version=2, operator_paused={cid: True for cid in controllers},
                 entry_risk_enrollment=enrollment.snapshot())
    return after, enrollment


def validate_prospective_daily_enrollment_archive(
        archive_key, archive_value, *, script_config, controllers, current_state, configured_limit,
        before_state=None, expected_before_state_sha256=None, expected_native_state_sha256=None):
    """Validate a pending daily enrollment against its immutable native receipt.

    This is deliberately narrower than accepting risk-state version 2: it
    accepts only the exact prospective enrollment transition produced by this
    module, from the supplied original V1 state to the current pending V2 row.
    """
    _require(isinstance(archive_key, str) and isinstance(archive_value, str),
             'Native daily-risk transition archive is unavailable')
    try:
        archived = json.loads(archive_value)
    except (TypeError, ValueError):
        raise ValueError('Native daily-risk transition archive is malformed') from None
    _require(isinstance(archived, dict), 'Native daily-risk transition archive is malformed')
    _require(_canonical(archived) == archive_value,
             'Native daily-risk transition archive is not canonical')
    expected_fields = {'schema_version', 'script_config', 'request', 'kind', 'before', 'after',
                       'archive_key', 'native_state_sha256', 'transition_id'}
    _require(isinstance(archived, dict) and set(archived) == expected_fields
             and type(archived.get('schema_version')) is int and archived['schema_version'] == 1
             and archived.get('script_config') == script_config
             and archived.get('kind') == 'prospective_daily_enrollment_preserve_history',
             'Native daily-risk transition receipt has an unsupported scope or kind')
    request = archived['request']
    _require(isinstance(request, dict) and set(request) == {
        'command_id', 'requested_at', 'limit_quote', 'controllers', 'owner_stopped',
        'entries_fenced', 'allow_new_risk_scope'},
        'Native daily-risk transition request is malformed')
    _require(request['owner_stopped'] is True and request['entries_fenced'] is True
             and request['allow_new_risk_scope'] is False
             and request['controllers'] == controllers
             and controllers == sorted(set(controllers)),
             'Native daily-risk transition owner/controller scope differs')
    try:
        configured_limit = Decimal(str(configured_limit))
        request_limit = Decimal(str(request['limit_quote']))
        requested_at = EntryRiskEnrollment(
            schema_version=1, command_id=request['command_id'], limit_quote=request['limit_quote'],
            requested_at=request['requested_at'], historical_peaks_available=True).requested_at
    except (ArithmeticError, TypeError, ValueError):
        raise ValueError('Native daily-risk transition request is malformed') from None
    _require(configured_limit.is_finite() and configured_limit > 0
             and request_limit.is_finite() and request_limit == configured_limit,
             'Native daily-risk enrollment limit differs from staged runner configuration')
    expected_key = 'strategy-risk-transition:' + _digest([script_config, request['command_id']])
    _require(archive_key == expected_key and archived['archive_key'] == expected_key,
             'Native daily-risk transition archive identity differs')
    _require(isinstance(archived['native_state_sha256'], str)
             and len(archived['native_state_sha256']) == 64
             and all(character in '0123456789abcdef' for character in archived['native_state_sha256']),
             'Native daily-risk transition source digest is malformed')
    if expected_native_state_sha256 is not None:
        _require(archived['native_state_sha256'] == expected_native_state_sha256,
                 'Native daily-risk transition pre-state digest does not match the stopped database')
    proposal = {key: value for key, value in archived.items() if key != 'transition_id'}
    _require(archived['transition_id'] == _digest(proposal),
             'Native daily-risk transition identity digest differs')
    _require(isinstance(archived['before'], dict),
             'Native daily-risk transition original risk state is unavailable')
    before_digest = _digest(archived['before'])
    if before_state is not None:
        _require(archived['before'] == before_state,
                 'Native daily-risk transition does not preserve the original preflight risk state')
    if expected_before_state_sha256 is not None:
        _require(before_digest == expected_before_state_sha256,
                 'Native daily-risk transition original state digest changed')
    expected_after, enrollment = _prospective_daily_enrollment_after(
        archived['before'], request, controllers)
    _require(enrollment.pending and enrollment.historical_peaks_available
             and enrollment.limit_quote == configured_limit
             and enrollment.requested_at == requested_at
             and archived['after'] == expected_after and current_state == expected_after,
             'Native daily-risk transition changed preserved history or current pending state')
    return {
        'schema_version': 1,
        'kind': 'prospective_daily_enrollment_preserve_history',
        'archive_key': expected_key,
        'transition_id': archived['transition_id'],
        'archive_sha256': hashlib.sha256(archive_value.encode()).hexdigest(),
        'before_state_sha256': before_digest,
        'after_state_sha256': _digest(current_state),
        'request_sha256': _digest(request),
        'limit_quote': str(configured_limit),
    }


def plan_entry_risk_transition(connection, script_config, request):
    """Build a deterministic proposal; no records or flags are changed.

    A first daily enrollment samples PnL only after native restart reports are
    fresh. Missing entire history additionally requires an explicit new scope;
    neither this planner nor the runner infers historical peaks. All enrollment
    proposals leave operator entries paused. A limit-only transition preserves
    every baseline, breach, latch, peak and command receipt.
    """
    _require(isinstance(script_config, str) and re.fullmatch(r'[A-Za-z0-9_.-]+\.yml', script_config),
             'One native script owner is required')
    _require(isinstance(request, dict) and set(request) == {
        'command_id', 'requested_at', 'limit_quote', 'controllers', 'owner_stopped',
        'entries_fenced', 'allow_new_risk_scope'}, 'Explicit bounded risk request is required')
    _require(request['owner_stopped'] is True and request['entries_fenced'] is True,
             'Stopped and entry-fenced owner proof is required')
    _require(type(request['allow_new_risk_scope']) is bool, 'New risk scope acknowledgement must be boolean')
    controllers = request['controllers']
    _require(isinstance(controllers, list) and controllers
             and all(isinstance(cid, str) and re.fullmatch(r'[A-Za-z0-9_.:-]{1,180}', cid) for cid in controllers)
             and controllers == sorted(set(controllers)),
             'Exact sorted native controller scope is required')
    enrollment = EntryRiskEnrollment(schema_version=1, command_id=request['command_id'], limit_quote=request['limit_quote'],
                                     requested_at=request['requested_at'], historical_peaks_available=True)
    key = 'strategy-risk:' + script_config
    row = connection.execute('SELECT value FROM Metadata WHERE key=?', (key,)).fetchone()
    before = json.loads(row[0]) if row is not None else None
    if before is None:
        _require(request['allow_new_risk_scope'], 'Missing historical risk requires explicit new-scope approval')
        # This is a prospective policy with unknown historical peaks, not recovered history.
        after = {'version': 2, 'controllers': controllers, 'max_global_pnl': None,
                 'max_pnl_by_controller': {cid: None for cid in controllers},
                 'drawdown_exited_controllers': [], 'stopped': False,
                 'operator_paused': {cid: True for cid in controllers}}
        enrollment.historical_peaks_available = False
        after['entry_risk_enrollment'] = enrollment.snapshot()
        kind = 'prospective_new_risk_scope'
    else:
        _require(not request['allow_new_risk_scope'], 'Existing historical risk must be preserved')
        _validate_state(before, controllers)
        after = copy.deepcopy(before)
        daily = before.get('daily_entry_risk')
        if daily is not None:
            current = DailyEntryRisk.model_validate(daily)
            _require(current.limit_quote != enrollment.limit_quote, 'Requested daily loss limit is already current')
            after['daily_entry_risk']['limit_quote'] = str(enrollment.limit_quote)
            if after.get('entry_risk_enrollment') is not None:
                after['entry_risk_enrollment']['limit_quote'] = str(enrollment.limit_quote)
            kind = 'limit_only_preserve_history'
        else:
            after, _ = _prospective_daily_enrollment_after(before, request, controllers)
            kind = 'prospective_daily_enrollment_preserve_history'
    archive_key = 'strategy-risk-transition:' + _digest([script_config, request['command_id']])
    _require(connection.execute('SELECT 1 FROM Metadata WHERE key=?', (archive_key,)).fetchone() is None,
             'Risk transition command was already applied')
    proposal = {'schema_version': 1, 'script_config': script_config, 'request': copy.deepcopy(request),
                'kind': kind, 'before': before, 'after': after, 'archive_key': archive_key,
                'native_state_sha256': financial_state_digest(connection)}
    return {**proposal, 'transition_id': _digest(proposal)}


def apply_entry_risk_transition(connection, proposal, *, now):
    """Atomically apply an exact reviewed proposal; a changed row or replay refuses."""
    _require(not connection.in_transaction, 'Risk transition owns its SQLite transaction')
    _require(type(now) in (float, int) and math.isfinite(now), 'Finite transition clock is required')
    _require(isinstance(proposal, dict), 'Reviewed transition proposal is required')
    stamp = proposal['request']['requested_at']
    _require(type(stamp) in (float, int) and 0 <= now - stamp < 300, 'Fresh approved risk transition is required')
    connection.execute('BEGIN IMMEDIATE')
    try:
        actual = plan_entry_risk_transition(connection, proposal['script_config'], proposal['request'])
        _require(actual == proposal, 'Native state or reviewed risk transition changed')
        key = 'strategy-risk:' + actual['script_config']
        connection.execute('INSERT INTO Metadata (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                           (key, _canonical(actual['after'])))
        connection.execute('INSERT INTO Metadata (key,value) VALUES (?,?)',
                           (actual['archive_key'], _canonical(actual)))
        # Own the SQL transaction even when sqlite3.Connection.autocommit=True;
        # Python's commit()/rollback() methods are no-ops in that mode.
        connection.execute('COMMIT')
    except BaseException:
        if connection.in_transaction:
            connection.execute('ROLLBACK')
        raise
    return actual['transition_id']
