"""Offline proof for the one V1 failed-preclaim forward-repair exception."""
import json
import sqlite3

import pytest

from hummingbot.strategy_v2.executors import planned_position_resume as resume
from hummingbot.strategy_v2.migrations.entry_risk import TABLES, validate_stopped_forward_repair_delta

PAIRS = {f'ok-rsi-{asset.lower()}': f'{asset}-USDC' for asset in ('BNB', 'BTC', 'ETH', 'SOL', 'XRP')}
SCRIPT = 'ok_rsi.yml'
ARCHIVE = 'strategy-risk-transition:synthetic'


def _decision_key(controller, pair):
    return 'ok-rsi-entry-decision:' + json.dumps([SCRIPT, controller, 'okx', pair, '1m'], separators=(',', ':'))


def _database(path, *, after):
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE Metadata (key TEXT PRIMARY KEY, value TEXT)')
        connection.execute('CREATE TABLE Position (id TEXT PRIMARY KEY, controller_id TEXT, side TEXT, '
                           'timestamp INTEGER, amount INTEGER, breakeven_price INTEGER, '
                           'unrealized_pnl_quote INTEGER, realized_pnl_quote INTEGER, cum_fees_quote INTEGER)')
        connection.execute('CREATE TABLE TradeFill (id TEXT PRIMARY KEY, trade_fee_in_quote INTEGER)')
        for table in TABLES:
            if table not in {'Metadata', 'Position', 'TradeFill'}:
                connection.execute(f'CREATE TABLE "{table}" (id TEXT PRIMARY KEY)')
        connection.execute('INSERT INTO Metadata VALUES (?, ?)', ('strategy-risk:' + SCRIPT,
                           json.dumps({'version': 2 if after else 1})))
        if after:
            connection.execute('INSERT INTO Metadata VALUES (?, ?)', (ARCHIVE, '{}'))
        for controller, pair in PAIRS.items():
            state = {'version': 1, 'candle': 900 if after else 600, 'armed': True,
                     'consumed_candle': 0, 'authorization': None}
            connection.execute('INSERT INTO Metadata VALUES (?, ?)',
                               (_decision_key(controller, pair), json.dumps(state, sort_keys=True)))
            connection.execute('INSERT INTO Position VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                               (controller, controller, 'BUY', 940000 if after else 700000,
                                1000000, 100000000, 1000000 if after else 0, 0, 10000))
        connection.execute('INSERT INTO TradeFill VALUES (?, ?)', ('fill', 100))


def _proof(before, current):
    with sqlite3.connect(f'file:{before}?mode=ro', uri=True) as old, \
            sqlite3.connect(f'file:{current}?mode=ro', uri=True) as new:
        return validate_stopped_forward_repair_delta(
            old, new, script_config=SCRIPT, controller_pairs=PAIRS,
            archive_key=ARCHIVE, observed_at=1000,
            failed_boot_started_at=920, failed_boot_finished_at=950)


def test_forward_repair_accepts_only_five_advanced_decisions_and_marks(tmp_path):
    before, current = tmp_path / 'before.sqlite', tmp_path / 'current.sqlite'
    _database(before, after=False)
    _database(current, after=True)
    proof = _proof(before, current)
    assert len(proof['decision_values']) == len(proof['position_marks']) == 5
    assert proof['stopped_financial_state_sha256'] != proof['current_financial_state_sha256']


@pytest.mark.parametrize('mutation', [
    'authorization', 'candle_regression', 'candle_off_grid', 'inventory',
    'mark_price', 'fee', 'extra_metadata', 'missing_position',
    'candle_outside_boot', 'mark_outside_boot', 'order_status',
])
def test_forward_repair_rejects_non_mark_financial_or_decision_drift(tmp_path, mutation):
    before, current = tmp_path / 'before.sqlite', tmp_path / 'current.sqlite'
    _database(before, after=False)
    _database(current, after=True)
    with sqlite3.connect(current) as connection:
        key = _decision_key('ok-rsi-bnb', 'BNB-USDC')
        if mutation in {'authorization', 'candle_regression', 'candle_off_grid', 'candle_outside_boot'}:
            state = json.loads(connection.execute('SELECT value FROM Metadata WHERE key=?', (key,)).fetchone()[0])
            if mutation == 'authorization':
                state['authorization'] = {'executor_id': 'invented'}
            elif mutation == 'candle_regression':
                state['candle'] = 540
            elif mutation == 'candle_outside_boot':
                state['candle'] = 840
            else:
                state['candle'] = 901
            connection.execute('UPDATE Metadata SET value=? WHERE key=?', (json.dumps(state), key))
        elif mutation == 'inventory':
            connection.execute('UPDATE Position SET amount=amount+1 WHERE id=?', ('ok-rsi-bnb',))
        elif mutation == 'mark_price':
            connection.execute('UPDATE Position SET unrealized_pnl_quote=-200000000 WHERE id=?', ('ok-rsi-bnb',))
        elif mutation == 'fee':
            connection.execute('UPDATE TradeFill SET trade_fee_in_quote=101')
        elif mutation == 'order_status':
            connection.execute('INSERT INTO OrderStatus VALUES (?)', ('new-status',))
        elif mutation == 'mark_outside_boot':
            connection.execute('UPDATE Position SET timestamp=960000 WHERE id=?', ('ok-rsi-bnb',))
        elif mutation == 'extra_metadata':
            connection.execute('INSERT INTO Metadata VALUES (?, ?)', ('unreviewed', '1'))
        else:
            connection.execute('DELETE FROM Position WHERE id=?', ('ok-rsi-bnb',))
    with pytest.raises(ValueError, match='Forward repair'):
        _proof(before, current)


@pytest.mark.parametrize('mutation', [None, 'process_window', 'venue_fill', 'venue_pending',
                                      'venue_method', 'venue_process', 'venue_window'])
def test_failed_boot_receipts_bound_to_one_stopped_get_only_window(tmp_path, mutation):
    process_path, venue_path = tmp_path / 'process.json', tmp_path / 'venue.json'
    process = {'container_id': 'synthetic-container', 'image': 'sha256:' + 'a' * 64,
               'started_at': 920, 'finished_at': 950, 'running': False, 'exit_code': 0}
    venue = {'observed_at': 960, 'http_methods': ['GET'], 'native_application_started': False,
             'window_begin_ms': 918000, 'window_end_ms': 952000,
             'fills': {'code': '0', 'data': [], 'msg': ''},
             'pending': [{'pair': pair, 'regular': [], 'regular_count': 0,
                          'algos': {name: [] for name in resume.SPOT_ALGO_TYPES}, 'algo_count': 0}
                         for pair in PAIRS.values()],
             'container_proof': process.copy()}
    if mutation == 'process_window':
        process['finished_at'] = 1010
    elif mutation == 'venue_fill':
        venue['fills']['data'] = [{'invented': 'fill'}]
    elif mutation == 'venue_pending':
        venue['pending'][0]['regular_count'] = 1
    elif mutation == 'venue_method':
        venue['http_methods'] = ['POST']
    elif mutation == 'venue_process':
        venue['container_proof']['image'] = 'sha256:' + 'b' * 64
    elif mutation == 'venue_window':
        venue['window_end_ms'] = 940000
    process_path.write_text(json.dumps(process))
    venue_path.write_text(json.dumps(venue))
    if mutation is None:
        result = resume._validated_failed_boot_receipts(process_path, venue_path, PAIRS, 910, 970)
        assert result['started_at'] == 920 and result['finished_at'] == 950
        assert result['process_sha256'] and result['venue_sha256']
    else:
        with pytest.raises(ValueError, match='forward repair'):
            resume._validated_failed_boot_receipts(process_path, venue_path, PAIRS, 910, 970)
