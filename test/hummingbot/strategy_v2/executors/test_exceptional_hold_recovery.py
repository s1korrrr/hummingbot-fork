import copy
import hashlib
import json
import sqlite3
from decimal import Decimal
from test.hummingbot.strategy_v2.executors.test_planned_position_resume import stopped_fixture
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.strategy_v2.executors import exceptional_hold_recovery as recovery, planned_position_resume as resume
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator


async def pending_fixture(tmp_path):
    db, before, root, recorder, executor, evidence = await stopped_fixture(tmp_path)
    with sqlite3.connect(db) as connection:
        connection.execute('UPDATE Executors SET status=3, is_active=0, is_trading=0')
        connection.execute('UPDATE Position SET amount=200000')
        risk = {'version': 1, 'controllers': ['owner'], 'drawdown_exited_controllers': [],
                'max_global_pnl': '12.3', 'max_pnl_by_controller': {'owner': '12.3'},
                'operator_paused': {'owner': False}, 'stopped': True}
        connection.execute('INSERT OR REPLACE INTO Metadata VALUES (?, ?)',
                           ('strategy-risk:owner.yml', json.dumps(risk)))
        connection.commit()
        with sqlite3.connect(before) as destination:
            connection.backup(destination)
    with sqlite3.connect(before) as connection:
        risk['stopped'] = False
        connection.execute('UPDATE Metadata SET value=? WHERE key=?',
                           (json.dumps(risk), 'strategy-risk:owner.yml'))
        connection.commit()
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    (root / recovery.SOURCE).write_text((resume.ROOT / recovery.SOURCE).read_text())
    evidence['files'] = resume.bound_files(evidence['config_paths'], root)
    evidence['files'][recovery.SOURCE] = hashlib.sha256((root / recovery.SOURCE).read_bytes()).hexdigest()
    evidence['preflight_sha256'] = hashlib.sha256(before.read_bytes()).hexdigest()
    for phase in ('account_pending_before', 'account_pending_after'):
        evidence['reconciliation'][phase] = {'regular': [], 'algos': {kind: [] for kind in resume.SPOT_ALGO_TYPES}}
    refresh_hash(db, evidence)
    return db, before, root, evidence, recorder, executor


def refresh_hash(db, evidence):
    with sqlite3.connect(db) as connection:
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    sha = hashlib.sha256(db.read_bytes()).hexdigest()
    evidence['reconciliation']['database'] = {'sha256': sha, 'sha256_after': sha}


def rows(db, table):
    with sqlite3.connect(db) as connection:
        return connection.execute(f'SELECT * FROM "{table}"').fetchall()


@pytest.mark.asyncio
async def test_atomic_hold_completion_preserves_exact_ownership_and_risk(tmp_path):
    db, before, root, evidence, recorder, executor = await pending_fixture(tmp_path)
    protected = {table: rows(db, table) for table in ('Order', 'TradeFill', 'PositionHoldLedger')}
    receipt = recovery.recover(db, before, evidence, now=1000, root=root, restore_administrative_stop=True)
    assert receipt['original_stopped']['status'] == 3
    assert receipt['original_preflight']['status'] == 3
    assert receipt['ownership_archive']['position_rows'][0]['amount'] == 200000
    for table, expected in protected.items():
        assert rows(db, table) == expected
    assert rows(db, 'Position') == []
    with resume.connect(db) as connection:
        owner = connection.execute('SELECT * FROM Executors').fetchone()
        assert owner['status'] == 4 and owner['close_type'] == 10
        assert json.loads(owner['custom_info'])['exceptional_hold_recovery'] == receipt['id']
        risk = json.loads(connection.execute("SELECT value FROM Metadata WHERE key='strategy-risk:owner.yml'").fetchone()[0])
        assert risk['stopped'] is False and risk['max_global_pnl'] == '12.3'
        assert len(connection.execute(f'SELECT * FROM {recovery.TABLE}').fetchall()) == 1
    strategy = executor._strategy
    strategy.controllers = {'owner': SimpleNamespace(config=SimpleNamespace(connector_name='okx', trading_pair='ETH-USDT'))}
    strategy.markets = {'okx': {'ETH-USDT'}}
    strategy.market_data_provider = MagicMock()
    strategy.market_data_provider.get_price_by_type.return_value = Decimal('100')
    strategy.buy = MagicMock()
    strategy.sell = MagicMock()
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        restored = ExecutorOrchestrator(strategy)
    assert restored.recovery_required == {}
    assert sum(p.buy_amount_base - p.sell_amount_base for p in restored.positions_held['owner']) == Decimal('1.2')
    strategy.buy.assert_not_called()
    strategy.sell.assert_not_called()
    refresh_hash(db, evidence)
    with pytest.raises(ValueError, match='unfinished'):
        recovery.recover(db, before, evidence, now=1001, root=root)


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['stale', 'source', 'fill', 'projection', 'risk', 'running_preflight',
                                   'pending_algo', 'ledger_receipt', 'duplicate_owner'])
async def test_rejects_unproven_recovery_without_writes(tmp_path, fault):
    db, before, root, evidence, _, _ = await pending_fixture(tmp_path)
    if fault == 'stale':
        evidence['observed_at'] = 0
    elif fault == 'source':
        evidence['files'][recovery.SOURCE] = 'bad'
    elif fault == 'fill':
        evidence['reconciliation']['selected_executors'][0]['venue_orders'][0]['raw_fills'][0]['fillSz'] = '2'
    elif fault == 'projection':
        with sqlite3.connect(db) as connection:
            connection.execute('UPDATE Position SET amount=999')
        refresh_hash(db, evidence)
    elif fault == 'risk':
        with sqlite3.connect(db) as connection:
            state = json.loads(connection.execute("SELECT value FROM Metadata WHERE key='strategy-risk:owner.yml'").fetchone()[0])
            state['max_global_pnl'] = '0'
            connection.execute("UPDATE Metadata SET value=? WHERE key='strategy-risk:owner.yml'", (json.dumps(state),))
        refresh_hash(db, evidence)
    elif fault == 'running_preflight':
        with sqlite3.connect(before) as connection:
            connection.execute('UPDATE Executors SET status=2, close_type=NULL, is_active=1')
            connection.commit()
            connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        evidence['preflight_sha256'] = hashlib.sha256(before.read_bytes()).hexdigest()
    elif fault == 'pending_algo':
        evidence['reconciliation']['account_pending_after']['algos']['conditional'] = [{'ordId': 'pending'}]
    elif fault == 'ledger_receipt':
        with sqlite3.connect(db) as connection:
            connection.execute("UPDATE PositionHoldLedger SET order_payload='{}' WHERE client_order_id='entry'")
        refresh_hash(db, evidence)
    elif fault == 'duplicate_owner':
        with sqlite3.connect(db) as connection:
            columns = [row[1] for row in connection.execute('PRAGMA table_info(PositionHoldLedger)')]
            selected = ["id || '-duplicate'" if name == 'id' else name for name in columns]
            connection.execute(f"INSERT INTO PositionHoldLedger SELECT {', '.join(selected)} FROM PositionHoldLedger WHERE client_order_id='entry'")
        refresh_hash(db, evidence)
    original = {table: rows(db, table) for table in ('Executors', 'Position', 'PositionHoldLedger', 'Metadata')}
    with pytest.raises(ValueError):
        recovery.recover(db, before, evidence, now=1000, root=root, restore_administrative_stop=True)
    for table, expected in original.items():
        assert rows(db, table) == expected


def test_risk_release_rejects_peak_or_pause_changes():
    before = {'version': 1, 'controllers': ['a'], 'drawdown_exited_controllers': [],
              'max_global_pnl': '3', 'max_pnl_by_controller': {'a': '3'},
              'operator_paused': {'a': False}, 'stopped': False}
    after = {**before, 'stopped': True}
    assert recovery.restored_risk_state(before, after) == before
    changed = copy.deepcopy(after)
    changed['operator_paused']['a'] = True
    with pytest.raises(ValueError, match='beyond'):
        recovery.restored_risk_state(before, changed)


@pytest.mark.asyncio
async def test_wal_write_between_validation_and_lock_is_rejected(tmp_path, monkeypatch):
    db, before, root, evidence, _, _ = await pending_fixture(tmp_path)
    validate = resume.validate_reconciliation
    writer = sqlite3.connect(db)

    def race(*args):
        validate(*args)
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute("UPDATE Metadata SET value='changed' WHERE key='strategy-risk:owner.yml'")
        writer.commit()

    monkeypatch.setattr(resume, 'validate_reconciliation', race)
    original = rows(db, 'Executors')
    try:
        with pytest.raises(ValueError, match='WAL'):
            recovery.recover(db, before, evidence, now=1000, root=root)
        assert rows(db, 'Executors') == original
    finally:
        writer.close()


@pytest.mark.asyncio
async def test_receipt_failure_rolls_back_terminal_projection_and_risk_changes(tmp_path, monkeypatch):
    db, before, root, evidence, _, _ = await pending_fixture(tmp_path)
    original = {table: rows(db, table) for table in ('Executors', 'Position', 'PositionHoldLedger', 'Metadata')}
    canonical = resume.canonical

    def fail_receipt(value):
        if isinstance(value, dict) and value.get('kind') == 'exceptional_stopped_hold_completion':
            raise RuntimeError('receipt unavailable')
        return canonical(value)

    monkeypatch.setattr(resume, 'canonical', fail_receipt)
    with pytest.raises(RuntimeError, match='receipt unavailable'):
        recovery.recover(db, before, evidence, now=1000, root=root, restore_administrative_stop=True)
    for table, expected in original.items():
        assert rows(db, table) == expected
    with resume.connect(db) as connection:
        assert recovery.TABLE not in resume.tables(connection)
