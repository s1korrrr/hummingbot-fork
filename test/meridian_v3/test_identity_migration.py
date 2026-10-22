"""F12: a code-only upgrade carries owner state only through the explicit operator migration.

The upgrade is simulated by changing the bytes the seals read (see test_source_seal),
so every identity moves exactly as a new image would move it. Native orders, fills,
executors, positions and restart continuity are the real paper engine.
"""
import json
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.lifecycle_harness import Scenario, creates, rebound
from test.meridian_v3.test_source_seal import mutate
from unittest import IsolatedAsyncioTestCase

import pytest

from controllers.generic.meridian_v3_unified_core import migration
from controllers.generic.meridian_v3_unified_core.migration import (
    IDENTITY_MIGRATION_REQUIRED,
    IDENTITY_WITNESS_MISSING,
    MigrationRefused,
    apply_identity_migration,
    plan_identity_migration,
)
from controllers.generic.modular_spot_core.state import decode
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata

BTC, ETH, SOL = 'BTC-USDC', 'ETH-USDC', 'SOL-USDC'
UPGRADED = 'controllers/generic/modular_spot_core/bag.py'  # moves the p23 fingerprint and every Meridian seal


class OwnerStateCase(IsolatedAsyncioTestCase):
    """A paper owner with held and active state, closed and restored under changed identities."""
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def asyncSetUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.directory = Path(self._directory.name)
        self.scenarios = []
        self.patch = pytest.MonkeyPatch()

    async def asyncTearDown(self):
        for scenario in self.scenarios:
            if not scenario.session._closed:
                await scenario.close()
        self.patch.undo()
        self._directory.cleanup()

    def scenario(self, **kwargs):
        scenario = Scenario(self.directory, **kwargs)
        self.scenarios.append(scenario)
        return scenario

    async def entered(self, s, pair):
        receipt = await s.step(rsi={pair: rebound()})
        [create] = creates(receipt, pair)
        await s.fill(pair)
        [executor] = [e for e in s.executors(pair) if e.config.id == create.executor_config.id]
        return executor

    async def owner_with_active_and_held_state(self, **config):
        """BTC is a held bag, ETH an active filled tranche; both keep bound grants."""
        s = self.scenario(**config)
        await s.step()  # the first observed cut only seeds each symbol
        btc = await self.entered(s, BTC)
        eth = await self.entered(s, ETH)
        await s.step(mids={BTC: '96.9'})
        await s.step(mids={BTC: '96.9'})
        self.assertGreater(s.held(BTC), 0)
        self.assertTrue(eth.is_active and eth.open_filled_amount > 0)
        grants = s.grants()
        self.assertEqual({g.symbol for g in grants}, {BTC, ETH})
        self.assertEqual({g.status for g in grants}, {'BOUND'})
        return await self.closed(s, grants=grants, held=s.held(BTC), eth=eth.config.id, btc=btc.config.id)

    async def closed(self, s, **state):
        state.update(rows=self.rows(s), cash=s.cash, minute=s.minute, sequence=dict(s.sequence), trades=dict(s.trade_sequence))
        await s.close()
        return state

    def restore(self, state, **config):
        # The paper session marker binds its controller files; the new sealed file is the
        # operator's reseal. Live owners have no such marker; the recorder state is the subject.
        import hashlib
        from test.meridian_v3.test_unified_runtime import unified
        marker = self.directory / 'meridian.sqlite.paper-session.json'
        values = json.loads(marker.read_text())
        values['controllers_sha256'] = hashlib.sha256(json.dumps(
            [unified(execution_enabled=True, **config).model_dump(mode='json')], sort_keys=True).encode()).hexdigest()
        marker.write_text(json.dumps(values))
        r = self.scenario(restore=True, cash=state.get('cash', '500'), **config)
        r.minute, r.sequence, r.trade_sequence = state['minute'], dict(state['sequence']), dict(state['trades'])
        return r

    def rows(self, s):
        with s.session.recorder.sql_manager.get_new_session() as session:
            return {row.key: row.value for row in session.query(Metadata).all()}

    BAG_TRAIL = 'modular-spot:bag:v1:'  # held-bag management keeps running during an identity hold

    def migration_rows(self, subject):
        rows = subject['rows'] if isinstance(subject, dict) else self.rows(subject)
        return {key: value for key, value in rows.items() if not key.startswith(self.BAG_TRAIL)}

    def sessions(self, s):
        recorder = s.session.recorder
        return recorder.sql_manager.get_new_session, recorder.config_file_path

    def decision_keys(self, s):
        from controllers.generic.modular_spot_core.state import DecisionStore
        recorder, c = s.session.recorder, s.controller.config
        return {pair: DecisionStore(recorder, c.id, c.connector_name, pair).key for pair in s.controller.contexts}

    def decisions(self, s):
        return {pair: c.runtime.ensure_store().load()[0] for pair, c in s.controller.contexts.items()}


class IdentityMigrationTests(OwnerStateCase):
    async def test_code_upgrade_holds_entries_until_migration_then_carries_every_obligation(self):
        before = await self.owner_with_active_and_held_state()
        mutate(self.patch, UPGRADED)
        r = self.restore(before)
        # Fail closed and observable: no entry, no checkpoint rewrite, protective management continues.
        receipt = await r.step(mids={BTC: '96.9'}, rsi={SOL: rebound()})
        self.assertEqual(creates(receipt), [])
        hold = r.controller.portfolio.identity_hold
        self.assertTrue(hold.startswith(f'{IDENTITY_MIGRATION_REQUIRED}:migration_available:code_upgrade:'), hold)
        info = r.controller.get_custom_info()
        self.assertEqual(info['identity_hold'], hold)
        self.assertEqual(info['portfolio']['reason'], IDENTITY_MIGRATION_REQUIRED)
        summary = r.controller.get_status_summary()  # The fleet runtime status reports this gate.
        self.assertEqual((summary['gate'], summary['state']), (IDENTITY_MIGRATION_REQUIRED, IDENTITY_MIGRATION_REQUIRED))
        self.assertEqual(summary['note'], hold[:120])
        self.assertEqual({row['status'] for row in info['symbols'].values()}, {IDENTITY_MIGRATION_REQUIRED})
        self.assertEqual(self.migration_rows(r), self.migration_rows(before))  # nothing rewritten or deleted
        # The paper engine suspends filled tranches into held positions at close; live restart
        # continuity restores the persisted ETH tranche, which the upgraded code still accepts.
        [eth] = [i for i in r.session.recorder.get_all_executors() if i.id == before['eth']]
        from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
        self.assertIs(r.controller.restart_continuity_executor_class(eth.config), ModularPositionExecutor)
        self.assertEqual(r.held(BTC), before['held'])
        await r.step(mids={BTC: '96.8'})
        self.assertEqual(self.migration_rows(r), self.migration_rows(before))
        self.assertTrue(all(c.runtime.fault is None for c in r.controller.contexts.values()))

        plan = plan_identity_migration(r.config, *self.sessions(r))
        self.assertEqual(plan['kinds'], ['code_upgrade'])
        self.assertNotEqual(plan['from']['source_sha256'], plan['to']['source_sha256'])
        self.assertNotEqual(plan['from']['epoch'], plan['to']['epoch'])
        self.assertEqual(plan['from']['grant_policy_sha256'], plan['to']['grant_policy_sha256'])
        self.assertEqual({p for p, d in plan['carried']['decisions'].items() if d['claims']}, {BTC, ETH})
        old_states = {pair: decode(before['rows'][k]) for pair, k in self.decision_keys(r).items()
                      if k in before['rows']}
        result = apply_identity_migration(r.config, *self.sessions(r), plan['transition_id'])
        rows = self.rows(r)
        self.assertTrue(set(before['rows']) <= set(rows))  # no checkpoint deleted
        receipt_row = json.loads(rows[result['receipt_key']])
        self.assertEqual({k: v for k, v in receipt_row['before'].items()},
                         {k: before['rows'][k] for k in receipt_row['before']})
        for pair, old in old_states.items():
            new = decode(rows[self.decision_keys(r)[pair]])
            self.assertEqual(new.model_copy(update={'fingerprint': old.fingerprint}), old)
            self.assertEqual(new.fingerprint, r.controller.contexts[pair].runtime.fingerprint)
        grant_key = r.controller.portfolio.store().key
        self.assertEqual(rows[grant_key], before['rows'][grant_key])  # grants carried unchanged
        with self.assertRaises(MigrationRefused):
            apply_identity_migration(r.config, *self.sessions(r), plan['transition_id'])

        # Bound again: the old chain continues, grants and bags are intact, entries resume.
        head = r.controller.portfolio.target_store().head()
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        self.assertEqual(r.controller.get_status_summary()['gate'], 'ready')
        r.controller.set_operator_entry_pause(True, reason='test')
        self.assertEqual(r.controller.get_status_summary()['gate'], 'operator_entry_pause')
        r.controller.set_operator_entry_pause(False)
        runtime = r.controller.contexts[SOL].runtime
        runtime.fault = 'PolicyError:test'
        self.assertEqual(r.controller.get_status_summary()['gate'], 'symbol_fault')
        self.assertEqual(r.controller.get_status_summary()['note'], SOL)
        runtime.fault = None
        self.assertEqual({g.grant_id for g in r.grants()}, {g.grant_id for g in before['grants']})
        self.assertEqual(r.held(BTC), before['held'])
        receipt = await r.step(rsi={SOL: rebound()})
        self.assertEqual(len(creates(receipt, SOL)), 1, r.controller.to_format_status())
        stored = r.controller.portfolio.target_store().read()
        self.assertEqual(stored['previous_sha256'], head['target_sha256'])
        self.assertGreaterEqual(stored['target']['sequence'], head['sequence'])

    async def test_identity_hold_keeps_bag_recovery_sells_and_holds_only_acquisitions(self):
        before = await self.owner_with_active_and_held_state()
        mutate(self.patch, UPGRADED)
        r = self.restore(before)
        for mid in ('101', '103', '104'):
            receipt = await r.step(mids={BTC: mid}, rsi={BTC: (80, 80), SOL: rebound()})
            self.assertEqual([c for c in creates(receipt) if c.executor_config.side == TradeType.BUY], [])
        status = r.controller.contexts[BTC].runtime.bag_status or {}
        self.assertTrue(status.get('trail_armed'), status)
        receipt = await r.step(mids={BTC: '103.05'}, rsi={BTC: (80, 80)})
        [sell] = creates(receipt, BTC)
        self.assertEqual((sell.executor_config.side, sell.executor_config.amount), (TradeType.SELL, before['held']))
        self.assertTrue(r.controller.portfolio.identity_hold.startswith(IDENTITY_MIGRATION_REQUIRED))
        # Only the held-bag trail moves; the reviewed migration state is untouched.
        rows = self.rows(r)
        changed = {key for key in set(rows) | set(before['rows']) if rows.get(key) != before['rows'].get(key)}
        self.assertTrue(changed and all(key.startswith(self.BAG_TRAIL) for key in changed), changed)
        plan_identity_migration(r.config, *self.sessions(r))  # still plannable after the sell

    async def test_same_code_restart_is_bound_and_writes_nothing_new(self):
        before = await self.owner_with_active_and_held_state()
        r = self.restore(before)
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        self.assertEqual({k: v for k, v in self.rows(r).items() if k.startswith(migration.WITNESS_PREFIX)},
                         {k: v for k, v in before['rows'].items() if k.startswith(migration.WITNESS_PREFIX)})

    async def test_configuration_change_is_refused_with_or_without_new_code(self):
        before = await self.owner_with_active_and_held_state()
        for upgrade in (False, True):
            if upgrade:
                mutate(self.patch, UPGRADED)
            r = self.restore(before, max_candle_age_seconds=90)
            await r.step(rsi={SOL: rebound()})
            hold = r.controller.portfolio.identity_hold
            self.assertTrue(hold.startswith(f'{IDENTITY_MIGRATION_REQUIRED}:migration_refused:'), hold)
            self.assertIn('max_candle_age_seconds', hold)
            with self.assertRaisesRegex(MigrationRefused, 'behavioural configuration changed'):
                plan_identity_migration(r.config, *self.sessions(r))
            self.assertEqual(self.migration_rows(r), self.migration_rows(before))
            await r.close()
            before.update(minute=r.minute, sequence=dict(r.sequence), trades=dict(r.trade_sequence))

    async def test_state_without_a_witness_is_held_and_cannot_be_migrated(self):
        before = await self.owner_with_active_and_held_state()
        witness = [k for k in before['rows'] if k.startswith(migration.WITNESS_PREFIX)]
        self.assertEqual(len(witness), 1)
        mutate(self.patch, UPGRADED)
        r = self.restore(before)
        with r.session.recorder.sql_manager.get_new_session() as session, session.begin():
            session.delete(session.get(Metadata, witness[0]))  # an owner recorded before witnesses existed
        await r.step(rsi={SOL: rebound()})
        hold = r.controller.portfolio.identity_hold
        self.assertTrue(hold.startswith(IDENTITY_WITNESS_MISSING), hold)
        with self.assertRaisesRegex(MigrationRefused, 'no identity witness'):
            plan_identity_migration(r.config, *self.sessions(r))

    async def test_a_plan_is_refused_once_recorder_state_changes(self):
        before = await self.owner_with_active_and_held_state()
        mutate(self.patch, UPGRADED)
        r = self.restore(before)
        await r.step()
        plan = plan_identity_migration(r.config, *self.sessions(r))
        key = self.decision_keys(r)[BTC]
        with r.session.recorder.sql_manager.get_new_session() as session, session.begin():
            row = session.get(Metadata, key)
            state = decode(row.value)
            row.value = json.dumps({**json.loads(row.value), 'last_entry_time': str(state.last_entry_time + D('1'))},
                                   sort_keys=True, separators=(',', ':'))
        with self.assertRaisesRegex(MigrationRefused, 'changed since the reviewed plan'):
            apply_identity_migration(r.config, *self.sessions(r), plan['transition_id'])

    async def test_operator_cli_plans_and_applies_offline_then_the_owner_is_bound(self):
        from test.meridian_v3.test_unified_runtime import unified

        import yaml

        from scripts.meridian_v3_identity_migration import main
        before = await self.owner_with_active_and_held_state()
        mutate(self.patch, UPGRADED)
        sealed = self.directory / 'sealed.yml'
        sealed.write_text(yaml.safe_dump(unified(execution_enabled=True).model_dump(mode='json'), sort_keys=False))
        args = ['--config', str(sealed), '--database', str(self.directory / 'meridian.sqlite')]
        with self.assertRaisesRegex(MigrationRefused, 'no identity witness'):
            main(['plan', *args, '--recorder-name', 'another-strategy.yml'])
        plan = main(['plan', *args, '--recorder-name', 'native-paper-session'])
        with self.assertRaises(SystemExit):
            main(['apply', *args, '--recorder-name', 'native-paper-session'])
        main(['apply', *args, '--recorder-name', 'native-paper-session', '--transition', plan['transition_id']])
        r = self.restore(before)
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        self.assertEqual({g.grant_id for g in r.grants()}, {g.grant_id for g in before['grants']})
