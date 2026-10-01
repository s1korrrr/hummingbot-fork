"""B1: `capital_stage` changes to and from `full_2000` go through the operator identity migration.

The stage table is the single source of truth: a stage change is carried only when the
witnessed identity is exactly the running configuration projected onto the source stage,
so every stage-derived value (envelope, per-asset, per-order, tranche count, bag slice,
depth budget, capital policy, grant policy) equals the table and nothing else moved. The
reserved-exposure gate and the carried per-symbol state must fit the target stage.
"""
import json
from decimal import Decimal as D
from test.meridian_v3.lifecycle_harness import creates, rebound
from test.meridian_v3.test_identity_migration import BTC, ETH, OwnerStateCase
from test.meridian_v3.test_unified_runtime import LIVE_CAPITAL, unified
from types import MappingProxyType

import pytest

from controllers.generic import meridian_v3_portfolio
from controllers.generic.meridian_v3_portfolio import CAPITAL_STAGES, CapitalStage
from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.meridian_v3_unified_core import config as config_module, migration
from controllers.generic.meridian_v3_unified_core.migration import (
    IDENTITY_MIGRATION_REQUIRED,
    MigrationRefused,
    apply_identity_migration,
    owner_identity,
    plan_identity_migration,
)
from controllers.generic.modular_spot_core.state import decode
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.components.recipe_contract import canonical_json
from hummingbot.strategy_v2.portfolio_v3.runtime_contracts_v1 import PortfolioGrant

CASH = '3000'
BNB, SOL, XRP = 'BNB-USDC', 'SOL-USDC', 'XRP-USDC'
SIZED = ('mandate.funding_envelope_quote', 'mandate.reference_quote', 'mandate.per_asset_quote',
         'mandate.order_quote', 'configuration.capital_stage', 'configuration.total_amount_quote',
         'grant_policy.envelope_quote', 'grant_policy.per_asset_quote', 'grant_policy.order_quote',
         'grant_policy.mandate_sha256', f'symbols.{BTC}.total_amount_quote', f'symbols.{BTC}.budget.max_quote',
         f'symbols.{BTC}.bag.slice_quote', f'symbols.{BTC}.depth_budget.budget_quote',
         f'symbols.{BTC}.capital.slice_quote', f'symbols.{BTC}.capital.position_cap_quote',
         f'symbols.{BTC}.capital.funded_cap_quote', f'symbols.{BTC}.capital.max_active_tranches')


# ---------------------------------------------------------------------- planner over a witness only
def rows(witness, grants=None):
    return {'witness': canonical_json(witness), 'target': None, 'decisions': {pair: None for pair in PAIRS},
            'grants': None if grants is None else json.dumps(grants)}


def plan(witness_config, current_config, grants=None):
    witness = owner_identity(witness_config)
    return migration._plan(current_config, owner_identity(current_config), rows(witness, grants))


def bound_grants(witness_config, symbol, quotes):
    witness = owner_identity(witness_config)
    grants = [PortfolioGrant(grant_id=f'{i:064x}', symbol=symbol, setup_id=f'bnb_p23_mr:{i}', target_sha256='a' * 64,
                             epoch='paper.x', fence=1, ceiling_quote=D(q), admitted_at_ms=1, submit_deadline_ms=2,
                             status='BOUND', executor_id=f'executor-{i}', bound_quote=D(q)).model_dump(mode='json')
              for i, q in enumerate(quotes, 1)]
    return {'schema_version': 1, 'policy_hash': witness['grant_policy_sha256'], 'grants': grants,
            'released': [], 'consumed': []}


@pytest.mark.parametrize('frm,to', [('full', 'full_2000'), ('full_2000', 'full'),
                                    ('canary', 'full_2000'), ('full_2000', 'canary')])
def test_every_stage_pair_is_a_planned_capital_stage_change(frm, to):
    result = plan(unified(capital_stage=frm), unified(capital_stage=to))
    assert result['kinds'] == [f'capital_stage_{frm}_to_{to}']
    assert set(SIZED) - {'mandate.per_asset_quote', 'grant_policy.per_asset_quote'} <= set(result['stage_fields'])
    assert not [f for f in result['stage_fields'] if '.signal.' in f or '.flash_crash.' in f or '.buy_gap.' in f]


def test_stage_fields_are_derived_from_the_table_not_a_second_copy():
    result = plan(unified(capital_stage='full'), unified(capital_stage='full_2000'))
    assert set(SIZED) <= set(result['stage_fields'])
    assert {f'symbols.{p}.capital.max_active_tranches' for p in PAIRS} <= set(result['stage_fields'])
    canary = plan(unified(capital_stage='canary'), unified(capital_stage='full'))['stage_fields']
    assert 'mandate.order_quote' not in canary and 'mandate.funding_envelope_quote' in canary


def test_a_hand_edited_stage_derived_value_is_refused(monkeypatch):
    edited = MappingProxyType({**CAPITAL_STAGES, 'full_2000': CapitalStage(D('2000'), D('2000'), D('99'), 20)})
    with monkeypatch.context() as patch:
        patch.setattr(meridian_v3_portfolio, 'CAPITAL_STAGES', edited)
        patch.setattr(config_module, 'CAPITAL_STAGES', edited)
        witness = owner_identity(unified(capital_stage='full_2000'))  # an owner sized 99 per order
    assert witness['mandate']['order_quote'] == '99'
    current = unified(capital_stage='full')
    with pytest.raises(MigrationRefused, match=r'stage-derived .*full_2000 stage table.*mandate\.order_quote'):
        migration._plan(current, owner_identity(current), rows(witness))
    upgrade = unified(capital_stage='full_2000')  # the same edit is refused when it is the source either way
    with pytest.raises(MigrationRefused, match='mandate changed beyond the capital stage'):
        migration._plan(upgrade, owner_identity(upgrade), rows(witness))


def test_a_non_stage_behavioural_change_with_the_stage_is_refused():
    with pytest.raises(MigrationRefused, match=r'behavioural configuration changed \(.*max_candle_age_seconds'):
        plan(unified(capital_stage='full', max_candle_age_seconds=90), unified(capital_stage='full_2000'))
    reordered = ('ETH-USDC', 'BTC-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC')
    with pytest.raises(MigrationRefused, match=r'behavioural configuration changed \(.*priority'):
        plan(unified(capital_stage='full_2000', priority=reordered), unified(capital_stage='full'))


def test_live_capital_binding_change_beyond_its_envelope_is_refused():
    def live(stage, **binding):
        envelope = str(CAPITAL_STAGES[stage].envelope_quote)
        return unified('live', capital_stage=stage, live_capital={**LIVE_CAPITAL, 'envelope_quote': envelope, **binding})
    assert plan(live('full'), live('full_2000'))['kinds'] == ['capital_stage_full_to_full_2000']
    with pytest.raises(MigrationRefused, match='live capital binding changed beyond its stage envelope'):
        plan(live('full'), live('full_2000', approved_by='someone_else'))
    with pytest.raises(MigrationRefused, match='live capital binding changed beyond its stage envelope'):
        plan(unified('live', capital_stage='full'), live('full_2000'))


def test_a_downgrade_refuses_more_open_tranches_than_the_target_stage_admits():
    wide, full = unified(capital_stage='full_2000'), unified(capital_stage='full')
    nine = bound_grants(wide, BTC, ['10'] * 9)  # 90 reserved: inside the full per-asset 100
    with pytest.raises(MigrationRefused, match=rf'{BTC} holds 9 open grants .* full stage admits 8'):
        plan(wide, full, nine)
    result = plan(wide, full, bound_grants(wide, BTC, ['10'] * 8))
    assert result['kinds'] == ['capital_stage_full_2000_to_full'] and result['carried']['grants'] == 8


def test_the_exposure_gate_is_unchanged_for_full_2000():
    wide, full = unified(capital_stage='full_2000'), unified(capital_stage='full')
    with pytest.raises(MigrationRefused, match=r'reserved exposure 200 quote \(largest asset 200\) exceeds the full '
                                               r'envelope 500 / per-asset 100; let exposure fall below the target'):
        plan(wide, full, bound_grants(wide, BTC, ['100', '100']))
    grants = bound_grants(wide, BTC, ['100'])
    for i, pair in enumerate((ETH, BNB, SOL, XRP, ETH), 2):
        extra = bound_grants(wide, pair, ['100'])['grants'][0]
        grants['grants'].append({**extra, 'grant_id': f'{i:064x}', 'executor_id': f'executor-{i}'})
    with pytest.raises(MigrationRefused, match=r'reserved exposure 600 quote .* full envelope 500'):
        plan(wide, full, grants)


# ---------------------------------------------------------------------- the native paper owner
class Full2000MigrationTests(OwnerStateCase):
    async def owner(self, stage, entries):
        """`entries` is one pair per filled buy; a repeated pair is a deeper DCA tranche."""
        s = self.scenario(capital_stage=stage, cash=CASH)
        await s.step()
        for pair in entries:
            mid = s.mids[pair] * D('.97') if any(e.config.trading_pair == pair for e in s.executors()) else s.mids[pair]
            receipt = await s.step(mids={pair: mid}, rsi={pair: rebound()})
            self.assertEqual(len(creates(receipt, pair)), 1, s.controller.to_format_status())
            await s.fill(pair)
        grants = s.grants()
        self.assertEqual(len(grants), len(entries))
        return await self.closed(s, grants=grants, held=s.held(BTC))

    def assert_hold(self, r, outcome):
        hold = r.controller.portfolio.identity_hold
        self.assertTrue(hold.startswith(f'{IDENTITY_MIGRATION_REQUIRED}:{outcome}'), hold)

    async def migrate(self, before, stage):
        r = self.restore(before, capital_stage=stage)
        await r.step()
        frm = json.loads(next(v for k, v in before['rows'].items()
                              if k.startswith(migration.WITNESS_PREFIX)))['configuration']['capital_stage']
        self.assert_hold(r, f'migration_available:capital_stage_{frm}_to_{stage}')
        self.assertEqual(self.migration_rows(r), self.migration_rows(before))
        result = plan_identity_migration(r.config, *self.sessions(r))
        self.assertEqual(result['kinds'], [f'capital_stage_{frm}_to_{stage}'])
        apply_identity_migration(r.config, *self.sessions(r), result['transition_id'])
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        return r, result

    async def test_full_to_full_2000_carries_bags_tranches_and_grants(self):
        before = await self.owner_with_active_and_held_state()
        r, result = await self.migrate(before, 'full_2000')
        self.assertIn('mandate.order_quote', result['stage_fields'])
        self.assertEqual(r.grants(), before['grants'])
        self.assertEqual(r.held(BTC), before['held'])
        # The ETH tranche's grant stays bound to the same native executor across the stage change.
        self.assertEqual([g.executor_id for g in r.grants() if g.symbol == ETH], [before['eth']])
        receipt = await r.step(rsi={SOL: rebound()})  # entries resume at the full_2000 order size (50)
        [entry] = creates(receipt, SOL)
        quote = entry.executor_config.amount * entry.executor_config.entry_price
        self.assertGreater(quote, D('45'))
        self.assertLessEqual(quote, D('50'))

    async def test_full_2000_to_full_migrates_when_exposure_fits_full(self):
        before = await self.owner('full_2000', (BTC, ETH))
        self.assertTrue(all(g.reserved_quote <= 100 for g in before['grants']))
        r, _ = await self.migrate(before, 'full')
        self.assertEqual(r.grants(), before['grants'])
        self.assertEqual(r.controller.get_custom_info()['limits'],
                         {'order_quote_including_fees': '20', 'asset_quote': '100', 'envelope_quote': '500'})

    async def test_full_2000_to_full_is_refused_above_the_full_per_asset_limit(self):
        before = await self.owner('full_2000', (BTC, BTC, BTC))  # three 50 tranches on one asset: 150 > 100
        r = self.restore(before, capital_stage='full')
        await r.step()
        held = r.held(BTC)
        self.assertGreater(held, 0)
        self.assert_hold(r, 'migration_refused:capital stage full_2000 -> full refused')
        with self.assertRaisesRegex(MigrationRefused, r'per-asset 100; let exposure fall below the target stage'):
            plan_identity_migration(r.config, *self.sessions(r))
        self.assertEqual(self.migration_rows(r), self.migration_rows(before))
        await r.step()
        self.assertEqual(r.held(BTC), held)  # nothing is sold or closed to make it fit
        self.assertEqual(self.migration_rows(r), self.migration_rows(before))  # grants and claims untouched

    async def test_full_2000_to_full_is_refused_above_the_full_envelope(self):
        before = await self.owner('full_2000', (BTC, ETH, BNB, SOL, XRP, BTC, ETH, BNB, SOL, XRP, ETH))  # 11 x 50 = 550 > 500
        r = self.restore(before, capital_stage='full')
        await r.step()
        with self.assertRaisesRegex(MigrationRefused, r'reserved exposure 5\d\d(\.\d+)? quote .* full envelope 500'):
            plan_identity_migration(r.config, *self.sessions(r))

    async def test_canary_and_full_2000_migrate_both_ways(self):
        before = await self.owner('canary', (BTC, ETH))
        r, _ = await self.migrate(before, 'full_2000')
        self.assertEqual(r.grants(), before['grants'])
        before = await self.closed(r, grants=r.grants())
        r, _ = await self.migrate(before, 'canary')
        self.assertEqual(r.grants(), before['grants'])

    async def test_round_trip_full_full_2000_full_through_reviewed_transitions(self):
        before = await self.owner_with_active_and_held_state()
        r, _ = await self.migrate(before, 'full_2000')
        before = await self.closed(r, grants=r.grants(), held=r.held(BTC))
        r = self.restore(before, capital_stage='full')
        await r.step()
        reviewed = plan_identity_migration(r.config, *self.sessions(r))
        self.assertEqual(reviewed['kinds'], ['capital_stage_full_2000_to_full'])
        self.assertIn('mandate.order_quote', reviewed['stage_fields'])
        key = self.decision_keys(r)[BTC]
        with r.session.recorder.sql_manager.get_new_session() as session, session.begin():
            row = session.get(Metadata, key)
            state = decode(row.value)
            row.value = json.dumps({**json.loads(row.value), 'last_entry_time': str(state.last_entry_time + D('1'))},
                                   sort_keys=True, separators=(',', ':'))
        with self.assertRaisesRegex(MigrationRefused, 'changed since the reviewed plan'):
            apply_identity_migration(r.config, *self.sessions(r), reviewed['transition_id'])
        replanned = plan_identity_migration(r.config, *self.sessions(r))
        self.assertNotEqual(replanned['transition_id'], reviewed['transition_id'])
        result = apply_identity_migration(r.config, *self.sessions(r), replanned['transition_id'])
        self.assertIn(result['receipt_key'], self.rows(r))
        with self.assertRaisesRegex(MigrationRefused, 'unchanged; nothing to migrate'):
            apply_identity_migration(r.config, *self.sessions(r), replanned['transition_id'])
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        self.assertEqual(r.grants(), before['grants'])
        self.assertEqual(r.held(BTC), before['held'])
