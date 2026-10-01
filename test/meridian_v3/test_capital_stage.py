"""Explicit Meridian capital stage: canary (100 envelope) or full (500), same order and asset sizes."""
import json
from decimal import Decimal as D
from test.meridian_v3.lifecycle_harness import creates, rebound
from test.meridian_v3.test_identity_migration import BTC, ETH, SOL, UPGRADED, OwnerStateCase
from test.meridian_v3.test_source_seal import mutate
from test.meridian_v3.test_unified_runtime import LIVE_CAPITAL, unified

import pytest

from controllers.generic.meridian_v3_portfolio import MeridianPortfolioRecipe
from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig
from controllers.generic.meridian_v3_unified_core.migration import (
    IDENTITY_MIGRATION_REQUIRED,
    MigrationRefused,
    apply_identity_migration,
    grant_policy,
    owner_identity,
    plan_identity_migration,
)

BNB, XRP = 'BNB-USDC', 'XRP-USDC'


# ---------------------------------------------------------------------- validation and identity
def values(**changes):
    result = unified().model_dump(mode='json')
    result.pop('total_amount_quote')
    return {**result, **changes}


def test_the_stage_is_explicit_and_admits_only_its_versioned_numbers():
    with pytest.raises(ValueError, match='capital_stage'):
        MeridianUnifiedConfig(**{k: v for k, v in values().items() if k != 'capital_stage'})
    with pytest.raises(ValueError):
        MeridianUnifiedConfig(**values(capital_stage='half'))
    canary, full = MeridianUnifiedConfig(**values(capital_stage='canary')), MeridianUnifiedConfig(**values())
    assert canary.total_amount_quote == 100 and full.total_amount_quote == 500
    for config, envelope in ((canary, 100), (full, 500)):
        m = config.mandate()
        assert (m.order_quote, m.per_asset_quote, m.funding_envelope_quote, m.reference_quote) == (20, 100, envelope, envelope)
        assert {c.budget.max_quote for c in config.symbol_configs().values()} == {envelope}
        assert {c.total_amount_quote for c in config.symbol_configs().values()} == {100}
    for stage, amount in (('canary', '500'), ('full', '100'), ('canary', '250')):
        with pytest.raises(ValueError, match='envelope'):
            MeridianUnifiedConfig(**values(capital_stage=stage, total_amount_quote=amount))
    with pytest.raises(ValueError, match='stage envelope'):
        unified('live', capital_stage='canary', live_capital=LIVE_CAPITAL)  # a 500 binding on a canary owner
    live = unified('live', capital_stage='canary', live_capital={**LIVE_CAPITAL, 'envelope_quote': '100'})
    assert live.live_capital.envelope_quote == 100
    recipe = MeridianPortfolioRecipe(canary.mandate(), canary.p0_policy())
    with pytest.raises(ValueError, match='mandate'):
        MeridianPortfolioRecipe(recipe.mandate.model_copy(update={'funding_envelope_quote': D(250),
                                                                  'reference_quote': D(250)}), recipe.policy)


def test_canary_and_full_are_different_identities_everywhere():
    canary, full = unified(capital_stage='canary', execution_enabled=True), unified(execution_enabled=True)
    assert canary.mandate().identity != full.mandate().identity
    assert canary.recipe_digest() != full.recipe_digest()
    assert canary.seal().configuration_sha256 != full.seal().configuration_sha256
    assert canary.seal().source_sha256 == full.seal().source_sha256
    recipes = [MeridianPortfolioRecipe(c.mandate(), c.p0_policy()) for c in (canary, full)]
    assert recipes[0].recipe_sha256 != recipes[1].recipe_sha256
    assert grant_policy(canary, recipes[0]).identity != grant_policy(full, recipes[1]).identity
    a, b = owner_identity(canary), owner_identity(full)
    assert all(a['symbols'][p]['fingerprint'] != b['symbols'][p]['fingerprint'] for p in PAIRS)
    with pytest.raises(ValueError, match='recipe binding differs'):
        MeridianUnifiedConfig(**{**canary.model_dump(mode='json'), 'capital_stage': 'full', 'total_amount_quote': '500'})


# ---------------------------------------------------------------------- runtime and migration
class CapitalStageTests(OwnerStateCase):
    async def test_canary_envelope_is_shared_by_priority_at_full_order_size(self):
        s = self.scenario(capital_stage='canary')
        await s.step()
        receipt = await s.step(rsi={p: rebound() for p in PAIRS})
        entries = creates(receipt)
        self.assertEqual(len(entries), 5)  # five 20-quote increments fill the 100 envelope exactly
        for action in entries:
            config = action.executor_config
            self.assertLessEqual(config.amount * config.entry_price * D('1.003'), D('20'))
        for pair in PAIRS:
            await s.fill(pair)
        receipt = await s.step(mids={ETH: '99'}, rsi={ETH: rebound()})  # an eligible DCA stage
        self.assertEqual(creates(receipt), [])
        self.assertIn('ACQUISITION_FULL_INCREMENT_UNAVAILABLE', s.controller.portfolio.last_target.reasons)
        self.assertEqual(s.controller.get_custom_info()['limits'],
                         {'order_quote_including_fees': '20', 'asset_quote': '100', 'envelope_quote': '100'})

    async def test_canary_to_full_carries_held_and_active_state_through_the_migration(self):
        before = await self.owner_with_active_and_held_state(capital_stage='canary')
        r = self.restore(before, capital_stage='canary')  # the unchanged canary owner is bound
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        before = await self.closed(r, **{k: before[k] for k in ('grants', 'held', 'eth', 'btc')})
        r = self.restore(before, capital_stage='full')
        receipt = await r.step(rsi={SOL: rebound()})
        self.assertEqual(creates(receipt), [])
        hold = r.controller.portfolio.identity_hold
        self.assertTrue(hold.startswith(f'{IDENTITY_MIGRATION_REQUIRED}:migration_available:capital_stage_canary_to_full'),
                        hold)
        self.assertEqual(self.migration_rows(r), self.migration_rows(before))
        plan = plan_identity_migration(r.config, *self.sessions(r))
        self.assertEqual(plan['kinds'], ['capital_stage_canary_to_full'])
        self.assertEqual(plan['carried']['reserved_quote'], str(sum(g.reserved_quote for g in before['grants'])))
        claims = {p: d for p, d in plan['carried']['decisions'].items() if d['claims']}
        self.assertEqual(set(claims), {BTC, ETH})
        apply_identity_migration(r.config, *self.sessions(r), plan['transition_id'])
        grant_key = r.controller.portfolio.store().key
        self.assertEqual(json.loads(self.rows(r)[grant_key])['policy_hash'], r.controller.portfolio.store().policy_hash)
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        self.assertEqual(r.grants(), before['grants'])  # the same bound grants, now under the 500 policy
        self.assertEqual(r.held(BTC), before['held'])
        for pair, state in self.decisions(r).items():
            if pair in claims:
                self.assertEqual(len(state.claims), claims[pair]['claims'])
                self.assertEqual(state.fingerprint, r.controller.contexts[pair].runtime.fingerprint)
        receipt = await r.step(rsi={p: rebound() for p in (SOL, BNB, XRP)})
        self.assertEqual({a.executor_config.trading_pair for a in creates(receipt)}, {SOL, BNB, XRP})

    async def full_owner_with_exposure(self, pairs):
        s = self.scenario()
        await s.step()
        receipt = await s.step(rsi={p: rebound() for p in pairs})
        self.assertEqual(len(creates(receipt)), len(pairs))
        for pair in pairs:
            await s.fill(pair)
        return s

    async def test_full_to_canary_is_refused_while_reserved_exposure_exceeds_the_canary_envelope(self):
        s = await self.full_owner_with_exposure(PAIRS)
        receipt = await s.step(mids={ETH: '99'}, rsi={ETH: rebound()})  # a sixth increment: 120 reserved
        self.assertEqual(len(creates(receipt, ETH)), 1)
        await s.fill(ETH)
        before = await self.closed(s)
        r = self.restore(before, capital_stage='canary')
        await r.step()
        hold = r.controller.portfolio.identity_hold
        self.assertIn('migration_refused:capital stage full -> canary refused', hold)
        with self.assertRaisesRegex(MigrationRefused, r'reserved exposure 1\d\d(\.\d+)? quote .* canary envelope 100'):
            plan_identity_migration(r.config, *self.sessions(r))
        self.assertEqual(self.migration_rows(r), self.migration_rows(before))

    async def test_full_to_canary_with_small_exposure_and_new_code_migrates_in_one_reviewed_step(self):
        s = await self.full_owner_with_exposure((BTC, ETH))
        before = await self.closed(s)
        mutate(self.patch, UPGRADED)
        r = self.restore(before, capital_stage='canary')
        await r.step()
        plan = plan_identity_migration(r.config, *self.sessions(r))
        self.assertEqual(plan['kinds'], ['code_upgrade', 'capital_stage_full_to_canary'])
        apply_identity_migration(r.config, *self.sessions(r), plan['transition_id'])
        await r.step()
        self.assertIsNone(r.controller.portfolio.identity_hold)
        self.assertEqual({g.symbol for g in r.grants()}, {BTC, ETH})
