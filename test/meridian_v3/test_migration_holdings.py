"""The identity migration plan shows what the owner really holds, not only what it reserves.

A `holdings` block reports, per symbol, the coins this recorder's native fills leave
held (amount and fee-inclusive cost, through the same native projection the runtime
snapshot uses), beside the persisted grant reservations. For a capital stage change it
compares the settled usage with the target stage's limits. It is informational: it adds
no refusal, sells nothing, and is not part of the transition id.
"""
import json
from decimal import Decimal as D
from test.meridian_v3 import test_capital_stage_full_2000_migration as full2000
from test.meridian_v3.lifecycle_harness import creates, rebound
from test.meridian_v3.test_identity_migration import BTC, ETH, OwnerStateCase
from test.meridian_v3.test_unified_runtime import unified

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.meridian_v3_unified_core import migration
from controllers.generic.meridian_v3_unified_core.migration import (
    apply_identity_migration,
    owner_identity,
    plan_identity_migration,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.model.trade_fill import TradeFill

SOL = 'SOL-USDC'
CONNECTOR = 'okx_paper_trade'
ZERO = '0'


def fill(pair, order, side, amount, price, fee='0', *, market=CONNECTOR):
    amount, price, fee = map(D, (amount, price, fee))
    base, quote = pair.split('-')
    return TradeFill(config_file_path='test', strategy='v2_with_controllers', market=market, symbol=pair,
                     base_asset=base, quote_asset=quote, timestamp=int(order) * 1000, order_id=f'{pair}:{order}',
                     trade_type=side, order_type='LIMIT', price=price, amount=amount, leverage=1, position='NIL',
                     trade_fee={'percent': '0', 'percent_token': None, 'flat_fees': [{'token': quote, 'amount': str(fee)}]},
                     trade_fee_in_quote=fee, exact_amount=str(amount), exact_price=str(price),
                     exact_trade_fee_in_quote=str(fee), exact_receipt_source='order_filled_event_v1',
                     exchange_trade_id=str(order))


def plan(frm, to, *, grants=None, fills=()):
    witness, current = unified(capital_stage=frm), unified(capital_stage=to)
    return migration._plan(current, owner_identity(current), full2000.rows(owner_identity(witness), grants),
                           fills=list(fills))


def released_bag(amount='15', price='100', fee='.15'):
    """A BTC bag whose tranche grants are no longer persisted (no open grant)."""
    return [fill(BTC, 1, 'BUY', amount, price, fee)]


def test_no_bags_reports_zero_holdings_and_no_notes():
    holdings = plan('full_2000', 'full', fills=())['holdings']
    assert set(holdings['symbols']) == set(PAIRS)
    for entry in holdings['symbols'].values():
        assert entry == {'held_amount_base': ZERO, 'held_cost_quote': ZERO, 'net_cash_consumed_quote': ZERO,
                         'admission_usage_quote': ZERO, 'reserved_grant_quote': ZERO, 'open_grants': 0,
                         'total_quote': ZERO, 'over_target': False}
    assert holdings['totals'] == {'held_cost_quote': ZERO, 'admission_usage_quote': ZERO,
                                  'reserved_grant_quote': ZERO, 'total_quote': ZERO, 'over_target': False}
    assert holdings['target'] == {'stage': 'full', 'envelope_quote': '500', 'per_asset_quote': '100'}
    assert holdings['notes'] == []


def test_a_bag_without_open_grants_is_shown_at_cost_and_flagged_above_the_downgrade_target():
    result = plan('full_2000', 'full', fills=released_bag())
    assert result['kinds'] == ['capital_stage_full_2000_to_full']  # informational: no new refusal
    assert result['carried']['reserved_quote'] == '0'
    btc = result['holdings']['symbols'][BTC]
    assert D(btc['held_amount_base']) == 15 and D(btc['held_cost_quote']) == D('1500.15')
    assert D(btc['admission_usage_quote']) == D('1500.15') and D(btc['total_quote']) == D('1500.15')
    assert btc['reserved_grant_quote'] == ZERO and btc['open_grants'] == 0 and btc['over_target'] is True
    totals = result['holdings']['totals']
    assert D(totals['held_cost_quote']) == D('1500.15') and totals['over_target'] is True
    notes = ' | '.join(result['holdings']['notes'])
    assert f'{BTC} holds 15 base at 1500.15 quote cost, above the full per-asset 100' in notes
    assert 'above the full envelope 500' in notes
    assert 'no new buy on any symbol' in notes and 'nothing is sold by this migration' in notes
    assert not any(result['holdings']['symbols'][p]['over_target'] for p in PAIRS if p != BTC)


def test_a_bag_with_an_open_grant_reports_both_and_their_overlap():
    grants = full2000.bound_grants(unified(capital_stage='full_2000'), BTC, ['40'])
    result = plan('full_2000', 'full', grants=grants, fills=[fill(BTC, 1, 'BUY', '.5', '100', '.05')])
    btc = result['holdings']['symbols'][BTC]
    assert (D(btc['held_cost_quote']), D(btc['reserved_grant_quote']), btc['open_grants']) == (D('50.05'), 40, 1)
    assert D(btc['total_quote']) == D('90.05') and D(btc['admission_usage_quote']) == D('50.05')
    assert btc['over_target'] is False and result['holdings']['totals']['over_target'] is False
    assert any('total_quote counts it twice' in note for note in result['holdings']['notes'])
    over = plan('full_2000', 'full', grants=grants, fills=[fill(BTC, 1, 'BUY', '1.5', '100', '.15')])
    assert over['holdings']['symbols'][BTC]['over_target'] is True


def test_profitable_sells_reduce_held_cost_and_settled_usage_takes_the_larger_measure():
    fills = [fill(ETH, 1, 'BUY', '1', '100'), fill(ETH, 2, 'BUY', '1', '50'), fill(ETH, 3, 'SELL', '1', '120')]
    eth = plan('full_2000', 'full', fills=fills)['holdings']['symbols'][ETH]
    assert D(eth['held_amount_base']) == 1 and D(eth['held_cost_quote']) == 75  # blended breakeven
    assert D(eth['net_cash_consumed_quote']) == 30 and D(eth['admission_usage_quote']) == 75


def test_an_upgrade_is_not_flagged():
    result = plan('full', 'full_2000', fills=released_bag())
    assert result['holdings']['target'] == {'stage': 'full_2000', 'envelope_quote': '2000', 'per_asset_quote': '2000'}
    assert result['holdings']['symbols'][BTC]['over_target'] is False
    assert result['holdings']['totals']['over_target'] is False
    assert not [n for n in result['holdings']['notes'] if 'above the' in n]


def test_holdings_never_enter_the_transition_id():
    assert (plan('full_2000', 'full', fills=())['transition_id']
            == plan('full_2000', 'full', fills=released_bag())['transition_id'])


def test_unreadable_fills_are_reported_not_hidden():
    result = plan('full_2000', 'full', fills=[fill(BTC, 1, 'SELL', '1', '100')])  # sells unowned inventory
    btc = result['holdings']['symbols'][BTC]
    assert btc['unavailable'].startswith('ValueError:') and 'over_target' not in btc
    assert result['holdings']['totals']['over_target'] is None
    assert any(n.startswith(f'{BTC} holdings unavailable') for n in result['holdings']['notes'])


def test_the_operator_script_prints_the_holdings_summary():
    from scripts.meridian_v3_identity_migration import holdings_summary
    text = holdings_summary(plan('full_2000', 'full', fills=released_bag())['holdings'])
    assert 'vs full per-asset 100 / envelope 500' in text
    assert f'{BTC}: held 15 base at cost 1500.15 | reserved 0 (0 grants) | usage 1500.15  OVER TARGET' in text
    assert 'total: held cost 1500.15 | reserved 0 | usage 1500.15  OVER TARGET' in text
    assert 'note: BTC-USDC holds 15 base at 1500.15 quote cost' in text


# ---------------------------------------------------------------------- the native paper owner
class MigrationHoldingsTests(OwnerStateCase):
    owner = full2000.Full2000MigrationTests.owner
    assert_hold = full2000.Full2000MigrationTests.assert_hold

    async def test_plan_and_apply_report_the_same_native_holdings_and_transition(self):
        before = await self.owner('full_2000', (BTC,))
        r = self.restore(before, capital_stage='full')
        await r.step()
        held = r.held(BTC)
        self.assertGreater(held, 0)
        reviewed = plan_identity_migration(r.config, *self.sessions(r))
        btc = reviewed['holdings']['symbols'][BTC]
        self.assertEqual(D(btc['held_amount_base']), held)
        self.assertGreater(D(btc['held_cost_quote']), D('45'))
        self.assertEqual(D(btc['reserved_grant_quote']), sum(g.reserved_quote for g in before['grants']))
        self.assertFalse(btc['over_target'])
        self.assertEqual(plan_identity_migration(r.config, *self.sessions(r)), reviewed)  # deterministic
        applied = apply_identity_migration(r.config, *self.sessions(r), reviewed['transition_id'])
        self.assertEqual({k: v for k, v in applied.items() if k != 'receipt_key'}, reviewed)
        receipt = json.loads(self.rows(r)[applied['receipt_key']])
        self.assertEqual(receipt['plan']['holdings'], reviewed['holdings'])

    def release_grants(self, r, pair):
        """What `release_held` leaves: the symbol's grants dropped, their executors released."""
        key = r.controller.portfolio.store().key
        with r.session.recorder.sql_manager.get_new_session() as session, session.begin():
            row = session.get(Metadata, key)
            value = json.loads(row.value)
            ids = [g['executor_id'] for g in value['grants'] if g['symbol'] == pair]
            value['grants'] = [g for g in value['grants'] if g['symbol'] != pair]
            value['released'] = sorted({*value['released'], *ids})
            row.value = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
        return ids

    async def test_a_downgrade_over_the_target_per_asset_holds_buys_and_sells_nothing(self):
        before = await self.owner('full_2000', (BTC, BTC, BTC))  # three 50 tranches on one asset: 150 > 100
        r = self.restore(before, capital_stage='full')
        self.assertEqual(len(self.release_grants(r, BTC)), 3)
        await r.step(mids={BTC: '96.9'})
        self.assert_hold(r, 'migration_available:capital_stage_full_2000_to_full')
        held = r.held(BTC)
        reviewed = plan_identity_migration(r.config, *self.sessions(r))
        btc = reviewed['holdings']['symbols'][BTC]
        self.assertEqual(reviewed['carried']['reserved_quote'], '0')
        self.assertTrue(btc['over_target'])
        self.assertGreater(D(btc['held_cost_quote']), D('100'))
        apply_identity_migration(r.config, *self.sessions(r), reviewed['transition_id'])
        for _ in range(3):
            receipt = await r.step(mids={BTC: '90', SOL: '100'}, rsi={BTC: rebound(), SOL: rebound()})
            buys = [a for a in creates(receipt) if a.executor_config.side == TradeType.BUY]
            self.assertEqual(buys, [], r.controller.to_format_status())
        self.assertIsNone(r.controller.portfolio.identity_hold)
        self.assertIn('CURRENT_BOOK_EXCEEDS_MANDATE', r.controller.portfolio.last_target.reasons)
        self.assertEqual(r.held(BTC), held)  # nothing sold or closed by the migration or the hold
        self.assertTrue(all(c.runtime.fault is None for c in r.controller.contexts.values()))

    async def test_control_the_same_bag_inside_the_stage_limits_does_not_hold_other_buys(self):
        before = await self.owner('full_2000', (BTC, BTC))
        r = self.restore(before, capital_stage='full_2000')  # same identity: no migration, limits 2000
        self.release_grants(r, BTC)
        await r.step(mids={BTC: '96.9'})
        self.assertIsNone(r.controller.portfolio.identity_hold)
        receipt = await r.step(mids={SOL: '100'}, rsi={SOL: rebound()})
        self.assertEqual(len([a for a in creates(receipt, SOL) if a.executor_config.side == TradeType.BUY]), 1,
                         r.controller.to_format_status())
