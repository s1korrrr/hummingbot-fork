"""One Meridian controller, five symbols, native paper execution. Synthetic, not economics."""
import json
import tempfile
from decimal import Decimal as D
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v3.test_native_paper import BOOK, candles, frames, paper
from unittest import IsolatedAsyncioTestCase

import pytest

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.meridian_v3_unified import MeridianUnifiedController
from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig
from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.paper_session import NativePaperSession

RISK = dict(max_drawdown_fraction='.10', max_daily_loss_fraction='.05',
            max_spread_fraction='.003', fee_reserve_fraction='.003', cash_reserve_quote='0')
PRIORITY = ('BTC-USDC', 'ETH-USDC', 'BNB-USDC', 'SOL-USDC', 'XRP-USDC')
OWNER = 'modular_spot_meridian_v3_unified_paper'


def unified(mode='paper', execution_enabled=False, disabled=(), **overrides):
    values = dict(id=f'modular_spot_meridian_v3_unified_{mode}', execution_mode=mode,
                  connector_name='okx_paper_trade' if mode == 'paper' else 'okx', account_id=f'meridian_{mode}',
                  symbols=[{'trading_pair': p, 'entry_enabled': p not in disabled} for p in PAIRS],
                  priority=PRIORITY, risk=RISK)
    if mode != 'paper':
        values['data'] = {'mode': 'shared', 'transport': 'stream', 'origin': 'http://127.0.0.1:8092',
                          'token_file': '/run/candles/token'}
    values.update(overrides)
    if execution_enabled:
        values['recipe_binding'] = MeridianUnifiedConfig(**values).seal()
        values['execution_enabled'] = True
    return MeridianUnifiedConfig(**values)


def books(step, now, pairs=PAIRS):
    return [dict(**{**BOOK, 'trading_pair': pair}, sequence=step + 2, timestamp=now) for pair in pairs]


def all_candles(frame, now, pairs=PAIRS):
    return [{**candles(frame, now), 'trading_pair': pair} for pair in pairs]


# ---------------------------------------------------------------------- contract
def test_one_owner_five_symbol_contexts_share_identity_not_wallets():
    c = unified()
    assert c.get_controller_class() is MeridianUnifiedController
    configs = c.symbol_configs()
    assert tuple(sorted(configs)) == tuple(sorted(PAIRS))
    assert {x.id for x in configs.values()} == {OWNER}
    assert {x.account for x in configs.values()} == {'meridian_paper'}
    assert all(x.capital.slice_quote == 20 and x.capital.position_cap_quote == 100 for x in configs.values())
    m = c.mandate()
    assert (m.order_quote, m.per_asset_quote, m.funding_envelope_quote, m.reference_quote) == (20, 100, 500, 500)
    assert not m.execution_enabled and c.p0_policy().pair_priority == PRIORITY


@pytest.mark.parametrize('change', [
    {'symbols': [{'trading_pair': p} for p in PAIRS[:4]]},
    {'symbols': [{'trading_pair': p} for p in (*PAIRS[:4], 'DOGE-USDC')]},
    {'priority': PRIORITY[:4]},
    {'priority': (*PRIORITY[:4], 'BTC-USDC')},
    {'id': 'modular_spot_meridian_v3_unified_live'},
    {'connector_name': 'okx'},
    {'total_amount_quote': '600'},
    {'capital_domain': {'account_id': 'meridian_paper', 'connector': 'okx_paper_trade', 'exclusive': True,
                        'evidence_sha256': 'a' * 64, 'approved_by': 'operator'}},
    {'symbols': [{'trading_pair': p, 'policy_version': 'custom'} for p in PAIRS]},
    {'unknown_field': 1},
])
def test_invalid_portfolio_configurations_are_rejected(change):
    with pytest.raises(ValueError):
        unified(**change)


def test_observation_cannot_execute_and_live_needs_shared_data():
    with pytest.raises(ValueError, match='sealed recipe binding'):
        MeridianUnifiedConfig(**{**unified().model_dump(mode='json'), 'execution_enabled': True})
    observation = unified('observation')
    with pytest.raises(ValueError, match='observation mode cannot dispatch'):
        MeridianUnifiedConfig(**{**observation.model_dump(mode='json'), 'execution_enabled': True,
                                 'recipe_binding': observation.seal().model_dump(mode='json')})
    with pytest.raises(ValueError, match='shared OKX candles'):
        unified('live', data={'mode': 'native'})
    live = unified('live', execution_enabled=True)
    assert live.capital_domain is None and live.execution_enabled


def test_seal_binds_configuration_and_ignores_control_fields():
    sealed = unified(execution_enabled=True)
    toggled = sealed.model_dump(mode='json')
    toggled.update(execution_enabled=False, manual_kill_switch=True,
                   symbols=[{'trading_pair': p, 'entry_enabled': p != 'SOL-USDC'} for p in PAIRS])
    MeridianUnifiedConfig(**toggled)
    with pytest.raises(ValueError, match='behavioural configuration changed'):
        MeridianUnifiedConfig(**{**sealed.model_dump(mode='json'), 'max_candle_age_seconds': 90})
    with pytest.raises(ValueError, match='recipe binding differs'):
        MeridianUnifiedConfig(**{**sealed.model_dump(mode='json'), 'priority': tuple(reversed(PRIORITY))})


# ---------------------------------------------------------------------- native paper lifecycle
class MeridianUnifiedPaperTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp  # denies all network connections

    async def enter(self, session, pairs=PAIRS):
        before, rebound = frames()
        first = await session.step(600000, books=books(0, 600000), candles=all_candles(before, 600000))
        self.assertEqual(first['actions'], [])
        return await session.step(600060, books=books(1, 600060), candles=all_candles(rebound, 600060))

    def grants(self, controller):
        controller.portfolio.begin_tick()
        return controller.portfolio.store().grants(now_ms=controller.portfolio.now_ms(),
                                                   native_executors=controller.portfolio.evidence())

    async def test_one_controller_admits_five_symbols_from_one_pool(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'), [unified(execution_enabled=True)],
                                         Path(directory) / 'meridian.sqlite')
            try:
                receipt = await self.enter(session)
                controller = session.controllers[OWNER]
                self.assertEqual(len(receipt['actions']), 5, controller.to_format_status())
                self.assertEqual({a.controller_id for a in receipt['actions']}, {OWNER})
                executors = session.executor_orchestrator.active_executors[OWNER]
                self.assertEqual({e.config.trading_pair for e in executors}, set(PAIRS))
                self.assertTrue(all(type(e) is ModularPositionExecutor for e in executors))
                for e in executors:
                    self.assertLessEqual(e.config.amount * e.config.entry_price * D('1.003'), D('20'))
                grants = self.grants(controller)
                self.assertEqual(len(grants), 5)
                self.assertEqual({g.status for g in grants}, {'BOUND'})
                self.assertEqual({g.executor_id for g in grants}, {e.config.id for e in executors})
                self.assertLessEqual(sum(g.reserved_quote for g in grants), D('100'))
                info = controller.get_custom_info()
                self.assertEqual(info['target']['status'], 'PROPOSAL_ONLY')
                self.assertEqual(len(info['admission']['admitted']), 5)
                self.assertEqual(set(info['symbols']), set(PAIRS))
                stored = controller.portfolio.target_store().read()
                self.assertEqual(stored['scope'], 'RECORDER_LOCAL_PROPOSAL_ONLY')
                self.assertFalse(stored['execution_authorized'])
                duplicate = await session.step(600061, books=books(2, 600061))
                self.assertEqual(duplicate['actions'], [])
                self.assertEqual(len(self.grants(controller)), 5)
            finally:
                await session.close()

    async def test_scarce_cash_follows_explicit_priority(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '45'), [unified(execution_enabled=True)],
                                         Path(directory) / 'meridian.sqlite')
            try:
                receipt = await self.enter(session)
                controller = session.controllers[OWNER]
                pairs = {a.executor_config.trading_pair for a in receipt['actions']}
                self.assertEqual(pairs, {'BTC-USDC', 'ETH-USDC'}, controller.get_custom_info())
                self.assertIn('ACQUISITION_FULL_INCREMENT_UNAVAILABLE', controller.portfolio.last_target.reasons)
                self.assertEqual({g.symbol for g in self.grants(controller)}, pairs)
            finally:
                await session.close()

    async def test_disabled_symbol_gets_no_grant_while_others_enter(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'),
                                         [unified(execution_enabled=True, disabled=('BNB-USDC',))],
                                         Path(directory) / 'meridian.sqlite')
            try:
                receipt = await self.enter(session)
                pairs = {a.executor_config.trading_pair for a in receipt['actions']}
                self.assertEqual(pairs, set(PAIRS) - {'BNB-USDC'})
                self.assertNotIn('BNB-USDC', {g.symbol for g in self.grants(session.controllers[OWNER])})
            finally:
                await session.close()

    async def test_first_observed_cut_only_seeds_no_catch_up_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'), [unified(execution_enabled=True)],
                                         Path(directory) / 'meridian.sqlite')
            try:
                _, rebound = frames()
                receipt = await session.step(600060, books=books(0, 600060), candles=all_candles(rebound, 600060))
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(self.grants(session.controllers[OWNER]), ())
            finally:
                await session.close()

    async def test_without_execution_capability_targets_are_observable_but_never_granted(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'), [unified(execution_enabled=False)],
                                         Path(directory) / 'meridian.sqlite')
            try:
                receipt = await self.enter(session)
                controller = session.controllers[OWNER]
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(controller.portfolio.last_target.status, 'PROPOSAL_ONLY')
                self.assertEqual(controller.portfolio.last_admission.reasons, ('EXECUTION_CAPABILITY_UNBOUND',))
                self.assertEqual(self.grants(controller), ())
            finally:
                await session.close()

    async def test_partial_fill_restart_restores_grants_without_reentry(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'meridian.sqlite'
            config = unified(execution_enabled=True)
            session = NativePaperSession(paper(PAIRS, '500'), [config], db)
            try:
                await self.enter(session)
                await session.step(600061, trades=[dict(trading_pair='BTC-USDC', sequence=1, timestamp=600061,
                                                        side=TradeType.SELL, price=D('99.49'), amount=D('.05'))])
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
                bound = {g.grant_id: g for g in self.grants(session.controllers[OWNER])}
                expected = session._connector.get_all_balances()
            finally:
                await session.close()
            restored = NativePaperSession(paper(PAIRS, '500'), [config], db, restore=True)
            try:
                self.assertEqual(restored._connector.get_all_balances(), expected)
                _, rebound = frames()
                receipt = await restored.step(600062, books=books(3, 600062), candles=all_candles(rebound, 600062))
                self.assertEqual(receipt['actions'], [])
                # Unfilled entries ended flat at shutdown and released; the partially filled
                # BTC tranche is held inventory and keeps its bound grant across the restart.
                after = self.grants(restored.controllers[OWNER])
                btc = [g for g in bound.values() if g.symbol == 'BTC-USDC']
                self.assertEqual(after, tuple(btc))
                self.assertEqual(after[0].status, 'BOUND')
                held = [i for i in restored.recorder.get_all_executors() if i.config.trading_pair == 'BTC-USDC']
                self.assertEqual(held[0].custom_info['execution_receipt']['accounting_owner'], 'held_position')
                self.assertEqual(restored._connector.get_balance('BTC'), D('.05'))
            finally:
                await restored.close()

    async def test_operator_entry_pause_holds_portfolio_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'), [unified(execution_enabled=True)],
                                         Path(directory) / 'meridian.sqlite')
            try:
                session.controllers[OWNER].set_operator_entry_pause(True, reason='test')
                receipt = await self.enter(session)
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(session.controllers[OWNER].portfolio.last_target.status, 'ENTRY_HOLD')
                self.assertEqual(json.loads(json.dumps(session.controllers[OWNER].get_custom_info(), default=str))
                                 ['operator']['entry_paused'], True)
            finally:
                await session.close()


class MeridianUnifiedManagementTests(IsolatedAsyncioTestCase):
    """Management survives entry holds, disabled symbols and portfolio failure (R06/R08)."""
    setUp = session_tests.NativePaperSessionTests.setUp

    async def filled(self, session, pair):
        await MeridianUnifiedPaperTests.enter(self, session)
        executor = next(e for e in session.executor_orchestrator.active_executors[OWNER]
                        if e.config.trading_pair == pair)
        amount = executor._open_order.order.amount
        await session.step(600061, trades=[dict(trading_pair=pair, sequence=1, timestamp=600061,
                                                side=TradeType.SELL, price=D('99.49'), amount=D('1'))])
        self.assertEqual(executor.open_filled_amount, amount)
        return executor, amount

    async def test_disabling_a_symbol_keeps_its_filled_executor_managed(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'), [unified(execution_enabled=True)],
                                         Path(directory) / 'meridian.sqlite')
            try:
                executor, amount = await self.filled(session, 'BNB-USDC')
                controller = session.controllers[OWNER]
                changed = controller.config.model_dump(mode='json')
                changed['symbols'] = [{'trading_pair': p, 'entry_enabled': p != 'BNB-USDC'} for p in PAIRS]
                controller.update_config(MeridianUnifiedConfig(**changed))
                self.assertFalse(controller.symbol_config('BNB-USDC').acquisition_enabled)
                receipt = await session.step(600062, books=books(3, 600062))
                self.assertEqual(receipt['actions'], [])
                self.assertTrue(executor.is_active)
                self.assertEqual(session._connector.get_balance('BNB'), amount)
                self.assertIn('BNB-USDC', {g.symbol for g in MeridianUnifiedPaperTests.grants(self, controller)})
            finally:
                await session.close()

    async def test_drawdown_bag_stop_is_symbol_scoped_and_survives_portfolio_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(PAIRS, '500'), [unified(execution_enabled=True)],
                                         Path(directory) / 'meridian.sqlite')
            try:
                executor, amount = await self.filled(session, 'BTC-USDC')
                controller = session.controllers[OWNER]

                def broken(markets):
                    raise RuntimeError('injected portfolio failure')
                controller.portfolio.evaluate = broken
                crash = [{**BOOK, 'trading_pair': 'BTC-USDC', 'sequence': 4, 'timestamp': 600062,
                          'bids': [(96, 10)], 'asks': [(96.01, 10)]}]
                crash += [b for b in books(2, 600062) if b['trading_pair'] != 'BTC-USDC']
                receipt = await session.step(600062, books=crash)
                stops = [a for a in receipt['actions'] if getattr(a, 'keep_position', False)]
                self.assertEqual([a.executor_id for a in stops], [executor.config.id])
                self.assertIn('injected portfolio failure', controller.get_custom_info()['fault'])
                self.assertEqual(session._connector.get_balance('BTC'), amount)
                held = [p for p in controller.positions_held if p.trading_pair == 'BTC-USDC']
                self.assertEqual(sum(p.amount for p in held), amount)
                self.assertFalse([p for p in controller.positions_held if p.trading_pair != 'BTC-USDC'])
                others = [e for e in session.executor_orchestrator.active_executors[OWNER]
                          if e.config.trading_pair != 'BTC-USDC']
                self.assertTrue(others and all(e.is_active for e in others))
            finally:
                await session.close()


def test_seal_tool_writes_a_new_sealed_paper_file_and_refuses_live_or_overwrite(tmp_path):
    import yaml

    from scripts.meridian_v3_seal import seal
    source = tmp_path / 'meridian.yml'
    source.write_text(yaml.safe_dump(unified().model_dump(mode='json'), sort_keys=False))
    binding = seal(source, tmp_path / 'sealed.yml')
    sealed = MeridianUnifiedConfig.model_validate(yaml.safe_load((tmp_path / 'sealed.yml').read_text()))
    assert sealed.execution_enabled and sealed.recipe_binding.model_dump(mode='json') == binding
    with pytest.raises(FileExistsError):
        seal(source, tmp_path / 'sealed.yml')
    with pytest.raises(SystemExit, match='unsealed'):
        seal(tmp_path / 'sealed.yml', tmp_path / 'again.yml')
    live = tmp_path / 'live.yml'
    live.write_text(yaml.safe_dump(unified('live').model_dump(mode='json'), sort_keys=False))
    with pytest.raises(SystemExit, match='only paper'):
        seal(live, tmp_path / 'live-sealed.yml')


def test_native_controller_loader_resolves_the_unified_yaml():
    from hummingbot.strategy_v2.controllers.controller_loader import load_controller_config
    loaded = load_controller_config(unified().model_dump(mode='json'))
    assert type(loaded) is MeridianUnifiedConfig and loaded.get_controller_class() is MeridianUnifiedController
    assert loaded.owned_trading_pairs() == tuple(sorted(PAIRS))
