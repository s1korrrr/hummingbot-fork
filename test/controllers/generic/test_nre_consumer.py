import asyncio
import importlib
import threading
import unittest
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch

from nre_fixtures import ranged_snapshot

from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.components.source_identity import component_root, component_source_files
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction


def nre_fields():
    return dict(
        nre_model_sha256=digest('nre-model'),
        nre_scaler_sha256=digest('nre-scaler'),
        nre_horizon_ms=600000,
        nre_minimum_net_bps=D('1'),
        nre_atr_length=14,
        nre_required_features=('extension_atr', 'rebound_atr', 'retention'),
        nre_path=dict(lookback_bars=2, rebound_atr=1.0, invalidation_atr=0.5, max_bars=20,
                      max_transport_age_ms=15000),
    )


class IsolatedConsumerTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[3]
        self.root = root
        if not (root / 'controllers/generic/nre_v2.py').exists():
            self.fail('isolated nre_v2 consumer is absent')
        self.nre = importlib.import_module('controllers.generic.nre_v2')
        self.admission = importlib.import_module('controllers.generic.nre_v2_core.admission')

    def config(self, **overrides):
        values = dict(id='modular_spot_nre', connector_name='okx', trading_pair='ETH-USDC', **nre_fields())
        values.update(overrides)
        return self.nre.NREModularControllerConfig(**values)

    def runtime(self, cfg, *, time_s=3600.007):
        controller = self.nre.NREModularController(cfg, NS(ready=True, time=lambda: time_s), asyncio.Queue())
        controller._status = RunnableStatus.RUNNING
        return controller

    def arm(self, controller, *, proposal, revised, state=None, snapshot=None):
        from controllers.generic.modular_spot_core.execution import native_config
        from controllers.generic.modular_spot_core.market import MarketFeatures
        rt = controller.runtime
        store = MagicMock()
        store.load.return_value = (state, 'raw')
        store.compare_and_swap.return_value = True
        store.recorder.get_all_executors.return_value = []
        budget = MagicMock()
        budget.reserve.return_value = True
        native = native_config(controller.config, identifier=proposal.identifier, timestamp=proposal.timestamp,
                               price=proposal.price, amount=proposal.amount, cycle=proposal.cycle, stage=proposal.stage)
        rt.observe = lambda: MarketFeatures(1140, D('100'), D('29'), D('31'), False, False)
        rt.acquisition_permitted = lambda: True
        rt.ensure_store = lambda: store
        rt.budget = budget
        rt.selection_store = NS(freeze=lambda **kwargs: None)
        rt.snapshot = lambda: dict(bid=D('100'), ask=D('100.1'), available_quote=D('100'), pnl=D('0'),
                                   amount_step=D('0.001'), minimum_amount=D('0.001'), minimum_notional=D('1'))
        rt.bindings.config = lambda cfg, item: native
        called = {}
        rt.bindings.action = lambda cid, item: called.setdefault(
            'action', CreateExecutorAction(controller_id=cid, executor_config=item))
        if snapshot is not None:
            rt.nre_snapshot = snapshot
        return rt, store, budget, called, native

    def test_family_and_host_source_files_exclude_nre(self):
        modular_spot = importlib.import_module('controllers.generic.modular_spot')
        rsi_modular = importlib.import_module('controllers.generic.rsi_modular')
        native_composition = importlib.import_module('controllers.generic.modular_spot_core.native_composition')
        collections = (
            modular_spot.source_files(),
            rsi_modular.source_files('ok_rsi'),
            native_composition.source_files('ok_rsi'),
        )
        for files in collections:
            joined = '\n'.join(Path(path).as_posix() for path in files)
            with self.subTest(sample=joined[:80]):
                self.assertNotIn('nre_v2.py', joined)
                self.assertNotIn('nre_v2_core', joined)
        canonical = '\n'.join(Path(path).as_posix() for path in component_source_files(component_root()))
        self.assertNotIn('nre_v2_core', canonical)
        self.assertNotIn('/nre_v2.py', canonical)

    def test_nre_source_files_include_host_and_core(self):
        files = [Path(path).as_posix() for path in self.nre.source_files()]
        joined = '\n'.join(files)
        self.assertTrue(any(path.endswith('/nre_v2.py') for path in files))
        self.assertIn('nre_v2_core/binding.py', joined)
        self.assertIn('nre_v2_core/admission.py', joined)
        self.assertTrue(any(path.endswith('/modular_spot.py') for path in files))
        self.assertTrue(any('modular_spot_core/' in path for path in files))
        manifest = importlib.import_module('controllers.generic.nre_v2_core.source_manifest')
        for name in manifest.MODULES:
            self.assertTrue(any(path.endswith('/nre_v2_core/' + name) for path in files), name)

    def test_sell_only_never_calls_evaluate(self):
        controller = self.runtime(self.config(execution_enabled=True, acquisition_enabled=False,
                                              universe={'mode': 'sell_only'}))
        with patch('controllers.generic.nre_v2_core.admission.evaluate') as evaluate:
            actions = controller.determine_executor_actions()
        evaluate.assert_not_called()
        self.assertFalse(any(isinstance(item, CreateExecutorAction) for item in actions))

    def test_scale_in_bypasses_gate(self):
        from controllers.generic.modular_spot_core.planner import EntryProposal
        from controllers.generic.modular_spot_core.state import DecisionState
        cfg = self.config(execution_enabled=True, budget={'group': 'nre', 'max_quote': '100', 'max_assets': 1})
        controller = self.runtime(cfg)
        proposal = EntryProposal('scale-1', D('100'), D('1'), 3600.007, 0, 1)
        revised = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1140,
                                claims=('owned', 'scale-1'), committed_quote=D('100.5'))
        state = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1080,
                              claims=('owned',), committed_quote=D('50'))
        rt, store, budget, called, native = self.arm(controller, proposal=proposal, revised=revised, state=state)
        rt.dispatched = {'owned'}
        with patch('controllers.generic.modular_spot_core.runtime.plan',
                   return_value=(revised, proposal, 'entry_proposed')), \
             patch('controllers.generic.nre_v2_core.admission.evaluate') as evaluate:
            actions = controller.determine_executor_actions()
        evaluate.assert_not_called()
        budget.reserve.assert_called_once()
        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], CreateExecutorAction)

    def test_plan_hook_does_not_gate_other_controller_thread(self):
        from controllers.generic.modular_spot_core.planner import EntryProposal
        from controllers.generic.modular_spot_core.state import DecisionState
        cfg = self.config(execution_enabled=True, budget={'group': 'nre', 'max_quote': '100', 'max_assets': 1})
        controller = self.runtime(cfg)
        proposal = EntryProposal('other-claim', D('100'), D('1'), 3600.007, 0, 0)
        revised = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1140,
                                claims=('other-claim',), committed_quote=D('100.5'))
        state = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1080)
        self.arm(controller, proposal=proposal, revised=revised, state=state)
        foreign = []

        def original_plan(*args, **kwargs):
            if threading.current_thread() is threading.main_thread():
                worker = threading.Thread(target=lambda: foreign.append(
                    self.nre.modular_runtime.plan(state, None)))
                worker.start()
                worker.join(timeout=2)
                self.assertFalse(worker.is_alive())
            return revised, proposal, 'entry_proposed'

        with patch('controllers.generic.modular_spot_core.runtime.plan', side_effect=original_plan), \
             patch('controllers.generic.nre_v2_core.admission.evaluate', return_value=self.admission.GateResult(
                 True, 'unavailable', Prerequisite(name='nre_initial_entry', status='UNAVAILABLE'))):
            controller.determine_executor_actions()
        self.assertEqual(foreign, [(revised, proposal, 'entry_proposed')])

    def test_initial_entry_hold_does_not_consume_existing_claim(self):
        from controllers.generic.modular_spot_core.planner import EntryProposal
        from controllers.generic.modular_spot_core.state import DecisionState
        snapshot = ranged_snapshot()
        cfg = self.config(execution_enabled=True, budget={'group': 'nre', 'max_quote': '100', 'max_assets': 1})
        controller = self.runtime(cfg, time_s=snapshot.published_at)
        existing = 'keep-exec'
        proposal = EntryProposal('new-claim', D('100'), D('1'), snapshot.published_at, 0, 0)
        state = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1080,
                              claims=(existing,), committed_quote=D('50'))
        revised = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1140,
                                claims=(existing, 'new-claim'), committed_quote=D('100.5'))
        rt, store, budget, called, native = self.arm(
            controller, proposal=proposal, revised=revised, state=state, snapshot=snapshot)
        rt.dispatched = {existing}
        rt.pending = {existing: NS(id=existing)}
        hold = self.admission.GateResult(
            True, 'net_edge_below_threshold', Prerequisite(name='nre_initial_entry', status='HOLD'))
        with patch('controllers.generic.modular_spot_core.runtime.plan',
                   return_value=(revised, proposal, 'entry_proposed')), \
             patch('controllers.generic.nre_v2_core.admission.evaluate', return_value=hold) as evaluate:
            actions = controller.determine_executor_actions()
        evaluate.assert_called()
        self.assertEqual(actions, [])
        budget.reserve.assert_not_called()
        self.assertEqual(rt.dispatched, {existing})
        self.assertNotIn('new-claim', rt.pending)
        self.assertIn(existing, rt.pending)
        self.assertTrue(store.compare_and_swap.called)
        persisted = store.compare_and_swap.call_args[0][1]
        self.assertEqual(persisted.claims, (existing,))
        self.assertNotIn('new-claim', persisted.claims)

    def test_unknown_role_holds(self):
        from controllers.generic.modular_spot_core.planner import EntryProposal
        from controllers.generic.modular_spot_core.state import DecisionState
        snapshot = ranged_snapshot()
        cfg = self.config(execution_enabled=True, budget={'group': 'nre', 'max_quote': '100', 'max_assets': 1})
        controller = self.runtime(cfg, time_s=snapshot.published_at)
        proposal = EntryProposal('unknown-1', D('100'), D('1'), snapshot.published_at, 0, 0)
        revised = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1140,
                                claims=('unknown-1',), committed_quote=D('100.5'))
        state = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1080)
        self.arm(controller, proposal=proposal, revised=revised, state=state, snapshot=snapshot)
        result = self.admission.GateResult(
            True, 'initial_entry_role_unavailable', Prerequisite(name='nre_initial_entry', status='HOLD'))
        with patch('controllers.generic.modular_spot_core.runtime.plan',
                   return_value=(revised, proposal, 'entry_proposed')), \
             patch('controllers.generic.nre_v2_core.admission.classify_role', return_value='UNKNOWN'), \
             patch('controllers.generic.nre_v2_core.admission.evaluate', return_value=result) as evaluate:
            actions = controller.determine_executor_actions()
        evaluate.assert_called()
        self.assertEqual(actions, [])
        self.assertEqual(controller.runtime.pending, {})

    def test_execution_authorized_stays_false(self):
        cfg = self.config()
        self.assertFalse(cfg.execution_enabled)
        bundle = self.nre.render_nre_recipe({
            'id': 'modular_spot_nre', 'connector_name': 'okx', 'trading_pair': 'ETH-USDC',
            **nre_fields(),
        })
        self.assertFalse(bundle['execution_authorized'])
        self.assertFalse(bundle['controllers'][0]['execution_enabled'])
        self.assertFalse(self.admission.GateResult(True, 'net_edge_sufficient', None).execution_authorized)

    def test_loader_discovers_isolated_host(self):
        from hummingbot.strategy_v2.controllers.controller_loader import load_controller_config
        cfg = load_controller_config({
            'controller_name': 'nre_v2', 'controller_type': 'generic',
            'id': 'modular_spot_nre', 'connector_name': 'okx', 'trading_pair': 'ETH-USDC',
            **nre_fields(),
        })
        self.assertIs(type(cfg), self.nre.NREModularControllerConfig)
        self.assertFalse(cfg.execution_enabled)
        self.assertEqual(cfg.get_controller_class(), self.nre.NREModularController)

    def test_initial_entry_binds_atr_to_seed_snapshot(self):
        from controllers.generic.modular_spot_core.planner import EntryProposal
        from controllers.generic.modular_spot_core.state import DecisionState
        snapshot = ranged_snapshot()
        cfg = self.config(execution_enabled=True, budget={'group': 'nre', 'max_quote': '100', 'max_assets': 1})
        controller = self.runtime(cfg, time_s=snapshot.published_at)
        proposal = EntryProposal('new-claim', D('100'), D('1'), snapshot.published_at, 0, 0)
        revised = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1140,
                                claims=('new-claim',), committed_quote=D('100.5'))
        state = DecisionState(fingerprint=controller.runtime.fingerprint, candle=1080)
        self.arm(controller, proposal=proposal, revised=revised, state=state, snapshot=snapshot)
        hold = self.admission.GateResult(
            True, 'path_model_or_policy_unavailable', Prerequisite(name='nre_initial_entry', status='UNAVAILABLE'))
        with patch('controllers.generic.modular_spot_core.runtime.plan',
                   return_value=(revised, proposal, 'entry_proposed')), \
             patch('controllers.generic.nre_v2_core.admission.evaluate', return_value=hold), \
             patch('controllers.generic.nre_v2.scale_from_snapshot', wraps=self.nre.scale_from_snapshot) as scale:
            controller.determine_executor_actions()
        scale.assert_called()
        args, kwargs = scale.call_args
        self.assertIs(args[0], snapshot)
        self.assertEqual(kwargs['length'], 14)


if __name__ == '__main__':
    unittest.main()
