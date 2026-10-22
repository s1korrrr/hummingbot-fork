"""Gate contracts consume synthetic predictions; this file fits no model."""
import dataclasses
import importlib
import unittest
from decimal import Decimal, localcontext
from pathlib import Path

from nre_fixtures import contract, tape
from hummingbot.strategy_v2.components.decision_protocol import Opportunity, Prerequisite, decide
from hummingbot.strategy_v2.components.recipe_contract import digest


class InitialEntryGateTests(unittest.TestCase):
    def setUp(self):
        if not (Path(__file__).resolve().parents[3] / 'controllers/generic/nre_v2_core/admission.py').exists():
            self.fail('isolated initial-entry prerequisite adapter is absent')
        self.m = importlib.import_module('controllers.generic.nre_v2_core.admission')
        self.binding = importlib.import_module('controllers.generic.nre_v2_core.binding')
        snaps = tape()
        self.context = contract(self.binding, snaps)
        self.state = None
        for s in snaps[:3]:
            self.state = self.binding.observe(self.context, self.state, s, now_ms=self.binding.seconds_to_ms(s.published_at))
        self.now = self.state.evaluated_at_ms
        self.op = Opportunity(scope=self.context.scope, setup=self.context.setup, action='acquire',
                              primary=self.binding.observation_ref(self.state.snapshot), created_at_ms=self.now, lifetime_ms=10000)
        self.policy = self.m.GatePolicy(recipe=self.context.recipe, model_sha256=digest('model'),
                                       scaler_sha256=digest('scaler'), horizon_ms=600000,
                                       required_features=('extension_atr', 'rebound_atr', 'retention'), minimum_net_bps=Decimal('1'))
        self.role = self.m.ActionRole(self.op.opportunity_id, 'INITIAL_ENTRY', digest('native-role-evidence'), self.now)
        self.prediction = self.m.Prediction(self.op.opportunity_id, self.state.state_sha256,
                                           digest('model'), digest('scaler'), self.now - 120000,
                                           self.now - 1000, self.now, self.now + 5000, 600000,
                                           Decimal('25'), Decimal('10'), Decimal('5'))

    def gate(self, **changes):
        args = dict(opportunity=self.op, role=self.role, state=self.state, prediction=self.prediction,
                    policy=self.policy, now_ms=self.now)
        args.update(changes)
        return self.m.evaluate(**args)

    def test_positive_net_edge_only_produces_native_prerequisite(self):
        r = self.gate()
        self.assertTrue(r.applicable)
        self.assertEqual(r.prerequisite.status, 'PASS')
        self.assertEqual(Decimal(r.net_edge_bps), Decimal('10'))
        self.assertFalse(r.execution_authorized)
        _, trace = decide(None, self.op, now_ms=self.now, prerequisites=(r.prerequisite,))
        self.assertEqual(trace.reason, 'seeded')
        self.assertFalse(trace.execution_authorized)

    def test_low_net_edge_holds_without_altering_the_proposal(self):
        before = self.op.model_dump(mode='json')
        r = self.gate(prediction=dataclasses.replace(self.prediction, gross_return_bps=Decimal('12')))
        self.assertEqual(r.prerequisite.status, 'HOLD')
        self.assertEqual(self.op.model_dump(mode='json'), before)

    def test_reduce_does_not_consult_broken_nre_state(self):
        op = self.op.model_copy(update={'action': 'reduce'})
        r = self.gate(opportunity=op, role=None, state=None, prediction=None, policy=None)
        self.assertFalse(r.applicable)
        self.assertIsNone(r.prerequisite)

    def test_scale_in_and_pyramiding_are_not_filtered(self):
        for role in ('SCALE_IN', 'PYRAMID'):
            r = self.gate(role=dataclasses.replace(self.role, role=role), state=None, prediction=None, policy=None)
            self.assertFalse(r.applicable)
            self.assertIsNone(r.prerequisite)

    def test_unknown_or_unbound_role_cannot_become_initial_entry(self):
        for role in (None, dataclasses.replace(self.role, role='UNKNOWN'),
                     dataclasses.replace(self.role, opportunity_id='f' * 64),
                     dataclasses.replace(self.role, available_at_ms=self.now + 1)):
            with self.subTest(role=role):
                self.assertNotEqual(self.gate(role=role).prerequisite.status, 'PASS')

    def test_stale_forecast_and_future_model_fail_closed(self):
        self.assertEqual(self.gate(now_ms=self.now + 5000).prerequisite.status, 'STALE')
        for changes in ({'model_available_at_ms': self.now + 1, 'generated_at_ms': self.now + 1},
                        {'training_cutoff_ms': self.now - 2000},
                        {'path_state_sha256': 'f' * 64}, {'model_sha256': 'f' * 64}, {'horizon_ms': 1}):
            with self.subTest(changes=changes):
                p = dataclasses.replace(self.prediction, **changes)
                self.assertNotEqual(self.gate(prediction=p).prerequisite.status, 'PASS')

    def test_path_scope_revision_and_proposal_expiry_cannot_be_relabelled(self):
        self.assertEqual(self.gate(now_ms=self.now + 10000).prerequisite.status, 'STALE')
        op = self.op.model_copy(update={'setup': 'other-setup'})
        role = dataclasses.replace(self.role, opportunity_id=op.opportunity_id)
        prediction = dataclasses.replace(self.prediction, opportunity_id=op.opportunity_id)
        self.assertNotEqual(self.gate(opportunity=op, role=role, prediction=prediction).prerequisite.status, 'PASS')

    def test_unavailable_path_and_missing_retention_never_become_zero(self):
        self.assertEqual(self.gate(state=None).prerequisite.status, 'UNAVAILABLE')
        snaps = tape(prices=(100, 99, 99.5))
        context = contract(self.binding, snaps)
        state = None
        for s in snaps:
            state = self.binding.observe(context, state, s, now_ms=self.binding.seconds_to_ms(s.published_at))
        op = self.op.model_copy(update={'primary': self.binding.observation_ref(state.snapshot)})
        p = dataclasses.replace(self.prediction, path_state_sha256=state.state_sha256, opportunity_id=op.opportunity_id)
        role = dataclasses.replace(self.role, opportunity_id=op.opportunity_id)
        self.assertEqual(self.gate(opportunity=op, state=state, prediction=p, role=role).prerequisite.status, 'UNAVAILABLE')

    def test_numeric_bounds_and_decimal_context(self):
        for value in (Decimal('NaN'), Decimal('Infinity'), Decimal('1e-100000'), True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                dataclasses.replace(self.prediction, cost_bps=value)
        p = dataclasses.replace(self.prediction, gross_return_bps=Decimal('16.00001'))
        with localcontext() as ctx:
            ctx.prec = 3
            self.assertEqual(self.gate(prediction=p).prerequisite.status, 'PASS')
            self.assertEqual(Decimal(self.gate(prediction=p).net_edge_bps), Decimal('1.00001'))

    def test_native_risk_veto_and_event_consumption_are_preserved(self):
        earlier = self.op.model_copy(update={'primary': self.op.primary.model_copy(update={'event_time_ms': self.op.primary.event_time_ms - 60000})})
        cursor, _ = decide(None, earlier, now_ms=self.now, prerequisites=())
        r = self.gate()
        cursor, result = decide(cursor, self.op, now_ms=self.now,
                                prerequisites=(r.prerequisite, Prerequisite(name='risk', status='HOLD')))
        self.assertEqual(result.status, 'HOLD')
        _, duplicate = decide(cursor, self.op, now_ms=self.now, prerequisites=(r.prerequisite,))
        self.assertEqual(duplicate.reason, 'already_observed')
        self.assertFalse(duplicate.execution_authorized)

    def test_basis_point_bound_is_independent_of_decimal_context(self):
        with localcontext() as ctx:
            ctx.prec = 3
            with self.assertRaises(ValueError):
                dataclasses.replace(self.prediction, gross_return_bps=Decimal('1000000.0001'))


if __name__ == '__main__':
    unittest.main()
