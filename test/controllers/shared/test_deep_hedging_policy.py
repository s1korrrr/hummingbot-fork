import hashlib
import json
import unittest
from dataclasses import replace
from decimal import Decimal

from controllers.shared.deep_hedging.policy import Context, load_policy
from controllers.shared.deep_hedging.shadow import ShadowController, Snapshot, bounded_spot_entry


def artifact():
    return {
        'schema': 'deep_hedging.band_policy.v1', 'mode': 'shadow_only',
        'context': {'program_id': 'option_reference', 'capital_model': 'SIMULATED_OPTION_FINANCED',
                    'instrument': 'SYNTHETIC-CALL', 'book_id': 'reference-1'},
        'features': [{'name': 'x', 'unit': 'ratio', 'mean': 0.0, 'scale': 1.0, 'lower': -5.0, 'upper': 5.0}],
        'position_bounds': [0.0, 1.0],
        'network': {'hidden_weight': [[0.0]], 'hidden_bias': [0.0],
                    'output_weight': [[0.0], [0.0]], 'output_bias': [0.0, 0.0]},
        'provenance': {'code_sha': 'a' * 40, 'data_sha256': 'b' * 64, 'config_sha256': 'c' * 64,
                       'seed': 7, 'framework': 'fixture', 'dtype': 'float64',
                       'evidence_state': 'ENGINEERING_ONLY'}
    }


def load(data=None):
    data = artifact() if data is None else data
    raw = json.dumps(data, sort_keys=True, allow_nan=False).encode()
    return load_policy(raw, hashlib.sha256(raw).hexdigest(), Context(**data['context']))


def snapshot(**changes):
    data = Snapshot(context=Context(**artifact()['context']), owner_id='research-owner',
                    snapshot_id='d' * 64, sequence=1, market_event_at=99.0, market_available_at=99.5,
                    account_event_at=99.0, account_available_at=99.5, reconciliation_ref='e' * 64,
                    position=Decimal('0.10'), unresolved_orders=0, values={'x': 0.0}, units={'x': 'ratio'})
    return replace(data, **changes)


def controller():
    return ShadowController(load(), expected_owner='research-owner', max_age=5.0, decision_ttl=2.0,
                            max_change=Decimal('0.20'), lot_size=Decimal('0.03'))


class PolicyTests(unittest.TestCase):
    def test_zero_network_has_ordered_band(self):
        p = load()
        self.assertEqual(p.band({'x': 0.0}, {'x': 'ratio'}), (0.5, 0.75))
        self.assertEqual(p.target({'x': 0.0}, {'x': 'ratio'}, 0.6), 0.6)
        self.assertEqual(p.target({'x': 0.0}, {'x': 'ratio'}, 0.1), 0.5)

    def test_wrong_digest_and_context_fail(self):
        raw = json.dumps(artifact()).encode()
        with self.assertRaises(ValueError):
            load_policy(raw, 'f' * 64, Context(**artifact()['context']))
        context = replace(Context(**artifact()['context']), capital_model='SPOT_UNLEVERAGED')
        with self.assertRaises(ValueError):
            load_policy(raw, hashlib.sha256(raw).hexdigest(), context)

    def test_bad_artifacts_fail(self):
        for change in ('mode', 'shape', 'bounds', 'scale', 'evidence', 'bool', 'extra', 'weight', 'seed'):
            with self.subTest(change=change):
                data = artifact()
                if change == 'mode': data['mode'] = 'live'
                if change == 'shape': data['network']['output_weight'] = [[0.0]]
                if change == 'bounds': data['position_bounds'] = [1.0, 0.0]
                if change == 'scale': data['features'][0]['scale'] = 0.0
                if change == 'evidence': data['provenance']['evidence_state'] = 'SUPPORTED'
                if change == 'bool': data['features'][0]['mean'] = True
                if change == 'extra': data['authority'] = 'live'
                if change == 'weight': data['network']['hidden_weight'] = [[1e308]]
                if change == 'seed': data['provenance']['seed'] = True
                with self.assertRaises(ValueError): load(data)

    def test_json_duplicate_nonfinite_and_oversize_rejected(self):
        context = Context(**artifact()['context'])
        for raw in (b'{"schema":1,"schema":2}', b'{"x":NaN}', b' ' * 131073):
            with self.subTest(raw=raw[:30]):
                with self.assertRaises(ValueError):
                    load_policy(raw, hashlib.sha256(raw).hexdigest(), context)

    def test_features_are_strict(self):
        p = load()
        for values, units in (({'x': float('nan')}, {'x': 'ratio'}), ({'x': 6.0}, {'x': 'ratio'}),
                              ({'x': True}, {'x': 'ratio'}), ({}, {}), ({'x': 0.0}, {'x': 'dollars'})):
            with self.subTest(values=values, units=units):
                with self.assertRaises(ValueError): p.band(values, units)

    def test_artifact_is_copied_and_immutable(self):
        data = artifact()
        p = load(data)
        data['network']['output_bias'][0] = 50.0
        self.assertEqual(p.band({'x': 0.0}, {'x': 'ratio'}), (0.5, 0.75))
        with self.assertRaises((AttributeError, TypeError)):
            p.features[0].scale = 0.0


class ShadowTests(unittest.TestCase):
    def test_bounded_quantized_advice_not_order_authority(self):
        d = controller().advise(snapshot(), now=100.0)
        self.assertEqual(d.reason, 'SHADOW_READY')
        self.assertEqual(d.delta, Decimal('0.18'))
        self.assertEqual(d.target, Decimal('0.28'))
        self.assertEqual(d.valid_until, 102.0)
        self.assertFalse(d.execution_permitted)
        self.assertEqual(d.executor_actions, ())
        self.assertEqual(d.to_record()['authority'], 'NONE')
        json.dumps(d.to_record(), allow_nan=False)

    def test_no_trade_inside_band(self):
        d = controller().advise(snapshot(position=Decimal('0.6')), now=100.0)
        self.assertEqual(d.delta, Decimal('0'))
        self.assertEqual(d.target, Decimal('0.6'))

    def test_holds_on_invalid_state(self):
        cases = [({'owner_id': 'sui-owner'}, 'HOLD_OWNER'),
                 ({'reconciliation_ref': None}, 'HOLD_RECONCILIATION'),
                 ({'unresolved_orders': 2}, 'HOLD_UNRESOLVED_ORDERS'),
                 ({'position': Decimal('1.1')}, 'HOLD_POSITION_LIMIT'),
                 ({'position': Decimal('NaN')}, 'HOLD_INVALID_STATE'),
                 ({'sequence': True}, 'HOLD_INVALID_STATE'),
                 ({'market_event_at': 90.0}, 'HOLD_STALE'),
                 ({'account_event_at': 90.0}, 'HOLD_STALE'),
                 ({'market_available_at': 101.0}, 'HOLD_TIMING'),
                 ({'account_event_at': 99.8}, 'HOLD_TIMING'),
                 ({'values': {'x': 20.0}}, 'HOLD_FEATURES')]
        for changes, reason in cases:
            with self.subTest(changes=changes):
                d = controller().advise(snapshot(**changes), now=100.0)
                self.assertEqual(d.reason, reason)
                self.assertIsNone(d.target)
                self.assertFalse(d.execution_permitted)

    def test_context_mismatch_and_duplicate(self):
        c = controller()
        wrong = replace(Context(**artifact()['context']), book_id='other')
        self.assertEqual(c.advise(snapshot(context=wrong), now=100.0).reason, 'HOLD_CONTEXT')
        self.assertEqual(c.advise(snapshot(), now=100.0).reason, 'SHADOW_READY')
        self.assertEqual(c.advise(snapshot(), now=100.0).reason, 'HOLD_SEQUENCE')

    def test_reconciliation_after_restart_and_clock_regression(self):
        c = controller()
        c.advise(snapshot(), now=100.0)
        self.assertEqual(c.advise(snapshot(sequence=2), now=99.9).reason, 'HOLD_CLOCK')
        self.assertEqual(controller().advise(snapshot(reconciliation_ref=None), now=100.0).reason,
                         'HOLD_RECONCILIATION')

    def test_expiry_cannot_outlive_oldest_input(self):
        d = controller().advise(snapshot(account_event_at=96.0), now=100.0)
        self.assertEqual(d.valid_until, 101.0)
        d = controller().advise(snapshot(account_event_at=95.0), now=100.0)
        self.assertEqual(d.reason, 'HOLD_STALE')


class SpotSizingTests(unittest.TestCase):
    def arguments(self):
        return dict(baseline_amount=Decimal('2'), multiplier=Decimal('0.8'),
                    free_quote=Decimal('100'), unit_price=Decimal('100'), cost_fraction=Decimal('0.01'),
                    max_increment=Decimal('3'), lot_size=Decimal('0.1'), min_amount=Decimal('0.1'),
                    min_notional=Decimal('5'), eligible=True, fresh=True, reconciled=True,
                    cost_basis_valid=True, unresolved_orders=0)

    def test_cash_fees_and_baseline_are_caps(self):
        args = self.arguments()
        self.assertEqual(bounded_spot_entry(**args), Decimal('0.9'))
        args.update(free_quote=Decimal('1000'), baseline_amount=Decimal('0.51'))
        self.assertEqual(bounded_spot_entry(**args), Decimal('0.4'))

    def test_no_sale_no_size_increase_no_small_order(self):
        for key in ('eligible', 'fresh', 'reconciled', 'cost_basis_valid'):
            args = self.arguments(); args[key] = False
            self.assertEqual(bounded_spot_entry(**args), Decimal('0'))
        args = self.arguments(); args['unresolved_orders'] = 1
        self.assertEqual(bounded_spot_entry(**args), Decimal('0'))
        args = self.arguments(); args['max_increment'] = Decimal('0.01')
        self.assertEqual(bounded_spot_entry(**args), Decimal('0'))
        for value in (Decimal('-1'), Decimal('1.01'), Decimal('NaN')):
            args = self.arguments(); args['multiplier'] = value
            with self.assertRaises(ValueError): bounded_spot_entry(**args)


if __name__ == '__main__':
    unittest.main()
