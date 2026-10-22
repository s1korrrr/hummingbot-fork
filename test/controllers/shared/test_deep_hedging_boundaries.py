import json
import unittest
from decimal import Decimal, localcontext
from unittest.mock import patch

from controllers.shared.deep_hedging.shadow import bounded_spot_entry
import test_deep_hedging_policy as fixtures
from test_deep_hedging_policy import artifact, controller, load, snapshot


class BoundaryTests(unittest.TestCase):
    def test_hidden_affine_overflow_is_not_hidden_by_tanh(self):
        data = artifact()
        data['features'][0].update(lower=-1e308, upper=1e308)
        data['network']['hidden_weight'] = [[1e6]]
        with self.assertRaises(ValueError):
            load(data).band({'x': 1e308}, {'x': 'ratio'})

    def test_invalid_snapshot_identity_is_serializable(self):
        decision = controller().advise(snapshot(snapshot_id=float('nan')), now=100.0)
        self.assertEqual(decision.reason, 'HOLD_INVALID_STATE')
        json.dumps(decision.to_record(), allow_nan=False)

    def test_spot_amount_is_independent_of_ambient_decimal_precision(self):
        args = fixtures.SpotSizingTests().arguments()
        with localcontext() as context:
            context.prec = 1
            amount = bounded_spot_entry(**args)
        self.assertEqual(amount, Decimal('0.9'))

    def test_shadow_delta_is_independent_of_ambient_decimal_precision(self):
        with localcontext() as context:
            context.prec = 1
            decision = controller().advise(snapshot(), now=100.0)
        self.assertEqual(decision.target, Decimal('0.28'))
        self.assertEqual(decision.delta, Decimal('0.18'))

    def test_inference_and_shadow_have_no_io(self):
        with patch('builtins.open', side_effect=AssertionError('unexpected file I/O')), \
             patch('socket.socket', side_effect=AssertionError('unexpected socket')):
            decision = controller().advise(snapshot(), now=100.0)
        self.assertEqual(decision.reason, 'SHADOW_READY')
        self.assertEqual(decision.executor_actions, ())
