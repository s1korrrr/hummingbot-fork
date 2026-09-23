import importlib
import math
import unittest
from hashlib import sha256
from pathlib import Path

from nre_fixtures import ranged_snapshot, tape

from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.components.source_identity import checked_source


def wilder_atr(rows, length):
    trs = []
    for prev, row in zip(rows, rows[1:]):
        high, low, close = float(row.high), float(row.low), float(prev.close)
        trs.append(max(high - low, abs(high - close), abs(low - close)))
    atr = sum(trs[:length]) / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr


class NativeProviderTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[3]
        if not (root / 'controllers/generic/nre_v2_core/binding.py').exists():
            self.fail('native ATR provider is absent')
        self.binding = importlib.import_module('controllers.generic.nre_v2_core.binding')
        self.admission = importlib.import_module('controllers.generic.nre_v2_core.admission')

    def test_atr_parent_equals_seed_snapshot_ref(self):
        snapshot = ranged_snapshot()
        atr, parent, impl, init = self.binding.scale_from_snapshot(snapshot, length=14)
        self.assertEqual(parent, self.binding.observation_ref(snapshot))
        self.assertGreater(atr, 0)
        self.assertTrue(math.isfinite(atr))
        self.assertAlmostEqual(atr, wilder_atr(snapshot.rows, 14))
        source = checked_source(Path(self.binding.__file__))
        self.assertEqual(impl, sha256(source).hexdigest())
        self.assertEqual(init, digest({
            'length': 14,
            'dataset_key': snapshot.dataset.key,
            'parent_content_sha256': parent.content_sha256,
        }))

    def test_atr_uses_confirmed_rows_and_rejects_unconfirmed_last(self):
        snapshot = ranged_snapshot()
        atr, _, _, _ = self.binding.scale_from_snapshot(snapshot, length=2)
        self.assertAlmostEqual(atr, wilder_atr(tuple(row for row in snapshot.rows if row.confirmed), 2))
        last = snapshot.rows[-1]
        unconfirmed = last.model_construct(**{**last.model_dump(), 'confirmed': False})
        bad = snapshot.model_construct(
            schema_version=snapshot.schema_version, dataset=snapshot.dataset, epoch=snapshot.epoch,
            sequence=snapshot.sequence, published_at=snapshot.published_at,
            rows=(*snapshot.rows[:-1], unconfirmed))
        with self.assertRaises(ValueError):
            self.binding.scale_from_snapshot(bad, length=2)

    def test_zero_range_tape_and_short_history_reject(self):
        flat = tape(prices=(100,))[0]
        with self.assertRaises(ValueError):
            self.binding.scale_from_snapshot(flat, length=14)
        short = ranged_snapshot(bars=3)
        with self.assertRaises(ValueError):
            self.binding.scale_from_snapshot(short, length=14)
        with self.assertRaises(ValueError):
            self.binding.scale_from_snapshot(ranged_snapshot(), length=1)
        with self.assertRaises(ValueError):
            self.binding.scale_from_snapshot(ranged_snapshot(), length=61)
        with self.assertRaises(ValueError):
            self.binding.scale_from_snapshot(ranged_snapshot(), length=True)

    def test_classify_role_table(self):
        cases = (
            ('acquire', 0, 'INITIAL_ENTRY'),
            ('acquire', 1, 'SCALE_IN'),
            ('acquire', 4, 'SCALE_IN'),
            ('reduce', 0, 'REDUCE'),
            ('reduce', 3, 'REDUCE'),
            ('hold', 0, 'UNKNOWN'),
            ('acquire', -1, 'UNKNOWN'),
        )
        for action, stage, role in cases:
            with self.subTest(action=action, stage=stage):
                self.assertEqual(self.admission.classify_role(action=action, stage=stage), role)

    def test_acquisition_dispatch_allowed_table(self):
        passing = Prerequisite(name='nre_initial_entry', status='PASS', evidence_sha256=digest('gate'))
        rows = (
            (self.admission.GateResult(False, 'management_not_filtered', None), True),
            (self.admission.GateResult(False, 'noninitial_acquisition_not_filtered', None), True),
            (self.admission.GateResult(True, 'net_edge_sufficient', passing), True),
            (self.admission.GateResult(True, 'net_edge_below_threshold',
                                       Prerequisite(name='nre_initial_entry', status='HOLD')), False),
            (self.admission.GateResult(True, 'path_model_or_policy_unavailable',
                                       Prerequisite(name='nre_initial_entry', status='UNAVAILABLE')), False),
            (self.admission.GateResult(True, 'prediction_expired',
                                       Prerequisite(name='nre_initial_entry', status='STALE')), False),
            (self.admission.GateResult(True, 'unbound_action_role', None), False),
        )
        for result, allowed in rows:
            with self.subTest(reason=result.reason, applicable=result.applicable):
                self.assertEqual(self.admission.acquisition_dispatch_allowed(result), allowed)
                self.assertFalse(result.execution_authorized)


if __name__ == '__main__':
    unittest.main()
