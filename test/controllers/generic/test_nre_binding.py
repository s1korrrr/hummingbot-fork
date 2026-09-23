import dataclasses
import importlib
import unittest
from decimal import Decimal
from pathlib import Path

from nre_fixtures import contract, tape


class NativeBindingTests(unittest.TestCase):
    def setUp(self):
        if not (Path(__file__).resolve().parents[3] / 'controllers/generic/nre_v2_core/binding.py').exists():
            self.fail('bound native snapshot adapter is absent')
        self.m = importlib.import_module('controllers.generic.nre_v2_core.binding')
        self.snapshots = tape()
        self.contract = contract(self.m, self.snapshots)

    def observe(self, state, snapshot):
        return self.m.observe(self.contract, state, snapshot, now_ms=self.m.seconds_to_ms(snapshot.published_at))

    def test_native_snapshot_hash_and_missingness_are_bound(self):
        s = self.observe(None, self.snapshots[0])
        out = self.m.evidence(s, now_ms=s.evaluated_at_ms)
        self.assertEqual(out['primary']['content_sha256'], self.snapshots[0].digest)
        self.assertEqual(out['features']['available_at_ms'], 3660007)
        self.assertIn('extension_warmup', out['missingness'])
        self.assertIn('retention_unconfirmed', out['missingness'])
        self.assertFalse(out['execution_authorized'])
        self.assertFalse(out['economic_backtest'])

    def test_bound_chain_and_frozen_input_identity(self):
        a = self.observe(None, self.snapshots[0])
        b = self.observe(a, self.snapshots[1])
        self.assertEqual(b.previous_state_sha256, a.state_sha256)
        self.assertNotEqual(a.state_sha256, b.state_sha256)
        for changed in (dataclasses.replace(self.contract, setup='dip-2'),
                        dataclasses.replace(self.contract, initialization_sha256='1' * 64),
                        dataclasses.replace(self.contract, frozen_scale=3.0)):
            with self.assertRaises(ValueError):
                self.m.observe(changed, a, self.snapshots[1], now_ms=3720007)
        other = self.m.observe(dataclasses.replace(self.contract, setup='dip-2'), None, self.snapshots[0], now_ms=3660007)
        self.assertNotEqual(other.state_sha256, a.state_sha256)

    def test_duplicate_and_heartbeat_do_not_refresh_confirmation_or_expiry(self):
        s = self.observe(None, self.snapshots[0])
        self.assertIs(self.observe(s, self.snapshots[0]), s)
        heartbeat = self.snapshots[0].model_copy(update={'published_at': 3661.007})
        again = self.m.observe(self.contract, s, heartbeat, now_ms=3661007)
        self.assertIs(again, s)
        self.assertEqual(self.m.evidence(again, now_ms=3661007)['valid_until_ms'], 3720000)
        with self.assertRaises(ValueError):
            self.m.evidence(again, now_ms=3720000)

    def test_reconnect_sequence_gap_revision_and_order_reject(self):
        s = self.observe(None, self.snapshots[0])
        invalid = (self.snapshots[1].model_copy(update={'epoch': 'fixture-b'}),
                   self.snapshots[1].model_copy(update={'sequence': 3}), self.snapshots[2])
        for value in invalid:
            with self.subTest(value=value.sequence), self.assertRaises(ValueError):
                self.observe(s, value)
        rows = list(self.snapshots[1].rows)
        rows[0] = rows[0].model_copy(update={'volume': Decimal('2')})
        with self.assertRaises(ValueError):
            self.observe(s, self.snapshots[1].model_copy(update={'rows': tuple(rows)}))
        self.assertEqual(s.path_state.count, 1)

    def test_snapshot_copy_bypass_unconfirmed_and_incomplete_history_reject(self):
        s = self.snapshots[0]
        for value in (s.model_copy(update={'rows': s.rows[1:]}),
                      s.model_copy(update={'rows': (*s.rows[:-1], s.rows[-1].model_copy(update={'confirmed': False}))}),
                      s.model_copy(update={'sequence': True})):
            with self.assertRaises(ValueError):
                self.observe(None, value)

    def test_wrong_dataset_and_future_scale_reject(self):
        s = self.snapshots[0]
        with self.assertRaises(ValueError):
            self.observe(None, s.model_copy(update={'dataset': s.dataset.model_copy(update={'trading_pair': 'BTC-USDC'})}))
        for parent in (self.contract.scale_parent.model_copy(update={'dataset_sha256': 'a' * 64}),
                       self.contract.scale_parent.model_copy(update={'available_time_ms': 3660008})):
            with self.assertRaises(ValueError):
                dataclasses.replace(self.contract, scale_parent=parent)
        with self.assertRaises(ValueError):
            dataclasses.replace(self.contract, scope=self.contract.scope.model_copy(update={'recipe_sha256': 'a' * 64}))

    def test_exact_milliseconds_and_invalid_precision(self):
        self.assertEqual(self.m.seconds_to_ms(3660.007), 3660007)
        self.assertEqual(self.m.seconds_to_ms(Decimal('3660.007')), 3660007)
        for value in (3660.0001, True, float('nan'), '3660.007', Decimal('1E100')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.m.seconds_to_ms(value)
        with self.assertRaises(ValueError):
            self.observe(None, self.snapshots[0].model_copy(update={'published_at': 3660.0001}))

    def test_seed_time_and_stale_transport_reject(self):
        with self.assertRaises(ValueError):
            self.observe(None, self.snapshots[1])
        for now in (3660006, 3675008, 3720000, True):
            with self.subTest(now=now), self.assertRaises(ValueError):
                self.m.observe(self.contract, None, self.snapshots[0], now_ms=now)

    def test_snapshot_caller_mutation_does_not_rewrite_state(self):
        s = self.observe(None, self.snapshots[0])
        before = self.m.evidence(s, now_ms=s.evaluated_at_ms)
        changed = self.snapshots[0].model_copy(update={'epoch': 'changed'})
        with self.assertRaises(ValueError):
            self.observe(s, changed)
        self.assertEqual(before, self.m.evidence(s, now_ms=s.evaluated_at_ms))
        before['features']['rebound_atr'] = 999
        self.assertNotEqual(before, self.m.evidence(s, now_ms=s.evaluated_at_ms))

    def test_scale_parent_must_match_the_seed_snapshot(self):
        for updates in ({'content_sha256': 'f' * 64}, {'epoch': 'foreign'}, {'revision': 9}):
            changed = dataclasses.replace(self.contract, scale_parent=self.contract.scale_parent.model_copy(update=updates))
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                self.m.observe(changed, None, self.snapshots[0], now_ms=3660007)

    def test_later_setup_knowledge_cannot_backdate_a_recovery(self):
        snaps = tape(prices=(102, 103, 104))
        c = dataclasses.replace(contract(self.m, snaps), setup_available_ms=3660017,
                                policy=dataclasses.replace(self.contract.policy, rebound_atr=0.001))
        with self.assertRaises(ValueError):
            self.m.observe(c, None, snaps[0], now_ms=3660007)
        s = self.m.observe(c, None, snaps[0], now_ms=3660017)
        self.assertEqual(self.m.evidence(s, now_ms=3660017)['available_at_ms'], 3660017)


if __name__ == '__main__':
    unittest.main()
