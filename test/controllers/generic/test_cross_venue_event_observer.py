import asyncio
import importlib.util
import json
import pathlib
import tempfile
import unittest

from controllers.generic.cross_venue_event_core import SCHEMA, read_snapshot


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = pathlib.Path(self.directory.name) / 'snapshot.json'
        self.now = 1780000000000
        self.data = dict(schema=SCHEMA, generated_ms=self.now, risk_history_complete=True,
                         events=[], instruments=[], quotes=[])

    def write(self):
        self.path.write_text(json.dumps(self.data))

    def test_empty_but_complete_observation_is_not_a_trade(self):
        self.write()
        self.assertEqual(read_snapshot(self.path, self.now), ([], [], []))

    def test_partial_future_stale_oversized_or_unknown_fields_rejected(self):
        for changes in ({'risk_history_complete': False}, {'generated_ms': self.now + 1},
                        {'generated_ms': self.now - 5001}, {'orders': []}, {'events': {}}):
            with self.subTest(changes=changes):
                old = self.data.copy()
                self.data.update(changes)
                self.write()
                with self.assertRaises(ValueError):
                    read_snapshot(self.path, self.now)
                self.data = old
        self.path.write_bytes(b'x' * 2_000_001)
        with self.assertRaises(ValueError):
            read_snapshot(self.path, self.now)

    def test_duplicate_keys_nonfinite_json_and_symlinks_rejected(self):
        for raw in ('{"schema":"x","schema":"x"}', '{"generated_ms":NaN}', '{'):
            self.path.write_text(raw)
            with self.assertRaises(ValueError):
                read_snapshot(self.path, self.now)
        self.write()
        link = self.path.with_name('link.json')
        link.symlink_to(self.path)
        with self.assertRaises(OSError):
            read_snapshot(link, self.now)

    def test_native_observer_source_exists(self):
        self.assertTrue(pathlib.Path('controllers/generic/cross_venue_event.py').is_file())


try:
    from controllers.generic.cross_venue_event import CrossVenueEventConfig, CrossVenueEventController
    NATIVE = True
except ModuleNotFoundError:
    NATIVE = False


@unittest.skipUnless(NATIVE, 'Exact native Hummingbot dependencies are not installed; no stub replacement')
class NativeObserverTests(unittest.TestCase):
    def test_config_cannot_enable_execution_or_adopt_inventory(self):
        from pydantic import ValidationError
        for changes in ({'execution_enabled': True}, {'total_amount_quote': 1},
                        {'initial_positions': [{'connector_name': 'okx', 'trading_pair': 'BTC-USDC', 'amount': '1'}]}):
            with self.assertRaises(ValidationError):
                CrossVenueEventConfig(id='events', snapshot_path='/tmp/events.json', **changes)
        config = CrossVenueEventConfig(id='events', snapshot_path='/tmp/events.json')
        self.assertIs(config.get_controller_class(), CrossVenueEventController)

    def test_native_boundaries_reject_actions_without_starting_engine(self):
        class Clock:
            def time(self):
                return 1780000000
        controller = CrossVenueEventController(
            CrossVenueEventConfig(id='events', snapshot_path='/tmp/no-snapshot.json'), Clock(), asyncio.Queue())
        self.assertEqual(controller.determine_executor_actions(), [])
        self.assertFalse(controller.validate_executor_config(object()))
        with self.assertRaises(PermissionError):
            asyncio.run(controller.send_actions([object()]))
        asyncio.run(controller.update_processed_data())
        self.assertEqual(controller.processed_data['status'], 'UNAVAILABLE')
