import ast
import asyncio
import json
import pathlib
import tempfile
import unittest


class AdapterImportTests(unittest.TestCase):
    def test_v2_adapter_does_not_import_legacy_policy_families(self):
        path = pathlib.Path('controllers/generic/cross_venue_event_v2.py')
        tree = ast.parse(path.read_text())
        names = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module)
        self.assertTrue(path.is_file())
        self.assertFalse(any('ok_rsi' in name or 'rsi_v5' in name or 'rsi_modular' in name for name in names))
        self.assertFalse(any(name.endswith('cross_venue_event_core') for name in names))

    def test_recipe_mutation_outside_components_does_not_change_component_walk(self):
        from hummingbot.strategy_v2.components import source_identity
        from hummingbot.strategy_v2.components.source_identity import component_source_files
        names = {path.name for path in component_source_files(source_identity.component_root())}
        self.assertIn('contracts.py', names)
        self.assertNotIn('cross_venue_events_v2.py', names)
        self.assertNotIn('cross_venue_event_v2.py', names)


try:
    from controllers.generic.cross_venue_event_v2 import CrossVenueEventV2Config, CrossVenueEventV2Controller
    NATIVE = True
except ModuleNotFoundError:
    NATIVE = False


class NativeAdapterTests(unittest.TestCase):
    @unittest.skipUnless(NATIVE, 'Exact native Hummingbot dependencies are not installed; no stub replacement')
    def test_native_loader_holds_stale_and_mutated_snapshots_without_actions(self):
        from test.cross_venue_events_v2.test_contracts import T, snapshot

        class Clock:
            now_ms = T + 1

            def time(self):
                return self.now_ms / 1000

        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'snapshot.json'
            first = snapshot(())
            path.write_text(json.dumps(first.model_dump(mode='json')))
            clock = Clock()
            controller = CrossVenueEventV2Controller(
                CrossVenueEventV2Config(id='events-v2', snapshot_path=str(path)), clock, asyncio.Queue())
            asyncio.run(controller.update_processed_data())
            self.assertEqual(controller.processed_data['status'], 'NO_ORDER_OBSERVER_V2')
            changed = snapshot((), dataset_sha256='f' * 64)
            path.write_text(json.dumps(changed.model_dump(mode='json')))
            asyncio.run(controller.update_processed_data())
            self.assertEqual(controller.processed_data['status'], 'UNAVAILABLE')
            self.assertEqual(controller.determine_executor_actions(), [])
            path.write_text(json.dumps(first.model_dump(mode='json')))
            clock.now_ms = T + 120_001
            asyncio.run(controller.update_processed_data())
            self.assertEqual(controller.processed_data['status'], 'UNAVAILABLE')

    @unittest.skipUnless(NATIVE, 'Exact native Hummingbot dependencies are not installed; no stub replacement')
    def test_config_cannot_enable_execution_or_adopt_inventory(self):
        from pydantic import ValidationError
        for changes in ({'execution_enabled': True}, {'total_amount_quote': 1},
                        {'initial_positions': [{'connector_name': 'okx', 'trading_pair': 'BTC-USDC', 'amount': '1'}]}):
            with self.assertRaises(ValidationError):
                CrossVenueEventV2Config(id='events-v2', snapshot_path='/tmp/events-v2.json', **changes)

    @unittest.skipUnless(NATIVE, 'Exact native Hummingbot dependencies are not installed; no stub replacement')
    def test_native_boundaries_reject_actions_without_starting_engine(self):
        import asyncio

        class Clock:
            def time(self):
                return 1780000000
        controller = CrossVenueEventV2Controller(
            CrossVenueEventV2Config(id='events-v2', snapshot_path='/tmp/no-snapshot-v2.json'), Clock(), asyncio.Queue())
        from controllers.generic.cross_venue_event_v2 import source_files
        from hummingbot.strategy_v2.components.recipe_contract import source_digest
        owner = pathlib.Path(__file__).resolve().parents[2]
        self.assertEqual(controller._recipe.binding.source_sha256, source_digest(owner, source_files()))
        self.assertEqual(controller.determine_executor_actions(), [])
        self.assertFalse(controller.validate_executor_config(object()))
        with self.assertRaises(PermissionError):
            asyncio.run(controller.send_actions([object()]))
