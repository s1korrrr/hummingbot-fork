import tempfile
import unittest
from pathlib import Path

from hummingbot.strategy_v2.components.source_identity import component_source_files


class SourceIsolationTests(unittest.TestCase):
    def test_added_component_is_part_of_existing_seal(self):
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            for name in ('__init__.py', 'source_identity.py', 'recipe_contract.py'):
                (tmp_path / name).write_text('')
            before = component_source_files(tmp_path)
            child = tmp_path / 'events_v2'
            child.mkdir()
            (child / 'contracts.py').write_text('VALUE = 1\n')
            after = component_source_files(tmp_path)
            self.assertEqual(len(after), len(before) + 1)

    def test_recipe_file_outside_components_is_not_in_component_seal(self):
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve() / 'components'
            tmp_path.mkdir()
            for name in ('__init__.py', 'source_identity.py', 'recipe_contract.py'):
                (tmp_path / name).write_text('')
            (tmp_path / 'events_v2').mkdir()
            (tmp_path / 'events_v2' / 'contracts.py').write_text('VALUE = 1\n')
            before = component_source_files(tmp_path)
            recipe_root = tmp_path.parent / 'recipes'
            recipe_root.mkdir()
            (recipe_root / 'cross_venue_events_v2.py').write_text('VALUE = 2\n')
            after = component_source_files(tmp_path)
            self.assertEqual(after, before)

    def test_live_events_v2_files_are_inside_the_component_walk(self):
        from hummingbot.strategy_v2.components import source_identity
        files = {path.name for path in component_source_files(source_identity.component_root())}
        self.assertIn('contracts.py', files)
        self.assertIn('validation.py', files)
        self.assertNotIn('cross_venue_events_v2.py', files)
