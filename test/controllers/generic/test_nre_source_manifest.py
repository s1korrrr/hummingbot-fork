import importlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from hummingbot.strategy_v2.components.source_identity import component_root, component_source_files


class SourceManifestTests(unittest.TestCase):
    def setUp(self):
        if not (Path(__file__).resolve().parents[3] / 'controllers/generic/nre_v2_core/source_manifest.py').exists():
            self.fail('exact isolated source manifest is absent')
        self.m = importlib.import_module('controllers.generic.nre_v2_core.source_manifest')

    def test_real_source_hash_is_verified_not_just_declared(self):
        expected = self.m.implementation_sha256()
        self.assertEqual(self.m.verify_implementation(expected), expected)
        with self.assertRaises(ValueError):
            self.m.verify_implementation('f' * 64)
        records = self.m.source_manifest()
        self.assertTrue(any(name.startswith('@strategy_components/') for name, _ in records))
        self.assertTrue(any(name.endswith('/recovery_path.py') for name, _ in records))

    def test_exact_module_set_rejects_extra_and_missing_files(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for name in self.m.MODULES:
                (root / name).write_text('')
            self.assertEqual(len(self.m._package_files(root)), len(self.m.MODULES))
            (root / 'extra.py').write_text('')
            with self.assertRaises(ValueError):
                self.m._package_files(root)
            (root / 'extra.py').unlink()
            (root / 'binding.py').unlink()
            with self.assertRaises(ValueError):
                self.m._package_files(root)

    def test_symlink_and_nested_module_are_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for name in self.m.MODULES:
                (root / name).write_text('')
            (root / 'dangling').symlink_to(root / 'absent')
            with self.assertRaises(ValueError):
                self.m._package_files(root)
            (root / 'dangling').unlink()
            (root / 'nested').mkdir()
            (root / 'nested/extra.py').write_text('')
            with self.assertRaises(ValueError):
                self.m._package_files(root)

    def test_isolated_package_is_outside_protected_component_scan(self):
        canonical = component_source_files(component_root())
        own = self.m._package_files(Path(self.m.__file__).parent)
        self.assertFalse(set(canonical) & set(own))
        self.assertNotIn('modular_spot_core', str(Path(self.m.__file__).parent))
        self.assertNotIn('strategy_v2/components', str(Path(self.m.__file__).parent))

    def test_import_performs_no_external_actions_or_family_import(self):
        script = '''
import sys, threading
before = {t.ident for t in threading.enumerate()}
def audit(event, args):
    if event in {'socket.__new__', 'subprocess.Popen', 'os.system', 'sqlite3.connect'}:
        raise AssertionError(event)
sys.addaudithook(audit)
import controllers.generic.nre_v2_core.admission
import controllers.generic.nre_v2_core.source_manifest
assert not any(n.startswith('controllers.directional_trading') for n in sys.modules)
assert {t.ident for t in threading.enumerate()} == before
'''
        result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
