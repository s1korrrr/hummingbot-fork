import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from controllers.generic.cross_venue_event_core import timestamp

class Boundaries(unittest.TestCase):
    def test_timestamp_rejects_seconds_microseconds_and_boolean(self):
        for value in (1780000000, 1780000000000000, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                timestamp(value, 'event')

    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'POSIX file-type guard')
    def test_fifo_does_not_block_observer(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'pipe'
            os.mkfifo(path)
            source = ('from controllers.generic.cross_venue_event_core import read_snapshot\n'
                      'try: read_snapshot(' + repr(str(path)) + ',1780000000000)\n'
                      'except ValueError: raise SystemExit(0)\nraise SystemExit(1)')
            try:
                result = subprocess.run([sys.executable,'-c',source],capture_output=True,timeout=2)
            except subprocess.TimeoutExpired:
                self.fail('non-regular snapshot blocked its reader')
            self.assertEqual(result.returncode,0,result.stderr)

if __name__ == '__main__':
    unittest.main()
