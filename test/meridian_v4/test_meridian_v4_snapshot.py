"""The V4 paper snapshot is the native snapshot with readiness scoped to the controller's own pair."""
import hashlib
import inspect
import tempfile
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_session as session_tests
from test.meridian_v4 import test_meridian_v4_native_paper as native_paper
from test.meridian_v4.test_meridian_v4_contract import config
from test.meridian_v4.test_meridian_v4_native_paper import OWNER, PAIR, paper
from unittest import IsolatedAsyncioTestCase

from controllers.generic.meridian_v4 import _V4PaperBindings
from controllers.generic.modular_spot_core.domain import DataUnavailable
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from hummingbot.strategy_v2.paper_session import NativePaperSession

# NativeBindings.snapshot as reviewed for the V4 copy (2026-10-15). A change means the V4 copy must be re-reviewed.
NATIVE_SNAPSHOT_SHA256 = '0a7827a3f4ed2fce7ce71f396a38018c1eb3657714ac1a65d053939ba2d664d5'  # noqa: documentation


def native_digest():
    return hashlib.sha256(inspect.getsource(NativeBindings.snapshot).encode()).hexdigest()


class _NativeOnPaperConnector(NativeBindings):
    """Native snapshot logic on the exact paper connector (the only V4 difference besides readiness)."""
    spot_connector = staticmethod(_V4PaperBindings.spot_connector)


class V4SnapshotTests(IsolatedAsyncioTestCase):
    setUp = session_tests.NativePaperSessionTests.setUp

    def test_native_snapshot_source_is_the_reviewed_one(self):
        self.assertEqual(native_digest(), NATIVE_SNAPSHOT_SHA256)

    async def test_equal_to_native_when_every_book_is_fresh_and_pair_scoped_otherwise(self):
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(('BNB-USDC', 'ETH-USDC')), [config(execution_enabled=True)],
                                         Path(directory) / 'v4.sqlite')
            try:
                await native_paper.MeridianV4RestingEntryTests.rest_entry(self, session)
                controller = session.controllers[OWNER]
                self.assertEqual(_V4PaperBindings.snapshot(controller), _NativeOnPaperConnector.snapshot(controller))
                session._connector.invalidate_order_book('ETH-USDC')
                with self.assertRaises(DataUnavailable):  # native aggregate readiness: another pair holds everyone
                    _NativeOnPaperConnector.snapshot(controller)
                scoped = _V4PaperBindings.snapshot(controller)  # another pair's quiet book does not hold this owner
                self.assertGreater(scoped['bid'], 0)
                session._connector.invalidate_order_book(PAIR)
                with self.assertRaises(DataUnavailable):
                    _V4PaperBindings.snapshot(controller)
            finally:
                await session.close()
