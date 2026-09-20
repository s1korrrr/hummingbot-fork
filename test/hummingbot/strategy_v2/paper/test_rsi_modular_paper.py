"""Public profiles exercise the same native recorder/funding lifecycle."""
import asyncio
import tempfile
from pathlib import Path
from test.hummingbot.strategy_v2.paper import test_paper_funding as funding_tests
from test.hummingbot.strategy_v2.paper.test_paper_session import PAIR, D, paper
from unittest import IsolatedAsyncioTestCase

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.paper_session import NativePaperSession


def public_config(profile):
    from controllers.generic.rsi_modular import get_config_class
    return get_config_class(profile)(id='modular_spot_public_paper', profile=profile,
                                     execution_mode='paper', connector_name='okx_paper_trade', candles_connector='okx',
                                     execution_enabled=True, trading_pair=PAIR, strict_hmm_mode=False,
                                     trend_confirmation_enabled=False, min_signal_score=1, dynamic_position_sizing=False,
                                     funding={'allocation_quote': '50', 'fee_reserve_fraction': '.003'})


class PublicProfilePaperTests(IsolatedAsyncioTestCase):
    setUp = funding_tests.NativePaperFundingTests.setUp
    seed = funding_tests.NativePaperFundingTests.seed

    async def test_both_public_profiles_partial_cancel_recovery_and_cross_profile_hold(self):
        from controllers.generic.modular_spot_core.domain import PolicyError
        from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
        from controllers.generic.modular_spot_core.native_recovery import NativeRecoveryStore
        for profile in ('ok_rsi', 'rsi_v5'):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                db = Path(directory) / 'paper.sqlite'
                config = public_config(profile)
                session = NativePaperSession(paper(), [config], db)
                try:
                    await self.seed(session)
                    owner = next(iter(session.controllers.values()))
                    executor = session.executor_orchestrator.active_executors[owner.config.id][0]
                    assert executor.config.entry_order_policy == ('single_attempt' if profile == 'ok_rsi' else 'retry')
                    assert owner.get_custom_info()['modular_composition']['profile'] == profile
                    await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                            side=TradeType.SELL, price=executor.config.entry_price, amount=D('.1'))])
                    other = public_config('rsi_v5' if profile == 'ok_rsi' else 'ok_rsi')
                    foreign = other.get_controller_class()(other, session.market_data_provider, asyncio.Queue())
                    with self.assertRaisesRegex(PolicyError, 'identity/schema'):
                        NativeFundingStore(foreign, session.recorder).reconcile()
                    with self.assertRaisesRegex(PolicyError, 'source/configuration changed'):
                        NativeRecoveryStore(foreign, session.recorder, foreign._native_source_hash).prepare()
                finally:
                    await session.close()
                with self.assertRaisesRegex(ValueError, 'identity'):
                    NativePaperSession(paper(), [other], db, restore=True)
                restored = NativePaperSession(paper(), [config], db, restore=True)
                try:
                    owner = next(iter(restored.controllers.values()))
                    restored._connector.set_order_book(PAIR, 4, restored._connector.current_timestamp, [(90, 10)], [(90.1, 10)])
                    owner.executors_info = restored.recorder.get_executors_by_controller(owner.config.id)
                    assert NativeFundingStore(owner, restored.recorder).reconcile()['released_intents'] == 1
                    assert restored._connector.get_balance('BTC') == D('.1')
                finally:
                    await restored.close()
