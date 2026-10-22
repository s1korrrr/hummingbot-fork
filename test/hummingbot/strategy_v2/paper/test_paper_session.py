import tempfile
from pathlib import Path
from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import (
    PAIR,
    D,
    delayed_research_paper,
    paper,
)
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import numpy as np
import pandas as pd

from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig
from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.strategy_v2.paper_session import NativePaperSession


def candles(now, count=600):
    x = np.arange(count)
    close = 100 + np.sin(x / 20) + x / 10000
    return pd.DataFrame({'timestamp': np.arange(now - count * 60, now, 60),
                         'open': close - .02, 'high': close + .1, 'low': close - .1,
                         'close': close, 'volume': np.ones(count) * 10,
                         'quote_asset_volume': close * 10})


def config(family=ModularOKRSIControllerConfig, **kwargs):
    # Other native tests deliberately reload controller modules; resolve the
    # current genuine class, preserving the production exact-type boundary.
    from controllers.generic.modular_ok_rsi import ModularOKRSIControllerConfig as CurrentOK
    from controllers.generic.modular_rsi_v5 import ModularRSIV5ControllerConfig as CurrentV5
    family = {'ModularOKRSIControllerConfig': CurrentOK, 'ModularRSIV5ControllerConfig': CurrentV5}[family.__name__]
    return family(id='modular_spot_paper_fixture', trading_pair=PAIR,
                  connector_name='okx_paper_trade', execution_mode='paper', candles_connector='okx',
                  strict_hmm_mode=False, trend_confirmation_enabled=False, **kwargs)


def causal_reversal_frames():
    """Synthetic causal native-policy fixture, not a historical/economic dataset."""
    frame = candles(600000)
    closes = np.r_[100 + np.sin(np.arange(550) / 10), np.linspace(100, 90, 50)]
    frame['open'] = frame['close'] = closes
    frame['high'], frame['low'] = closes + .1, closes - .1
    row = frame.iloc[-1].copy()
    row['timestamp'], row['open'], row['close'], row['high'], row['low'] = 600000, 90, 90.02, 90.12, 89.9
    return frame, pd.concat([frame, pd.DataFrame([row])], ignore_index=True)


def reversal_config(family=ModularOKRSIControllerConfig):
    return config(family, execution_enabled=True, min_signal_score=1, dynamic_position_sizing=False)


class NativePaperSessionTests(IsolatedAsyncioTestCase):
    def setUp(self):
        prior = MarketsRecorder._shared_instance
        MarketsRecorder._shared_instance = None
        self.addCleanup(setattr, MarketsRecorder, '_shared_instance', prior)
        for target in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.socket.sendto', 'socket.getaddrinfo'):
            guard = patch(target, side_effect=AssertionError('network forbidden'))
            guard.start()
            self.addCleanup(guard.stop)

    async def test_research_second_input_routes_through_native_connector_and_recorder(self):
        from hummingbot.core.data_type.common import OrderType, PositionAction
        with tempfile.TemporaryDirectory() as directory:
            market = delayed_research_paper()
            session = NativePaperSession(market, [config()], Path(directory) / 'research.sqlite')
            try:
                await session.step(1000, books=[dict(trading_pair=PAIR, sequence=2, timestamp=1000,
                                                     bids=[(99, 10)], asks=[(101, 10)])])
                order_id = session.buy(market.name, PAIR, D('2'), OrderType.LIMIT_MAKER, D('99'), PositionAction.NIL)
                with self.assertRaisesRegex(ValueError, 'preceding second'):
                    await session.step(1001, observed_seconds=[dict(trading_pair=PAIR, timestamp=1001,
                                                                    low=98, high=102, volume=100)])
                await session.step(1001, books=[dict(trading_pair=PAIR, sequence=3, timestamp=1000,
                                                     bids=[(99, 10)], asks=[(101, 10)])],
                                   observed_seconds=[dict(trading_pair=PAIR, timestamp=1000,
                                                          low=98, high=102, volume=100)])
                self.assertEqual(market._order_tracker.fetch_order(order_id).executed_amount_base, D('0'))
                await session.step(1002, books=[dict(trading_pair=PAIR, sequence=4, timestamp=1001,
                                                     bids=[(99, 10)], asks=[(101, 10)])],
                                   observed_seconds=[dict(trading_pair=PAIR, timestamp=1001,
                                                          low=98, high=102, volume=100)])
                self.assertEqual(market._order_tracker.fetch_order(order_id).executed_amount_base, D('1'))
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
            finally:
                await session.close()

    async def test_actual_native_feature_pipeline_for_both_families_no_orders_when_disabled(self):
        for family in (ModularOKRSIControllerConfig, ModularRSIV5ControllerConfig):
            with self.subTest(family=family.__name__), tempfile.TemporaryDirectory() as directory:
                market = paper()
                session = NativePaperSession(market, [config(family)], Path(directory) / 'paper.sqlite')
                try:
                    receipt = await session.step(600000, books=[dict(trading_pair=PAIR, sequence=2,
                                                                     timestamp=600000, bids=[(99, 10)], asks=[(101, 10)])], candles=[dict(
                                                                         connector_name='okx', trading_pair=PAIR, interval='1m', frame=candles(600000), available_at=600000)])
                    controller = session.controllers['modular_spot_paper_fixture']
                    self.assertTrue(controller.processed_data['data_health']['ready'])
                    self.assertFalse(controller.processed_data['features'].empty)
                    self.assertIn(f'RSI_{controller.config.rsi_length}', controller.processed_data['features'].columns)
                    self.assertEqual(receipt['actions'], [])
                    self.assertEqual(market.in_flight_orders, {})
                finally:
                    await session.close()
                self.assertIsNone(MarketsRecorder.get_existing_instance())

    async def test_rejects_live_and_legacy_connector_and_reuses_no_existing_db(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            with self.assertRaises(ValueError):
                NativePaperSession(object(), [config()], db)
            with self.assertRaises(ValueError):
                NativePaperSession(paper(), [ModularOKRSIControllerConfig(id='modular_spot_live')], db)
            db.write_text('existing unrelated data')
            with self.assertRaises(ValueError):
                NativePaperSession(paper(), [config()], db)
            self.assertEqual(db.read_text(), 'existing unrelated data')

    async def test_causal_native_controller_action_orchestrator_partial_fill_fee_and_restart(self):
        from hummingbot.core.data_type.common import TradeType
        from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            session = NativePaperSession(paper(), [reversal_config()], db)
            frames = causal_reversal_frames()
            try:
                for index, frame in enumerate(frames):
                    now = 600000 + index * 60
                    receipt = await session.step(now, books=[dict(trading_pair=PAIR, sequence=2 + index,
                                                                  timestamp=now, bids=[(90, 10)], asks=[(90.1, 10)])], candles=[dict(
                                                                      connector_name='okx', trading_pair=PAIR, interval='1m', frame=frame, available_at=now)])
                self.assertEqual(len(receipt['actions']), 1)
                owner = 'modular_spot_paper_fixture'
                executor = session.executor_orchestrator.active_executors[owner][0]
                self.assertIs(type(executor), PositionExecutor)
                self.assertIsNotNone(executor._open_order.order)
                price = executor._open_order.order.price
                receipt = await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1,
                                                                  timestamp=600061, side=TradeType.SELL, price=price - D('.01'), amount=D('.1'))])
                self.assertEqual(session._connector.get_balance('BTC'), D('.1'))
                self.assertEqual(executor.open_filled_amount, D('.1'))
                self.assertEqual(session._connector.get_balance('USDC'), D('1000') - D('.1') * price * D('1.001'))
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
                self.assertGreater(receipt['reports'][owner]['executors'][0].filled_amount_quote, 0)
                expected_balances = session._connector.get_all_balances()
            finally:
                await session.close()
            restored = NativePaperSession(paper(), [reversal_config()], db, restore=True)
            try:
                self.assertEqual(restored._connector.get_all_balances(), expected_balances)
                self.assertFalse(restored._connector.ready)
                receipt = await restored.step(600062, books=[dict(trading_pair=PAIR, sequence=4,
                                                                  timestamp=600062, bids=[(90, 10)], asks=[(90.1, 10)])], candles=[dict(
                                                                      connector_name='okx', trading_pair=PAIR, interval='1m', frame=frames[1], available_at=600062)])
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(restored._connector.in_flight_orders, {})
                self.assertEqual(restored._connector.get_balance('BTC'), D('.1'))
                self.assertTrue(receipt['readiness']['ready'], receipt['readiness']['reasons'])
            finally:
                await restored.close()

    async def test_executor_pause_follows_the_paper_clock_not_the_wall_clock(self):
        """A shutting-down executor's 1 s venue pause must not stall a replay step in real time."""
        import asyncio
        import time

        from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
        with tempfile.TemporaryDirectory() as directory:
            session = NativePaperSession(paper(), [reversal_config()], Path(directory) / 'pause.sqlite')
            try:
                for index, frame in enumerate(causal_reversal_frames()):
                    now = 600000 + index * 60
                    await session.step(now, books=[dict(trading_pair=PAIR, sequence=2 + index, timestamp=now,
                                                        bids=[(90, 10)], asks=[(90.1, 10)])], candles=[dict(
                                                            connector_name='okx', trading_pair=PAIR, interval='1m',
                                                            frame=frame, available_at=now)])
                executor = session.executor_orchestrator.active_executors['modular_spot_paper_fixture'][0]
                self.assertIs(type(executor), PositionExecutor)
                started = time.monotonic()
                await asyncio.wait_for(executor._sleep(1.0), timeout=0.25)
                self.assertLess(time.monotonic() - started, 0.25)
            finally:
                await session.close()
        # Outside the paper session the native executor keeps its real venue pause.
        with patch('asyncio.sleep') as sleep:
            await PositionExecutor._sleep(object(), 1.0)
        sleep.assert_awaited_once_with(1.0)

    async def test_direct_candle_boundary_rejects_future_gap_geometry_and_volume(self):
        from hummingbot.strategy_v2.paper_session import InjectedPaperMarketData
        market = paper()
        market.tick(600000)
        provider = InjectedPaperMarketData(market)
        original = candles(600000)
        for mutation in ('future_close', 'gap', 'unaligned', 'negative_volume', 'bad_high', 'unconfirmed'):
            frame = original.copy()
            if mutation == 'future_close':
                frame.loc[frame.index[-1], 'timestamp'] = 600000
            elif mutation == 'gap':
                frame = frame.drop(frame.index[-2])
            elif mutation == 'unaligned':
                frame['timestamp'] += 1
            elif mutation == 'negative_volume':
                frame.loc[frame.index[-1], 'volume'] = -1
            elif mutation == 'bad_high':
                frame.loc[frame.index[-1], 'high'] = 1
            else:
                frame['confirmed'] = False
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                provider.set_candles('okx', PAIR, '1m', frame, available_at=600000)

    async def test_restore_requires_own_marker_and_matching_initial_cash(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            session = NativePaperSession(paper(), [config()], db)
            await session.close()
            changed = paper()
            changed.set_balance('USDC', D('2000'))
            with self.assertRaisesRegex(ValueError, 'identity'):
                NativePaperSession(changed, [config()], db, restore=True)
            marker = db.with_suffix('.sqlite.paper-session.json')
            import json
            payload = json.loads(marker.read_text())
            payload['source_runtime_sha256'] = '0' * 64
            marker.write_text(json.dumps(payload))
            with self.assertRaisesRegex(ValueError, 'identity'):
                NativePaperSession(paper(), [config()], db, restore=True)
            marker.unlink()
            with self.assertRaises(ValueError):
                NativePaperSession(paper(), [config()], db, restore=True)
            self.assertIsNone(MarketsRecorder.get_existing_instance())

    async def test_abrupt_checkpoint_failure_retains_native_inventory_and_holds_restart(self):
        from hummingbot.core.data_type.common import TradeType
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / 'paper.sqlite'
            session = NativePaperSession(paper(), [reversal_config()], db)
            frames = causal_reversal_frames()
            for index, frame in enumerate(frames):
                now = 600000 + index * 60
                await session.step(now, books=[dict(trading_pair=PAIR, sequence=2 + index,
                                                    timestamp=now, bids=[(90, 10)], asks=[(90.1, 10)])], candles=[dict(
                                                        connector_name='okx', trading_pair=PAIR, interval='1m', frame=frame, available_at=now)])
            owner = 'modular_spot_paper_fixture'
            executor = session.executor_orchestrator.active_executors[owner][0]
            await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                    side=TradeType.SELL, price=executor._open_order.order.price - D('.01'), amount=D('.1'))])
            with patch.object(session.controllers[owner]._recovery_store, 'persist', side_effect=RuntimeError('checkpoint crash')):
                with self.assertRaisesRegex(RuntimeError, 'checkpoint crash'):
                    await session.close()
            restored = NativePaperSession(paper(), [reversal_config()], db, restore=True)
            try:
                receipt = await restored.step(600062, books=[dict(trading_pair=PAIR, sequence=4,
                                                                  timestamp=600062, bids=[(90, 10)], asks=[(90.1, 10)])], candles=[dict(
                                                                      connector_name='okx', trading_pair=PAIR, interval='1m', frame=frames[1], available_at=600062)])
                self.assertFalse(receipt['readiness']['ready'])
                self.assertTrue(any('inventory changed across restart' in r for r in receipt['readiness']['reasons']))
                self.assertEqual(receipt['actions'], [])
                self.assertEqual(restored._connector.get_balance('BTC'), D('.1'))
                self.assertEqual(restored._connector.in_flight_orders, {})
            finally:
                await restored.close()

    async def _session_with_resting_native_entry(self, db):
        session = NativePaperSession(paper(), [reversal_config()], db)
        for index, frame in enumerate(causal_reversal_frames()):
            now = 600000 + index * 60
            await session.step(now, books=[dict(trading_pair=PAIR, sequence=2 + index, timestamp=now,
                                                bids=[(90, 10)], asks=[(90.1, 10)])], candles=[dict(
                                                    connector_name='okx', trading_pair=PAIR, interval='1m', frame=frame, available_at=now)])
        executor = session.executor_orchestrator.active_executors['modular_spot_paper_fixture'][0]
        return session, executor

    async def test_late_invalid_batch_rejected_before_any_clock_fill_or_watermark_effect(self):
        from hummingbot.core.data_type.common import TradeType
        with tempfile.TemporaryDirectory() as directory:
            session, executor = await self._session_with_resting_native_entry(Path(directory) / 'paper.sqlite')
            try:
                before = session._connector.tracking_states
                price = executor._open_order.order.price
                trade = dict(trading_pair=PAIR, sequence=1, timestamp=600061, side=TradeType.SELL,
                             price=price - D('.01'), amount=D('.1'))
                for invalid in (dict(trade, sequence=2, amount=-1), dict(trade, trading_pair='INVALID-USDC'),
                                dict(trade, timestamp=600062), dict(trade, unexpected=True)):
                    with self.subTest(invalid=invalid), self.assertRaises((ValueError, TypeError)):
                        await session.step(600061, trades=[trade, invalid])
                    self.assertEqual(session._connector.tracking_states, before)
                    self.assertEqual(executor.open_filled_amount, D('0'))
                    self.assertEqual(session.recorder.get_trades_for_config('native-paper-session'), [])
                    self.assertFalse(session._failure_path.exists())
            finally:
                await session.close()

    async def test_failure_after_native_fill_is_durable_hold_even_if_failure_receipt_write_fails(self):
        from hummingbot.core.data_type.common import TradeType
        for fail_receipt in (False, True):
            with self.subTest(fail_receipt=fail_receipt), tempfile.TemporaryDirectory() as directory:
                db = Path(directory) / 'paper.sqlite'
                session, executor = await self._session_with_resting_native_entry(db)
                price = executor._open_order.order.price
                write = session._write_step_receipt

                def write_receipt(receipt):
                    if fail_receipt and receipt['status'] == 'failed':
                        raise OSError('injected receipt write failure')
                    write(receipt)
                with patch.object(session, '_write_step_receipt', side_effect=write_receipt), patch.object(
                        session, '_refresh_reports', side_effect=RuntimeError('injected report failure')):
                    with self.assertRaisesRegex(RuntimeError, 'injected report failure'):
                        await session.step(600061, trades=[dict(trading_pair=PAIR, sequence=1, timestamp=600061,
                                                                side=TradeType.SELL, price=price - D('.01'), amount=D('.1'))])
                self.assertEqual(session._connector.get_balance('BTC'), D('.1'))
                self.assertEqual(session._connector.get_balance('USDC'), D('1000') - D('.1') * price * D('1.001'))
                self.assertEqual(executor.open_filled_amount, D('.1'))
                self.assertEqual(len(session.recorder.get_trades_for_config('native-paper-session')), 1)
                self.assertTrue(executor._open_order.order.order_fills)
                self.assertTrue(session._failure_path.exists())
                with self.assertRaisesRegex(RuntimeError, 'HELD'):
                    await session.step(600062)
                controller = session.controllers['modular_spot_paper_fixture']
                with patch.object(controller._recovery_store, 'persist', side_effect=AssertionError('no clean checkpoint')):
                    await session.close()
                restored = NativePaperSession(paper(), [reversal_config()], db, restore=True)
                try:
                    self.assertEqual(restored._connector.get_balance('BTC'), D('.1'))
                    with self.assertRaisesRegex(RuntimeError, 'HELD'):
                        await restored.step(600062)
                finally:
                    await restored.close()
