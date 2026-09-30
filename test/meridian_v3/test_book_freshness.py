"""Live book timing boundaries; synthetic messages only, with no network or replay."""
import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from controllers.generic.meridian_v3_unified_core.symbols import SymbolBindings, SymbolRuntime
from controllers.generic.modular_spot_core.domain import DataUnavailable
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from controllers.generic.modular_spot_core.runtime import ModularRuntime
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker


def message(kind, event, update):
    return OrderBookMessage(kind, {
        'trading_pair': 'BTC-USDC', 'update_id': update,
        'bids': [['99', '2']], 'asks': [['101', '2']]}, event)


def epoch_message(kind, event, update, epoch, *, bid='99', amount='2'):
    result = message(kind, event, update)
    result.content.update(stream_epoch=epoch, bids=[[bid, amount]])
    return result


def epoch_tracker(epoch=0):
    source = SimpleNamespace(requires_order_book_stream_validation=True, epoch=epoch,
                             request_order_book_resync=AsyncMock())
    source.order_book_stream_valid = lambda pair, received: received == source.epoch
    tracker = OrderBookTracker(source, ['BTC-USDC'])
    tracker._order_books['BTC-USDC'] = OrderBook()
    return tracker, source


async def apply_messages(tracker, *messages):
    tracker._tracking_message_queues['BTC-USDC'] = Mock(get=AsyncMock(
        side_effect=[*messages, asyncio.CancelledError()]))
    with patch('hummingbot.core.data_type.order_book_tracker.time.time', return_value=100):
        with pytest.raises(asyncio.CancelledError):
            await tracker._track_single_book('BTC-USDC')


@pytest.mark.asyncio
async def test_new_epoch_requires_its_snapshot_applied_before_any_diff_can_restore_quotes():
    tracker, source = epoch_tracker()
    await apply_messages(tracker, epoch_message(OrderBookMessageType.SNAPSHOT, 99, 99000, 0))
    assert tracker.get_order_book_observation('BTC-USDC') is not None
    source.epoch = 1
    assert tracker.get_order_book_observation('BTC-USDC') is None
    await apply_messages(tracker, epoch_message(OrderBookMessageType.DIFF, 99.5, 99500, 1, bid='100'))
    assert tracker.get_order_book_observation('BTC-USDC') is None
    assert tracker.order_books['BTC-USDC'].get_price(False) == 99
    source.request_order_book_resync.assert_awaited_once_with('BTC-USDC')


@pytest.mark.asyncio
async def test_recovery_snapshot_discards_old_epoch_diffs_and_queued_updates():
    tracker, _ = epoch_tracker(1)
    tracker._past_diffs_windows['BTC-USDC'].append(
        epoch_message(OrderBookMessageType.DIFF, 99.9, 99900, 0, bid='105'))
    await apply_messages(tracker,
                         epoch_message(OrderBookMessageType.DIFF, 99.9, 99900, 0, bid='104'),
                         epoch_message(OrderBookMessageType.SNAPSHOT, 99.1, 99100, 1),
                         epoch_message(OrderBookMessageType.DIFF, 99.2, 99200, 1, bid='100'))
    assert tracker.order_books['BTC-USDC'].get_price(False) == 100
    assert tracker.get_order_book_observation('BTC-USDC').event_timestamp == 99.2
    assert all(row.content['stream_epoch'] == 1 for row in tracker._past_diffs_windows['BTC-USDC'])


@pytest.mark.asyncio
async def test_unsequenced_rest_snapshot_cannot_replace_valid_ws_levels():
    tracker, _ = epoch_tracker()
    rest = message(OrderBookMessageType.SNAPSHOT, 99.5, 99500)
    rest.content['bids'] = [['98', '3']]
    await apply_messages(tracker,
                         epoch_message(OrderBookMessageType.SNAPSHOT, 99, 99000, 0), rest,
                         epoch_message(OrderBookMessageType.DIFF, 99.6, 99600, 0, bid='100'))
    assert tracker.order_books['BTC-USDC'].get_price(False) == 100
    assert {row.price for row in tracker.order_books['BTC-USDC'].bid_entries()} == {99, 100}
    assert tracker.get_order_book_observation('BTC-USDC').event_timestamp == 99.6


@pytest.mark.asyncio
async def test_stream_recovery_snapshot_routes_even_when_initial_rest_snapshot_is_newer():
    tracker, _ = epoch_tracker()
    book = tracker.order_books['BTC-USDC']
    book.apply_snapshot([], [], 99900)
    snapshot = epoch_message(OrderBookMessageType.SNAPSHOT, 99.1, 99100, 0)
    tracker._order_book_diff_stream = Mock(get=AsyncMock(side_effect=[snapshot, asyncio.CancelledError()]))
    queue = asyncio.Queue()
    tracker._tracking_message_queues['BTC-USDC'] = queue
    with pytest.raises(asyncio.CancelledError):
        await tracker._order_book_diff_router()
    assert queue.qsize() == 1
    assert queue.get_nowait() is snapshot


def test_stale_live_book_cannot_demote_a_tranche_through_the_bag_stop_fallback():
    runtime = object.__new__(SymbolRuntime)
    runtime.controller = Mock()
    runtime.bindings = Mock()
    runtime.bindings.fresh_book.side_effect = DataUnavailable('stale source book')
    with patch.object(ModularRuntime, '_bag_stops_only', return_value=['demotion']) as fallback:
        assert runtime._bag_stops_only() == []
    fallback.assert_not_called()


@pytest.mark.asyncio
async def test_book_timing_is_recorded_after_apply_and_not_refreshed_by_reads():
    tracker = OrderBookTracker(Mock(), ['BTC-USDC'])
    book = Mock()
    tracker._order_books['BTC-USDC'] = book
    tracker._tracking_message_queues['BTC-USDC'] = Mock(get=AsyncMock(side_effect=[
        message(OrderBookMessageType.DIFF, 99.25, 99250), asyncio.CancelledError()]))
    with patch('hummingbot.core.data_type.order_book_tracker.time.time', return_value=100):
        with pytest.raises(asyncio.CancelledError):
            await tracker._track_single_book('BTC-USDC')
    observed = tracker.get_order_book_observation('BTC-USDC')
    assert observed.event_timestamp == 99.25
    assert observed.applied_timestamp == 100
    assert tracker.get_order_book_observation('BTC-USDC') == observed
    tracker._order_books['BTC-USDC'] = Mock()  # replacement cannot inherit a fresh stamp
    assert tracker.get_order_book_observation('BTC-USDC') is None


@pytest.mark.asyncio
async def test_snapshot_replay_keeps_last_applied_diff_event_time():
    tracker = OrderBookTracker(Mock(), ['BTC-USDC'])
    tracker._order_books['BTC-USDC'] = Mock()
    tracker._past_diffs_windows['BTC-USDC'].append(message(OrderBookMessageType.DIFF, 99.9, 99900))
    tracker._tracking_message_queues['BTC-USDC'] = Mock(get=AsyncMock(side_effect=[
        message(OrderBookMessageType.SNAPSHOT, 99.1, 99100), asyncio.CancelledError()]))
    with patch('hummingbot.core.data_type.order_book_tracker.time.time', return_value=100):
        with pytest.raises(asyncio.CancelledError):
            await tracker._track_single_book('BTC-USDC')
    assert tracker.get_order_book_observation('BTC-USDC').event_timestamp == 99.9


@pytest.mark.parametrize('event,applied', [
    (90, 100), (99, 90), (101, 100), (99, 101),
    (float('nan'), 100), (99, float('inf'))])
def test_ready_live_connector_cannot_supply_stale_future_or_nonfinite_book(event, applied):
    controller = SimpleNamespace(config=SimpleNamespace(trading_pair='BTC-USDC'), max_quote_age_ms=5000,
                                 market_data_provider=SimpleNamespace(time=lambda: 100))
    connector = SimpleNamespace(ready=True, get_order_book_observation=lambda pair: SimpleNamespace(
        event_timestamp=event, applied_timestamp=applied))
    bindings = SymbolBindings('owner', 'BTC-USDC')
    with patch.object(bindings, 'spot_connector', return_value=connector), \
            patch.object(NativeBindings, 'snapshot', return_value={'bid': 99, 'ask': 101}) as base:
        with pytest.raises(DataUnavailable, match='book'):
            bindings.snapshot(controller)
        base.assert_not_called()


def test_missing_timing_blocks_but_current_timing_is_preserved():
    controller = SimpleNamespace(config=SimpleNamespace(trading_pair='BTC-USDC'), max_quote_age_ms=5000,
                                 market_data_provider=SimpleNamespace(time=lambda: 100))
    connector = SimpleNamespace(ready=True, get_order_book_observation=lambda pair: None)
    bindings = SymbolBindings('owner', 'BTC-USDC')
    with patch.object(bindings, 'spot_connector', return_value=connector), \
            patch.object(NativeBindings, 'snapshot', return_value={'bid': 99, 'ask': 101}):
        with pytest.raises(DataUnavailable, match='book'):
            bindings.snapshot(controller)
        connector.get_order_book_observation = lambda pair: SimpleNamespace(
            event_timestamp=99.25, applied_timestamp=99.5)
        assert bindings.snapshot(controller)['bid'] == 99
        assert bindings.quote_observation.event_timestamp == 99.25
        assert bindings.quote_observation.applied_timestamp == 99.5


def test_stale_quote_does_not_read_trailing_reference_create_exit_or_demote_inventory():
    from controllers.generic.modular_spot_core.execution import ModularPositionExecutor
    from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
    from hummingbot.strategy_v2.models.executors import CloseType

    executor = object.__new__(ModularPositionExecutor)
    executor.config = SimpleNamespace(controller_id='owner', trading_pair='BTC-USDC')
    executor._strategy = SimpleNamespace(controllers={'owner': SimpleNamespace(quote_permission=lambda pair: False)})
    executor.early_stop = Mock()
    executor.get_price = Mock()
    with patch.object(PositionExecutor, 'place_close_order_and_cancel_open_orders') as place:
        assert executor._current_trailing_reference_price() is None
        executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)
        executor.get_price.assert_not_called()
        executor.early_stop.assert_not_called()
        place.assert_not_called()


@pytest.mark.parametrize('trigger', [None, Decimal('.02')])
def test_stale_quote_cannot_mutate_or_persist_actual_trailing_control(trigger):
    from controllers.generic.modular_spot_core.execution import ModularPositionExecutor

    owner = SimpleNamespace(quote_permission=lambda pair: False)
    executor = object.__new__(ModularPositionExecutor)
    executor.config = SimpleNamespace(
        controller_id='owner', trading_pair='BTC-USDC',
        triple_barrier_config=SimpleNamespace(trailing_stop=SimpleNamespace(
            activation_price=Decimal('.02'), trailing_delta=Decimal('.01'))))
    executor._strategy = SimpleNamespace(controllers={'owner': owner}, current_timestamp=100)
    executor._resume_conflicting_fill = None
    executor._trailing_stop_trigger_pct = trigger
    executor._trailing_stop_move_count = 2
    executor._trailing_stop_armed_timestamp = 90
    executor._trailing_stop_last_update_timestamp = 95
    executor._trailing_floor_hold = False
    executor.get_net_pnl_pct = Mock(return_value=Decimal('.10'))
    executor._floored_trailing_trigger_pct = lambda value: value
    executor._checkpoint_trailing_state = Mock(return_value=True)
    executor._trailing_status_snapshot = Mock(return_value={})
    executor._price_for_net_pnl_pct = Mock(return_value=Decimal('100'))
    executor._log_message = Mock(return_value='fixture')
    executor.logger = Mock(return_value=Mock())
    executor.control_trailing_stop()  # Exercise the real inherited state machine.
    assert executor._trailing_stop_trigger_pct == trigger
    assert executor._trailing_stop_move_count == 2
    assert executor._trailing_stop_armed_timestamp == 90
    assert executor._trailing_stop_last_update_timestamp == 95
    executor._checkpoint_trailing_state.assert_not_called()
    executor.get_net_pnl_pct.assert_not_called()
    owner.quote_permission = lambda pair: True
    executor.control_trailing_stop()
    assert executor._trailing_stop_trigger_pct == Decimal('.09')
    assert executor._trailing_stop_last_update_timestamp == 100
    executor._checkpoint_trailing_state.assert_called_once()


def test_stale_bag_does_not_reprice_but_pending_cancellation_continues():
    from controllers.generic.meridian_v3_unified_core.execution import MeridianRecoveryExecutor
    from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor

    executor = object.__new__(MeridianRecoveryExecutor)
    executor.config = SimpleNamespace(controller_id='owner', trading_pair='BTC-USDC')
    executor._strategy = SimpleNamespace(controllers={'owner': SimpleNamespace(quote_permission=lambda pair: False)})
    executor._cancel_pending = True
    executor.cancel_order = Mock()
    with patch.object(OrderExecutor, 'control_order') as control, patch.object(OrderExecutor, 'place_open_order') as place:
        executor.control_order()
        executor.place_open_order()
        executor.cancel_order.assert_called_once()
        control.assert_not_called()
        place.assert_not_called()
