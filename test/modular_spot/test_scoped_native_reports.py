"""Native controller-scoped reads preserve unavailable peer accounting."""
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold
from hummingbot.strategy_v2.models.executors_info import PerformanceReport


class UnavailableExecutor:
    @property
    def executor_info(self):
        raise RuntimeError('stale peer cannot be marked')


@pytest.fixture
def native():
    recorder = MagicMock(spec=MarketsRecorder)
    recorder.config_file_path = 'scope.yml'
    recorder.get_all_executors.return_value = []
    recorder.get_all_positions.return_value = []
    recorder.get_all_position_hold_ledger_entries.return_value = []
    recorder.get_trades_for_config.return_value = []
    recorder.get_positions_by_controller.return_value = []
    configs = {cid: SimpleNamespace(config=SimpleNamespace(connector_name='binance', trading_pair=pair))
               for cid, pair in [('fresh', 'ETH-USDT'), ('stale', 'BTC-USDT')]}
    provider = MagicMock()

    def price(connector, pair, kind):
        if pair == 'BTC-USDT':
            raise RuntimeError('stale peer cannot be marked')
        return Decimal('100')
    provider.get_price_by_type.side_effect = price
    strategy = SimpleNamespace(controllers=configs, markets={'binance': {'ETH-USDT', 'BTC-USDT'}},
                               market_data_provider=provider, current_timestamp=1000, connectors={})
    with patch.object(MarketsRecorder, 'get_instance', return_value=recorder):
        owner = ExecutorOrchestrator(strategy)
        stale = UnavailableExecutor()
        owner.active_executors = {'fresh': [], 'stale': [stale]}
        owner.positions_held = {'fresh': [], 'stale': [PositionHold('binance', 'BTC-USDT', TradeType.BUY)]}
        owner.cached_performance = {'fresh': PerformanceReport(), 'stale': PerformanceReport()}
        yield owner, recorder, stale


def test_scoped_report_and_sync_never_read_or_zero_unavailable_peer(native):
    owner, recorder, stale = native
    stale_positions = owner.positions_held['stale']
    receipts = [{'order_id': 'retained', 'trade_id': 'retained'}]
    owner._wallet_sale_receipts[('stale', 'binance', 'BTC-USDT')] = receipts
    reports = owner.get_all_reports(controller_ids={'fresh'})
    assert set(reports) == {'fresh'}
    owner.sync_live_state_to_db(controller_ids={'fresh'})
    recorder.get_positions_by_controller.assert_called_once_with('fresh')
    assert owner.active_executors['stale'] == [stale]
    assert owner.positions_held['stale'] is stale_positions
    assert owner._wallet_sale_receipts[('stale', 'binance', 'BTC-USDT')] is receipts
    recorder.store_or_update_executor.assert_not_called()
    recorder.update_or_store_position.assert_not_called()
    with pytest.raises(RuntimeError, match='stale peer'):
        owner.get_all_reports()


def test_scoped_store_clear_preserves_unselected_native_state(native):
    owner, recorder, stale = native
    stale_positions = owner.positions_held['stale']
    owner.store_all_positions(controller_ids={'fresh'}, strict=True)
    owner.store_all_executors(controller_ids={'fresh'})
    assert set(owner.active_executors) == {'stale'}
    assert owner.active_executors['stale'] == [stale]
    assert set(owner.positions_held) == {'stale'}
    assert owner.positions_held['stale'] is stale_positions
    recorder.update_or_store_position.assert_not_called()
    recorder.store_or_update_executor.assert_not_called()


def test_empty_scope_is_not_all_owners(native):
    owner, recorder, stale = native
    assert owner.get_all_reports(controller_ids=set()) == {}
    owner.sync_live_state_to_db(controller_ids=set())
    owner.store_all_positions(controller_ids=set())
    owner.store_all_executors(controller_ids=set())
    assert owner.active_executors['stale'] == [stale]
    recorder.get_positions_by_controller.assert_not_called()
