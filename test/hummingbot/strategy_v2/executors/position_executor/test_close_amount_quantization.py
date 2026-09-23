from decimal import Decimal
from test.hummingbot.strategy_v2.executors.position_executor.test_partial_fill_lifecycle import make_executor, tracked
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


class TestPositionExecutorCloseAmountQuantization(TestCase):

    def setUp(self):
        self.executor = object.__new__(PositionExecutor)
        self.executor.config = SimpleNamespace(
            connector_name="binance",
            trading_pair="ETH-USDT",
            entry_order_policy="multiple_attempts",
        )
        self.executor._resume_entry_sealed = False
        self.connector = MagicMock()
        self.executor.connectors = {"binance": self.connector}
        self.executor.get_available_balance = MagicMock(return_value=Decimal("2"))
        self.logger = MagicMock()
        self.logger_patch = patch.object(PositionExecutor, "logger", return_value=self.logger)
        self.logger_patch.start()
        self.addCleanup(self.logger_patch.stop)
        self.executor._log_message = MagicMock(side_effect=lambda message: message)

    def _close_amount(self):
        with patch.object(
            PositionExecutor,
            "amount_to_close",
            new_callable=PropertyMock,
            return_value=Decimal("1.2345"),
        ), patch.object(
            PositionExecutor,
            "is_perpetual",
            new_callable=PropertyMock,
            return_value=False,
        ), patch.object(
            PositionExecutor,
            "close_order_side",
            new_callable=PropertyMock,
            return_value=TradeType.SELL,
        ):
            return self.executor._close_order_amount()

    def test_close_amount_quantization_exception_fails_closed(self):
        self.connector.quantize_order_amount.side_effect = RuntimeError("rules unavailable")

        self.assertEqual(self._close_amount(), Decimal("0"))
        self.logger.error.assert_called_once()

    def test_close_amount_quantization_cannot_expand_available_cap(self):
        self.executor.get_available_balance.return_value = Decimal("1.1")
        self.connector.quantize_order_amount.return_value = Decimal("1.2")

        self.assertEqual(self._close_amount(), Decimal("0"))
        self.logger.error.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('close_type', [CloseType.TRAILING_STOP, CloseType.EARLY_STOP])
@pytest.mark.parametrize('minimum', [Decimal('0'), Decimal('.01')])
@pytest.mark.parametrize('invalid', [None, Decimal('NaN'), Decimal('Infinity'), Decimal('-1'),
                                     Decimal('1.1'), Decimal('0'), RuntimeError('rules unavailable')])
async def test_actual_shutdown_quantizer_failure_retains_inventory_and_retries(minimum, invalid, close_type):
    executor = make_executor()
    executor._open_order = tracked('filled-entry', TradeType.BUY, '1', OrderState.FILLED)
    executor.trading_rules.min_order_size = minimum
    executor._trailing_stop_trigger_pct = Decimal('.02')
    executor.get_price.return_value = Decimal('110')
    executor._status = RunnableStatus.SHUTTING_DOWN
    executor.close_type = close_type
    executor.place_order = MagicMock(return_value='local-close')
    quantizer = executor.connectors['okx'].quantize_order_amount
    if isinstance(invalid, Exception):
        quantizer.side_effect = invalid
    else:
        quantizer.side_effect = None
        quantizer.return_value = invalid

    await executor.control_shutdown_process()

    executor.place_order.assert_not_called()
    assert executor._close_order is None
    assert executor.status == RunnableStatus.SHUTTING_DOWN
    assert executor.amount_to_close == Decimal('1')
    assert executor._trailing_stop_trigger_pct == Decimal('.02')
    assert executor.close_type == close_type

    quantizer.side_effect = None
    quantizer.return_value = Decimal('1')
    await executor.control_shutdown_process()
    executor.place_order.assert_called_once()
    assert executor.place_order.call_args.kwargs['amount'] == Decimal('1')
    assert executor._close_order.order_id == 'local-close'
    assert executor.amount_to_close == Decimal('1')  # Submission never fabricates fills.


def test_actual_close_preserves_successful_string_quantization_and_available_cap():
    executor = make_executor()
    executor._open_order = tracked('filled-entry', TradeType.BUY, '1', OrderState.FILLED)
    executor.place_order = MagicMock(return_value='local-close')
    connector = executor.connectors['okx']
    connector.get_available_balance.return_value = Decimal('.8')
    connector.quantize_order_amount.side_effect = None
    connector.quantize_order_amount.return_value = '.79'
    executor.place_close_order_and_cancel_open_orders(CloseType.EARLY_STOP)
    connector.quantize_order_amount.assert_called_once_with(trading_pair='ETH-USDT', amount=Decimal('.8'))
    assert executor.place_order.call_args.kwargs['amount'] == Decimal('.79')
    assert executor.amount_to_close == Decimal('1')


@pytest.mark.parametrize('owned,expected', [('5.2', '5.2'), ('5', '5')])
def test_trailing_exit_closes_full_owned_lot_without_sweeping_shared_wallet(owned, expected):
    executor = make_executor()
    executor._open_order = tracked('filled-entry', TradeType.BUY, owned, OrderState.FILLED)
    executor.place_order = MagicMock(return_value='local-close')
    executor.get_price.return_value = Decimal('110')
    executor.connectors['okx'].get_available_balance.return_value = Decimal('5.2')
    executor.connectors['okx'].quantize_order_amount.side_effect = None
    executor.connectors['okx'].quantize_order_amount.return_value = Decimal(expected)

    executor.place_close_order_and_cancel_open_orders(CloseType.TRAILING_STOP)

    assert executor.place_order.call_args.kwargs['amount'] == Decimal(expected)
    assert executor.amount_to_close == Decimal(owned)
