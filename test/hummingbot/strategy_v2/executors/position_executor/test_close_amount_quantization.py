from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor


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
