from decimal import Decimal

import pandas as pd
import pytest

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.backtesting.backtesting_engine_base import BacktestingEngineBase
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig


def test_native_order_executor_cannot_disappear_from_backtest():
    engine = BacktestingEngineBase()
    config = OrderExecutorConfig(timestamp=60, connector_name='okx', trading_pair='BTC-USDC',
                                 side=TradeType.BUY, amount=Decimal('1'), price=Decimal('100'),
                                 execution_strategy=ExecutionStrategy.LIMIT_MAKER)
    with pytest.raises(NotImplementedError, match='OrderExecutorConfig.*silently discarded'):
        engine.simulate_executor(config, pd.DataFrame(), 0.001)
