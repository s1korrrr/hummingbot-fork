import pytest
import pytest_asyncio

from .fixtures import recipe


@pytest.fixture
def r():
    return recipe()


@pytest_asyncio.fixture
async def native_evidence(tmp_path):
    from decimal import Decimal as D

    from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config
    from controllers.generic.momentum_fusion_v2_runtime.native_paper_capital import NativePaperAccountEvidence
    from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.connector.trading_rule import TradingRule
    from hummingbot.strategy_v2.paper_session import NativePaperSession
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    market = NativePaperTradeExchange({'BNB-USDC': TradingRule('BNB-USDC', min_order_size=D('.01'),
                                                               min_base_amount_increment=D('.01'), min_price_increment=D('.01'))})
    market.set_balance('USDC', D('100'))
    market.tick(1_700_000_005)
    market.set_order_book('BNB-USDC', 1, 1_700_000_005, [(99, 10)], [(101, 10)])
    config = MomentumFusionV2Config(id='momentum_v2_fixture', trading_pair='BNB-USDC', recipe=recipe(),
                                    execution_mode='paper', connector_name='okx_paper_trade')
    session = NativePaperSession(market, [config], tmp_path / 'account.sqlite')
    try:
        yield NativePaperAccountEvidence(session)
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior
