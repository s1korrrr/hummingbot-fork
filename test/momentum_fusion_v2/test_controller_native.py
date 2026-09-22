import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config, MomentumFusionV2Controller
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.strategy_v2.paper_session import NativePaperSession

from .fixtures import T, frames, recipe


def config(**changes):
    values = dict(id='momentum_v2_fixture', trading_pair='BNB-USDC', recipe=recipe())
    values.update(changes)
    return MomentumFusionV2Config(**values)


@pytest.mark.parametrize('mode,connector', [('observation', 'okx'), ('shadow', 'okx'), ('paper', 'okx_paper_trade')])
@pytest.mark.parametrize('enabled', [True, 1, 'false', None])
def test_all_execution_enabling_and_coercion_refused(mode, connector, enabled):
    with pytest.raises(ValueError):
        config(execution_mode=mode, connector_name=connector, execution_enabled=enabled)


@pytest.mark.parametrize('mode,connector', [('paper', 'okx'), ('observation', 'okx_paper_trade'), ('shadow', 'okx_paper_trade')])
def test_mode_connector_binding(mode, connector):
    with pytest.raises(ValueError):
        config(execution_mode=mode, connector_name=connector)


def test_host_calls_canonical_three_stream_pipeline_and_never_emits():
    r = recipe()
    provider = MagicMock()
    provider.time.return_value = T + 1
    data = frames(r)
    provider.get_candle_snapshot.side_effect = lambda dataset: data[dataset.key]
    host = MomentumFusionV2Controller(config(), provider, asyncio.Queue())
    asyncio.run(host.update_processed_data())
    assert host.processed_data['data_health']['ready']
    assert host.processed_data['decision']['trace']['execution_authorized'] is False
    assert len(host.get_candles_config()) == 3
    assert host.determine_executor_actions() == []
    assert host.validate_executor_config(object()) is False
    projection = host.get_custom_info()['momentum_fusion_v2']
    assert projection['observed_at'] == T + 1
    assert projection['recipe_sha256'] == r.digest
    assert projection['execution_enabled'] is False
    provider.time.return_value = T + 30
    assert host.get_custom_info()['momentum_fusion_v2']['observed_at'] == T + 1


def test_no_publication_bound_snapshot_means_unavailable():
    provider = MagicMock(spec=['time', 'initialize_rate_sources'])
    provider.time.return_value = T + 1
    host = MomentumFusionV2Controller(config(), provider, asyncio.Queue())
    asyncio.run(host.update_processed_data())
    assert not host.processed_data['data_health']['ready']
    assert host.determine_executor_actions() == []


def test_live_mode_and_adopted_inventory_are_not_an_execution_escape():
    with pytest.raises(ValueError):
        config(execution_mode='live')


@pytest.mark.asyncio
async def test_real_native_paper_session_observes_three_streams_without_orders(tmp_path):
    from decimal import Decimal as D

    import pandas as pd

    from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
    from hummingbot.connector.trading_rule import TradingRule
    prior = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    market = NativePaperTradeExchange({'BNB-USDC': TradingRule('BNB-USDC', min_order_size=D('.01'),
                                                               min_base_amount_increment=D('.01'), min_price_increment=D('.01'))})
    market.set_balance('USDC', D('1000'))
    cfg = config(execution_mode='paper', connector_name='okx_paper_trade')
    session = NativePaperSession(market, [cfg], Path(tmp_path) / 'momentum.sqlite')
    try:
        rows = []
        for snapshot in frames(cfg.recipe).values():
            frame = pd.DataFrame([{'timestamp': c.open_time, 'open': c.open, 'high': c.high, 'low': c.low,
                                   'close': c.close, 'volume': c.volume} for c in snapshot.rows])
            rows.append(dict(connector_name='okx', trading_pair='BNB-USDC', interval=snapshot.dataset.interval,
                             frame=frame, available_at=T + 1))
        result = await session.step(T + 1, candles=rows, books=[dict(trading_pair='BNB-USDC', sequence=1,
                                                                     timestamp=T + 1, bids=[(99, 10)], asks=[(101, 10)])])
        assert result['actions'] == []
        assert market.in_flight_orders == {}
        host = session.controllers[cfg.id]
        assert host.processed_data['data_health']['ready']
        assert host.get_custom_info()['momentum_fusion_v2']['observed_at'] == T + 1
    finally:
        await session.close()
        MarketsRecorder._shared_instance = prior
