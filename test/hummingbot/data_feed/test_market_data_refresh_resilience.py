import asyncio
from decimal import Decimal

import pytest

from hummingbot.data_feed.market_data_provider import MarketDataProvider


class PriceConnector:
    name = 'okx'

    def __init__(self, prices):
        self.prices = prices
        self.pending = set()

    async def _get_last_traded_price(self, trading_pair):
        value = self.prices[trading_pair]
        if value == 'slow':
            self.pending.add(trading_pair)
            try:
                await asyncio.sleep(10)
            finally:
                self.pending.discard(trading_pair)
        if isinstance(value, Exception):
            raise value
        return value


@pytest.mark.asyncio
async def test_slow_pair_does_not_discard_successful_prices():
    provider = MarketDataProvider({})
    connector = PriceConnector({'BTC-USDT': '100', 'ETH-USDT': 'slow'})
    result = await provider._safe_get_last_traded_prices(connector, list(connector.prices), timeout=0.01)
    assert result == {'BTC-USDT': Decimal('100')}
    assert not connector.pending


@pytest.mark.asyncio
@pytest.mark.parametrize('invalid', [RuntimeError('unavailable'), 'NaN', 'Infinity', '-1', '0', True])
async def test_invalid_or_failed_prices_are_never_returned_as_rates(invalid):
    provider = MarketDataProvider({})
    connector = PriceConnector({'BTC-USDT': '100', 'ETH-USDT': invalid})
    result = await provider._safe_get_last_traded_prices(connector, list(connector.prices))
    assert result == {'BTC-USDT': Decimal('100')}


@pytest.mark.asyncio
async def test_batch_cancellation_drains_all_pair_requests():
    provider = MarketDataProvider({})
    connector = PriceConnector({'BTC-USDT': 'slow', 'ETH-USDT': 'slow'})
    task = asyncio.create_task(provider._safe_get_last_traded_prices(connector, list(connector.prices)))
    try:
        while len(connector.pending) != 2:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not connector.pending
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cached_rate_expires_and_recovers_without_zero_substitution(monkeypatch):
    provider = MarketDataProvider({}, rates_update_interval=60)
    now = [1000.0]
    monkeypatch.setattr(provider, 'time', lambda: now[0])
    connector = PriceConnector({'BTC-USDT': '100'})
    await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
    assert provider.get_rate('BTC-USDT') == Decimal('100')
    connector.prices['BTC-USDT'] = TimeoutError()
    now[0] += 60
    await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
    assert provider.get_rate_health()[0]['state'] == 'degraded'
    assert provider.get_rate('BTC-USDT') == Decimal('100')
    now[0] += 61
    assert provider.get_rate_health()[0]['state'] == 'stale'
    with pytest.raises(ValueError, match='unavailable or stale'):
        provider.get_rate('BTC-USDT')
    connector.prices['BTC-USDT'] = '105'
    await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
    assert provider.get_rate('BTC-USDT') == Decimal('105')
    assert provider.get_rate_health()[0]['state'] == 'ready'


@pytest.mark.asyncio
async def test_failure_without_history_is_unavailable_and_clock_rollback_is_rejected(monkeypatch):
    provider = MarketDataProvider({})
    now = [1000.0]
    monkeypatch.setattr(provider, 'time', lambda: now[0])
    connector = PriceConnector({'BTC-USDT': TimeoutError()})
    await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
    assert provider.get_rate_health()[0]['state'] == 'unavailable'
    with pytest.raises(ValueError, match='unavailable or stale'):
        provider.get_rate('BTC-USDT')
    connector.prices['BTC-USDT'] = '100'
    await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
    now[0] -= 1
    with pytest.raises(ValueError, match='unavailable or stale'):
        provider.get_rate('BTC-USDT')


@pytest.mark.asyncio
async def test_identical_refresh_failures_log_once_and_recovery_is_visible(caplog):
    import logging
    provider = MarketDataProvider({})
    connector = PriceConnector({'BTC-USDT': TimeoutError()})
    with caplog.at_level(logging.INFO):
        await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
        await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
        connector.prices['BTC-USDT'] = '100'
        await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
    assert sum('Price refresh incomplete' in r.message for r in caplog.records) == 1
    assert any('Price refresh recovered' in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fast_pair_age_is_not_reset_when_slow_sibling_finishes(monkeypatch):
    provider = MarketDataProvider({})
    now = [1000.0]
    monkeypatch.setattr(provider, 'time', lambda: now[0])

    class DelayedSibling(PriceConnector):
        async def _get_last_traded_price(self, trading_pair):
            if trading_pair == 'BTC-USDT':
                return '100'
            await asyncio.sleep(0.01)
            now[0] = 1005.0
            raise TimeoutError()

    await provider._safe_get_last_traded_prices(DelayedSibling({}), ['BTC-USDT', 'ETH-USDT'])
    btc = next(row for row in provider.get_rate_health() if row['trading_pair'] == 'BTC-USDT')
    assert btc['age_seconds'] == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("pair, expected", [("USDT-BTC", "0.01"), ("BTC-ETH", "2")])
async def test_inverse_and_indirect_managed_rates_use_same_freshness_contract(monkeypatch, pair, expected):
    from hummingbot.core.rate_oracle.rate_oracle import RateOracle

    provider = MarketDataProvider({}, rates_update_interval=60)
    now = [1000.0]
    monkeypatch.setattr(provider, 'time', lambda: now[0])
    connector = PriceConnector({'BTC-USDT': '100'})
    oracle = RateOracle()
    oracle.set_price('BTC-USDT', Decimal('50'))
    oracle.set_price('ETH-USDT', Decimal('50'))
    oracle.set_price('USDT-BTC', Decimal('0.02'))
    monkeypatch.setattr(RateOracle, 'get_instance', lambda: oracle)
    await provider._safe_get_last_traded_prices(connector, ['BTC-USDT'])
    assert provider.get_rate(pair) == Decimal(expected)
    now[0] += 121
    with pytest.raises(ValueError, match='unavailable or stale'):
        provider.get_rate(pair)
    assert oracle.prices['BTC-USDT'] == Decimal('50')
