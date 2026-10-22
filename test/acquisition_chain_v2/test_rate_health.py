from decimal import Decimal
from unittest.mock import patch

import pytest

from hummingbot.data_feed.market_data_provider import MarketDataProvider


@pytest.mark.parametrize('quote,now,expected', [((Decimal('100'), 1000), 1066, 'stale'),
                                                (None, 1001, 'unavailable'),
                                                ((Decimal('100'), 1000), 999, 'stale'),
                                                ((Decimal('100'), 1000), 1001, 'ready')])
def test_managed_health_uses_same_quote_and_deadline_as_get_rate(quote, now, expected):
    provider = MarketDataProvider({})
    key = ('okx', 'BTC-USDT')
    provider._ticker_rates[key] = quote
    provider._rate_refresh_state[key] = dict(last_success=1000, price=Decimal('100'), error=None, failures=0)
    with patch('hummingbot.data_feed.market_data_provider.time.monotonic', return_value=now), \
            patch.object(provider, 'time', return_value=now):
        assert provider.get_rate_health()[0]['state'] == expected
        if expected == 'ready':
            assert provider.get_rate('BTC-USDT') == Decimal('100')
        else:
            with pytest.raises(ValueError):
                provider.get_rate('BTC-USDT')


def test_registered_ticker_before_first_refresh_is_reported_unavailable():
    provider = MarketDataProvider({})
    provider._ticker_rates[('okx', 'BTC-USDT')] = None
    health = provider.get_rate_health()
    assert len(health) == 1
    assert health[0]['state'] == 'unavailable'
    assert health[0]['age_seconds'] is None
    with pytest.raises(ValueError):
        provider.get_rate('BTC-USDT')
