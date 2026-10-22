import asyncio

from controllers.shared.hyperliquid_portfolio_signal_client import HyperliquidPortfolioSignalClient


class FrozenClock:
    def __init__(self, value=1_700_000_040.0):
        self.value = value

    def __call__(self):
        return self.value


def _response(**overrides):
    payload = {
        "schema": "rsibot.hyperliquid_portfolio_rsi.signal_store_snapshot.v1",
        "pair_id": "HYPE-USDC",
        "status": "fresh",
        "usable": True,
        "age_seconds": 1.0,
        "advisory": {
            "schema": "rsibot.hyperliquid_portfolio_rsi.advisory_signal.v1",
            "event_id": "event-1",
            "pair_id": "HYPE-USDC",
            "source": "quants-lab",
            "source_timestamp": "2023-11-14T22:14:00Z",
            "feature_cutoff": "2023-11-14T22:13:00Z",
            "buy_score": "0.75",
            "sell_score": "0",
            "metadata": {},
        },
    }
    payload.update(overrides)
    return payload


def test_client_returns_fresh_pair_matched_advisory_and_reuses_poll_cache():
    clock = FrozenClock()
    calls = []

    def fetcher(url, timeout):
        calls.append((url, timeout))
        return _response()

    client = HyperliquidPortfolioSignalClient(
        base_url="http://api:8000",
        poll_seconds=30,
        request_timeout_seconds=2,
        time_provider=clock,
        http_fetcher=fetcher,
    )

    first = asyncio.run(client.snapshot("HYPE-USDC"))
    second = asyncio.run(client.snapshot("HYPE-USDC"))

    assert first == second
    assert first["buy_score"] == "0.75"
    assert len(calls) == 1
    assert calls[0] == (
        "http://api:8000/portfolio/hyperliquid-rsi/signals/HYPE-USDC",
        2.0,
    )


def test_client_rejects_stale_unusable_or_pair_mismatched_response():
    cases = [
        _response(status="stale", usable=False),
        _response(usable=False),
        _response(pair_id="OTHER-USDC"),
        _response(advisory={"pair_id": "OTHER-USDC"}),
    ]
    for response in cases:
        client = HyperliquidPortfolioSignalClient(
            time_provider=FrozenClock(),
            http_fetcher=lambda url, timeout, response=response: response,
        )
        assert asyncio.run(client.snapshot("HYPE-USDC")) is None


def test_client_fails_closed_on_fetch_error_and_exposes_health():
    def failing_fetcher(url, timeout):
        raise TimeoutError("simulated timeout")

    client = HyperliquidPortfolioSignalClient(
        time_provider=FrozenClock(),
        http_fetcher=failing_fetcher,
    )

    assert asyncio.run(client.snapshot("HYPE-USDC")) is None
    health = client.status("HYPE-USDC")
    assert health["status"] == "error"
    assert health["last_error"] == "TimeoutError"


def test_disabled_client_never_calls_transport():
    called = False

    def fetcher(url, timeout):
        nonlocal called
        called = True
        return _response()

    client = HyperliquidPortfolioSignalClient(enabled=False, http_fetcher=fetcher)

    assert asyncio.run(client.snapshot("HYPE-USDC")) is None
    assert called is False
    assert client.status("HYPE-USDC")["status"] == "disabled"
