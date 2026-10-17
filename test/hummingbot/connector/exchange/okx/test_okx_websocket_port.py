"""OKX stops accepting WebSocket connections on port 8443 on 2026-10-31.

https://www.okx.com/en-us/help/okx-websocket-port-8443-discontinuation-announcement: remove the port so `wss://`
uses its default 443; host and path stay the same. Every OKX WebSocket endpoint the connectors and candle feeds
open must therefore carry no explicit port.
"""
import unittest
from urllib.parse import urlsplit

from hummingbot.connector.derivative.okx_perpetual import okx_perpetual_constants as perpetual
from hummingbot.connector.exchange.okx import okx_constants as spot
from hummingbot.data_feed.candles_feed.okx_perpetual_candles import constants as perpetual_candles
from hummingbot.data_feed.candles_feed.okx_spot_candles import constants as spot_candles


class OKXWebSocketPortTests(unittest.TestCase):
    def assert_default_port(self, url: str, host: str, path: str = ""):
        parts = urlsplit(url)
        self.assertEqual("wss", parts.scheme, url)
        self.assertIsNone(parts.port, url)
        self.assertEqual(host, parts.hostname, url)
        self.assertEqual(path, parts.path, url)

    def test_spot_websocket_base_urls_use_default_port_for_every_subdomain(self):
        expected = {"www": "ws.okx.com", "app": "wsus.okx.com", "my": "wseea.okx.com"}
        for sub_domain, host in expected.items():
            with self.subTest(sub_domain=sub_domain):
                self.assert_default_port(spot.get_ws_url(sub_domain), host)

    def test_spot_public_and_private_endpoints_keep_host_and_path(self):
        self.assert_default_port(spot.get_okx_ws_uri_public("www"), "ws.okx.com", "/ws/v5/public")
        self.assert_default_port(spot.get_okx_ws_uri_private("www"), "ws.okx.com", "/ws/v5/private")

    def test_perpetual_websocket_urls_use_default_port(self):
        for table, channel in ((perpetual.WSS_PUBLIC_URLS, "public"), (perpetual.WSS_PRIVATE_URLS, "private"),
                               (perpetual.WSS_BUSINESS_URLS, "business")):
            for domain, url in table.items():
                with self.subTest(domain=domain, channel=channel):
                    parts = urlsplit(url)
                    self.assertIsNone(parts.port, url)
                    self.assertEqual(f"/ws/v5/{channel}", parts.path, url)

    def test_candle_feeds_use_default_port(self):
        for url in (spot_candles.WSS_URL, perpetual_candles.WSS_URL):
            with self.subTest(url=url):
                self.assert_default_port(url, "ws.okx.com", "/ws/v5/business")
