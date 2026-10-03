import logging
from typing import Optional

import pandas as pd

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.okx_service_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.components.domain import PolicyError


class OKXServiceCandles(CandlesBase):
    """Confirmed OKX spot 1m candles read from the shared candle service instead of OKX's public websocket.

    Serves the frame `OKXSpotCandles.candles_df` serves: the same ten float columns, oldest first, confirmed bars only
    (the service never publishes a forming bar, as the native parsers admit exchange confirm=1 rows only). It never
    opens a public OKX connection: REST backfill, websocket and historical fetches are refused. While the service
    stream is down, stale or restarting the feed is not ready, serves an empty frame (the native feed's state while its
    websocket is down) and logs the reason once per change.
    """

    closed_candles_only = True

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        from hummingbot.data_feed.candles_feed.okx_service_candles.service_hub import ServiceCandleHub, get_service_hub
        hub = get_service_hub()
        if interval not in CONSTANTS.INTERVALS:
            raise PolicyError(f"the shared candle service serves 1m candles only, not {interval!r}")
        if type(max_records) is not int or not 1 <= max_records <= hub.history:
            raise PolicyError(
                f"service candle max_records must be an integer from 1 to {hub.history}, not {max_records!r}")
        ServiceCandleHub.validate_pair(trading_pair)
        self._hub = hub
        self._attached = False
        self._last_reason: Optional[str] = None
        self._frame_cache = None
        super().__init__(trading_pair, interval, max_records)

    # ------------------------------------------------------------------ surface used by MarketDataProvider

    @property
    def name(self):
        return f"okx_service_{self._trading_pair}"

    @property
    def rate_limits(self):
        return []

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    def get_exchange_trading_pair(self, trading_pair):
        return trading_pair

    def set_throttler(self, throttler) -> None:
        """No exchange request budget exists: this feed makes no public OKX request."""

    def start(self):
        super().start()
        if not self._attached:
            self._hub.attach(self._trading_pair)
            self._attached = True

    def stop(self):
        super().stop()
        if self._attached:
            self._attached = False
            self._hub.detach(self._trading_pair)

    async def start_network(self):
        """The base class would open OKX's public websocket here; the shared stream is owned by the hub."""

    async def stop_network(self):
        """Nothing to stop: this feed owns no connection."""

    async def check_network(self) -> NetworkStatus:
        snapshot, _ = self._hub.view(self._trading_pair)
        return NetworkStatus.CONNECTED if snapshot is not None else NetworkStatus.NOT_CONNECTED

    @property
    def ready(self):
        if not self._attached:
            return False
        snapshot, reason = self._hub.view(self._trading_pair)
        self._observe(reason)
        return snapshot is not None

    @property
    def unavailable_reason(self) -> Optional[str]:
        """Why no rows are served right now; None while the shared stream serves this pair."""
        if not self._attached:
            return "service candle feed is not started"
        return self._hub.view(self._trading_pair)[1]

    @property
    def candles_df(self) -> pd.DataFrame:
        snapshot, reason = None, "service candle feed is not started"
        if self._attached:
            snapshot, reason = self._hub.view(self._trading_pair)
        self._observe(reason)
        if snapshot is None:
            return pd.DataFrame([], columns=self.columns, dtype=float)
        cached = self._frame_cache
        if cached is not None and cached[0] is snapshot.rows and cached[1] == self.max_records:
            return cached[2].copy()
        rows = snapshot.rows[-self.max_records:]
        if any(row.quote_volume is None for row in rows):
            self._observe("service candle rows lack quote volume")
            return pd.DataFrame([], columns=self.columns, dtype=float)
        frame = pd.DataFrame(
            [[float(row.open_time), float(row.open), float(row.high), float(row.low), float(row.close),
              float(row.volume), float(row.quote_volume), 0., 0., 0.] for row in rows],
            columns=self.columns, dtype=float)
        self._frame_cache = (snapshot.rows, self.max_records, frame)
        return frame.copy()

    def service_status(self) -> dict:
        """Read-only health view: the shared stream state as this pair sees it."""
        snapshot, reason = self._hub.view(self._trading_pair) if self._attached else (None, self.unavailable_reason)
        last = None if snapshot is None else snapshot.rows[-1].open_time
        return {"feed": self.name, "ready": snapshot is not None, "reason": reason, "last_open_time": last,
                "published_at": None if snapshot is None else snapshot.published_at,
                "hub": self._hub.status()}

    def _observe(self, reason: Optional[str]) -> None:
        if reason == self._last_reason:
            return
        previous, self._last_reason = self._last_reason, reason
        if reason is None:
            self.logger().info(f"Service candle feed available | feed={self.name} interval={self.interval}")
        else:
            self.logger().warning(
                f"Service candle feed unavailable | feed={self.name} interval={self.interval} reason={reason}"
                + ("" if previous is None else f" previous={previous}"))

    # ------------------------------------------------------------------ nothing here may reach OKX

    @staticmethod
    def _refused(operation: str):
        raise PolicyError(f"{operation} is refused: the service candle feed never contacts OKX; the shared candle "
                          "service is its only source")

    async def fetch_candles(self, *args, **kwargs):
        self._refused("fetch_candles")

    async def get_historical_candles(self, *args, **kwargs):
        self._refused("get_historical_candles")

    async def fill_historical_candles(self):
        self._refused("fill_historical_candles")

    async def listen_for_subscriptions(self):
        self._refused("listen_for_subscriptions")

    def load_candles_from_csv(self, data_path: str):
        self._refused("load_candles_from_csv")
