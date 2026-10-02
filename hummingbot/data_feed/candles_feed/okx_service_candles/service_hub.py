"""One process-wide reader of the shared OKX candle service for V1's candle feeds.

Hummingbot's `MarketDataProvider` creates one candle feed per controller. Every `OKXServiceCandles` of a process shares
the single `CandleStreamClient` owned here, so the five V1 controllers hold one stream, not five. The hub never opens a
public OKX connection and never falls back to one: when the service is unavailable the feeds report why and serve no
rows, exactly as the native feed serves none while its websocket is down.
"""
import asyncio
import logging
import re
import time
from typing import Callable, Dict, FrozenSet, Optional, Tuple

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.candles_feed.okx_service_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.components.candle_data import CandleSnapshot, Dataset
from hummingbot.strategy_v2.components.candle_stream import CandleStreamClient
from hummingbot.strategy_v2.components.candle_transport import validate_origin
from hummingbot.strategy_v2.components.data_reader import DataPolicy
from hummingbot.strategy_v2.components.domain import PolicyError

# The stream client admits OKX spot USDC pairs only (candle_stream.CandleStreamClient).
PAIR_PATTERN = re.compile(r"^[A-Z0-9]+-USDC$")


class ServiceCandleHub:
    """Owns the shared stream client; rebuilds it only when the subscribed pair set grows."""

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, policy: DataPolicy, *, history: int = CONSTANTS.SERVICE_HISTORY,
                 clock: Callable[[], float] = time.time, client_factory: Callable = CandleStreamClient,
                 coalesce_seconds: float = CONSTANTS.COALESCE_SECONDS,
                 idle_close_seconds: float = CONSTANTS.IDLE_CLOSE_SECONDS):
        if (policy.mode != "shared" or policy.transport != "stream" or policy.source_profile != "native"
                or policy.symbols):
            raise PolicyError("service candles require data mode shared, transport stream, native profile, no symbols")
        if type(history) is not int or not 60 <= history <= CONSTANTS.SERVICE_HISTORY:
            raise PolicyError(f"service candle history must be an integer from 60 to {CONSTANTS.SERVICE_HISTORY}")
        self.policy = policy
        self.origin = validate_origin(policy.origin)
        self.token_file = policy.token_file
        self.history = history
        self.clock = clock
        self.coalesce_seconds = coalesce_seconds
        self.idle_close_seconds = idle_close_seconds
        self._client_factory = client_factory
        self.client = None
        self.error: Optional[str] = None
        self.builds = 0
        self._client_pairs: FrozenSet[str] = frozenset()
        self._pairs = set()
        self._refs: Dict[str, int] = {}
        self._wake = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._idle_handle = None

    @property
    def key(self) -> Tuple[str, str]:
        return self.origin, self.token_file

    # ------------------------------------------------------------------ feed registration

    @staticmethod
    def validate_pair(trading_pair: str) -> str:
        if not isinstance(trading_pair, str) or PAIR_PATTERN.fullmatch(trading_pair) is None:
            raise PolicyError(f"the shared candle service serves OKX spot USDC pairs only, not {trading_pair!r}")
        return trading_pair

    def expect(self, trading_pair: str) -> None:
        """Declare a pair a controller will read, so the first stream build subscribes all of them at once."""
        self._pairs.add(self.validate_pair(trading_pair))

    def attach(self, trading_pair: str) -> None:
        self.validate_pair(trading_pair)
        self._cancel_idle()
        self._refs[trading_pair] = self._refs.get(trading_pair, 0) + 1
        self._pairs.add(trading_pair)
        if self._task is None or self._task.done():
            self._task = safe_ensure_future(self._supervise())
        self._wake.set()

    def detach(self, trading_pair: str) -> None:
        count = self._refs.get(trading_pair, 0) - 1
        if count > 0:
            self._refs[trading_pair] = count
            return
        self._refs.pop(trading_pair, None)
        if self._refs:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._pairs.clear()
            return
        self._cancel_idle()
        self._idle_handle = loop.call_later(self.idle_close_seconds, self._idle_expired)

    def _cancel_idle(self) -> None:
        handle, self._idle_handle = self._idle_handle, None
        if handle is not None:
            handle.cancel()

    def _idle_expired(self) -> None:
        self._idle_handle = None
        if not self._refs:
            self._pairs.clear()
            self._wake.set()

    # ------------------------------------------------------------------ stream ownership

    async def _supervise(self) -> None:
        while True:
            await self._wake.wait()
            await asyncio.sleep(self.coalesce_seconds)
            self._wake.clear()
            await self._reconcile()

    async def _close_client(self) -> None:
        client, self.client = self.client, None
        self._client_pairs = frozenset()
        if client is not None:
            await client.close()

    async def _reconcile(self) -> None:
        wanted = frozenset(self._pairs)
        if not wanted:
            await self._close_client()
            return
        if self.client is not None and self._client_pairs == wanted:
            return
        await self._close_client()
        try:
            specs = tuple(Dataset(connector="okx", trading_pair=pair, interval="1m", history=self.history,
                                  source_profile=self.policy.source_profile) for pair in sorted(wanted))
            client = self._client_factory(self.origin, self.token_file, specs, clock=self.clock)
            client.start()
        except (ValueError, TypeError, OSError) as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            self.logger().error(f"Service candle client could not start | origin={self.origin} error={self.error}")
            return
        self.client = client
        self._client_pairs = wanted
        self.error = None
        self.builds += 1
        self.logger().info(
            f"Service candle stream started | origin={self.origin} pairs={sorted(wanted)} history={self.history} "
            f"build={self.builds}")

    async def close(self) -> None:
        """Stop the stream and the supervisor; used by tests and orderly process shutdown."""
        self._cancel_idle()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._refs.clear()
        self._pairs.clear()
        await self._close_client()

    # ------------------------------------------------------------------ reads

    def view(self, trading_pair: str, now: Optional[float] = None) -> Tuple[Optional[CandleSnapshot], Optional[str]]:
        """Return (snapshot, None) when the pair is served now, else (None, the reason it is not)."""
        now = self.clock() if now is None else now
        if self.error is not None:
            return None, f"service candle client unavailable: {self.error}"
        client = self.client
        if client is None or trading_pair not in self._client_pairs:
            return None, "service candle stream is starting"
        replica = client.replica
        if not replica.connected:
            detail = f" ({client.error})" if client.error else ""
            return None, f"service candle stream disconnected{detail}"
        spec = client.by_pair.get(trading_pair)
        snapshot = None if spec is None else replica.snapshots.get(spec.key)
        if snapshot is None:
            reported = None if spec is None else client.dataset_errors.get(spec.key)
            return None, (f"service reports candles unavailable: {reported}" if reported
                          else "service candle snapshot not yet received")
        if not 0 <= now - snapshot.published_at <= replica.max_transport_age:
            return None, "service candle transport is stale"
        return snapshot, None

    def status(self) -> dict:
        client = self.client
        return {"origin": self.origin, "history": self.history, "pairs": sorted(self._pairs), "builds": self.builds,
                "error": self.error, "sessions": getattr(client, "sessions", None),
                "failures": getattr(client, "failures", None), "client_error": getattr(client, "error", None)}


_HUB: Optional[ServiceCandleHub] = None


def configure_service_candles(policy: DataPolicy, *, history: int = CONSTANTS.SERVICE_HISTORY,
                              **options) -> ServiceCandleHub:
    """Opt this process into the shared candle service and route the OKX spot feed selector to it.

    Idempotent for an identical configuration; a conflicting one is an error because one process holds one stream.
    """
    global _HUB
    from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
    hub = ServiceCandleHub(policy, history=history, **options)
    if _HUB is not None:
        if _HUB.key != hub.key or _HUB.history != hub.history:
            raise PolicyError("conflicting service candle configuration in one process")
        return _HUB
    _HUB = hub
    CandlesFactory.route("okx", CONSTANTS.SERVICE_CONNECTOR)
    return hub


def get_service_hub() -> ServiceCandleHub:
    if _HUB is None:
        raise PolicyError("okx_service candles require a configured shared candle service (data.mode shared)")
    return _HUB


def reset_service_candles() -> None:
    """Drop the process-wide hub and the factory route (tests; the process owns no other lifecycle)."""
    global _HUB
    from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
    hub, _HUB = _HUB, None
    CandlesFactory.unroute("okx")
    if hub is not None:
        hub._cancel_idle()
        if hub._task is not None:
            hub._task.cancel()
            hub._task = None
        client, hub.client = hub.client, None
        task = getattr(client, "task", None)
        if task is not None:
            task.cancel()
