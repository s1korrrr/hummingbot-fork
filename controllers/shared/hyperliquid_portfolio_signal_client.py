from __future__ import annotations

import asyncio
import base64
import json
import math
import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Mapping, Optional
from urllib import error as urllib_error, parse as urllib_parse, request as urllib_request

ADVISORY_SCHEMA = "rsibot.hyperliquid_portfolio_rsi.advisory_signal.v1"
DEFAULT_API_BASE_URL = os.environ.get("HUMMINGBOT_API_BASE_URL", "http://localhost:8000")
DEFAULT_USERNAME_ENV = "HUMMINGBOT_API_USERNAME"
DEFAULT_PASSWORD_ENV = "HUMMINGBOT_API_PASSWORD"

HttpFetcher = Callable[[str, float], Dict[str, Any]]


def _basic_auth_header(username: Optional[str], password: Optional[str]) -> Dict[str, str]:
    if not username or not password:
        return {}
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def _default_http_fetcher(
    url: str,
    timeout: float,
    *,
    username: Optional[str],
    password: Optional[str],
) -> Dict[str, Any]:
    request = urllib_request.Request(url, headers=_basic_auth_header(username, password))
    try:
        with urllib_request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - configured internal API
            status = getattr(response, "status", None) or response.getcode()
            if int(status or 0) // 100 != 2:
                raise RuntimeError(f"unexpected HTTP status {status}")
            body = response.read()
    except urllib_error.URLError as exc:
        raise RuntimeError(f"portfolio signal fetch failed: {exc}") from exc
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("portfolio signal fetch returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("portfolio signal fetch returned a non-object payload")
    return payload


class HyperliquidPortfolioSignalClient:
    """Timeout-bounded, cached, fail-closed reader for Hummingbot API signals."""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        poll_seconds: float = 15.0,
        max_age_seconds: float = 120.0,
        request_timeout_seconds: float = 2.0,
        max_future_skew_seconds: float = 5.0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        time_provider: Callable[[], float] = time.time,
        http_fetcher: Optional[HttpFetcher] = None,
        enabled: bool = True,
    ):
        self._base_url = (base_url or DEFAULT_API_BASE_URL).rstrip("/")
        self._poll_seconds = max(1.0, float(poll_seconds))
        self._max_age_seconds = max(self._poll_seconds, float(max_age_seconds))
        self._request_timeout_seconds = max(0.1, float(request_timeout_seconds))
        self._max_future_skew_seconds = max(0.0, float(max_future_skew_seconds))
        self._time_provider = time_provider
        self._enabled = bool(enabled)
        self._username = username if username is not None else os.environ.get(DEFAULT_USERNAME_ENV)
        self._password = password if password is not None else os.environ.get(DEFAULT_PASSWORD_ENV)
        self._http_fetcher = http_fetcher or (
            lambda url, timeout: _default_http_fetcher(
                url,
                timeout,
                username=self._username,
                password=self._password,
            )
        )
        self._lock = threading.RLock()
        self._cache: Dict[str, Optional[Dict[str, Any]]] = {}
        self._loaded_at: Dict[str, float] = {}
        self._health: Dict[str, Dict[str, Any]] = {}

    async def snapshot(self, pair_id: str) -> Optional[Mapping[str, Any]]:
        pair = self._normalize_pair(pair_id)
        now = float(self._time_provider())
        if not self._enabled:
            self._set_health(pair, status="disabled", last_error=None, now=now)
            return None
        with self._lock:
            loaded_at = self._loaded_at.get(pair)
            if loaded_at is not None and now - loaded_at < self._poll_seconds:
                cached = self._cache.get(pair)
                return dict(cached) if cached is not None else None
        url = self.url(pair)
        try:
            response = await asyncio.to_thread(
                self._http_fetcher,
                url,
                self._request_timeout_seconds,
            )
            advisory = self._validate_response(pair, response, now)
            status = "fresh" if advisory is not None else "invalid"
            self._set_cache(pair, advisory, loaded_at=now)
            self._set_health(pair, status=status, last_error=None, now=now)
            return dict(advisory) if advisory is not None else None
        except Exception as exc:
            self._set_cache(pair, None, loaded_at=now)
            self._set_health(pair, status="error", last_error=type(exc).__name__, now=now)
            return None

    def url(self, pair_id: str) -> str:
        pair = urllib_parse.quote(self._normalize_pair(pair_id), safe="-")
        return f"{self._base_url}/portfolio/hyperliquid-rsi/signals/{pair}"

    def status(self, pair_id: str) -> Dict[str, Any]:
        pair = self._normalize_pair(pair_id)
        with self._lock:
            health = dict(self._health.get(pair) or {})
        if not health:
            health = {
                "status": "disabled" if not self._enabled else "unknown",
                "last_error": None,
                "updated_at": None,
            }
        return {"pair_id": pair, **health}

    def _validate_response(
        self,
        pair: str,
        response: Any,
        now: float,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(response, Mapping):
            return None
        if response.get("status") != "fresh" or response.get("usable") is not True:
            return None
        if self._normalize_pair(response.get("pair_id")) != pair:
            return None
        advisory = response.get("advisory")
        if not isinstance(advisory, Mapping):
            return None
        if advisory.get("schema") != ADVISORY_SCHEMA:
            return None
        if self._normalize_pair(advisory.get("pair_id")) != pair:
            return None
        source_timestamp = self._parse_timestamp(advisory.get("source_timestamp"))
        feature_cutoff = self._parse_timestamp(advisory.get("feature_cutoff"))
        if source_timestamp is None or feature_cutoff is None or feature_cutoff > source_timestamp:
            return None
        age = now - source_timestamp
        if age < -self._max_future_skew_seconds or age > self._max_age_seconds:
            return None
        for score_name in ("buy_score", "sell_score"):
            score = self._finite_float(advisory.get(score_name))
            if score is None or not 0.0 <= score <= 1.0:
                return None
        if not str(advisory.get("event_id") or "").strip():
            return None
        return dict(advisory)

    def _set_cache(self, pair: str, advisory: Optional[Dict[str, Any]], *, loaded_at: float):
        with self._lock:
            self._cache[pair] = dict(advisory) if advisory is not None else None
            self._loaded_at[pair] = loaded_at

    def _set_health(self, pair: str, *, status: str, last_error: Optional[str], now: float):
        with self._lock:
            self._health[pair] = {
                "status": status,
                "last_error": last_error,
                "updated_at": now,
            }

    @staticmethod
    def _normalize_pair(value: Any) -> str:
        pair = str(value or "").strip().upper().replace("/", "-")
        if pair.count("-") != 1:
            raise ValueError("pair_id must use BASE-QUOTE format")
        return pair

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            try:
                return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
            except (TypeError, ValueError):
                return None
        while abs(numeric) >= 100_000_000_000:
            numeric /= 1_000.0
        return numeric if math.isfinite(numeric) else None

    @staticmethod
    def _finite_float(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric if math.isfinite(numeric) else None


__all__ = ["HyperliquidPortfolioSignalClient"]
