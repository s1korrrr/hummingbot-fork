"""Public-data, native spot paper runtime. Never constructs an exchange client.

Hosts either the component_breakout reference on explicit pairs, or a paper twin of live rsi_modular controllers
built from their mounted live YAML files. A twin differs from its live controller only in its declared fields:
a separate owner id, paper execution, the native paper connector, the explicit public OKX candle owner and
injected (native) candles taken from the same candle service instead of the shared stream.

Books are observed five-level snapshots; fills use the native paper connector's
explicit participation assumption. Queue priority and venue execution equivalence
are not established. A stale book holds the complete configured portfolio.
"""

import argparse
import asyncio
import fcntl
import hashlib
import json
import math
import os
import signal
import sys
import tempfile
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Literal

import aiohttp
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from controllers.generic.component_breakout import ComponentBreakoutConfig
from hummingbot.connector.exchange.okx import okx_constants as OKX, okx_web_utils
from hummingbot.connector.exchange.okx.okx_api_order_book_data_source import OkxAPIOrderBookDataSource
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.strategy_v2.components.candle_data import Dataset
from hummingbot.strategy_v2.components.candle_stream import CandleStreamClient
from hummingbot.strategy_v2.components.domain import DataUnavailable
from hummingbot.strategy_v2.paper_session import NativePaperSession


class PaperConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bot_name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$")
    pairs: tuple[str, ...] = Field(min_length=1, max_length=1000)
    state_path: Path
    status_path: Path
    candle_origin: str
    candle_token_file: Path | None = None
    simulated_quote: Decimal = Field(default=Decimal("5000"), gt=0, le=1000000)
    position_quote: Decimal = Field(default=Decimal("25"), gt=0, le=50)
    maker_fee: Decimal = Field(default=Decimal(".001"), ge=0, le=Decimal(".05"))
    taker_fee: Decimal = Field(default=Decimal(".001"), ge=0, le=Decimal(".05"))
    trade_participation: Decimal = Field(default=Decimal(".1"), gt=0, le=1)
    book_max_age_seconds: float = Field(default=5, gt=0, le=5)
    controller_kind: Literal["component_breakout", "rsi_modular", "modular_spot_research",
                             "meridian_v3_unified", "meridian_v4"] = "component_breakout"
    controller_files: tuple[Path, ...] = ()

    @field_validator("pairs")
    @classmethod
    def pairs_valid(cls, value):
        import re

        if len(set(value)) != len(value) or any(
            not re.fullmatch(r"[A-Z0-9]+-USDC", p) or p == "USDC-USDC" for p in value
        ):
            raise ValueError("Distinct explicit OKX USDC spot pairs required")
        return value

    @model_validator(mode="after")
    def paths_valid(self):
        if (
            not self.state_path.is_absolute()
            or not self.status_path.is_absolute()
            or self.state_path == self.status_path
        ):
            raise ValueError("Separate absolute state and status paths required")
        if self.position_quote > self.simulated_quote:
            raise ValueError("Position allocation exceeds simulated cash")
        if (self.controller_kind != "component_breakout") != bool(self.controller_files):
            raise ValueError("controller_files are required except for component_breakout, which forbids them")
        if any(not f.is_absolute() for f in self.controller_files) or len(set(self.controller_files)) != len(self.controller_files):
            raise ValueError("Distinct absolute controller_files required")
        if self.controller_kind == "meridian_v3_unified" and len(self.controller_files) != 1:
            raise ValueError("Meridian unified is one portfolio controller file")
        return self


def owned_pairs(config):
    """One declared pair, or every pair one multi-symbol owner declares."""
    owned = getattr(config, "owned_trading_pairs", None)
    return tuple(owned()) if callable(owned) else (config.trading_pair,)


def json_value(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, (Decimal, Path)):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    raise TypeError(type(value).__name__)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as output:
            json.dump(
                value, output, default=json_value, sort_keys=True, allow_nan=False
            )
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


TWIN_FIELDS = ("id", "execution_mode", "connector_name", "candles_connector", "data", "shared_history")


def rsi_modular_paper_configs(paths, pairs):
    """Paper twins of live rsi_modular controller files; returns configs and the per-controller divergence."""
    import yaml

    from controllers.generic.rsi_modular import get_config_class

    configs, divergence = [], {}
    for path in paths:
        live = yaml.safe_load(Path(path).read_text())
        if (not isinstance(live, dict) or live.get("controller_name") != "rsi_modular"
                or live.get("execution_mode") != "live" or live.get("connector_name") != "okx"
                or not str(live.get("id", "")).startswith("modular_spot_") or live.get("profile") not in ("ok_rsi", "rsi_v5")):
            raise ValueError(f"Paper twin requires a live rsi_modular controller file: {path}")
        cls = get_config_class(live["profile"])
        reference = cls.model_validate(live).model_dump(mode="json")
        paper = {k: v for k, v in live.items() if k not in ("data", "shared_history")}
        paper.update(id="modular_spot_paper_" + live["id"][len("modular_spot_"):], execution_mode="paper",
                     connector_name="okx_paper_trade", candles_connector="okx")
        config = cls.model_validate(paper)
        if config.data.mode != "native":
            raise ValueError("Paper twin candles must be injected")
        values = config.model_dump(mode="json")
        changed = {k: [reference.get(k), values.get(k)] for k in set(reference) | set(values)
                   if reference.get(k) != values.get(k)}
        if not set(changed) <= set(TWIN_FIELDS):
            raise ValueError(f"Paper twin diverges beyond its declared fields: {sorted(set(changed) - set(TWIN_FIELDS))}")
        configs.append(config)
        divergence[config.id] = changed
    if sorted(c.trading_pair for c in configs) != sorted(pairs):
        raise ValueError("Paper twin controller pairs must equal the configured pairs")
    return configs, divergence


def modular_spot_research_paper_configs(paths, pairs):
    """Research-bound modular_spot paper controllers (okx_paper_trade, injected 1m candles) from YAML files."""
    import yaml

    from controllers.generic.modular_spot_research import ModularSpotResearchConfig

    configs = []
    for path in paths:
        values = yaml.safe_load(Path(path).read_text())
        if not isinstance(values, dict) or values.get("controller_name") != "modular_spot_research":
            raise ValueError(f"modular_spot_research paper requires a modular_spot_research controller file: {path}")
        config = ModularSpotResearchConfig.model_validate(values)
        if config.connector_name != "okx_paper_trade" or config.data.mode != "native" or config.interval_seconds() != 60:
            raise ValueError("modular_spot_research paper needs okx_paper_trade, injected native 1m candles")
        configs.append(config)
    if sorted(c.trading_pair for c in configs) != sorted(pairs):
        raise ValueError("Paper controller pairs must equal the configured pairs")
    return configs


def parse_rules(payload, pairs):
    if payload.get("code") != "0" or not isinstance(payload.get("data"), list):
        raise ValueError("Public instrument metadata unavailable")
    rows = {}
    for row in payload["data"]:
        pair = row.get("instId")
        if pair not in pairs:
            continue
        if (
            pair in rows
            or row.get("state") != "live"
            or row.get("instType") != "SPOT"
            or row.get("quoteCcy") != "USDC"
        ):
            raise ValueError("Pair is duplicate, non-live, or not USDC spot")
        values = [Decimal(row[k]) for k in ("minSz", "tickSz", "lotSz")]
        if any(not v.is_finite() or v <= 0 for v in values):
            raise ValueError("Invalid public trading rules")
        rows[pair] = TradingRule(
            pair,
            min_order_size=values[0],
            min_price_increment=values[1],
            min_base_amount_increment=values[2],
        )
    if set(rows) != set(pairs):
        raise ValueError("Configured pair missing from current public instruments")
    return rows


async def public_rules(pairs):
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20), trust_env=False
    ) as client:
        async with client.get(
            OKX.DEFAULT_DOMAIN.rstrip("/") + OKX.OKX_INSTRUMENTS_PATH,
            params={"instType": "SPOT"},
            allow_redirects=False,
        ) as response:
            response.raise_for_status()
            return parse_rules(await response.json(), pairs)


class PublicSymbols:
    """Only the native parser's symbol lookup capability; no trading client."""

    domain = OKX.DEFAULT_DOMAIN
    okx_registration_sub_domain = "www"

    def __init__(self, pairs):
        self.pairs = frozenset(pairs)

    async def trading_pair_associated_to_exchange_symbol(self, symbol):
        if symbol not in self.pairs:
            raise ValueError("Unsubscribed public instrument")
        return symbol

    async def exchange_symbol_associated_to_pair(self, trading_pair):
        return await self.trading_pair_associated_to_exchange_symbol(trading_pair)


def books5_snapshot(payload, pair):
    """One OKX books5 frame as one native snapshot message.

    books5 pushes a complete five-level book in every frame (no `action`, no `prevSeqId`), so
    no stream continuity applies. The OKX data source's book parser serves the incremental
    `books` channel and refuses frames without that continuity, so it is not used here. Book
    contents are validated afterwards by the native paper connector's book validator.
    """
    (data,) = payload["data"]
    update_id = int(data["ts"])
    return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
        "trading_pair": pair, "update_id": update_id,
        "bids": [(row[0], row[1]) for row in data["bids"]],
        "asks": [(row[0], row[1]) for row in data["asks"]],
    }, update_id * 1e-3)


class PublicLiquidity:
    """Bounded public snapshots/trades: books5 frames as native snapshots, trades by Hummingbot's OKX datasource."""
    ACK_TIMEOUT_SECONDS = 15.0

    def __init__(self, pairs, max_age=5, *, book_validator=None):
        self.pairs = tuple(pairs)
        self.max_age = max_age
        self.book_validator = book_validator
        self.factory = (
            okx_web_utils.build_api_factory_without_time_synchronizer_pre_processor(
                okx_web_utils.create_throttler()
            )
        )
        self.parser = OkxAPIOrderBookDataSource(
            list(pairs), PublicSymbols(pairs), self.factory
        )
        self.books = {}
        self.trades = deque()
        self.tasks = []
        self.failures = 0
        self.messages = 0
        self.stale_inputs = 0
        self.fatal = None
        self._trade_ids = {}
        self._book_sequences = {}
        self.acknowledged = set()
        self.recent_errors = deque(maxlen=8)
        self.failures_by_stage = {}
        self.book_rejections = 0
        self.invalid_book_reasons = {}
        self.received_at = {}
        self.latencies = {channel: deque(maxlen=1024) for channel in ('books5', 'trades')}
        self.latency_totals = {channel: {'count': 0, 'sum': 0.0, 'max': 0.0} for channel in self.latencies}

    def latency_summary(self):
        result = {}
        for channel, samples in self.latencies.items():
            total = self.latency_totals[channel]
            ordered = sorted(samples)
            result[channel] = {'count': total['count'], 'mean_seconds': total['sum'] / total['count'] if total['count'] else None,
                               'max_seconds': total['max'] if total['count'] else None,
                               'rolling_p95_seconds': ordered[max(0, math.ceil(len(ordered) * .95) - 1)] if ordered else None,
                               'rolling_samples': len(ordered)}
        return result

    def record_error(self, stage, error, pairs):
        self.failures += 1
        self.failures_by_stage[stage] = self.failures_by_stage.get(stage, 0) + 1
        self.recent_errors.append({'stage': stage, 'error_type': type(error).__name__,
                                   'reason': str(error).replace('\n', ' ')[:240],
                                   'first_pair': pairs[0], 'pair_count': len(pairs), 'observed_at': time.time()})

    def reject_book(self, pair, error):
        self.book_rejections += 1
        self.invalid_book_reasons[pair] = str(error).replace('\n', ' ')[:240]
        self.books.pop(pair, None)
        self.trades = deque(t for t in self.trades if t['trading_pair'] != pair)

    def subscribed(self, pair):
        return all((pair, channel) in self.acknowledged for channel in ('books5', 'trades'))

    def disconnect_pairs(self, pairs):
        for pair in pairs:
            self.books.pop(pair, None)
            self.acknowledged.difference_update((pair, channel) for channel in ('books5', 'trades'))

    async def accept(self, payload, now):
        if payload.get("event") == "error":
            raise ValueError('Public subscription rejected; code=' + str(payload.get('code', 'unknown'))[:32])
        if payload.get('event') == 'subscribe':
            arg = payload['arg']
            pair = await self.parser._connector.trading_pair_associated_to_exchange_symbol(arg['instId'])
            if arg['channel'] not in ('books5', 'trades') or payload.get('code', '0') != '0':
                raise ValueError('Invalid public subscription acknowledgement')
            self.acknowledged.add((pair, arg['channel']))
            return
        if "data" not in payload:
            return
        arg = payload["arg"]
        pair = await self.parser._connector.trading_pair_associated_to_exchange_symbol(
            arg["instId"]
        )
        channel = arg["channel"]
        if channel not in ("books5", "trades"):
            raise ValueError("Unexpected public channel")
        if (pair, channel) not in self.acknowledged:
            raise ValueError('Public data before subscription acknowledgement')
        if channel == 'trades' and any(row.get('instId') != pair for row in payload['data']):
            raise ValueError('Trade instrument differs from acknowledged channel')
        queue = asyncio.Queue(maxsize=1024)
        if channel == "books5":
            # OKX books5 is a complete snapshot channel. Unlike books, its
            # observed public messages omit action entirely.
            if payload.get("action") not in (None, "snapshot"):
                raise ValueError("Five-level snapshot required")
            if len(payload['data']) != 1 or payload['data'][0].get('instId', pair) != pair:
                raise ValueError('Snapshot instrument differs from acknowledged channel')
            queue.put_nowait(books5_snapshot(payload, pair))
        else:
            await self.parser._parse_trade_message(payload, queue)
        while not queue.empty():
            message = queue.get_nowait()
            timestamp = float(message.timestamp)
            if not math.isfinite(timestamp) or not 0 <= now - timestamp <= self.max_age:
                self.stale_inputs += 1
                continue
            if channel == "books5":
                sequence = int(payload["data"][0]["ts"])
                if sequence <= self._book_sequences.get(pair, -1):
                    continue
                self._book_sequences[pair] = sequence
                candidate = {
                    "trading_pair": pair,
                    "sequence": sequence,
                    "timestamp": timestamp,
                    "bids": [(row.price, row.amount) for row in message.bids],
                    "asks": [(row.price, row.amount) for row in message.asks],
                }
                if self.book_validator is not None:
                    try:
                        self.book_validator(**candidate, current_timestamp=now,
                                            previous_sequence=-1, previous_timestamp=0)
                    except ValueError as error:
                        self.reject_book(pair, error)
                        continue
                self.books[pair] = candidate
                self.invalid_book_reasons.pop(pair, None)
            else:
                sequence = int(message.trade_id)
                if sequence <= self._trade_ids.get(pair, -1):
                    continue
                if len(self.trades) >= 4096:
                    self.fatal = "public_trade_queue_overflow"
                    raise RuntimeError(self.fatal)
                self._trade_ids[pair] = sequence
                self.trades.append(
                    {
                        "trading_pair": pair,
                        "sequence": sequence,
                        "timestamp": timestamp,
                        "side": TradeType(int(message.content["trade_type"])),
                        "price": message.content["price"],
                        "amount": message.content["amount"],
                    }
                )
            self.messages += 1
            delay = now - timestamp
            self.received_at[(pair, channel)] = now
            self.latencies[channel].append(delay)
            total = self.latency_totals[channel]
            total['count'] += 1
            total['sum'] += delay
            total['max'] = max(total['max'], delay)

    async def _run(self, pairs):
        backoff = 1
        while not self.fatal:
            ws = None
            stage = 'connect'
            try:
                ws = await self.factory.get_ws_assistant()
                async with self.factory.throttler.execute_task(
                    OKX.WS_CONNECTION_LIMIT_ID
                ):
                    await ws.connect(
                        ws_url=OKX.get_okx_ws_uri_public("www"), message_timeout=20
                    )
                args = [
                    {"channel": channel, "instId": pair}
                    for pair in pairs
                    for channel in ("books5", "trades")
                ]
                stage = 'subscribe'
                async with self.factory.throttler.execute_task(
                    OKX.WS_SUBSCRIPTION_LIMIT_ID
                ):
                    await ws.send(
                        WSJSONRequest(payload={"op": "subscribe", "args": args})
                    )
                acknowledgement_deadline = time.monotonic() + self.ACK_TIMEOUT_SECONDS
                while True:
                    try:
                        if not all(self.subscribed(pair) for pair in pairs):
                            stage = 'acknowledgement'
                            remaining = acknowledgement_deadline - time.monotonic()
                            if remaining <= 0:
                                raise ValueError('Public subscription acknowledgement timeout')
                            response = await asyncio.wait_for(ws.receive(), remaining)
                        else:
                            stage = 'receive'
                            response = await ws.receive()
                    except asyncio.TimeoutError:
                        if not all(self.subscribed(pair) for pair in pairs):
                            raise ValueError('Public subscription acknowledgement timeout') from None
                        await ws.send(WSPlainTextRequest(payload="ping"))
                        response = await asyncio.wait_for(ws.receive(), 10)
                    if response.data == "pong":
                        continue
                    stage = 'decode'
                    if response.data.get('event') != 'error' and response.data.get('arg', {}).get('instId') not in pairs:
                        raise ValueError('Public message outside connection subscription')
                    await self.accept(response.data, time.time())
                    if all(self.subscribed(pair) for pair in pairs):
                        backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.record_error(stage, error, pairs)
            finally:
                self.disconnect_pairs(pairs)
                if ws is not None:
                    try:
                        await ws.disconnect()
                    except Exception as error:
                        self.record_error('disconnect', error, pairs)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def start(self):
        self.tasks = [
            asyncio.create_task(self._run(self.pairs[i:i + 50]))
            for i in range(0, len(self.pairs), 50)
        ]

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.factory._connections_factory.close()


def meridian_v4_paper_configs(paths, pairs):
    """Sealed per-pair V4 or V4 D2h paper controllers (one program per runtime, injected 1m candles)."""
    import yaml

    from controllers.generic.meridian_v4 import MeridianV4Config
    from controllers.generic.meridian_v4_d2h import MeridianV4D2hConfig

    classes = {"meridian_v4": MeridianV4Config, "meridian_v4_d2h": MeridianV4D2hConfig}
    configs = []
    for path in paths:
        values = yaml.safe_load(Path(path).read_text())
        cls = classes.get(values.get("controller_name")) if isinstance(values, dict) else None
        if cls is None:
            raise ValueError(f"V4 paper requires a meridian_v4 or meridian_v4_d2h controller file: {path}")
        config = cls.model_validate(values)
        if (config.execution_mode != "paper" or config.connector_name != "okx_paper_trade"
                or config.data.mode != "native" or not config.execution_enabled or config.recipe_binding is None):
            raise ValueError("V4 paper needs a sealed, enabled okx_paper_trade controller with injected native 1m candles")
        configs.append(config)
    if len({type(c) for c in configs}) != 1 or len({c.budget.group for c in configs}) != 1:
        raise ValueError("One V4 program per paper runtime; run each A/B arm in its own runtime")
    if sum(c.total_amount_quote for c in configs) > configs[0].budget.max_quote:
        raise ValueError("Per-pair budgets exceed the program budget")
    if sorted(c.trading_pair for c in configs) != sorted(pairs):
        raise ValueError("Paper controller pairs must equal the configured pairs")
    return configs


def meridian_unified_paper_configs(paths, pairs):
    """The one Meridian portfolio controller (native paper wallet, injected 1m candles)."""
    import yaml

    from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig

    (path,) = paths
    values = yaml.safe_load(Path(path).read_text())
    if not isinstance(values, dict) or values.get("controller_name") != "meridian_v3_unified":
        raise ValueError(f"Meridian unified paper requires a meridian_v3_unified controller file: {path}")
    config = MeridianUnifiedConfig.model_validate(values)
    if config.execution_mode != "paper" or config.connector_name != "okx_paper_trade" or config.data.mode != "native":
        raise ValueError("Meridian unified paper needs okx_paper_trade and injected native 1m candles")
    if sorted(config.owned_trading_pairs()) != sorted(pairs):
        raise ValueError("Meridian controller pairs must equal the configured pairs")
    return [config]


class PaperRuntime:
    def __init__(self, config, rules):
        self.config = config
        self.twin_divergence = {}
        if config.controller_kind == "rsi_modular":
            configs, self.twin_divergence = rsi_modular_paper_configs(config.controller_files, config.pairs)
            history = {c.trading_pair: max(feed.max_records for feed in c.candles_config) for c in configs}
            if any(feed.interval != "1m" for c in configs for feed in c.candles_config):
                raise ValueError("Paper twin injects 1m candles only")
        elif config.controller_kind == "modular_spot_research":
            configs = modular_spot_research_paper_configs(config.controller_files, config.pairs)
            history = {c.trading_pair: c.max_records for c in configs}  # the binding reads max_records closed candles
        elif config.controller_kind == "meridian_v4":
            configs = meridian_v4_paper_configs(config.controller_files, config.pairs)
            if sum(c.total_amount_quote for c in configs) > config.simulated_quote:
                raise ValueError("Per-pair budgets exceed the simulated paper wallet")
            history = {c.trading_pair: c.max_records for c in configs}
        elif config.controller_kind == "meridian_v3_unified":
            configs = meridian_unified_paper_configs(config.controller_files, config.pairs)
            history = {p: 1500 for p in config.pairs}  # every symbol context reads the p23 1500-candle window
        else:
            configs = [
                ComponentBreakoutConfig(
                    id="paper_" + p.replace("-", "_"),
                    trading_pair=p,
                    execution_enabled=True,
                    total_amount_quote=config.position_quote,
                    trade_quote=config.position_quote,
                )
                for p in config.pairs
            ]
            history = {p: 60 for p in config.pairs}
        self.controller_ids = {pair: c.id for c in configs for pair in owned_pairs(c)}
        self.datasets = [
            Dataset(connector="okx", trading_pair=p, history=history[p]) for p in config.pairs
        ]
        self.candles = CandleStreamClient(
            config.candle_origin, config.candle_token_file, self.datasets
        )
        connector = NativePaperTradeExchange(
            rules,
            max_data_age=config.book_max_age_seconds,
            trade_participation=config.trade_participation,
            fee_schema=TradeFeeSchema(
                maker_percent_fee_decimal=config.maker_fee,
                taker_percent_fee_decimal=config.taker_fee,
            ),
        )
        self.liquidity = PublicLiquidity(config.pairs, config.book_max_age_seconds,
                                         book_validator=connector.validate_order_book_input)
        connector.set_balance("USDC", config.simulated_quote)
        for pair in config.pairs:
            connector.set_balance(pair.split("-")[0], Decimal("0"))
        config.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_fd = os.open(
            str(config.state_path) + '.runtime.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.restored = config.state_path.exists()
            self.source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
            binding = {'schema': 'native_component_paper_runtime_v1', 'source_sha256': self.source_sha256,
                       'config': config.model_dump(mode='json')}
            if config.controller_files:  # a changed live file is a changed twin; restore must refuse it
                binding['controller_files_sha256'] = {str(f): hashlib.sha256(Path(f).read_bytes()).hexdigest()
                                                      for f in config.controller_files}
            receipt = Path(str(config.state_path) + '.runtime.json')
            if self.restored:
                if not receipt.is_file() or receipt.is_symlink() or json.loads(receipt.read_text()) != binding:
                    raise ValueError('Paper driver source/config identity mismatch; reconciliation required')
            else:
                if receipt.exists():
                    raise ValueError('Orphaned paper driver identity; reconciliation required')
                atomic_json(receipt, binding)
            self.session = NativePaperSession(connector, configs, config.state_path, restore=self.restored)
        except BaseException:
            os.close(self._lock_fd)
            self._lock_fd = None
            raise
        self.boot_id = os.urandom(16).hex()
        self.steps = 0
        self.actions = 0
        self.last_result = None
        self.report_observed_at = None
        self.failure = None
        self._seen_candles = {}
        self._applied_books = dict(connector.tracking_states["book_sequence"])
        self.journal_path = config.status_path.with_name(config.bot_name + ".decisions.jsonl")
        self._journal_state = {}
        self.journal_failures = 0
        self.journal_error = None

    def status(self, now, reasons):
        reports = (self.last_result['reports'] if self.last_result and self.failure is None
                   and 'runtime_stopped' not in reasons else {})
        return {
            "schema_version": 1,
            "paper_runtime_schema": "native_component_paper_runtime_v1",
            "bot_name": self.config.bot_name,
            "timestamp": now,
            "generated_at": now,
            "execution_mode": "paper",
            "controller_kind": self.config.controller_kind,
            "twin_divergence": self.twin_divergence,
            "process_boot_id": self.boot_id,
            "process_pid": os.getpid(),
            "source_sha256": self.source_sha256,
            "restored": self.restored,
            "reconciliation": self.session.reconciliation_status(),
            "decision_journal": {"failures": self.journal_failures, "last_error": self.journal_error},
            "ready": not reasons and self.failure is None,
            "reasons": reasons,
            "failure": self.failure,
            "steps": self.steps,
            "actions": self.actions,
            "public_liquidity": {
                "messages": self.liquidity.messages,
                "failures": self.liquidity.failures,
                "stale_inputs": self.liquidity.stale_inputs,
                "book_rejections": self.liquidity.book_rejections,
                "invalid_book_reasons": dict(self.liquidity.invalid_book_reasons),
                "queued_trades": len(self.liquidity.trades),
                "failures_by_stage": dict(self.liquidity.failures_by_stage),
                "recent_errors": list(self.liquidity.recent_errors),
                "acknowledged_pairs": sum(self.liquidity.subscribed(p) for p in self.config.pairs),
                "fresh_book_pairs": sum(p in self.liquidity.books and 0 <= now - self.liquidity.books[p]['timestamp']
                                        <= self.config.book_max_age_seconds for p in self.config.pairs),
                "accepted_latency": self.liquidity.latency_summary(),
                "pairs": {p: {'subscribed': self.liquidity.subscribed(p),
                              'book_age_seconds': now - self.liquidity.books[p]['timestamp'] if p in self.liquidity.books else None,
                              'book_received_at': self.liquidity.received_at.get((p, 'books5')),
                              'trade_received_at': self.liquidity.received_at.get((p, 'trades'))} for p in self.config.pairs},
            },
            "candle_transport": {
                "sessions": self.candles.sessions,
                "failures": self.candles.failures,
            },
            "controllers": [self._controller_row(c) for c in self.session.controllers.values()],
            "native_reports": reports,
            "report_observed_at": self.report_observed_at if reports else None,
            "unavailable_controller_ids": sorted(set(self.session.controllers) - set(reports)),
            "simulation_assumptions": {
                "quote": str(self.config.simulated_quote),
                "trade_participation": str(self.config.trade_participation),
                "book_depth": 5,
                "queue_priority_verified": False,
            },
        }

    @staticmethod
    def _controller_row(controller):
        """Native status plus the identity the paper reader binds (filled from config only when absent)."""
        row = dict(controller.get_status_summary(), id=controller.config.id)
        row.setdefault("controller_name", controller.config.controller_name)
        pairs = owned_pairs(controller.config)
        row.setdefault("pair", pairs[0] if len(pairs) == 1 else list(pairs))
        row.setdefault("execution_mode", controller.config.execution_mode)
        # The bounded native custom info (decision reason, candle, bag) the live publisher sends.
        row.setdefault("custom_info", controller.get_custom_info())
        return row

    def journal(self, now, result):
        """Append the decision trail the status file overwrites every step (bounded growth).

        One line per controller per closed candle: step counts per decision status (`same_candle` counts
        steps after the decision), the final decision and its full reason (e.g. data_hold:<cause>),
        `decided` from the owner's persisted checkpoint, and the bag only when it changed.
        One line per executor action, immediately."""
        lines = []
        acted = {}
        for action in result["actions"]:
            config = getattr(action, "executor_config", None)
            acted.setdefault(action.controller_id, []).append({
                "type": type(action).__name__, "executor_id": getattr(action, "executor_id", None)
                or getattr(config, "id", None),
                "side": None if getattr(config, "side", None) is None else str(getattr(config.side, "name", config.side)),
                "amount": None if getattr(config, "amount", None) is None else str(config.amount),
                "price": None if getattr(config, "entry_price", None) is None else str(config.entry_price),
            })
        for controller_id, controller in self.session.controllers.items():
            info = controller.get_custom_info()
            detail = getattr(getattr(controller, "runtime", None), "reason", None)
            detail = None if detail is None else str(detail)[:240]
            status, candle = info.get("modular_status"), info.get("candle_open")
            stats = self._journal_state.get(controller_id)
            if stats is not None and stats["candle_open"] != candle and stats["statuses"]:
                lines.append(self._candle_line(controller_id, stats))
            if stats is None or stats["candle_open"] != candle:
                previous_bag = None if stats is None else stats["logged_bag"]
                stats = {"candle_open": candle, "statuses": {}, "final": None, "detail": None,
                         "bag": None, "logged_bag": previous_bag}
                self._journal_state[controller_id] = stats
            stats["statuses"][status] = stats["statuses"].get(status, 0) + 1
            if status != "same_candle":
                stats["final"], stats["detail"] = status, detail
            stats["controller"] = controller
            stats["bag"] = info.get("bag")
            if controller_id in acted:
                lines.append(json.dumps({"t": now, "controller": controller_id, "kind": "action",
                                         "candle_open": candle, "status": status, "detail": detail,
                                         "bag": info.get("bag"), "actions": acted[controller_id]},
                                        sort_keys=True, default=json_value, allow_nan=False))
        if lines:
            with open(self.journal_path, "a") as output:
                output.write("\n".join(lines) + "\n")

    def _candle_line(self, controller_id, stats):
        bag = json.dumps(stats["bag"], sort_keys=True, default=json_value)
        changed = bag != stats["logged_bag"]
        stats["logged_bag"] = bag
        return json.dumps({"t": time.time(), "controller": controller_id, "kind": "candle",
                           "candle_open": stats["candle_open"], "statuses": stats["statuses"],
                           "final": stats["final"], "detail": stats["detail"],
                           "decided": self._decided(stats.get("controller"), stats["candle_open"]),
                           **({"bag": stats["bag"]} if changed else {})},
                          sort_keys=True, default=json_value, allow_nan=False)

    @staticmethod
    def _decided(controller, candle_open):
        """Whether the owner's persisted decision checkpoint reached this candle (True/False), or None when the
        owner has no modular decision store. A status line alone cannot prove it: a later snapshot in the same
        step can overwrite the decision reason with a data hold."""
        store = getattr(getattr(controller, "runtime", None), "store", None)
        if store is None or candle_open is None:
            return None
        state, _ = store.load()
        return state is not None and state.candle is not None and state.candle >= candle_open

    def write_journal(self, now, result):
        """The journal is evidence, not session state: its failure is counted, reported and logged,
        never turned into a native session failure (the step has already been persisted)."""
        try:
            self.journal(now, result)
        except Exception as error:
            self.journal_failures += 1
            self.journal_error = f"{type(error).__name__}: {str(error)[:200]}"
            print(f"ERROR decision journal write failed ({self.journal_failures}): {self.journal_error}",
                  file=sys.stderr, flush=True)

    async def step(self, now):
        if self.failure:
            raise RuntimeError("Paper runtime held after failure")
        if self.liquidity.fatal:
            self.failure = self.liquidity.fatal
            raise RuntimeError(self.failure)
        reasons = []
        frames = []
        submitted_digests = {}
        breakout = self.config.controller_kind == "component_breakout"
        for spec in self.datasets:
            controller = self.session.controllers[self.controller_ids[spec.trading_pair]]
            try:
                frame = self.candles.replica.frame(spec.key, now=now, max_age=70)
                snapshot = self.candles.replica.snapshots[spec.key]
                if not self.candles.replica.entry_ready(spec.key):
                    if not breakout:  # the twin, like its live stream owner, acts only after a live candle
                        reasons.append(spec.trading_pair + ":candles_warming")
                        continue
                    controller._last_decision = int(frame.iloc[-1].timestamp) + 60
                if self._seen_candles.get(spec.key) != snapshot.digest:
                    submitted_digests[spec.key] = snapshot.digest
                    frames.append(
                        dict(
                            connector_name="okx",
                            trading_pair=spec.trading_pair,
                            interval="1m",
                            frame=frame,
                            available_at=now,
                        )
                    )
            except (DataUnavailable, ValueError):
                reasons.append(spec.trading_pair + ":candles_unavailable")
                # Preserve existing restart hold; prevent a stale cached frame admitting entry.
                if breakout and not self.restored:
                    controller.recovery_hold = "public_candles_unavailable"
                continue
            if breakout and not self.restored:
                controller.recovery_hold = ""
        for pair, book in list(self.liquidity.books.items()):
            if book['sequence'] > self._applied_books.get(pair, -1):
                try:
                    self.session._connector.validate_order_book_input(**book, current_timestamp=now)
                except ValueError as error:
                    self.liquidity.reject_book(pair, error)
        stale = [
            p
            for p in self.config.pairs
            if not self.liquidity.subscribed(p) or p not in self.liquidity.books
            or not 0
            <= now - self.liquidity.books[p]["timestamp"]
            <= self.config.book_max_age_seconds
        ]
        if stale:
            reasons.append("fresh_books_required:" + ",".join(stale))
            for pair in stale:
                self.session._connector.invalidate_order_book(pair)
        books = [
            b
            for p, b in self.liquidity.books.items()
            if p not in stale and b["sequence"] > self._applied_books.get(p, -1)
        ]
        trades = list(self.liquidity.trades)
        self.liquidity.trades.clear()
        trades = [
            t
            for t in trades
            if t["trading_pair"] not in stale
            and 0 <= now - t["timestamp"] <= self.config.book_max_age_seconds
        ]
        try:
            result = await self.session.step(
                now, books=books, trades=trades, candles=frames
            )
            self.last_result = result
            self.report_observed_at = now if result['reports'] else None
            self.steps += 1
            self.actions += len(result["actions"])
            for book in books:
                self._applied_books[book["trading_pair"]] = book["sequence"]
            # Feed tasks can publish newer candles while the native step awaits.
            # Acknowledge only the exact frames submitted to this completed batch.
            self._seen_candles.update(submitted_digests)
            for reason in result['readiness']['reasons']:
                owner, _, detail = reason.partition(': ')
                controller = self.session.controllers.get(owner)
                if (detail == 'native_candles_unavailable' and controller is not None
                        and controller.processed_data.get('data_health', {}).get('reason') == 'public_book_unavailable'):
                    reason = owner + ': public_book_unavailable'
                reasons.append(reason)
            atomic_json(self.config.status_path, self.status(now, reasons))
        except BaseException as error:
            self.failure = type(error).__name__
            atomic_json(
                self.config.status_path,
                self.status(now, ["native_session_failure_requires_reconciliation"]),
            )
            raise
        self.write_journal(now, result)
        return result

    async def run(self, stop):
        self.candles.start()
        self.liquidity.start()
        try:
            while not stop.is_set():
                await self.step(time.time())
                try:
                    await asyncio.wait_for(stop.wait(), 0.5)
                except asyncio.TimeoutError:
                    pass
        finally:
            await self.close()

    async def close(self):
        if self._lock_fd is None:
            return
        try:
            await self.candles.close()
            await self.liquidity.close()
            await self.session.close()
            atomic_json(
                self.config.status_path, self.status(time.time(), ["runtime_stopped"])
            )
        finally:
            os.close(self._lock_fd)
            self._lock_fd = None


async def main(path):
    config = PaperConfig.model_validate_json(Path(path).read_text())
    runtime = PaperRuntime(config, await public_rules(config.pairs))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for event in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(event, stop.set)
    await runtime.run(stop)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    asyncio.run(main(parser.parse_args().config))
