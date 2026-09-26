import asyncio
import json
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.exchange.okx import okx_constants as CONSTANTS, okx_utils, okx_web_utils as web_utils
from hummingbot.connector.exchange.okx.okx_api_order_book_data_source import OkxAPIOrderBookDataSource
from hummingbot.connector.exchange.okx.okx_api_user_stream_data_source import OkxAPIUserStreamDataSource
from hummingbot.connector.exchange.okx.okx_auth import OkxAuth
from hummingbot.connector.exchange_base import s_decimal_NaN
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.terminal_reconciliation import terminal_reconciliation
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _discard_task_result(task: asyncio.Task) -> None:
    try:
        task.exception()
    except (asyncio.CancelledError, asyncio.InvalidStateError):
        pass


class OkxOrderRejectedError(IOError):
    """The exchange explicitly rejected this submission (distinct from transport uncertainty)."""


# The compiled network loop (NetworkIterator) marks a connector NOT_CONNECTED, silently and without stop_network(),
# when one check call outlives check_network_timeout (a stalled event loop or a paused Docker VM), and calls
# start_network() on the next good check. start_network() stops and rebuilds every order book, the user stream and
# the polling tasks. While networking was never stopped and the previous good check is this recent, that rebuild
# only discards healthy state (measured 2026-09-26: V2 926 s of cold books from 12 such rebuilds in 5 h), so it is
# skipped. A longer silence, or any logged stop, still rebuilds.
NETWORK_REBUILD_GAP_SECONDS = 120.0


class OkxExchange(ExchangePyBase):

    UNATTENDED_RESTART_HOLD_CONTRACT_VERSION = 1
    supports_terminal_fill_reconciliation = True

    web_utils = web_utils

    def __init__(self,
                 okx_api_key: str,
                 okx_secret_key: str,
                 okx_passphrase: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 okx_registration_sub_domain: str = "www"):
        """
        :param okx_registration_sub_domain: The subdomain to use - options are "www" (default), "app" (US users), or "my" (EEA users)
                              See: https://github.com/ccxt/ccxt/issues/24601
        """
        self.okx_api_key = okx_api_key
        self.okx_secret_key = okx_secret_key
        self.okx_passphrase = okx_passphrase
        self.okx_registration_sub_domain = okx_registration_sub_domain or "www"
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_traded_price_source_timestamps = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self._spot_ticker_lock = asyncio.Lock()
        self._spot_ticker_cache = None
        self.check_network_timeout = 30.0
        self._networking_running = False
        self._last_good_network_check = None
        self._good_network_check_gap = None

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        if os.environ.get('RSIBOT_V2_UNATTENDED_RESTART_HOLD') == '1':
            raise RuntimeError('V2 unattended restart hold forbids order submission')
        if os.environ.get('RSIBOT_V2_DURABLE_ORDER_TRACKING') == '1':
            from hummingbot.connector.markets_recorder import MarketsRecorder
            recorder = MarketsRecorder.get_existing_instance()
            if (recorder is None or recorder.config_file_path != 'rsi_modular_v2.yml'
                    or self not in recorder._markets):
                raise ValueError('A matching durable V2 native recorder is required before submission')
            # The existing MarketState is the native order-tracker checkpoint,
            # including PENDING_CREATE. Commit its client ID before venue I/O.
            with recorder._sql_manager.get_new_session() as session:
                with session.begin():
                    recorder.save_market_states(recorder.config_file_path, self, session)
                    from hummingbot.model.metadata import Metadata
                    key = 'v2-submitted-order:' + order.client_order_id
                    if session.get(Metadata, key) is not None:
                        raise ValueError('Durable V2 client order ID was already submitted')
                    session.add(Metadata(key=key, value=json.dumps({
                        'owner': recorder.config_file_path, 'order': order.to_json()}, sort_keys=True)))
        return await super()._place_order_and_process_update(order, **kwargs)

    @property
    def authenticator(self):
        return OkxAuth(
            api_key=self.okx_api_key,
            secret_key=self.okx_secret_key,
            passphrase=self.okx_passphrase,
            time_provider=self._time_synchronizer)

    async def native_orders_reconciled(self) -> bool:
        from hummingbot.connector.exchange.okx.native_reconciliation import verify_no_open_orders

        return await verify_no_open_orders(self)

    @property
    def name(self) -> str:
        return "okx"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return CONSTANTS.get_okx_base_url(self.okx_registration_sub_domain)

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.CLIENT_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.OKX_INSTRUMENTS_PATH

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.OKX_INSTRUMENTS_PATH

    @property
    def check_network_request_path(self):
        return CONSTANTS.OKX_SERVER_TIME_PATH

    async def check_network(self) -> NetworkStatus:
        probe = asyncio.create_task(self._make_network_check_request())
        try:
            done, _pending = await asyncio.wait({probe}, timeout=4.0)
            if probe not in done:
                probe.cancel()
                probe.add_done_callback(_discard_task_result)
                if getattr(self, "_okx_time_ok", False):
                    return self._good_network_check()
                return NetworkStatus.NOT_CONNECTED
            exc = probe.exception()
            if exc is not None:
                if isinstance(exc, asyncio.CancelledError):
                    raise exc
                self._okx_time_ok = False
                return NetworkStatus.NOT_CONNECTED
            self._okx_time_ok = True
            return self._good_network_check()
        except asyncio.CancelledError:
            if not probe.done():
                probe.cancel()
                probe.add_done_callback(_discard_task_result)
            raise

    def _good_network_check(self) -> NetworkStatus:
        now = time.monotonic()
        last = self._last_good_network_check
        self._good_network_check_gap = None if last is None else now - last
        self._last_good_network_check = now
        return NetworkStatus.CONNECTED

    async def start_network(self):
        gap = self._good_network_check_gap
        if self._networking_running and gap is not None and gap <= NETWORK_REBUILD_GAP_SECONDS:
            self.logger().info(f"Network check recovered {gap:.0f} s after the previous good check and networking was "
                               "never stopped; keeping the running order books and streams.")
            return
        self._networking_running = False
        await super().start_network()
        self._networking_running = True

    async def stop_network(self):
        self._networking_running = False
        await super().stop_network()

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        # RESTAssistant includes the response body after "Error: ". Only an explicit
        # timestamp rejection is safe to retry; 50113 means invalid signature.
        try:
            response = json.loads(str(request_exception).split("Error: ", 1)[-1])
        except (TypeError, ValueError):
            return False
        return isinstance(response, dict) and response.get("code") == "50102"

    async def _recover_time_synchronizer(self, rejected_generation: int):
        # Recovery is serialized with initialization, and concurrent rejected
        # requests reuse a successful replacement instead of flooding public/time.
        await self._time_synchronizer.reset_server_time_offset_with_time_provider(
            time_provider=self.web_utils.get_current_server_time(throttler=self._throttler, domain=self.domain),
            rejected_generation=rejected_generation,
        )

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # Missing history is not a terminal order acknowledgement. Keep ownership until an explicit final state.
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # Cancellation acceptance / missing-order replies are asynchronous and require final status reconciliation.
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth,
            domain=self.domain)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return OkxAPIOrderBookDataSource(
            trading_pairs=self.trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return OkxAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:

        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _initialize_trading_pair_symbol_map(self):
        # This has to be reimplemented because the request requires an extra parameter
        try:
            exchange_info = await self._api_get(
                path_url=self.trading_pairs_request_path,
                params={"instType": "SPOT"},
            )
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(okx_utils.is_exchange_information_valid, exchange_info["data"]):
            mapping[symbol_data["instId"]] = combine_to_hb_trading_pair(base=symbol_data["baseCcy"],
                                                                        quote=symbol_data["quoteCcy"])
        self._set_trading_pair_symbol_map(mapping)

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:

        if os.environ.get('RSIBOT_V2_UNATTENDED_RESTART_HOLD') == '1':
            raise RuntimeError('V2 unattended restart hold forbids order submission')

        data = {
            "clOrdId": order_id,
            "tdMode": "cash",
            "ordType": CONSTANTS.ORDER_TYPE_MAP[order_type],
            "side": trade_type.name.lower(),
            "instId": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "sz": str(amount),
        }
        if order_type.is_limit_type():
            data["px"] = f"{price:f}"
        else:
            # Specify that the order quantity for market orders is denominated in base currency
            data["tgtCcy"] = "base_ccy"

        exchange_order_id = await self._api_request(
            path_url=CONSTANTS.OKX_PLACE_ORDER_PATH,
            method=RESTMethod.POST,
            data=data,
            is_auth_required=True,
            limit_id=CONSTANTS.OKX_PLACE_ORDER_PATH,
        )
        data = exchange_order_id["data"][0]
        if data["sCode"] != "0":
            raise OkxOrderRejectedError(f"Error submitting order {order_id}: {data['sCode']}: {data['sMsg']}")
        return str(data["ordId"]), self.current_timestamp

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        if isinstance(exception, OkxOrderRejectedError):
            order = self._order_tracker.fetch_order(order_id)
            if order is not None and order.exchange_order_id is None and order.executed_amount_base == 0:
                order.terminal_reconciliation = terminal_reconciliation(order, 'okx_submission_rejected_v1', 'REJECTED', '0')
            return super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)
        self.logger().error(
            f"Submission status unknown for {order_id}: {type(exception).__name__}: {exception}. "
            "Keeping the client order identity reserved; polling order details before any retry."
        )

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        This implementation specific function is called by _cancel, and returns True if successful
        """
        params = {
            "clOrdId": order_id,
            "instId": tracked_order.trading_pair
        }
        cancel_result = await self._api_post(
            path_url=CONSTANTS.OKX_ORDER_CANCEL_PATH,
            data=params,
            is_auth_required=True,
        )
        if cancel_result["data"][0]["sCode"] == "0":
            final_result = True
        elif cancel_result["data"][0]["sCode"] == "51400":
            # Cancelation failed because the order does not exist
            final_result = True
        elif cancel_result["data"][0]["sCode"] == "51401":
            # Cancelation failed because order has been cancelled
            final_result = True
        else:
            raise IOError(f"Error cancelling order {order_id}: {cancel_result}")

        return final_result

    async def _spot_ticker_snapshot(self) -> Tuple[Dict[str, float], Dict[str, Optional[float]]]:
        # ponytail: 2s all-spot ticker cache; provider-level batch if 5s still times out
        async with self._spot_ticker_lock:
            now = time.monotonic()
            cached = self._spot_ticker_cache
            if cached is not None and now - cached[0] < 2.0:
                return cached[1], cached[2]
            resp_json = await self._api_get(
                path_url=CONSTANTS.OKX_TICKERS_PATH,
                params={"instType": "SPOT"},
            )
            prices: Dict[str, float] = {}
            timestamps: Dict[str, Optional[float]] = {}
            for ticker in resp_json["data"]:
                last = ticker.get("last")
                if not last:
                    continue
                inst_id = ticker["instId"]
                prices[inst_id] = float(last)
                try:
                    source_timestamp = Decimal(str(ticker.get("ts"))) / Decimal("1000")
                    timestamps[inst_id] = (
                        float(source_timestamp)
                        if source_timestamp.is_finite() and source_timestamp > 0
                        else None
                    )
                except (ArithmeticError, TypeError, ValueError):
                    timestamps[inst_id] = None
            self._spot_ticker_cache = (time.monotonic(), prices, timestamps)
            return prices, timestamps

    async def _fetch_single_ticker(self, trading_pair: str) -> float:
        params = {"instId": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)}
        resp_json = await self._api_request(
            path_url=CONSTANTS.OKX_TICKER_PATH,
            params=params,
        )
        ticker_data, *_ = resp_json["data"]
        try:
            source_timestamp = Decimal(str(ticker_data.get("ts"))) / Decimal("1000")
            source_timestamp = float(source_timestamp) if source_timestamp.is_finite() and source_timestamp > 0 else None
        except (ArithmeticError, TypeError, ValueError):
            source_timestamp = None
        if not hasattr(self, "_last_traded_price_source_timestamps"):
            self._last_traded_price_source_timestamps = {}
        self._last_traded_price_source_timestamps[trading_pair] = source_timestamp
        return float(ticker_data["last"])

    async def get_last_traded_prices(self, trading_pairs: List[str] = None) -> Dict[str, float]:
        if trading_pairs and len(trading_pairs) == 1:
            return {trading_pairs[0]: await self._fetch_single_ticker(trading_pairs[0])}
        prices, _timestamps = await self._spot_ticker_snapshot()
        if not trading_pairs:
            return prices
        return {pair: prices[pair] for pair in trading_pairs if pair in prices}

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        prices, timestamps = await self._spot_ticker_snapshot()
        if symbol not in prices:
            raise IOError(f"OKX ticker missing last price for {symbol}")
        if not hasattr(self, "_last_traded_price_source_timestamps"):
            self._last_traded_price_source_timestamps = {}
        self._last_traded_price_source_timestamps[trading_pair] = timestamps.get(symbol)
        return prices[symbol]

    def last_traded_price_source_timestamp(self, trading_pair: str) -> Optional[float]:
        return self._last_traded_price_source_timestamps.get(trading_pair)

    async def _update_balances(self):
        msg = await self._api_request(
            path_url=CONSTANTS.OKX_BALANCE_PATH,
            is_auth_required=True)

        if msg['code'] == '0':
            balances = msg['data'][0]['details']
        else:
            raise Exception(msg['msg'])

        self._account_available_balances.clear()
        self._account_balances.clear()

        for balance in balances:
            self._update_balance_from_details(balance_details=balance)

    def _update_balance_from_details(self, balance_details: Dict[str, Any]):
        equity_text = balance_details["eq"]
        available_equity_text = balance_details["availEq"]

        if equity_text and available_equity_text:
            total = Decimal(equity_text)
            available = Decimal(available_equity_text)
        else:
            available = Decimal(balance_details["availBal"])
            total = available + Decimal(balance_details["frozenBal"])
        self._account_balances[balance_details["ccy"]] = total
        self._account_available_balances[balance_details["ccy"]] = available

    async def _update_trading_rules(self):
        # This has to be reimplemented because the request requires an extra parameter
        # TODO: Normalize the rest requests so they can be used standalone
        exchange_info = await self._api_get(
            path_url=self.trading_rules_request_path,
            params={"instType": "SPOT"},
        )
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    async def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        trading_rules = []

        for info in raw_trading_pair_info.get("data", []):
            try:
                if okx_utils.is_exchange_information_valid(exchange_info=info):
                    trading_rules.append(
                        TradingRule(
                            trading_pair=await self.trading_pair_associated_to_exchange_symbol(symbol=info["instId"]),
                            min_order_size=Decimal(info["minSz"]),
                            min_price_increment=Decimal(info["tickSz"]),
                            min_base_amount_increment=Decimal(info["lotSz"]),
                        )
                    )
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {info}. Skipping.")
        return trading_rules

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _request_order_update(self, order: InFlightOrder) -> Dict[str, Any]:
        return await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.OKX_ORDER_DETAILS_PATH,
            params={
                "instId": await self.exchange_symbol_associated_to_pair(order.trading_pair),
                "clOrdId": order.client_order_id},
            is_auth_required=True)

    async def _request_order_fills(self, order: InFlightOrder) -> Dict[str, Any]:
        return await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.OKX_TRADE_FILLS_PATH,
            params={
                "instType": "SPOT",
                "instId": await self.exchange_symbol_associated_to_pair(order.trading_pair),
                "ordId": await order.get_exchange_order_id()},
            is_auth_required=True)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            all_fills_response = await self._request_order_fills(order=order)
            fills_data = all_fills_response["data"]

            for fill_data in fills_data:
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=fill_data["feeCcy"],
                    flat_fees=[TokenAmount(amount=-Decimal(fill_data["fee"]), token=fill_data["feeCcy"])]
                )
                trade_update = TradeUpdate(
                    trade_id=str(fill_data["tradeId"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=str(fill_data["ordId"]),
                    trading_pair=order.trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(fill_data["fillSz"]),
                    fill_quote_amount=Decimal(fill_data["fillSz"]) * Decimal(fill_data["fillPx"]),
                    fill_price=Decimal(fill_data["fillPx"]),
                    fill_timestamp=int(fill_data["ts"]) * 1e-3,
                    is_taker=self._is_taker_fill(fill_data),
                )
                trade_updates.append(trade_update)

        return trade_updates

    def _is_taker_fill(self, data: Dict[str, Any]) -> bool:
        role = data.get("execType")
        if role == "M":
            return False
        if role != "T":
            self.logger().warning("OKX fill execution role unavailable; treating as taker trade_id=%s",
                                  data.get("tradeId"))
        return True

    @staticmethod
    def _okx_order_misc_updates(data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        source = data.get("cancelSource")
        if source is None:
            return None
        text = str(source).strip()
        if not text:
            return None
        return {"cancel_source": text}

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        updated_order_data = await self._request_order_update(order=tracked_order)

        order_data = updated_order_data["data"][0]
        new_state = CONSTANTS.ORDER_STATE[order_data["state"]]
        if new_state in {OrderState.CANCELED, OrderState.FILLED}:
            await self._reconcile_terminal_order_fills(tracked_order, order_data)

        misc_updates = self._okx_order_misc_updates(order_data)
        # Funding release requires the actual venue identity, not the request's
        # substituted client ID. Older incomplete status fixtures remain usable
        # by ordinary status polling but cannot attest funding release.
        if (new_state in {OrderState.CANCELED, OrderState.FILLED}
                and order_data.get('clOrdId') == tracked_order.client_order_id
                and order_data.get('side') == tracked_order.trade_type.name.lower()
                and order_data.get('instId')
                and order_data['instId'] == await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)):
            misc_updates = dict(misc_updates or {})
            misc_updates['terminal_reconciliation'] = terminal_reconciliation(
                tracked_order, 'okx_terminal_status_v1', new_state.name, order_data['accFillSz'])
        cancel_source = None if misc_updates is None else misc_updates.get("cancel_source")
        if cancel_source:
            tracked_order.cancel_source = cancel_source
        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order_data["ordId"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(order_data["uTime"]) * 1e-3,
            new_state=new_state,
            misc_updates=misc_updates,
        )
        return order_update

    async def _reconcile_terminal_order_fills(self, order: InFlightOrder, data: Dict[str, Any]):
        try:
            expected = Decimal(str(data["accFillSz"]))
            exchange_id = str(data["ordId"])
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise IOError("Terminal order fill total is unavailable") from exc
        if not expected.is_finite() or expected < 0 or expected > order.amount:
            raise IOError("Terminal order fill total is invalid")
        if not exchange_id or (order.exchange_order_id is not None and exchange_id != order.exchange_order_id):
            raise IOError("Terminal order identity differs from the tracked order")
        order.update_exchange_order_id(exchange_id)
        if order.executed_amount_base < expected:
            for trade_update in await self._all_trade_updates_for_order(order):
                self._order_tracker.process_trade_update(trade_update)
        if order.executed_amount_base != expected:
            raise IOError(f"Terminal fill reconciliation pending: expected={expected}, observed={order.executed_amount_base}")

    async def _user_stream_event_listener(self):
        async for stream_message in self._iter_user_event_queue():
            try:
                args = stream_message.get("arg", {})
                channel = args.get("channel", None)

                if channel == CONSTANTS.OKX_WS_ORDERS_CHANNEL:
                    for data in stream_message.get("data", []):
                        order_status = CONSTANTS.ORDER_STATE[data["state"]]
                        client_order_id = data["clOrdId"]
                        trade_id = data["tradeId"]
                        fillable_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                        updatable_order = self._order_tracker.all_updatable_orders.get(client_order_id)

                        if (fillable_order is not None
                                and order_status in [OrderState.PARTIALLY_FILLED, OrderState.FILLED]
                                and trade_id):
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=fillable_order.trade_type,
                                percent_token=data["fillFeeCcy"],
                                flat_fees=[TokenAmount(amount=-Decimal(data["fillFee"]), token=data["fillFeeCcy"])]
                            )
                            trade_update = TradeUpdate(
                                trade_id=str(trade_id),
                                client_order_id=fillable_order.client_order_id,
                                exchange_order_id=str(data["ordId"]),
                                trading_pair=fillable_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(data["fillSz"]),
                                fill_quote_amount=Decimal(data["fillSz"]) * Decimal(data["fillPx"]),
                                fill_price=Decimal(data["fillPx"]),
                                fill_timestamp=int(data["uTime"]) * 1e-3,
                                is_taker=self._is_taker_fill(data),
                            )
                            self._order_tracker.process_trade_update(trade_update)

                        if updatable_order is not None:
                            if order_status in {OrderState.CANCELED, OrderState.FILLED}:
                                await self._reconcile_terminal_order_fills(updatable_order, data)
                            order_update = OrderUpdate(
                                trading_pair=updatable_order.trading_pair,
                                update_timestamp=int(data["uTime"]) * 1e-3,
                                new_state=order_status,
                                client_order_id=updatable_order.client_order_id,
                                exchange_order_id=str(data["ordId"]),
                                misc_updates=self._okx_order_misc_updates(data),
                            )
                            self._order_tracker.process_order_update(order_update=order_update)

                elif channel == CONSTANTS.OKX_WS_ACCOUNT_CHANNEL:
                    for data in stream_message.get("data", []):
                        for details in data.get("details", []):
                            self._update_balance_from_details(balance_details=details)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(5.0)
