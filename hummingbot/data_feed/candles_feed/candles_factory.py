from typing import Dict, Type

from hummingbot.data_feed.candles_feed.aevo_perpetual_candles import AevoPerpetualCandles
from hummingbot.data_feed.candles_feed.ascend_ex_spot_candles.ascend_ex_spot_candles import AscendExSpotCandles
from hummingbot.data_feed.candles_feed.binance_perpetual_candles import BinancePerpetualCandles
from hummingbot.data_feed.candles_feed.binance_spot_candles import BinanceSpotCandles
from hummingbot.data_feed.candles_feed.bitget_perpetual_candles import BitgetPerpetualCandles
from hummingbot.data_feed.candles_feed.bitget_spot_candles import BitgetSpotCandles
from hummingbot.data_feed.candles_feed.bitmart_perpetual_candles.bitmart_perpetual_candles import (
    BitmartPerpetualCandles,
)
from hummingbot.data_feed.candles_feed.btc_markets_spot_candles.btc_markets_spot_candles import BtcMarketsSpotCandles
from hummingbot.data_feed.candles_feed.bybit_perpetual_candles.bybit_perpetual_candles import BybitPerpetualCandles
from hummingbot.data_feed.candles_feed.bybit_spot_candles.bybit_spot_candles import BybitSpotCandles
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.data_feed.candles_feed.dexalot_spot_candles.dexalot_spot_candles import DexalotSpotCandles
from hummingbot.data_feed.candles_feed.gate_io_perpetual_candles import GateioPerpetualCandles
from hummingbot.data_feed.candles_feed.gate_io_spot_candles import GateioSpotCandles
from hummingbot.data_feed.candles_feed.hyperliquid_perpetual_candles.hyperliquid_perpetual_candles import (
    HyperliquidPerpetualCandles,
)
from hummingbot.data_feed.candles_feed.hyperliquid_spot_candles.hyperliquid_spot_candles import HyperliquidSpotCandles
from hummingbot.data_feed.candles_feed.kraken_spot_candles.kraken_spot_candles import KrakenSpotCandles
from hummingbot.data_feed.candles_feed.kucoin_perpetual_candles.kucoin_perpetual_candles import KucoinPerpetualCandles
from hummingbot.data_feed.candles_feed.kucoin_spot_candles.kucoin_spot_candles import KucoinSpotCandles
from hummingbot.data_feed.candles_feed.mexc_perpetual_candles.mexc_perpetual_candles import MexcPerpetualCandles
from hummingbot.data_feed.candles_feed.mexc_spot_candles.mexc_spot_candles import MexcSpotCandles
from hummingbot.data_feed.candles_feed.okx_perpetual_candles.okx_perpetual_candles import OKXPerpetualCandles
from hummingbot.data_feed.candles_feed.okx_service_candles.okx_service_candles import OKXServiceCandles
from hummingbot.data_feed.candles_feed.okx_spot_candles.okx_spot_candles import OKXSpotCandles
from hummingbot.data_feed.candles_feed.pacifica_perpetual_candles import PacificaPerpetualCandles


class UnsupportedConnectorException(Exception):
    """
    Exception raised when an unsupported connector is requested.
    """

    def __init__(self, connector: str):
        message = f"The connector {connector} is not available. Please select another one."
        super().__init__(message)


class CandlesFactory:
    """
    The CandlesFactory class creates and returns a Candle object based on the specified configuration.
    It uses a mapping of connector names to their respective candle classes.
    """

    _candles_map: Dict[str, Type[CandlesBase]] = {
        "aevo_perpetual": AevoPerpetualCandles,
        "binance_perpetual": BinancePerpetualCandles,
        "binance": BinanceSpotCandles,
        "bitget": BitgetSpotCandles,
        "bitget_perpetual": BitgetPerpetualCandles,
        "gate_io": GateioSpotCandles,
        "gate_io_perpetual": GateioPerpetualCandles,
        "kucoin": KucoinSpotCandles,
        "kucoin_perpetual": KucoinPerpetualCandles,
        "ascend_ex": AscendExSpotCandles,
        "okx_perpetual": OKXPerpetualCandles,
        "okx": OKXSpotCandles,
        "okx_service": OKXServiceCandles,
        "kraken": KrakenSpotCandles,
        "mexc": MexcSpotCandles,
        "mexc_perpetual": MexcPerpetualCandles,
        "bybit": BybitSpotCandles,
        "bybit_perpetual": BybitPerpetualCandles,
        "hyperliquid": HyperliquidSpotCandles,
        "hyperliquid_perpetual": HyperliquidPerpetualCandles,
        "dexalot": DexalotSpotCandles,
        "bitmart_perpetual": BitmartPerpetualCandles,
        "btc_markets": BtcMarketsSpotCandles,
        "pacifica_perpetual": PacificaPerpetualCandles,
    }

    # Opt-in connector aliases, empty unless a process routes one (see okx_service_candles.service_hub). A route keeps
    # the caller's connector name, so keys, namespaces and recorded identities stay unchanged.
    _routes: Dict[str, str] = {}

    @classmethod
    def route(cls, connector: str, target: str) -> None:
        """Serve `connector` from the `target` feed class; an existing different route is an error."""
        if target not in cls._candles_map:
            raise UnsupportedConnectorException(target)
        if cls._routes.get(connector, target) != target:
            raise ValueError(f"The connector {connector} is already routed to {cls._routes[connector]}.")
        cls._routes[connector] = target

    @classmethod
    def unroute(cls, connector: str) -> None:
        cls._routes.pop(connector, None)

    @classmethod
    def get_candle(cls, candles_config: CandlesConfig) -> CandlesBase:
        """
        Returns a Candle object based on the specified configuration.

        :param candles_config: CandlesConfig
        :return: Instance of CandleBase or its subclass.
        :raises UnsupportedConnectorException: If the connector is not supported.
        """
        connector_class = cls._candles_map.get(cls._routes.get(candles_config.connector, candles_config.connector))
        if connector_class:
            return connector_class(candles_config.trading_pair, candles_config.interval, candles_config.max_records)
        else:
            raise UnsupportedConnectorException(candles_config.connector)
