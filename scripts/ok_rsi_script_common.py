from collections.abc import Iterable, Mapping
from typing import List, Literal

from controllers.directional_trading import ok_rsi as ok_rsi_controller


def normalize_okx_usdt_pairs(value) -> List[str]:
    if value is None:
        raise ValueError("At least one OKX trading pair is required")
    if isinstance(value, str):
        value = [pair.strip() for pair in value.split(",") if pair.strip()]
    elif isinstance(value, Mapping) or not isinstance(value, Iterable):
        raise ValueError("OKX trading pairs must be a list or comma-separated string")
    normalized = [str(pair).strip().upper() for pair in value]
    for pair in normalized:
        parts = pair.split("-")
        if len(parts) != 2 or not all(parts) or parts[1] != "USDT":
            raise ValueError(f"OKX spot pair must use BASE-USDT format: {pair!r}")
    if not normalized:
        raise ValueError("At least one OKX trading pair is required")
    if len(normalized) != len(set(normalized)):
        raise ValueError("Trading pairs must be unique")
    return normalized


def build_ok_rsi_controller_configs(
    config,
    trade_mode: Literal["buy_sell", "sell_only"],
) -> List[ok_rsi_controller.OKRSIControllerConfig]:
    shared = {
        "connector_name": config.connector_name,
        "leverage": config.leverage,
        "interval": config.interval,
        "strict_hmm_mode": config.strict_hmm_mode,
        "total_amount_quote": config.total_amount_quote,
        "usd_per_entry": config.usd_per_entry,
        "max_total_position_usd": config.max_total_position_usd,
        "max_executors_per_side": config.max_executors_per_side,
        "sell_rsi_overbought": config.sell_rsi_overbought,
        "sell_only_inventory_fraction": config.sell_only_inventory_fraction,
        "sell_all_below_quote": config.sell_all_below_quote,
    }
    return [
        ok_rsi_controller.OKRSIControllerConfig(
            id=f"ok-rsi-{trading_pair.lower()}-{trade_mode.replace('_', '-')}",
            trading_pair=trading_pair,
            trade_mode=trade_mode,
            sell_only_if_profitable=trade_mode == "buy_sell",
            **shared,
        )
        for trading_pair in config.trading_pairs
    ]
