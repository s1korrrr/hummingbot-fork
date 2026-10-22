import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from pydantic import ValidationError

from controllers.directional_trading.hyperliquid_portfolio_rsi_v2 import (
    HyperliquidPortfolioRSIController,
    HyperliquidPortfolioRSIControllerConfig,
)
from controllers.shared.hyperliquid_portfolio_signal_client import HyperliquidPortfolioSignalClient
from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

AS_OF = 1_700_000_040.0


def _candles(closes):
    first_open = AS_OF - (len(closes) * 60)
    rows = []
    for index, close in enumerate(closes):
        rows.append({
            "timestamp": first_open + (index * 60),
            "open": close,
            "high": close * 1.002,
            "low": close * 0.998,
            "close": close,
            "volume": 1_000 + index,
        })
    rows.append({
        "timestamp": AS_OF,
        "open": closes[-1],
        "high": closes[-1] * 1.002,
        "low": closes[-1] * 0.998,
        "close": closes[-1],
        "volume": 2_000,
    })
    return pd.DataFrame(rows)


def _order_book(price):
    book = MagicMock()
    book.snapshot_uid = int(AS_OF * 1_000)
    book.snapshot = (
        pd.DataFrame([
            {"price": price * 0.9999, "amount": 1_000, "update_id": book.snapshot_uid},
            {"price": price * 0.9998, "amount": 1_000, "update_id": book.snapshot_uid},
        ]),
        pd.DataFrame([
            {"price": price * 1.0001, "amount": 1_000, "update_id": book.snapshot_uid},
            {"price": price * 1.0002, "amount": 1_000, "update_id": book.snapshot_uid},
        ]),
    )
    return book


def _provider_and_wallet():
    provider = MagicMock()
    provider.time.return_value = AS_OF
    provider.initialize_rate_sources.return_value = None
    provider.initialize_candles_feed.return_value = None
    provider.initialize_order_books = AsyncMock(return_value={
        "HYPE-USDC": True,
        "PURR-USDC": True,
    })
    provider.quantize_order_amount.side_effect = lambda connector, pair, amount: amount.quantize(Decimal("0.01"))
    provider.quantize_order_price.side_effect = lambda connector, pair, price: price

    wallet = MagicMock()
    wallet.ready = True
    balances = {
        "USDC": Decimal("1000"),
        "HYPE": Decimal("0"),
        "PURR": Decimal("2"),
        "UNKNOWN": Decimal("3"),
    }
    wallet.get_all_balances.return_value = balances
    wallet.get_available_balance.side_effect = lambda asset: balances.get(asset, Decimal("0"))
    wallet.all_trading_pairs = AsyncMock(return_value=["HYPE-USDC", "PURR-USDC"])
    wallet.limit_orders = []
    provider.get_connector.return_value = wallet

    hype_closes = [120 - (index * 0.1) for index in range(80)]
    purr_closes = [80 + (index * 0.5) for index in range(75)] + [118, 121, 124, 127, 126]
    candles_by_pair = {
        "HYPE-USDC": _candles(hype_closes),
        "PURR-USDC": _candles(purr_closes),
    }
    prices = {pair: float(frame["close"].iloc[-2]) for pair, frame in candles_by_pair.items()}
    books = {pair: _order_book(price) for pair, price in prices.items()}
    provider.get_candles_df.side_effect = lambda connector_name, trading_pair, interval, max_records: candles_by_pair[
        trading_pair
    ]
    provider.get_order_book.side_effect = lambda connector_name, trading_pair: books[trading_pair]
    provider.get_trading_rules.return_value = SimpleNamespace(
        min_notional_size=Decimal("10"),
        min_base_amount_increment=Decimal("0.01"),
        min_price_increment=Decimal("0.000001"),
        max_price_significant_digits=Decimal("5"),
    )
    return provider, wallet


def _config(**overrides):
    payload = {
        "id": "hyperliquid-portfolio-rsi-test",
        "buy_sell_assets": ["HYPE"],
        "protected_assets": [],
        "mode": "shadow",
        "usd_per_entry_quote": Decimal("30"),
        "max_symbol_exposure_quote": Decimal("500"),
        "gross_exposure_cap_quote": Decimal("10000"),
        "turnover_cap_quote": Decimal("1000"),
        "min_quote_reserve": Decimal("100"),
        "max_orders_per_tick": 2,
    }
    payload.update(overrides)
    return HyperliquidPortfolioRSIControllerConfig(**payload)


def test_config_rejects_live_and_real_connector_paper_execution():
    with pytest.raises(ValidationError):
        _config(mode="live")

    with pytest.raises(ValidationError):
        _config(
            mode="paper",
            paper_actions_enabled=True,
            execution_connector_name="hyperliquid",
        )

    with pytest.raises(ValidationError):
        _config(interval="banana")

    with pytest.raises(ValidationError):
        _config(interval="7m")


def test_config_updates_initial_markets_from_buy_allowlist():
    config = _config()

    markets = config.update_markets({})

    assert markets == {"hyperliquid": {"HYPE-USDC"}}


def test_configured_advisory_api_builds_read_only_signal_client():
    provider, _ = _provider_and_wallet()
    controller = HyperliquidPortfolioRSIController(
        _config(
            advisory_api_enabled=True,
            advisory_api_base_url="http://hummingbot-api:8000",
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )

    assert isinstance(controller._advisory_provider, HyperliquidPortfolioSignalClient)
    assert controller._advisory_provider.url("HYPE-USDC") == (
        "http://hummingbot-api:8000/portfolio/hyperliquid-rsi/signals/HYPE-USDC"
    )


@pytest.mark.asyncio
async def test_shadow_tick_detects_whole_wallet_and_builds_one_portfolio_plan():
    provider, _ = _provider_and_wallet()
    controller = HyperliquidPortfolioRSIController(
        _config(),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )

    await controller.update_processed_data()

    state = controller.processed_data
    policies = {row["base_asset"]: row for row in state["pair_policies"]}
    assert state["schema"] == "rsibot.hyperliquid_portfolio_rsi.controller_state.v1"
    assert set(state["portfolio_snapshot"]["balances"]) == {"USDC", "HYPE", "PURR", "UNKNOWN"}
    assert policies["HYPE"]["mode"] == "buy_sell"
    assert policies["PURR"]["mode"] == "sell_only"
    assert policies["UNKNOWN"]["enabled"] is False
    assert policies["UNKNOWN"]["symbol_mapping_status"] == "unresolved"
    assert state["signal_snapshots"]["pairs"]["HYPE-USDC"]["freshness_status"] == "fresh"
    assert state["signal_snapshots"]["pairs"]["PURR-USDC"]["freshness_status"] == "fresh"
    assert state["portfolio_plan"]["mode"] == "shadow"
    assert state["portfolio_plan"]["created_order_count"] == 0
    assert controller.determine_executor_actions() == []
    provider.initialize_order_books.assert_awaited_once_with(
        "hyperliquid",
        ["HYPE-USDC", "PURR-USDC"],
    )


@pytest.mark.asyncio
async def test_required_but_missing_advisory_blocks_resolved_pairs():
    provider, _ = _provider_and_wallet()
    controller = HyperliquidPortfolioRSIController(
        _config(advisory_required=True),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )

    await controller.update_processed_data()

    hype_signal = controller.processed_data["signal_snapshots"]["pairs"]["HYPE-USDC"]
    assert hype_signal["actionable"] is False
    assert "advisory_missing" in hype_signal["blockers"]
    hype_plan = next(
        row
        for row in controller.processed_data["portfolio_plan"]["rows"]
        if row["pair_id"] == "HYPE-USDC"
    )
    assert "feature_invalid" in hype_plan["skip_reasons"]


@pytest.mark.asyncio
async def test_paper_actions_bootstrap_execution_balances_once_and_plan_from_paper_wallet():
    provider, wallet = _provider_and_wallet()
    paper_balances = {}
    paper = MagicMock()
    paper.ready = True
    paper.limit_orders = []
    paper.get_all_balances.side_effect = lambda: dict(paper_balances)
    paper.get_available_balance.side_effect = lambda asset: paper_balances.get(asset, Decimal("0"))

    def set_paper_balance(asset, balance):
        paper_balances[asset] = Decimal(str(balance))

    paper.set_balance.side_effect = set_paper_balance
    provider.get_connector.side_effect = lambda connector_name: (
        paper if connector_name == "hyperliquid_paper_trade" else wallet
    )
    controller = HyperliquidPortfolioRSIController(
        _config(mode="paper", paper_actions_enabled=True),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )

    await controller.update_processed_data()

    state = controller.processed_data
    assert state["paper_balance_sync"]["status"] == "bootstrapped"
    assert state["portfolio_snapshot"]["source_connector"] == "hyperliquid_paper_trade"
    assert state["portfolio_snapshot"]["balances"]["PURR"]["total_units"] == "2"
    assert state["reference_wallet_snapshot"]["balances"]["PURR"]["total_units"] == "2"
    bootstrap_call_count = paper.set_balance.call_count
    assert bootstrap_call_count == 4

    paper_balances["PURR"] = Decimal("1")
    await controller.update_processed_data()

    assert paper.set_balance.call_count == bootstrap_call_count
    assert controller.processed_data["paper_balance_sync"]["status"] == "active"
    assert controller.processed_data["portfolio_snapshot"]["balances"]["PURR"]["total_units"] == "1"


def test_shadow_mode_never_translates_actionable_rows_to_executors():
    provider, _ = _provider_and_wallet()
    controller = HyperliquidPortfolioRSIController(
        _config(),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    controller.processed_data = {
        "portfolio_plan": {
            "readiness": "shadow_ready",
            "rows": [{
                "pair_id": "HYPE-USDC",
                "target_action": "buy",
                "proposed_units": "1",
                "proposed_value_quote": "100",
            }],
        },
        "signal_snapshots": {"pairs": {"HYPE-USDC": {"market": {"ask_price": 100, "bid_price": 99}}}},
    }

    assert controller.determine_executor_actions() == []


def test_explicit_paper_mode_builds_bounded_limit_order_executor_and_deduplicates():
    provider, _ = _provider_and_wallet()
    controller = HyperliquidPortfolioRSIController(
        _config(mode="paper", paper_actions_enabled=True, max_orders_per_tick=1),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    controller.processed_data = {
        "portfolio_plan": {
            "readiness": "paper_ready",
            "rows": [
                {
                    "pair_id": "PURR-USDC",
                    "target_action": "sell",
                    "proposed_units": "0.5",
                    "proposed_value_quote": "50",
                },
                {
                    "pair_id": "HYPE-USDC",
                    "target_action": "buy",
                    "proposed_units": "1",
                    "proposed_value_quote": "100",
                },
            ],
        },
        "signal_snapshots": {
            "pairs": {
                "PURR-USDC": {
                    "actionable": True,
                    "freshness_status": "fresh",
                    "symbol_mapping_status": "resolved",
                    "feature_cutoff": "2023-11-14T22:14:00Z",
                    "market": {"ask_price": 101, "bid_price": 100},
                },
                "HYPE-USDC": {
                    "actionable": True,
                    "freshness_status": "fresh",
                    "symbol_mapping_status": "resolved",
                    "feature_cutoff": "2023-11-14T22:14:00Z",
                    "market": {"ask_price": 51, "bid_price": 50},
                },
            }
        },
    }

    actions = controller.determine_executor_actions()

    assert len(actions) == 1
    executor = actions[0].executor_config
    assert executor.connector_name == "hyperliquid_paper_trade"
    assert executor.trading_pair == "PURR-USDC"
    assert executor.side == TradeType.SELL
    assert executor.position_action == PositionAction.CLOSE
    assert executor.execution_strategy == ExecutionStrategy.LIMIT
    assert executor.amount == Decimal("0.50")
    assert executor.price == Decimal("100")

    controller.executors_info = [SimpleNamespace(
        is_active=True,
        config=SimpleNamespace(
            connector_name="hyperliquid_paper_trade",
            trading_pair="PURR-USDC",
            side=TradeType.SELL,
        ),
    )]
    assert controller.determine_executor_actions() == []


def test_manual_kill_switch_stops_active_controller_executors():
    provider, _ = _provider_and_wallet()
    controller = HyperliquidPortfolioRSIController(
        _config(
            mode="paper",
            paper_actions_enabled=True,
            manual_kill_switch=True,
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    controller.executors_info = [
        SimpleNamespace(
            id="active-order-1",
            is_active=True,
            controller_id=controller.config.id,
            config=SimpleNamespace(connector_name="hyperliquid_paper_trade"),
        ),
        SimpleNamespace(
            id="other-controller-order",
            is_active=True,
            controller_id="other-controller",
            config=SimpleNamespace(connector_name="hyperliquid_paper_trade"),
        ),
    ]

    actions = controller.determine_executor_actions()

    assert actions == [
        StopExecutorAction(
            controller_id=controller.config.id,
            executor_id="active-order-1",
            keep_position=False,
        )
    ]


def test_stale_paper_executor_is_stopped_before_new_actions():
    provider, _ = _provider_and_wallet()
    controller = HyperliquidPortfolioRSIController(
        _config(
            mode="paper",
            paper_actions_enabled=True,
            order_timeout_seconds=30,
        ),
        market_data_provider=provider,
        actions_queue=asyncio.Queue(),
    )
    controller.executors_info = [
        SimpleNamespace(
            id="stale-order-1",
            timestamp=AS_OF - 31,
            is_active=True,
            controller_id=controller.config.id,
            config=SimpleNamespace(
                connector_name="hyperliquid_paper_trade",
                timestamp=AS_OF - 31,
            ),
        )
    ]
    controller.processed_data = {
        "portfolio_plan": {"readiness": "paper_ready", "rows": []},
        "signal_snapshots": {"pairs": {}},
    }

    actions = controller.determine_executor_actions()

    assert actions == [
        StopExecutorAction(
            controller_id=controller.config.id,
            executor_id="stale-order-1",
            keep_position=True,
        )
    ]
    assert controller.processed_data["action_telemetry"]["created_order_count"] == 0
    assert controller.processed_data["action_telemetry"]["stopped_executor_count"] == 1
    assert controller.processed_data["action_telemetry"]["blocked_reason"] == "stale_executor_timeout"
