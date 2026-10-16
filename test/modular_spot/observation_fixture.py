"""Synthetic Rust-schema observation fixture for isolated native replay tests.

This is a typed contract fixture, not a captured or Rust-produced tape.
"""

import json
from decimal import Decimal
from hashlib import sha256

from controllers.generic.modular_spot_observations_v1 import ModularSpotObservationsV1PaperConfig
from hummingbot.strategy_v2.market_context.observation_entry_builder import DurableCursor, JointCoverageReceipt
from hummingbot.strategy_v2.market_context.observation_reader import ObservationReader, SourceQualification

SOURCE_ID = "synthetic_rust_schema_v1"
SOURCE_HASH = sha256(b"synthetic-rust-schema-contract-v1").hexdigest()
PAIR = "BTC-USDC"
INSTRUMENT = "okx:spot:BTC-USDC"


def paper_config(*, controller_id="modular_spot_observations_replay_fixture"):
    return ModularSpotObservationsV1PaperConfig(
        id=controller_id,
        trading_pair=PAIR,
        budget={"group": "observations-replay-fixture", "max_quote": "100", "max_assets": 1},
        account="observation_replay_fixture",
        observation_source_profile="okx_global_public_v1",
        observation_source_id=SOURCE_ID,
        observation_source_contract_hash=SOURCE_HASH,
        observation_max_trades=64,
        observation_max_quotes=64,
        observation_max_wait_ms=60_000,
        observation_window_ms=100_000,
        observation_stabilization_window_ms=50_000,
        observation_max_quote_age_ms=60_000,
        observation_max_feature_age_ms=60_000,
        observation_max_spread_bps=Decimal("30"),
        observation_minimum_buy_imbalance=Decimal("0"),
        observation_minimum_adverse_drop_bps=Decimal("10"),
        observation_maximum_late_decline_bps=Decimal("0"),
    )


def fixture_stream(*, as_of_ms=1_060_000, side="buy"):
    """Return a reader, ordered updates, receipt, and exact fixture verifiers."""
    if type(as_of_ms) is not int or as_of_ms < 200_000 or side not in {"buy", "sell"}:
        raise ValueError("bounded synthetic fixture parameters required")
    start = as_of_ms - 100_000
    trade_epoch, quote_epoch, selection = "fixture-trades-e1", "fixture-bbo-e2", "fixture-selection"
    metadata = "fixture-meta-v1"
    source = SourceQualification(
        instrument_id=INSTRUMENT,
        source_id=SOURCE_ID,
        source_contract_hash=SOURCE_HASH,
        source_profile="okx_global_public_v1",
        metadata_revision=metadata,
        metadata_observed_sha256=sha256(b"fixture-observed-metadata").hexdigest(),
        selection_epoch=selection,
        qualified_at_ms=start,
        review_available_at_ms=start,
        expires_at_ms=as_of_ms + 60_000,
        review_receipt_sha256=sha256(f"{SOURCE_ID}:{SOURCE_HASH}:{selection}".encode()).hexdigest(),
    )
    reader = ObservationReader(
        instrument_id=INSTRUMENT,
        source_id=SOURCE_ID,
        source_contract_hash=SOURCE_HASH,
        source_profile="okx_global_public_v1",
        max_trades=64,
        max_quotes=64,
    )
    reader.attach_verified_source(source, verify=lambda item: item == source)
    updates = []

    def emit(channel, epoch, kind, at, payload):
        cursor = len(updates) + 1
        updates.append(
            {
                "schema_version": "okx.observations.update.v1",
                "sequence": cursor * 2 - 1,
                "epoch": epoch,
                "record_kind": kind,
                "channel": channel,
                "instrument_id": INSTRUMENT,
                "available_at_ms": at,
                "quality": "qualified",
                "reason_codes": [],
                "accepted_at_ms": at,
                "durable_acknowledged_at_ms": at,
                "computed_at_ms": None,
                "published_at_ms": at,
                "payload": payload,
                "durable_cursor": {"segment": 1, "sequence": cursor, "hash": f"{cursor:064x}"},
            }
        )
        return DurableCursor(1, cursor, f"{cursor:064x}")

    emit(
        "trades-all",
        trade_epoch,
        "subscription_ack",
        start,
        {"event": "subscription_ack", "channel": "trades-all", "inst_id": PAIR},
    )
    emit(
        "bbo-tbt",
        quote_epoch,
        "subscription_ack",
        start,
        {"event": "subscription_ack", "channel": "bbo-tbt", "inst_id": PAIR},
    )
    quotes = ((start, "9.99", "10.01"), (as_of_ms - 50_000, "9.99", "10.01"), (as_of_ms - 10, "9.99", "10.01"))
    for index, (event, bid, ask) in enumerate(quotes, 1):
        bbo = {
            "schema_version": "okx.observations.v1",
            "venue": "okx",
            "environment": "public",
            "region": "global",
            "instrument_id": INSTRUMENT,
            "channel": "bbo-tbt",
            "snapshot": True,
            "depth_scope": "single_level",
            "metadata_revision": metadata,
            "source_epoch": quote_epoch,
            "event_at_ms": event,
            "available_at_ms": event,
            "sequence": str(index),
            "bid_price": bid,
            "bid_quantity": "2",
            "ask_price": ask,
            "ask_quantity": "3",
            "quality": "qualified",
            "reason_codes": [],
        }
        emit("bbo-tbt", quote_epoch, "bbo", event, {"observation": bbo})
    trade_at = as_of_ms - 25_000
    trade = {
        "schema_version": "okx.observations.v1",
        "venue": "okx",
        "environment": "public",
        "region": "global",
        "instrument_id": INSTRUMENT,
        "channel": "trades-all",
        "trade_id": "1",
        "price": "10",
        "base_quantity": "4",
        "quote_notional": "40",
        "side": side,
        "aggregate_count": 1,
        "event_at_ms": trade_at,
        "available_at_ms": trade_at,
        "metadata_revision": metadata,
        "source_epoch": trade_epoch,
        "quality": "qualified",
        "connection_id": "fixture-connection",
        "receive_sequence": 1,
        "raw_sha256": sha256(b"fixture-trade-1").hexdigest(),
        "reason_codes": [],
    }
    emit("trades-all", trade_epoch, "trade", trade_at, {"observation": trade})
    trade_cursor = emit(
        "trades-all",
        trade_epoch,
        "timer",
        as_of_ms,
        {"event": "timer", "channel": "trades-all", "qualified_source": False},
    )
    quote_cursor = emit(
        "bbo-tbt", quote_epoch, "timer", as_of_ms, {"event": "timer", "channel": "bbo-tbt", "qualified_source": False}
    )
    # A single consumer observes publications in sequence and receipt order.
    # Keep the synthetic tape physically causal across both source channels.
    updates.sort(key=lambda row: row["published_at_ms"])
    for index, row in enumerate(updates, 1):
        row["sequence"] = index * 2 - 1
        row["durable_cursor"] = {"segment": 1, "sequence": index, "hash": f"{index:064x}"}
    trade_cursor = DurableCursor.from_mapping(
        next(
            row["durable_cursor"] for row in updates if row["record_kind"] == "timer" and row["channel"] == "trades-all"
        )
    )
    quote_cursor = DurableCursor.from_mapping(
        next(row["durable_cursor"] for row in updates if row["record_kind"] == "timer" and row["channel"] == "bbo-tbt")
    )
    receipt_data = {
        "instrument_id": INSTRUMENT,
        "source_id": SOURCE_ID,
        "source_contract_hash": SOURCE_HASH,
        "source_profile": "okx_global_public_v1",
        "metadata_revision": metadata,
        "selection_epoch": selection,
        "trade_source_epoch": trade_epoch,
        "quote_source_epoch": quote_epoch,
        "trade_coverage_start_ms": start,
        "trade_coverage_end_ms": as_of_ms,
        "quote_coverage_start_ms": start,
        "quote_coverage_end_ms": as_of_ms,
        "available_at_ms": as_of_ms,
        "quality": "qualified",
        "side_verified": True,
        "trade_cursor": trade_cursor,
        "quote_cursor": quote_cursor,
    }
    receipt_sha = sha256(json.dumps(receipt_data, sort_keys=True, default=str).encode()).hexdigest()
    receipt = JointCoverageReceipt(**receipt_data, receipt_sha256=receipt_sha)
    return reader, tuple(updates), receipt, lambda item: item == receipt
