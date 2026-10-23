"""Exact, stopped-owner repair for one V2 filled acquisition order projection.

The caller must prove the recorder owner is stopped, hold its exclusive maintenance
lock, preserve a consistent backup, pass integrity checks, and obtain a fresh
authenticated OKX GET receipt. This module never opens a path, reads credentials,
contacts a venue, or starts an owner. Planning is read-only. Applying is one
``BEGIN IMMEDIATE`` transaction and changes only ``Order.last_status`` and
``Order.last_update_timestamp`` plus one append-only ``Metadata`` receipt. The caller remains responsible for rechecking
owner stopped/lock/backup/integrity immediately before apply and for guarded restart.
"""

import hashlib
import json
import re
import sqlite3
import time
from decimal import Decimal, InvalidOperation

from controllers.generic.modular_spot_core.domain import identity
from hummingbot.strategy_v2.migrations.entry_risk import financial_state_digest

CONFIG_PATH = "rsi_modular_v2.yml"
MARKET = "okx"
PAIR = "ETH-USDC"
EXECUTOR_ID = "CdCG2PyZsSJXynkawLHxVkWWSVGbobZhWJ9jooTZVFhC"
TRADE_ID = "3249391"
QUANTITY = Decimal("0.021332")
MAX_VENUE_PROOF_AGE_MS = 300_000


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _decimal(value, name, *, positive=False):
    _require(not isinstance(value, bool), f"Invalid {name}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"Invalid {name}") from None
    _require(result.is_finite() and (not positive or result > 0), f"Invalid {name}")
    return result


def _stored_six_dp(value, name):
    parsed = _decimal(value, name)
    return parsed / Decimal(1_000_000) if type(value) is int else parsed


def _row(connection, table, where, values):
    columns = [entry[1] for entry in connection.execute(f'PRAGMA table_info("{table}")')]
    _require(columns, f"Native {table} table is required")
    result = connection.execute(f'SELECT * FROM "{table}" WHERE {where}', values).fetchall()
    return columns, [dict(zip(columns, item)) for item in result]


def _validate_venue_proof(proof, order_id, native_order, exact_order, native_fill, *, now_ms):
    _require(
        isinstance(proof, dict)
        and set(proof) == {"schema", "source", "observed_at_ms", "order", "fills"}
        and type(proof["schema"]) is int
        and proof["schema"] == 1
        and proof["source"] == "authenticated_okx_get"
        and type(proof["observed_at_ms"]) is int
        and 0 < proof["observed_at_ms"] <= now_ms
        and now_ms - proof["observed_at_ms"] <= MAX_VENUE_PROOF_AGE_MS,
        "Fresh authenticated OKX GET proof is required",
    )
    venue_order = proof["order"]
    _require(
        isinstance(venue_order, dict)
        and venue_order.get("ordId") == native_order["exchange_order_id"]
        and venue_order.get("clOrdId") == order_id
        and venue_order.get("instId") == PAIR
        and venue_order.get("side") == "buy"
        and venue_order.get("state") == "filled"
        and venue_order.get("tdMode") == "cash"
        and venue_order.get("ordType") == "post_only"
        and venue_order.get("instType", "SPOT") == "SPOT"
        and _decimal(venue_order.get("sz"), "venue submitted size", positive=True)
        == _decimal(exact_order["amount"], "native exact order size", positive=True)
        and _decimal(venue_order.get("px"), "venue order price", positive=True)
        == _decimal(exact_order["price"], "native exact order price", positive=True)
        and _decimal(venue_order.get("accFillSz"), "venue filled size", positive=True) == QUANTITY,
        "Venue order does not prove the exact filled acquisition",
    )
    if venue_order.get("avgPx") not in (None, ""):
        _require(
            _decimal(venue_order["avgPx"], "venue average price", positive=True)
            == _decimal(native_fill["exact_price"] or native_fill["price"], "native exact fill price", positive=True),
            "Venue average price differs from the exact native fill",
        )
    venue_fills = proof["fills"]
    _require(
        isinstance(venue_fills, list) and len(venue_fills) == 1 and isinstance(venue_fills[0], dict),
        "Venue fill proof must contain exactly one fill",
    )
    venue_fill = venue_fills[0]
    _require(
        str(venue_fill.get("tradeId")) == TRADE_ID
        and venue_fill.get("ordId") == native_order["exchange_order_id"]
        and venue_fill.get("instId") == PAIR
        and venue_fill.get("side") == "buy"
        and _decimal(venue_fill.get("fillSz"), "venue fill size", positive=True) == QUANTITY
        and _decimal(venue_fill.get("fillPx"), "venue fill price", positive=True)
        == _decimal(native_fill["exact_price"] or native_fill["price"], "native fill price", positive=True),
        "Venue fill differs from the native exact fill",
    )
    _require("fee" in venue_fill and "feeCcy" in venue_fill, "Venue fee proof is incomplete")
    try:
        native_fee = json.loads(native_fill["trade_fee"])
    except (TypeError, ValueError):
        raise ValueError("Native fill fee is malformed") from None
    fees = native_fee.get("flat_fees") if isinstance(native_fee, dict) else None
    _require(isinstance(fees, list) and len(fees) == 1, "One native flat fee is required")
    venue_paid_fee = -_decimal(venue_fill["fee"], "venue fee")
    native_paid_fee = _decimal(fees[0].get("amount"), "native fee", positive=True)
    _require(
        _decimal(native_fee.get("percent"), "native percent fee") == 0
        and fees[0].get("token") == venue_fill["feeCcy"]
        and venue_paid_fee > 0
        and native_paid_fee == venue_paid_fee,
        "Venue paid fee differs from the native fill",
    )


def _funding_release(connection, order_id):
    rows = connection.execute("SELECT key, value FROM Metadata ORDER BY key").fetchall()
    matches = []
    for key, value in rows:
        if not str(key).startswith("modular-candidate-funding:v1:") or ":release:" not in str(key):
            continue
        try:
            record = json.loads(value)
        except (TypeError, ValueError):
            continue
        if isinstance(record, dict) and record.get("executor_id") == EXECUTOR_ID:
            receipt = record.get("receipt")
            if isinstance(receipt, dict):
                matches.append((key, value, receipt))
    _require(len(matches) == 1, "One native OKX funding release for the target executor is required")
    key, value, receipt = matches[0]
    prefix = key.split(":release:", 1)[0]
    header_row = connection.execute("SELECT value FROM Metadata WHERE key=?", (prefix,)).fetchone()
    _require(header_row is not None, "Native funding checkpoint header is missing")
    try:
        header = json.loads(header_row[0])
    except (TypeError, ValueError):
        raise ValueError("Native funding checkpoint header is malformed") from None
    records = connection.execute(
        "SELECT key, value FROM Metadata WHERE key LIKE ? ORDER BY key", (prefix + ":%",)
    ).fetchall()
    claims, fill_hashes, releases = {}, {}, {}
    for record_key, raw in records:
        try:
            item = json.loads(raw)
        except (TypeError, ValueError):
            raise ValueError("Native funding audit row is malformed") from None
        kind = record_key.removeprefix(prefix + ":").split(":", 1)[0]
        _require(
            isinstance(item, dict) and item.get("binding") == header.get("binding"),
            "Native funding audit binding differs",
        )
        if kind == "claim":
            executor_id = item.get("executor_id")
            _require(
                isinstance(executor_id, str)
                and record_key == prefix + ":claim:" + identity(executor_id)
                and set(item) == {"binding", "executor_id", "claim"},
                "Native funding claim identity is malformed",
            )
            claims[executor_id] = item["claim"]
        elif kind == "fill":
            fill_key, fill_digest = item.get("fill_key"), item.get("digest")
            _require(
                set(item) == {"binding", "fill_key", "digest"}
                and isinstance(fill_key, str)
                and isinstance(fill_digest, str)
                and record_key == prefix + ":fill:" + fill_key,
                "Native funding fill audit is malformed",
            )
            fill_hashes[fill_key] = fill_digest
        elif kind == "release":
            executor_id = item.get("executor_id")
            _require(
                isinstance(executor_id, str)
                and record_key == prefix + ":release:" + identity(executor_id)
                and set(item) == {"binding", "executor_id", "receipt"},
                "Native funding release identity is malformed",
            )
            releases[executor_id] = item["receipt"]
        else:
            raise ValueError("Unknown native funding audit row")
    parts = [sorted(claims.items()), sorted(fill_hashes.items())]
    if releases:
        parts.append(sorted(releases.items()))
    _require(
        type(header.get("schema")) is int
        and header["schema"] == 1
        and type(header.get("revision")) is int
        and header["revision"] > 0
        and header.get("audit_sha256") == identity(parts)
        and EXECUTOR_ID in claims
        and EXECUTOR_ID in releases,
        "Native funding audit hash or executor admission differs",
    )
    if receipt.get("source") == "native_terminal_entry_set_v1":
        _require(
            set(receipt) == {"schema", "source", "orders"}
            and type(receipt.get("schema")) is int
            and receipt["schema"] == 1,
            "Native funding release proof set is malformed",
        )
        proofs = receipt["orders"]
    else:
        _require(
            set(receipt)
            == {
                "schema",
                "source",
                "order_id",
                "exchange_order_id",
                "trading_pair",
                "side",
                "amount",
                "filled_base",
                "state",
                "trade_ids",
            },
            "Native funding release proof is malformed",
        )
        proofs = [receipt]
    _require(isinstance(proofs, list) and proofs, "Native funding release proof set is malformed")
    target_proofs = [proof for proof in proofs if isinstance(proof, dict) and proof.get("order_id") == order_id]
    _require(len(target_proofs) == 1, "Native funding release omits or duplicates the target child")
    target = target_proofs[0]
    _require(
        target.get("source") == "okx_terminal_status_v1"
        and target.get("exchange_order_id")
        and target.get("trading_pair") == PAIR
        and target.get("side") == "BUY"
        and _decimal(target.get("filled_base"), "funding release quantity", positive=True) == QUANTITY
        and target.get("state") == "FILLED"
        and target.get("trade_ids") == [TRADE_ID],
        "Native funding release conflicts with the target order",
    )
    return {"key": key, "value_sha256": _sha(value), "receipt": receipt, "target_proof": target}


def _validate_target(connection, order_id, venue_proof, *, now_ms):
    _require(isinstance(order_id, str) and order_id and "\x00" not in order_id, "Exact native order id is required")
    order_columns, orders = _row(connection, "Order", '"id"=?', (order_id,))
    _require(len(orders) == 1, "Exact target native order must exist once")
    order = orders[0]
    _require(
        order.get("config_file_path") == CONFIG_PATH
        and order.get("market") == MARKET
        and order.get("symbol") == PAIR
        and order.get("base_asset") == "ETH"
        and order.get("quote_asset") == "USDC"
        and order.get("leverage") == 1
        and order.get("position") in (None, "", "NIL")
        and order.get("order_type") == "LIMIT_MAKER"
        and order.get("last_status") == "OrderFilled"
        and order.get("exchange_order_id"),
        "Native order owner or filled projection differs",
    )
    exact_columns, exact_orders = _row(connection, "OrderExact", '"order_id"=?', (order_id,))
    _require(
        len(exact_orders) == 1
        and _decimal(exact_orders[0].get("amount"), "exact order amount", positive=True) == QUANTITY,
        "Exact native order size differs",
    )
    _require(
        _stored_six_dp(order.get("amount"), "native stored order amount")
        == _decimal(exact_orders[0]["amount"], "exact order amount", positive=True)
        and _stored_six_dp(order.get("price"), "native stored order price")
        == _decimal(exact_orders[0]["price"], "exact order price", positive=True),
        "Native order columns conflict with their exact receipt",
    )
    status_columns, statuses = _row(connection, "OrderStatus", '"order_id"=? ORDER BY "timestamp", "id"', (order_id,))
    _require(
        any(status.get("status") == "BuyOrderCompleted" for status in statuses)
        and not any(
            status.get("status") in {"OrderCancelled", "OrderCanceled", "OrderFailure", "OrderExpired"}
            for status in statuses
        ),
        "Native terminal completion event is missing or conflicting",
    )
    completion_events = [status for status in statuses if status.get("status") == "BuyOrderCompleted"]
    latest_completion = (
        max((status["timestamp"], status["id"]) for status in completion_events) if completion_events else None
    )
    _require(
        latest_completion is not None
        and latest_completion[0] >= order.get("last_update_timestamp", 0)
        and not any(
            (status.get("timestamp", 0), status.get("id", 0)) > latest_completion
            and status.get("status") != "BuyOrderCompleted"
            for status in statuses
        ),
        "Native order has a later unresolved status event",
    )
    fill_columns, fills = _row(connection, "TradeFill", '"order_id"=?', (order_id,))
    _require(len(fills) == 1, "Exactly one native fill is required")
    fill = fills[0]
    _require(
        fill.get("config_file_path") == CONFIG_PATH
        and fill.get("market") == MARKET
        and fill.get("symbol") == PAIR
        and fill.get("base_asset") == "ETH"
        and fill.get("quote_asset") == "USDC"
        and fill.get("trade_type") == "BUY"
        and fill.get("leverage") == 1
        and str(fill.get("exchange_trade_id")) == TRADE_ID
        and fill.get("exact_receipt_source") == "order_filled_event_v1"
        and _decimal(fill.get("exact_amount"), "native exact fill amount", positive=True) == QUANTITY
        and _stored_six_dp(fill.get("amount"), "native stored fill amount")
        == _decimal(fill.get("exact_amount"), "native exact fill amount", positive=True)
        and _stored_six_dp(fill.get("price"), "native stored fill price")
        == _decimal(fill.get("exact_price"), "native exact fill price", positive=True),
        "Native fill owner, identity, or exact quantity differs",
    )
    exact_fill_columns, exact_fills = _row(
        connection,
        "TradeFillExact",
        '"market"=? AND "order_id"=? AND "exchange_trade_id"=?',
        (MARKET, order_id, TRADE_ID),
    )
    _require(
        len(exact_fills) == 1
        and _decimal(exact_fills[0].get("amount"), "companion fill amount", positive=True) == QUANTITY
        and _decimal(exact_fills[0].get("price"), "companion fill price", positive=True)
        == _decimal(fill.get("exact_price"), "native exact price", positive=True)
        and exact_fills[0].get("fee_in_quote") is not None
        and _decimal(exact_fills[0]["fee_in_quote"], "companion exact fee")
        == _decimal(fill.get("exact_trade_fee_in_quote"), "native exact fee"),
        "Companion exact fill receipt is missing or conflicts",
    )
    executor_columns, executors = _row(connection, "Executors", '"id"=?', (EXECUTOR_ID,))
    _require(len(executors) == 1, "Native acquisition executor is missing")
    executor = executors[0]
    try:
        config, custom = json.loads(executor["config"]), json.loads(executor["custom_info"])
    except (TypeError, ValueError):
        raise ValueError("Native acquisition executor evidence is malformed") from None
    snapshots = custom.get("execution_order_snapshots")
    _require(
        executor.get("type") == "position_executor"
        and executor.get("controller_id")
        and config.get("controller_id") == executor.get("controller_id")
        and config.get("trading_pair") == PAIR
        and config.get("connector_name") == MARKET
        and config.get("entry_chain_contract") == "v2_bounded_acquisition_v1"
        and config.get("entry_acquisition_policy") == "down_only_maker_v1"
        and order_id in custom.get("order_ids", [])
        and isinstance(snapshots, list)
        and len(snapshots) >= 1
        and all(isinstance(item, dict) for item in snapshots),
        "Native executor does not own the filled acquisition order",
    )
    matching_snapshots = [
        item for item in snapshots if item.get("client_order_id") == order_id and item.get("trade_type") == "BUY"
    ]
    _require(len(matching_snapshots) == 1, "Native acquisition child snapshot is missing or ambiguous")
    snapshot = matching_snapshots[0]
    _require(
        snapshot.get("client_order_id") == order_id
        and snapshot.get("exchange_order_id") == order.get("exchange_order_id")
        and snapshot.get("trading_pair") == PAIR
        and snapshot.get("trade_type") == "BUY"
        and snapshot.get("last_state") in ("FILLED", "5")
        and snapshot.get("order_type") == "LIMIT_MAKER"
        and _decimal(snapshot.get("amount"), "executor order amount", positive=True) == QUANTITY
        and _decimal(snapshot.get("executed_amount_base"), "executor filled amount", positive=True) == QUANTITY
        and snapshot.get("recorder_pending_events") in (None, [], {})
        and list((snapshot.get("order_fills") or {}).keys()) == [TRADE_ID],
        "Native acquisition child is open, unresolved, or differs from its exact fill",
    )
    release = _funding_release(connection, order_id)
    _validate_native_acquisition_chain(config, custom, release["receipt"])
    _validate_venue_proof(venue_proof, order_id, order, exact_orders[0], fill, now_ms=now_ms)
    return {
        "order_columns": order_columns,
        "order": order,
        "statuses": statuses,
        "completion_timestamp": latest_completion[0],
        "fills": fills,
        "exact_order": exact_orders[0],
        "exact_fill": exact_fills[0],
        "executor": executor,
        "funding_release": release,
    }


def _validate_native_acquisition_chain(config, custom, terminal_release):
    """Use the production validator as the authority for acquisition-chain semantics."""
    try:
        from hummingbot.strategy_v2.executors.acquisition_chain import validate_acquisition_chain
        from hummingbot.strategy_v2.executors.position_executor.data_types import parse_position_config

        validate_acquisition_chain(parse_position_config(config), custom, terminal_release)
    except (ImportError, AttributeError, TypeError, ValueError, KeyError) as error:
        raise ValueError("Native acquisition-chain validation failed") from error


def _plan_order_projection_snapshot(connection, *, order_id, venue_proof, now_ms):
    evidence = _validate_target(connection, order_id, venue_proof, now_ms=now_ms)
    _require(
        not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name IN (?, ?) LIMIT 1", ("Order", "Metadata")
        ).fetchone(),
        "Order or Metadata triggers make the repair delta ambiguous",
    )
    after_order = dict(
        evidence["order"], last_status="BuyOrderCompleted", last_update_timestamp=evidence["completion_timestamp"]
    )
    receipt_key = "v2-order-projection:v1:" + _sha(order_id)
    _require(
        connection.execute("SELECT 1 FROM Metadata WHERE key=?", (receipt_key,)).fetchone() is None,
        "A projection receipt already exists; use the original plan to verify replay",
    )
    body = {
        "schema": 1,
        "kind": "v2_filled_order_projection",
        "order_id": order_id,
        "before_financial_state_sha256": financial_state_digest(connection),
        "before_order": evidence["order"],
        "after_order": after_order,
        "order_columns": evidence["order_columns"],
        "receipt_key": receipt_key,
        "funding_release": evidence["funding_release"],
        "venue_proof_sha256": _sha(_canonical(venue_proof)),
        "venue_proof": venue_proof,
        "now_ms": now_ms,
    }
    body["plan_sha256"] = _sha(_canonical(body))
    return body


def plan_order_projection(connection, *, order_id, venue_proof, now_ms=None):
    """Create a coherent read-only proposal in a module-owned SQLite transaction."""
    _require(not connection.in_transaction, "Planning requires an idle SQLite connection")
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    _require(type(now_ms) is int and now_ms > 0, "Planning clock is invalid")
    connection.execute("BEGIN")
    try:
        plan = _plan_order_projection_snapshot(connection, order_id=order_id, venue_proof=venue_proof, now_ms=now_ms)
        connection.commit()
        return plan
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise


def _verify_plan_hash(plan, expected):
    _require(
        isinstance(plan, dict) and isinstance(expected, str) and re.fullmatch("[a-f0-9]{64}", expected),
        "Expected plan digest is required",
    )
    body = dict(plan)
    actual = body.pop("plan_sha256", None)
    _require(actual == expected == _sha(_canonical(body)), "Order projection plan digest differs")


def _restored_financial_digest(connection, plan):
    """Digest all protected native tables after undoing only the planned projection on a memory copy."""
    clone = sqlite3.connect(":memory:")
    try:
        connection.backup(clone)
        clone.execute(
            'UPDATE "Order" SET last_status=?, last_update_timestamp=? WHERE id=?',
            (plan["before_order"]["last_status"], plan["before_order"]["last_update_timestamp"], plan["order_id"]),
        )
        clone.execute("DELETE FROM Metadata WHERE key=?", (plan["receipt_key"],))
        return financial_state_digest(clone)
    finally:
        clone.close()


def apply_order_projection(connection, *, plan, expected_plan_sha256, now_ms=None):
    """CAS-apply a reviewed plan atomically; a repeated call verifies committed state."""
    _verify_plan_hash(plan, expected_plan_sha256)
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    _require(type(now_ms) is int and now_ms >= plan["now_ms"], "Apply clock is invalid")
    receipt_key = plan["receipt_key"]
    if not connection.in_transaction:
        connection.execute("BEGIN IMMEDIATE")
    else:
        raise ValueError("Apply requires an idle connection so it can own BEGIN IMMEDIATE")
    try:
        _require(
            not connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name IN (?, ?) LIMIT 1", ("Order", "Metadata")
            ).fetchone(),
            "Order or Metadata triggers make the repair delta ambiguous",
        )
        old_receipt = connection.execute("SELECT value FROM Metadata WHERE key=?", (receipt_key,)).fetchone()
        if old_receipt is not None:
            receipt = json.loads(old_receipt[0])
            _require(
                receipt.get("plan_sha256") == expected_plan_sha256 and receipt.get("order_id") == plan["order_id"],
                "Existing order projection receipt differs",
            )
            columns = plan["order_columns"]
            current = connection.execute('SELECT * FROM "Order" WHERE id=?', (plan["order_id"],)).fetchone()
            _require(
                current is not None and dict(zip(columns, current)) == plan["after_order"],
                "Committed order projection after-state differs",
            )
            connection.commit()
            _require(
                _restored_financial_digest(connection, plan) == plan["before_financial_state_sha256"],
                "Committed replay changed protected native rows",
            )
            return {"status": "already_applied", "receipt": receipt}
        _require(
            financial_state_digest(connection) == plan["before_financial_state_sha256"],
            "Financial state changed after order projection planning",
        )
        current_evidence = _validate_target(connection, plan["order_id"], plan["venue_proof"], now_ms=now_ms)
        _require(
            current_evidence["order"] == plan["before_order"]
            and current_evidence["funding_release"] == plan["funding_release"],
            "Target order evidence changed after planning",
        )
        # Exact projection update. The native event and fill rows
        # remain untouched; the audit receipt is the only added row.
        cursor = connection.execute(
            'UPDATE "Order" SET last_status=?, last_update_timestamp=? WHERE id=? '
            "AND last_status=? AND last_update_timestamp=?",
            (
                "BuyOrderCompleted",
                plan["after_order"]["last_update_timestamp"],
                plan["order_id"],
                "OrderFilled",
                plan["before_order"]["last_update_timestamp"],
            ),
        )
        _require(cursor.rowcount == 1, "Target projection compare-and-swap failed")
        receipt = {
            "schema": 1,
            "kind": plan["kind"],
            "order_id": plan["order_id"],
            "plan_sha256": expected_plan_sha256,
            "before_financial_state_sha256": plan["before_financial_state_sha256"],
            "venue_proof_sha256": plan["venue_proof_sha256"],
            "applied_at_ms": now_ms,
        }
        connection.execute("INSERT INTO Metadata(key, value) VALUES (?, ?)", (receipt_key, _canonical(receipt)))
        connection.commit()
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    # Verify the committed projection on the same connection after COMMIT.
    columns = plan["order_columns"]
    actual_order = connection.execute('SELECT * FROM "Order" WHERE id=?', (plan["order_id"],)).fetchone()
    _require(
        actual_order is not None and dict(zip(columns, actual_order)) == plan["after_order"],
        "Committed order projection after-state differs",
    )
    stored = connection.execute("SELECT value FROM Metadata WHERE key=?", (receipt_key,)).fetchone()
    _require(stored is not None and json.loads(stored[0]) == receipt, "Committed immutable receipt differs")
    _require(
        _restored_financial_digest(connection, plan) == plan["before_financial_state_sha256"],
        "Committed projection changed protected native rows",
    )
    return {"status": "applied", "receipt": receipt}
