import json
import sqlite3

import pytest

from hummingbot.strategy_v2.components.domain import identity
from hummingbot.strategy_v2.migrations import order_projection
from hummingbot.strategy_v2.migrations.order_projection import apply_order_projection, plan_order_projection

ORDER_ID = "client-order-1"
EXECUTOR_ID = "CdCG2PyZsSJXynkawLHxVkWWSVGbobZhWJ9jooTZVFhC"
CONFIG_PATH = "rsi_modular_v2.yml"


def database():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE Metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
        CREATE TABLE "Order" (id TEXT PRIMARY KEY, config_file_path TEXT, strategy TEXT, market TEXT,
          symbol TEXT, base_asset TEXT, quote_asset TEXT, creation_timestamp INTEGER, order_type TEXT,
          amount TEXT, leverage INTEGER, price TEXT, last_status TEXT, last_update_timestamp INTEGER,
          exchange_order_id TEXT, position TEXT);
        CREATE TABLE OrderStatus (id INTEGER PRIMARY KEY, order_id TEXT, timestamp INTEGER, status TEXT);
        CREATE TABLE TradeFill (config_file_path TEXT, strategy TEXT, market TEXT, symbol TEXT,
          base_asset TEXT, quote_asset TEXT, timestamp INTEGER, order_id TEXT, trade_type TEXT,
          order_type TEXT, price TEXT, amount TEXT, leverage INTEGER, trade_fee TEXT,
          trade_fee_in_quote TEXT, exact_amount TEXT, exact_price TEXT, exact_trade_fee_in_quote TEXT,
          exact_receipt_source TEXT, execution_timestamp_ms INTEGER, execution_timestamp_source TEXT,
          exchange_trade_id TEXT, position TEXT);
        CREATE TABLE Executors (id TEXT PRIMARY KEY, config TEXT, custom_info TEXT, controller_id TEXT,
          type TEXT, is_active INTEGER, status INTEGER);
        CREATE TABLE OrderExact (order_id TEXT PRIMARY KEY, amount TEXT, price TEXT);
        CREATE TABLE TradeFillExact (market TEXT, order_id TEXT, exchange_trade_id TEXT,
          amount TEXT, price TEXT, fee_in_quote TEXT);
    """
    )
    connection.execute(
        """INSERT INTO "Order" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            ORDER_ID,
            CONFIG_PATH,
            "pure_market_making",
            "okx",
            "ETH-USDC",
            "ETH",
            "USDC",
            1000,
            "LIMIT_MAKER",
            "0.021332",
            1,
            "3000",
            "OrderFilled",
            2000,
            "venue-order-7",
            "NIL",
        ),
    )
    connection.execute("INSERT INTO OrderStatus VALUES (1, ?, 2001, 'BuyOrderCompleted')", (ORDER_ID,))
    connection.execute(
        """INSERT INTO TradeFill VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            CONFIG_PATH,
            "pure_market_making",
            "okx",
            "ETH-USDC",
            "ETH",
            "USDC",
            2000,
            ORDER_ID,
            "BUY",
            "LIMIT_MAKER",
            "3000",
            "0.021332",
            1,
            '{"fee_type":"AddedToCost","flat_fees":[{"amount":"0.0000170656","token":"ETH"}],"percent":"0","percent_token":"ETH"}',
            "0.040444448064",
            "0.021332",
            "3000",
            "0.040444448064",
            "order_filled_event_v1",
            2000,
            "exchange",
            "3249391",
            "NIL",
        ),
    )
    connection.execute("INSERT INTO OrderExact VALUES (?, ?, ?)", (ORDER_ID, "0.021332", "3000"))
    connection.execute(
        "INSERT INTO TradeFillExact VALUES (?, ?, ?, ?, ?, ?)",
        ("okx", ORDER_ID, "3249391", "0.021332", "3000", "0.040444448064"),
    )
    connection.execute(
        "INSERT INTO Executors VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            EXECUTOR_ID,
            json.dumps(
                {
                    "id": EXECUTOR_ID,
                    "type": "position_executor",
                    "controller_id": "eth-owner",
                    "connector_name": "okx",
                    "trading_pair": "ETH-USDC",
                    "side": 1,
                    "leverage": 1,
                    "timestamp": 1000,
                    "entry_price": "3000",
                    "amount": "0.021332",
                    "triple_barrier_config": {"open_order_type": 3},
                    "entry_order_policy": "single_attempt",
                    "require_terminal_entry_reconciliation": True,
                    "entry_acquisition_policy": "down_only_maker_v1",
                    "entry_chain_contract": "v2_bounded_acquisition_v1",
                    "decision_best_bid": "3000",
                    "decision_best_ask": "3001",
                    "decision_atr_1m": "1",
                    "decision_price_step": "0.01",
                    "maximum_acceptable_price": "3000",
                    "entry_total_max_age_seconds": 300,
                    "entry_max_downward_reprices": 3,
                    "entry_reprice_cooldown_seconds": 5,
                    "entry_reprice_min_ticks": 3,
                    "entry_reprice_atr_fraction": "0.05",
                }
            ),
            json.dumps(
                {
                    "order_ids": [ORDER_ID],
                    "acquisition_chain": {
                        "schema": 1,
                        "contract": "v2_bounded_acquisition_v1",
                        "parent_id": EXECUTOR_ID,
                        "authorized_base": "0.021332",
                        "maximum_price": "3000",
                        "children": [
                            {
                                "generation": 0,
                                "order_id": ORDER_ID,
                                "predecessor_id": None,
                                "purpose": "initial",
                                "submitted_base": "0.021332",
                                "limit_price": "3000",
                            }
                        ],
                        "successor_pending": None,
                        "membership_closed": True,
                        "gross_filled_base": "0.021332",
                    },
                    "execution_order_snapshots": [
                        {
                            "client_order_id": ORDER_ID,
                            "exchange_order_id": "venue-order-7",
                            "trading_pair": "ETH-USDC",
                            "order_type": "LIMIT_MAKER",
                            "trade_type": "BUY",
                            "price": "3000",
                            "amount": "0.021332",
                            "executed_amount_base": "0.021332",
                            "executed_amount_quote": "63.996",
                            "last_state": "5",
                            "leverage": "1",
                            "position": "NIL",
                            "creation_timestamp": 1000,
                            "recorder_pending_events": {},
                            "order_fills": {
                                "3249391": {
                                    "trade_id": "3249391",
                                    "client_order_id": ORDER_ID,
                                    "exchange_order_id": "venue-order-7",
                                    "trading_pair": "ETH-USDC",
                                    "fill_timestamp": 2000,
                                    "fill_price": "3000",
                                    "fill_base_amount": "0.021332",
                                    "fill_quote_amount": "63.996",
                                    "fee": {
                                        "fee_type": "AddedToCost",
                                        "percent": "0",
                                        "percent_token": "ETH",
                                        "flat_fees": [{"token": "ETH", "amount": "0.0000170656"}],
                                    },
                                }
                            },
                        }
                    ],
                }
            ),
            "eth-owner",
            "position_executor",
            0,
            3,
        ),
    )
    release = {
        "schema": 1,
        "source": "okx_terminal_status_v1",
        "order_id": ORDER_ID,
        "exchange_order_id": "venue-order-7",
        "trading_pair": "ETH-USDC",
        "side": "BUY",
        "amount": "0.021332",
        "filled_base": "0.021332",
        "state": "FILLED",
        "trade_ids": ["3249391"],
    }
    claim = {"config": "b" * 64, "amount": "0.021332", "price": "3000", "fee_fraction": "0.01"}
    binding = "c" * 64
    claim_key = "modular-candidate-funding:v1:fixture:claim:" + identity(EXECUTOR_ID)
    release_key = "modular-candidate-funding:v1:fixture:release:" + identity(EXECUTOR_ID)
    release_record = {"binding": binding, "executor_id": EXECUTOR_ID, "receipt": release}
    connection.execute(
        "INSERT INTO Metadata VALUES (?, ?)",
        (
            "modular-candidate-funding:v1:fixture",
            json.dumps(
                {
                    "schema": 1,
                    "binding": binding,
                    "revision": 1,
                    "audit_sha256": identity([[(EXECUTOR_ID, claim)], [], [(EXECUTOR_ID, release)]]),
                }
            ),
        ),
    )
    connection.execute(
        "INSERT INTO Metadata VALUES (?, ?)",
        (claim_key, json.dumps({"binding": binding, "executor_id": EXECUTOR_ID, "claim": claim})),
    )
    connection.execute("INSERT INTO Metadata VALUES (?, ?)", (release_key, json.dumps(release_record)))
    connection.commit()
    return connection


def venue_proof(**overrides):
    value = {
        "schema": 1,
        "source": "authenticated_okx_get",
        "observed_at_ms": 3000,
        "order": {
            "ordId": "venue-order-7",
            "clOrdId": ORDER_ID,
            "instId": "ETH-USDC",
            "side": "buy",
            "state": "filled",
            "tdMode": "cash",
            "ordType": "post_only",
            "instType": "SPOT",
            "sz": "0.021332",
            "accFillSz": "0.021332",
            "px": "3000",
            "avgPx": "3000",
        },
        "fills": [
            {
                "tradeId": "3249391",
                "ordId": "venue-order-7",
                "instId": "ETH-USDC",
                "side": "buy",
                "fillSz": "0.021332",
                "fillPx": "3000",
                "fee": "-0.0000170656",
                "feeCcy": "ETH",
            }
        ],
    }
    value.update(overrides)
    return value


@pytest.fixture(autouse=True)
def use_native_chain_adapter_in_unit_fixture(monkeypatch):
    # This host lacks Hummingbot's compiled limit_order extension. The native
    # validator itself is separately exercised in the deployed image test.
    def validate(config, custom_info, proof):
        assert config["entry_chain_contract"] == "v2_bounded_acquisition_v1"
        assert custom_info["acquisition_chain"]["membership_closed"] is True
        assert custom_info["acquisition_chain"]["successor_pending"] is None
        assert proof["source"] == "okx_terminal_status_v1"

    monkeypatch.setattr(order_projection, "_validate_native_acquisition_chain", validate)


def test_plan_then_apply_is_projection_only_and_replay_is_idempotent():
    connection = database()
    plan = plan_order_projection(connection, order_id=ORDER_ID, venue_proof=venue_proof(), now_ms=4000)
    before = tuple(connection.execute('SELECT * FROM "Order"').fetchone())
    result = apply_order_projection(connection, plan=plan, expected_plan_sha256=plan["plan_sha256"], now_ms=4000)
    after = tuple(connection.execute('SELECT * FROM "Order"').fetchone())
    assert after[:12] == before[:12] and after[14:] == before[14:]
    assert after[12] == "BuyOrderCompleted"
    assert after[13] == 2001
    assert connection.execute("SELECT COUNT(*) FROM OrderStatus").fetchone()[0] == 1
    assert connection.execute("SELECT COUNT(*) FROM TradeFill").fetchone()[0] == 1
    assert result["status"] == "applied"
    replay = apply_order_projection(connection, plan=plan, expected_plan_sha256=plan["plan_sha256"], now_ms=4000)
    assert replay["status"] == "already_applied"
    assert connection.execute("SELECT COUNT(*) FROM Metadata WHERE key LIKE 'v2-order-projection:%'").fetchone()[0] == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "owner",
        "pair",
        "missing_event",
        "conflicting_terminal",
        "quantity",
        "venue",
        "wrong_mode",
        "wrong_type",
        "wrong_price",
        "missing_fee",
        "positive_fee",
    ],
)
def test_plan_rejects_unproved_or_conflicting_target(mutation):
    connection = database()
    proof = venue_proof()
    if mutation == "owner":
        connection.execute('UPDATE "Order" SET config_file_path=?', ("other.yml",))
    elif mutation == "pair":
        connection.execute('UPDATE "Order" SET symbol=?', ("BTC-USDC",))
    elif mutation == "missing_event":
        connection.execute("DELETE FROM OrderStatus")
    elif mutation == "conflicting_terminal":
        connection.execute("INSERT INTO OrderStatus VALUES (2, ?, 2001, 'OrderCancelled')", (ORDER_ID,))
    elif mutation == "quantity":
        connection.execute("UPDATE TradeFill SET amount='0.02'")
    elif mutation == "venue":
        proof["order"]["state"] = "canceled"
    elif mutation == "wrong_mode":
        proof["order"]["tdMode"] = "cross"
    elif mutation == "wrong_type":
        proof["order"]["ordType"] = "limit"
    elif mutation == "wrong_price":
        proof["order"]["px"] = "2999"
    elif mutation == "missing_fee":
        proof["fills"][0].pop("fee")
    elif mutation == "positive_fee":
        proof["fills"][0]["fee"] = "0.0000170656"
    with pytest.raises(ValueError):
        plan_order_projection(connection, order_id=ORDER_ID, venue_proof=proof, now_ms=4000)


def test_apply_requires_plan_cas_and_rolls_back_on_append_failure():
    connection = database()
    plan = plan_order_projection(connection, order_id=ORDER_ID, venue_proof=venue_proof(), now_ms=4000)
    with pytest.raises(ValueError):
        apply_order_projection(connection, plan=plan, expected_plan_sha256="0" * 64)
    connection.execute(
        "CREATE TRIGGER reject_receipt BEFORE INSERT ON Metadata WHEN NEW.key LIKE 'v2-order-projection:%' BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with pytest.raises(Exception):
        apply_order_projection(connection, plan=plan, expected_plan_sha256=plan["plan_sha256"], now_ms=4000)
    assert connection.execute('SELECT last_status FROM "Order"').fetchone()[0] == "OrderFilled"
    assert connection.execute("SELECT COUNT(*) FROM Metadata WHERE key LIKE 'v2-order-projection:%'").fetchone()[0] == 0


def test_plan_and_apply_refuse_caller_owned_sqlite_transactions():
    connection = database()
    plan = plan_order_projection(connection, order_id=ORDER_ID, venue_proof=venue_proof(), now_ms=4000)
    connection.execute("BEGIN IMMEDIATE")
    with pytest.raises(ValueError, match="idle connection"):
        apply_order_projection(connection, plan=plan, expected_plan_sha256=plan["plan_sha256"], now_ms=4000)
    connection.rollback()
    connection.execute("BEGIN IMMEDIATE")
    with pytest.raises(ValueError, match="idle SQLite"):
        plan_order_projection(connection, order_id=ORDER_ID, venue_proof=venue_proof(), now_ms=4000)
    connection.rollback()


def test_changed_protected_state_after_plan_fails_digest_cas():
    connection = database()
    plan = plan_order_projection(connection, order_id=ORDER_ID, venue_proof=venue_proof(), now_ms=4000)
    connection.execute("UPDATE OrderStatus SET timestamp=timestamp+1 WHERE id=1")
    connection.commit()
    with pytest.raises(ValueError, match="Financial state changed"):
        apply_order_projection(connection, plan=plan, expected_plan_sha256=plan["plan_sha256"], now_ms=4000)
    assert connection.execute('SELECT last_status FROM "Order"').fetchone()[0] == "OrderFilled"


def test_later_pending_status_event_is_rejected():
    connection = database()
    connection.execute("INSERT INTO OrderStatus VALUES (2, ?, 2001, 'BuyOrderCreated')", (ORDER_ID,))
    connection.commit()
    with pytest.raises(ValueError, match="later unresolved"):
        plan_order_projection(connection, order_id=ORDER_ID, venue_proof=venue_proof(), now_ms=4000)
