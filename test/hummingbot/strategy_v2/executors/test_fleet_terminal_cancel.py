"""Owner snapshot cancellation is zero exposure only with native terminal proof."""
import json
import sqlite3
from decimal import Decimal
from test.hummingbot.strategy_v2.executors.test_planned_position_resume import write_fleet_owner_snapshot

import pytest

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.strategy_v2.executors import planned_position_resume as resume


def canceled_predecessor(tmp_path):
    path = write_fleet_owner_snapshot(tmp_path / "owner.sqlite", "meridian_v3", active_amount=".25")
    canceled = InFlightOrder("canceled-predecessor", "BNB-USDC", OrderType.LIMIT, TradeType.BUY,
                             Decimal(".25"), 1, Decimal("100"), exchange_order_id="canceled-venue")
    canceled.current_state = OrderState.CANCELED
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE OrderStatus (id INTEGER PRIMARY KEY, order_id TEXT, timestamp INTEGER, status TEXT)")
        connection.row_factory = sqlite3.Row
        order = dict(connection.execute('SELECT * FROM "Order" LIMIT 1').fetchone())
        order.update(id="canceled-predecessor", exchange_order_id="canceled-venue", last_status="OrderCancelled")
        connection.execute('INSERT INTO "Order" (' + ','.join('"' + k + '"' for k in order) + ') VALUES ('
                           + ','.join('?' for _ in order) + ')', tuple(order.values()))
        connection.execute("INSERT INTO OrderStatus VALUES (1, ?, ?, 'OrderCancelled')",
                           (order["id"], order["last_update_timestamp"]))
        custom = json.loads(connection.execute("SELECT custom_info FROM Executors").fetchone()[0])
        custom["execution_order_snapshots"].insert(0, canceled.to_json())
        custom["order_ids"].insert(0, canceled.client_order_id)
        connection.execute("UPDATE Executors SET custom_info=?", (json.dumps(custom),))
    return path


def test_zero_fill_canceled_predecessor_preserves_replacement_inventory(tmp_path):
    path = canceled_predecessor(tmp_path)
    before = path.read_bytes()
    result = resume.inspect_fleet_owner_snapshot(path, "meridian_v3")
    assert result["owned_residual_by_asset"] == {"BNB": "0.25"}
    assert path.read_bytes() == before


@pytest.mark.parametrize("corruption", ["open", "base", "quote", "fill", "wrong_owner", "wrong_pair",
                                        "wrong_venue_id", "not_cancelled", "missing_event", "wrong_event_clock"])
def test_uncorroborated_cancellation_is_rejected(tmp_path, corruption):
    path = canceled_predecessor(tmp_path)
    with sqlite3.connect(path) as connection:
        custom = json.loads(connection.execute("SELECT custom_info FROM Executors").fetchone()[0])
        order = custom["execution_order_snapshots"][0]
        if corruption == "open":
            order["last_state"] = str(OrderState.OPEN.value)
        elif corruption in ("base", "quote"):
            order["executed_amount_" + corruption] = "0.01"
        elif corruption == "fill":
            # A receipt hidden behind zero cumulative totals must not be ignored.
            order["order_fills"] = custom["execution_order_snapshots"][1]["order_fills"]
        elif corruption == "wrong_owner":
            connection.execute('UPDATE "Order" SET config_file_path="foreign.yml" WHERE id="canceled-predecessor"')
        elif corruption == "wrong_pair":
            connection.execute('UPDATE "Order" SET symbol="ETH-USDC" WHERE id="canceled-predecessor"')
        elif corruption == "wrong_venue_id":
            connection.execute('UPDATE "Order" SET exchange_order_id="foreign-venue" WHERE id="canceled-predecessor"')
        elif corruption == "not_cancelled":
            connection.execute('UPDATE "Order" SET last_status="BuyOrderCreated" WHERE id="canceled-predecessor"')
        elif corruption == "missing_event":
            connection.execute("DELETE FROM OrderStatus")
        else:
            connection.execute("UPDATE OrderStatus SET timestamp=0")
        connection.execute("UPDATE Executors SET custom_info=?", (json.dumps(custom),))
    with pytest.raises((ValueError, AssertionError)):
        resume.inspect_fleet_owner_snapshot(path, "meridian_v3")


def test_held_projection_excludes_active_inventory_of_the_same_asset(tmp_path):
    path = write_fleet_owner_snapshot(tmp_path / "mixed.sqlite", "rsi_modular_v2", active_amount=".25")
    result = resume.inspect_fleet_owner_snapshot(path, "rsi_modular_v2")
    # .25 held buy minus .001 base fee, plus independent .25 active buy.
    assert result["owned_residual_by_asset"] == {"BNB": "0.499"}


def test_active_inventory_cannot_hide_corrupted_held_projection(tmp_path):
    path = write_fleet_owner_snapshot(tmp_path / "mixed.sqlite", "rsi_modular_v2", active_amount=".25")
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE Position SET amount=499000")
    with pytest.raises(ValueError, match="position projection"):
        resume.inspect_fleet_owner_snapshot(path, "rsi_modular_v2")
