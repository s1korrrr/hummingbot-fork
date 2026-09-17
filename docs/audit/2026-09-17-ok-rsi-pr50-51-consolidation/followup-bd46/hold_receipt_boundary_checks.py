"""Source-extracted controller checks with REAL SQLAlchemy/SQLite round trips.

Run inside the Hummingbot checkout:
    RSIBOT_SOURCE_ROOT="$PWD" python -m pytest -q path/to/this_file.py

The two controller methods and decimal validation are extracted from source;
no bot/connector is instantiated. Recorder availability and executor terminal
ownership are deliberately supplied by the harness. This is a persistence
boundary counterprobe, NOT the native Hummingbot suite or exchange validation.
"""
import ast
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest
from sqlalchemy import JSON, BigInteger, Column, Index, Text, TypeDecorator, create_engine
from sqlalchemy.orm import Session, declarative_base

ROOT = Path(os.environ.get("RSIBOT_SOURCE_ROOT", ".")).resolve()
FIELDS = ("executed_amount_base", "executed_amount_quote", "cumulative_fee_paid_quote")


def _node(path, name):
    tree = ast.parse((ROOT / path).read_text())
    return next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)


def _compile(nodes, namespace, filename):
    tree = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(tree)
    exec(compile(tree, filename, "exec"), namespace)


@pytest.fixture(scope="module")
def classes():
    namespace = dict(Decimal=Decimal, InvalidOperation=InvalidOperation, TypeDecorator=TypeDecorator,
                     JSON=JSON, BigInteger=BigInteger, Column=Column, Index=Index, Text=Text,
                     Any=Any, Dict=Dict, List=List, Optional=Optional, Tuple=Tuple,
                     HummingbotBase=declarative_base())
    path = "hummingbot/model/decimal_type_decorator.py"
    _compile([_node(path, "SqliteDecimal")], namespace, path)
    path = "hummingbot/model/position_hold_ledger.py"
    _compile([_node(path, "PositionHoldLedger")], namespace, path)
    path = "controllers/directional_trading/hl_rsi.py"
    controller = _node(path, "HLRSIController")
    wanted = {"_executor_hold_required_fills", "_executor_hold_released_to_bag",
              "_finite_nonnegative_base", "_hold_ledger_cumulative_receipt"}
    controller.body = [n for n in controller.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    controller.bases = []
    controller.decorator_list = []
    _compile([controller], namespace, path)
    cls = namespace["HLRSIController"]

    def parse(value):
        try:
            result = Decimal(str(value))
            return result if result.is_finite() else None
        except (InvalidOperation, ValueError, TypeError):
            return None

    cls._decimal_or_none = staticmethod(parse)
    cls._executor_custom_info = lambda self, ex: ex.custom_info
    cls._executor_claims_held_position = lambda self, ex: ex.hold_claim
    cls._executor_is_done = lambda self, ex: ex.is_done
    cls._hold_ledger_rows = lambda self: self.rows
    return cls, namespace["PositionHoldLedger"], namespace["HummingbotBase"]


@pytest.fixture
def boundary(classes):
    controller_cls, model, base = classes
    engine = create_engine("sqlite:///:memory:")
    base.metadata.create_all(engine)
    controller = controller_cls()
    controller.config = SimpleNamespace(id="ok-rsi-eth", connector_name="okx", trading_pair="ETH-USDC")
    controller.rows = []

    def write_read(raw, *, payload_override=None, column_override=None, row_id="ledger-1"):
        payload = dict(raw) if payload_override is None else payload_override
        columns = {field: Decimal(str(raw.get(field, "0"))) for field in FIELDS}
        columns.update(column_override or {})
        row = model(id=row_id, controller_id="ok-rsi-eth", executor_id="executor-1", connector_name="okx",
                    trading_pair="ETH-USDC", position_side="BUY", order_side="BUY", timestamp=1000,
                    client_order_id="order-1", exchange_order_id="venue-1", price=Decimal("2379.63"),
                    amount=columns[FIELDS[0]], order_payload=payload, **columns)
        with Session(engine) as session:
            session.add(row)
            session.commit()
        # NEW session: never compare the unpersisted identity-map object.
        with Session(engine) as session:
            persisted = session.get(model, row_id)
            session.expunge(persisted)
        controller.rows = [persisted]
        return persisted

    yield controller, write_read
    engine.dispose()


def fill(base="0.028006", quote="66.644", fee="0.01"):
    return dict(client_order_id="order-1", trade_type="BUY", executed_amount_base=base,
                executed_amount_quote=quote, cumulative_fee_paid_quote=fee)


def claimant(*orders):
    return SimpleNamespace(id="executor-1", is_done=True, hold_claim=True, side="BUY",
                           custom_info={"held_position_orders": list(orders)})


def test_six_place_exact_round_trip_is_accepted(boundary):
    controller, write_read = boundary
    raw = fill()
    write_read(raw)
    assert controller._executor_hold_released_to_bag(claimant(raw)) is True


@pytest.mark.parametrize("field,value", [
    ("executed_amount_base", "0.02800601"),
    ("executed_amount_quote", "66.64391778"),
    ("cumulative_fee_paid_quote", "0.06664391778"),
])
def test_exact_payload_survives_scaled_column_round_trip(boundary, field, value):
    controller, write_read = boundary
    raw = fill()
    raw[field] = value
    row = write_read(raw)
    assert row.order_payload[field] == value
    assert getattr(row, field) != Decimal(value), "Test must exercise actual precision loss"
    assert controller._executor_hold_released_to_bag(claimant(raw)) is True


@pytest.mark.parametrize("missing", [None, ""])
def test_unknown_amount_in_second_held_order_cannot_be_skipped(boundary, missing):
    controller, write_read = boundary
    raw = fill()
    write_read(raw)
    unknown = dict(client_order_id="order-2", executed_amount_base=missing)
    assert controller._executor_hold_released_to_bag(claimant(raw, unknown)) is False


def test_explicit_zero_fill_child_does_not_need_a_hold_row(boundary):
    controller, write_read = boundary
    raw = fill()
    write_read(raw)
    zero = dict(client_order_id="order-2", executed_amount_base="0")
    assert controller._executor_hold_released_to_bag(claimant(raw, zero)) is True


@pytest.mark.parametrize("field", ["executed_amount_quote", "cumulative_fee_paid_quote"])
def test_missing_positive_fill_cost_receipt_is_not_zero(boundary, field):
    controller, write_read = boundary
    raw = fill()
    raw[field] = "0"
    write_read(raw)
    incomplete = dict(raw)
    del incomplete[field]
    assert controller._executor_hold_released_to_bag(claimant(incomplete)) is False


def test_payload_cannot_disagree_with_scaled_columns(boundary):
    controller, write_read = boundary
    raw = fill()
    corrupt = dict(raw, executed_amount_base="0.9")
    write_read(raw, payload_override=corrupt)
    assert controller._executor_hold_released_to_bag(claimant(raw)) is False


def test_payload_identity_cannot_disagree_with_row_identity(boundary):
    controller, write_read = boundary
    raw = fill()
    corrupt = dict(raw, client_order_id="different-order")
    write_read(raw, payload_override=corrupt)
    assert controller._executor_hold_released_to_bag(claimant(raw)) is False


def test_stale_payload_below_column_precision_is_not_close_enough(boundary):
    controller, write_read = boundary
    expected = fill(base="0.02800609")
    stale = fill(base="0.02800601")
    write_read(stale)
    assert controller._executor_hold_released_to_bag(claimant(expected)) is False


@pytest.mark.parametrize("field,value", [
    ("executed_amount_base", "NaN"),
    ("executed_amount_base", "-1"),
    ("executed_amount_quote", "NaN"),
    ("cumulative_fee_paid_quote", "Infinity"),
])
def test_invalid_held_receipt_is_rejected(boundary, field, value):
    controller, write_read = boundary
    raw = fill()
    write_read(raw)
    bad = dict(raw)
    bad[field] = value
    assert controller._executor_hold_released_to_bag(claimant(bad)) is False


@pytest.mark.parametrize("field,value", [
    ("executor_id", "other"), ("trading_pair", "BTC-USDC"),
    ("connector_name", "elsewhere"), ("position_side", "SELL"),
])
def test_wrong_ledger_identity_is_rejected(boundary, field, value):
    controller, write_read = boundary
    raw = fill()
    row = write_read(raw)
    setattr(row, field, value)
    assert controller._executor_hold_released_to_bag(claimant(raw)) is False


def test_unterminated_owner_cannot_be_released(boundary):
    controller, write_read = boundary
    raw = fill()
    write_read(raw)
    ex = claimant(raw)
    ex.is_done = False
    assert controller._executor_hold_released_to_bag(ex) is False


def test_unused_zero_fill_ledger_row_does_not_poison_complete_transfer(boundary):
    controller, write_read = boundary
    raw = fill()
    write_read(raw)
    controller.rows.append(SimpleNamespace(executor_id="executor-1", connector_name="okx",
                                          trading_pair="ETH-USDC", position_side="BUY",
                                          client_order_id="zero-child", executed_amount_base=Decimal("0"),
                                          executed_amount_quote=Decimal("0"), cumulative_fee_paid_quote=Decimal("0")))
    assert controller._executor_hold_released_to_bag(claimant(raw)) is True


def test_exact_legacy_numeric_row_remains_acceptable(boundary):
    controller, write_read = boundary
    raw = fill()
    write_read(raw, payload_override={})
    assert controller._executor_hold_released_to_bag(claimant(raw)) is True


def test_legacy_rounded_row_without_exact_payload_stays_unavailable(boundary):
    controller, write_read = boundary
    raw = fill(base="0.02800601")
    write_read(raw, payload_override={})
    assert controller._executor_hold_released_to_bag(claimant(raw)) is False
