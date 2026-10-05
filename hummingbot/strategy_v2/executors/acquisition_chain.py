"""Validate one bounded BUY authorization across its native child orders.

This is a reader of executor snapshots and terminal receipts, not an order or
funding ledger. Callers still verify venue/recorder authority and exact fees.
"""

from decimal import Decimal, InvalidOperation

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState

ACQUISITION_CHAIN_V1 = "v2_bounded_acquisition_v1"
_CHILD_KEYS = {"generation", "order_id", "predecessor_id", "purpose", "submitted_base", "limit_price"}
_CHAIN_KEYS = {"schema", "contract", "parent_id", "authorized_base", "maximum_price", "children",
               "successor_pending", "membership_closed", "gross_filled_base"}
_PROOF_KEYS = {"schema", "source", "order_id", "exchange_order_id", "trading_pair", "side",
               "amount", "filled_base", "state", "trade_ids"}
_PURPOSES = {"downward_reprice", "post_only_repair"}


def _amount(value, label, *, positive=False):
    if isinstance(value, bool):
        raise ValueError(f"Acquisition chain {label} is invalid")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"Acquisition chain {label} is invalid") from None
    if not result.is_finite() or result < 0 or positive and result == 0:
        raise ValueError(f"Acquisition chain {label} is invalid")
    return result


def _proofs(value):
    if isinstance(value, dict) and value.get("source") == "native_terminal_entry_set_v1":
        if (set(value) != {"schema", "source", "orders"} or type(value["schema"]) is not int
                or value["schema"] != 1 or not isinstance(value["orders"], list)):
            raise ValueError("Acquisition chain terminal proof set is invalid")
        return value["orders"]
    if isinstance(value, dict):
        return [value]
    if value is None:
        return []
    if isinstance(value, list):
        return value
    raise ValueError("Acquisition chain terminal proofs are invalid")


def validate_acquisition_chain(config, custom_info, proofs, *, require_closed=True,
                               allow_unacknowledged_tail=False) -> dict:
    """Return the validated V2 chain; reject missing or contradictory evidence.

    `require_closed=False` accepts an incomplete, still fully reserved parent.
    It never certifies capacity release. A terminal predecessor cannot release
    the parent while a successor is authorized or submission is uncertain.
    """
    if (getattr(config, "entry_chain_contract", None) != ACQUISITION_CHAIN_V1
            or getattr(config, "entry_acquisition_policy", None) != "down_only_maker_v1"
            or config.side is not TradeType.BUY or config.leverage != 1
            or config.triple_barrier_config.open_order_type is not OrderType.LIMIT_MAKER):
        raise ValueError("Acquisition chain configuration is unsupported")
    if not isinstance(custom_info, dict):
        raise ValueError("Acquisition chain custom info is unavailable")
    if custom_info.get("resume_conflicting_fill") is not None:
        raise ValueError("Acquisition chain has a contradictory predecessor fill")
    chain = custom_info.get("acquisition_chain")
    if (not isinstance(chain, dict) or set(chain) != _CHAIN_KEYS
            or type(chain["schema"]) is not int or chain["schema"] != 1
            or chain["contract"] != ACQUISITION_CHAIN_V1
            or chain["parent_id"] != config.id
            or type(chain["membership_closed"]) is not bool
            or not isinstance(chain["children"], list)
            or len(chain["children"]) > config.entry_max_downward_reprices + 1):
        raise ValueError("Acquisition chain identity or membership is invalid")
    authorized = _amount(chain["authorized_base"], "authorized base", positive=True)
    maximum_price = _amount(chain["maximum_price"], "maximum price", positive=True)
    if (authorized != config.amount or maximum_price != config.maximum_acceptable_price
            or maximum_price != config.entry_price):
        raise ValueError("Acquisition chain parent authorization differs")
    pending = chain["successor_pending"]
    if pending is not None:
        if (not isinstance(pending, dict) or set(pending) != {"predecessor_id", "purpose"}
                or pending["purpose"] not in _PURPOSES
                or not isinstance(pending["predecessor_id"], str)):
            raise ValueError("Acquisition chain pending successor is invalid")
    if chain["membership_closed"] and pending is not None or require_closed and not chain["membership_closed"]:
        raise ValueError("Acquisition chain still authorizes a successor")

    children = chain["children"]
    ids = []
    prices = []
    for index, child in enumerate(children):
        if (not isinstance(child, dict) or set(child) != _CHILD_KEYS
                or type(child["generation"]) is not int or child["generation"] != index
                or not isinstance(child["order_id"], str) or not child["order_id"]
                or child["order_id"] in ids):
            raise ValueError("Acquisition chain child identity is invalid")
        if index == 0:
            if child["predecessor_id"] is not None or child["purpose"] != "initial":
                raise ValueError("Acquisition chain initial child differs")
        elif child["predecessor_id"] != ids[-1] or child["purpose"] not in _PURPOSES:
            raise ValueError("Acquisition chain predecessor differs")
        amount = _amount(child["submitted_base"], "submitted base", positive=True)
        price = _amount(child["limit_price"], "child price", positive=True)
        if (amount > authorized or price > maximum_price or index and
                (price > prices[-1] or child["purpose"] == "downward_reprice" and price >= prices[-1])):
            raise ValueError("Acquisition chain child exceeds authorization")
        ids.append(child["order_id"])
        prices.append(price)
    if pending is not None and (not ids or pending["predecessor_id"] != ids[-1]):
        raise ValueError("Acquisition chain pending predecessor differs")
    if chain["membership_closed"] and not ids:
        raise ValueError("Acquisition chain closed without an entry child")

    raw_snapshots = custom_info.get("execution_order_snapshots") or []
    if not isinstance(raw_snapshots, list):
        raise ValueError("Acquisition chain native snapshots are invalid")
    snapshots = {}
    for raw in raw_snapshots:
        if not isinstance(raw, dict):
            raise ValueError("Acquisition chain native snapshot is invalid")
        if raw.get("trade_type") != "BUY":
            continue
        try:
            order = InFlightOrder.from_json(raw)
        except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation):
            raise ValueError("Acquisition chain native snapshot is invalid") from None
        if order.client_order_id in snapshots:
            raise ValueError("Acquisition chain native snapshot is duplicated")
        snapshots[order.client_order_id] = order
    # A child is recorded when `place_order` returns its client id; its native order
    # object arrives with the created event after the exchange acknowledges it. Only a
    # peer's capacity projection (`allow_unacknowledged_tail`) may see that open tail
    # without a snapshot: it has no fills and the parent's remaining authorization
    # stays committed. An executor's own reconciliation and release proofs stay strict.
    in_flight = (ids[-1:] if allow_unacknowledged_tail and not require_closed
                 and not chain["membership_closed"] else [])
    if set(snapshots) - set(ids) or set(ids) - set(snapshots) - set(in_flight):
        raise ValueError("Acquisition chain native child coverage differs")
    tracked = [child for child in children if child["order_id"] in snapshots]
    for child in tracked:
        order = snapshots[child["order_id"]]
        submitted = _amount(child["submitted_base"], "submitted base", positive=True)
        price = _amount(child["limit_price"], "child price", positive=True)
        filled = _amount(order.executed_amount_base, "native gross fill")
        if (order.trading_pair != config.trading_pair or order.trade_type is not TradeType.BUY
                or order.order_type is not OrderType.LIMIT_MAKER
                or order.amount != submitted or order.price != price or filled > submitted):
            raise ValueError("Acquisition chain native child scope or bound differs")
        if sum((fill.fill_base_amount for fill in order.order_fills.values()), Decimal("0")) != filled:
            raise ValueError("Acquisition chain native fill total differs")
        if sum((fill.fill_quote_amount for fill in order.order_fills.values()), Decimal("0")) != _amount(
                order.executed_amount_quote, "native gross quote"):
            raise ValueError("Acquisition chain native quote total differs")
        if any(fill.trade_id != str(key) or fill.client_order_id != order.client_order_id
               or fill.exchange_order_id != order.exchange_order_id or fill.trading_pair != config.trading_pair
               or not fill.fill_base_amount.is_finite() or fill.fill_base_amount <= 0
               or not fill.fill_price.is_finite() or fill.fill_price <= 0
               or fill.fill_quote_amount != fill.fill_base_amount * fill.fill_price
               for key, fill in order.order_fills.items()):
            raise ValueError("Acquisition chain native fill identity or amount differs")

    proof_rows = _proofs(proofs)
    proof_by_id = {}
    gross = Decimal("0")
    all_trade_ids = set()
    for proof in proof_rows:
        if (not isinstance(proof, dict) or set(proof) != _PROOF_KEYS
                or type(proof["schema"]) is not int or proof["schema"] != 1
                or proof["order_id"] not in snapshots or proof["order_id"] in proof_by_id
                or proof["side"] != "BUY" or proof["trading_pair"] != config.trading_pair
                or proof["state"] not in {"CANCELED", "FILLED", "REJECTED"}
                or not isinstance(proof["trade_ids"], list)
                or any(not isinstance(item, str) or not item for item in proof["trade_ids"])
                or proof["trade_ids"] != sorted(set(proof["trade_ids"]))):
            raise ValueError("Acquisition chain terminal proof is invalid")
        snapshot = snapshots[proof["order_id"]]
        terminal_source = ('native_paper_terminal_v1' if config.connector_name == 'okx_paper_trade'
                           else 'okx_terminal_status_v1' if config.connector_name == 'okx' else None)
        expected_source = 'okx_submission_rejected_v1' if proof['state'] == 'REJECTED' else terminal_source
        if terminal_source is None or proof['source'] != expected_source:
            raise ValueError("Acquisition chain terminal proof source differs")
        if (snapshot.trading_pair != config.trading_pair or snapshot.trade_type is not TradeType.BUY
                or snapshot.order_type is not OrderType.LIMIT_MAKER
                or snapshot.exchange_order_id != proof["exchange_order_id"]):
            raise ValueError("Acquisition chain terminal identity differs")
        amount = _amount(proof["amount"], "terminal amount", positive=True)
        filled = _amount(proof["filled_base"], "terminal filled base")
        if (amount != snapshot.amount or filled != snapshot.executed_amount_base
                or filled > amount or set(proof["trade_ids"]) != {str(key) for key in snapshot.order_fills}):
            raise ValueError("Acquisition chain terminal quantity or fills differ")
        if (proof["state"] == "FILLED" and filled != amount
                or proof["state"] == "REJECTED" and (filled != 0 or proof["exchange_order_id"] is not None)
                or proof["state"] != "REJECTED" and not proof["exchange_order_id"]):
            raise ValueError("Acquisition chain terminal state differs")
        native_state = {"CANCELED": OrderState.CANCELED, "FILLED": OrderState.FILLED,
                        "REJECTED": OrderState.FAILED}[proof["state"]]
        if snapshot.current_state != native_state:
            raise ValueError("Acquisition chain native terminal state differs")
        native_filled = sum((fill.fill_base_amount for fill in snapshot.order_fills.values()), Decimal("0"))
        if native_filled != filled:
            raise ValueError("Acquisition chain native fill total differs")
        trade_ids = set(proof["trade_ids"])
        if all_trade_ids & trade_ids:
            raise ValueError("Acquisition chain trade identity is duplicated")
        all_trade_ids.update(trade_ids)
        proof_by_id[proof["order_id"]] = proof
        gross += filled
    if require_closed and set(proof_by_id) != set(ids):
        raise ValueError("Acquisition chain terminal proof coverage differs")
    if gross > authorized:
        raise ValueError("Acquisition chain gross fills exceed authorization")
    snapshot_gross = sum((order.executed_amount_base for order in snapshots.values()), Decimal("0"))
    if (_amount(chain["gross_filled_base"], "gross filled base") != snapshot_gross
            or snapshot_gross > authorized):
        raise ValueError("Acquisition chain gross fill snapshot differs")
    for index, child in enumerate(children):
        order = snapshots.get(child["order_id"])
        if order is None:
            continue  # The unacknowledged tail: nothing native to bound yet.
        if (order.amount != _amount(child["submitted_base"], "submitted base", positive=True)
                or order.price != _amount(child["limit_price"], "child price", positive=True)
                or order.amount > authorized - sum((snapshots[item].executed_amount_base for item in ids[:index]), Decimal("0"))):
            raise ValueError("Acquisition chain submitted order exceeds authorization")
        if index:
            previous = proof_by_id.get(ids[index - 1])
            if previous is None or previous["state"] != "CANCELED" or _amount(previous["filled_base"], "predecessor fill") != 0:
                raise ValueError("Acquisition chain predecessor lacks zero-fill terminal proof")
    if pending is not None:
        previous = proof_by_id.get(ids[-1])
        if previous is not None and (previous["state"] != "CANCELED"
                                     or _amount(previous["filled_base"], "predecessor fill") != 0):
            raise ValueError("Acquisition chain pending successor lacks zero-fill predecessor proof")
    return chain
