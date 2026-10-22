from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

ZERO = Decimal("0")
QUOTE_ALIASES = {
    "USD": ("USD", "USDC"),
    "USDC": ("USDC", "USD"),
}


def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _str_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def _asset_key(asset: Any) -> str:
    return str(asset or "").strip().upper()


def _pair_base(pair_id: str) -> str:
    return str(pair_id or "").split("-", 1)[0].strip()


def _pair_quote(pair_id: str) -> str:
    parts = str(pair_id or "").rsplit("-", 1)
    return parts[1].strip() if len(parts) == 2 else ""


@dataclass(frozen=True)
class PairPolicy:
    pair_id: str
    base_asset: str
    quote_asset: str
    mode: str
    enabled: bool = True
    max_sell_fraction_per_tick: Decimal = Decimal("0")
    max_symbol_exposure_quote: Optional[Decimal] = None
    min_quote_reserve: Decimal = ZERO
    min_trade_value_quote: Decimal = ZERO

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "PairPolicy":
        mode = str(raw.get("mode", "")).strip().lower()
        if mode not in {"sell_only", "buy_sell"}:
            raise ValueError(f"Unsupported pair policy mode for {raw.get('pair_id')}: {mode}")
        pair_id = str(raw.get("pair_id") or "").strip()
        if not pair_id:
            raise ValueError("Pair policy requires pair_id")
        base_asset = str(raw.get("base_asset") or pair_id.split("-", 1)[0]).strip()
        quote_asset = str(raw.get("quote_asset") or "USDC").strip()
        return cls(
            pair_id=pair_id,
            base_asset=base_asset,
            quote_asset=quote_asset,
            mode=mode,
            enabled=bool(raw.get("enabled", True)),
            max_sell_fraction_per_tick=_decimal(raw.get("max_sell_fraction_per_tick"), Decimal("0")),
            max_symbol_exposure_quote=(
                _decimal(raw.get("max_symbol_exposure_quote"))
                if raw.get("max_symbol_exposure_quote") is not None
                else None
            ),
            min_quote_reserve=_decimal(raw.get("min_quote_reserve"), ZERO),
            min_trade_value_quote=_decimal(raw.get("min_trade_value_quote"), ZERO),
        )


@dataclass(frozen=True)
class TokenBalance:
    asset: str
    total_units: Decimal = ZERO
    available_units: Decimal = ZERO
    price_quote: Decimal = ZERO
    min_trade_value_quote: Decimal = ZERO

    @classmethod
    def from_mapping(cls, asset: str, raw: Mapping[str, Any]) -> "TokenBalance":
        return cls(
            asset=asset,
            total_units=_decimal(raw.get("total_units"), ZERO),
            available_units=_decimal(raw.get("available_units"), ZERO),
            price_quote=_decimal(raw.get("price_quote"), ZERO),
            min_trade_value_quote=_decimal(raw.get("min_trade_value_quote"), ZERO),
        )

    @property
    def current_value_quote(self) -> Decimal:
        return max(self.total_units, ZERO) * max(self.price_quote, ZERO)


@dataclass(frozen=True)
class PairPlanRow:
    pair_id: str
    policy_mode: str
    wallet_units: str
    available_units: str
    current_value_quote: str
    target_action: str
    proposed_units: str
    proposed_value_quote: str
    caps_applied: List[str] = field(default_factory=list)
    skip_reasons: List[str] = field(default_factory=list)
    signal_reasons: List[str] = field(default_factory=list)
    risk_reasons: List[str] = field(default_factory=list)
    evidence_cutoff: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "policy_mode": self.policy_mode,
            "wallet_units": self.wallet_units,
            "available_units": self.available_units,
            "current_value_quote": self.current_value_quote,
            "target_action": self.target_action,
            "proposed_units": self.proposed_units,
            "proposed_value_quote": self.proposed_value_quote,
            "caps_applied": list(self.caps_applied),
            "skip_reasons": list(self.skip_reasons),
            "signal_reasons": list(self.signal_reasons),
            "risk_reasons": list(self.risk_reasons),
            "evidence_cutoff": self.evidence_cutoff,
        }


@dataclass(frozen=True)
class PortfolioPlan:
    plan_id: str
    request_id: str
    as_of: str
    mode: str
    readiness: str
    created_order_count: int
    portfolio_reasons: List[str]
    rows: List[PairPlanRow]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": "rsibot.hyperliquid_portfolio_rsi.portfolio_plan.v1",
            "plan_id": self.plan_id,
            "request_id": self.request_id,
            "as_of": self.as_of,
            "mode": self.mode,
            "readiness": self.readiness,
            "created_order_count": self.created_order_count,
            "portfolio_reasons": list(self.portfolio_reasons),
            "rows": [row.to_dict() for row in self.rows],
        }


def _balances_by_asset(portfolio_snapshot: Mapping[str, Any]) -> Dict[str, TokenBalance]:
    raw_balances = portfolio_snapshot.get("balances") or {}
    return {
        str(asset): TokenBalance.from_mapping(str(asset), raw)
        for asset, raw in raw_balances.items()
        if isinstance(raw, Mapping)
    }


def _raw_asset(raw: Any) -> str:
    if isinstance(raw, Mapping):
        return str(raw.get("asset") or raw.get("base_asset") or raw.get("token") or "").strip()
    return str(raw or "").strip()


def _raw_overrides(raw: Any) -> Dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _resolve_pair_id(
    *,
    asset: str,
    quote_asset: str,
    symbol_map: Optional[Mapping[str, str]],
    tradable_pairs: Optional[Sequence[str]],
) -> tuple[str, bool]:
    normalized_symbol_map = {
        _asset_key(key): str(value).strip()
        for key, value in (symbol_map or {}).items()
        if str(value or "").strip()
    }
    tradable_lookup = {
        _asset_key(pair): str(pair).strip()
        for pair in (tradable_pairs or [])
        if str(pair or "").strip()
    }
    asset_key = _asset_key(asset)
    quote_key = _asset_key(quote_asset)
    quote_candidates = QUOTE_ALIASES.get(quote_key, (quote_key,))
    candidates = [f"{asset}-{quote}" for quote in quote_candidates]
    for candidate in candidates:
        candidate_key = _asset_key(candidate)
        mapped_candidate = normalized_symbol_map.get(candidate_key)
        if mapped_candidate and (not tradable_lookup or _asset_key(mapped_candidate) in tradable_lookup):
            return tradable_lookup.get(_asset_key(mapped_candidate), mapped_candidate), True
        if candidate_key in tradable_lookup:
            return tradable_lookup[candidate_key], True

    mapped_pair = normalized_symbol_map.get(asset_key)
    if mapped_pair:
        allowed_quotes = {_asset_key(quote) for quote in quote_candidates}
        if _asset_key(_pair_quote(mapped_pair)) not in allowed_quotes:
            return candidates[0], False
        if not tradable_lookup or _asset_key(mapped_pair) in tradable_lookup:
            return tradable_lookup.get(_asset_key(mapped_pair), mapped_pair), True
        return mapped_pair, False

    return candidates[0], not normalized_symbol_map and not tradable_lookup


def build_pair_policies_from_wallet(
    *,
    portfolio_snapshot: Mapping[str, Any],
    buy_sell_assets: Iterable[Any],
    protected_assets: Optional[Iterable[Any]] = None,
    quote_asset: Optional[str] = None,
    symbol_map: Optional[Mapping[str, str]] = None,
    tradable_pairs: Optional[Sequence[str]] = None,
    default_sell_fraction_per_tick: Any = "0.20",
    default_min_trade_value_quote: Any = "5",
    default_max_symbol_exposure_quote: Optional[Any] = None,
    default_min_quote_reserve: Any = "0",
) -> List[Dict[str, Any]]:
    """Derive pair policies from wallet inventory plus an explicit buy allowlist.

    Assets present in ``buy_sell_assets`` may buy or sell. Nonzero wallet assets
    outside that allowlist default to sell-only unless protected. Assets that
    cannot be resolved to a connector-valid trading pair are returned disabled
    so the planner fails closed instead of silently dropping inventory.
    """
    balances = _balances_by_asset(portfolio_snapshot)
    resolved_quote_asset = str(quote_asset or portfolio_snapshot.get("quote_asset") or "USDC").strip()
    protected_keys = {_asset_key(resolved_quote_asset)}
    for alias in QUOTE_ALIASES.get(_asset_key(resolved_quote_asset), ()):
        protected_keys.add(_asset_key(alias))
    for protected_asset in protected_assets or []:
        protected_keys.add(_asset_key(_raw_asset(protected_asset)))

    buy_overrides: Dict[str, Dict[str, Any]] = {}
    buy_assets: Dict[str, str] = {}
    for raw in buy_sell_assets:
        asset = _raw_asset(raw)
        if not asset:
            continue
        key = _asset_key(asset)
        buy_assets[key] = asset
        buy_overrides[key] = _raw_overrides(raw)

    policy_assets: Dict[str, tuple[str, str, Dict[str, Any]]] = {}
    for key, asset in buy_assets.items():
        if key not in protected_keys:
            policy_assets[key] = (asset, "buy_sell", buy_overrides.get(key, {}))

    for asset, balance in balances.items():
        key = _asset_key(asset)
        if key in protected_keys or key in policy_assets:
            continue
        if balance.total_units > ZERO:
            policy_assets[key] = (asset, "sell_only", {})

    policies: List[Dict[str, Any]] = []
    for key in sorted(policy_assets):
        asset, mode, overrides = policy_assets[key]
        pair_id, resolved = _resolve_pair_id(
            asset=asset,
            quote_asset=str(overrides.get("quote_asset") or resolved_quote_asset),
            symbol_map=symbol_map,
            tradable_pairs=tradable_pairs,
        )
        policy: Dict[str, Any] = {
            "pair_id": pair_id,
            "base_asset": _pair_base(pair_id) or asset,
            "quote_asset": _pair_quote(pair_id) or str(overrides.get("quote_asset") or resolved_quote_asset),
            "mode": mode,
            "enabled": resolved and bool(overrides.get("enabled", True)),
            "symbol_mapping_status": "resolved" if resolved else "unresolved",
        }
        if mode == "sell_only":
            policy["max_sell_fraction_per_tick"] = str(
                overrides.get("max_sell_fraction_per_tick", default_sell_fraction_per_tick)
            )
            policy["min_trade_value_quote"] = str(
                overrides.get("min_trade_value_quote", default_min_trade_value_quote)
            )
        else:
            if overrides.get("max_symbol_exposure_quote", default_max_symbol_exposure_quote) is not None:
                policy["max_symbol_exposure_quote"] = str(
                    overrides.get("max_symbol_exposure_quote", default_max_symbol_exposure_quote)
                )
            policy["min_quote_reserve"] = str(overrides.get("min_quote_reserve", default_min_quote_reserve))
            policy["min_trade_value_quote"] = str(
                overrides.get("min_trade_value_quote", default_min_trade_value_quote)
            )
        policies.append(policy)
    return policies


def _reserved_sell_units_by_asset(portfolio_snapshot: Mapping[str, Any]) -> Dict[str, Decimal]:
    reservations: Dict[str, Decimal] = {}
    raw_orders = portfolio_snapshot.get("open_orders") or []
    if not isinstance(raw_orders, list):
        return reservations
    for raw_order in raw_orders:
        if not isinstance(raw_order, Mapping):
            continue
        if str(raw_order.get("side") or "").lower() != "sell":
            continue
        base_asset = str(raw_order.get("base_asset") or "").strip()
        if not base_asset:
            pair_id = str(raw_order.get("pair_id") or "").strip()
            base_asset = pair_id.split("-", 1)[0] if pair_id else ""
        if not base_asset:
            continue
        reserved_units = _decimal(
            raw_order.get("remaining_units", raw_order.get("quantity", raw_order.get("amount"))),
            ZERO,
        )
        if reserved_units > ZERO:
            reservations[base_asset] = reservations.get(base_asset, ZERO) + reserved_units
    return reservations


def _signal_for_pair(signal_snapshots: Mapping[str, Any], pair_id: str) -> Mapping[str, Any]:
    pairs = signal_snapshots.get("pairs") if isinstance(signal_snapshots, Mapping) else {}
    raw = pairs.get(pair_id) if isinstance(pairs, Mapping) else None
    if isinstance(raw, Mapping):
        return raw
    return {}


def _freshness_reason(signal_snapshots: Mapping[str, Any], pair_id: str) -> Optional[str]:
    pair_signal = _signal_for_pair(signal_snapshots, pair_id)
    freshness = str(
        pair_signal.get("freshness_status")
        or signal_snapshots.get("freshness_status")
        or "missing"
    ).lower()
    symbol_mapping = str(
        pair_signal.get("symbol_mapping_status")
        or signal_snapshots.get("symbol_mapping_status")
        or "unresolved"
    ).lower()
    if freshness != "fresh":
        return f"feature_{freshness}"
    if symbol_mapping != "resolved":
        return "symbol_unresolved"
    feature_cutoff = pair_signal.get("feature_cutoff") or signal_snapshots.get("feature_cutoff")
    if not str(feature_cutoff or "").strip():
        return "feature_cutoff_missing"
    return None


def _signal_score(signal_snapshots: Mapping[str, Any], pair_id: str, field_name: str) -> Decimal:
    return _decimal(_signal_for_pair(signal_snapshots, pair_id).get(field_name), ZERO)


def _policy_signal_priority(signal_snapshots: Mapping[str, Any], policy: PairPolicy) -> Decimal:
    if policy.mode == "sell_only":
        return _signal_score(signal_snapshots, policy.pair_id, "sell_score")
    return max(
        _signal_score(signal_snapshots, policy.pair_id, "buy_score"),
        _signal_score(signal_snapshots, policy.pair_id, "sell_score"),
    )


def _signal_price_quote(signal_snapshots: Mapping[str, Any], pair_id: str, balance: TokenBalance) -> Decimal:
    pair_signal = _signal_for_pair(signal_snapshots, pair_id)
    return _decimal(
        pair_signal.get("price_quote", pair_signal.get("mid_price_quote")),
        balance.price_quote,
    )


def _sell_quantity(policy: PairPolicy, balance: TokenBalance) -> Decimal:
    if balance.available_units <= ZERO:
        return ZERO
    if policy.max_sell_fraction_per_tick <= ZERO:
        return ZERO
    return min(balance.available_units, balance.available_units * policy.max_sell_fraction_per_tick)


def _risk_pause_reasons(risk_gate_state: Mapping[str, Any]) -> List[str]:
    reasons: List[str] = []
    daily_loss_limit = risk_gate_state.get("daily_loss_limit_pct")
    if daily_loss_limit is not None and _decimal(risk_gate_state.get("daily_loss_pct"), ZERO) >= _decimal(daily_loss_limit):
        reasons.append("daily_loss_limit")
    drawdown_limit = risk_gate_state.get("drawdown_limit_pct")
    if drawdown_limit is not None and _decimal(risk_gate_state.get("drawdown_pct"), ZERO) >= _decimal(drawdown_limit):
        reasons.append("drawdown_limit")
    return reasons


def _cooldown_reason(risk_gate_state: Mapping[str, Any], pair_id: str) -> Optional[str]:
    pair_cooldowns = risk_gate_state.get("pair_cooldowns") or {}
    if not isinstance(pair_cooldowns, Mapping):
        return None
    if _decimal(pair_cooldowns.get(pair_id), ZERO) > ZERO:
        return "pair_cooldown"
    return None


def _max_orders_per_tick(risk_gate_state: Mapping[str, Any]) -> Optional[int]:
    raw_limit = risk_gate_state.get("max_orders_per_tick")
    if raw_limit is None:
        return None
    limit = int(_decimal(raw_limit, ZERO))
    return max(limit, 0)


def _buy_value_quote(
    *,
    policy: PairPolicy,
    balance: TokenBalance,
    portfolio_snapshot: Mapping[str, Any],
    risk_gate_state: Mapping[str, Any],
    price_quote: Decimal,
) -> tuple[Decimal, List[str], List[str], List[str]]:
    caps_applied: List[str] = []
    skip_reasons: List[str] = []
    risk_reasons: List[str] = []
    cooldown_reason = _cooldown_reason(risk_gate_state, policy.pair_id)
    if cooldown_reason:
        return ZERO, [cooldown_reason], caps_applied, risk_reasons
    pause_reasons = _risk_pause_reasons(risk_gate_state)
    if pause_reasons:
        return ZERO, pause_reasons, caps_applied, risk_reasons
    if price_quote <= ZERO:
        return ZERO, ["price_missing"], caps_applied, risk_reasons

    quote_room = _decimal(portfolio_snapshot.get("quote_available"), ZERO) - policy.min_quote_reserve
    if quote_room <= ZERO:
        return ZERO, ["quote_reserve"], caps_applied, risk_reasons
    caps_applied.append("quote_reserve")
    risk_reasons.append("quote_reserve_passed")

    if policy.max_symbol_exposure_quote is None:
        return ZERO, ["symbol_exposure_cap_missing"], caps_applied, risk_reasons
    symbol_room = policy.max_symbol_exposure_quote - balance.current_value_quote
    if symbol_room <= ZERO:
        return ZERO, ["symbol_exposure_cap"], caps_applied, risk_reasons
    caps_applied.append("max_symbol_exposure_quote")
    risk_reasons.append("symbol_exposure_cap_passed")

    candidate_rooms = [quote_room, symbol_room]
    if risk_gate_state.get("usd_per_entry_quote") is not None:
        entry_room = _decimal(risk_gate_state.get("usd_per_entry_quote"), ZERO)
        if entry_room <= ZERO:
            return ZERO, ["usd_per_entry_invalid"], caps_applied, risk_reasons
        candidate_rooms.append(entry_room)
        caps_applied.append("usd_per_entry_quote")
        risk_reasons.append("usd_per_entry_passed")
    if risk_gate_state.get("gross_exposure_cap_quote") is not None:
        gross_room = _decimal(risk_gate_state.get("gross_exposure_cap_quote"), ZERO) - _decimal(
            risk_gate_state.get("gross_exposure_quote"), ZERO
        )
        if gross_room <= ZERO:
            return ZERO, ["gross_exposure_cap"], caps_applied, risk_reasons
        candidate_rooms.append(gross_room)
        caps_applied.append("gross_exposure_cap_quote")
        risk_reasons.append("gross_exposure_cap_passed")

    if risk_gate_state.get("turnover_cap_quote") is not None:
        turnover_room = _decimal(risk_gate_state.get("turnover_cap_quote"), ZERO) - _decimal(
            risk_gate_state.get("turnover_used_quote"), ZERO
        )
        if turnover_room <= ZERO:
            return ZERO, ["turnover_cap"], caps_applied, risk_reasons
        candidate_rooms.append(turnover_room)
        caps_applied.append("turnover_cap_quote")
        risk_reasons.append("turnover_cap_passed")

    buy_value = min(candidate_rooms)
    min_trade_value = max(policy.min_trade_value_quote, balance.min_trade_value_quote)
    if buy_value < min_trade_value:
        skip_reasons.append("min_trade_value")
    return buy_value, skip_reasons, caps_applied, risk_reasons


def build_portfolio_plan(
    *,
    pair_policies: Iterable[Mapping[str, Any]],
    portfolio_snapshot: Mapping[str, Any],
    signal_snapshots: Mapping[str, Any],
    risk_gate_state: Optional[Mapping[str, Any]] = None,
) -> PortfolioPlan:
    risk_gate_state = risk_gate_state or {}
    mode = "shadow" if bool(risk_gate_state.get("shadow_mode", True)) else str(risk_gate_state.get("mode", "shadow"))
    portfolio_reasons: List[str] = []
    wallet_refresh_status = str(portfolio_snapshot.get("refresh_status") or "fresh").lower()
    if wallet_refresh_status != "fresh":
        portfolio_reasons.append(f"wallet_{wallet_refresh_status}")
    if bool(risk_gate_state.get("manual_kill_switch", False)):
        portfolio_reasons.append("manual_kill_switch")
    if mode == "paper" and not bool(risk_gate_state.get("paper_actions_enabled", False)):
        portfolio_reasons.append("paper_mode_not_enabled")
    elif mode == "live":
        portfolio_reasons.append("live_mode_not_enabled")

    balances = _balances_by_asset(portfolio_snapshot)
    reserved_sell_units = _reserved_sell_units_by_asset(portfolio_snapshot)
    rows: List[PairPlanRow] = []
    planned_order_count = 0
    planned_turnover_quote = ZERO
    parsed_policies = [PairPolicy.from_mapping(raw_policy) for raw_policy in pair_policies]
    ranked_policies = sorted(
        enumerate(parsed_policies),
        key=lambda item: (-_policy_signal_priority(signal_snapshots, item[1]), item[0]),
    )
    for _, policy in ranked_policies:
        balance = balances.get(policy.base_asset, TokenBalance(asset=policy.base_asset))
        skip_reasons: List[str] = []
        caps_applied: List[str] = []
        signal_reasons: List[str] = []
        row_risk_reasons = portfolio_reasons.copy()
        reserved_units = reserved_sell_units.get(policy.base_asset, ZERO)
        if reserved_units > ZERO:
            balance = TokenBalance(
                asset=balance.asset,
                total_units=balance.total_units,
                available_units=max(balance.available_units - reserved_units, ZERO),
                price_quote=balance.price_quote,
                min_trade_value_quote=balance.min_trade_value_quote,
            )
            caps_applied.append("open_order_reservation")
        target_action = "none"
        proposed_units = ZERO
        proposed_value_quote = ZERO
        if not policy.enabled:
            skip_reasons.append("pair_disabled")
        freshness_reason = _freshness_reason(signal_snapshots, policy.pair_id)
        if freshness_reason:
            skip_reasons.append(freshness_reason)
        if portfolio_reasons:
            skip_reasons.extend(portfolio_reasons)
        if not skip_reasons:
            effective_risk_gate_state = dict(risk_gate_state)
            effective_risk_gate_state["turnover_used_quote"] = (
                _decimal(risk_gate_state.get("turnover_used_quote"), ZERO) + planned_turnover_quote
            )
            sell_score = _signal_score(signal_snapshots, policy.pair_id, "sell_score")
            buy_score = _signal_score(signal_snapshots, policy.pair_id, "buy_score")
            sell_signal_selected = (
                sell_score >= Decimal("0.5")
                and (policy.mode == "sell_only" or sell_score >= buy_score)
            )
            if sell_signal_selected:
                sell_units = _sell_quantity(policy, balance)
                sell_value = sell_units * balance.price_quote
                min_trade_value = max(policy.min_trade_value_quote, balance.min_trade_value_quote)
                if policy.max_sell_fraction_per_tick <= ZERO:
                    skip_reasons.append("sell_fraction_missing")
                elif balance.available_units <= ZERO:
                    skip_reasons.append("no_available_inventory")
                elif sell_units <= ZERO:
                    skip_reasons.append("sell_quantity_zero")
                elif sell_value < min_trade_value:
                    skip_reasons.append("min_trade_value")
                else:
                    target_action = "sell"
                    proposed_units = sell_units
                    proposed_value_quote = sell_value
                    caps_applied.append("max_sell_fraction_per_tick")
                    signal_reasons.append("sell_score_threshold")
            elif policy.mode == "buy_sell" and buy_score >= Decimal("0.5"):
                price_quote = _signal_price_quote(signal_snapshots, policy.pair_id, balance)
                buy_value, buy_skip_reasons, buy_caps, buy_risk_reasons = _buy_value_quote(
                    policy=policy,
                    balance=balance,
                    portfolio_snapshot=portfolio_snapshot,
                    risk_gate_state=effective_risk_gate_state,
                    price_quote=price_quote,
                )
                if buy_skip_reasons:
                    skip_reasons.extend(buy_skip_reasons)
                    caps_applied.extend(buy_caps)
                else:
                    target_action = "buy"
                    proposed_value_quote = buy_value
                    proposed_units = buy_value / price_quote
                    caps_applied.extend(buy_caps)
                    signal_reasons.append("buy_score_threshold")
                    row_risk_reasons.extend(reason for reason in buy_risk_reasons if reason not in row_risk_reasons)
            else:
                skip_reasons.append("signal_below_threshold")
        max_orders = _max_orders_per_tick(risk_gate_state)
        if target_action != "none" and max_orders is not None and planned_order_count >= max_orders:
            target_action = "none"
            proposed_units = ZERO
            proposed_value_quote = ZERO
            skip_reasons.append("max_orders_per_tick")
        elif target_action != "none":
            planned_order_count += 1
            planned_turnover_quote += proposed_value_quote

        rows.append(
            PairPlanRow(
                pair_id=policy.pair_id,
                policy_mode=policy.mode,
                wallet_units=_str_decimal(balance.total_units),
                available_units=_str_decimal(balance.available_units),
                current_value_quote=_str_decimal(balance.current_value_quote),
                target_action=target_action,
                proposed_units=_str_decimal(proposed_units),
                proposed_value_quote=_str_decimal(proposed_value_quote),
                caps_applied=caps_applied,
                skip_reasons=skip_reasons,
                signal_reasons=signal_reasons,
                risk_reasons=row_risk_reasons,
                evidence_cutoff=str(signal_snapshots.get("feature_cutoff") or "") or None,
            )
        )

    if mode == "paper":
        readiness = "paper_ready" if not portfolio_reasons else "paper_blocked"
    elif mode == "live":
        readiness = "live_blocked"
    else:
        readiness = "shadow_ready" if not portfolio_reasons else "shadow_blocked"
    return PortfolioPlan(
        plan_id=str(portfolio_snapshot.get("snapshot_id") or "shadow-plan"),
        request_id=str(risk_gate_state.get("request_id") or portfolio_snapshot.get("snapshot_id") or "shadow-request"),
        as_of=str(portfolio_snapshot.get("as_of") or signal_snapshots.get("as_of") or ""),
        mode=mode,
        readiness=readiness,
        created_order_count=0,
        portfolio_reasons=portfolio_reasons,
        rows=rows,
    )
