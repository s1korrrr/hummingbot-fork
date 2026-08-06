from __future__ import annotations

from typing import Optional

from controllers.directional_trading.rsi_v5_core.canonical import stable_hash
from controllers.directional_trading.rsi_v5_core.domain import (
    EntryIntent,
    EntryRole,
    Lifecycle,
    RiskEvaluation,
    Side,
    StrategyInput,
    StrategyState,
)
from controllers.directional_trading.rsi_v5_core.risk import evaluate_entry_risk


def evaluate_entry(
    state: StrategyState,
    strategy_input: StrategyInput,
) -> tuple[Optional[EntryIntent], RiskEvaluation, tuple[str, ...]]:
    market = strategy_input.market
    config = strategy_input.config
    reasons = []

    if state.entry_paused:
        reasons.append("operator_pause")
    if state.lifecycle in (Lifecycle.EXITING, Lifecycle.FAULTED):
        reasons.append("lifecycle_block")
    if not market.data_health.ready:
        reasons.append("data_unhealthy")
    if not market.condition_ok:
        reasons.append(market.condition_reason or "market_condition")
    if market.rsi is None:
        reasons.append("no_rsi")
    elif market.rsi > config.rsi_buy_threshold:
        reasons.append("rsi_not_oversold")
    if not market.raw_buy_reversal:
        reasons.append("no_reversal")
    if not market.buy_confirmation_ready:
        reasons.append("confirmation_pending")
    if market.signal_score < config.min_signal_score:
        reasons.append("signal_score_low")

    proposed = config.usd_per_entry
    risk = evaluate_entry_risk(strategy_input, proposed)
    reasons.extend(risk.reason_codes)
    reasons = list(dict.fromkeys(reasons))
    if reasons:
        return None, risk, tuple(reasons)

    reference_price = market.best_bid
    amount = proposed / reference_price
    snapshot_hash = stable_hash(strategy_input)
    unsigned = {
        "controller_id": state.controller_id,
        "event_id": market.event_id,
        "sequence": market.sequence,
        "config_hash": config.identity_hash,
        "snapshot_hash": snapshot_hash,
        "side": Side.BUY,
        "role": EntryRole.FULL,
        "notional": proposed,
        "reference_price": reference_price,
        "amount_base": amount,
        "leverage": config.leverage,
    }
    intent = EntryIntent(
        intent_id=stable_hash({"kind": "entry", **unsigned}),
        **unsigned,
    )
    return intent, risk, ()
