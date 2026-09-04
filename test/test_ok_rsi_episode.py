from dataclasses import asdict, replace
from decimal import Decimal as D

import pytest

from controllers.directional_trading.ok_rsi_core.episode import (
    EpisodeConfig,
    EpisodeInputError,
    EpisodeState,
    MarketSnapshot,
    OrderUpdate,
    remaining_budget,
    transition,
)


def config(**overrides):
    values = dict(
        controller_id="test-btc",
        connector="okx",
        trading_pair="BTC-USDC",
        mode="buy_sell",
        budget_quote=D("1000"),
        mark_cap_quote=D("1000"),
        fee_rate=D("0"),
    )
    return EpisodeConfig(**(values | overrides))


def market(**overrides):
    values = dict(
        timestamp=6000,
        bid=D("100"),
        ask=D("100"),
        low=D("100"),
        atr_1m=D("1"),
        atr_15m=D("1"),
        atr_1h=D("1"),
        anchor=D("102"),
        quote_available=D("5000"),
        base_available=D("0"),
        min_amount=D("0.001"),
        min_notional=D("1"),
        amount_step=D("0.001"),
        price_step=D("0.01"),
        htf_timestamp=3600,
        entry_signal=True,
    )
    return MarketSnapshot(**(values | overrides))


def bought(cfg=None):
    cfg = cfg or config()
    first = transition(cfg, EpisodeState(), market())
    order = first.requests[0]
    filled = OrderUpdate(order.id, filled_base=D("1"), filled_quote=D("100"), done=True)
    second = transition(cfg, first.state, market(timestamp=6060, base_available=D("1"), entry_signal=False), [filled])
    return second.state, filled


def test_spent_budget_does_not_reopen_on_price_decline_or_duplicate_fill():
    cfg = config()
    state, fill = bought(cfg)
    result = transition(
        cfg,
        state,
        market(timestamp=6120, bid=D("75"), ask=D("75"), low=D("75"), base_available=D("1"), entry_signal=False),
        [fill],
    )
    assert result.state.gross_spent == D("100")
    assert remaining_budget(cfg, result.state) == D("900")
    assert result.state.base == D("1")


def test_pending_reservation_is_not_spend_and_partial_fill_moves_it_once():
    cfg = config(fee_rate=D("0.01"))
    first = transition(cfg, EpisodeState(), market())
    request = first.requests[0]
    assert request.reserved_quote <= D("100")
    partial = OrderUpdate(request.id, filled_base=D("0.4"), filled_quote=D("40"), fee_quote=D("0.4"))
    second = transition(cfg, first.state, market(timestamp=6060, base_available=D("0.4")), [partial])
    assert second.state.gross_spent == D("40.4")
    assert second.state.base == D("0.4")
    assert remaining_budget(cfg, second.state) == D("1000") - request.reserved_quote
    assert not second.requests


def test_zero_fill_terminal_buy_rearms_level_without_suppressing_next_signal():
    cfg = config(adaptive_entry_pricing=True)
    first = transition(cfg, EpisodeState(), market(entry_confidence=D("0.8")))
    request = first.requests[0]

    expired = transition(
        cfg,
        first.state,
        market(timestamp=6060, entry_signal=False, entry_confidence=D("0.8")),
        [OrderUpdate(request.id, filled_base=D("0"), filled_quote=D("0"), done=True)],
    )

    assert expired.state.levels[0] == "OPEN"
    assert remaining_budget(cfg, expired.state) == D("1000")
    assert expired.reason == "need_entry_confirmation"
    retried = transition(
        cfg,
        expired.state,
        market(timestamp=6120, entry_signal=True, entry_confidence=D("0.8")),
    )
    assert retried.requests[0].level == 0
    assert retried.requests[0].id != request.id
    duplicate = transition(
        cfg,
        retried.state,
        market(timestamp=6180, entry_signal=False, entry_confidence=D("0.8")),
        [OrderUpdate(request.id, filled_base=D("0"), filled_quote=D("0"), done=True)],
    )
    assert duplicate.state.levels[0] == "AUTHORIZED"


def test_partially_filled_terminal_buy_consumes_level_once():
    cfg = config(adaptive_entry_pricing=True)
    first = transition(cfg, EpisodeState(), market(entry_confidence=D("0.8")))
    request = first.requests[0]
    closed = transition(
        cfg,
        first.state,
        market(timestamp=6060, base_available=D("0.5"), entry_signal=False, entry_confidence=D("0.8")),
        [OrderUpdate(request.id, filled_base=D("0.5"), filled_quote=D("49.5"), done=True)],
    )

    assert closed.state.levels[0] == "AUTHORIZED"
    assert closed.state.base == D("0.5")


def test_base_fee_reduces_inventory_without_double_counting_quote_cost():
    cfg = config()
    first = transition(cfg, EpisodeState(), market())
    request = first.requests[0]
    result = transition(
        cfg,
        first.state,
        market(timestamp=6060, base_available=D("0.99"), entry_signal=False),
        [OrderUpdate(request.id, D("1"), D("100"), fee_base=D("0.01"), done=True)],
    )
    assert result.state.base == D("0.99")
    assert result.state.cost == D("100")
    assert result.state.gross_spent == D("100")


def test_zero_or_hostile_followup_admission_cannot_be_upsized_to_minimum():
    first = transition(config(), EpisodeState(), market(cascade=True))
    assert not first.requests
    too_small = transition(config(budget_quote=D("10")), EpisodeState(), market(min_notional=D("20")))
    assert not too_small.requests
    assert too_small.reason == "below_exchange_minimum"


def test_adaptive_pricing_changes_price_not_authorization_or_quote_cap():
    result = transition(
        config(adaptive_entry_pricing=True),
        EpisodeState(),
        market(
            bid=D("99.95"),
            ask=D("100"),
            entry_confidence=D("0.8"),
            trend_alignment=D("-0.2"),
        ),
    )

    assert result.reason == "scout"
    assert len(result.requests) == 1
    request = result.requests[0]
    assert request.price == D("99.80")
    assert request.reference_price == D("100")
    assert request.pricing_policy == "ADAPTIVE_MAKER_V1"
    assert request.pricing_discount == D("0.20")
    assert request.pricing_discount_atr == D("0.20")
    assert request.signal_confidence == D("0.8")
    assert request.trend_alignment == D("-0.2")
    assert request.reserved_quote <= D("100")

    restored = EpisodeState.from_dict(result.state.to_dict())
    restored_request = restored.orders[request.id].request
    assert restored_request == request


def test_reserved_adaptive_entry_keeps_authorized_order_through_context_decay():
    cfg = config(adaptive_entry_pricing=True, adaptive_reserved_entry=True)
    first = transition(
        cfg,
        EpisodeState(),
        market(
            bid=D("99.95"),
            ask=D("100"),
            entry_confidence=D("0.8"),
            trend_alignment=D("-0.2"),
        ),
    )
    request = first.requests[0]

    decayed = transition(
        cfg,
        first.state,
        market(
            timestamp=6060,
            entry_signal=False,
            buy_allowed=False,
            reservation_allowed=True,
            hostile=True,
            recovery=True,
            markup=True,
            entry_confidence=D("0.8"),
            trend_alignment=D("-0.2"),
        ),
    )

    assert request.pricing_policy == "ADAPTIVE_RESERVED_ENTRY_V2"
    assert decayed.cancel_ids == ()
    assert decayed.reason == "pending_execution"
    assert not decayed.requests


def test_reserved_adaptive_entry_still_cancels_for_hard_rail_or_expiry():
    cfg = config(adaptive_entry_pricing=True, adaptive_reserved_entry=True)
    first = transition(
        cfg,
        EpisodeState(),
        market(entry_confidence=D("0.8")),
    )
    request = first.requests[0]

    hard_rail = transition(
        cfg,
        first.state,
        market(
            timestamp=6060,
            entry_signal=False,
            buy_allowed=False,
            reservation_allowed=False,
            entry_confidence=D("0.8"),
        ),
    )
    expired = transition(
        cfg,
        first.state,
        market(
            timestamp=request.expires_at,
            entry_signal=False,
            reservation_allowed=True,
            entry_confidence=D("0.8"),
        ),
    )

    assert hard_rail.cancel_ids == (request.id,)
    assert expired.cancel_ids == (request.id,)


def test_reserved_adaptive_entry_requires_adaptive_pricing():
    with pytest.raises(EpisodeInputError, match="reserved adaptive"):
        config(adaptive_reserved_entry=True)


def test_reference_fallback_requires_reserved_adaptive_entry():
    with pytest.raises(EpisodeInputError, match="reference fallback"):
        config(
            adaptive_entry_pricing=True,
            adaptive_reference_fallback=True,
        )


def test_natural_expiry_creates_one_quote_bounded_reference_fallback():
    cfg = config(
        adaptive_entry_pricing=True,
        adaptive_reserved_entry=True,
        adaptive_reference_fallback=True,
    )
    first = transition(
        cfg,
        EpisodeState(),
        market(
            bid=D("99.95"),
            ask=D("100"),
            entry_confidence=D("0.8"),
            trend_alignment=D("-0.2"),
        ),
    )
    parent = first.requests[0]

    expired = transition(
        cfg,
        first.state,
        market(
            timestamp=parent.expires_at,
            bid=D("99.90"),
            ask=D("100"),
            entry_signal=False,
            buy_allowed=False,
            hostile=True,
            entry_confidence=D("0.8"),
            trend_alignment=D("-0.2"),
        ),
        [
            OrderUpdate(
                parent.id,
                filled_base=D("0"),
                filled_quote=D("0"),
                done=True,
                terminal_cause="NATURAL_EXPIRY",
            )
        ],
    )

    assert expired.reason == "adaptive_reference_fallback"
    assert len(expired.requests) == 1
    child = expired.requests[0]
    assert child.pricing_policy == "ADAPTIVE_REFERENCE_FALLBACK_V3"
    assert child.parent_request_id == parent.id
    assert child.price == D("99.89")
    assert child.price <= parent.reference_price
    assert child.price < D("99.90")
    assert child.amount <= parent.amount
    assert child.reserved_quote <= parent.reserved_quote
    assert child.expires_at == parent.expires_at + 600

    restored = EpisodeState.from_dict(expired.state.to_dict())
    assert restored.orders[child.id].request == child
    duplicate = transition(
        cfg,
        restored,
        market(
            timestamp=parent.expires_at + 60,
            entry_signal=False,
            buy_allowed=False,
            recovery=True,
            markup=True,
            entry_confidence=D("0.8"),
            trend_alignment=D("-0.2"),
        ),
        [
            OrderUpdate(
                parent.id,
                filled_base=D("0"),
                filled_quote=D("0"),
                done=True,
                terminal_cause="NATURAL_EXPIRY",
            )
        ],
    )
    assert duplicate.requests == ()
    assert duplicate.cancel_ids == ()
    assert duplicate.reason == "pending_execution"


def test_reference_fallback_rejects_unproven_hard_or_stale_terminal():
    cfg = config(
        adaptive_entry_pricing=True,
        adaptive_reserved_entry=True,
        adaptive_reference_fallback=True,
    )
    first = transition(
        cfg,
        EpisodeState(),
        market(entry_confidence=D("0.8")),
    )
    parent = first.requests[0]
    hard = transition(
        cfg,
        first.state,
        market(
            timestamp=6060,
            entry_signal=False,
            buy_allowed=False,
            reservation_allowed=False,
            entry_confidence=D("0.8"),
        ),
    )
    assert hard.cancel_ids == (parent.id,)
    terminal = transition(
        cfg,
        hard.state,
        market(
            timestamp=6120,
            entry_signal=False,
            entry_confidence=D("0.8"),
        ),
        [
            OrderUpdate(
                parent.id,
                filled_base=D("0"),
                filled_quote=D("0"),
                done=True,
                terminal_cause="NATURAL_EXPIRY",
            )
        ],
    )
    assert terminal.requests == ()

    fresh = transition(
        cfg,
        EpisodeState(),
        market(entry_confidence=D("0.8")),
    )
    stale_parent = fresh.requests[0]
    stale = transition(
        cfg,
        fresh.state,
        market(
            timestamp=stale_parent.expires_at + 120,
            entry_signal=False,
            entry_confidence=D("0.8"),
        ),
        [
            OrderUpdate(
                stale_parent.id,
                filled_base=D("0"),
                filled_quote=D("0"),
                done=True,
                terminal_cause="NATURAL_EXPIRY",
            )
        ],
    )
    assert stale.requests == ()

    simultaneous = transition(
        cfg,
        EpisodeState(),
        market(entry_confidence=D("0.8")),
    )
    simultaneous_parent = simultaneous.requests[0]
    blocked = transition(
        cfg,
        simultaneous.state,
        market(
            timestamp=simultaneous_parent.expires_at,
            entry_signal=False,
            buy_allowed=False,
            reservation_allowed=False,
            entry_confidence=D("0.8"),
        ),
        [
            OrderUpdate(
                simultaneous_parent.id,
                D("0"),
                D("0"),
                done=True,
                terminal_cause="NATURAL_EXPIRY",
            )
        ],
    )
    assert blocked.requests == ()
    assert blocked.state.orders[simultaneous_parent.id].terminal_cause == "HARD_RAIL"
    cleared = transition(
        cfg,
        blocked.state,
        market(
            timestamp=simultaneous_parent.expires_at + 60,
            entry_signal=False,
            entry_confidence=D("0.8"),
        ),
    )
    assert cleared.requests == ()


def test_reference_fallback_does_not_rebuy_after_an_exit_intent_is_armed():
    cfg = config(
        adaptive_entry_pricing=True,
        adaptive_reserved_entry=True,
        adaptive_reference_fallback=True,
    )
    first = transition(
        cfg,
        EpisodeState(),
        market(entry_confidence=D("0.8")),
    )
    parent = first.requests[0]
    first.state.phase = "RECOVER"
    first.state.exit_intent = "RECOVERY_TAKE_PROFIT"

    result = transition(
        cfg,
        first.state,
        market(
            timestamp=parent.expires_at,
            entry_signal=False,
            entry_confidence=D("0.8"),
        ),
        [
            OrderUpdate(
                parent.id,
                D("0"),
                D("0"),
                done=True,
                terminal_cause="NATURAL_EXPIRY",
            )
        ],
    )

    assert result.requests == ()


def test_reference_fallback_requires_terminal_cause_and_never_adds_third_stage():
    cfg = config(
        adaptive_entry_pricing=True,
        adaptive_reserved_entry=True,
        adaptive_reference_fallback=True,
    )
    first = transition(
        cfg,
        EpisodeState(),
        market(entry_confidence=D("0.8")),
    )
    parent = first.requests[0]
    with pytest.raises(EpisodeInputError, match="terminal cause"):
        transition(
            cfg,
            first.state,
            market(timestamp=parent.expires_at, entry_confidence=D("0.8")),
            [OrderUpdate(parent.id, D("0"), D("0"), done=True)],
        )

    fallback = transition(
        cfg,
        first.state,
        market(
            timestamp=parent.expires_at,
            bid=D("99.9"),
            entry_signal=False,
            entry_confidence=D("0.8"),
        ),
        [OrderUpdate(parent.id, D("0"), D("0"), done=True, terminal_cause="NATURAL_EXPIRY")],
    )
    child = fallback.requests[0]
    finished = transition(
        cfg,
        fallback.state,
        market(
            timestamp=child.expires_at,
            entry_signal=False,
            entry_confidence=D("0.8"),
        ),
        [OrderUpdate(child.id, D("0"), D("0"), done=True, terminal_cause="NATURAL_EXPIRY")],
    )
    assert finished.requests == ()
    assert finished.state.levels[0] == "OPEN"


def test_reference_fallback_checkpoint_rejects_unknown_parent():
    cfg = config(
        adaptive_entry_pricing=True,
        adaptive_reserved_entry=True,
        adaptive_reference_fallback=True,
    )
    first = transition(
        cfg,
        EpisodeState(),
        market(entry_confidence=D("0.8")),
    )
    parent = first.requests[0]
    fallback = transition(
        cfg,
        first.state,
        market(
            timestamp=parent.expires_at,
            bid=D("99.9"),
            entry_signal=False,
            entry_confidence=D("0.8"),
        ),
        [OrderUpdate(parent.id, D("0"), D("0"), done=True, terminal_cause="NATURAL_EXPIRY")],
    )
    child = fallback.requests[0]
    payload = fallback.state.to_dict()
    payload["orders"][child.id]["request"]["parent_request_id"] = "missing-parent"
    restored = EpisodeState.from_dict(payload)

    with pytest.raises(EpisodeInputError, match="fallback parent"):
        transition(
            cfg,
            restored,
            market(
                timestamp=parent.expires_at + 60,
                entry_signal=False,
                entry_confidence=D("0.8"),
            ),
        )


def test_disabled_adaptive_pricing_preserves_top_of_book_behavior():
    result = transition(
        config(adaptive_entry_pricing=False),
        EpisodeState(),
        market(
            bid=D("99.95"),
            ask=D("100"),
            entry_confidence=D("0.1"),
            trend_alignment=D("-1"),
        ),
    )

    request = result.requests[0]
    assert request.price == D("100")
    assert request.reference_price == D("100")
    assert request.pricing_policy == "TOP_OF_BOOK"
    assert request.pricing_discount == D("0")


def test_adaptive_pricing_never_authorizes_a_sell_only_buy():
    cfg = config(
        mode="sell_only",
        adaptive_entry_pricing=True,
        assignment_id="assigned",
        initial_base=D("1"),
        initial_cost=D("100"),
    )
    result = transition(cfg, EpisodeState(), market(base_available=D("1"), entry_signal=True))

    assert all(request.side != "BUY" for request in result.requests)


def test_disabled_adaptive_pricing_preserves_pre_feature_config_identity():
    from controllers.directional_trading.ok_rsi_core.episode import _hash

    cfg = config(adaptive_entry_pricing=False)
    legacy_values = asdict(cfg)
    legacy_values.pop("adaptive_entry_pricing")
    legacy_values.pop("adaptive_reserved_entry")
    legacy_values.pop("adaptive_reference_fallback")
    legacy_values.pop("thin_scout_release")
    legacy_values.pop("thin_scout_rearm")
    legacy_values.pop("profitable_episode_rearm")
    legacy_values.pop("deep_recovery_capital_release")
    legacy_values.pop("deep_recovery_rotation_exit")
    legacy_values.pop("deep_recovery_strength_runner")
    legacy_values.pop("scout_rebound_confirmation")
    legacy_values.pop("recovery_aware_scout")
    legacy_values.pop("recovery_rebound_scout")
    legacy_values.pop("structure_held_runner")
    legacy_values.pop("markup_release_runner")

    assert cfg.identity == _hash(legacy_values)
    assert replace(cfg, adaptive_entry_pricing=True).identity != cfg.identity
    assert replace(cfg, adaptive_entry_pricing=True, adaptive_reserved_entry=True).identity != cfg.identity
    assert replace(cfg, scout_rebound_confirmation=True).identity != cfg.identity
    assert replace(cfg, recovery_aware_scout=True).identity != cfg.identity
    assert replace(cfg, recovery_rebound_scout=True).identity != cfg.identity
    assert replace(cfg, thin_scout_release=True).identity != cfg.identity
    assert replace(cfg, thin_scout_release=True, thin_scout_rearm=True).identity != cfg.identity
    assert replace(cfg, profitable_episode_rearm=True).identity != cfg.identity
    assert replace(cfg, deep_recovery_capital_release=True).identity != cfg.identity
    assert replace(
        cfg,
        deep_recovery_capital_release=True,
        deep_recovery_rotation_exit=True,
    ).identity != cfg.identity
    assert replace(
        cfg,
        deep_recovery_capital_release=True,
        deep_recovery_strength_runner=True,
    ).identity != cfg.identity
    structure = replace(cfg, structure_held_runner=True)
    structure_values = legacy_values | {"structure_held_runner": True}
    assert structure.identity == _hash(structure_values)
    assert structure.identity != cfg.identity
    assert replace(cfg, structure_held_runner=True, markup_release_runner=True).identity != cfg.identity


def test_markup_release_runner_requires_structure_held_runner():
    with pytest.raises(EpisodeInputError, match="markup release"):
        config(markup_release_runner=True)


def test_deep_recovery_rotation_requires_capital_release():
    with pytest.raises(EpisodeInputError, match="deep recovery rotation"):
        config(deep_recovery_rotation_exit=True)


def test_deep_recovery_strength_runner_requires_release_and_excludes_rotation():
    with pytest.raises(EpisodeInputError, match="recovery strength"):
        config(deep_recovery_strength_runner=True)
    with pytest.raises(EpisodeInputError, match="mutually exclusive"):
        config(
            deep_recovery_capital_release=True,
            deep_recovery_rotation_exit=True,
            deep_recovery_strength_runner=True,
        )


def test_disabled_structure_runner_preserves_legacy_checkpoint_shape():
    state = transition(config(), EpisodeState(), market()).state
    payload = state.to_dict()

    assert "structure_runner_active" not in payload
    assert "exit_target_revision" not in payload
    assert "last_exit_trail_breach" not in payload
    request = next(iter(payload["orders"].values()))["request"]
    assert not any(key.startswith("exit_") for key in request)
    assert EpisodeState.from_dict(payload).to_dict() == payload


def test_recovery_aware_scout_preserves_immediate_non_recovery_entry():
    baseline = transition(config(), EpisodeState(), market(recovery=False))
    routed = transition(
        config(recovery_aware_scout=True),
        EpisodeState(),
        market(recovery=False),
    )

    assert routed.reason == baseline.reason == "scout"
    assert len(routed.requests) == len(baseline.requests) == 1
    expected = baseline.requests[0]
    request = routed.requests[0]
    assert (request.id, request.amount, request.price, request.reserved_quote) == (
        expected.id,
        expected.amount,
        expected.price,
        expected.reserved_quote,
    )
    assert request.setup_policy == "RECOVERY_AWARE_SCOUT_V1"
    assert request.setup_started_at == 0
    assert request.setup_low == 0
    assert EpisodeState.from_dict(routed.state.to_dict()).orders[request.id].request == request


def test_recovery_aware_scout_defers_recovery_pulse_and_requires_later_signal():
    cfg = config(recovery_aware_scout=True)

    deferred = transition(cfg, EpisodeState(), market(recovery=True))
    cleared_without_signal = transition(
        cfg,
        deferred.state,
        market(timestamp=6060, recovery=False, entry_signal=False),
    )
    admitted = transition(
        cfg,
        cleared_without_signal.state,
        market(timestamp=6120, recovery=False, entry_signal=True),
    )

    assert deferred.reason == "recovery_scout_deferred"
    assert deferred.requests == ()
    assert deferred.state.episode_id is None
    assert cleared_without_signal.reason == "need_dislocation"
    assert admitted.reason == "scout"
    assert admitted.requests[0].setup_policy == "RECOVERY_AWARE_SCOUT_V1"


@pytest.mark.parametrize("context", [{"hostile": True}, {"reset": True}, {"hostile": True, "reset": True}])
def test_recovery_aware_scout_does_not_veto_non_recovery_hostile_or_reset_context(context):
    result = transition(
        config(recovery_aware_scout=True),
        EpisodeState(),
        market(recovery=False, **context),
    )

    assert result.reason == "scout"
    assert result.requests[0].setup_policy == "RECOVERY_AWARE_SCOUT_V1"


def test_recovery_rebound_scout_preserves_exact_immediate_non_recovery_entry():
    baseline = transition(config(), EpisodeState(), market(recovery=False))
    routed = transition(
        config(recovery_rebound_scout=True),
        EpisodeState(),
        market(recovery=False),
    )

    expected = baseline.requests[0]
    request = routed.requests[0]
    assert routed.reason == baseline.reason == "scout"
    assert (request.id, request.amount, request.price, request.reserved_quote) == (
        expected.id,
        expected.amount,
        expected.price,
        expected.reserved_quote,
    )
    assert request.setup_policy == "RECOVERY_REBOUND_SCOUT_V1"
    assert request.setup_started_at == 0
    assert request.setup_confirmation_started_at == 0


def test_recovery_rebound_scout_waits_for_clear_then_confirms_original_opportunity():
    cfg = config(recovery_rebound_scout=True)
    armed = transition(
        cfg,
        EpisodeState(),
        market(recovery=True, low=D("99.8")),
    )
    waiting = transition(
        cfg,
        armed.state,
        market(
            timestamp=6060,
            recovery=True,
            low=D("99"),
            bid=D("99.05"),
            ask=D("99.1"),
            entry_signal=False,
        ),
    )
    cleared = transition(
        cfg,
        waiting.state,
        market(
            timestamp=6120,
            recovery=False,
            low=D("99.05"),
            bid=D("99.05"),
            ask=D("99.1"),
            entry_signal=False,
        ),
    )
    once = transition(
        cfg,
        cleared.state,
        market(
            timestamp=6180,
            recovery=False,
            low=D("99.05"),
            bid=D("99.05"),
            ask=D("99.1"),
            entry_signal=False,
        ),
    )
    confirmed = transition(
        cfg,
        once.state,
        market(
            timestamp=6240,
            recovery=False,
            low=D("99.05"),
            bid=D("100.25"),
            ask=D("100.3"),
            entry_signal=False,
        ),
    )

    assert armed.reason == "recovery_rebound_armed"
    assert armed.state.setup_start == 6000
    assert armed.state.setup_confirmation_start == 0
    assert waiting.reason == "recovery_rebound_wait"
    assert waiting.state.setup_low == D("99")
    assert cleared.reason == "recovery_rebound_cleared"
    assert cleared.state.setup_confirmation_start == 6120
    assert once.reason == "need_recovery_rebound"
    assert confirmed.reason == "scout"
    request = confirmed.requests[0]
    assert request.setup_policy == "RECOVERY_REBOUND_SCOUT_V1"
    assert request.setup_started_at == 6000
    assert request.setup_confirmation_started_at == 6120
    assert request.setup_low == D("99")
    assert request.setup_rebound == D("0.25")
    assert request.setup_no_new_low == 2
    assert EpisodeState.from_dict(confirmed.state.to_dict()).orders[request.id].request == request
    with pytest.raises(EpisodeInputError, match="recovery rebound"):
        replace(request, setup_confirmation_started_at=request.setup_started_at)


def test_recovery_rebound_wait_does_not_consume_post_clear_confirmation_window():
    cfg = config(
        recovery_rebound_scout=True,
        setup_bars=2,
        stabilization_bars=1,
    )
    result = transition(cfg, EpisodeState(), market(recovery=True))
    for timestamp in (6060, 6120, 6180, 6240, 6300):
        result = transition(
            cfg,
            result.state,
            market(timestamp=timestamp, recovery=True, entry_signal=False),
        )

    assert result.reason == "recovery_rebound_wait"
    assert result.state.episode_id is not None
    assert result.state.setup_confirmation_start == 0
    cleared = transition(
        cfg,
        result.state,
        market(timestamp=6360, recovery=False, entry_signal=False),
    )
    assert cleared.reason == "recovery_rebound_cleared"
    assert cleared.state.setup_confirmation_start == 6360


@pytest.mark.parametrize("context_flag", ["hostile", "reset", "cascade"])
def test_recovery_rebound_unsafe_context_abandons_unfunded_setup(context_flag):
    cfg = config(recovery_rebound_scout=True)
    armed = transition(cfg, EpisodeState(), market(recovery=True))

    abandoned = transition(
        cfg,
        armed.state,
        market(
            timestamp=6060,
            recovery=True,
            entry_signal=False,
            **{context_flag: True},
        ),
    )

    assert abandoned.reason == "recovery_rebound_abandoned"
    assert abandoned.requests == ()
    assert abandoned.state.episode_id is None
    assert abandoned.state.setup_level == -1


def test_recovery_rebound_post_clear_timeout_abandons_and_requires_fresh_signal():
    cfg = config(
        recovery_rebound_scout=True,
        setup_bars=2,
        stabilization_bars=1,
    )
    armed = transition(cfg, EpisodeState(), market(recovery=True))
    cleared = transition(
        cfg,
        armed.state,
        market(
            timestamp=6060,
            recovery=False,
            bid=D("99"),
            ask=D("99.1"),
            low=D("99"),
            entry_signal=False,
        ),
    )
    waiting = transition(
        cfg,
        cleared.state,
        market(
            timestamp=6120,
            recovery=False,
            bid=D("99"),
            ask=D("99.1"),
            low=D("99"),
            entry_signal=False,
        ),
    )
    expired = transition(
        cfg,
        waiting.state,
        market(
            timestamp=6180,
            recovery=False,
            bid=D("99"),
            ask=D("99.1"),
            low=D("99"),
            entry_signal=False,
        ),
    )

    assert expired.reason == "recovery_rebound_expired"
    assert expired.state.episode_id is None
    assert expired.requests == ()


def test_rebound_scout_latches_signal_tracks_low_and_waits_for_causal_reclaim():
    cfg = config(scout_rebound_confirmation=True)

    armed = transition(cfg, EpisodeState(), market(low=D("99.8")))
    lower = transition(
        cfg,
        armed.state,
        market(timestamp=6060, bid=D("99.05"), ask=D("99.1"), low=D("99"), entry_signal=False),
    )
    once = transition(
        cfg,
        lower.state,
        market(timestamp=6120, bid=D("99.05"), ask=D("99.1"), low=D("99.05"), entry_signal=False),
    )
    confirmed = transition(
        cfg,
        once.state,
        market(timestamp=6180, bid=D("100.25"), ask=D("100.3"), low=D("99.05"), entry_signal=False),
    )

    assert armed.reason == "scout_rebound_armed"
    assert armed.requests == ()
    assert armed.state.setup_level == 0
    assert lower.state.setup_low == D("99")
    assert lower.state.no_new_low == 0
    assert once.reason == "need_scout_rebound"
    assert confirmed.reason == "scout"
    request = confirmed.requests[0]
    assert request.price == D("100.3")
    assert request.setup_policy == "SCOUT_REBOUND_V1"
    assert request.setup_started_at == 6000
    assert request.setup_low == D("99")
    assert request.setup_rebound == D("0.25")
    assert request.setup_no_new_low == 2
    assert EpisodeState.from_dict(confirmed.state.to_dict()).orders[request.id].request == request


def test_rebound_scout_timeout_abandons_empty_episode_and_requires_fresh_signal():
    cfg = config(scout_rebound_confirmation=True, setup_bars=2, stabilization_bars=1)
    armed = transition(cfg, EpisodeState(), market(low=D("99.8")))
    waiting = transition(
        cfg,
        armed.state,
        market(timestamp=6060, bid=D("99"), ask=D("99.1"), low=D("99"), entry_signal=False),
    )

    expired = transition(
        cfg,
        waiting.state,
        market(timestamp=6120, bid=D("99"), ask=D("99.1"), low=D("99"), entry_signal=False),
    )
    no_signal = transition(
        cfg,
        expired.state,
        market(timestamp=6180, bid=D("99"), ask=D("99.1"), low=D("99"), entry_signal=False),
    )
    fresh = transition(
        cfg,
        no_signal.state,
        market(timestamp=6240, bid=D("99"), ask=D("99.1"), low=D("99"), entry_signal=True),
    )

    assert expired.reason == "scout_rebound_expired"
    assert expired.state.episode_id is None
    assert expired.state.setup_level == -1
    assert no_signal.reason == "need_dislocation"
    assert no_signal.requests == ()
    assert fresh.reason == "scout_rebound_armed"
    assert fresh.state.episode_id is not None


@pytest.mark.parametrize("context_flag", ["hostile", "reset", "recovery", "cascade"])
def test_rebound_scout_context_reset_abandons_unfunded_setup(context_flag):
    cfg = config(scout_rebound_confirmation=True)
    armed = transition(cfg, EpisodeState(), market(low=D("99.8")))

    cleared = transition(
        cfg,
        armed.state,
        market(timestamp=6060, entry_signal=False, **{context_flag: True}),
    )

    assert cleared.requests == ()
    assert cleared.state.episode_id is None
    assert cleared.state.setup_level == -1


def test_rebound_scout_is_opt_in_and_sell_only_never_arms_it():
    immediate = transition(config(), EpisodeState(), market())
    sell_only = transition(
        config(
            mode="sell_only",
            scout_rebound_confirmation=True,
            assignment_id="assigned",
            initial_base=D("1"),
            initial_cost=D("100"),
        ),
        EpisodeState(),
        market(base_available=D("1")),
    )

    assert immediate.requests[0].setup_policy == "IMMEDIATE"
    assert sell_only.state.setup_level == -1
    assert all(request.side != "BUY" for request in sell_only.requests)


def test_recovery_rebound_scout_never_arms_for_sell_only_inventory():
    result = transition(
        config(
            mode="sell_only",
            recovery_rebound_scout=True,
            assignment_id="assigned",
            initial_base=D("1"),
            initial_cost=D("100"),
        ),
        EpisodeState(),
        market(base_available=D("1"), recovery=True),
    )

    assert result.state.setup_level == -1
    assert result.state.setup_confirmation_start == 0
    assert all(request.side != "BUY" for request in result.requests)


def test_entry_treatments_cannot_be_composed_without_a_new_policy_contract():
    with pytest.raises(EpisodeInputError, match="cannot be combined"):
        config(adaptive_entry_pricing=True, scout_rebound_confirmation=True)
    with pytest.raises(EpisodeInputError, match="cannot be combined"):
        config(adaptive_entry_pricing=True, recovery_aware_scout=True)
    with pytest.raises(EpisodeInputError, match="cannot be combined"):
        config(scout_rebound_confirmation=True, recovery_aware_scout=True)
    with pytest.raises(EpisodeInputError, match="cannot be combined"):
        config(recovery_aware_scout=True, recovery_rebound_scout=True)
    with pytest.raises(EpisodeInputError, match="cannot be combined"):
        config(scout_rebound_confirmation=True, recovery_rebound_scout=True)


def test_stabilization_window_must_fit_before_setup_expiry():
    with pytest.raises(EpisodeInputError, match="stabilization_bars cannot exceed setup_bars"):
        config(setup_bars=10, stabilization_bars=15)


def test_setup_waits_for_two_subsequent_closed_bars_and_skips_crossed_levels():
    cfg = config()
    state, _ = bought(cfg)
    first = transition(
        cfg, state, market(timestamp=6120, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=D("1"))
    )
    assert not first.requests
    second = transition(
        cfg, first.state, market(timestamp=6180, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )
    assert not second.requests
    third = transition(
        cfg, second.state, market(timestamp=6240, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )
    assert len(third.requests) == 1
    assert third.requests[0].level == 3
    assert third.state.levels[1:3] == ["SKIPPED", "SKIPPED"]
    assert third.requests[0].reserved_quote <= D("250")


def test_sequential_scale_in_selects_lowest_eligible_level_without_skips():
    cfg = config(sequential_scale_ins=True)
    state, _ = bought(cfg)
    first = transition(
        cfg, state, market(timestamp=6120, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=D("1"))
    )
    second = transition(
        cfg, first.state, market(timestamp=6180, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )
    third = transition(
        cfg, second.state, market(timestamp=6240, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )

    assert third.requests[0].level == 1
    assert third.state.levels[2:4] == ["OPEN", "OPEN"]


def test_causal_scale_in_requires_fresh_owner_buy_signal_at_authorization():
    cfg = config(sequential_scale_ins=True, require_scale_in_signal=True)
    state, _ = bought(cfg)
    armed = transition(
        cfg,
        state,
        market(timestamp=6120, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=D("1")),
    )
    once = transition(
        cfg,
        armed.state,
        market(timestamp=6180, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1")),
    )
    blocked = transition(
        cfg,
        once.state,
        market(
            timestamp=6240,
            bid=D("96.8"),
            ask=D("96.8"),
            low=D("96.1"),
            base_available=D("1"),
            entry_signal=False,
        ),
    )
    admitted = transition(
        cfg,
        blocked.state,
        market(timestamp=6300, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1")),
    )

    assert blocked.reason == "need_scale_in_signal"
    assert not blocked.requests
    assert admitted.requests[0].level == 1


def test_sequential_setup_expiry_rearms_level_instead_of_consuming_budget():
    cfg = config(sequential_scale_ins=True, setup_bars=2)
    state, _ = bought(cfg)
    armed = transition(
        cfg, state, market(timestamp=6120, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=D("1"))
    )
    once = transition(
        cfg, armed.state, market(timestamp=6180, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )
    expired = transition(
        cfg, once.state, market(timestamp=6240, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )

    assert expired.reason == "setup_expired_rearm"
    assert expired.state.levels[1] == "OPEN"
    assert remaining_budget(cfg, expired.state) == D("900")


def test_adverse_episode_latch_blocks_scale_in_until_completed_hourly_recovery():
    cfg = config(sequential_scale_ins=True, hostile_risk_latch=True, stabilization_bars=1)
    first = transition(cfg, EpisodeState(), market(reset=True))
    assert first.requests[0].reserved_quote <= D("25")
    scout_base = first.requests[0].amount
    fill = OrderUpdate(first.requests[0].id, filled_base=scout_base, filled_quote=D("25"), done=True)
    state = transition(
        cfg,
        first.state,
        market(timestamp=6060, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=scout_base),
        [fill],
    ).state
    armed = transition(
        cfg,
        state,
        market(timestamp=6120, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=scout_base),
    )
    blocked = transition(
        cfg,
        armed.state,
        market(timestamp=6180, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=scout_base),
    )
    released = transition(
        cfg,
        blocked.state,
        market(
            timestamp=6240,
            bid=D("96.8"),
            ask=D("96.8"),
            low=D("96.1"),
            base_available=scout_base,
            hourly_recovered=True,
        ),
    )
    admitted = transition(
        cfg,
        released.state,
        market(
            timestamp=6300,
            bid=D("96.8"),
            ask=D("96.8"),
            low=D("96.1"),
            base_available=scout_base,
            hourly_recovered=True,
        ),
    )

    assert blocked.reason == "adverse_regime_latch"
    assert not blocked.requests
    assert released.reason == "level_armed"
    assert admitted.requests[0].level == 1


def test_hostile_context_after_scout_latches_episode_before_any_scale_in():
    cfg = config(sequential_scale_ins=True, hostile_risk_latch=True, stabilization_bars=1)
    first = transition(cfg, EpisodeState(), market())
    scout_base = first.requests[0].amount
    fill = OrderUpdate(first.requests[0].id, scout_base, D("100"), done=True)
    defensive = transition(
        cfg,
        first.state,
        market(timestamp=6060, base_available=scout_base, hostile=True, entry_signal=False),
        [fill],
    )
    blocked = transition(
        cfg,
        defensive.state,
        market(timestamp=6120, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=scout_base),
    )

    assert defensive.state.adverse_entry_latched is True
    assert blocked.reason == "adverse_regime_latch"
    assert not blocked.requests


def test_same_bar_cannot_count_again_as_stabilization():
    cfg = config()
    state, _ = bought(cfg)
    snap = market(timestamp=6120, bid=D("97"), ask=D("97"), low=D("96"), base_available=D("1"))
    once = transition(cfg, state, snap)
    again = transition(cfg, once.state, snap)
    assert once.state.to_dict() == again.state.to_dict()
    assert not again.requests


def test_unknown_or_reversed_execution_receipt_fails_closed():
    cfg = config()
    state, fill = bought(cfg)
    with pytest.raises(EpisodeInputError, match="unknown"):
        transition(cfg, state, market(timestamp=6120), [replace(fill, request_id="foreign")])
    with pytest.raises(EpisodeInputError, match="decreased"):
        transition(cfg, state, market(timestamp=6120), [replace(fill, filled_base=D("0.5"))])


def test_state_roundtrip_preserves_budget_levels_and_pending_identity():
    cfg = config()
    first = transition(cfg, EpisodeState(), market())
    restored = EpisodeState.from_dict(first.state.to_dict())
    assert restored.to_dict() == first.state.to_dict()
    again = transition(cfg, restored, market(timestamp=6060))
    assert not again.requests
    assert remaining_budget(cfg, again.state) == D("900")
    with pytest.raises(EpisodeInputError, match="configuration"):
        transition(replace(cfg, budget_quote=D("2000")), restored, market(timestamp=6120))


def test_checkpoint_rejects_buy_authorization_with_reopened_level():
    cfg = config()
    reserved = transition(cfg, EpisodeState(), market())
    corrupted = EpisodeState.from_dict(reserved.state.to_dict())
    corrupted.levels[0] = "OPEN"

    with pytest.raises(EpisodeInputError, match="authorization level"):
        transition(cfg, corrupted, market(timestamp=6060))


def test_requests_carry_exact_scout_scale_and_distribution_intents():
    cfg = config()
    scout = transition(cfg, EpisodeState(), market())
    assert scout.requests[0].intent == "SCOUT_BUY"
    fill = OrderUpdate(scout.requests[0].id, D("1"), D("100"), done=True)
    state = transition(
        cfg,
        scout.state,
        market(timestamp=6060, base_available=D("1"), entry_signal=False),
        [fill],
    ).state
    armed = transition(
        cfg, state, market(timestamp=6120, bid=D("96.8"), ask=D("96.8"), low=D("96"), base_available=D("1"))
    )
    once = transition(
        cfg, armed.state, market(timestamp=6180, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )
    scale = transition(
        cfg, once.state, market(timestamp=6240, bid=D("96.8"), ask=D("96.8"), low=D("96.1"), base_available=D("1"))
    )
    assert scale.requests[0].intent == "SCALE_IN_BUY"

    markup = transition(
        cfg,
        state,
        market(timestamp=6300, bid=D("101"), ask=D("101"), low=D("101"), base_available=D("1"), markup=True),
    )
    distribution = transition(
        cfg,
        markup.state,
        market(timestamp=6360, bid=D("108"), ask=D("108"), low=D("108"), base_available=D("1")),
    )
    assert distribution.requests[0].intent == "DISTRIBUTION_SELL"


def test_profitable_recovery_arms_trail_and_exits_without_hourly_markup():
    cfg = config(recovery_exit_enabled=True)
    state, _ = bought(cfg)
    armed = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            atr_15m=D(".1"),
            base_available=D("1"),
            recovery=True,
        ),
    )
    higher = transition(
        cfg,
        armed.state,
        market(timestamp=6180, bid=D("102"), ask=D("102"), low=D("102"), atr_15m=D(".1"), base_available=D("1")),
    )
    result = transition(
        cfg,
        higher.state,
        market(
            timestamp=6240,
            bid=D("101.4"),
            ask=D("101.4"),
            low=D("101.4"),
            atr_15m=D(".1"),
            base_available=D("1"),
        ),
    )

    assert armed.state.phase == "RECOVER"
    assert armed.reason == "recovery_exit_armed"
    assert not armed.requests
    assert result.state.phase == "EXIT"
    assert result.reason == "recovery_take_profit"
    assert result.requests[0].side == "SELL"
    assert result.requests[0].intent == "RECOVERY_TAKE_PROFIT"


def test_recovery_exit_never_bypasses_fee_adjusted_profit_floor():
    cfg = config(min_profit=D(".02"), recovery_exit_enabled=True)
    state, _ = bought(cfg)
    result = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("101.99"),
            ask=D("101.99"),
            low=D("101.99"),
            base_available=D("1"),
            recovery=True,
        ),
    )

    assert result.state.phase == "RECOVER"
    assert result.reason == "recovery_profit_guard"
    assert not result.requests


def test_sell_only_assignment_uses_same_profitable_recovery_exit_without_buying():
    cfg = config(
        mode="sell_only",
        assignment_id="inventory-1",
        initial_base=D("1"),
        initial_cost=D("100"),
        recovery_exit_enabled=True,
    )
    armed = transition(
        cfg,
        EpisodeState(),
        market(base_available=D("1"), bid=D("101"), ask=D("101"), atr_15m=D(".1"), recovery=True),
    )
    result = transition(
        cfg,
        armed.state,
        market(
            timestamp=6060,
            base_available=D("1"),
            bid=D("100.4"),
            ask=D("100.4"),
            low=D("100.4"),
            atr_15m=D(".1"),
        ),
    )

    assert armed.reason == "recovery_exit_armed"
    assert not armed.requests
    assert len(result.requests) == 1
    assert result.requests[0].side == "SELL"
    assert result.requests[0].intent == "RECOVERY_TAKE_PROFIT"


def test_sell_only_recovery_trail_remains_profit_protected_when_ordinary_sells_are_not():
    cfg = config(
        mode="sell_only",
        assignment_id="inventory-1",
        initial_base=D("1"),
        initial_cost=D("100"),
        require_profit=False,
        recovery_exit_enabled=True,
    )
    armed = transition(
        cfg,
        EpisodeState(),
        market(base_available=D("1"), bid=D("101"), ask=D("101"), atr_15m=D(".1"), recovery=True),
    )
    blocked = transition(
        cfg,
        armed.state,
        market(timestamp=6060, base_available=D("1"), bid=D("99"), ask=D("99"), low=D("99"), atr_15m=D(".1")),
    )

    assert blocked.reason == "profit_guard_blocked"
    assert not blocked.requests


def test_distribution_targets_ratchet_and_profit_guard_blocks_terminal_sale():
    cfg = config()
    state, _ = bought(cfg)
    markup = transition(
        cfg, state, market(timestamp=6120, bid=D("101"), ask=D("101"), low=D("101"), base_available=D("1"), markup=True)
    )
    assert markup.state.phase == "MARKUP"
    distribute = transition(
        cfg, markup.state, market(timestamp=6180, bid=D("108"), ask=D("108"), low=D("108"), base_available=D("1"))
    )
    assert distribute.state.target_base == D("0.6")
    assert distribute.requests[0].side == "SELL"
    assert distribute.requests[0].intent == "DISTRIBUTION_SELL"
    assert distribute.requests[0].amount == D("0.4")
    fill = OrderUpdate(distribute.requests[0].id, D("0.4"), D("43.2"), done=True)
    crash = transition(
        cfg,
        distribute.state,
        market(timestamp=6240, bid=D("90"), ask=D("90"), low=D("90"), base_available=D("0.6")),
        [fill],
    )
    assert crash.state.phase == "EXIT"
    assert crash.state.target_base == D("0")
    assert crash.reason == "profit_guard_blocked"
    assert not crash.requests
    assert crash.state.gross_spent == D("100")
    assert crash.state.cost == D("60")


def test_profitable_thin_scout_release_is_opt_in_and_uses_existing_first_target():
    default_state, _ = bought(config())
    default_markup = transition(
        config(),
        default_state,
        market(
            timestamp=6120,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            markup=True,
        ),
    )
    default_distribution = transition(
        config(),
        default_markup.state,
        market(
            timestamp=6180,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("1"),
        ),
    )

    cfg = config(thin_scout_release=True)
    state, _ = bought(cfg)
    markup = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            markup=True,
        ),
    )
    released = transition(
        cfg,
        markup.state,
        market(
            timestamp=6180,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("1"),
        ),
    )

    assert default_distribution.state.phase == "DISTRIBUTE"
    assert default_distribution.state.target_base == D(".8")
    assert default_distribution.requests[0].amount == D(".2")
    assert default_distribution.requests[0].intent == "DISTRIBUTION_SELL"
    assert default_distribution.requests[0].exit_policy == "DEFAULT"
    assert released.state.phase == "EXIT"
    assert released.state.target_base == D("0")
    assert released.state.exit_intent == "THIN_SCOUT_RELEASE"
    assert released.reason == "thin_scout_release"
    assert released.requests[0].amount == D("1")
    assert released.requests[0].intent == "THIN_SCOUT_RELEASE"
    assert released.requests[0].exit_policy == "THIN_SCOUT_RELEASE_V1"
    assert released.requests[0].exit_target_base == D("0")
    assert released.requests[0].exit_target_revision == 1


def test_thin_scout_release_excludes_any_episode_with_a_scale_in_fill():
    cfg = config(thin_scout_release=True)
    state, _ = bought(cfg)
    setup = market(
        timestamp=6120,
        bid=D("96.8"),
        ask=D("96.8"),
        low=D("96"),
        base_available=D("1"),
    )
    armed = transition(cfg, state, setup)
    confirmed = transition(cfg, armed.state, replace(setup, timestamp=6180, low=D("96.1")))
    scale = transition(cfg, confirmed.state, replace(setup, timestamp=6240, low=D("96.1")))
    scale_request = scale.requests[0]
    scale_fill = OrderUpdate(
        scale_request.id,
        scale_request.amount,
        scale_request.amount * scale_request.price,
        done=True,
    )
    base = D("1") + scale_request.amount
    settled = transition(
        cfg,
        scale.state,
        market(timestamp=6300, base_available=base, entry_signal=False),
        [scale_fill],
    )
    markup = transition(
        cfg,
        settled.state,
        market(
            timestamp=6360,
            bid=D("105"),
            ask=D("105"),
            low=D("105"),
            base_available=base,
            markup=True,
        ),
    )
    distributed = transition(
        cfg,
        markup.state,
        market(
            timestamp=6420,
            bid=D("110"),
            ask=D("110"),
            low=D("110"),
            base_available=base,
        ),
    )

    assert scale_request.level > 0
    assert distributed.state.phase == "DISTRIBUTE"
    assert distributed.state.target_base > D("0")
    assert distributed.requests[0].amount < base
    assert distributed.requests[0].intent == "DISTRIBUTION_SELL"
    assert distributed.requests[0].exit_policy == "DEFAULT"


def test_sell_only_assignment_never_becomes_a_thin_scout_release():
    cfg = config(
        mode="sell_only",
        assignment_id="assigned-inventory",
        initial_base=D("1"),
        initial_cost=D("100"),
        thin_scout_release=True,
    )
    assigned = transition(
        cfg,
        EpisodeState(),
        market(base_available=D("1"), entry_signal=False),
    )
    markup = transition(
        cfg,
        assigned.state,
        market(
            timestamp=6060,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            entry_signal=False,
            markup=True,
        ),
    )
    distributed = transition(
        cfg,
        markup.state,
        market(
            timestamp=6120,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("1"),
            entry_signal=False,
        ),
    )

    assert distributed.state.phase == "DISTRIBUTE"
    assert distributed.state.target_base == D(".8")
    assert distributed.requests[0].intent == "DISTRIBUTION_SELL"
    assert distributed.requests[0].exit_policy == "DEFAULT"
    assert all(order.request.side == "SELL" for order in distributed.state.orders.values())


def test_thin_scout_release_keeps_profit_guard_and_retries_only_unfilled_base():
    cfg = config(
        thin_scout_release=True,
        require_profit=False,
        min_profit=D(".10"),
    )
    state, _ = bought(cfg)
    markup = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("100"),
            ask=D("100"),
            low=D("100"),
            base_available=D("1"),
            markup=True,
        ),
    )
    blocked = transition(
        cfg,
        markup.state,
        market(
            timestamp=6180,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("1"),
        ),
    )

    assert blocked.state.phase == "EXIT"
    assert blocked.state.target_base == D("0")
    assert blocked.state.exit_intent == "THIN_SCOUT_RELEASE"
    assert blocked.reason == "profit_guard_blocked"
    assert not blocked.requests

    authorized = transition(
        cfg,
        blocked.state,
        market(
            timestamp=6240,
            bid=D("111"),
            ask=D("111"),
            low=D("111"),
            base_available=D("1"),
        ),
    )
    pending = transition(
        cfg,
        authorized.state,
        market(
            timestamp=6300,
            bid=D("111"),
            ask=D("111"),
            low=D("111"),
            base_available=D("1"),
        ),
    )
    partial = transition(
        cfg,
        EpisodeState.from_dict(authorized.state.to_dict()),
        market(
            timestamp=6300,
            bid=D("111"),
            ask=D("111"),
            low=D("111"),
            base_available=D(".6"),
        ),
        [OrderUpdate(authorized.requests[0].id, D(".4"), D("44.4"), done=True)],
    )

    assert authorized.requests[0].intent == "THIN_SCOUT_RELEASE"
    assert authorized.requests[0].exit_policy == "THIN_SCOUT_RELEASE_V1"
    assert pending.reason == "pending_execution"
    assert not pending.requests
    assert partial.state.base == D(".6")
    assert partial.requests[0].amount == D(".6")
    assert partial.requests[0].intent == "THIN_SCOUT_RELEASE"
    assert partial.requests[0].exit_policy == "THIN_SCOUT_RELEASE_V1"


def test_thin_scout_release_dust_stays_latched_until_exchange_minimum_allows_it():
    cfg = config(thin_scout_release=True)
    state, _ = bought(cfg)
    markup = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            markup=True,
        ),
    )
    dust = transition(
        cfg,
        markup.state,
        market(
            timestamp=6180,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("1"),
            min_amount=D("2"),
        ),
    )
    released = transition(
        cfg,
        EpisodeState.from_dict(dust.state.to_dict()),
        market(
            timestamp=6240,
            bid=D("105"),
            ask=D("105"),
            low=D("105"),
            base_available=D("1"),
        ),
    )

    assert dust.state.phase == "EXIT"
    assert dust.state.target_base == D("0")
    assert dust.reason == "dust"
    assert not dust.requests
    assert released.requests[0].amount == D("1")
    assert released.requests[0].intent == "THIN_SCOUT_RELEASE"


def test_thin_scout_release_does_not_latch_when_wallet_cannot_cover_episode_inventory():
    cfg = config(thin_scout_release=True)
    state, _ = bought(cfg)
    markup = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            markup=True,
        ),
    )

    short = transition(
        cfg,
        markup.state,
        market(
            timestamp=6180,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D(".4"),
        ),
    )

    assert short.reason == "inventory_balance_shortfall"
    assert short.state.phase == "MARKUP"
    assert short.state.target_base == D("1")
    assert short.state.exit_intent is None
    assert not short.requests


def test_thin_scout_rearm_reopens_entry_after_reconciled_full_release():
    cfg = config(
        thin_scout_release=True,
        thin_scout_rearm=True,
        profitable_episode_rearm=True,
    )
    state, _ = bought(cfg)
    markup = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            markup=True,
        ),
    )
    released = transition(
        cfg,
        markup.state,
        market(
            timestamp=6180,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("1"),
        ),
    )
    release = released.requests[0]
    settled = transition(
        cfg,
        released.state,
        market(
            timestamp=6240,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("0"),
            entry_signal=False,
        ),
        [
            OrderUpdate(
                release.id,
                release.amount,
                release.amount * release.price,
                done=True,
            )
        ],
    )

    assert settled.reason == "thin_scout_rearmed"
    assert settled.state.phase == "WATCH"
    assert settled.state.episode_id is None
    assert settled.state.episode_number == released.state.episode_number
    assert settled.state.orders[release.id].done is True
    assert settled.state.base == D("0")
    assert not settled.requests

    restored = EpisodeState.from_dict(settled.state.to_dict())
    duplicate = transition(
        cfg,
        restored,
        market(
            timestamp=6300,
            bid=D("104"),
            ask=D("104"),
            low=D("104"),
            base_available=D("0"),
            entry_signal=False,
        ),
        [
            OrderUpdate(
                release.id,
                release.amount,
                release.amount * release.price,
                done=True,
            )
        ],
    )
    assert duplicate.state.base == D("0")
    assert duplicate.state.gross_spent == D("0")
    assert duplicate.state.sell_proceeds == D("0")
    assert duplicate.state.orders[release.id].done is True

    next_entry = transition(
        cfg,
        duplicate.state,
        market(
            timestamp=6360,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=D("0"),
            entry_signal=True,
        ),
    )
    assert next_entry.requests[0].side == "BUY"
    assert next_entry.requests[0].level == 0


def test_thin_scout_rearm_requires_thin_scout_release():
    with pytest.raises(EpisodeInputError, match="thin scout rearm"):
        config(thin_scout_rearm=True)


def _completed_profitable_episode(cfg):
    state, _ = bought(cfg)
    armed = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("102"),
            ask=D("102"),
            low=D("102"),
            atr_15m=D("0.1"),
            base_available=D("1"),
            markup=True,
        ),
    )
    high = transition(
        cfg,
        armed.state,
        market(
            timestamp=6180,
            bid=D("103"),
            ask=D("103"),
            low=D("103"),
            atr_15m=D("0.1"),
            base_available=D("1"),
            markup=True,
        ),
    )
    exiting = transition(
        cfg,
        high.state,
        market(
            timestamp=6240,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            base_available=D("1"),
        ),
    )
    request = exiting.requests[0]
    completed = transition(
        cfg,
        exiting.state,
        market(
            timestamp=6300,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            base_available=D("0"),
            entry_signal=True,
        ),
        [
            OrderUpdate(
                request.id,
                request.amount,
                request.amount * request.price,
                done=True,
            )
        ],
    )
    return completed, request


def test_profitable_episode_rearm_reopens_after_any_profitable_buy_sell_exit():
    cfg = config(profitable_episode_rearm=True)

    completed, request = _completed_profitable_episode(cfg)

    assert completed.reason == "profitable_episode_rearmed"
    assert completed.state.phase == "WATCH"
    assert completed.state.episode_id is None
    assert completed.state.base == D("0")
    assert completed.state.orders[request.id].done is True
    assert completed.requests == ()

    restored = EpisodeState.from_dict(completed.state.to_dict())
    next_entry = transition(
        cfg,
        restored,
        market(
            timestamp=6360,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=D("0"),
            entry_signal=True,
        ),
    )
    assert next_entry.requests[0].side == "BUY"
    assert next_entry.requests[0].level == 0


def test_profitable_episode_rearm_is_default_off_and_requires_positive_net_cash():
    ordinary, _ = _completed_profitable_episode(config())
    assert ordinary.reason == "episode_completed"
    assert ordinary.state.phase == "COOLDOWN"

    cfg = config(profitable_episode_rearm=True, require_profit=False)
    state, _ = bought(cfg)
    armed = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            atr_15m=D("0.1"),
            base_available=D("1"),
            markup=True,
        ),
    )
    high = transition(
        cfg,
        armed.state,
        market(
            timestamp=6180,
            bid=D("99.5"),
            ask=D("99.5"),
            low=D("99.5"),
            atr_15m=D("0.1"),
            base_available=D("1"),
            markup=True,
        ),
    )
    exiting = transition(
        cfg,
        high.state,
        market(
            timestamp=6240,
            bid=D("98.9"),
            ask=D("98.9"),
            low=D("98.9"),
            atr_15m=D("0.1"),
            base_available=D("1"),
        ),
    )
    request = exiting.requests[0]
    completed = transition(
        cfg,
        exiting.state,
        market(timestamp=6300, base_available=D("0"), entry_signal=False),
        [OrderUpdate(request.id, request.amount, request.amount * request.price, done=True)],
    )
    assert completed.state.sell_proceeds < completed.state.gross_spent
    assert completed.reason == "episode_completed"
    assert completed.state.phase == "COOLDOWN"


def test_profitable_episode_rearm_does_not_reassign_sell_only_inventory():
    cfg = config(
        mode="sell_only",
        assignment_id="assigned",
        initial_base=D("1"),
        initial_cost=D("100"),
        profitable_episode_rearm=True,
    )
    assigned = transition(
        cfg,
        EpisodeState(),
        market(base_available=D("1"), entry_signal=False),
    )
    armed = transition(
        cfg,
        assigned.state,
        market(
            timestamp=6060,
            bid=D("102"),
            ask=D("102"),
            low=D("102"),
            atr_15m=D("0.1"),
            base_available=D("1"),
            entry_signal=False,
            markup=True,
        ),
    )
    high = transition(
        cfg,
        armed.state,
        market(
            timestamp=6120,
            bid=D("103"),
            ask=D("103"),
            low=D("103"),
            atr_15m=D("0.1"),
            base_available=D("1"),
            entry_signal=False,
            markup=True,
        ),
    )
    exiting = transition(
        cfg,
        high.state,
        market(
            timestamp=6180,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            base_available=D("1"),
            entry_signal=False,
        ),
    )
    request = exiting.requests[0]
    completed = transition(
        cfg,
        exiting.state,
        market(timestamp=6240, base_available=D("0"), entry_signal=False),
        [OrderUpdate(request.id, request.amount, request.amount * request.price, done=True)],
    )

    assert completed.state.sell_proceeds > completed.state.gross_spent
    assert completed.reason == "episode_completed"
    assert completed.state.phase == "COOLDOWN"
    assert completed.state.assignment_consumed is True


def _deep_recovery_state(cfg):
    state, _ = bought(cfg)
    setup = market(
        timestamp=6120,
        bid=D("95.8"),
        ask=D("95.8"),
        low=D("95"),
        base_available=D("1"),
        entry_signal=False,
    )
    armed = transition(cfg, state, setup)
    stable = transition(cfg, armed.state, replace(setup, timestamp=6180, low=D("95.1")))
    scale = transition(cfg, stable.state, replace(setup, timestamp=6240, low=D("95.1")))
    request = scale.requests[0]
    assert request.level == len(cfg.level_fractions) - 1
    total_base = state.base + request.amount
    settled = transition(
        cfg,
        scale.state,
        market(
            timestamp=6300,
            bid=D("96"),
            ask=D("96"),
            low=D("96"),
            base_available=total_base,
            entry_signal=False,
        ),
        [OrderUpdate(request.id, request.amount, request.amount * request.price, done=True)],
    )
    recovered = transition(
        cfg,
        settled.state,
        market(
            timestamp=6360,
            bid=D("98"),
            ask=D("98"),
            low=D("98"),
            base_available=total_base,
            entry_signal=False,
            recovery=True,
        ),
    )
    assert recovered.state.phase == "RECOVER"
    return recovered.state, total_base


def test_deep_recovery_releases_principal_to_existing_runner_target():
    cfg = config(deep_recovery_capital_release=True)
    state, total_base = _deep_recovery_state(cfg)
    basis = state.cost / state.base
    release = transition(
        cfg,
        state,
        market(
            timestamp=6420,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=total_base,
            entry_signal=False,
            markup=True,
        ),
    )

    assert (D("99") - basis) / D("1") < cfg.distribution_atr_levels[0]
    assert release.state.phase == "DISTRIBUTE"
    assert release.state.target_base == total_base * cfg.remaining_fractions[-1]
    assert release.state.exit_target_revision == 1
    assert release.state.exit_intent == "RECOVERY_CAPITAL_RELEASE"
    assert release.reason == "deep_recovery_capital_release"
    assert len(release.requests) == 1
    request = release.requests[0]
    assert request.side == "SELL"
    assert request.intent == "RECOVERY_CAPITAL_RELEASE"
    assert request.exit_policy == "DEEP_RECOVERY_CAPITAL_RELEASE_V4"
    assert request.exit_target_base == release.state.target_base
    assert request.exit_target_revision == 1
    assert request.price >= state.cost / state.base


def test_deep_recovery_rotation_exits_the_full_episode_to_normal_cooldown():
    cfg = config(
        deep_recovery_capital_release=True,
        deep_recovery_rotation_exit=True,
    )
    state, total_base = _deep_recovery_state(cfg)
    rotated = transition(
        cfg,
        state,
        market(
            timestamp=6420,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=total_base,
            entry_signal=False,
            markup=True,
        ),
    )

    assert rotated.state.phase == "EXIT"
    assert rotated.state.target_base == 0
    assert rotated.state.exit_target_revision == 1
    assert rotated.state.exit_intent == "RECOVERY_ROTATION_EXIT"
    assert rotated.reason == "deep_recovery_rotation_exit"
    request = rotated.requests[0]
    assert request.amount == total_base
    assert request.intent == "RECOVERY_ROTATION_EXIT"
    assert request.exit_policy == "DEEP_RECOVERY_ROTATION_EXIT_V5"
    assert request.exit_target_base == 0
    assert request.price >= state.cost / state.base

    restored = EpisodeState.from_dict(rotated.state.to_dict())
    completed = transition(
        cfg,
        restored,
        market(
            timestamp=6480,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=D("0"),
            entry_signal=False,
        ),
        [
            OrderUpdate(
                request.id,
                request.amount,
                request.amount * request.price,
                done=True,
            )
        ],
    )

    assert completed.reason == "episode_completed"
    assert completed.state.phase == "COOLDOWN"
    assert completed.state.base == 0
    assert completed.state.sell_proceeds > completed.state.gross_spent
    assert not completed.requests


def test_deep_recovery_strength_runner_scales_target_by_normalized_extension():
    cfg = config(
        deep_recovery_capital_release=True,
        deep_recovery_strength_runner=True,
    )
    state, total_base = _deep_recovery_state(cfg)
    basis = state.cost / state.base
    bid = D("99")
    released = transition(
        cfg,
        state,
        market(
            timestamp=6420,
            bid=bid,
            ask=bid,
            low=bid,
            base_available=total_base,
            entry_signal=False,
            markup=True,
        ),
    )

    extension = (bid - basis) / D("1")
    strength = extension / cfg.distribution_atr_levels[0]
    v14_target = total_base * cfg.remaining_fractions[-1]
    expected_target = v14_target * strength

    assert D("0") < strength < D("1")
    assert released.state.phase == "DISTRIBUTE"
    assert released.state.target_base == expected_target
    assert D("0") < released.state.target_base < v14_target
    assert released.state.exit_target_revision == 1
    assert released.state.exit_intent == "RECOVERY_STRENGTH_RELEASE"
    assert released.reason == "deep_recovery_strength_runner"
    request = released.requests[0]
    assert request.intent == "RECOVERY_STRENGTH_RELEASE"
    assert request.exit_policy == "DEEP_RECOVERY_STRENGTH_RUNNER_V6"
    assert request.exit_target_base == expected_target
    assert D("0") <= total_base - expected_target - request.amount < D(".001")
    assert request.price >= basis

    restored = EpisodeState.from_dict(released.state.to_dict())
    assert restored.exit_intent == "RECOVERY_STRENGTH_RELEASE"
    assert restored.target_base == expected_target

    expired = transition(
        cfg,
        restored,
        market(
            timestamp=6480,
            bid=bid,
            ask=bid,
            low=bid,
            base_available=total_base,
            entry_signal=False,
        ),
        [
            OrderUpdate(
                request.id,
                D("0"),
                D("0"),
                done=True,
                terminal_cause="NATURAL_EXPIRY",
            )
        ],
    )
    retry = expired.requests[0]
    assert retry.id != request.id
    assert retry.intent == "RECOVERY_STRENGTH_RELEASE"
    assert retry.exit_policy == "DEEP_RECOVERY_STRENGTH_RUNNER_V6"
    assert retry.exit_target_base == expected_target


def test_deep_recovery_release_excludes_shallow_and_strong_markup():
    cfg = config(deep_recovery_capital_release=True)
    shallow, _ = bought(cfg)
    recovered = transition(
        cfg,
        shallow,
        market(
            timestamp=6120,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=D("1"),
            entry_signal=False,
            recovery=True,
        ),
    )
    shallow_markup = transition(
        cfg,
        recovered.state,
        market(
            timestamp=6180,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            entry_signal=False,
            markup=True,
        ),
    )
    deep, total_base = _deep_recovery_state(cfg)
    basis = deep.cost / deep.base
    strong_price = basis + cfg.distribution_atr_levels[0] * D("1")
    strong_markup = transition(
        cfg,
        deep,
        market(
            timestamp=6420,
            bid=strong_price,
            ask=strong_price,
            low=strong_price,
            base_available=total_base,
            entry_signal=False,
            markup=True,
        ),
    )

    assert shallow_markup.reason == "markup_armed"
    assert not shallow_markup.requests
    assert strong_markup.reason == "markup_armed"
    assert not strong_markup.requests


def test_deep_recovery_release_is_profit_protected_and_retries_only_unfilled_base():
    cfg = config(
        deep_recovery_capital_release=True,
        require_profit=False,
        min_profit=D(".03"),
    )
    state, total_base = _deep_recovery_state(cfg)
    profit_floor = state.cost / state.base * (D("1") + cfg.min_profit)
    ordinary = transition(
        cfg,
        state,
        market(
            timestamp=6420,
            bid=profit_floor - D(".01"),
            ask=profit_floor - D(".01"),
            low=profit_floor - D(".01"),
            base_available=total_base,
            entry_signal=False,
            markup=True,
        ),
    )
    assert ordinary.reason == "markup_armed"
    assert not ordinary.requests

    state, total_base = _deep_recovery_state(cfg)
    authorized = transition(
        cfg,
        state,
        market(
            timestamp=6420,
            bid=profit_floor + D(".01"),
            ask=profit_floor + D(".01"),
            low=profit_floor + D(".01"),
            base_available=total_base,
            entry_signal=False,
            markup=True,
        ),
    )
    pending = transition(
        cfg,
        authorized.state,
        market(
            timestamp=6480,
            bid=profit_floor + D(".02"),
            ask=profit_floor + D(".02"),
            low=profit_floor + D(".02"),
            base_available=total_base,
            entry_signal=False,
        ),
    )
    request = authorized.requests[0]
    partial_amount = request.amount / D("2")
    partial = transition(
        cfg,
        EpisodeState.from_dict(authorized.state.to_dict()),
        market(
            timestamp=6480,
            bid=profit_floor + D(".02"),
            ask=profit_floor + D(".02"),
            low=profit_floor + D(".02"),
            base_available=total_base - partial_amount,
            entry_signal=False,
        ),
        [
            OrderUpdate(
                request.id,
                partial_amount,
                partial_amount * request.price,
                done=True,
            )
        ],
    )

    assert request.price >= profit_floor
    assert pending.reason == "pending_execution"
    assert not pending.requests
    assert partial.requests[0].intent == "RECOVERY_CAPITAL_RELEASE"
    assert partial.requests[0].amount <= request.amount - partial_amount


def test_deep_recovery_release_reconciles_to_ordinary_runner_and_survives_restart():
    cfg = config(deep_recovery_capital_release=True)
    state, total_base = _deep_recovery_state(cfg)
    authorized = transition(
        cfg,
        state,
        market(
            timestamp=6420,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=total_base,
            entry_signal=False,
            markup=True,
        ),
    )
    request = authorized.requests[0]
    reconciled = transition(
        cfg,
        EpisodeState.from_dict(authorized.state.to_dict()),
        market(
            timestamp=6480,
            bid=D("99"),
            ask=D("99"),
            low=D("99"),
            base_available=authorized.state.target_base,
            entry_signal=False,
        ),
        [OrderUpdate(request.id, request.amount, request.amount * request.price, done=True)],
    )

    assert reconciled.reason == "dust"
    assert reconciled.state.phase == "DISTRIBUTE"
    assert reconciled.state.base >= authorized.state.target_base
    assert reconciled.state.base - authorized.state.target_base < D(".001")
    assert reconciled.state.exit_intent == "DISTRIBUTION_SELL"
    assert not reconciled.requests
    assert EpisodeState.from_dict(reconciled.state.to_dict()).to_dict() == reconciled.state.to_dict()


def test_sell_only_assignment_uses_shared_deep_recovery_release_without_buy_history():
    cfg = config(
        mode="sell_only",
        assignment_id="assigned-deep-bag",
        initial_base=D("1"),
        initial_cost=D("100"),
        deep_recovery_capital_release=True,
    )
    release = transition(
        cfg,
        EpisodeState(),
        market(
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            entry_signal=False,
            markup=True,
        ),
    )

    assert release.state.assignment_consumed is True
    assert release.state.phase == "DISTRIBUTE"
    assert release.state.target_base == D(".4")
    assert release.requests[0].amount == D(".6")
    assert release.requests[0].intent == "RECOVERY_CAPITAL_RELEASE"
    assert all(order.request.side == "SELL" for order in release.state.orders.values())


def test_sell_only_assignment_uses_shared_deep_recovery_rotation_without_buying():
    cfg = config(
        mode="sell_only",
        assignment_id="assigned-deep-bag",
        initial_base=D("1"),
        initial_cost=D("100"),
        deep_recovery_capital_release=True,
        deep_recovery_rotation_exit=True,
    )
    rotation = transition(
        cfg,
        EpisodeState(),
        market(
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            entry_signal=True,
            markup=True,
        ),
    )

    assert rotation.state.assignment_consumed is True
    assert rotation.state.phase == "EXIT"
    assert rotation.state.target_base == 0
    assert rotation.requests[0].amount == D("1")
    assert rotation.requests[0].intent == "RECOVERY_ROTATION_EXIT"
    assert all(order.request.side == "SELL" for order in rotation.state.orders.values())


def test_sell_only_assignment_uses_shared_recovery_strength_runner_without_buying():
    cfg = config(
        mode="sell_only",
        assignment_id="assigned-deep-bag",
        initial_base=D("1"),
        initial_cost=D("100"),
        deep_recovery_capital_release=True,
        deep_recovery_strength_runner=True,
    )
    release = transition(
        cfg,
        EpisodeState(),
        market(
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            base_available=D("1"),
            entry_signal=True,
            markup=True,
        ),
    )

    assert release.state.assignment_consumed is True
    assert release.state.phase == "DISTRIBUTE"
    assert release.state.target_base == D(".1")
    assert release.requests[0].amount == D(".9")
    assert release.requests[0].intent == "RECOVERY_STRENGTH_RELEASE"
    assert release.requests[0].exit_policy == "DEEP_RECOVERY_STRENGTH_RUNNER_V6"
    assert all(order.request.side == "SELL" for order in release.state.orders.values())


def _structure_runner_setup(cfg):
    state, _ = bought(cfg)
    armed = transition(
        cfg,
        state,
        market(
            timestamp=6120,
            bid=D("102"),
            ask=D("102"),
            low=D("102"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            markup=True,
        ),
    )
    high = transition(
        cfg,
        armed.state,
        market(
            timestamp=6180,
            bid=D("103"),
            ask=D("103"),
            low=D("103"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
    )
    assert high.state.target_base == D("1")
    assert not high.requests
    return high.state


def test_structure_held_trail_breach_sells_excess_and_keeps_final_runner():
    cfg = config(structure_held_runner=True)
    state = _structure_runner_setup(cfg)

    result = transition(
        cfg,
        state,
        market(
            timestamp=6240,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
    )

    assert result.state.phase == "DISTRIBUTE"
    assert result.state.target_base == D("0.4")
    assert result.state.structure_runner_active is True
    assert result.state.exit_target_revision == 1
    assert result.reason == "structure_runner_distribution"
    assert len(result.requests) == 1
    request = result.requests[0]
    assert request.side == "SELL"
    assert request.intent == "DISTRIBUTION_SELL"
    assert request.amount == D("0.6")
    assert request.exit_policy == "STRUCTURE_HELD_RUNNER_V1"
    assert request.exit_hourly_recovered is True
    assert request.exit_risk_clear is True
    assert request.exit_trail_breach == "PRICE_AND_EQUITY"
    assert request.exit_target_base == D("0.4")
    assert request.exit_target_revision == 1

    restored = EpisodeState.from_dict(result.state.to_dict())
    assert restored.to_dict() == result.state.to_dict()
    assert restored.structure_runner_active is True
    assert restored.last_exit_trail_breach == "PRICE_AND_EQUITY"


def test_structure_runner_exits_only_after_later_unrecovered_trail_breach():
    cfg = config(structure_held_runner=True)
    held = transition(
        cfg,
        _structure_runner_setup(cfg),
        market(
            timestamp=6240,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
    )
    first = held.requests[0]
    filled = OrderUpdate(first.id, filled_base=D("0.6"), filled_quote=D("61.44"), done=True)

    terminal = transition(
        cfg,
        held.state,
        market(
            timestamp=6300,
            bid=D("102.3"),
            ask=D("102.3"),
            low=D("102.3"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("0.4"),
            hourly_recovered=False,
        ),
        [filled],
    )

    assert terminal.state.phase == "EXIT"
    assert terminal.state.target_base == D("0")
    assert terminal.state.structure_runner_active is False
    assert terminal.state.exit_target_revision == 2
    assert terminal.reason == "trailing_profit_exit"
    assert terminal.requests[0].amount == D("0.4")
    assert terminal.requests[0].intent == "TRAILING_PROFIT_EXIT"
    assert terminal.requests[0].exit_hourly_recovered is False
    assert terminal.requests[0].exit_risk_clear is True
    assert terminal.requests[0].exit_target_base == D("0")
    assert terminal.requests[0].exit_target_revision == 2


def test_structure_runner_terminal_target_cancels_pending_excess_without_duplicate_sell():
    cfg = config(structure_held_runner=True)
    held = transition(
        cfg,
        _structure_runner_setup(cfg),
        market(
            timestamp=6240,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
    )

    terminal = transition(
        cfg,
        held.state,
        market(
            timestamp=6300,
            bid=D("102.3"),
            ask=D("102.3"),
            low=D("102.3"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=False,
        ),
    )

    assert terminal.state.phase == "EXIT"
    assert terminal.state.target_base == D("0")
    assert terminal.state.exit_target_revision == 2
    assert terminal.cancel_ids == (held.requests[0].id,)
    assert not terminal.requests


def test_terminal_retry_preserves_context_that_revised_the_target():
    cfg = config(structure_held_runner=True)
    held = transition(
        cfg,
        _structure_runner_setup(cfg),
        market(
            timestamp=6240,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
    )
    pending = held.requests[0]
    terminal = transition(
        cfg,
        held.state,
        market(
            timestamp=6300,
            bid=D("102.3"),
            ask=D("102.3"),
            low=D("102.3"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=False,
        ),
    )
    retried = transition(
        cfg,
        terminal.state,
        market(
            timestamp=6360,
            bid=D("102.2"),
            ask=D("102.2"),
            low=D("102.2"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
        [OrderUpdate(pending.id, filled_base=D("0"), filled_quote=D("0"), done=True)],
    )

    assert retried.requests[0].amount == D("1")
    assert retried.requests[0].exit_hourly_recovered is False
    assert retried.requests[0].exit_risk_clear is True
    assert retried.requests[0].exit_trail_breach == "PRICE_AND_EQUITY"
    assert retried.requests[0].exit_target_revision == 2


@pytest.mark.parametrize("risk_flag", ["cascade", "hostile", "reset"])
def test_structure_runner_never_protects_inventory_during_unsafe_context(risk_flag):
    cfg = config(structure_held_runner=True)
    state = _structure_runner_setup(cfg)
    risky_market = dict(
        timestamp=6240,
        bid=D("103.1"),
        ask=D("103.1"),
        low=D("103.1"),
        atr_15m=D("0.1"),
        atr_1h=D("10"),
        base_available=D("1"),
        hourly_recovered=True,
    )
    risky_market[risk_flag] = True

    result = transition(cfg, state, market(**risky_market))

    assert result.state.phase == "EXIT"
    assert result.state.target_base == D("0")
    assert result.state.structure_runner_active is False
    assert result.requests[0].amount == D("1")
    assert result.requests[0].exit_risk_clear is False
    assert result.requests[0].exit_trail_breach == "RISK_OVERRIDE"


def test_sell_only_uses_same_structure_held_runner_without_buy_authorization():
    cfg = config(
        mode="sell_only",
        assignment_id="inventory-1",
        initial_base=D("1"),
        initial_cost=D("100"),
        structure_held_runner=True,
    )
    armed = transition(
        cfg,
        EpisodeState(),
        market(
            bid=D("102"),
            ask=D("102"),
            low=D("102"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            markup=True,
        ),
    )
    high = transition(
        cfg,
        armed.state,
        market(
            timestamp=6060,
            bid=D("103"),
            ask=D("103"),
            low=D("103"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
    )
    result = transition(
        cfg,
        high.state,
        market(
            timestamp=6120,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
        ),
    )

    assert result.state.target_base == D("0.4")
    assert result.requests[0].side == "SELL"
    assert result.requests[0].amount == D("0.6")
    assert all(order.request.side != "BUY" for order in result.state.orders.values())


def _markup_release_held_runner(cfg, *, sell_only=False):
    if sell_only:
        armed = transition(
            cfg,
            EpisodeState(),
            market(
                bid=D("102"),
                ask=D("102"),
                low=D("102"),
                atr_15m=D("0.1"),
                atr_1h=D("10"),
                base_available=D("1"),
                markup=True,
            ),
        )
        high = transition(
            cfg,
            armed.state,
            market(
                timestamp=6060,
                bid=D("103"),
                ask=D("103"),
                low=D("103"),
                atr_15m=D("0.1"),
                atr_1h=D("10"),
                base_available=D("1"),
                hourly_recovered=True,
                markup=True,
            ),
        )
        state = high.state
        high_timestamp = 6120
    else:
        state = _structure_runner_setup(cfg)
        high_timestamp = 6240
    held = transition(
        cfg,
        state,
        market(
            timestamp=high_timestamp,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
            markup=True,
        ),
    )
    assert held.state.structure_runner_active is True
    assert held.state.target_base == D("0.4")
    return held


def test_markup_loss_at_initial_trail_breach_uses_terminal_target():
    cfg = config(structure_held_runner=True, markup_release_runner=True)
    state = _structure_runner_setup(cfg)

    result = transition(
        cfg,
        state,
        market(
            timestamp=6240,
            bid=D("102.4"),
            ask=D("102.4"),
            low=D("102.4"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
            markup=False,
        ),
    )

    assert result.state.phase == "EXIT"
    assert result.state.target_base == D("0")
    assert result.state.structure_runner_active is False
    assert result.reason == "trailing_profit_exit"
    request = result.requests[0]
    assert request.amount == D("1")
    assert request.exit_policy == "MARKUP_RELEASED_RUNNER_V1"
    assert request.exit_trail_breach == "MARKUP_LOSS"
    assert request.exit_hourly_recovered is True
    assert request.exit_risk_clear is True

    with pytest.raises(EpisodeInputError, match="markup loss"):
        replace(request, exit_policy="STRUCTURE_HELD_RUNNER_V1")


def test_active_runner_latches_zero_target_on_later_markup_loss_without_new_trail_breach():
    cfg = config(structure_held_runner=True, markup_release_runner=True)
    held = _markup_release_held_runner(cfg)
    partial = held.requests[0]
    fill = OrderUpdate(
        partial.id,
        filled_base=D("0.6"),
        filled_quote=D("61.44"),
        done=True,
    )

    result = transition(
        cfg,
        held.state,
        market(
            timestamp=6300,
            bid=D("103.2"),
            ask=D("103.2"),
            low=D("103.2"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("0.4"),
            hourly_recovered=True,
            markup=False,
        ),
        [fill],
    )

    assert result.state.phase == "EXIT"
    assert result.state.target_base == D("0")
    assert result.state.exit_target_revision == 2
    assert result.state.last_exit_trail_breach == "MARKUP_LOSS"
    assert result.requests[0].amount == D("0.4")
    assert result.requests[0].exit_trail_breach == "MARKUP_LOSS"


def test_markup_loss_target_stays_zero_until_profit_guard_allows_full_retry():
    cfg = config(
        structure_held_runner=True,
        markup_release_runner=True,
        min_profit=D("0.02"),
    )
    held = _markup_release_held_runner(cfg)
    partial = held.requests[0]
    fill = OrderUpdate(
        partial.id,
        filled_base=D("0.6"),
        filled_quote=D("61.44"),
        done=True,
    )

    blocked = transition(
        cfg,
        held.state,
        market(
            timestamp=6300,
            bid=D("101"),
            ask=D("101"),
            low=D("101"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("0.4"),
            hourly_recovered=True,
            markup=False,
        ),
        [fill],
    )
    assert blocked.reason == "profit_guard_blocked"
    assert blocked.state.phase == "EXIT"
    assert blocked.state.target_base == D("0")
    assert blocked.state.last_exit_trail_breach == "MARKUP_LOSS"
    assert not blocked.requests

    retried = transition(
        cfg,
        blocked.state,
        market(
            timestamp=6360,
            bid=D("103"),
            ask=D("103"),
            low=D("103"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("0.4"),
            hourly_recovered=True,
            markup=False,
        ),
    )
    assert retried.requests[0].amount == D("0.4")
    assert retried.requests[0].exit_policy == "MARKUP_RELEASED_RUNNER_V1"
    assert retried.requests[0].exit_trail_breach == "MARKUP_LOSS"
    assert retried.requests[0].exit_target_revision == 2


def test_markup_loss_cancels_pending_runner_sell_without_duplicate_quantity():
    cfg = config(structure_held_runner=True, markup_release_runner=True)
    held = _markup_release_held_runner(cfg)

    result = transition(
        cfg,
        held.state,
        market(
            timestamp=6300,
            bid=D("103.2"),
            ask=D("103.2"),
            low=D("103.2"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("1"),
            hourly_recovered=True,
            markup=False,
        ),
    )

    assert result.state.phase == "EXIT"
    assert result.state.target_base == D("0")
    assert result.cancel_ids == (held.requests[0].id,)
    assert not result.requests


def test_sell_only_uses_same_markup_loss_runner_release_without_buying():
    cfg = config(
        mode="sell_only",
        assignment_id="inventory-1",
        initial_base=D("1"),
        initial_cost=D("100"),
        structure_held_runner=True,
        markup_release_runner=True,
    )
    held = _markup_release_held_runner(cfg, sell_only=True)
    partial = held.requests[0]

    result = transition(
        cfg,
        held.state,
        market(
            timestamp=6180,
            bid=D("103.2"),
            ask=D("103.2"),
            low=D("103.2"),
            atr_15m=D("0.1"),
            atr_1h=D("10"),
            base_available=D("0.4"),
            hourly_recovered=True,
            markup=False,
            entry_signal=True,
        ),
        [
            OrderUpdate(
                partial.id,
                filled_base=D("0.6"),
                filled_quote=D("61.44"),
                done=True,
            )
        ],
    )

    assert result.requests[0].side == "SELL"
    assert result.requests[0].amount == D("0.4")
    assert result.requests[0].exit_trail_breach == "MARKUP_LOSS"
    assert all(order.request.side != "BUY" for order in result.state.orders.values())


def test_sell_only_assignment_never_creates_buy_or_adopts_deposit():
    cfg = config(mode="sell_only", assignment_id="inventory-1", initial_base=D("1"), initial_cost=D("100"))
    first = transition(cfg, EpisodeState(), market(base_available=D("5")))
    assert first.state.base == D("1")
    assert first.state.phase == "RECOVER"
    assert not first.requests
    another = transition(cfg, first.state, market(timestamp=6060, base_available=D("10"), bid=D("75"), ask=D("75")))
    assert another.state.base == D("1")
    assert not another.requests


def test_data_failure_cancels_pending_without_releasing_reservation():
    cfg = config()
    first = transition(cfg, EpisodeState(), market())
    result = transition(cfg, first.state, market(timestamp=6060, valid=False))
    assert result.cancel_ids == (first.requests[0].id,)
    assert remaining_budget(cfg, result.state) == D("900")
    assert not result.requests


def test_operator_veto_blocks_all_levels_without_consuming_authorization():
    cfg = config()
    first = transition(cfg, EpisodeState(), market(buy_allowed=False))
    assert not first.requests
    assert remaining_budget(cfg, first.state) == D("1000")
    state, _ = bought(cfg)
    veto = transition(
        cfg,
        state,
        market(timestamp=6120, bid=D("96"), ask=D("96"), low=D("96"), base_available=D("1"), buy_allowed=False),
    )
    assert not veto.requests
    assert veto.reason == "buy_veto"


def test_sell_only_basis_larger_than_buy_budget_does_not_block_exit():
    cfg = config(mode="sell_only", assignment_id="owned", initial_base=D("20"), initial_cost=D("2000"))
    first = transition(cfg, EpisodeState(), market(base_available=D("20"), markup=True, bid=D("102"), ask=D("102")))
    follow = transition(cfg, first.state, market(timestamp=6060, base_available=D("20"), bid=D("112"), ask=D("112")))
    assert follow.requests[0].side == "SELL"
    assert follow.state.fault is None


def test_pending_sell_does_not_hide_new_high_or_terminal_breach():
    cfg = config()
    state, _ = bought(cfg)
    arm = transition(cfg, state, market(timestamp=6120, base_available=D("1"), markup=True, bid=D("102"), ask=D("102")))
    first = transition(cfg, arm.state, market(timestamp=6180, base_available=D("1"), bid=D("108"), ask=D("108")))
    high = transition(cfg, first.state, market(timestamp=6240, base_available=D("0.6"), bid=D("115"), ask=D("115")))
    assert high.state.peak == D("115")
    assert high.state.target_base == D("0.4")
    assert not high.requests
    crash = transition(cfg, high.state, market(timestamp=6300, base_available=D("0.6"), bid=D("103"), ask=D("103")))
    assert crash.state.phase == "EXIT"
    assert crash.state.target_base == D("0")
    assert crash.cancel_ids == (first.requests[0].id,)
    assert not crash.requests


def test_quality_attenuation_zero_cannot_create_minimum_order():
    result = transition(config(), EpisodeState(), market(quality_factor=D("0")))
    assert not result.requests
    assert result.reason == "zero_allocation"
    with pytest.raises(EpisodeInputError):
        market(quality_factor=D("1.01"))


def test_price_bound_violations_record_actual_spend_and_block_new_orders():
    cfg = config()
    first = transition(cfg, EpisodeState(), market())
    order = first.requests[0]
    overrun = OrderUpdate(order.id, filled_base=D("1"), filled_quote=D("102"), done=True)
    result = transition(cfg, first.state, market(timestamp=6060, base_available=D("1")), [overrun])
    assert result.state.gross_spent == D("102")
    assert result.reason == "buy_reservation_overrun"
    assert not result.requests


def test_confirmation_gap_restarts_stabilization_instead_of_counting_missing_bar():
    cfg = config()
    state, _ = bought(cfg)
    snap = market(timestamp=6120, base_available=D("1"), bid=D("96.8"), ask=D("96.8"), low=D("96"))
    armed = transition(cfg, state, snap)
    once = transition(cfg, armed.state, replace(snap, timestamp=6180, low=D("96.1")))
    gap = transition(cfg, once.state, replace(snap, timestamp=6300, low=D("96.1")))
    assert not gap.requests
    assert gap.state.no_new_low == 0


def test_invalid_data_does_not_preserve_almost_confirmed_setup():
    cfg = config()
    state, _ = bought(cfg)
    snap = market(timestamp=6120, base_available=D("1"), bid=D("96.8"), ask=D("96.8"), low=D("96"))
    armed = transition(cfg, state, snap)
    once = transition(cfg, armed.state, replace(snap, timestamp=6180, low=D("96.1")))
    invalid = transition(cfg, once.state, replace(snap, timestamp=6240, valid=False))
    resumed = transition(cfg, invalid.state, replace(snap, timestamp=6300, low=D("96.1")))
    assert not resumed.requests
    assert resumed.state.no_new_low == 0


def test_episode_can_only_restart_after_later_higher_timeframe_reset():
    cfg = config()
    first = transition(cfg, EpisodeState(), market())
    closed = transition(
        cfg, first.state, market(timestamp=6060), [OrderUpdate(first.requests[0].id, D("0"), D("0"), done=True)]
    )
    assert closed.state.phase == "COOLDOWN"
    same_hour = transition(cfg, closed.state, market(timestamp=6120, reset=True))
    assert same_hour.state.phase == "COOLDOWN"
    later = transition(cfg, same_hour.state, market(timestamp=7260, htf_timestamp=7200, reset=True))
    assert later.state.phase == "WATCH"
    assert not later.requests


def test_late_fees_consume_cash_without_changing_already_filled_quantity():
    cfg = config(fee_rate=D(".01"))
    first = transition(cfg, EpisodeState(), market())
    req = first.requests[0]
    fill = OrderUpdate(req.id, D(".5"), D("50"), done=True)
    done = transition(cfg, first.state, market(timestamp=6060, base_available=D(".5"), entry_signal=False), [fill])
    fees = transition(
        cfg,
        done.state,
        market(timestamp=6120, base_available=D(".5"), entry_signal=False),
        [replace(fill, fee_quote=D(".5"))],
    )
    assert fees.state.base == D(".5")
    assert fees.state.gross_spent == D("50.5")
    assert fees.state.cost == D("50.5")


def test_terminal_partial_sell_reissues_only_unfilled_target_after_ack():
    cfg = config(mode="sell_only", assignment_id="owned", initial_base=D("1"), initial_cost=D("100"))
    arm = transition(cfg, EpisodeState(), market(base_available=D("1"), markup=True, bid=D("102"), ask=D("102")))
    high = transition(cfg, arm.state, market(timestamp=6060, base_available=D("1"), bid=D("112"), ask=D("112")))
    completed = transition(
        cfg,
        high.state,
        market(timestamp=6120, base_available=D(".8"), bid=D("105"), ask=D("105")),
        [OrderUpdate(high.requests[0].id, D(".2"), D("22.4"), done=True)],
    )
    assert completed.state.phase == "EXIT"
    assert completed.requests[0].amount == D(".8")
    assert completed.requests[0].id != high.requests[0].id


def test_cooldown_reset_cannot_erase_a_late_fill():
    cfg = config()
    first = transition(cfg, EpisodeState(), market())
    req = first.requests[0]
    cancelled = transition(cfg, first.state, market(timestamp=6060), [OrderUpdate(req.id, D("0"), D("0"), done=True)])
    late = transition(
        cfg,
        cancelled.state,
        market(timestamp=7260, htf_timestamp=7200, reset=True, base_available=D("1")),
        [OrderUpdate(req.id, D("1"), D("100"), done=True)],
    )
    assert late.state.base == D("1")
    assert late.state.gross_spent == D("100")
    assert late.state.phase == "EXIT"
    assert not late.requests


def test_old_receipt_identity_survives_episode_reset_and_fails_on_new_fill():
    cfg = config()
    first = transition(cfg, EpisodeState(), market())
    req = first.requests[0]
    cancelled = transition(cfg, first.state, market(timestamp=6060), [OrderUpdate(req.id, D("0"), D("0"), done=True)])
    reset = transition(cfg, cancelled.state, market(timestamp=7260, htf_timestamp=7200, reset=True))
    assert req.id in reset.state.orders
    with pytest.raises(EpisodeInputError, match="closed episode"):
        transition(
            cfg,
            reset.state,
            market(timestamp=7320, base_available=D("1")),
            [OrderUpdate(req.id, D("1"), D("100"), done=True)],
        )


@pytest.mark.parametrize("field,value", [("done", "false"), ("filled_base", "-1"), ("seen", 1)])
def test_corrupt_checkpoint_cannot_release_pending_reservation(field, value):
    state = transition(config(), EpisodeState(), market()).state.to_dict()
    order = next(iter(state["orders"].values()))
    order[field] = value
    with pytest.raises(EpisodeInputError):
        EpisodeState.from_dict(state)


def test_legacy_checkpoint_defaults_recovery_confirmation_clock_to_zero():
    payload = EpisodeState().to_dict()
    payload.pop("setup_confirmation_start")

    restored = EpisodeState.from_dict(payload)

    assert restored.setup_confirmation_start == 0


@pytest.mark.parametrize(
    "change", [{"fee_rate": D("NaN")}, {"budget_quote": D("-1")}, {"level_fractions": (D("1"), D("1"))}]
)
def test_invalid_config_is_rejected(change):
    with pytest.raises((EpisodeInputError, ValueError)):
        config(**change)


@pytest.mark.parametrize("field", ["base", "cost", "gross_spent", "sell_proceeds"])
def test_restored_accounting_must_match_attributed_receipts(field):
    cfg = config()
    state, _ = bought(cfg)
    payload = state.to_dict()
    payload[field] = "0" if field != "sell_proceeds" else "10"
    restored = EpisodeState.from_dict(payload)
    with pytest.raises(EpisodeInputError, match="accounting"):
        transition(cfg, restored, market(timestamp=6120, base_available=D("1"), markup=True))


def test_late_fee_restatement_does_not_trigger_equity_price_reversal():
    cfg = config(fee_rate=D(".01"))
    first = transition(cfg, EpisodeState(), market())
    request = first.requests[0]
    receipt = OrderUpdate(request.id, D(".5"), D("50"), done=True)
    snap = market(
        timestamp=6060, bid=D("103"), ask=D("103"), low=D("103"), atr_15m=D(".1"), base_available=D(".5"), markup=True
    )
    markup = transition(cfg, first.state, snap, [receipt])
    unchanged = transition(cfg, markup.state, replace(snap, timestamp=6120))
    corrected = transition(cfg, unchanged.state, replace(snap, timestamp=6180), [replace(receipt, fee_quote=D(".5"))])
    assert corrected.state.phase == "MARKUP"
    assert not corrected.requests
    assert corrected.state.cost == D("50.5")
    assert corrected.state.equity_peak == unchanged.state.equity_peak - D(".5")
    reversal = transition(cfg, corrected.state, replace(snap, timestamp=6240, bid=D("102"), ask=D("102")))
    assert reversal.state.phase == "EXIT"


def test_late_buy_fee_after_partial_sale_is_allocated_to_remaining_inventory():
    cfg = config(fee_rate=D(".01"))
    first = transition(cfg, EpisodeState(), market())
    receipt = OrderUpdate(first.requests[0].id, D(".5"), D("50"), done=True)
    snap = market(timestamp=6060, bid=D("108"), ask=D("108"), low=D("108"), base_available=D(".5"), markup=True)
    markup = transition(cfg, first.state, snap, [receipt])
    sale = transition(cfg, markup.state, replace(snap, timestamp=6120))
    sold = OrderUpdate(sale.requests[0].id, D(".2"), D("21.6"), done=True)
    filled = transition(cfg, sale.state, replace(snap, timestamp=6180, base_available=D(".3")), [sold])
    corrected = transition(
        cfg, filled.state, replace(snap, timestamp=6240, base_available=D(".3")), [replace(receipt, fee_quote=D(".5"))]
    )
    assert corrected.state.base == D(".3")
    assert corrected.state.cost == D("30.3")
    assert corrected.state.gross_spent == D("50.5")


def test_base_fee_restatement_during_invalid_book_uses_last_valid_valuation():
    cfg = config(fee_rate=D(".01"))
    first = transition(cfg, EpisodeState(), market())
    receipt = OrderUpdate(first.requests[0].id, D(".5"), D("50"), done=True)
    snap = market(
        timestamp=6060, bid=D("103"), ask=D("103"), low=D("103"), atr_15m=D(".1"), base_available=D(".5"), markup=True
    )
    markup = transition(cfg, first.state, snap, [receipt])
    stable = transition(cfg, markup.state, replace(snap, timestamp=6120))
    invalid = transition(
        cfg,
        stable.state,
        replace(snap, timestamp=6180, valid=False, bid=D("0"), ask=D("0")),
        [replace(receipt, fee_base=D(".005"))],
    )
    recovered = transition(cfg, invalid.state, replace(snap, timestamp=6240, base_available=D(".495")))
    assert recovered.state.phase == "MARKUP"
    assert not recovered.requests
    assert recovered.state.base == D(".495")
