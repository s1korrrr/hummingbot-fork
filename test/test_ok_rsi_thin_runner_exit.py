from dataclasses import replace
from test.test_ok_rsi_episode import D, _structure_runner_setup, config, market

import pytest

from controllers.directional_trading.ok_rsi_core.episode import EpisodeState, transition


@pytest.mark.parametrize("policy", ["stage", "confirmed", "skim"])
def test_thin_release_after_unreduced_protected_runner_clears_active_flag(policy):
    cfg = config(
        thin_scout_release=True,
        structure_held_runner=True,
        markup_release_runner=True,
        bounded_markup_runner=True,
        stage_preserving_runner=True,
        next_stage_confirmation=policy != "stage",
        earned_stage_risk_skim=policy == "skim",
    )
    state = _structure_runner_setup(cfg)
    state.distribution_atr = D("1")
    context = market(
        timestamp=6240,
        bid=D("102.4"),
        ask=D("102.4"),
        low=D("102.4"),
        atr_15m=D(".1"),
        atr_1h=D("10"),
        base_available=D("1"),
        hourly_recovered=True,
        markup=True,
    )
    protected = transition(cfg, state, context)
    assert protected.state.phase == "DISTRIBUTE"
    assert (
        protected.state.target_base == protected.state.distribution_start_base == D("1")
    )
    assert protected.state.exit_target_revision == 0
    assert protected.state.structure_runner_active
    assert not protected.requests
    restored = EpisodeState.from_dict(protected.state.to_dict())
    released = transition(
        cfg,
        restored,
        replace(
            context, timestamp=6300, bid=D("104.1"), ask=D("104.1"), low=D("104.1")
        ),
    )
    assert released.state.phase == "EXIT"
    assert released.state.target_base == D("0")
    assert released.state.exit_target_revision == 1
    assert released.requests[0].intent == "THIN_SCOUT_RELEASE"
    assert released.requests[0].amount == D("1")
    assert released.state.structure_runner_active is False
