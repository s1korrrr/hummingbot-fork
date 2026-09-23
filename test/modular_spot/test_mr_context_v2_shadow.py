from test_mr_context_v2_native import buy
from test_mr_context_v2_policy import packet

from controllers.generic.mr_context_v2_core.native_adapter import apply_to_actions
from controllers.generic.mr_context_v2_core.policy import project_shadow
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction


def test_shadow_native_actions_match_off_baseline():
    off = packet(mode='off')
    shadow = packet(mode='shadow')
    created = buy('1')
    stop = StopExecutorAction(controller_id='modular_spot_v2_btc', executor_id='held', keep_position=True)
    off_actions, off_assessment = apply_to_actions(
        (created, stop), recipe=off['recipe'], binding=off['binding'],
        source_sha256=off['source_sha256'], opportunity=off['opportunity'],
        proposal=off['proposal'], capacity=off['capacity'], snapshot=off['snapshot'],
        now_ms=off['now_ms'])
    shadow_actions, shadow_assessment = apply_to_actions(
        (created, stop), recipe=shadow['recipe'], binding=shadow['binding'],
        source_sha256=shadow['source_sha256'], opportunity=shadow['opportunity'],
        proposal=shadow['proposal'], capacity=shadow['capacity'], snapshot=shadow['snapshot'],
        now_ms=shadow['now_ms'])
    assert off_assessment is None
    assert shadow_assessment.mode == 'shadow'
    assert shadow_assessment.execution_authorized is False
    assert off_actions == shadow_actions == (created, stop)
    assert created.executor_config.side == TradeType.BUY


def test_shadow_cursor_is_not_the_baseline_cursor():
    values = packet(mode='shadow')
    created = buy('1')
    actions, assessment = apply_to_actions(
        (created,), recipe=values['recipe'], binding=values['binding'],
        source_sha256=values['source_sha256'], opportunity=values['opportunity'],
        proposal=values['proposal'], capacity=values['capacity'], snapshot=values['snapshot'],
        now_ms=values['now_ms'])
    cursor, trace = project_shadow(None, values['opportunity'], assessment, prerequisites=())
    _, second = project_shadow(cursor, values['opportunity'], assessment, prerequisites=())
    assert actions == (created,)
    assert trace.execution_authorized is False
    assert second.reason == 'already_observed'
    assert cursor is not None
    assert second.execution_authorized is False
