from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction

from .test_native_admission import host_controller


def test_host_is_not_an_entry_screener_and_emits_no_stop_actions():
    controller = host_controller()
    assert controller.determine_executor_actions() == []
    assert controller.validate_executor_config(object()) is False
    assert not any(isinstance(item, StopExecutorAction) for item in controller.determine_executor_actions())
