from test_native_composition import FAMILIES, controllers


def owner(**kwargs):
    return controllers(*FAMILIES[0], **kwargs)[0]


def test_funding_rejection_is_visible_in_compact_and_native_status():
    controller = owner(execution_enabled=True)
    controller._funding_hold = 'Funding checkpoint identity/schema changed'
    assert controller.get_custom_info()['entry_admission']['state'] == 'blocked'
    assert controller.get_custom_info()['buy_gate'].startswith('funding:')
    assert controller.get_status_summary()['gate'] == 'funding: Funding checkpoint identity/schema changed'


def test_unchecked_funding_never_reports_entry_ready():
    controller = owner(execution_enabled=True)
    assert controller.get_status_summary()['gate'] == 'funding_unverified'
    assert controller.get_custom_info()['entry_admission']['state'] == 'unverified'


def test_full_entry_plan_does_not_advertise_disabled_scout_runner():
    controller = owner(execution_enabled=True, use_split_entries=True, buy_confirmation_mode='none')
    result = controller._compute_plan_fields()
    assert 'scout' not in result['plan_anchor']
    assert 'runner' not in result['plan_next']
    assert 'full' in result['plan_anchor']


def test_recovery_hold_remains_distinct_from_funding_hold():
    controller = owner(execution_enabled=True)
    controller._recovery_hold = 'native held inventory report and recorder have not reconciled'
    assert controller.get_status_summary()['gate'].startswith('recovery:')


def test_funding_sizing_database_failure_holds_new_buy():
    from types import SimpleNamespace
    from unittest.mock import Mock

    from sqlalchemy.exc import OperationalError

    from hummingbot.core.data_type.common import TradeType
    controller = owner(execution_enabled=True)
    controller.config.funding = controller.config.funding.model_copy(update={'resize_to_available_quote': True})
    controller._release_entry_decision = Mock()
    controller._funding_store = Mock()
    controller._funding_store.entry_notional_ceiling.side_effect = OperationalError('read budget', {}, Exception('locked'))
    assert controller._effective_buy_entry_usd() == 0
    assert 'locked' in controller._funding_hold
    assert controller.validate_executor_config(SimpleNamespace(side=TradeType.BUY)) is False
    assert controller.get_status_summary()['gate'].startswith('funding:')
