from decimal import Decimal
from unittest.mock import Mock

import pytest
from test_native_composition import FAMILIES, controllers


@pytest.fixture(autouse=True)
def isolated_controller_status(monkeypatch):
    # Pure status fixtures must not inherit another test's disposed recorder.
    from hummingbot.connector.markets_recorder import MarketsRecorder
    monkeypatch.setattr(MarketsRecorder, '_shared_instance', None)


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


def test_funded_buy_never_exceeds_configured_per_entry_quote():
    controller = owner(execution_enabled=True, usd_per_entry=Decimal('100'), dynamic_position_sizing=True,
                       max_position_size_multiplier=1.5)
    controller.config.funding = controller.config.funding.model_copy(update={'resize_to_available_quote': True})
    controller._funding_store = Mock()
    controller._funding_store.entry_notional_ceiling.return_value = Decimal('500')
    state = {'size_multiplier': 1.5, 'buy_entry_role': 'full'}
    assert controller._effective_buy_entry_usd(state) == Decimal('100')


def test_funded_executor_quantization_cannot_exceed_entry_cap():
    from decimal import ROUND_UP

    from hummingbot.connector.trading_rule import TradingRule
    from hummingbot.core.data_type.common import TradeType

    controller = owner(execution_enabled=True, usd_per_entry=Decimal('100'))
    controller.config.funding = controller.config.funding.model_copy(update={'resize_to_available_quote': True})
    controller._funding_store = Mock()
    controller._funding_store.entry_notional_ceiling.return_value = Decimal('500')
    controller._get_trading_rule = Mock(return_value=TradingRule(
        controller.config.trading_pair, min_order_size=Decimal('.01'),
        min_base_amount_increment=Decimal('.01'), min_notional_size=Decimal('10')))
    controller._quantize_amount = Mock(side_effect=lambda amount: amount.quantize(Decimal('.01'), rounding=ROUND_UP))
    config = controller.get_executor_config(TradeType.BUY, Decimal('90'), Decimal('1'))
    assert config.amount * config.entry_price <= Decimal('100')
    assert config.amount * config.entry_price >= Decimal('99')
    assert controller._funding_hold is None


def test_funded_entry_above_venue_minimum_is_held_when_minimum_exceeds_cap():
    from hummingbot.connector.trading_rule import TradingRule
    from hummingbot.core.data_type.common import TradeType

    controller = owner(execution_enabled=True, usd_per_entry=Decimal('100'))
    controller.config.funding = controller.config.funding.model_copy(update={'resize_to_available_quote': True})
    controller._funding_store = Mock()
    controller._funding_store.entry_notional_ceiling.return_value = Decimal('500')
    controller._release_entry_decision = Mock()
    controller._get_trading_rule = Mock(return_value=TradingRule(
        controller.config.trading_pair, min_order_size=Decimal('.01'),
        min_base_amount_increment=Decimal('.01'), min_notional_size=Decimal('120')))
    config = controller.get_executor_config(TradeType.BUY, Decimal('90'), Decimal('1'))
    assert 'per-entry' in controller._funding_hold
    assert controller.validate_executor_config(config) is False
    controller._release_entry_decision.assert_called_once()


def test_buy_refused_before_the_validator_releases_the_reserved_entry_decision():
    """The engine can drop a queued BUY (risk refusal, resume hold, create failure) before this
    owner's validator runs; the reserved ok_rsi entry decision must not stay held until restart."""
    from types import SimpleNamespace

    from hummingbot.core.data_type.common import TradeType
    controller = owner(execution_enabled=True)
    controller._release_entry_decision = Mock()
    controller.create_action_refused(SimpleNamespace(executor_config=SimpleNamespace(side=TradeType.SELL)))
    controller._release_entry_decision.assert_not_called()
    controller.create_action_refused(SimpleNamespace(executor_config=SimpleNamespace(side=TradeType.BUY)))
    controller._release_entry_decision.assert_called_once_with()


def test_frozen_v1_controller_does_not_adopt_the_refused_create_hook():
    from controllers.directional_trading.ok_rsi import OKRSIController
    assert not hasattr(OKRSIController, 'create_action_refused')
