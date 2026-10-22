from decimal import Decimal

import pytest
from test_mr_context_v2_policy import packet

from controllers.generic.mr_context_v2_core.native_adapter import apply_to_actions
from controllers.generic.mr_context_v2_core.policy import evaluate_context
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction, StoreExecutorAction


def buy(amount='1'):
    config = PositionExecutorConfig(
        timestamp=1, trading_pair='BTC-USDC', connector_name='okx', side=TradeType.BUY,
        amount=Decimal(amount), entry_price=Decimal('100'))
    return CreateExecutorAction(controller_id='modular_spot_v2_btc', executor_config=config)


def sell(amount='1'):
    config = PositionExecutorConfig(
        timestamp=1, trading_pair='BTC-USDC', connector_name='okx', side=TradeType.SELL,
        amount=Decimal(amount), entry_price=Decimal('100'))
    return CreateExecutorAction(controller_id='modular_spot_v2_btc', executor_config=config)


@pytest.mark.parametrize('actions', [
    (buy(),),
    (StopExecutorAction(controller_id='modular_spot_v2_btc', executor_id='held'), sell()),
    (),
])
def test_restrict_post_reservation_adapter_fails_closed(actions):
    values = packet()
    with pytest.raises(ValueError, match='pre.reservation'):
        apply_to_actions(actions, **values)


def test_shadow_cannot_enter_admission_and_preserves_actions():
    values = packet(mode='shadow')
    created = buy('1')
    stop = StopExecutorAction(controller_id='modular_spot_v2_btc', executor_id='held')
    actions, assessment = apply_to_actions((created, stop), **values)
    assert assessment.mode == 'shadow'
    assert assessment.execution_authorized is False
    assert actions == (created, stop)


def test_off_mode_is_a_no_op():
    values = packet(mode='off')
    created = buy('1')
    actions, assessment = apply_to_actions((created,), **values)
    assert assessment is None
    assert actions == (created,)


def test_native_policy_recipe_rejects_context_family():
    from pydantic import ValidationError

    from controllers.generic.modular_spot_core.native_recipe import NativePolicyRecipe
    try:
        NativePolicyRecipe.model_validate({
            'name': 'mr_v2_context_r1', 'family': 'mr_v2_context_r1',
            'controller_id': 'modular_spot_btc',
        })
    except ValidationError:
        return
    raise AssertionError('compiler family must reject mr_v2 context')


def test_context_package_is_not_a_loadable_controller():
    try:
        from hummingbot.exceptions import InvalidController
        from hummingbot.strategy_v2.controllers.controller_loader import load_controller_config
    except ModuleNotFoundError:
        pytest.skip('compiled hummingbot extensions unavailable')
    try:
        load_controller_config({'controller_type': 'generic', 'controller_name': 'mr_context_v2_core'})
    except (InvalidController, AttributeError, ValueError):
        return
    raise AssertionError('mr_context_v2_core must not resolve as a controller')


def test_unsupported_action_fails_visibly():
    values = packet(mode='shadow')
    store = StoreExecutorAction(controller_id='modular_spot_v2_btc', executor_id='x')
    with pytest.raises(ValueError, match='unsupported native action'):
        apply_to_actions((store,), **values)


def test_harness_does_not_touch_live_connectors_or_credentials(monkeypatch):
    import os
    from pathlib import Path
    monkeypatch.delenv('OKX_API_KEY', raising=False)
    monkeypatch.delenv('OKX_SECRET_KEY', raising=False)
    source = Path(__file__).resolve().parents[2] / 'controllers/generic/mr_context_v2_core/native_adapter.py'
    text = source.read_text()
    assert 'place_order' not in text
    assert 'okx_api' not in text.lower()
    assert os.environ.get('OKX_API_KEY') in (None, '')


def test_compiler_rejects_unregistered_context_family():
    from pydantic import ValidationError

    from controllers.generic.modular_spot_core.recipe_compiler import parse_recipe
    try:
        parse_recipe({'name': 'mr_v2_context_r1', 'family': 'mr_v2_context_r1',
                      'controller_id': 'modular_spot_btc'})
    except (ValidationError, ValueError, ModuleNotFoundError):
        return
    raise AssertionError('parse_recipe must reject unregistered context family')


def test_restrict_plan_veto_does_not_cas_the_new_claim():
    from decimal import Decimal as D

    from controllers.generic.modular_spot_core.planner import EntryProposal
    from controllers.generic.modular_spot_core.state import DecisionState
    from controllers.generic.mr_context_v2_core.native_adapter import apply_restrict_to_plan

    previous = DecisionState(fingerprint='a' * 64, candle=1000)
    proposal = EntryProposal('claim-1', D('100'), D('1'), 1200.0, 0, 0)
    revised = previous.model_copy(update={'candle': 1140, 'claims': ('claim-1',),
                                          'committed_quote': D('100.1'), 'last_entry_time': D('1200')})
    values = packet()
    values = packet(snapshot_changes={'evidence_kind': 'reconstructed'})
    assessment = evaluate_context(**values)
    held, out, reason = apply_restrict_to_plan(
        previous=previous, revised=revised, proposal=proposal, reason='entry_proposed',
        assessment=assessment, mode='restrict', fee_fraction=D('0.001'))
    assert out is None and reason == 'acquisition_disabled'
    assert held.claims == () and held.committed_quote == 0
    assert held.candle == 1140


def test_restrict_plan_rewrites_commitment_before_cas():
    from decimal import Decimal as D

    from controllers.generic.modular_spot_core.planner import EntryProposal
    from controllers.generic.modular_spot_core.state import DecisionState
    from controllers.generic.mr_context_v2_core.native_adapter import apply_restrict_to_plan

    previous = DecisionState(fingerprint='a' * 64, candle=1000)
    proposal = EntryProposal('claim-1', D('100'), D('1'), 1200.0, 0, 0)
    revised = previous.model_copy(update={'candle': 1140, 'claims': ('claim-1',),
                                          'committed_quote': D('100.1'), 'last_entry_time': D('1200')})
    values = packet()
    assessment = evaluate_context(**values)
    held, out, reason = apply_restrict_to_plan(
        previous=previous, revised=revised, proposal=proposal, reason='entry_proposed',
        assessment=assessment, mode='restrict', fee_fraction=D('0.001'))
    assert reason == 'entry_proposed'
    assert out.amount == assessment.quantity < D('1')
    assert out.identifier == proposal.identifier
    assert held.claims == ('claim-1',)
    assert held.committed_quote == assessment.quantity * D('100') * D('1.001')


def test_restrict_plan_rejects_unbound_reservation_or_fee_fraction():
    from decimal import Decimal as D

    from controllers.generic.modular_spot_core.planner import EntryProposal
    from controllers.generic.modular_spot_core.state import DecisionState
    from controllers.generic.mr_context_v2_core.native_adapter import apply_restrict_to_plan

    previous = DecisionState(fingerprint='a' * 64, candle=1000)
    proposal = EntryProposal('claim-1', D('100'), D('1'), 1200.0, 0, 0)
    revised = previous.model_copy(update={'candle': 1140, 'claims': ('claim-1',),
                                          'committed_quote': D('100.2'), 'last_entry_time': D('1200')})
    values = packet()
    assessment = evaluate_context(**values)
    with pytest.raises(ValueError, match='reservation'):
        apply_restrict_to_plan(previous=previous, revised=revised, proposal=proposal,
                               reason='entry_proposed', assessment=assessment, mode='restrict',
                               fee_fraction=D('0.001'))
    matching = revised.model_copy(update={'committed_quote': D('100.1')})
    with pytest.raises(ValueError, match='fee'):
        apply_restrict_to_plan(previous=previous, revised=matching, proposal=proposal,
                               reason='entry_proposed', assessment=assessment, mode='restrict',
                               fee_fraction=D('NaN'))


def test_modular_spot_host_round_trip_stays_disabled_without_inventory():
    from pydantic import ValidationError

    from controllers.generic.modular_spot import ModularSpotControllerConfig
    from hummingbot.strategy_v2.controllers.controller_loader import load_controller_config

    payload = {'controller_type': 'generic', 'controller_name': 'modular_spot',
               'id': 'modular_spot_btc', 'connector_name': 'okx', 'trading_pair': 'BTC-USDC'}
    config = load_controller_config(payload)
    assert config.controller_name == 'modular_spot'
    assert config.execution_enabled is False
    assert config.initial_positions == []
    restored = ModularSpotControllerConfig.model_validate(config.model_dump(mode='json'))
    assert restored.id == config.id
    try:
        ModularSpotControllerConfig.model_validate({**payload, 'context_mode': 'restrict'})
        raise AssertionError('host extra fields must fail visibly')
    except (ValidationError, ValueError):
        pass
    try:
        ModularSpotControllerConfig.model_validate({
            **payload, 'initial_positions': [{'connector_name': 'okx', 'trading_pair': 'BTC-USDC',
                                              'amount': '1', 'side': 'BUY'}],
        })
    except (ValidationError, ValueError):
        return
    raise AssertionError('inventory adoption must fail')
