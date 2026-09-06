from decimal import Decimal
import pytest

from controllers.directional_trading.rsi_v5_core.spot_decision_quality import DeferredScaleIn, OneShotHarvestState, apply_x1_fill, evaluate_d1, evaluate_x1

def test_d1_requires_fresh_reference_and_non_declining_close():
    d=DeferredScaleIn("p",Decimal("1"),"e1")
    assert evaluate_d1(deferred=d,fresh_reference_quantity=Decimal("1"),fresh_reference_intent=False,previous_close=Decimal("100"),latest_close=Decimal("101")) is None
    assert evaluate_d1(deferred=d,fresh_reference_quantity=Decimal("1"),fresh_reference_intent=True,previous_close=Decimal("100"),latest_close=Decimal("99")) is None
    proposal=evaluate_d1(deferred=d,fresh_reference_quantity=Decimal("0.4"),fresh_reference_intent=True,previous_close=Decimal("100"),latest_close=Decimal("101"))
    assert proposal.quantity==Decimal("0.4") and proposal.side=="BUY"

def test_x1_is_one_shot_quantized_and_restart_safe():
    state=OneShotHarvestState("p")
    proposal=evaluate_x1(state=state,eligible_unreserved_inventory=Decimal("2.03"),activation_reached=True,net_profitable=True,lot_size=Decimal("0.01"))
    assert proposal.quantity==Decimal("0.50")
    persisted=apply_x1_fill(state,Decimal("0.20"))
    assert persisted.consumed and persisted.filled_quantity==Decimal("0.20")
    assert evaluate_x1(state=persisted,eligible_unreserved_inventory=Decimal("1.83"),activation_reached=True,net_profitable=True,lot_size=Decimal("0.01")) is None

def test_x1_zero_fill_does_not_consume_marker():
    state=apply_x1_fill(OneShotHarvestState("p"),Decimal("0"))
    assert not state.consumed

def test_non_finite_inputs_fail_closed():
    with pytest.raises(ValueError):
        evaluate_x1(state=OneShotHarvestState("p"),eligible_unreserved_inventory=Decimal("NaN"),activation_reached=True,net_profitable=True,lot_size=Decimal("0.01"))
