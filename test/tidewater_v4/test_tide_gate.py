"""Tidewater tide gate: occupancy-paced spacing between buys of one pair."""
from decimal import Decimal as D

import pytest

from controllers.generic.tidewater_v4_core.tide_gate import TideGateConfig, multiplier, occupancy, tide_gate

GATE = TideGateConfig(low_occupancy='0.30', high_occupancy='0.90', max_multiplier='4', upward_max_occupancy='0.50')


def test_multiplier_is_one_below_low_occupancy_and_capped_above_high():
    assert multiplier(GATE, D('0')) == 1
    assert multiplier(GATE, D('0.30')) == 1
    assert multiplier(GATE, D('0.60')) == D('2.5')  # halfway between 0.30 and 0.90 -> 1 + 0.5 * 3
    assert multiplier(GATE, D('0.90')) == 4
    assert multiplier(GATE, D('1.20')) == 4


def test_first_tranche_of_a_pair_is_never_gated():
    decision = tide_gate(GATE, gap=D('0.008'), occupancy=D('0.95'), reference=None, bid=D('100'))
    assert decision.allowed and decision.reason == 'tide_gate_first_tranche'


def test_downward_add_needs_a_deeper_drop_as_occupancy_rises():
    # At u=0.30 the gap is the pair's own 0.8%; at u=0.90 it is four times that (3.2%).
    assert tide_gate(GATE, gap=D('0.008'), occupancy=D('0.30'), reference=D('100'), bid=D('99.2')).allowed
    refused = tide_gate(GATE, gap=D('0.008'), occupancy=D('0.90'), reference=D('100'), bid=D('99.2'))
    assert not refused.allowed and refused.reason == 'tide_gate_down'
    assert refused.required_price == D('96.800')
    assert tide_gate(GATE, gap=D('0.008'), occupancy=D('0.90'), reference=D('100'), bid=D('96.8')).allowed


def test_upward_add_requires_spare_capital_and_the_scaled_gap():
    assert tide_gate(GATE, gap=D('0.008'), occupancy=D('0.20'), reference=D('100'), bid=D('100.8')).allowed
    full = tide_gate(GATE, gap=D('0.008'), occupancy=D('0.50'), reference=D('100'), bid=D('110'))
    assert not full.allowed and full.reason == 'tide_gate_up_occupancy'
    near = tide_gate(GATE, gap=D('0.008'), occupancy=D('0.45'), reference=D('100'), bid=D('100.8'))
    assert not near.allowed and near.reason == 'tide_gate_up_gap'  # m(0.45)=1.75 -> needs +1.4%


def test_occupancy_is_cost_plus_commitments_over_envelope():
    assert occupancy(inventory_cost=D('600'), commitments=D('250'), envelope=D('1000')) == D('0.85')
    with pytest.raises(ValueError):
        occupancy(inventory_cost=D('1'), commitments=D('0'), envelope=D('0'))
    with pytest.raises(ValueError):
        occupancy(inventory_cost=D('-1'), commitments=D('0'), envelope=D('10'))


@pytest.mark.parametrize('values', [
    dict(low_occupancy='0.9', high_occupancy='0.3', max_multiplier='4', upward_max_occupancy='0.5'),
    dict(low_occupancy='0.3', high_occupancy='0.9', max_multiplier='0.5', upward_max_occupancy='0.5'),
    dict(low_occupancy='0.3', high_occupancy='1.5', max_multiplier='4', upward_max_occupancy='0.5'),
])
def test_invalid_configuration_is_rejected(values):
    with pytest.raises(ValueError):
        TideGateConfig(**values)
