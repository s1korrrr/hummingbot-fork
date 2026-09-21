from pydantic import ValidationError
import pytest

from controllers.generic.momentum_fusion_v2_core import recipe, setups
from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding


def proposed(name, reason='proposed'):
    return setups.SetupState(setup=name, phase='PROPOSED', reason=reason, last_event_ms=1)


def test_c1_beats_b0_regardless_of_input_order():
    enabled = frozenset({'b0_breakout', 'c1_shelf'})
    checks = (Prerequisite(name='data', status='PASS', evidence_sha256='a' * 64),)
    left = recipe.select((proposed('b0_breakout'), proposed('c1_shelf')), checks, enabled)
    right = recipe.select((proposed('c1_shelf'), proposed('b0_breakout')), checks, enabled)
    assert left.setup == right.setup == 'c1_shelf'


def test_safety_veto_dominates_alpha():
    enabled = frozenset({'b0_breakout', 'c1_shelf'})
    veto = (Prerequisite(name='risk', status='HOLD'),)
    assert recipe.select((proposed('c1_shelf'),), veto, enabled) is None


def test_disabled_modules_cannot_win():
    enabled = frozenset({'b0_breakout'})
    checks = (Prerequisite(name='data', status='PASS', evidence_sha256='a' * 64),)
    assert recipe.select((proposed('c1_shelf'), proposed('b0_breakout')), checks, enabled).setup == 'b0_breakout'


def test_unknown_recipe_fields_are_rejected():
    with pytest.raises(ValueError):
        recipe.RecipeSpec(enabled=('made_up',))
    with pytest.raises(ValidationError):
        recipe.RecipeSpec.model_validate({'recipe_id': 'mom_v2', 'unknown_field': 1})


def test_binding_is_not_authority():
    binding = recipe.bind('a' * 64, 'b' * 64)
    assert isinstance(binding, RecipeBinding)
    assert binding.recipe_id == 'mom_v2'


def test_binding_hashes_the_actual_enabled_modules():
    default = recipe.bind('a' * 64, 'b' * 64)
    later = recipe.bind('a' * 64, 'b' * 64, recipe.RecipeSpec(enabled=('c2_compression',)))
    assert later.recipe_sha256 != default.recipe_sha256
