from controllers.generic.momentum_fusion_v2_core import recipe, setups
from hummingbot.strategy_v2.components.decision_protocol import Prerequisite


def test_first_slice_excludes_compression_and_reclaim():
    spec = recipe.RecipeSpec()
    assert spec.modules() == frozenset({'b0_breakout', 'c1_shelf'})
    assert 'c2_compression' not in spec.modules()
    assert 'c3_reclaim' not in spec.modules()


def test_c2_and_c3_require_an_explicit_later_recipe():
    later = recipe.RecipeSpec(enabled=('c2_compression', 'c3_reclaim'))
    assert later.modules() == frozenset({'c2_compression', 'c3_reclaim'})
    assert later.execution_enabled is False


def test_enabling_all_modules_is_not_the_first_comparison():
    everything = recipe.RecipeSpec(enabled=recipe.PRECEDENCE)
    assert everything.modules() != recipe.FIRST_SLICE
    assert set(recipe.PRECEDENCE) - recipe.FIRST_SLICE == {'c2_compression', 'c3_reclaim'}


def test_interaction_does_not_silently_blend_setups():
    checks = (Prerequisite(name='data', status='PASS', evidence_sha256='a' * 64),)
    proposed = (
        setups.SetupState(setup='c2_compression', phase='PROPOSED', reason='c2', last_event_ms=1),
        setups.SetupState(setup='c3_reclaim', phase='PROPOSED', reason='c3', last_event_ms=1),
        setups.SetupState(setup='b0_breakout', phase='PROPOSED', reason='b0', last_event_ms=1),
    )
    chosen = recipe.select(proposed, checks, frozenset(recipe.PRECEDENCE))
    assert chosen.setup == 'b0_breakout'
    later = recipe.select(proposed, checks, frozenset({'c2_compression', 'c3_reclaim'}))
    assert later.setup == 'c2_compression'
