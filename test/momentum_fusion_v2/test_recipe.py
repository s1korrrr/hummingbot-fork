import pytest

from controllers.generic.momentum_fusion_v2_core.recipe import MomentumRecipe, bind_recipe, verify_recipe, assert_single_treatment
from .fixtures import recipe, scope


def test_complete_binding_checks_recipe_id_and_hash(r):
    binding = bind_recipe(r, source_sha256='1'*64)
    verify_recipe(r, binding, source_sha256='1'*64)
    for key, value in [('recipe_id','other'), ('recipe_sha256','2'*64),
                       ('configuration_sha256','2'*64), ('source_sha256','2'*64)]:
        with pytest.raises(ValueError):
            verify_recipe(r, binding.model_copy(update={key:value}), source_sha256='1'*64)


@pytest.mark.parametrize('values', [{'variant':'C2'}, {'execution_enabled':True}, {'execution_enabled':0},
    {'leverage':2}, {'profile':'ok_rsi'}, {'initialization_sha256':'bad'}, {'schema_version':True},
    {'features':{'lookback':True}}, {'shelf':{'max_pullback':True}}, {'features':{'min_relative_volume':'NaN'}},
    {'features':{'lookback':100}}, {'shelf':{'min_bars':7,'max_bars':6}}, {'symbol':'USDC-USDC'}])
def test_unsupported_or_invalid_recipe_rejected(values):
    with pytest.raises(ValueError):
        recipe(**values)


def test_overlay_comparison_changes_only_variant():
    assert_single_treatment(recipe('B0'), recipe('C1'))
    with pytest.raises(ValueError):
        assert_single_treatment(recipe('B0'), recipe('C1', sizing_policy_sha256='9'*64))
    with pytest.raises(ValueError):
        assert_single_treatment(recipe('B0'), recipe('B0'))


def test_scope_and_serialization(r):
    assert MomentumRecipe.model_validate_json(r.model_dump_json()) == r
    assert scope(r, 'shadow') != scope(r)
    assert scope(r).recipe_sha256 == r.digest
    assert scope(r).connector == 'okx'


def test_copy_cannot_enable_execution(r):
    with pytest.raises(ValueError):
        bind_recipe(r.model_copy(update={'execution_enabled':True}), source_sha256='1'*64)
