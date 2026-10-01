"""The `full_2000` capital stage (operator 2026-10-01): five pairs, 100 per buy, 2,000 per controller,
no per-pair cap. Only sizing changes with the stage; the p23 rules and the sealed canary/full
recipe values stay byte-identical."""
from decimal import Decimal as D
from test.meridian_v3.test_unified_runtime import LIVE_CAPITAL, unified

import pytest

from controllers.generic import meridian_v3_recipe as recipe
from controllers.generic.meridian_v3_portfolio import CAPITAL_STAGES, MeridianPortfolioRecipe
from controllers.generic.modular_spot_core.bag import LiveCapitalConfig

SIZED = {'bag', 'depth_budget'}


def test_the_stage_table_admits_exactly_three_versioned_scales():
    assert set(CAPITAL_STAGES) == {'canary', 'full', 'full_2000'}
    assert CAPITAL_STAGES['canary'].limits() == (D('100'), D('100'), D('20'))
    assert CAPITAL_STAGES['full'].limits() == (D('500'), D('100'), D('20'))
    assert CAPITAL_STAGES['full_2000'].limits() == (D('2000'), D('2000'), D('100'))
    assert (CAPITAL_STAGES['canary'].max_active_tranches, CAPITAL_STAGES['full'].max_active_tranches,
            CAPITAL_STAGES['full_2000'].max_active_tranches) == (8, 8, 20)


def test_the_frozen_recipe_basis_is_unchanged():
    modules = recipe.strategy_parameters()
    assert modules['bag'].slice_quote == D('40') and modules['depth_budget'].budget_quote == D('100')
    basis = LiveCapitalConfig(slice_quote='20', position_cap_quote='100', max_active_tranches=8,
                              funded_cap_quote='100', funding_fee_reserve='.003')
    assert recipe.capital_policy('.003') == basis
    frozen = {k: v.model_dump(mode='json') for k, v in modules.items()}
    for stage in ('canary', 'full'):
        config = unified(capital_stage=stage)
        assert {k: v.model_dump(mode='json') for k, v in config.recipe_modules().items()} == frozen
        assert config.recipe_capital() == recipe.capital_policy('.003')


def test_full_2000_scales_only_the_sizing_modules():
    config = unified(capital_stage='full_2000')
    assert config.total_amount_quote == 2000 and config.stage().limits() == (D('2000'), D('2000'), D('100'))
    modules, frozen = config.recipe_modules(), recipe.strategy_parameters()
    assert modules['bag'].slice_quote == D('200') and modules['depth_budget'].budget_quote == D('2000')
    assert modules['bag'].model_dump(exclude={'slice_quote'}) == frozen['bag'].model_dump(exclude={'slice_quote'})
    assert modules['depth_budget'].model_dump(exclude={'budget_quote'}) == frozen['depth_budget'].model_dump(exclude={'budget_quote'})
    for name in set(frozen) - SIZED:
        assert modules[name] == frozen[name], name
    wide = LiveCapitalConfig(slice_quote='100', position_cap_quote='2000', max_active_tranches=20,
                             funded_cap_quote='2000', funding_fee_reserve='.003')
    assert config.recipe_capital() == wide
    for pair, symbol in config.symbol_configs().items():
        assert symbol.total_amount_quote == 2000 and symbol.capital == config.recipe_capital()
        assert symbol.budget.max_quote == 2000 and symbol.bag.slice_quote == D('200') and symbol.depth_budget.budget_quote == D('2000')
    mandate = config.mandate()
    assert (mandate.funding_envelope_quote, mandate.per_asset_quote, mandate.order_quote) == (D('2000'), D('2000'), D('100'))
    MeridianPortfolioRecipe(mandate, config.p0_policy())  # admitted as a versioned stage


def test_full_2000_identity_differs_from_full_and_binds_its_envelope():
    full, wide = unified(capital_stage='full'), unified(capital_stage='full_2000')
    assert full.recipe_digest() != wide.recipe_digest()
    live = unified('live', capital_stage='full_2000', live_capital={**LIVE_CAPITAL, 'envelope_quote': '2000'})
    assert live.live_capital.envelope_quote == 2000
    with pytest.raises(ValueError, match='stage envelope'):
        unified('live', capital_stage='full_2000', live_capital=LIVE_CAPITAL)
    with pytest.raises(ValueError, match='envelope is 2000'):
        unified(capital_stage='full_2000', total_amount_quote='500')


def test_recipe_sizing_rejects_an_order_above_its_asset_limit():
    with pytest.raises(ValueError, match='order_quote'):
        recipe.strategy_parameters(order_quote='200', per_asset_quote='100')
