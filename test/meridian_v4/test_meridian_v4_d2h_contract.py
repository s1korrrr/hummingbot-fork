"""D2h binds V4 plus the frozen depth-scaled sizing of the edge search, as a separate paper program."""
import hashlib
import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from controllers.generic import meridian_v4_recipe as v4
from controllers.generic.meridian_v4 import MeridianV4Config
from controllers.generic.meridian_v4_d2h import MeridianV4D2hConfig, MeridianV4D2hController
from controllers.generic.meridian_v4_d2h_recipe import CANDIDATE_SHA256, PREPARED_SHA256, RECIPE_ID, strategy_parameters
from controllers.generic.modular_spot_core.bag import depth_slice_quote
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, configuration_digest, digest

FIXTURES = Path(__file__).with_name('fixtures')
RISK = dict(max_drawdown_fraction='.10', max_daily_loss_fraction='.05',
            max_spread_fraction='.003', fee_reserve_fraction='.003', cash_reserve_quote='0')
BUDGET = dict(group='meridian_v4_d2h', max_quote='200000', max_assets=5)
RESEARCH_ONLY = {('bag', 'price_tick'), ('bag', 'maker_fee_rate')}


def config(pair='BNB-USDC', **overrides):
    values = dict(id=f'modular_spot_meridian_v4_d2h_paper_{pair.split("-")[0].lower()}',
                  trading_pair=pair, execution_mode='paper', connector_name='okx_paper_trade',
                  account='meridian_v4_d2h_paper', risk=RISK, budget=BUDGET, total_amount_quote='40000')
    values.update(overrides)
    if values.get('execution_enabled') and 'recipe_binding' not in values:
        disabled = MeridianV4D2hConfig(**{**values, 'execution_enabled': False})
        values['recipe_binding'] = disabled.seal()
    return MeridianV4D2hConfig(**values)


def flat(modules):
    return {(name, key): value for name, module in modules.items()
            for key, value in module.model_dump(mode='json').items()}


@pytest.mark.parametrize('pair', v4.PAIRS)
def test_five_pairs_bind_the_d2h_recipe_with_the_depth_ladder_at_the_pair_budget(pair):
    c = config(pair)
    assert c.get_controller_class() is MeridianV4D2hController
    assert c.capital.slice_quote == 50 and c.bag.slice_quote == 100 and c.bag.remainder_sweep_quote == D('2.5')
    assert c.capital.position_cap_quote == c.capital.funded_cap_quote == c.depth_budget.budget_quote == 40000
    d = c.depth_budget
    assert (d.lookback_minutes, d.full_depth, d.base_fraction, d.max_slice_multiplier) == (1440, D('.25'), 1, 5)
    assert [depth_slice_quote(d, c.capital.slice_quote, x) for x in ('0', '.125', '.25', '.5')] == [50, 150, 250, 250]


def test_d2h_differs_from_v4_only_in_the_declared_values():
    v4_values, d2h_values = flat(v4.strategy_parameters()), flat(strategy_parameters())
    assert v4_values.keys() == d2h_values.keys()
    changed = {k: (v4_values[k], d2h_values[k]) for k in v4_values if v4_values[k] != d2h_values[k]}
    assert changed == {('bag', 'slice_quote'): ('200', '100'), ('bag', 'remainder_sweep_quote'): ('5', '2.5')}


def test_every_executed_module_value_matches_the_frozen_stage3_run():
    raw = FIXTURES.joinpath('bnb-d2h-prepared.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == PREPARED_SHA256
    assert hashlib.sha256(FIXTURES.joinpath('bnb-d2h.json').read_bytes()).hexdigest() == CANDIDATE_SHA256
    prepared, c = json.loads(raw), config()
    assert prepared['candidate']['sha256'] == CANDIDATE_SHA256
    effective = prepared['effective_config']
    for field in ('signal', 'acquisition', 'trailing'):
        for key, expected in effective[field].items():
            actual = getattr(getattr(c, field), key)
            if isinstance(expected, (bool, list)) or key == 'kind':
                assert (tuple(D(x) for x in expected) == actual if isinstance(expected, list) else expected == actual)
            else:
                assert D(str(actual)) == D(str(expected)), (field, key)
    research = effective['research']
    for field in ('bag', 'adaptive_trailing', 'buy_gap', 'flash_crash'):
        for key, expected in research[field].items():
            if (field, key) not in RESEARCH_ONLY:
                assert D(str(getattr(getattr(c, field), key))) == D(str(expected)), (field, key)
    for key, expected in research['depth_budget'].items():
        assert D(str(getattr(c.depth_budget, key))) == D(str(expected)), key
    assert D(research['capital']['slice_quote']) == c.capital.slice_quote
    assert D(research['capital']['max_active_tranches']) == c.capital.max_active_tranches
    assert research.get('stop_loss') is None


@pytest.mark.parametrize('override', [
    {'depth_budget': {'budget_quote': '40000', 'lookback_minutes': 1440, 'full_depth': '.25',
                      'base_fraction': '1', 'max_slice_multiplier': '4'}},
    {'depth_budget': {'budget_quote': '20000', 'lookback_minutes': 1440, 'full_depth': '.25',
                      'base_fraction': '1', 'max_slice_multiplier': '5'}},
    {'bag': v4.strategy_parameters()['bag'].model_dump(mode='json')},
    {'id': 'modular_spot_meridian_v4_paper_bnb'}, {'account': 'meridian_v4_paper'},
    {'budget': {'group': 'meridian_v4', 'max_quote': '200000', 'max_assets': 5}},
    {'total_amount_quote': '249'}, {'execution_mode': 'live'}, {'controller_name': 'meridian_v4'},
])
def test_invalid_composition_cannot_relabel_itself_as_d2h_or_v4(override):
    with pytest.raises(ValueError):
        config(**override)


def test_v4_cannot_carry_a_depth_budget_or_the_d2h_identity():
    values = config().model_dump(mode='json')
    with pytest.raises(ValueError):
        MeridianV4Config.model_validate(values)


def test_seal_binds_sources_recipe_and_budget():
    c = config()
    bound = config(execution_enabled=True)
    assert bound.recipe_binding.recipe_id == RECIPE_ID
    assert bound.recipe_binding.recipe_sha256 == c.recipe_digest()
    assert config(total_amount_quote='20000').recipe_digest() != c.recipe_digest()
    assert {'meridian_v4_d2h.py', 'meridian_v4_d2h_recipe.py', 'meridian_v4.py', 'meridian_v4_recipe.py'} <= {
        p for p, _ in c.source_manifest()}
    with pytest.raises(ValueError, match='source'):
        config(execution_enabled=True,
               recipe_binding=bound.recipe_binding.model_copy(update={'source_sha256': '0' * 64}))
    with pytest.raises(ValueError, match='sealed recipe'):
        config(execution_enabled=True, recipe_binding=None)


def test_v4_recipe_digest_is_unchanged_by_the_variant_hooks():
    """V4's digest has no depth_budget key, so the hooks do not move V4's identity."""
    c = MeridianV4Config(id='modular_spot_meridian_v4_paper_bnb', trading_pair='BNB-USDC', execution_mode='paper',
                         connector_name='okx_paper_trade', account='meridian_v4_paper', risk=RISK,
                         budget=dict(group='meridian_v4', max_quote='200000', max_assets=5),
                         total_amount_quote='40000')
    expected = digest({'recipe_id': v4.RECIPE_ID, 'candidate_sha256': v4.CANDIDATE_SHA256,
                       'prepared_sha256': v4.PREPARED_SHA256,
                       'modules': {k: m.model_dump(mode='json') for k, m in v4.strategy_parameters().items()},
                       'capital': c.capital.model_dump(mode='json'), 'exit_floor': 'protected'})
    assert c.recipe_digest() == expected


def test_controller_refuses_a_v4_config():
    with pytest.raises(ValueError, match='Exact V4 D2h'):
        MeridianV4D2hController(MeridianV4Config.model_construct(), None, None)


def test_seal_is_the_binding_the_validator_verifies():
    c = config()
    assert c.seal() == RecipeBinding(recipe_id=RECIPE_ID, recipe_sha256=c.recipe_digest(),
                                     source_sha256=digest(c.source_manifest()),
                                     configuration_sha256=configuration_digest(c.model_dump(mode='json')))
