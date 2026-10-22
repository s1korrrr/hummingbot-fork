"""Meridian binds BNB p23; it does not inherit the legacy RSI controllers."""
import hashlib
import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from controllers.generic.meridian_v3 import MeridianV3Config, MeridianV3Controller
from controllers.generic.meridian_v3_recipe import CANDIDATE_SHA256, PAIRS, RECIPE_ID, RESULT_SHA256
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, configuration_digest, digest

RISK = dict(max_drawdown_fraction='.10', max_daily_loss_fraction='.05',
            max_spread_fraction='.003', fee_reserve_fraction='.003', cash_reserve_quote='0')


def config(pair='BNB-USDC', mode='paper', **overrides):
    values = dict(id=f'modular_spot_meridian_v3_{mode}_{pair.split("-")[0].lower()}',
                  trading_pair=pair, execution_mode=mode,
                  connector_name='okx_paper_trade' if mode == 'paper' else 'okx',
                  account=f'meridian_v3_{mode}', risk=RISK)
    if mode == 'shadow':
        values['data'] = {'mode': 'shared', 'transport': 'stream', 'origin': 'http://127.0.0.1:8092',
                          'token_file': '/run/candles/token'}
    values.update(overrides)
    if values.get('execution_enabled') and mode == 'paper' and 'recipe_binding' not in values:
        disabled = MeridianV3Config(**{**values, 'execution_enabled': False})
        values['recipe_binding'] = RecipeBinding(
            recipe_id=RECIPE_ID, recipe_sha256=disabled.recipe_digest(),
            source_sha256=digest(disabled.source_manifest()),
            configuration_sha256=configuration_digest(disabled.model_dump(mode='json')))
    return MeridianV3Config(**values)


@pytest.mark.parametrize('pair', PAIRS)
def test_five_pairs_bind_one_recipe_and_strict_caps(pair):
    c = config(pair)
    assert c.get_controller_class() is MeridianV3Controller
    assert c.recipe_version == RECIPE_ID
    assert c.capital.slice_quote == 20
    assert c.capital.position_cap_quote == c.capital.funded_cap_quote == c.total_amount_quote == 100
    assert c.capital.max_active_tranches == 8  # strategy limit, never five mandatory DCA stages
    assert c.depth_budget.budget_quote == 100
    assert c.budget.max_quote == 500 and c.budget.max_assets == 5
    assert c.budget.group == 'meridian_v3'
    assert c.execution_enabled is False


def test_recipe_matches_frozen_bnb_evidence_except_declared_sizing_and_venue_tick():
    raw = Path(__file__).with_name('fixtures').joinpath('bnb-p23.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == CANDIDATE_SHA256
    p23, c = json.loads(raw), config()
    for field in ('signal', 'acquisition', 'trailing'):
        for key, expected in p23[field].items():
            assert D(str(getattr(getattr(c, field), key))) == D(str(expected))
    for field in ('bag', 'adaptive_trailing', 'buy_gap', 'flash_crash', 'depth_budget'):
        for key, expected in p23['research'][field].items():
            if (field, key) in {('bag', 'slice_quote'), ('bag', 'price_tick'), ('bag', 'maker_fee_rate')}:
                continue
            assert D(str(getattr(getattr(c, field), key))) == D(str(expected))
    assert c.signal.rsi_length == 17 and c.signal.kind == 'rsi_rebound'
    assert c.bag.slice_quote == 40
    assert c.exit_floor == 'protected'


def test_complete_executed_recipe_including_inherited_p23_inputs():
    raw = Path(__file__).with_name('fixtures').joinpath('bnb-p23-result.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == RESULT_SHA256
    model, c = json.loads(raw)['model'], config()
    for field in ('signal', 'acquisition', 'trailing'):
        for key, expected in model[field].items():
            actual = getattr(getattr(c, field), key)
            if isinstance(expected, (bool, list)) or key == 'kind':
                assert (tuple(D(x) for x in expected) == actual if isinstance(expected, list) else expected == actual)
            else:
                assert D(str(actual)) == D(str(expected))
    assert model['candidate']['sha256'] == CANDIDATE_SHA256


@pytest.mark.parametrize('override', [
    {'trading_pair': 'SUI-USDC'}, {'trading_pair': 'BNB-USDT'},
    {'id': 'modular_spot_v2_bnb'}, {'account': 'incumbent'},
    {'execution_mode': 'live'}, {'connector_name': 'okx'},
    {'total_amount_quote': '101'}, {'budget': {'group': 'other', 'max_quote': '500', 'max_assets': 5}},
    {'capital': {'slice_quote': '20', 'position_cap_quote': '100', 'funded_cap_quote': '100.3'}},
    {'signal': {'rsi_length': 14}}, {'adaptive_trailing': {'min_locked_profit': '.004'}},
    {'exit_floor': 'live_no_loss'}, {'depth_budget': None}, {'bag': None},
    {'data': {'mode': 'shared', 'transport': 'stream', 'origin': 'http://127.0.0.1:8092',
              'token_file': '/run/candles/token'}},
])
def test_invalid_composition_cannot_silently_relabel_itself(override):
    with pytest.raises(ValueError):
        config(**override)


def test_shadow_cannot_dispatch_and_risk_must_be_explicit():
    config(mode='shadow')
    with pytest.raises(ValueError, match='shadow'):
        config(mode='shadow', execution_enabled=True)
    with pytest.raises(ValueError, match='shadow'):
        config(mode='shadow', data={'mode': 'native'})
    values = config().model_dump(mode='json')
    del values['risk']
    with pytest.raises(ValueError, match='risk'):
        MeridianV3Config.model_validate(values)
    with pytest.raises(ValueError, match='sealed recipe'):
        config(execution_enabled=True, recipe_binding=None)


def test_binding_seals_meridian_sources_recipe_and_behavior():
    c = config()
    binding = RecipeBinding(recipe_id=RECIPE_ID, recipe_sha256=c.recipe_digest(),
                            source_sha256=digest(c.source_manifest()),
                            configuration_sha256=configuration_digest(c.model_dump(mode='json')))
    bound = config(recipe_binding=binding)
    assert bound.fingerprint() != config('ETH-USDC').fingerprint()
    with pytest.raises(ValueError, match='source'):
        config(recipe_binding=binding.model_copy(update={'source_sha256': '0' * 64}))
    with pytest.raises(ValueError, match='recipe'):
        config(recipe_binding=binding.model_copy(update={'recipe_sha256': '0' * 64}))
    assert {'meridian_v3.py', 'meridian_v3_recipe.py'} <= {p for p, _ in c.source_manifest()}
    assert all('ok_rsi' not in cls.__name__.lower() and 'rsiv5' not in cls.__name__.lower()
               for cls in MeridianV3Controller.__mro__)
