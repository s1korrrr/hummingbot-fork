"""V4 binds the backtested v3c candidate as a stand-alone per-pair composition."""
import hashlib
import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from controllers.generic import meridian_v3_recipe
from controllers.generic.meridian_v4 import MeridianV4Config, MeridianV4Controller
from controllers.generic.meridian_v4_recipe import (
    CANDIDATE_SHA256,
    PAIRS,
    PREPARED_SHA256,
    RECIPE_ID,
    strategy_parameters,
)
from controllers.generic.modular_spot_core.signals import signal_decision
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, configuration_digest, digest

FIXTURES = Path(__file__).with_name('fixtures')
RISK = dict(max_drawdown_fraction='.10', max_daily_loss_fraction='.05',
            max_spread_fraction='.003', fee_reserve_fraction='.003', cash_reserve_quote='0')
BUDGET = dict(group='meridian_v4', max_quote='5000', max_assets=5)
# Research-only candle-model values: the venue price tick and the modelled maker fee come from the
# connector and the exchange in a native run, not from the recipe.
RESEARCH_ONLY = {('bag', 'price_tick'), ('bag', 'maker_fee_rate')}


def config(pair='BNB-USDC', mode='paper', **overrides):
    values = dict(id=f'modular_spot_meridian_v4_{mode}_{pair.split("-")[0].lower()}',
                  trading_pair=pair, execution_mode=mode,
                  connector_name='okx_paper_trade' if mode == 'paper' else 'okx',
                  account=f'meridian_v4_{mode}', risk=RISK, budget=BUDGET, total_amount_quote='1000')
    if mode == 'shadow':
        values['data'] = {'mode': 'shared', 'transport': 'stream', 'origin': 'http://127.0.0.1:8092',
                          'token_file': '/run/candles/token'}
    values.update(overrides)
    if values.get('execution_enabled') and mode == 'paper' and 'recipe_binding' not in values:
        disabled = MeridianV4Config(**{**values, 'execution_enabled': False})
        values['recipe_binding'] = RecipeBinding(
            recipe_id=RECIPE_ID, recipe_sha256=disabled.recipe_digest(),
            source_sha256=digest(disabled.source_manifest()),
            configuration_sha256=configuration_digest(disabled.model_dump(mode='json')))
    return MeridianV4Config(**values)


def flat(modules):
    return {(name, key): value for name, module in modules.items()
            for key, value in module.model_dump(mode='json').items()}


@pytest.mark.parametrize('pair', PAIRS)
def test_five_pairs_bind_one_recipe_and_explicit_budget(pair):
    c = config(pair)
    assert c.get_controller_class() is MeridianV4Controller
    assert c.recipe_version == RECIPE_ID
    assert c.capital.slice_quote == 100 and c.bag.slice_quote == 200
    assert c.capital.position_cap_quote == c.capital.funded_cap_quote == c.total_amount_quote == 1000
    assert c.capital.max_active_tranches == 64
    assert c.depth_budget is None
    assert (c.flash_crash.lookback, c.flash_crash.drop_pct, c.flash_crash.cooldown_seconds) == (5, D('.04'), 300)
    assert c.budget.group == 'meridian_v4' and c.budget.max_assets == 5
    assert c.execution_enabled is False


def test_recipe_is_the_backtested_v3c_candidate():
    raw = FIXTURES.joinpath('bnb-v3c.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == CANDIDATE_SHA256
    v3c, c = json.loads(raw), config()
    for field in ('signal', 'trailing'):
        for key, expected in v3c[field].items():
            assert D(str(getattr(getattr(c, field), key))) == D(str(expected))
    for field in ('bag', 'adaptive_trailing', 'buy_gap', 'flash_crash'):
        for key, expected in v3c['research'][field].items():
            if (field, key) not in RESEARCH_ONLY:
                assert D(str(getattr(getattr(c, field), key))) == D(str(expected)), (field, key)
    assert D(v3c['research']['capital']['slice_quote']) == c.capital.slice_quote
    assert 'depth_budget' not in v3c['research']


def test_every_executed_module_value_matches_the_sealed_prepared_run():
    """The owner's effective configuration of the BNB run, including values the candidate inherited."""
    raw = FIXTURES.joinpath('bnb-v3c-prepared.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == PREPARED_SHA256
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
    assert D(research['capital']['max_active_tranches']) == c.capital.max_active_tranches
    assert research.get('stop_loss') is None and research.get('depth_budget') is None


def test_v4_differs_from_meridian_v3_only_in_the_declared_values():
    v3 = meridian_v3_recipe.strategy_parameters(order_quote=100, per_asset_quote=1000)
    del v3['depth_budget']  # V4 runs without a depth budget
    v4_values, v3_values = flat(strategy_parameters()), flat(v3)
    assert v4_values.keys() == v3_values.keys()
    changed = {k: (v3_values[k], v4_values[k]) for k in v4_values if v4_values[k] != v3_values[k]}
    assert changed == {
        ('signal', 'max_atr_fraction'): ('0.01', '0.0012'),
        ('trailing', 'activation'): ('0.04', '0.025'),
        ('adaptive_trailing', 'min_locked_profit'): ('0.013', '0.010'),
        ('flash_crash', 'lookback'): (17, 5),
        ('flash_crash', 'drop_pct'): ('0.01060362829599566', '0.04'),
        ('flash_crash', 'cooldown_seconds'): (29993, 300),
    }


@pytest.mark.parametrize('atr, enter, reason', [
    ('0.0011', True, 'rsi_rebound'), ('0.0012', True, 'rsi_rebound'),
    ('0.0013', False, 'high_volatility'), ('0.0001', False, 'low_volatility'),
])
def test_entry_requires_the_atr_band(atr, enter, reason):
    result = signal_decision(config().signal, previous_rsi='28', rsi='30', atr_fraction=atr,
                             breakout=False, trend_up=False)
    assert (result.enter, result.reason) == (enter, reason)


@pytest.mark.parametrize('override', [
    {'trading_pair': 'SUI-USDC'}, {'trading_pair': 'BNB-USDT'},
    {'id': 'modular_spot_v2_bnb'}, {'account': 'incumbent'},
    {'execution_mode': 'live'}, {'connector_name': 'okx'},
    {'total_amount_quote': '99'}, {'budget': {'group': 'other', 'max_quote': '5000', 'max_assets': 5}},
    {'budget': {'group': 'meridian_v4', 'max_quote': '999', 'max_assets': 5}},
    {'capital': {'slice_quote': '100', 'position_cap_quote': '1000', 'funded_cap_quote': '1000.3'}},
    {'signal': {'rsi_length': 17, 'oversold': '29', 'max_atr_fraction': '.01'}},
    {'trailing': {'activation': '.04', 'delta': '.005'}},
    {'adaptive_trailing': {'min_locked_profit': '.013'}},
    {'flash_crash': {'lookback': 17, 'drop_pct': '.01060362829599566', 'cooldown_seconds': 29993}},
    {'depth_budget': {'budget_quote': '1000'}}, {'exit_floor': 'live_no_loss'}, {'bag': None},
    {'data': {'mode': 'shared', 'transport': 'stream', 'origin': 'http://127.0.0.1:8092',
              'token_file': '/run/candles/token'}},
])
def test_invalid_composition_cannot_silently_relabel_itself(override):
    with pytest.raises(ValueError):
        config(**override)


def test_shadow_cannot_dispatch_and_budget_and_risk_must_be_explicit():
    config(mode='shadow')
    with pytest.raises(ValueError, match='shadow'):
        config(mode='shadow', execution_enabled=True)
    with pytest.raises(ValueError, match='shadow'):
        config(mode='shadow', data={'mode': 'native'})
    for field in ('risk', 'budget', 'total_amount_quote'):
        values = config().model_dump(mode='json')
        del values[field]
        with pytest.raises(ValueError, match=field):
            MeridianV4Config.model_validate(values)
    with pytest.raises(ValueError, match='sealed recipe'):
        config(execution_enabled=True, recipe_binding=None)


def test_binding_seals_v4_sources_recipe_and_behavior():
    c = config()
    binding = RecipeBinding(recipe_id=RECIPE_ID, recipe_sha256=c.recipe_digest(),
                            source_sha256=digest(c.source_manifest()),
                            configuration_sha256=configuration_digest(c.model_dump(mode='json')))
    bound = config(recipe_binding=binding)
    assert bound.fingerprint() != config('ETH-USDC').fingerprint()
    assert config(total_amount_quote='2000').recipe_digest() != c.recipe_digest()  # the budget is sealed
    with pytest.raises(ValueError, match='source'):
        config(recipe_binding=binding.model_copy(update={'source_sha256': '0' * 64}))
    with pytest.raises(ValueError, match='recipe'):
        config(recipe_binding=binding.model_copy(update={'recipe_sha256': '0' * 64}))
    assert {'meridian_v4.py', 'meridian_v4_recipe.py'} <= {p for p, _ in c.source_manifest()}
    assert not {'meridian_v3_recipe.py', 'meridian_v3.py'} & {p for p, _ in c.source_manifest()}


def test_stand_alone_controller_inherits_only_the_generic_host():
    names = {cls.__module__ for cls in MeridianV4Controller.__mro__}
    assert 'controllers.generic.modular_spot' in names
    assert not any(n.startswith(('controllers.generic.meridian_v3', 'controllers.generic.tidewater'))
                   or 'ok_rsi' in n or 'rsi_v5' in n for n in names)
    with pytest.raises(ValueError, match='Exact V4'):
        MeridianV4Controller(object(), None, None)
