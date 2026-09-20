import importlib
from decimal import Decimal

import pytest

MODULE = 'controllers.generic.modular_spot_core.recipe_contract'


def api():
    try:
        return importlib.import_module(MODULE)
    except ModuleNotFoundError:
        pytest.fail('The approved immutable recipe contract is not implemented')


def recipe(**changes):
    values = dict(name='rebound', family='rebound', connector='okx', symbols=('BTC-USDC',),
                  quote_budget='100', max_positions=1)
    values.update(changes)
    return api().StrategyRecipe(**values)


def test_recipe_roundtrip_is_stable_and_execution_is_not_a_recipe_permission():
    value = recipe()
    assert value == api().StrategyRecipe.model_validate_json(value.model_dump_json())
    assert value.digest == api().StrategyRecipe.model_validate_json(value.model_dump_json()).digest
    with pytest.raises(ValueError):
        recipe(execution_enabled=True)


def test_recipe_is_deeply_immutable():
    value = recipe(symbols=['BTC-USDC'])
    assert value.symbols == ('BTC-USDC',)
    with pytest.raises(ValueError):
        value.signal.oversold = Decimal('90')
    with pytest.raises(ValueError):
        value.symbols = ('ETH-USDC',)


@pytest.mark.parametrize('changes', [
    {'quote_budget': True}, {'quote_budget': 'NaN'}, {'quote_budget': '-1'},
    {'max_positions': True}, {'symbols': ('BTC-USDC', 'BTC-USDC')},
    {'symbols': ('BTC-USDC', 'ETH-USDT')}, {'symbols': ()},
    {'symbols': ('BTC-USDC',), 'observation_symbols': ('ETH-USDC',)},
    {'max_positions': 2}, {'family': 'perpetuals'},
    {'family': 'impulse', 'signal': {'kind': 'rsi_rebound'}},
    {'family': 'impulse', 'acquisition': {'fractions': ['0.5', '0.5']}},
    {'family': 'sell_only', 'acquisition': {'fractions': ['0.5', '0.5']}},
    {'account_topology': 'independent_shared_wallets'},
])
def test_invalid_compositions_are_rejected(changes):
    with pytest.raises(ValueError):
        recipe(**changes)


def test_reference_recipes_have_explicit_different_behaviour():
    rebound = recipe(acquisition={'fractions': ['0.3', '0.4', '0.3']})
    impulse = recipe(family='impulse')
    seller = recipe(family='sell_only')
    assert rebound.signal.kind == 'rsi_rebound'
    assert impulse.signal.kind == 'breakout'
    assert impulse.acquisition.fractions == (Decimal('1'),)
    assert not seller.acquisition_allowed
    assert len({rebound.digest, impulse.digest, seller.digest}) == 3


def test_config_seal_detects_behaviour_change_but_allows_safe_pause_flags():
    m = api()
    payload = {'id': 'modular_spot_x', 'signal': {'kind': 'rsi_rebound'},
               'execution_enabled': False, 'manual_kill_switch': False, 'acquisition_enabled': True}
    bound = m.RecipeBinding(recipe_id='rebound', recipe_sha256='a' * 64,
                            source_sha256='b' * 64, configuration_sha256=m.configuration_digest(payload))
    m.verify_binding(bound, payload, source_sha256='b' * 64)
    m.verify_binding(bound, {**payload, 'manual_kill_switch': True, 'acquisition_enabled': False}, source_sha256='b' * 64)
    with pytest.raises(ValueError):
        m.verify_binding(bound, {**payload, 'signal': {'kind': 'breakout'}}, source_sha256='b' * 64)
    with pytest.raises(ValueError):
        m.verify_binding(bound, payload, source_sha256='c' * 64)


def test_source_digest_binds_relative_paths_and_bytes(tmp_path):
    m = api()
    (tmp_path / 'a.py').write_text('x = 1\n')
    first = m.source_digest(tmp_path, (tmp_path / 'a.py',))
    (tmp_path / 'a.py').write_text('x = 2\n')
    assert m.source_digest(tmp_path, (tmp_path / 'a.py',)) != first
    with pytest.raises(ValueError):
        m.source_digest(tmp_path, (tmp_path / 'a.py', tmp_path / 'a.py'))
