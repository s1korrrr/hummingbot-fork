import pytest

from controllers.generic.modular_spot_core.profiles import ProgramProfile, build_profile


def test_five_symbol_profile_has_one_native_pair_controller_per_symbol_and_one_feed_manifest():
    p = ProgramProfile(name='selected', connector='okx', symbols=('A-USDC', 'B-USDC', 'C-USDC', 'D-USDC', 'E-USDC'),
                       mode='fixed', quote_budget='500', max_positions=5)
    result = build_profile(p)
    assert len(result['controllers']) == 5
    assert len(result['candle_manifest']['datasets']) == 5
    assert {c['budget']['group'] for c in result['controllers']} == {'selected'}
    assert all(c['execution_enabled'] is False and c['total_amount_quote'] == '100' for c in result['controllers'])


def test_sell_only_profile_never_enables_acquisition_or_adopts_wallet_inventory():
    p = ProgramProfile(name='inventory', connector='okx', symbols=('A-USDC', 'B-USDC', 'C-USDC'),
                       mode='sell_only', quote_budget='300', max_positions=3)
    result = build_profile(p)
    assert len(result['controllers']) == 3
    assert all(not c['acquisition_enabled'] and c['initial_positions'] == [] for c in result['controllers'])
    assert result['inventory_adoption_supported'] is False


def test_impulse_can_observe_all_admitted_symbols_but_trade_only_an_explicit_subset():
    p = ProgramProfile(name='impulse', connector='okx', symbols=('A-USDC', 'B-USDC'),
                       observation_symbols=('A-USDC', 'B-USDC', 'C-USDC', 'BTC-USDC'),
                       mode='impulse', quote_budget='100', max_positions=1)
    result = build_profile(p)
    assert len(result['controllers']) == 2 and len(result['candle_manifest']['datasets']) == 4
    assert all(c['data']['symbols'] == ['A-USDC', 'B-USDC', 'C-USDC', 'BTC-USDC'] for c in result['controllers'])
    assert all(c['budget']['max_assets'] == 1 and c['universe']['observe_all'] for c in result['controllers'])
    assert all(c['acquisition']['fractions'] == ['1'] for c in result['controllers'])


@pytest.mark.parametrize('symbols', [(), ('BTC-USDC', 'ETH-USDT'), ('BTC-USDC', 'BTC-USDC')])
def test_invalid_or_mixed_quote_programs_rejected(symbols):
    with pytest.raises(ValueError):
        ProgramProfile(name='bad', connector='okx', symbols=symbols, mode='fixed', quote_budget='100', max_positions=1)


def test_profile_bundle_is_a_disabled_native_controller_config():
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    result = build_profile(ProgramProfile(
        name='selected', connector='okx', symbols=('BTC-USDC',), mode='fixed', quote_budget='100', max_positions=1))
    validated = ModularSpotControllerConfig.model_validate(result['controllers'][0])
    assert validated.execution_enabled is False
    assert validated.data.mode == 'shared'


def test_unknown_profile_keys_are_rejected():
    with pytest.raises(ValueError):
        ProgramProfile(name='selected', connector='okx', symbols=('BTC-USDC',), mode='fixed',
                       quote_budget='100', max_positions=1, capital_file='nope.yml')
