from test.test_ok_rsi_sell_policy import controller, position, strict_wallet_controller


def test_wallet_sell_plan_never_describes_buy_entries_without_owned_basis():
    bot = strict_wallet_controller()
    bot.processed_data['thresholds'] = {'rsi_sell': 79.25}
    result = bot._compute_plan_fields()
    assert result['plan_mode'] == 'EXITS'
    assert result['plan_target'] == 'rsi≥79.25'
    assert result['plan_slice'] == '10% of available SUI'
    assert 'scout' not in str(result) and 'runner' not in str(result)


def test_main_recovery_plan_uses_configured_quote_target_instead_of_legacy_fraction():
    bot = controller()
    bot.positions_held = [position(bot)]
    result = bot._compute_plan_fields()
    assert result['plan_slice'] == 'target 50 USDC, limited to sellable inventory'
