"""V2 owners bind funding ledgers and held-recovery checkpoints to their configuration digest.

A code upgrade that adds a configuration field must not silently change that
identity: live V2 blocked every entry on 2026-09-27 when a new field with a
behaviour-preserving default changed the digest.
"""
import json
from pathlib import Path

import pytest

from controllers.generic.rsi_modular import CONFIG_PROFILES
from hummingbot.strategy_v2.components.recipe_contract import IDENTITY_NEUTRAL_DEFAULTS, configuration_digest

PINNED = json.loads((Path(__file__).parent / 'fixtures' / 'configuration_identity_fields.json').read_text())


@pytest.mark.parametrize('profile', sorted(CONFIG_PROFILES))
def test_new_configuration_fields_are_an_explicit_identity_decision(profile):
    fields = set(CONFIG_PROFILES[profile].model_fields) - set(IDENTITY_NEUTRAL_DEFAULTS)
    added, removed = sorted(fields - set(PINNED[profile])), sorted(set(PINNED[profile]) - fields)
    assert not added and not removed, (
        f'{profile} configuration fields changed (added={added} removed={removed}). This changes the identity '
        'of every live V2 funding ledger and held-recovery checkpoint. If the new default preserves the previous '
        'behaviour, register it in IDENTITY_NEUTRAL_DEFAULTS; otherwise plan an explicit reconciliation and then '
        'update fixtures/configuration_identity_fields.json.')


@pytest.mark.parametrize('profile', sorted(CONFIG_PROFILES))
def test_identity_neutral_registry_matches_the_declared_defaults(profile):
    fields = CONFIG_PROFILES[profile].model_fields
    for name, neutral in IDENTITY_NEUTRAL_DEFAULTS.items():
        if name in fields:
            assert fields[name].default == neutral and type(fields[name].default) is type(neutral)


def test_neutral_default_is_invisible_to_the_digest_and_any_other_value_is_not():
    before = {'id': 'modular_spot_v2_sol', 'trading_pair': 'SOL-USDC', 'usd_per_entry': '25'}
    assert configuration_digest({**before, 'recovery_keep_trail_after_target': False}) == configuration_digest(before)
    assert configuration_digest({**before, 'recovery_keep_trail_after_target': True}) != configuration_digest(before)
    assert configuration_digest({**before, 'recovery_keep_trail_after_target': 0}) != configuration_digest(before)
    assert configuration_digest({**before, 'allocation_mode': 'bounded'}) == configuration_digest(before)
    assert configuration_digest({**before, 'allocation_mode': 'wallet_funded'}) != configuration_digest(before)
