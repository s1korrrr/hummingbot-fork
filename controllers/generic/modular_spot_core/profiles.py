"""Build disabled native pair-controller compositions and their candle manifest."""
import importlib
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .candle_data import SCHEMA_VERSION, Dataset
from .config import PolicyModel
from .universe import pairs

OWNER_PATTERN = re.compile(r'^modular_spot_[A-Za-z0-9_-]+$')


def _unique(*groups):
    seen = {}
    for group in groups:
        for item in group:
            seen.setdefault(item, None)
    return tuple(seen)


class ProgramProfile(PolicyModel):
    name: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    connector: str = Field(pattern=r'^[a-z0-9_]{1,80}$')
    symbols: tuple[str, ...]
    observation_symbols: tuple[str, ...] = ()
    benchmarks: tuple[str, ...] = ()
    native_owners: tuple[str, ...] = ()
    mode: Literal['fixed', 'impulse', 'sell_only']
    quote_budget: Decimal = Field(gt=0)
    max_positions: int = Field(ge=1, le=1000, strict=True)
    history: int = Field(default=300, ge=61, le=3000, strict=True)
    data_origin: str = 'http://127.0.0.1:8100'
    token_file: str = '/run/secrets/modular_candle_reader'
    account: str = Field(default='unassigned', pattern=r'^[A-Za-z0-9_-]{1,80}$')
    program_revision: str = Field(default='0', pattern=r'^[A-Za-z0-9._-]{1,64}$')
    policy_revision: str = Field(default='0', pattern=r'^[A-Za-z0-9._-]{1,64}$')

    @model_validator(mode='after')
    def valid_program(self):
        pairs(self.symbols)
        pairs(self.observation_symbols)
        pairs(self.benchmarks)
        if not self.symbols or len({symbol.split('-')[1] for symbol in self.symbols}) != 1:
            raise ValueError('nonempty, single-quote execution universe required')
        bases = tuple(symbol.split('-')[0] for symbol in self.symbols)
        if len(set(bases)) != len(bases):
            raise ValueError('duplicate base-asset concentration in execution universe')
        if self.max_positions > len(self.symbols):
            raise ValueError('position count exceeds the admitted execution universe')
        if self.observation_symbols and not set(self.symbols) <= set(self.observation_symbols):
            raise ValueError('execution symbols are missing from observation universe')
        if self.native_owners:
            if len(self.native_owners) != len(self.symbols):
                raise ValueError('native owner references must match execution symbols')
            if len(set(self.native_owners)) != len(self.native_owners):
                raise ValueError('conflicting owner IDs')
            if any(OWNER_PATTERN.fullmatch(owner) is None for owner in self.native_owners):
                raise ValueError('invalid native owner reference')
        from .candle_transport import validate_origin
        validate_origin(self.data_origin)
        if not self.token_file.startswith('/'):
            raise ValueError('private token-file path must be absolute')
        for symbol in _unique(self.symbols, self.observation_symbols, self.benchmarks):
            Dataset(connector=self.connector, trading_pair=symbol, interval='1m', history=self.history)
        return self


def _controller(profile: ProgramProfile, symbol: str, owner: str, observed, pair_budget, *, acquisition: bool):
    return {
        'id': owner,
        'controller_name': 'modular_spot', 'controller_type': 'generic',
        'connector_name': profile.connector, 'trading_pair': symbol,
        'execution_enabled': False, 'acquisition_enabled': acquisition,
        'initial_positions': [], 'interval': '1m', 'max_records': profile.history,
        'total_amount_quote': str(pair_budget),
        'account': profile.account, 'program_revision': profile.program_revision,
        'policy_revision': profile.policy_revision,
        'budget': {'group': profile.name, 'max_quote': str(profile.quote_budget), 'max_assets': profile.max_positions},
        'signal': {'kind': 'breakout' if profile.mode == 'impulse' else 'rsi_rebound'},
        'acquisition': {'fractions': ['1']},
        'data': {'mode': 'shared', 'origin': profile.data_origin, 'token_file': profile.token_file,
                 'symbols': list(observed)},
        'universe': {'mode': profile.mode, 'symbols': list(profile.symbols),
                     'benchmarks': list(profile.benchmarks),
                     'observe_all': profile.mode == 'impulse',
                     'max_candidates': profile.max_positions},
    }


def build_profile(profile: ProgramProfile, *, managed=(), pending=()) -> dict:
    management = _unique(pairs(managed), pairs(pending))
    observed = _unique(profile.symbols, profile.observation_symbols, profile.benchmarks, management)
    pair_budget = profile.quote_budget / profile.max_positions
    owners = profile.native_owners or tuple(
        f'modular_spot_{profile.name}_{symbol.replace("-", "_")}' for symbol in profile.symbols)
    controllers = []
    selected = set(profile.symbols)
    for symbol, owner in zip(profile.symbols, owners):
        controllers.append(_controller(profile, symbol, owner, observed, pair_budget,
                                       acquisition=profile.mode != 'sell_only'))
    for symbol in management:
        if symbol in selected:
            continue
        Dataset(connector=profile.connector, trading_pair=symbol, interval='1m', history=profile.history)
        owner = f'modular_spot_{profile.name}_{symbol.replace("-", "_")}'
        controllers.append(_controller(profile, symbol, owner, observed, pair_budget, acquisition=False))
    ids = [item['id'] for item in controllers]
    if len(set(ids)) != len(ids):
        raise ValueError('conflicting owner IDs')
    datasets = tuple(Dataset(connector=profile.connector, trading_pair=symbol, interval='1m', history=profile.history)
                     for symbol in observed)
    bundle = {'schema_version': SCHEMA_VERSION, 'controllers': controllers,
              'candle_manifest': {'schema_version': SCHEMA_VERSION,
                                  'datasets': [d.model_dump(mode='json') for d in datasets]},
              'account': profile.account, 'program_revision': profile.program_revision,
              'policy_revision': profile.policy_revision,
              'inventory_adoption_supported': False, 'execution_authorized': False}
    _validate_controller_payloads(bundle['controllers'])
    return bundle


def _validate_controller_payloads(controllers):
    # Isolation forbids ImportFrom of the entry module; the config class must stay there for native discovery.
    cls = importlib.import_module('controllers.generic.modular_spot').ModularSpotControllerConfig
    for payload in controllers:
        cls.model_validate(payload)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Export disabled modular profiles; never start or deploy a bot')
    parser.add_argument('--profile', required=True)
    parser.add_argument('--output', required=True, help='A new, nonexisting directory outside running configuration')
    args = parser.parse_args()
    source = Path(args.profile)
    if source.stat().st_size > 1024 * 1024:
        raise ValueError('profile exceeds size bound')
    bundle = build_profile(ProgramProfile.model_validate_json(source.read_text()))
    output = Path(args.output)
    if not output.is_absolute() or any(parent.is_symlink() for parent in output.parents):
        raise ValueError('export requires an absolute non-symlink destination')
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    for payload in bundle['controllers']:
        (output / (payload['id'] + '.yml')).write_text(json.dumps(payload, indent=2) + '\n')
    (output / 'candle-manifest.json').write_text(json.dumps(bundle['candle_manifest'], indent=2) + '\n')
    (output / 'export-receipt.json').write_text(json.dumps({'execution_authorized': False,
                                                            'controller_ids': [p['id'] for p in bundle['controllers']], 'inventory_adoption_supported': False}, indent=2) + '\n')


if __name__ == '__main__':
    main()
