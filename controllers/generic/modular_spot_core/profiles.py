"""Build disabled native pair-controller compositions and their candle manifest."""
import json
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .candle_data import Dataset
from .config import PolicyModel
from .universe import pairs


class ProgramProfile(PolicyModel):
    name: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    connector: str = Field(pattern=r'^[a-z0-9_]{1,80}$')
    symbols: tuple[str, ...]
    observation_symbols: tuple[str, ...] = ()
    mode: Literal['fixed', 'impulse', 'sell_only']
    quote_budget: Decimal = Field(gt=0)
    max_positions: int = Field(ge=1, le=1000, strict=True)
    history: int = Field(default=300, ge=61, le=3000, strict=True)
    data_origin: str = 'http://127.0.0.1:8100'
    token_file: str = '/run/secrets/modular_candle_reader'

    @model_validator(mode='after')
    def valid_program(self):
        pairs(self.symbols)
        pairs(self.observation_symbols)
        if not self.symbols or len({symbol.split('-')[1] for symbol in self.symbols}) != 1:
            raise ValueError('nonempty, single-quote execution universe required')
        if self.max_positions > len(self.symbols):
            raise ValueError('position count exceeds the admitted execution universe')
        if self.observation_symbols and not set(self.symbols) <= set(self.observation_symbols):
            raise ValueError('execution symbols are missing from observation universe')
        from .candle_transport import validate_origin
        validate_origin(self.data_origin)
        if not self.token_file.startswith('/'):
            raise ValueError('private token-file path must be absolute')
        return self


def build_profile(profile: ProgramProfile) -> dict:
    observed = profile.observation_symbols or profile.symbols
    controllers = []
    for symbol in profile.symbols:
        controllers.append({
            'id': f'modular_spot_{profile.name}_{symbol.replace("-", "_")}',
            'controller_name': 'modular_spot', 'controller_type': 'generic',
            'connector_name': profile.connector, 'trading_pair': symbol,
            'execution_enabled': False, 'acquisition_enabled': profile.mode != 'sell_only',
            'initial_positions': [], 'interval': '1m', 'max_records': profile.history,
            'total_amount_quote': str(profile.quote_budget / profile.max_positions),
            'budget': {'group': profile.name, 'max_quote': str(profile.quote_budget), 'max_assets': profile.max_positions},
            'signal': {'kind': 'breakout' if profile.mode == 'impulse' else 'rsi_rebound'},
            'acquisition': {'fractions': ['1']},
            'data': {'mode': 'shared', 'origin': profile.data_origin, 'token_file': profile.token_file,
                     'symbols': list(observed) if profile.mode == 'impulse' else []},
            'universe': {'mode': profile.mode, 'symbols': list(profile.symbols),
                         'observe_all': profile.mode == 'impulse',
                         'max_candidates': profile.max_positions if profile.mode == 'impulse' else len(profile.symbols)},
        })
    datasets = tuple(Dataset(connector=profile.connector, trading_pair=symbol, interval='1m', history=profile.history)
                     for symbol in observed)
    return {'schema_version': 1, 'controllers': controllers,
            'candle_manifest': {'schema_version': 1, 'datasets': [d.model_dump(mode='json') for d in datasets]},
            'inventory_adoption_supported': False, 'execution_authorized': False}


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
    from controllers.generic.modular_spot import ModularSpotControllerConfig
    for payload in bundle['controllers']:
        ModularSpotControllerConfig.model_validate(payload)
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
