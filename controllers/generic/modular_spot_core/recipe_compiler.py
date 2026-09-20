"""Render recipes through #55 profiles; export only after genuine native config validation."""
import argparse
import importlib
import json
import os
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from hummingbot.strategy_v2.components.strict_json import strict_json

from .config import SignalPolicy
from .domain import PolicyError
from .native_recipe import NativePolicyRecipe, render_native_recipe
from .profiles import ProgramProfile, build_profile
from .recipe_contract import RecipeBinding, StrategyRecipe, canonical_json, configuration_digest, digest, source_digest


def parse_recipe(payload):
    cls = NativePolicyRecipe if payload.get('family') in {'ok_rsi', 'rsi_v5'} else StrategyRecipe
    return cls.model_validate(payload)


def render_recipe(recipe: StrategyRecipe | NativePolicyRecipe) -> dict:
    if isinstance(recipe, NativePolicyRecipe):
        return render_native_recipe(recipe)
    recipe = StrategyRecipe.model_validate(recipe.model_dump(mode='json'))
    profile = ProgramProfile(
        name=recipe.name, connector=recipe.connector, symbols=recipe.symbols,
        observation_symbols=recipe.observation_symbols,
        benchmarks=recipe.benchmarks, native_owners=recipe.native_owners, account=recipe.account,
        program_revision=recipe.program_revision, policy_revision=recipe.policy_revision,
        mode='fixed' if recipe.family == 'rebound' else recipe.family,
        quote_budget=recipe.quote_budget, max_positions=recipe.max_positions,
        history=recipe.history, data_transport=recipe.data_transport,
        data_origin=recipe.data_origin, token_file=recipe.token_file,
    )
    bundle = build_profile(profile)
    for config in bundle['controllers']:
        # The native schema requires a signal even when acquisition is forbidden.
        config['signal'] = (recipe.signal or SignalPolicy()).model_dump(mode='json')
        config['acquisition'] = recipe.acquisition.model_dump(mode='json')
        config['trailing'] = recipe.trailing.model_dump(mode='json')
        config['risk'] = recipe.risk.model_dump(mode='json')
    return {**bundle, 'recipe': recipe.model_dump(mode='json'), 'recipe_sha256': recipe.digest,
            'receipt_kind': 'modular_spot_composition_v2', 'source_scope': 'controllers+strategy_components',
            'native_validation': 'NOT_RUN', 'native_runtime_validation': 'NOT_RUN',
            'capital_authority': 'NOT_PROVEN', 'account_topology': recipe.account_topology,
            'sell_only_handoff': 'UNSUPPORTED' if not recipe.acquisition_allowed else 'NOT_APPLICABLE'}


def bind_native_bundle(recipe: StrategyRecipe | NativePolicyRecipe) -> dict:
    if isinstance(recipe, NativePolicyRecipe):
        from controllers.generic import rsi_modular as native
    else:
        native = importlib.import_module('controllers.generic.modular_spot')
    config_type = native.get_config_class(recipe.family) if isinstance(recipe, NativePolicyRecipe) else getattr(native, native.CONFIG_CLASS)
    bundle = render_recipe(recipe)
    sources = native.source_files(recipe.family) if isinstance(recipe, NativePolicyRecipe) else native.source_files()
    source_hash = source_digest(native.source_root(), sources)
    controllers = []
    for payload in bundle['controllers']:
        config = config_type.model_validate(payload)
        normalized = config.model_dump(mode='json')
        normalized['recipe_binding'] = RecipeBinding(
            recipe_id=recipe.name, recipe_sha256=recipe.digest, source_sha256=source_hash,
            configuration_sha256=configuration_digest(normalized),
        ).model_dump(mode='json')
        sealed = config_type.model_validate(normalized)
        if sealed.execution_enabled or sealed.initial_positions:
            raise PolicyError('compiler must never activate execution or adopt inventory')
        controllers.append(sealed.model_dump(mode='json'))
    bundle.update(controllers=controllers, source_sha256=source_hash,
                  native_validation='CONFIGURATION_PASSED')
    return bundle


def write_bundle(bundle: dict, output: Path) -> None:
    output = Path(output)
    if (not output.is_absolute() or output.exists()
            or any(path.is_symlink() for path in (output, *output.parents))
            or not output.parent.is_dir()):
        raise ValueError('export requires a new absolute directory under an existing non-symlink parent')
    if bundle.get('native_validation') != 'CONFIGURATION_PASSED':
        raise ValueError('unvalidated recipe bundles cannot be exported as native configurations')
    recipe = parse_recipe(bundle['recipe'])
    # A caller-supplied PASSED string is not validation evidence.
    expected = bind_native_bundle(recipe)
    if canonical_json(expected) != canonical_json(bundle):
        raise PolicyError('bundle changed after native validation')
    files = {'recipe.json': recipe.model_dump(mode='json'),
             'candle-manifest.json': bundle['candle_manifest']}
    for config in bundle['controllers']:
        name = config['id'] + '.yml'
        if Path(name).name != name or name in files:
            raise PolicyError('unsafe or duplicated controller export name')
        files[name] = config
    encoded = {name: (json.dumps(value, indent=2, allow_nan=False) + '\n').encode() for name, value in files.items()}
    receipt = {key: deepcopy(value) for key, value in bundle.items() if key not in {'controllers', 'candle_manifest'}}
    receipt['files'] = {name: sha256(data).hexdigest() for name, data in sorted(encoded.items())}
    receipt['bundle_sha256'] = digest(bundle)
    encoded['export-receipt.json'] = (json.dumps(receipt, indent=2, allow_nan=False) + '\n').encode()
    output.mkdir(mode=0o700, exist_ok=False)
    directory = os.open(output, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        marker = os.open('INCOMPLETE', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        os.close(marker)
        for name, data in encoded.items():
            descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        os.unlink('INCOMPLETE', dir_fd=directory)
        os.fsync(directory)
    finally:
        os.close(directory)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Inspect or export disabled modular recipes in the native Hummingbot environment; never start a bot')
    parser.add_argument('command', choices=('inspect', 'export'))
    parser.add_argument('recipe', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    with args.recipe.open('rb') as stream:
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError('recipe exceeds one MiB')
    recipe = parse_recipe(strict_json(data.decode('utf-8')))
    if args.command == 'inspect':
        if args.output is not None:
            parser.error('inspect is read-only and does not accept an output directory')
        print(json.dumps(render_recipe(recipe), indent=2, allow_nan=False))
        return
    if args.output is None:
        parser.error('export requires --output')
    bundle = bind_native_bundle(recipe)
    write_bundle(bundle, args.output)
    print(json.dumps({'output': str(args.output), 'recipe_sha256': recipe.digest,
                      'native_validation': 'CONFIGURATION_PASSED', 'execution_authorized': False}))


if __name__ == '__main__':
    main()
