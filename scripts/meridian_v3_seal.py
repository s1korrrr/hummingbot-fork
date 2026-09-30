#!/usr/bin/env python
"""Seal one Meridian unified controller file against this exact engine source.

Reads an unsealed file (execution disabled), validates it, and writes a new file
with the recipe binding. Paper files are enabled as before. --prepare-live leaves
execution disabled, for source review only; it does not grant capital authority.
It never overwrites a file, starts a connector or contacts a venue. Activating a
live file still requires the separately reviewed bounded capital binding and
source/state/account-bound admission.
"""
import argparse
import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig  # noqa: E402


def seal(source: Path, output: Path, *, prepare_live: bool = False) -> dict:
    values = yaml.safe_load(source.read_text())
    if not isinstance(values, dict) or values.get('controller_name') != 'meridian_v3_unified':
        raise SystemExit('a meridian_v3_unified controller file is required')
    if values.get('execution_enabled') or values.get('recipe_binding') is not None:
        raise SystemExit('seal an unsealed, execution-disabled file')
    config = MeridianUnifiedConfig.model_validate(values)
    if prepare_live and config.execution_mode != 'live':
        raise SystemExit('--prepare-live requires a live file and leaves execution disabled')
    if not prepare_live and config.execution_mode != 'paper':
        raise SystemExit('only paper is enabled by this tool; live preparation requires --prepare-live and leaves execution disabled')
    sealed = MeridianUnifiedConfig.model_validate({**config.model_dump(mode='json'), 'execution_enabled': not prepare_live,
                                                   'recipe_binding': config.seal().model_dump(mode='json')})
    payload = sealed.model_dump(mode='json')
    with os.fdopen(os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as stream:
        yaml.safe_dump(payload, stream, sort_keys=False)
    return payload['recipe_binding']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--prepare-live', action='store_true',
                        help='seal a live recipe for review, preserving disabled execution')
    args = parser.parse_args()
    print(yaml.safe_dump(
        {'recipe_binding': seal(args.source, args.output, prepare_live=args.prepare_live)},
        sort_keys=False), end='')


if __name__ == '__main__':
    main()
