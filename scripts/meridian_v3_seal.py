#!/usr/bin/env python
"""Seal one Meridian unified controller file against this exact engine source.

Reads an unsealed file (execution disabled), validates it, and writes a new file
with `execution_enabled: true` and the recipe binding. It never overwrites a file,
starts a connector or contacts a venue. Live files are refused: live execution
additionally needs an exclusive capital-domain binding reviewed by the operator.
"""
import argparse
import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig  # noqa: E402


def seal(source: Path, output: Path) -> dict:
    values = yaml.safe_load(source.read_text())
    if not isinstance(values, dict) or values.get('controller_name') != 'meridian_v3_unified':
        raise SystemExit('a meridian_v3_unified controller file is required')
    if values.get('execution_enabled') or values.get('recipe_binding') is not None:
        raise SystemExit('seal an unsealed, execution-disabled file')
    config = MeridianUnifiedConfig.model_validate(values)
    if config.execution_mode != 'paper':
        raise SystemExit('only paper files are sealed here; live needs a reviewed capital-domain binding')
    sealed = MeridianUnifiedConfig.model_validate({**config.model_dump(mode='json'), 'execution_enabled': True,
                                                   'recipe_binding': config.seal().model_dump(mode='json')})
    payload = sealed.model_dump(mode='json')
    with os.fdopen(os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as stream:
        yaml.safe_dump(payload, stream, sort_keys=False)
    return payload['recipe_binding']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    print(yaml.safe_dump({'recipe_binding': seal(args.source, args.output)}, sort_keys=False), end='')


if __name__ == '__main__':
    main()
