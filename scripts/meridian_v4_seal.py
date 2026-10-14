#!/usr/bin/env python
"""Seal one V4 or V4 D2h paper controller file against this exact engine source.

Reads an unsealed paper file (execution disabled, no binding), validates it with its exact
config class (`meridian_v4` or `meridian_v4_d2h`) and writes a new file with the recipe binding
and execution enabled. Paper only: V4 has no live mode. It never overwrites a file, starts a
connector or contacts a venue.
"""
import argparse
import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from controllers.generic.meridian_v4 import MeridianV4Config  # noqa: E402
from controllers.generic.meridian_v4_d2h import MeridianV4D2hConfig  # noqa: E402

CLASSES = {'meridian_v4': MeridianV4Config, 'meridian_v4_d2h': MeridianV4D2hConfig}


def seal(source: Path, output: Path) -> dict:
    values = yaml.safe_load(source.read_text())
    cls = CLASSES.get(values.get('controller_name')) if isinstance(values, dict) else None
    if cls is None:
        raise SystemExit('a meridian_v4 or meridian_v4_d2h controller file is required')
    if values.get('execution_enabled') or values.get('recipe_binding') is not None:
        raise SystemExit('seal an unsealed, execution-disabled file')
    config = cls.model_validate(values)
    if config.execution_mode != 'paper':
        raise SystemExit('only paper files are sealed; V4 has no live mode')
    sealed = cls.model_validate({**config.model_dump(mode='json'), 'execution_enabled': True,
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
