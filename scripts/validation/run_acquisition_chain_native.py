#!/usr/bin/env python3
"""Test scoped acquisition changes against a specified native image, offline.

This never mounts a stack directory, exchange configuration or native database.
It cannot qualify a production cutover: it checks only the supplied image with
these explicit source overlays and the candidate's test directory.
"""
import argparse
import hashlib
import subprocess
from pathlib import Path

MODULES = (
    'hummingbot/core/data_type/in_flight_order.py',
    'controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py',
    'controllers/generic/modular_spot_core/native_composition.py',
    'controllers/generic/modular_spot_core/native_funding.py',
    'controllers/generic/modular_spot_core/native_funding_transition.py',
    'hummingbot/strategy_v2/executors/acquisition_chain.py',
    'hummingbot/strategy_v2/executors/planned_position_resume.py',
    'hummingbot/strategy_v2/executors/executor_orchestrator.py',
    'hummingbot/strategy_v2/executors/position_executor/position_executor.py',
    'hummingbot/strategy_v2/executors/position_executor/data_types.py',
    'hummingbot/data_feed/market_data_provider.py',
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True, help='Existing immutable sha256 Docker image ID')
    parser.add_argument('pytest_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.image.startswith('sha256:') or len(args.image) != 71:
        parser.error('--image must be an immutable sha256 image ID')
    root = Path(__file__).resolve().parents[2]
    command = ['docker', 'run', '--rm', '--pull', 'never', '--network', 'none',
               '--entrypoint', 'python', '-e', 'PYTHONDONTWRITEBYTECODE=1',
               '-e', 'PYTHONPATH=/home/hummingbot',
               '-v', f'{root / "test"}:/home/hummingbot/test:ro']
    print(f'Native image: {args.image}', flush=True)
    for module in MODULES:
        path = root / module
        print(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {module}', flush=True)
        command.extend(['-v', f'{path}:/home/hummingbot/{module}:ro'])
    tests = args.pytest_args
    if tests and tests[0] == '--':
        tests = tests[1:]
    command.extend([args.image, '-m', 'pytest', '-p', 'no:cacheprovider',
                    *(tests or ['test/acquisition_chain_v2', '-q'])])
    raise SystemExit(subprocess.call(command))


if __name__ == '__main__':
    main()
