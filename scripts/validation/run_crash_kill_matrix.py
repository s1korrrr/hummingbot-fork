#!/usr/bin/env python3
"""Real process-kill matrix for the bounded V2 acquisition chain, offline.

For every kill phase this runs two disposable, network-disabled containers on
the supplied immutable image with the candidate modules overlaid read-only:

1. ``drive``: runs the offline paper chain and hard-kills the interpreter
   (``os._exit``) at the boundary named by the phase;
2. ``verify``: a fresh process restores from the same database directory and
   reports whether the restart holds without a BUY or lost state.

It never mounts a stack directory, exchange configuration or native database.
"""
import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_acquisition_chain_native import MODULES  # noqa: E402

DRIVER = 'test/acquisition_chain_v2/crash_kill_driver.py'
PHASES = ('replacement_accepted_before_sync', 'first_fill_before_sync', 'closed_chain_between_steps')
KILL_CODE = 137


def container(root, image, work, mode, phase):
    command = ['docker', 'run', '--rm', '--pull', 'never', '--network', 'none',
               '--entrypoint', 'python', '-e', 'PYTHONDONTWRITEBYTECODE=1',
               '-e', 'PYTHONPATH=/home/hummingbot',
               '-v', f'{root / "test"}:/home/hummingbot/test:ro', '-v', f'{work}:/work']
    for module in MODULES:
        command.extend(['-v', f'{root / module}:/home/hummingbot/{module}:ro'])
    command.extend([image, '/home/hummingbot/' + DRIVER, mode, '--db', '/work/chain.sqlite', '--phase', phase])
    result = subprocess.run(command, capture_output=True, text=True)
    lines = [line[len('KILL_MATRIX '):] for line in result.stdout.splitlines() if line.startswith('KILL_MATRIX ')]
    payload = json.loads(lines[-1]) if lines else None
    return result.returncode, payload, result.stdout[-2000:] + result.stderr[-2000:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True, help='Existing immutable sha256 Docker image ID')
    parser.add_argument('--phase', action='append', choices=PHASES, help='Subset of phases (default: all)')
    args = parser.parse_args()
    if not args.image.startswith('sha256:') or len(args.image) != 71:
        parser.error('--image must be an immutable sha256 image ID')
    root = Path(__file__).resolve().parents[2]
    failures = 0
    for phase in args.phase or PHASES:
        with tempfile.TemporaryDirectory(prefix='kill-matrix-') as work:
            work = Path(work)
            work.chmod(0o777)
            code, payload, tail = container(root, args.image, work, 'drive', phase)
            if code != KILL_CODE or payload is None or payload.get('killed_after') is None:
                failures += 1
                print(f'[{phase}] DRIVE did not reach the kill point (exit {code})\n{tail}')
                continue
            print(f'[{phase}] killed after {payload["killed_after"]} (exit {code})')
            code, payload, tail = container(root, args.image, work, 'verify', phase)
            if code != 0 or payload is None or not payload.get('ok'):
                failures += 1
                print(f'[{phase}] VERIFY FAILED (exit {code})')
                print(json.dumps(payload, indent=1, sort_keys=True) if payload else tail)
                continue
            print(f'[{phase}] verify ok: ' + ', '.join(sorted(payload['checks'])))
    print('kill matrix:', 'PASS' if not failures else f'{failures} phase(s) failed')
    raise SystemExit(1 if failures else 0)


if __name__ == '__main__':
    main()
