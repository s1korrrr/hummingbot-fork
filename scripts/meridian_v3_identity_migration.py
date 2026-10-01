#!/usr/bin/env python
"""Plan or apply the identity migration of one Meridian unified owner's recorder state.

Operator maintenance only; never run by the controller. After a code-only image
upgrade the owner holds new acquisitions (IDENTITY_MIGRATION_REQUIRED) and keeps
its protective bag stops. Run `plan` with the new image, the owner's resealed
controller file and its recorder database, review the plan, then run `apply`
with the plan's transition id. Any behavioural configuration change is refused,
except an explicit `capital_stage` change whose every stage-derived value is the
stage table's; the plan lists those values (`stage_changes`) and is refused while
reserved exposure or open tranches exceed the target stage.

    meridian_v3_identity_migration.py plan  --config sealed.yml --database data/x.sqlite --recorder-name conf.yml
    meridian_v3_identity_migration.py apply --config sealed.yml --database data/x.sqlite --recorder-name conf.yml \
        --transition <id>

`--recorder-name` is the recorder's config file name (the strategy file the
owner was started with). A wrong name finds no identity witness and is refused.
Take a WAL-inclusive backup of the database first. The migration never deletes a
row: every row it changes is archived in its receipt. It contacts no venue and
starts no connector.
"""
import argparse
import json
import sys
from pathlib import Path

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig  # noqa: E402
from controllers.generic.meridian_v3_unified_core.migration import (  # noqa: E402
    apply_identity_migration,
    plan_identity_migration,
)


def run(action, config_path: Path, database: Path, recorder_name: str, transition=None) -> dict:
    values = yaml.safe_load(Path(config_path).read_text())
    if not isinstance(values, dict) or values.get('controller_name') != 'meridian_v3_unified':
        raise SystemExit('a meridian_v3_unified controller file is required')
    config = MeridianUnifiedConfig.model_validate(values)
    database = Path(database)
    if not database.is_file():
        raise SystemExit(f'recorder database does not exist: {database}')
    engine = create_engine('sqlite:///' + str(database), connect_args={'timeout': 30})
    try:
        sessions = sessionmaker(bind=engine)
        if action == 'plan':
            return plan_identity_migration(config, sessions, recorder_name)
        if not transition:
            raise SystemExit('apply requires the reviewed --transition id')
        return apply_identity_migration(config, sessions, recorder_name, transition)
    finally:
        engine.dispose()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('action', choices=('plan', 'apply'))
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--recorder-name', required=True)
    parser.add_argument('--transition')
    args = parser.parse_args(argv)
    result = run(args.action, args.config, args.database, args.recorder_name, args.transition)
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


if __name__ == '__main__':
    main()
