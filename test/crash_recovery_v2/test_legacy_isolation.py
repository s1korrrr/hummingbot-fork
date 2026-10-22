import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from hummingbot.strategy_v2.components.program_budget import Reservation

from .test_native_admission import FROZEN, host_config

ROOT = Path(__file__).resolve().parents[2]


def test_importing_the_v2_host_loads_no_frozen_controller_family():
    script = (
        'import sys\n'
        'import controllers.generic.crash_recovery_v2\n'
        f'frozen = {FROZEN!r}\n'
        'hits = []\n'
        'for name in frozen:\n'
        '    hits.extend(n for n in sys.modules\n'
        '                if n == "controllers.generic." + name\n'
        '                or n == "controllers.directional_trading." + name\n'
        '                or n.startswith("controllers.generic." + name + ".")\n'
        '                or n.startswith("controllers.directional_trading." + name + "."))\n'
        'assert not hits, hits\n'
    )
    result = subprocess.run([sys.executable, '-B', '-c', script], capture_output=True, text=True,
                            env={**os.environ, 'PYTHONPATH': str(ROOT)}, timeout=300)
    assert result.returncode == 0, result.stderr


def test_host_id_cannot_occupy_a_modular_spot_budget_slot():
    cfg = host_config()
    assert cfg.id.startswith('crash_recovery_v2_')
    claim = dict(executor_id='lot_1', trading_pair='BNB-USDC', reserved_quote='1', reserved_base='1',
                 account='main', connector='okx', program='v2', slot='BNB', expires_at='0')
    with pytest.raises(ValidationError):
        Reservation(controller_id=cfg.id, **claim)
    with pytest.raises(ValidationError):
        host_config(id='modular_spot_lot1')
    assert Reservation(controller_id='modular_spot_lot1', **claim).controller_id == 'modular_spot_lot1'
