import ast
import subprocess
import sys
from pathlib import Path

from controllers.generic.momentum_fusion_v2_core import features

ROOT=Path(features.__file__).resolve().parent


def test_policy_imports_do_not_reach_legacy_or_execution_modules():
    forbidden=('controllers.directional_trading', 'controllers.generic.modular_spot_core',
               'controllers.generic.rsi_modular', 'hummingbot.connector',
               'hummingbot.strategy_v2.executors', 'sqlite3', 'requests', 'aiohttp')
    for source in ROOT.glob('*.py'):
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node,ast.Import): names=[a.name for a in node.names]
            elif isinstance(node,ast.ImportFrom): names=[node.module or '']
            else: continue
            assert not any(name.startswith(forbidden) for name in names), (source,names)


def test_policy_functions_do_not_read_wall_clock_or_emit_orders():
    forbidden={'time','monotonic','perf_counter','buy','sell','create_order','CreateExecutorAction',
               'PositionExecutorConfig','connect','start','get_balance'}
    for source in ROOT.glob('*.py'):
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node,ast.Call):
                called=node.func.attr if isinstance(node.func,ast.Attribute) else getattr(node.func,'id','')
                assert called not in forbidden, (source,called)


def test_clean_process_import_starts_no_worker_or_legacy_controller():
    script='''import sys, threading
before = set(threading.enumerate())
from controllers.generic.momentum_fusion_v2_core import features, recipe, setups
assert set(threading.enumerate()) == before
assert not any(k.startswith('controllers.directional_trading') for k in sys.modules)
assert not any(k.startswith('hummingbot.strategy_v2.executors') for k in sys.modules)
print('isolated_import_ok')
'''
    result=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stderr
    assert result.stdout.strip()=='isolated_import_ok'
