import json
import subprocess
import sys

import controllers.generic.mean_reversion_v2_core.exit_a.admission as exit_a_admission
from controllers.generic.mean_reversion_v2_core.exit_a import contracts
from hummingbot.strategy_v2.components import decision_protocol, exit_policy, recipe_contract


def test_canonical_types_are_reused_not_mirrored():
    assert contracts.NativeExitView is exit_policy.NativeExitView
    assert contracts.ExitProposal is exit_policy.ExitProposal
    assert contracts.DecisionTrace is decision_protocol.DecisionTrace
    assert contracts.RecipeBinding is recipe_contract.RecipeBinding


def test_new_package_import_has_no_legacy_controller_or_service_side_effect():
    code = """
import json
import sys
import threading
before = {t.ident for t in threading.enumerate()}
def guard(event, args):
    if event in ('socket.connect', 'subprocess.Popen', 'os.system'):
        raise AssertionError(event)
sys.addaudithook(guard)
import controllers.generic.mean_reversion_v2_core.exit_a.policy
import controllers.generic.mean_reversion_v2_core.exit_a.inference
import controllers.generic.mean_reversion_v2_core.exit_a.recipe
import controllers.generic.mean_reversion_v2_core.exit_a.lifecycle
assert before == {t.ident for t in threading.enumerate()}
assert not any(n.startswith('controllers.directional_trading') for n in sys.modules)
print(json.dumps({'legacy_imports': False, 'new_threads': False}))
"""
    run = subprocess.run([sys.executable, '-c', code], check=True, capture_output=True, text=True, timeout=30)
    assert json.loads(run.stdout) == {'legacy_imports': False, 'new_threads': False}


def test_admission_import_does_not_load_legacy_controllers():
    assert exit_a_admission.admit.__name__ == 'admit'
    assert not any(name.startswith('controllers.directional_trading') for name in sys.modules)
