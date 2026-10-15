"""The V4 seal tool writes exactly the binding the config validator verifies, for paper only."""
from test.meridian_v4 import test_meridian_v4_contract as v4, test_meridian_v4_d2h_contract as d2h

import pytest
import yaml

from controllers.generic.meridian_v4 import MeridianV4Config
from controllers.generic.meridian_v4_d2h import MeridianV4D2hConfig
from scripts.meridian_v4_seal import seal


@pytest.mark.parametrize('factory, cls', [(v4.config, MeridianV4Config), (d2h.config, MeridianV4D2hConfig)])
def test_seal_writes_an_enabled_verified_paper_file(tmp_path, factory, cls):
    source, output = tmp_path / 'unsealed.yml', tmp_path / 'sealed.yml'
    unsealed = factory()
    source.write_text(yaml.safe_dump(unsealed.model_dump(mode='json'), sort_keys=False))
    binding = seal(source, output)
    loaded = cls.model_validate(yaml.safe_load(output.read_text()))
    assert loaded.execution_enabled and loaded.recipe_binding.model_dump(mode='json') == binding
    assert loaded.recipe_binding == unsealed.seal()
    assert oct(output.stat().st_mode & 0o777) == '0o600'
    with pytest.raises(FileExistsError):
        seal(source, output)  # never overwrites
    with pytest.raises(SystemExit, match='unsealed'):
        seal(output, tmp_path / 'again.yml')


def test_seal_refuses_other_controllers_and_shadow(tmp_path):
    source = tmp_path / 'x.yml'
    source.write_text(yaml.safe_dump({'controller_name': 'meridian_v3_unified'}))
    with pytest.raises(SystemExit, match='meridian_v4'):
        seal(source, tmp_path / 'y.yml')
    source.write_text(yaml.safe_dump(v4.config(mode='shadow').model_dump(mode='json'), sort_keys=False))
    with pytest.raises(SystemExit, match='paper'):
        seal(source, tmp_path / 'z.yml')
