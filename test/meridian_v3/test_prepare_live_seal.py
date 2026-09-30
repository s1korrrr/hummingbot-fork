"""Preparing a live recipe locally never enables its execution."""
from test.meridian_v3.test_unified_runtime import unified

import pytest
import yaml

from scripts.meridian_v3_seal import seal


def test_live_preparation_preserves_disabled_execution_and_unbound_capital(tmp_path):
    source, target = tmp_path / 'unsealed.yml', tmp_path / 'prepared.yml'
    source.write_text(yaml.safe_dump(unified('live').model_dump(mode='json')))
    binding = seal(source, target, prepare_live=True)
    saved = yaml.safe_load(target.read_text())
    assert saved['execution_enabled'] is False
    assert saved['live_capital'] is None
    assert saved['recipe_binding'] == binding
    assert saved['capital_stage'] == 'full'
    assert target.stat().st_mode & 0o077 == 0
    with pytest.raises(FileExistsError):
        seal(source, target, prepare_live=True)


def test_live_file_requires_explicit_preparation_mode(tmp_path):
    source = tmp_path / 'unsealed.yml'
    source.write_text(yaml.safe_dump(unified('live').model_dump(mode='json')))
    with pytest.raises(SystemExit, match='prepare-live'):
        seal(source, tmp_path / 'sealed.yml')


def test_prepare_live_cannot_be_applied_to_paper_mode(tmp_path):
    source = tmp_path / 'unsealed.yml'
    source.write_text(yaml.safe_dump(unified().model_dump(mode='json')))
    with pytest.raises(SystemExit, match='live file'):
        seal(source, tmp_path / 'sealed.yml', prepare_live=True)
