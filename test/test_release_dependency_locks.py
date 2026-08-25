import hashlib
import json
from pathlib import Path

import pytest

from scripts import validate_release_dependency_locks as lock_validator
from scripts.generate_release_dependency_locks import conda_specs_from_environment

ROOT = Path(__file__).resolve().parents[1]
LOCKS = ROOT / "setup/locks"
ENVIRONMENT = ROOT / "setup/environment.yml"
PIP_PACKAGES = ROOT / "setup/pip_packages.txt"


def _copy_lock_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    for artifact in LOCKS.iterdir():
        (lock_dir / artifact.name).write_bytes(artifact.read_bytes())
    environment = tmp_path / "environment.yml"
    pip_packages = tmp_path / "pip_packages.txt"
    environment.write_bytes(ENVIRONMENT.read_bytes())
    pip_packages.write_bytes(PIP_PACKAGES.read_bytes())
    return lock_dir, environment, pip_packages


def _refresh_artifact_fingerprint(lock_dir: Path, name: str) -> None:
    provenance_path = lock_dir / "provenance.json"
    provenance = json.loads(provenance_path.read_text())
    provenance["artifacts"][name] = hashlib.sha256((lock_dir / name).read_bytes()).hexdigest()
    provenance_path.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")


def test_conda_specs_from_environment_excludes_nested_pip_section(tmp_path: Path):
    environment = tmp_path / "environment.yml"
    environment.write_text("dependencies:\n  - python==3.13.15\n  - pip:\n    - injective-py==1.13.0\n")

    assert conda_specs_from_environment(environment) == ["python==3.13.15"]


def test_conda_specs_from_environment_excludes_channels_before_dependencies(tmp_path: Path):
    environment = tmp_path / "environment.yml"
    environment.write_text(
        "channels:\n  - conda-forge\n  - nodefaults\ndependencies:\n  - python==3.13.15\n  - pip:\n"
    )

    assert conda_specs_from_environment(environment) == ["python==3.13.15"]


def test_source_recipe_direct_pip_requirements_are_exactly_pinned():
    lock_validator._validate_source_pins(ENVIRONMENT, PIP_PACKAGES)


def test_committed_locks_validate_offline_for_both_release_targets():
    result = lock_validator.validate_lock_directory(LOCKS, ENVIRONMENT, PIP_PACKAGES)

    assert result.targets == ("linux-64", "linux-aarch64")
    assert result.python_versions == {"linux-64": "3.13.15", "linux-aarch64": "3.13.15"}


def test_validator_rejects_unsupported_target_in_provenance(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    provenance_path = lock_dir / "provenance.json"
    provenance = json.loads(provenance_path.read_text())
    provenance["targets"] = ["linux-64", "osx-arm64"]
    provenance_path.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")

    with pytest.raises(lock_validator.LockValidationError, match="unsupported target"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_foreign_conda_channel_url_before_fingerprint_check(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "linux-64.explicit.txt"
    lock_path.write_text(lock_path.read_text().replace("conda-forge", "defaults", 1))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="conda-forge"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_conda_subdir_mismatch_before_fingerprint_check(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "linux-aarch64.explicit.txt"
    lock_path.write_text(lock_path.read_text().replace("/linux-aarch64/", "/linux-64/", 1))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="subdir"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


@pytest.mark.parametrize("replacement", ["", "#sha256:bad", "#" + "A" * 64])
def test_validator_rejects_malformed_or_missing_conda_sha256(tmp_path: Path, replacement: str):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "linux-64.explicit.txt"
    original = lock_path.read_text()
    line = next(line for line in original.splitlines() if line.startswith("https://"))
    lock_path.write_text(original.replace(line, line.split("#", 1)[0] + replacement, 1))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="SHA-256"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_source_fingerprint_mismatch(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    environment.write_text(environment.read_text() + "# source drift\n")

    with pytest.raises(lock_validator.LockValidationError, match="source fingerprint mismatch"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_pip_version_drift_before_fingerprint_check(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "pip-direct.lock.txt"
    lock_path.write_text(lock_path.read_text().replace("injective-py==1.13.0", "injective-py==1.13.1"))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="pinned requirement"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_pip_hash_drift_before_fingerprint_check(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "pip-direct.lock.txt"
    lock_path.write_text(lock_path.read_text().replace("--hash=sha256:", "--hash=sha256:" + "0", 1))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="SHA-256"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_missing_target_lock(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    (lock_dir / "linux-aarch64.explicit.txt").unlink()

    with pytest.raises(lock_validator.LockValidationError, match="missing lock"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_tampered_lock_content_with_artifact_fingerprint(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "linux-64.explicit.txt"
    lock_path.write_text(lock_path.read_text() + "# content tampering\n")

    with pytest.raises(lock_validator.LockValidationError, match="artifact fingerprint mismatch"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)
