import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import validate_release_dependency_locks as lock_validator
from scripts.generate_release_dependency_locks import _solve_target, conda_specs_from_environment
from scripts import generate_release_dependency_locks as lock_generator

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


def test_source_recipe_constrains_eth_utils_for_the_combined_lock():
    assert "eth-utils>=5,<6" in conda_specs_from_environment(ENVIRONMENT)
    assert "eth-typing>=5,<6" in conda_specs_from_environment(ENVIRONMENT)
    assert "hexbytes>=1,<2" in conda_specs_from_environment(ENVIRONMENT)


def test_source_recipe_direct_pip_requirements_are_exactly_pinned():
    lock_validator._validate_source_pins(ENVIRONMENT, PIP_PACKAGES)


@pytest.mark.parametrize(
    "pip_section",
    [
        "dependencies:\n  - injective-py==1.13.0\n  - pip:\n    - other==1.0\n",
        "dependencies:\n  - pip:\n    - injective-py==1.13.0\n    - injective-py==1.13.1\n",
    ],
)
def test_source_recipe_rejects_misplaced_or_conflicting_pip_pin(tmp_path: Path, pip_section: str):
    environment = tmp_path / "environment.yml"
    environment.write_text(pip_section)
    pip_packages = tmp_path / "pip_packages.txt"
    pip_packages.write_text("eip712-structs==1.1.0\n")

    with pytest.raises(lock_validator.LockValidationError, match="dependencies.pip"):
        lock_validator._validate_source_pins(environment, pip_packages)


def test_pip_lock_accepts_uv_multiline_hash_continuations(tmp_path: Path):
    lock = tmp_path / "uv.lock"
    lock.write_text(
        "injective-py==1.13.0 \\\n"
        f"    --hash=sha256:{'a' * 64} \\\n"
        f"    --hash=sha256:{'b' * 64}\n"
    )

    lock_validator._validate_pip_lock(lock, {"injective-py": "1.13.0"})


def test_pip_lock_rejects_duplicate_pep503_aliases(tmp_path: Path):
    lock = tmp_path / "alias.lock"
    lock.write_text("foo_bar==1 \\\n    --hash=sha256:" + "a" * 64 + "\nfoo-bar==1 \\\n    --hash=sha256:" + "b" * 64 + "\n")
    with pytest.raises(lock_validator.LockValidationError, match="duplicate"):
        lock_validator._validate_pip_lock(lock, {"foo-bar": "1"})


def test_conda_alias_versions_fail_closed_when_divergent():
    entries = (
        "https://conda.anaconda.org/conda-forge/noarch/foo_bar-1.0-0.conda#" + "a" * 64,
        "https://conda.anaconda.org/conda-forge/noarch/foo-bar-2.0-0.conda#" + "b" * 64,
    )
    with pytest.raises(lock_validator.LockValidationError, match="divergent"):
        lock_validator._conda_package_versions(entries)


def test_conda_source_constraint_rejects_locked_urllib3_above_upper_bound(tmp_path: Path):
    environment = tmp_path / "environment.yml"
    environment.write_text("dependencies:\n  - urllib3>=1.26.15,<2\n")
    entry = "https://conda.anaconda.org/conda-forge/linux-64/urllib3-2.7.0-py_0.conda#" + "a" * 64
    with pytest.raises(lock_validator.LockValidationError, match="source constraint"):
        lock_validator._validate_conda_source_coverage(environment, {"linux-64": (entry,)})


def test_resolver_environment_strips_hostile_pip_and_uv_variables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    for key in ("PIP_INDEX_URL", "PIP_EXTRA_INDEX_URL", "PIP_FIND_LINKS", "PIP_CONFIG_FILE", "UV_INDEX", "UV_CONFIG_FILE", "UV_FIND_LINKS"):
        monkeypatch.setenv(key, "https://hostile.invalid")
    environment = lock_generator._resolver_environment(tmp_path)
    assert {key for key in environment if key.startswith(("PIP_", "UV_"))} == {"UV_CACHE_DIR"}
    assert environment["UV_CACHE_DIR"] == str(tmp_path / "uv-cache")


def test_solve_target_rejects_cache_dependent_fetch_omissions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    environment = tmp_path / "environment.yml"
    environment.write_text("dependencies:\n  - python==3.13.15\n  - requests\n")
    payload = {
        "actions": {
            "FETCH": [{"name": "python", "version": "3.13.15"}],
            "LINK": [{"name": "python", "version": "3.13.15"}, {"name": "requests", "version": "2.0"}],
        }
    }

    observed_environment = {}

    def fake_run(*args, **kwargs):
        observed_environment.update(kwargs["env"])
        return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr("scripts.generate_release_dependency_locks.subprocess.run", fake_run)

    with pytest.raises(lock_validator.LockValidationError, match="complete link plan"):
        _solve_target("conda", environment, tmp_path, "linux-64")
    assert observed_environment["CONDA_PKGS_DIRS"] == str(tmp_path / "conda-pkgs")


def test_solve_target_projects_link_identity_to_fetch_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    environment = tmp_path / "environment.yml"
    environment.write_text("dependencies:\n  - python==3.13.15\n")
    fetch_record = {
        "name": "python",
        "version": "3.13.15",
        "build": "h123_0",
        "build_number": 0,
        "subdir": "linux-64",
        "url": "https://fetch.example/python",
        "sha256": "a" * 64,
    }
    link_record = {
        "name": "python",
        "version": "3.13.15",
        "build_string": "h123_0",
        "build_number": 0,
        "platform": "linux-64",
    }
    payload = {"actions": {"FETCH": [fetch_record], "LINK": [link_record]}}
    monkeypatch.setattr(
        "scripts.generate_release_dependency_locks.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr=""),
    )

    assert _solve_target("conda", environment, tmp_path, "linux-64") == [fetch_record]


def test_solve_target_rejects_same_name_version_with_mismatched_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    environment = tmp_path / "environment.yml"
    environment.write_text("dependencies:\n  - python==3.13.15\n")
    payload = {
        "actions": {
            "FETCH": [{"name": "python", "version": "3.13.15", "build": "h123_0", "build_number": 0}],
            "LINK": [{"name": "python", "version": "3.13.15", "build_string": "h456_0", "build_number": 0}],
        }
    }
    monkeypatch.setattr(
        "scripts.generate_release_dependency_locks.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr=""),
    )

    with pytest.raises(lock_validator.LockValidationError, match="complete link plan"):
        _solve_target("conda", environment, tmp_path, "linux-64")


def test_committed_locks_validate_offline_for_both_release_targets():
    result = lock_validator.validate_lock_directory(LOCKS, ENVIRONMENT, PIP_PACKAGES)

    assert result.targets == ("linux-64", "linux-aarch64")
    assert result.python_versions == {"linux-64": "3.13.15", "linux-aarch64": "3.13.15"}


def test_both_explicit_locks_cover_every_canonical_conda_source_requirement():
    source_names = {
        lock_validator._conda_spec_name(spec)
        for spec in conda_specs_from_environment(ENVIRONMENT)
    }

    for target in lock_validator.TARGETS:
        entries = lock_validator._explicit_entries(LOCKS / f"{target}.explicit.txt", target)
        assert source_names <= lock_validator._explicit_package_names(entries)


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
    lock_path = lock_dir / "pip-injective-linux-64.lock.txt"
    lock_path.write_text(lock_path.read_text().replace("injective-py==1.13.0", "injective-py==1.13.1"))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="pinned requirement"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_pip_hash_drift_before_fingerprint_check(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "pip-injective-linux-64.lock.txt"
    lock_path.write_text(lock_path.read_text().replace("--hash=sha256:", "--hash=sha256:" + "0", 1))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="SHA-256"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_missing_target_lock(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    (lock_dir / "linux-aarch64.explicit.txt").unlink()

    with pytest.raises(lock_validator.LockValidationError, match="inventory"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_tampered_lock_content_with_artifact_fingerprint(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "linux-64.explicit.txt"
    lock_path.write_text(lock_path.read_text() + "# content tampering\n")

    with pytest.raises(lock_validator.LockValidationError, match="artifact fingerprint mismatch"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_unsupported_extra_lock_artifact(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    (lock_dir / "osx-arm64.explicit.txt").write_text("@EXPLICIT\n")

    with pytest.raises(lock_validator.LockValidationError, match="inventory"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("uv_version", "0.11.28", "uv generator version"),
        ("pip_targets", {"linux-64": "x86_64-manylinux_2_17"}, "pip target mapping"),
        ("runtime_substitution", {}, "runtime substitution"),
    ],
)
def test_validator_rejects_provenance_generator_target_or_exception_drift(
    tmp_path: Path, field: str, value: object, message: str
):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    provenance_path = lock_dir / "provenance.json"
    provenance = json.loads(provenance_path.read_text())
    provenance[field] = value
    provenance_path.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")

    with pytest.raises(lock_validator.LockValidationError, match=message):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_missing_injective_direct_input_before_fingerprint_check(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "pip-injective-linux-aarch64.lock.txt"
    contents = lock_path.read_text()
    start = contents.index("safe-pysha3==1.0.4")
    end = contents.find("\n\n", start)
    lock_path.write_text(contents[:start] + (contents[end + 2:] if end != -1 else ""))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)

    with pytest.raises(lock_validator.LockValidationError, match="coverage"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_transitive_version_drift_after_fingerprint_refresh(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "pip-injective-linux-64.lock.txt"
    lock_path.write_text(lock_path.read_text().replace("aiohappyeyeballs==2.7.1", "aiohappyeyeballs==2.7.2", 1))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)
    with pytest.raises(lock_validator.LockValidationError, match="overlap"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_non_overlapping_transitive_drift_at_closure_manifest(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    lock_path = lock_dir / "pip-injective-linux-64.lock.txt"
    lock_path.write_text(lock_path.read_text().replace("bech32==1.2.0", "bech32==1.2.1", 1))
    _refresh_artifact_fingerprint(lock_dir, lock_path.name)
    with pytest.raises(lock_validator.LockValidationError, match="closure manifest"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)


def test_validator_rejects_origin_policy_drift(tmp_path: Path):
    lock_dir, environment, pip_packages = _copy_lock_inputs(tmp_path)
    provenance_path = lock_dir / "provenance.json"
    provenance = json.loads(provenance_path.read_text())
    provenance["origin_policy"]["pypi_index"] = "https://hostile.invalid/simple"
    provenance_path.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    with pytest.raises(lock_validator.LockValidationError, match="origin policy"):
        lock_validator.validate_lock_directory(lock_dir, environment, pip_packages)
