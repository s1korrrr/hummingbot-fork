#!/usr/bin/env python3
"""Validate release dependency locks without accessing package indexes."""

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import yaml

TARGETS = ("linux-64", "linux-aarch64")
PYTHON_VERSION = "3.13.15"
CONDA_FORGE_HOST = "conda.anaconda.org"
CONDA_FORGE_PATH = "/conda-forge/"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PIP_TARGETS = {
    "linux-64": "x86_64-manylinux_2_17",
    "linux-aarch64": "aarch64-manylinux_2_17",
}
UV_VERSION = "0.11.29"
PYPI_INDEX = "https://pypi.org/simple"
ORIGIN_POLICY = {"pip_isolated": True, "pypi_index": PYPI_INDEX, "uv_no_config": True}
INJECTIVE_DIRECT_REQUIREMENTS = {"injective-py": "1.13.0", "safe-pysha3": "1.0.4"}
EIP712_DIRECT_REQUIREMENTS = {"eip712-structs": "1.1.0"}
RUNTIME_SUBSTITUTION = {"requirement": "pysha3>=1.0.2", "replacement": "safe-pysha3==1.0.4"}
PIP_REQUIREMENT_RE = re.compile(r"^([a-z0-9][a-z0-9._-]*)==([^\s\\]+)(?:\s+\\)?$")
PIP_HASH_RE = re.compile(r"^--hash=sha256:([0-9a-f]{64})(?:\s+\\)?$")


class LockValidationError(ValueError):
    """A committed dependency lock cannot safely be consumed."""


@dataclass(frozen=True)
class ValidationResult:
    targets: tuple[str, ...]
    python_versions: dict[str, str]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _required_mapping(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise LockValidationError(f"{context}: expected an object")
    return value


def _load_provenance(path: Path) -> dict[str, object]:
    try:
        return _required_mapping(json.loads(path.read_text()), str(path))
    except FileNotFoundError as error:
        raise LockValidationError(f"missing provenance record {path}") from error
    except json.JSONDecodeError as error:
        raise LockValidationError(f"{path}: invalid JSON: {error}") from error


def _validate_source_fingerprints(
    provenance: dict[str, object], environment_path: Path, pip_packages_path: Path
) -> None:
    sources = _required_mapping(provenance.get("sources"), "provenance sources")
    expected = {
        "setup/environment.yml": sha256_file(environment_path),
        "setup/pip_packages.txt": sha256_file(pip_packages_path),
    }
    if sources != expected:
        raise LockValidationError("source fingerprint mismatch")


def _validate_source_pins(environment_path: Path, pip_packages_path: Path) -> None:
    try:
        environment = _required_mapping(yaml.safe_load(environment_path.read_text()), str(environment_path))
    except yaml.YAMLError as error:
        raise LockValidationError(f"{environment_path}: invalid YAML") from error
    dependencies = environment.get("dependencies")
    if not isinstance(dependencies, list):
        raise LockValidationError(f"{environment_path}: dependencies must be a YAML list")
    pip_sections = [entry["pip"] for entry in dependencies if isinstance(entry, dict) and set(entry) == {"pip"}]
    if pip_sections != [["injective-py==1.13.0"]]:
        raise LockValidationError(f"{environment_path}: dependencies.pip must contain only injective-py==1.13.0")
    pip_lines = [line.strip() for line in pip_packages_path.read_text().splitlines() if line.strip()]
    if pip_lines != ["eip712-structs==1.1.0"]:
        raise LockValidationError(f"{pip_packages_path}: eip712-structs must be pinned to 1.1.0")


def _canonical_package_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value.lower())


def _conda_spec_name(spec: str) -> str:
    return _canonical_package_name(re.split(r"[<>=!~ ]", spec, maxsplit=1)[0])


def _explicit_package_names(entries: tuple[str, ...]) -> set[str]:
    names = set()
    for entry in entries:
        filename = Path(urlsplit(entry).path).name
        match = re.fullmatch(r"(.+)-([0-9][^-]*)-[^-]+\.(?:conda|tar\.bz2)", filename)
        if match is None:
            raise LockValidationError(f"{filename}: cannot identify Conda package name")
        names.add(_canonical_package_name(match.group(1)))
    return names


def _conda_package_versions(entries: tuple[str, ...]) -> dict[str, str]:
    versions = {}
    for entry in entries:
        match = re.fullmatch(r"(.+)-([0-9][^-]*)-[^-]+\.(?:conda|tar\.bz2)", Path(urlsplit(entry).path).name)
        if match is None:
            raise LockValidationError("cannot identify Conda package version")
        name, version = _canonical_package_name(match.group(1)), match.group(2)
        if name in versions and versions[name] != version:
            raise LockValidationError(f"divergent normalized Conda alias {name}")
        versions[name] = version
    return versions


def _validate_conda_source_coverage(environment_path: Path, target_entries: dict[str, tuple[str, ...]]) -> None:
    try:
        environment = _required_mapping(yaml.safe_load(environment_path.read_text()), str(environment_path))
    except yaml.YAMLError as error:
        raise LockValidationError(f"{environment_path}: invalid YAML") from error
    dependencies = environment.get("dependencies")
    if not isinstance(dependencies, list):
        raise LockValidationError(f"{environment_path}: dependencies must be a YAML list")
    source_names = {_conda_spec_name(item) for item in dependencies if isinstance(item, str) and item.strip()}
    for target, entries in target_entries.items():
        missing = sorted(source_names - _explicit_package_names(entries))
        if missing:
            raise LockValidationError(f"{target}: explicit lock misses canonical Conda requirements: {', '.join(missing)}")


def _explicit_entries(lock_path: Path, target: str) -> tuple[str, ...]:
    try:
        lines = lock_path.read_text().splitlines()
    except FileNotFoundError as error:
        raise LockValidationError(f"missing lock {lock_path}") from error
    if "@EXPLICIT" not in lines:
        raise LockValidationError(f"{lock_path}: missing @EXPLICIT header")
    entries = tuple(line for line in lines[lines.index("@EXPLICIT") + 1:] if line and not line.startswith("#"))
    if not entries:
        raise LockValidationError(f"{lock_path}: explicit lock has no package entries")
    python_versions = []
    for entry in entries:
        parsed = urlsplit(entry)
        if parsed.scheme != "https" or parsed.netloc != CONDA_FORGE_HOST or not parsed.path.startswith(CONDA_FORGE_PATH):
            raise LockValidationError(f"{lock_path}: Conda URL must be HTTPS conda-forge")
        if parsed.query or parsed.username or parsed.password or SHA256_RE.fullmatch(parsed.fragment) is None:
            raise LockValidationError(f"{lock_path}: Conda URL needs an exact lowercase SHA-256 fragment")
        path_parts = parsed.path.removeprefix(CONDA_FORGE_PATH).split("/")
        if len(path_parts) != 2 or path_parts[0] not in {target, "noarch"} or not path_parts[1]:
            raise LockValidationError(f"{lock_path}: Conda URL subdir must be {target} or noarch")
        match = re.fullmatch(r"python-([0-9][^-]*)-[^-]+\.(?:conda|tar\.bz2)", path_parts[1])
        if match is not None:
            python_versions.append(match.group(1))
    if python_versions != [PYTHON_VERSION]:
        raise LockValidationError(f"{lock_path}: expected exactly Python {PYTHON_VERSION}, got {python_versions}")
    return entries


def _validate_pip_lock(path: Path, expected_requirements: dict[str, str], *, closure: bool = False) -> None:
    try:
        lines = [line.strip() for line in path.read_text().splitlines() if line.strip() and not line.startswith("#")]
    except FileNotFoundError as error:
        raise LockValidationError(f"missing lock {path}") from error
    found: dict[str, tuple[str, list[str]]] = {}
    index = 0
    while index < len(lines):
        match = PIP_REQUIREMENT_RE.fullmatch(lines[index])
        if match is None:
            raise LockValidationError(f"{path}: expected a pinned requirement, got {lines[index]!r}")
        name, version = match.groups()
        hashes = []
        index += 1
        while index < len(lines) and lines[index].startswith("--hash="):
            hash_match = PIP_HASH_RE.fullmatch(lines[index])
            if hash_match is None:
                raise LockValidationError(f"{path}: pip hash must be an exact lowercase SHA-256")
            hashes.append(hash_match.group(1))
            index += 1
        if not hashes:
            raise LockValidationError(f"{path}: pip requirement {name} has no SHA-256 hash")
        name = _canonical_package_name(name)
        if name in found:
            raise LockValidationError(f"{path}: duplicate pip requirement {name}")
        found[name] = (version, hashes)
    expected_names = set(expected_requirements)
    if (not expected_names.issubset(found)) if closure else (set(found) != expected_names):
        raise LockValidationError(f"{path}: pip lock direct input coverage mismatch")
    for name, expected_version in expected_requirements.items():
        if found[name][0] != expected_version:
            raise LockValidationError(f"{path}: pinned requirement {name} must be {expected_version}")
    return {_canonical_package_name(name): version for name, (version, _) in found.items()}


def _validate_artifact_fingerprints(provenance: dict[str, object], lock_dir: Path) -> None:
    artifacts = _required_mapping(provenance.get("artifacts"), "provenance artifacts")
    expected_names = {f"{target}.explicit.txt" for target in TARGETS} | {f"pip-injective-{target}.lock.txt" for target in TARGETS} | {"pip-eip712-direct.lock.txt"}
    if set(artifacts) != expected_names:
        raise LockValidationError("provenance artifact fingerprint coverage mismatch")
    for name in sorted(expected_names):
        expected = artifacts[name]
        if not isinstance(expected, str) or SHA256_RE.fullmatch(expected) is None:
            raise LockValidationError(f"provenance artifact fingerprint for {name} is invalid")
        if sha256_file(lock_dir / name) != expected:
            raise LockValidationError(f"artifact fingerprint mismatch for {name}")


def _validate_lock_inventory(lock_dir: Path) -> None:
    expected = {f"{target}.explicit.txt" for target in TARGETS} | {f"pip-injective-{target}.lock.txt" for target in TARGETS} | {"pip-eip712-direct.lock.txt", "provenance.json"}
    try:
        actual = {path.name for path in lock_dir.iterdir()}
    except FileNotFoundError as error:
        raise LockValidationError(f"missing lock directory {lock_dir}") from error
    if actual != expected:
        raise LockValidationError("lock inventory mismatch")


def validate_lock_directory(lock_dir: Path, environment_path: Path, pip_packages_path: Path) -> ValidationResult:
    """Validate source binding and all release lock artifacts without network access."""
    _validate_lock_inventory(lock_dir)
    provenance = _load_provenance(lock_dir / "provenance.json")
    if provenance.get("format") != 1:
        raise LockValidationError("unsupported provenance format")
    targets = provenance.get("targets")
    if targets != list(TARGETS):
        raise LockValidationError(f"unsupported target coverage {targets!r}")
    if provenance.get("python_version") != PYTHON_VERSION:
        raise LockValidationError(f"provenance must pin Python {PYTHON_VERSION}")
    if provenance.get("pip_targets") != PIP_TARGETS:
        raise LockValidationError("pip target mapping mismatch")
    if provenance.get("uv_version") != UV_VERSION:
        raise LockValidationError("uv generator version mismatch")
    if provenance.get("injective_direct_requirements") != INJECTIVE_DIRECT_REQUIREMENTS:
        raise LockValidationError("Injective direct input binding mismatch")
    if provenance.get("runtime_substitution") != RUNTIME_SUBSTITUTION:
        raise LockValidationError("runtime substitution binding mismatch")
    _validate_source_pins(environment_path, pip_packages_path)
    _validate_source_fingerprints(provenance, environment_path, pip_packages_path)
    python_versions = {}
    target_entries = {}
    for target in TARGETS:
        entries = _explicit_entries(lock_dir / f"{target}.explicit.txt", target)
        target_entries[target] = entries
        python_entry = next(entry for entry in entries if "/python-" in entry)
        python_versions[target] = re.search(r"/python-([0-9][^-]*)-", python_entry).group(1)
    _validate_conda_source_coverage(environment_path, target_entries)
    closure_maps = {}
    overlap_maps = {}
    for target in TARGETS:
        closure = _validate_pip_lock(lock_dir / f"pip-injective-{target}.lock.txt", INJECTIVE_DIRECT_REQUIREMENTS, closure=True)
        closure_maps[target] = closure
        conda_versions = _conda_package_versions(target_entries[target])
        conflicts = {name: (conda_versions[name], closure[name]) for name in set(conda_versions) & set(closure) if conda_versions[name] != closure[name]}
        if conflicts:
            raise LockValidationError(f"{target}: incompatible Conda/pip overlap")
        overlap_maps[target] = {name: closure[name] for name in sorted(set(conda_versions) & set(closure))}
    if provenance.get("origin_policy") != ORIGIN_POLICY:
        raise LockValidationError("resolver origin policy mismatch")
    if provenance.get("pip_closures") != closure_maps:
        raise LockValidationError("pip closure manifest mismatch")
    if provenance.get("conda_pip_overlaps") != overlap_maps:
        raise LockValidationError("Conda/pip overlap manifest mismatch")
    _validate_pip_lock(lock_dir / "pip-eip712-direct.lock.txt", EIP712_DIRECT_REQUIREMENTS)
    _validate_artifact_fingerprints(provenance, lock_dir)
    return ValidationResult(targets=TARGETS, python_versions=python_versions)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock-dir", type=Path, default=Path("setup/locks"))
    parser.add_argument("--environment", type=Path, default=Path("setup/environment.yml"))
    parser.add_argument("--pip-packages", type=Path, default=Path("setup/pip_packages.txt"))
    args = parser.parse_args()
    result = validate_lock_directory(args.lock_dir, args.environment, args.pip_packages)
    print(f"validated targets: {', '.join(result.targets)}; Python {PYTHON_VERSION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
