#!/usr/bin/env python3
"""Validate release dependency locks without accessing package indexes."""

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

TARGETS = ("linux-64", "linux-aarch64")
PYTHON_VERSION = "3.13.15"
CONDA_FORGE_HOST = "conda.anaconda.org"
CONDA_FORGE_PATH = "/conda-forge/"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PIP_REQUIREMENTS = {
    "injective-py": "1.13.0",
    "eip712-structs": "1.1.0",
}
PIP_REQUIREMENT_RE = re.compile(r"^([a-z0-9][a-z0-9._-]*)==([^\s\\]+)(?:\s+\\)?$")
PIP_HASH_RE = re.compile(r"^--hash=sha256:([0-9a-f]{64})$")


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
    environment_lines = [line.strip() for line in environment_path.read_text().splitlines()]
    if "- injective-py==1.13.0" not in environment_lines:
        raise LockValidationError(f"{environment_path}: injective-py must be pinned to 1.13.0")
    pip_lines = [line.strip() for line in pip_packages_path.read_text().splitlines() if line.strip()]
    if pip_lines != ["eip712-structs==1.1.0"]:
        raise LockValidationError(f"{pip_packages_path}: eip712-structs must be pinned to 1.1.0")


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


def _validate_pip_lock(path: Path) -> None:
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
        if name in found:
            raise LockValidationError(f"{path}: duplicate pip requirement {name}")
        found[name] = (version, hashes)
    if set(found) != set(PIP_REQUIREMENTS):
        raise LockValidationError(f"{path}: pip lock must contain only {sorted(PIP_REQUIREMENTS)}")
    for name, expected_version in PIP_REQUIREMENTS.items():
        if found[name][0] != expected_version:
            raise LockValidationError(f"{path}: pinned requirement {name} must be {expected_version}")


def _validate_artifact_fingerprints(provenance: dict[str, object], lock_dir: Path) -> None:
    artifacts = _required_mapping(provenance.get("artifacts"), "provenance artifacts")
    expected_names = {f"{target}.explicit.txt" for target in TARGETS} | {"pip-direct.lock.txt"}
    if set(artifacts) != expected_names:
        raise LockValidationError("provenance artifact fingerprint coverage mismatch")
    for name in sorted(expected_names):
        expected = artifacts[name]
        if not isinstance(expected, str) or SHA256_RE.fullmatch(expected) is None:
            raise LockValidationError(f"provenance artifact fingerprint for {name} is invalid")
        if sha256_file(lock_dir / name) != expected:
            raise LockValidationError(f"artifact fingerprint mismatch for {name}")


def validate_lock_directory(lock_dir: Path, environment_path: Path, pip_packages_path: Path) -> ValidationResult:
    """Validate source binding and all release lock artifacts without network access."""
    provenance = _load_provenance(lock_dir / "provenance.json")
    if provenance.get("format") != 1:
        raise LockValidationError("unsupported provenance format")
    targets = provenance.get("targets")
    if targets != list(TARGETS):
        raise LockValidationError(f"unsupported target coverage {targets!r}")
    if provenance.get("python_version") != PYTHON_VERSION:
        raise LockValidationError(f"provenance must pin Python {PYTHON_VERSION}")
    _validate_source_pins(environment_path, pip_packages_path)
    _validate_source_fingerprints(provenance, environment_path, pip_packages_path)
    python_versions = {}
    for target in TARGETS:
        entries = _explicit_entries(lock_dir / f"{target}.explicit.txt", target)
        python_entry = next(entry for entry in entries if "/python-" in entry)
        python_versions[target] = re.search(r"/python-([0-9][^-]*)-", python_entry).group(1)
    _validate_pip_lock(lock_dir / "pip-direct.lock.txt")
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
