#!/usr/bin/env python3
"""Generate a deterministic, evidence-only third-party package inventory."""

import argparse
import hashlib
import json
import re
import sys
from email.parser import Parser
from pathlib import Path
from typing import Any

CONDA_FORGE_URL = "https://conda.anaconda.org/conda-forge/"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SOURCE_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
BASE_IMAGE_RE = re.compile(r"^\S+@sha256:[0-9a-f]{64}$")
PLATFORMS = {"linux/amd64", "linux/arm64"}


class InventoryError(ValueError):
    pass


def _required_text(record: dict[str, Any], field: str, context: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise InventoryError(f"{context}: missing {field}")
    return value.strip()


def _normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _normalize_conda_name(name: str) -> str:
    """Preserve Conda's package-name identity while ignoring case only."""
    return name.lower()


def _load_conda_packages(
    prefix: Path,
    conda_list_path: Path,
) -> tuple[list[dict[str, Any]], list[tuple[Path, dict[str, Any]]]]:
    metadata_dir = prefix / "conda-meta"
    records = sorted(metadata_dir.glob("*.json"))
    if not records:
        raise InventoryError(f"{prefix}: no Conda package records found")

    packages = []
    packages_by_name = {}
    for path in records:
        try:
            record = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise InventoryError(f"{path}: invalid Conda package record: {error}") from error
        context = f"{path}: Conda package"
        name = _required_text(record, "name", context)
        url = _required_text(record, "url", context)
        if not url.startswith(CONDA_FORGE_URL):
            raise InventoryError(f"{context} {name}: unexpected Conda source URL {url!r}")
        artifact_sha256 = _required_text(record, "sha256", context)
        if SHA256_RE.fullmatch(artifact_sha256) is None:
            raise InventoryError(f"{context} {name}: invalid sha256 {artifact_sha256!r}")
        package = {
            "artifact_sha256": artifact_sha256,
            "build": _required_text(record, "build", context),
            "ecosystem": "conda",
            "license": _required_text(record, "license", context),
            "license_family": record.get("license_family") or "",
            "name": name,
            "prefix": prefix.name,
            "source_url": url,
            "version": _required_text(record, "version", context),
        }
        normalized = _normalize_conda_name(name)
        if normalized in packages_by_name:
            raise InventoryError(f"{prefix}: duplicate Conda package metadata for {name}")
        packages_by_name[normalized] = package
        packages.append(package)

    try:
        solved_rows = json.loads(conda_list_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise InventoryError(f"{conda_list_path}: invalid conda list: {error}") from error
    if not isinstance(solved_rows, list):
        raise InventoryError(f"{conda_list_path}: conda list must be an array")

    listed_conda_names = set()
    pypi_rows = []
    for row in solved_rows:
        if not isinstance(row, dict):
            raise InventoryError(f"{conda_list_path}: invalid package row")
        if row.get("channel") == "pypi" or row.get("build_string") == "pypi_0":
            pypi_rows.append((conda_list_path, row))
            continue
        context = f"{conda_list_path}: Conda package"
        name = _required_text(row, "name", context)
        normalized = _normalize_conda_name(name)
        if normalized in listed_conda_names:
            raise InventoryError(f"{conda_list_path}: duplicate Conda package {name}")
        listed_conda_names.add(normalized)
        metadata = packages_by_name.get(normalized)
        if metadata is None:
            raise InventoryError(f"{context} {name}: missing conda-meta record")
        version = _required_text(row, "version", context)
        build = _required_text(row, "build_string", context)
        if metadata["version"] != version or metadata["build"] != build:
            raise InventoryError(
                f"{context} {name}: conda-meta identity does not match final conda list"
            )
    if listed_conda_names != set(packages_by_name):
        omitted = sorted(set(packages_by_name) - listed_conda_names)
        raise InventoryError(f"{conda_list_path}: final conda list omitted conda-meta packages {omitted}")
    return packages, pypi_rows


def _pip_license(metadata: dict[str, Any], context: str) -> str:
    for field in ("license_expression", "license"):
        value = metadata.get(field)
        if isinstance(value, str) and value.strip() and value.strip().upper() not in {
            "UNKNOWN",
            "NOASSERTION",
        }:
            return value.strip()
    classifiers = metadata.get("classifier", [])
    if isinstance(classifiers, list):
        license_classifiers = sorted(
            classifier.strip()
            for classifier in classifiers
            if isinstance(classifier, str) and classifier.startswith("License ::")
        )
        if license_classifiers:
            return "; ".join(license_classifiers)
    raise InventoryError(f"{context}: missing license")


def _load_pip_packages(report_paths: list[Path]) -> list[dict[str, Any]]:
    packages = []
    reported_names = set()
    for report_path in report_paths:
        try:
            report = json.loads(report_path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise InventoryError(f"{report_path}: invalid pip install report: {error}") from error
        installs = report.get("install")
        if not isinstance(installs, list) or not installs:
            raise InventoryError(f"{report_path}: pip install report has no installed packages")

        for install in installs:
            if not isinstance(install, dict):
                raise InventoryError(f"{report_path}: invalid pip install entry")
            metadata = install.get("metadata")
            download = install.get("download_info")
            if not isinstance(metadata, dict) or not isinstance(download, dict):
                raise InventoryError(f"{report_path}: pip entry is missing metadata or download_info")
            context = f"{report_path}: pip package"
            name = _required_text(metadata, "name", context)
            normalized = _normalize_name(name)
            if normalized in reported_names:
                raise InventoryError(f"{context} {name}: duplicate pip install report entry")
            reported_names.add(normalized)
            requested = install.get("requested")
            if not isinstance(requested, bool):
                raise InventoryError(f"{context} {name}: requested must be boolean")

            source_url = _required_text(download, "url", context)
            if not source_url.startswith("https://"):
                raise InventoryError(f"{context} {name}: source URL must use https")
            archive_info = download.get("archive_info")
            hashes = archive_info.get("hashes") if isinstance(archive_info, dict) else None
            artifact_sha256 = hashes.get("sha256") if isinstance(hashes, dict) else None
            if not isinstance(artifact_sha256, str) or SHA256_RE.fullmatch(artifact_sha256) is None:
                raise InventoryError(f"{context} {name}: missing sha256 artifact hash")
            packages.append(
                {
                    "artifact_sha256": artifact_sha256,
                    "ecosystem": "pip-direct" if requested else "pip-transitive",
                    "license": _pip_license(metadata, f"{context} {name}"),
                    "name": name,
                    "requested": requested,
                    "source_url": source_url,
                    "version": _required_text(metadata, "version", context),
                }
            )
    return packages


def _load_installed_pypi_packages(
    listed_pypi_rows: list[tuple[Path, dict[str, Any]]],
    python_prefix: Path,
    pip_artifacts: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    pypi_rows: dict[str, tuple[Path, dict[str, Any]]] = {}
    for conda_list_path, row in listed_pypi_rows:
        name = _required_text(row, "name", f"{conda_list_path}: PyPI package")
        normalized = _normalize_name(name)
        if normalized in pypi_rows:
            raise InventoryError(f"{conda_list_path}: duplicate PyPI package {name}")
        pypi_rows[normalized] = (conda_list_path, row)

    metadata_by_name: dict[str, tuple[Path, Any]] = {}
    for metadata_path in sorted(
        python_prefix.glob("lib/python*/site-packages/*.dist-info/METADATA")
    ):
        message = Parser().parsestr(metadata_path.read_text())
        name = message.get("Name")
        if not name:
            raise InventoryError(f"{metadata_path}: missing Name")
        normalized = _normalize_name(name)
        if normalized in metadata_by_name:
            raise InventoryError(f"{python_prefix}: duplicate Python distribution metadata for {name}")
        metadata_by_name[normalized] = (metadata_path, message)

    artifact_by_name = {_normalize_name(package["name"]): package for package in pip_artifacts}
    packages = []
    unresolved_sources = []
    for normalized, (conda_list_path, row) in sorted(pypi_rows.items()):
        if normalized not in metadata_by_name:
            raise InventoryError(
                f"{python_prefix}: missing installed distribution metadata for {row['name']}"
            )
        metadata_path, message = metadata_by_name[normalized]
        version = _required_text(row, "version", f"{conda_list_path}: PyPI package")
        if message.get("Version") != version:
            raise InventoryError(
                f"{metadata_path}: version {message.get('Version')!r} does not match conda list {version!r}"
            )
        record_path = metadata_path.with_name("RECORD")
        if not record_path.is_file():
            raise InventoryError(f"{record_path}: missing installed RECORD evidence")
        package = {
            "artifact_source": "UNAVAILABLE",
            "ecosystem": "pip-installed",
            "installed_record_sha256": hashlib.sha256(record_path.read_bytes()).hexdigest(),
            "license": _pip_license(
                {
                    "license_expression": message.get("License-Expression"),
                    "license": message.get("License"),
                    "classifier": message.get_all("Classifier", []),
                },
                f"{metadata_path}: Python distribution {row['name']}",
            ),
            "name": _required_text(row, "name", f"{conda_list_path}: PyPI package"),
            "version": version,
        }
        artifact = artifact_by_name.get(normalized)
        if artifact is None or artifact["version"] != version:
            unresolved_sources.append(package["name"])
        else:
            package["artifact_sha256"] = artifact["artifact_sha256"]
            package["artifact_source"] = artifact["source_url"]
        packages.append(package)
    return packages, sorted(unresolved_sources)


def _parse_dpkg_status(path: Path) -> list[dict[str, str]]:
    try:
        contents = path.read_text()
    except OSError as error:
        raise InventoryError(f"{path}: cannot read dpkg status: {error}") from error

    records: list[dict[str, str]] = []
    for paragraph in contents.split("\n\n"):
        if not paragraph.strip():
            continue
        record: dict[str, str] = {}
        current_field: str | None = None
        for line in paragraph.splitlines():
            if line.startswith((" ", "\t")) and current_field is not None:
                record[current_field] += "\n" + line[1:]
                continue
            if ":" not in line:
                raise InventoryError(f"{path}: malformed dpkg status line {line!r}")
            current_field, value = line.split(":", 1)
            record[current_field] = value.strip()
        if record.get("Status") == "install ok installed":
            records.append(record)
    if not records:
        raise InventoryError(f"{path}: no installed dpkg packages found")
    return records


def _load_dpkg_packages(root: Path) -> tuple[list[dict[str, Any]], str]:
    status_path = root / "var/lib/dpkg/status"
    status_bytes = status_path.read_bytes()
    records = _parse_dpkg_status(status_path)
    evidence_by_source_version: dict[tuple[str, str], Path] = {}
    for record in records:
        context = f"{status_path}: dpkg package"
        name = _required_text(record, "Package", context)
        version = _required_text(record, "Version", context)
        source_package = record.get("Source", name).split(" ", 1)[0]
        relative_license_path = Path("usr/share/doc") / name / "copyright"
        if (root / relative_license_path).is_file():
            evidence_by_source_version.setdefault(
                (source_package, version),
                relative_license_path,
            )

    packages = []
    for record in records:
        context = f"{status_path}: dpkg package"
        name = _required_text(record, "Package", context)
        version = _required_text(record, "Version", context)
        source_package = record.get("Source", name).split(" ", 1)[0]
        relative_license_path = Path("usr/share/doc") / name / "copyright"
        license_path = root / relative_license_path
        if not license_path.is_file():
            relative_license_path = evidence_by_source_version.get(
                (source_package, version),
                relative_license_path,
            )
            license_path = root / relative_license_path
        if not license_path.is_file():
            raise InventoryError(
                f"{context} {name}: missing dpkg copyright evidence for "
                f"source {source_package} version {version}"
            )
        license_bytes = license_path.read_bytes()
        packages.append(
            {
                "architecture": _required_text(record, "Architecture", context),
                "ecosystem": "dpkg",
                "license_evidence_path": f"/{relative_license_path}",
                "license_evidence_sha256": hashlib.sha256(license_bytes).hexdigest(),
                "name": name,
                "source_package": source_package,
                "version": version,
            }
        )
    return packages, hashlib.sha256(status_bytes).hexdigest()


def _build_inventory(args: argparse.Namespace) -> dict[str, Any]:
    if SOURCE_COMMIT_RE.fullmatch(args.source_commit) is None:
        raise InventoryError("source commit must be an exact lowercase 40-hex Git SHA")
    if BASE_IMAGE_RE.fullmatch(args.base_image) is None:
        raise InventoryError("base image must be bound to an exact sha256 digest")
    if args.platform not in PLATFORMS:
        raise InventoryError(f"unsupported platform {args.platform!r}")

    if len(args.conda_prefix) != len(args.conda_list):
        raise InventoryError("each Conda prefix requires one matching final conda list")
    packages = []
    listed_pypi_rows = []
    for prefix, conda_list_path in zip(args.conda_prefix, args.conda_list, strict=True):
        conda_packages, prefix_pypi_rows = _load_conda_packages(prefix, conda_list_path)
        packages.extend(conda_packages)
        listed_pypi_rows.extend(prefix_pypi_rows)
    pip_artifacts = _load_pip_packages(args.pip_report)
    packages.extend(pip_artifacts)
    installed_pypi_packages, unresolved_sources = _load_installed_pypi_packages(
        listed_pypi_rows,
        args.python_prefix,
        pip_artifacts,
    )
    if unresolved_sources:
        raise InventoryError(f"unresolved pip artifact sources: {unresolved_sources}")
    packages.extend(installed_pypi_packages)
    dpkg_packages, dpkg_status_sha256 = _load_dpkg_packages(args.dpkg_root)
    packages.extend(dpkg_packages)
    packages.sort(key=lambda package: (package["name"].lower(), package["ecosystem"], package.get("prefix", "")))
    notice_sha256 = {}
    for notice_path in sorted(args.notice, key=str):
        if not notice_path.is_file():
            raise InventoryError(f"{notice_path}: missing preserved notice evidence")
        notice_sha256[str(notice_path)] = hashlib.sha256(notice_path.read_bytes()).hexdigest()

    evidence_set = {
        "base_image": args.base_image,
        "dpkg_status_sha256": dpkg_status_sha256,
        "notice_sha256": notice_sha256,
        "packages": packages,
        "platform": args.platform,
        "schema_version": 1,
        "unresolved_artifact_sources": unresolved_sources,
    }
    evidence_set_sha256 = hashlib.sha256(
        json.dumps(evidence_set, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()

    return {
        "base_image": args.base_image,
        "dpkg_status_sha256": dpkg_status_sha256,
        "evidence_set_sha256": evidence_set_sha256,
        "legal_clearance": False,
        "notice_sha256": notice_sha256,
        "packages": packages,
        "platform": args.platform,
        "schema_version": 1,
        "source_commit": args.source_commit,
        "unresolved_artifact_sources": unresolved_sources,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--conda-prefix", action="append", required=True, type=Path)
    parser.add_argument("--pip-report", action="append", required=True, type=Path)
    parser.add_argument("--conda-list", action="append", required=True, type=Path)
    parser.add_argument("--python-prefix", required=True, type=Path)
    parser.add_argument("--dpkg-root", required=True, type=Path)
    parser.add_argument("--notice", action="append", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--base-image", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        inventory = _build_inventory(args)
        payload = json.dumps(inventory, indent=2, sort_keys=True) + "\n"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload)
    except (InventoryError, OSError) as error:
        print(f"license inventory failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
