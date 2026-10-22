import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/generate_third_party_license_inventory.py"


def _conda_record(name: str, *, license_name: str = "BSD-3-Clause", url: str | None = None) -> dict:
    return {
        "name": name,
        "version": "1.2.3",
        "build": "py_0",
        "license": license_name,
        "license_family": "BSD",
        "sha256": "b" * 64,
        "url": url or f"https://conda.anaconda.org/conda-forge/noarch/{name}-1.2.3-py_0.conda",
    }


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path]:
    base_prefix = tmp_path / "base"
    env_prefix = tmp_path / "env"
    for prefix, record in (
        (base_prefix, _conda_record("base-package")),
        (env_prefix, _conda_record("env-package", license_name="MIT")),
    ):
        metadata = prefix / "conda-meta"
        metadata.mkdir(parents=True, exist_ok=True)
        (metadata / f"{record['name']}.json").write_text(json.dumps(record))

    pip_report = tmp_path / "pip-report.json"
    pip_report.write_text(
        json.dumps(
            {
                "install": [
                    {
                        "requested": True,
                        "download_info": {
                            "url": "https://files.pythonhosted.org/packages/injective_py.whl",
                            "archive_info": {"hashes": {"sha256": "a" * 64}},
                        },
                        "metadata": {
                            "name": "injective-py",
                            "version": "1.13.1",
                            "license": "Apache-2.0",
                        },
                    },
                    {
                        "requested": False,
                        "download_info": {
                            "url": "https://files.pythonhosted.org/packages/env_pip.whl",
                            "archive_info": {"hashes": {"sha256": "f" * 64}},
                        },
                        "metadata": {
                            "name": "env-pip",
                            "version": "4.5",
                            "license": "MIT",
                        },
                    }
                ]
            }
        )
    )
    dpkg_root = tmp_path / "dpkg-root"
    (dpkg_root / "var/lib/dpkg").mkdir(parents=True, exist_ok=True)
    (dpkg_root / "usr/share/doc/runtime-package").mkdir(parents=True, exist_ok=True)
    (dpkg_root / "var/lib/dpkg/status").write_text(
        "Package: runtime-package\n"
        "Status: install ok installed\n"
        "Version: 2.4-1ubuntu1\n"
        "Architecture: arm64\n"
        "Source: runtime-source (2.4-1ubuntu1)\n\n"
    )
    (dpkg_root / "usr/share/doc/runtime-package/copyright").write_text(
        "License: MIT\n"
    )
    base_conda_list = tmp_path / "base-conda-list.json"
    base_conda_list.write_text(
        json.dumps(
            [
                {
                    "name": "base-package",
                    "version": "1.2.3",
                    "build_string": "py_0",
                    "channel": "conda-forge",
                }
            ]
        )
    )
    env_conda_list = tmp_path / "env-conda-list.json"
    env_conda_list.write_text(
        json.dumps(
            [
                {
                    "name": "env-package",
                    "version": "1.2.3",
                    "build_string": "py_0",
                    "channel": "conda-forge",
                },
                {
                    "name": "env-pip",
                    "version": "4.5",
                    "build_string": "pypi_0",
                    "channel": "pypi",
                },
                {
                    "name": "injective-py",
                    "version": "1.13.1",
                    "build_string": "pypi_0",
                    "channel": "pypi",
                },
            ]
        )
    )
    python_prefix = tmp_path / "python-prefix"
    site_packages = python_prefix / "lib/python3.13/site-packages"
    for name, version, license_expression in (
        ("env-pip", "4.5", "MIT"),
        ("injective-py", "1.13.1", "Apache-2.0"),
    ):
        dist_info = site_packages / f"{name.replace('-', '_')}-{version}.dist-info"
        dist_info.mkdir(parents=True, exist_ok=True)
        (dist_info / "METADATA").write_text(
            f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\n"
            f"License-Expression: {license_expression}\n"
        )
        (dist_info / "RECORD").write_text(f"{dist_info.name}/METADATA,,\n")
    notice = tmp_path / "NOTICE"
    notice.write_text("Preserved notice evidence\n")
    return (
        base_prefix,
        env_prefix,
        pip_report,
        dpkg_root,
        base_conda_list,
        env_conda_list,
        python_prefix,
        notice,
    )


def _run(
    tmp_path: Path,
    *extra_args: str,
    setup=None,
) -> subprocess.CompletedProcess[str]:
    (
        base_prefix,
        env_prefix,
        pip_report,
        dpkg_root,
        base_conda_list,
        env_conda_list,
        python_prefix,
        notice,
    ) = _write_inputs(tmp_path)
    if setup is not None:
        setup(base_prefix, base_conda_list)
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--conda-prefix",
            str(base_prefix),
            "--conda-prefix",
            str(env_prefix),
            "--pip-report",
            str(pip_report),
            "--conda-list",
            str(base_conda_list),
            "--conda-list",
            str(env_conda_list),
            "--python-prefix",
            str(python_prefix),
            "--dpkg-root",
            str(dpkg_root),
            "--notice",
            str(notice),
            "--source-commit",
            "c" * 40,
            "--base-image",
            "docker.io/condaforge/miniforge3:26.5.3-0@sha256:" + "d" * 64,
            "--platform",
            "linux/arm64",
            "--output",
            str(tmp_path / "inventory.json"),
            *extra_args,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_inventory_is_deterministic_and_records_exact_sources_and_licenses(tmp_path: Path):
    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    inventory_path = tmp_path / "inventory.json"
    first_bytes = inventory_path.read_bytes()
    inventory = json.loads(first_bytes)
    expected = {
        "legal_clearance": False,
        "base_image": "docker.io/condaforge/miniforge3:26.5.3-0@sha256:" + "d" * 64,
        "dpkg_status_sha256": "a860277ced19ca21357c26743a532dbb091731f91d926b4505c2ed96a988baf3",  # noqa: mock
        "notice_sha256": {
            str(tmp_path / "NOTICE"): hashlib.sha256(
                (tmp_path / "NOTICE").read_bytes()
            ).hexdigest()
        },
        "packages": [
            {
                "build": "py_0",
                "ecosystem": "conda",
                "license": "BSD-3-Clause",
                "license_family": "BSD",
                "name": "base-package",
                "prefix": "base",
                "artifact_sha256": "b" * 64,
                "source_url": (
                    "https://conda.anaconda.org/conda-forge/noarch/"
                    "base-package-1.2.3-py_0.conda"
                ),
                "version": "1.2.3",
            },
            {
                "build": "py_0",
                "ecosystem": "conda",
                "license": "MIT",
                "license_family": "BSD",
                "name": "env-package",
                "prefix": "env",
                "artifact_sha256": "b" * 64,
                "source_url": (
                    "https://conda.anaconda.org/conda-forge/noarch/"
                    "env-package-1.2.3-py_0.conda"
                ),
                "version": "1.2.3",
            },
            {
                "artifact_sha256": "f" * 64,
                "artifact_source": "https://files.pythonhosted.org/packages/env_pip.whl",
                "ecosystem": "pip-installed",
                "installed_record_sha256": (
                    "a6f07c8c1d6febc5bd2ff73bdcb5adcf751653a9ec0ec1cff8c65c6ea5d68696"  # noqa: mock
                ),
                "license": "MIT",
                "name": "env-pip",
                "version": "4.5",
            },
            {
                "artifact_sha256": "f" * 64,
                "ecosystem": "pip-transitive",
                "license": "MIT",
                "name": "env-pip",
                "requested": False,
                "source_url": "https://files.pythonhosted.org/packages/env_pip.whl",
                "version": "4.5",
            },
            {
                "artifact_sha256": "a" * 64,
                "ecosystem": "pip-direct",
                "license": "Apache-2.0",
                "name": "injective-py",
                "requested": True,
                "source_url": "https://files.pythonhosted.org/packages/injective_py.whl",
                "version": "1.13.1",
            },
            {
                "artifact_sha256": "a" * 64,
                "artifact_source": "https://files.pythonhosted.org/packages/injective_py.whl",
                "ecosystem": "pip-installed",
                "installed_record_sha256": (
                    "28bc0b141a3b3320fa04bbc72a32173f0d3e8fbef919cb1a4ec931298a5b0868"  # noqa: mock
                ),
                "license": "Apache-2.0",
                "name": "injective-py",
                "version": "1.13.1",
            },
            {
                "architecture": "arm64",
                "ecosystem": "dpkg",
                "license_evidence_path": "/usr/share/doc/runtime-package/copyright",
                "license_evidence_sha256": (
                    "f80b1fa5e6cd69638fa1c40c2cd397453c57d0e8bdf231b616a74e789e70418e"  # noqa: mock
                ),
                "name": "runtime-package",
                "source_package": "runtime-source",
                "version": "2.4-1ubuntu1",
            },
        ],
        "platform": "linux/arm64",
        "schema_version": 1,
        "source_commit": "c" * 40,
        "unresolved_artifact_sources": [],
    }
    evidence_fields = (
        "base_image",
        "dpkg_status_sha256",
        "notice_sha256",
        "packages",
        "platform",
        "schema_version",
        "unresolved_artifact_sources",
    )
    expected["evidence_set_sha256"] = hashlib.sha256(
        json.dumps(
            {field: expected[field] for field in evidence_fields},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    assert inventory == expected

    second = _run(tmp_path)
    assert second.returncode == 0, second.stderr
    assert inventory_path.read_bytes() == first_bytes


def test_conda_names_that_differ_by_hyphen_and_underscore_remain_distinct(tmp_path: Path):
    def add_distinct_conda_name(base_prefix: Path, base_conda_list: Path) -> None:
        second = _conda_record("cached_property")
        second["build"] = "pyha770c72_2"
        second["url"] = (
            "https://conda.anaconda.org/conda-forge/noarch/"
            "cached_property-1.2.3-pyha770c72_2.conda"
        )
        (base_prefix / "conda-meta/cached_property.json").write_text(json.dumps(second))
        rows = json.loads(base_conda_list.read_text())
        rows.append(
            {
                "name": "cached_property",
                "version": "1.2.3",
                "build_string": "pyha770c72_2",
                "channel": "conda-forge",
            }
        )
        base_conda_list.write_text(json.dumps(rows))

    result = _run(tmp_path, setup=add_distinct_conda_name)

    assert result.returncode == 0, result.stderr
    inventory = json.loads((tmp_path / "inventory.json").read_text())
    conda_names = {
        package["name"]
        for package in inventory["packages"]
        if package["ecosystem"] == "conda"
    }
    assert {"base-package", "cached_property"} <= conda_names


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("missing-conda-license", "missing license"),
        ("foreign-conda-source", "unexpected Conda source URL"),
        ("final-conda-omission", "omitted conda-meta packages"),
        ("missing-conda-meta", "missing conda-meta record"),
        ("missing-pip-hash", "missing sha256"),
        ("missing-pip-artifact", "unresolved pip artifact sources"),
        ("invalid-pip-requested", "requested must be boolean"),
    ),
)
def test_inventory_fails_closed_on_incomplete_or_unbound_metadata(
    tmp_path: Path, mutation: str, expected_error: str
):
    (
        base_prefix,
        env_prefix,
        pip_report,
        dpkg_root,
        base_conda_list,
        env_conda_list,
        python_prefix,
        notice,
    ) = _write_inputs(tmp_path)
    if mutation == "missing-conda-license":
        record_path = base_prefix / "conda-meta/base-package.json"
        record = json.loads(record_path.read_text())
        record["license"] = ""
        record_path.write_text(json.dumps(record))
    elif mutation == "foreign-conda-source":
        record_path = env_prefix / "conda-meta/env-package.json"
        record = json.loads(record_path.read_text())
        record["url"] = "https://repo.anaconda.com/pkgs/main/env-package.conda"
        record_path.write_text(json.dumps(record))
    elif mutation == "final-conda-omission":
        base_conda_list.write_text("[]")
    elif mutation == "missing-conda-meta":
        rows = json.loads(env_conda_list.read_text())
        rows.insert(
            0,
            {
                "name": "not-in-conda-meta",
                "version": "1.0",
                "build_string": "py_0",
                "channel": "conda-forge",
            },
        )
        env_conda_list.write_text(json.dumps(rows))
    else:
        report = json.loads(pip_report.read_text())
        if mutation == "missing-pip-hash":
            report["install"][0]["download_info"]["archive_info"]["hashes"] = {}
        elif mutation == "missing-pip-artifact":
            report["install"] = report["install"][:1]
        elif mutation == "invalid-pip-requested":
            report["install"][0]["requested"] = "yes"
        pip_report.write_text(json.dumps(report))

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--conda-prefix",
            str(base_prefix),
            "--conda-prefix",
            str(env_prefix),
            "--pip-report",
            str(pip_report),
            "--conda-list",
            str(base_conda_list),
            "--conda-list",
            str(env_conda_list),
            "--python-prefix",
            str(python_prefix),
            "--dpkg-root",
            str(dpkg_root),
            "--notice",
            str(notice),
            "--source-commit",
            "c" * 40,
            "--base-image",
            "docker.io/condaforge/miniforge3:26.5.3-0@sha256:" + "d" * 64,
            "--platform",
            "linux/arm64",
            "--output",
            str(tmp_path / "inventory.json"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert expected_error in result.stderr
    assert not (tmp_path / "inventory.json").exists()


def test_inventory_fails_closed_when_dpkg_license_evidence_is_missing(tmp_path: Path):
    (
        base_prefix,
        env_prefix,
        pip_report,
        dpkg_root,
        base_conda_list,
        env_conda_list,
        python_prefix,
        notice,
    ) = _write_inputs(tmp_path)
    (dpkg_root / "usr/share/doc/runtime-package/copyright").unlink()

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--conda-prefix",
            str(base_prefix),
            "--conda-prefix",
            str(env_prefix),
            "--pip-report",
            str(pip_report),
            "--conda-list",
            str(base_conda_list),
            "--conda-list",
            str(env_conda_list),
            "--python-prefix",
            str(python_prefix),
            "--dpkg-root",
            str(dpkg_root),
            "--notice",
            str(notice),
            "--source-commit",
            "c" * 40,
            "--base-image",
            "docker.io/condaforge/miniforge3:26.5.3-0@sha256:" + "d" * 64,
            "--platform",
            "linux/arm64",
            "--output",
            str(tmp_path / "inventory.json"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "missing dpkg copyright evidence" in result.stderr


def test_inventory_uses_same_source_and_version_dpkg_copyright_evidence(tmp_path: Path):
    (
        base_prefix,
        env_prefix,
        pip_report,
        dpkg_root,
        base_conda_list,
        env_conda_list,
        python_prefix,
        notice,
    ) = _write_inputs(tmp_path)
    status_path = dpkg_root / "var/lib/dpkg/status"
    status_path.write_text(
        "Package: runtime-package\n"
        "Status: install ok installed\n"
        "Version: 2.4-1ubuntu1\n"
        "Architecture: arm64\n"
        "Source: runtime-source (2.4-1ubuntu1)\n\n"
        "Package: runtime-tools\n"
        "Status: install ok installed\n"
        "Version: 2.4-1ubuntu1\n"
        "Architecture: arm64\n"
        "Source: runtime-source (2.4-1ubuntu1)\n\n"
    )
    tools_license = dpkg_root / "usr/share/doc/runtime-tools/copyright"
    tools_license.parent.mkdir(parents=True)
    tools_license.write_text("License: MIT\n")
    (dpkg_root / "usr/share/doc/runtime-package/copyright").unlink()

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--conda-prefix",
            str(base_prefix),
            "--conda-prefix",
            str(env_prefix),
            "--pip-report",
            str(pip_report),
            "--conda-list",
            str(base_conda_list),
            "--conda-list",
            str(env_conda_list),
            "--python-prefix",
            str(python_prefix),
            "--dpkg-root",
            str(dpkg_root),
            "--notice",
            str(notice),
            "--source-commit",
            "c" * 40,
            "--base-image",
            "docker.io/condaforge/miniforge3:26.5.3-0@sha256:" + "d" * 64,
            "--platform",
            "linux/arm64",
            "--output",
            str(tmp_path / "inventory.json"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    inventory = json.loads((tmp_path / "inventory.json").read_text())
    dpkg_packages = [
        package for package in inventory["packages"] if package["ecosystem"] == "dpkg"
    ]
    assert [package["name"] for package in dpkg_packages] == [
        "runtime-package",
        "runtime-tools",
    ]
    assert {
        package["license_evidence_path"] for package in dpkg_packages
    } == {"/usr/share/doc/runtime-tools/copyright"}
