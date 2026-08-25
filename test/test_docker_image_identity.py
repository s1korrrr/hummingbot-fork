import json
import os
import re
import shlex
import shutil
import subprocess
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier
from typing import Iterator
from uuid import uuid4

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "Dockerfile"
WORKFLOW = ROOT / ".github/workflows/docker_buildx_workflow.yml"
SOURCE_SHA = "6a8b5c37ea5bdfbdfc11127e3fa0a05a35e386ca"  # noqa: mock
PINNED_MINIFORGE = "docker.io/condaforge/miniforge3:26.5.3-0@sha256:b230e51ddd0ebc4b08bb44d7442c20f9443973e3ddd09cf7f621b04fe449ca66"  # noqa: mock
ENVIRONMENT_FILES = (ROOT / "setup/environment.yml", ROOT / "setup/environment_dydx.yml")
PINNED_ACTIONS = {
    "actions/checkout": "11bd71901bbe5b1630ceea73d27597364c9af683",  # noqa: mock
    "docker/setup-qemu-action": "29109295f81e9208d7d86ff1c6c12d2833863392",  # noqa: mock
    "docker/setup-buildx-action": "e468171a9de216ec08956ac3ada2f0791b6bd435",  # noqa: mock
    "docker/login-action": "5e57cd118135c172c3672efd75eb46360885c0ef",  # noqa: mock
    "docker/build-push-action": "263435318d21b8e681c14492fe198d362a7d2c83",  # noqa: mock
}
PINNED_RUNNER = "ubuntu-24.04"
PINNED_QEMU_IMAGE = "docker.io/tonistiigi/binfmt:qemu-v10.0.4@sha256:8f58e6214f4cc9dc83ce8f5acad1ece508eb6b20e696a8c1e9f274481982c541"  # noqa: mock
PINNED_BUILDX_VERSION = "v0.17.1"
PINNED_BUILDKIT_IMAGE = "moby/buildkit:v0.17.3@sha256:b2498ea62630c147e98f96145c2988a74720700b250e4cf7a4328e14f5b99fc3"  # noqa: mock
HUMMINGBOT_OCI_LABELS = {
    "org.opencontainers.image.title": "Hummingbot",
    "org.opencontainers.image.description": (
        "Open-source framework for designing and deploying automated trading strategies."
    ),
    "org.opencontainers.image.url": "https://hummingbot.org",
    "org.opencontainers.image.source": "https://github.com/hummingbot/hummingbot",
    "org.opencontainers.image.licenses": "Apache-2.0",
}


def _dockerfile() -> str:
    return DOCKERFILE.read_text()


def _workflow() -> str:
    return WORKFLOW.read_text()


def _workflow_document() -> dict:
    return yaml.load(_workflow(), Loader=yaml.BaseLoader)


def _release_validation_script() -> str:
    workflow = _workflow_document()
    return workflow["jobs"]["validate_release"]["steps"][0]["run"]


def _workflow_step(job_name: str, step_id: str) -> dict:
    steps = _workflow_document()["jobs"][job_name]["steps"]
    return next(step for step in steps if step.get("id") == step_id)


def _workflow_step_by_action(job_name: str, action: str) -> dict:
    steps = _workflow_document()["jobs"][job_name]["steps"]
    return next(step for step in steps if step.get("uses", "").startswith(f"{action}@"))


def _make_context(tmp_path: Path, name: str) -> Path:
    repo = tmp_path / name
    repo.mkdir()
    (repo / "Makefile").write_text((ROOT / "Makefile").read_text())
    (repo / "Dockerfile").write_text("FROM scratch\n")
    (repo / ".gitignore").write_text("*.so\n")
    (repo / "tracked.txt").write_text("clean\n")
    for command in (
        ["git", "init", "--quiet"],
        ["git", "config", "user.name", "Image identity test"],
        ["git", "config", "user.email", "image-identity-test@example.invalid"],
        ["git", "add", "Makefile", "Dockerfile", ".gitignore", "tracked.txt"],
        ["git", "commit", "--quiet", "-m", "initial context"],
    ):
        subprocess.run(command, cwd=repo, check=True)
    return repo


@dataclass(frozen=True)
class _BuiltImage:
    result: subprocess.CompletedProcess[str]
    image_id: str | None
    image_tag: str


@contextmanager
def _build_image(
    artifact_dir: Path,
    *,
    role: str,
    target: str,
    progress: str,
    build_args: tuple[str, ...],
) -> Iterator[_BuiltImage]:
    invocation = uuid4().hex
    image_tag = f"hummingbot:qte-{role}-test-{invocation}"
    image_id_file = artifact_dir / f"{role}-{invocation}.iid"
    result: subprocess.CompletedProcess[str] | None = None
    try:
        result = subprocess.run(
            [
                "docker",
                "buildx",
                "build",
                f"--progress={progress}",
                "--file",
                str(DOCKERFILE),
                "--target",
                target,
                "--load",
                "--iidfile",
                str(image_id_file),
                "--tag",
                image_tag,
                *build_args,
                str(ROOT),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        image_id = image_id_file.read_text().strip() if image_id_file.exists() else None
        if image_id is not None and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
            raise ValueError(f"BuildKit iidfile must contain an exact sha256 image ID; got {image_id!r}")
        if result.returncode == 0 and image_id is None:
            raise ValueError("BuildKit did not produce an exact sha256 image ID")
        yield _BuiltImage(result=result, image_id=image_id, image_tag=image_tag)
    finally:
        try:
            cleanup = subprocess.run(
                ["docker", "image", "rm", image_tag],
                check=False,
                capture_output=True,
                text=True,
            )
            if result is not None and result.returncode == 0 and cleanup.returncode != 0:
                raise RuntimeError(f"Failed to remove isolated Docker test tag {image_tag}: {cleanup.stderr}")
        finally:
            image_id_file.unlink(missing_ok=True)


def _build_metadata(artifact_dir: Path, *build_args: str) -> AbstractContextManager[_BuiltImage]:
    return _build_image(
        artifact_dir,
        role="image-identity-metadata",
        target="image-metadata",
        progress="quiet",
        build_args=build_args,
    )


def _build_bootstrap(artifact_dir: Path, target: str) -> AbstractContextManager[_BuiltImage]:
    return _build_image(
        artifact_dir,
        role=f"miniforge-{target}",
        target=target,
        progress="quiet",
        build_args=(
            "--build-arg",
            f"SOURCE_COMMIT={SOURCE_SHA}",
            "--build-arg",
            f"COMMIT={SOURCE_SHA}",
        ),
    )


def _build_environment(artifact_dir: Path) -> AbstractContextManager[_BuiltImage]:
    return _build_image(
        artifact_dir,
        role="condaforge-environment",
        target="builder-environment",
        progress="plain",
        build_args=(
            "--build-arg",
            f"SOURCE_COMMIT={SOURCE_SHA}",
            "--build-arg",
            f"COMMIT={SOURCE_SHA}",
        ),
    )


def _image_labels(image_id: str | None) -> dict[str, str]:
    assert image_id is not None
    result = subprocess.run(
        ["docker", "image", "inspect", image_id, "--format", "{{json .Config.Labels}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _image_environment(image_id: str | None) -> list[str]:
    assert image_id is not None
    result = subprocess.run(
        ["docker", "image", "inspect", image_id, "--format", "{{json .Config.Env}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _make_fake_docker(tmp_path: Path) -> tuple[Path, Path]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    removed_tags = tmp_path / "removed-tags"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = buildx ]; then\n"
        "  shift\n"
        "  while [ \"$#\" -gt 0 ]; do\n"
        "    if [ \"$1\" = --iidfile ]; then\n"
        "      printf '%s\\n' \"$FAKE_DOCKER_IID\" > \"$2\"\n"
        "      shift 2\n"
        "    else\n"
        "      shift\n"
        "    fi\n"
        "  done\n"
        "  exit \"$FAKE_DOCKER_BUILD_EXIT\"\n"
        "fi\n"
        "if [ \"$1\" = image ] && [ \"$2\" = rm ]; then\n"
        "  printf '%s\\n' \"$3\" >> \"$FAKE_DOCKER_REMOVED_TAGS\"\n"
        "  exit 0\n"
        "fi\n"
        "exit 99\n"
    )
    fake_docker.chmod(0o755)
    return fake_bin, removed_tags


@pytest.mark.parametrize(
    ("helper_name", "target"),
    [
        ("metadata", None),
        ("bootstrap", "builder-bootstrap"),
        ("environment", None),
    ],
)
def test_every_docker_build_helper_cleans_its_exact_tag_and_iidfile_after_failure(
    tmp_path: Path, monkeypatch, helper_name: str, target: str | None
):
    fake_bin, removed_tags = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", f"sha256:{'a' * 64}")
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "7")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))

    if helper_name == "metadata":
        invocation = _build_metadata(tmp_path)
    elif helper_name == "bootstrap":
        assert target is not None
        invocation = _build_bootstrap(tmp_path, target)
    else:
        invocation = _build_environment(tmp_path)

    with invocation as image:
        assert image.result.returncode == 7

    assert list(tmp_path.glob("*.iid")) == []
    cleaned_tags = removed_tags.read_text().splitlines()
    assert len(cleaned_tags) == 1
    assert cleaned_tags[0] == image.image_tag


@pytest.mark.parametrize(
    "malformed_iid",
    [
        "",
        "sha256:ABCDEF",
        f"sha256:{'A' * 64}",
        "a" * 64,
        f"sha256:{'a' * 63}",
        f"sha256:{'a' * 64}extra",
    ],
)
def test_docker_build_rejects_malformed_iid_and_cleans_exact_artifacts(
    tmp_path: Path, monkeypatch, malformed_iid: str
):
    fake_bin, removed_tags = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", malformed_iid)
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "0")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))

    with pytest.raises(ValueError, match="exact sha256 image ID"):
        with _build_metadata(tmp_path):
            pytest.fail("malformed image ID must fail before the helper yields")

    assert list(tmp_path.glob("*.iid")) == []
    cleaned_tags = removed_tags.read_text().splitlines()
    assert len(cleaned_tags) == 1
    assert cleaned_tags[0].startswith("hummingbot:qte-image-identity-metadata-test-")


def test_docker_build_helpers_use_collision_proof_tags_and_leave_zero_iidfiles(tmp_path: Path, monkeypatch):
    fake_bin, removed_tags = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", f"sha256:{'b' * 64}")
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "0")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))

    observed_tags = []
    for invocation in (
        _build_metadata(tmp_path),
        _build_bootstrap(tmp_path, "runtime-bootstrap"),
        _build_environment(tmp_path),
    ):
        with invocation as image:
            assert image.image_id == f"sha256:{'b' * 64}"
            observed_tags.append(image.image_tag)

    assert len(set(observed_tags)) == 3
    assert removed_tags.read_text().splitlines() == observed_tags
    assert list(tmp_path.glob("*.iid")) == []


def test_concurrent_metadata_builds_keep_distinct_labels_bound_to_immutable_ids(tmp_path: Path):
    assert shutil.which("docker"), "BuildKit is required for image identity verification"
    first_source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    second_source_commit = SOURCE_SHA
    assert first_source_commit != second_source_commit
    both_builds_finished = Barrier(2)

    def build_and_inspect(source_commit: str, branch: str) -> tuple[str, str, dict[str, str], list[str]]:
        with _build_metadata(
            tmp_path,
            "--build-arg",
            f"SOURCE_COMMIT={source_commit}",
            "--build-arg",
            f"COMMIT={source_commit}",
            "--build-arg",
            f"BRANCH={branch}",
            "--build-arg",
            "BUILD_DATE=2026-08-25T00:00:00Z",
        ) as image:
            assert image.result.returncode == 0, image.result.stderr
            assert image.image_id is not None
            both_builds_finished.wait(timeout=30)
            return image.image_id, image.image_tag, _image_labels(image.image_id), _image_environment(image.image_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(build_and_inspect, first_source_commit, "isolation-first")
        second = executor.submit(build_and_inspect, second_source_commit, "isolation-second")
        first_image_id, first_image_tag, first_labels, first_environment = first.result()
        second_image_id, second_image_tag, second_labels, second_environment = second.result()

    assert first_image_id != second_image_id
    assert first_image_tag != second_image_tag
    assert first_labels["commit"] == first_source_commit
    assert first_labels["org.opencontainers.image.revision"] == first_source_commit
    assert first_labels["branch"] == "isolation-first"
    assert f"COMMIT_SHA={first_source_commit}" in first_environment
    assert second_labels["commit"] == second_source_commit
    assert second_labels["org.opencontainers.image.revision"] == second_source_commit
    assert second_labels["branch"] == "isolation-second"
    assert f"COMMIT_SHA={second_source_commit}" in second_environment
    for image_tag in (first_image_tag, second_image_tag):
        cleanup_check = subprocess.run(
            ["docker", "image", "inspect", image_tag],
            check=False,
            capture_output=True,
            text=True,
        )
        assert cleanup_check.returncode != 0
    assert list(tmp_path.glob("*.iid")) == []


@pytest.mark.parametrize(
    ("name", "build_args"),
    [
        ("omitted", ()),
        ("empty", ("--build-arg", "SOURCE_COMMIT=", "--build-arg", "COMMIT=")),
        (
            "malformed",
            ("--build-arg", "SOURCE_COMMIT=not-a-commit", "--build-arg", "COMMIT=not-a-commit"),
        ),
        (
            "mismatched",
            (
                "--build-arg",
                f"SOURCE_COMMIT={SOURCE_SHA}",
                "--build-arg",
                f"COMMIT={SOURCE_SHA[:-1]}b",
            ),
        ),
    ],
)
def test_buildkit_metadata_target_rejects_invalid_source_identity(
    tmp_path: Path, name: str, build_args: tuple[str, ...]
):
    assert shutil.which("docker"), "BuildKit is required for image identity verification"

    with _build_metadata(tmp_path, *build_args) as image:
        result = image.result

    assert result.returncode != 0, name
    assert 'process "/bin/sh -c test -n' in f"{result.stdout}\n{result.stderr}"


def test_buildkit_metadata_target_replaces_base_identity_with_exact_hummingbot_build_identity(tmp_path: Path):
    assert shutil.which("docker"), "BuildKit is required for image identity verification"
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()

    with _build_metadata(
        tmp_path,
        "--build-arg",
        f"SOURCE_COMMIT={source_commit}",
        "--build-arg",
        f"COMMIT={source_commit}",
        "--build-arg",
        "BRANCH=metadata-test",
        "--build-arg",
        "BUILD_DATE=2026-08-25T00:00:00Z",
    ) as image:
        assert image.result.returncode == 0, image.result.stderr
        labels = _image_labels(image.image_id)
    assert labels["org.opencontainers.image.revision"] == source_commit
    assert labels["commit"] == source_commit
    assert labels["branch"] == "metadata-test"
    assert labels["date"] == "2026-08-25T00:00:00Z"
    assert labels["org.opencontainers.image.created"] == "2026-08-25T00:00:00Z"
    assert labels["org.opencontainers.image.version"] == source_commit
    for name, value in HUMMINGBOT_OCI_LABELS.items():
        assert labels[name] == value


def test_dockerfile_declares_a_shared_metadata_target_and_identity_guard():
    dockerfile = _dockerfile()

    assert "FROM identity-check AS image-metadata" in dockerfile
    assert "FROM image-metadata AS runtime-bootstrap" in dockerfile
    assert "FROM runtime-bootstrap AS release" in dockerfile
    assert "ARG SOURCE_COMMIT" in dockerfile
    assert 'test -n "${SOURCE_COMMIT}"' in dockerfile
    assert "^[0-9a-f]{40}$" in dockerfile
    assert 'test "${COMMIT}" = "${SOURCE_COMMIT}"' in dockerfile


def test_dockerfile_emits_oci_revision_and_retains_matching_legacy_commit_metadata():
    dockerfile = _dockerfile()

    assert 'org.opencontainers.image.revision="${SOURCE_COMMIT}"' in dockerfile
    assert "commit=${COMMIT}" in dockerfile
    assert "ENV COMMIT_SHA=${COMMIT}" in dockerfile
    assert "LABEL commit=${SOURCE_COMMIT}" not in dockerfile


def test_dockerfile_pins_every_miniforge_base_image_to_the_recorded_digest():
    dockerfile = _dockerfile()

    from_images = re.findall(r"^FROM\s+([^\s]+)", dockerfile, flags=re.MULTILINE)
    assert [image for image in from_images if image.startswith("docker.io/condaforge/")] == [PINNED_MINIFORGE]
    assert not any(image.startswith("anaconda/") for image in from_images)
    assert not any(image.startswith("continuumio/") for image in from_images)
    assert not re.search(r"^FROM\s+[^\s]*:latest", dockerfile, flags=re.MULTILINE)


def test_canonical_environment_files_are_conda_forge_only():
    for path in ENVIRONMENT_FILES:
        document = yaml.safe_load(path.read_text())

        assert document["channels"] == ["conda-forge", "nodefaults"], path


def test_dockerfile_copies_the_miniforge_prefix_into_the_release_image():
    dockerfile = _dockerfile()

    assert "COPY --from=builder /opt/conda/ /opt/conda/" in dockerfile
    assert "/opt/miniconda3/" not in dockerfile


def test_dockerfile_runs_pip_and_native_build_inside_the_hummingbot_environment():
    dockerfile = _dockerfile()

    assert "conda run -n hummingbot python -m pip install" in dockerfile
    assert "conda run -n hummingbot --no-capture-output python setup.py build_ext" in dockerfile
    assert "RUN python3 -m pip install" not in dockerfile
    assert "RUN python3 setup.py build_ext" not in dockerfile


def test_release_explicitly_preserves_tini_and_execs_the_hummingbot_python():
    dockerfile = _dockerfile()

    assert 'ENTRYPOINT ["/usr/bin/tini", "--"]' in dockerfile
    assert (
        'CMD ["/bin/bash", "-lc", "source /opt/conda/etc/profile.d/conda.sh && '
        'conda activate hummingbot && exec python '
        './bin/hummingbot_quickstart.py 2>> ./logs/errors.log"]' in dockerfile
    )


@pytest.mark.parametrize("target", ("builder-bootstrap", "runtime-bootstrap"))
def test_miniforge_bootstrap_targets_preserve_the_required_tool_contract(tmp_path: Path, target: str):
    assert shutil.which("docker"), "BuildKit is required for Miniforge bootstrap verification"

    with _build_bootstrap(tmp_path, target) as image:
        assert image.result.returncode == 0, image.result.stderr
        probe = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                image.image_id,
                "/bin/bash",
                "-lc",
                "test \"${CONDA_DIR}\" = /opt/conda "
                "&& test \"$(type -P conda)\" = /opt/conda/bin/conda "
                "&& command -v git "
                "&& command -v wget "
                "&& test \"$(ps -o comm= -p 1 | tr -d ' ')\" = tini",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    assert probe.returncode == 0, probe.stderr


def test_conda_environment_records_conda_forge_package_urls_and_selected_python(tmp_path: Path):
    assert shutil.which("docker"), "BuildKit is required for dependency provenance verification"

    with _build_environment(tmp_path) as image:
        assert image.result.returncode == 0, image.result.stderr
        probe = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "/bin/bash",
                image.image_id,
                "-lc",
                "set -euo pipefail; "
                "provenance=/opt/conda/conda-meta/hummingbot-package-urls.txt; "
                "python_record=/opt/conda/conda-meta/hummingbot-python-version.txt; "
                "test -s \"${provenance}\"; "
                "test -s \"${python_record}\"; "
                "! grep -Fq repo.anaconda.com \"${provenance}\"; "
                "grep -Eq '^https://conda.anaconda.org/conda-forge/' \"${provenance}\"; "
                "bad_urls=$(grep -E '^https?://' \"${provenance}\" | "
                "grep -Ev '^https://conda\\.anaconda\\.org/conda-forge/' || true); "
                "test -z \"${bad_urls}\"; "
                "test \"$(cat \"${python_record}\")\" = \"$(conda run -n hummingbot python -c "
                "'import platform; print(platform.python_version())')\"; "
                "cat \"${python_record}\"",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    assert probe.returncode == 0, probe.stderr
    assert re.fullmatch(r"3\.1[0-9]\.[0-9]+\n", probe.stdout)


def test_runtime_command_activates_the_environment_and_preserves_tini_signal_forwarding(tmp_path: Path):
    assert shutil.which("docker"), "Docker is required for runtime environment verification"
    with _build_environment(tmp_path) as image:
        assert image.result.returncode == 0, image.result.stderr

        cmd_match = re.search(r"^CMD (\[.*\])$", _dockerfile(), flags=re.MULTILINE)
        assert cmd_match is not None
        cmd = json.loads(cmd_match.group(1))
        assert cmd[:2] == ["/bin/bash", "-lc"]
        shell_command = cmd[2]
        runtime_tail = "exec python ./bin/hummingbot_quickstart.py 2>> ./logs/errors.log"
        assert shell_command.endswith(runtime_tail)

        probe_program = (
            "import json, os, shutil, sys; "
            "print(json.dumps({'sys_executable': sys.executable, "
            "'which_python': shutil.which('python'), "
            "'conda_prefix': os.environ.get('CONDA_PREFIX'), "
            "'path_head': os.environ['PATH'].split(':', 1)[0]}))"
        )
        probe_command = shell_command.removesuffix(runtime_tail) + f"exec python -c {shlex.quote(probe_program)}"
        probe = subprocess.run(
            ["docker", "run", "--rm", image.image_id, "/bin/bash", "-lc", probe_command],
            check=False,
            capture_output=True,
            text=True,
        )
        assert probe.returncode == 0, probe.stderr
        assert json.loads(probe.stdout) == {
            "sys_executable": "/opt/conda/envs/hummingbot/bin/python",
            "which_python": "/opt/conda/envs/hummingbot/bin/python",
            "conda_prefix": "/opt/conda/envs/hummingbot",
            "path_head": "/opt/conda/envs/hummingbot/bin",
        }

        signal_program = "import time; time.sleep(300)"
        signal_command = shell_command.removesuffix(runtime_tail) + f"exec python -c {shlex.quote(signal_program)}"
        container = subprocess.run(
            ["docker", "run", "--detach", image.image_id, "/bin/bash", "-lc", signal_command],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        try:
            deadline = time.monotonic() + 5
            processes = []
            while time.monotonic() < deadline:
                process_table = subprocess.run(
                    ["docker", "top", container, "-eo", "pid,ppid,comm,args"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.splitlines()
                processes = [line.split(maxsplit=3) for line in process_table[1:]]
                if any(process[2].startswith("python") for process in processes):
                    break
                time.sleep(0.1)
            tini = next(process for process in processes if process[2] == "tini")
            python = next(process for process in processes if process[2].startswith("python"))
            assert python[1] == tini[0]

            subprocess.run(["docker", "stop", "--time", "3", container], check=True, capture_output=True, text=True)
            state = subprocess.run(
                ["docker", "inspect", container, "--format", "{{json .State}}"],
                check=True,
                capture_output=True,
                text=True,
            )
            state_document = json.loads(state.stdout)
            assert state_document["Status"] == "exited"
            assert state_document["ExitCode"] == 143
            assert state_document["OOMKilled"] is False
        finally:
            subprocess.run(["docker", "rm", "--force", container], check=False, capture_output=True, text=True)


def test_workflow_pr_verification_is_non_pushing_and_binds_pr_head_sha():
    workflow = _workflow()
    document = _workflow_document()

    assert "pull_request:" in workflow
    assert "release" not in document["on"]
    assert "workflow_dispatch" in document["on"]
    assert "types: [closed]" not in workflow
    assert "github.event.pull_request.merged" not in workflow
    assert "github.event.pull_request.head.sha" in workflow
    assert "SOURCE_COMMIT=${{ github.event.pull_request.head.sha }}" in workflow
    assert "COMMIT=${{ github.event.pull_request.head.sha }}" in workflow

    pr_job = workflow.split("  validate_release:", 1)[0]
    assert "docker/login-action" not in pr_job
    assert "push: false" in pr_job
    assert "push: true" not in pr_job


def test_workflow_release_publication_is_manual_confirmed_and_uses_immutable_source_identity():
    workflow = _workflow()
    document = _workflow_document()

    assert "workflow_dispatch:" in workflow
    assert "confirm_release:" in workflow
    assert "source_commit:" in workflow
    assert "needs.validate_release.outputs.confirmed == 'true'" in workflow
    assert "needs.validate_release.outputs.source_ref == 'refs/heads/master'" in workflow
    assert "=~ ^[0-9a-f]{40}$" in workflow
    assert "WORKFLOW_COMMIT: ${{ github.sha }}" in workflow
    assert "WORKFLOW_REF: ${{ github.ref }}" in workflow
    assert '[[ "${REQUESTED_SOURCE_COMMIT}" != "${WORKFLOW_COMMIT}" ]]' in workflow
    assert "source_commit=${WORKFLOW_COMMIT}" in workflow
    assert document["jobs"]["validate_release"]["outputs"]["source_ref"] == "${{ steps.confirm.outputs.source_ref }}"
    assert "ref: ${{ needs.validate_release.outputs.source_commit }}" in workflow
    assert "SOURCE_COMMIT=${{ needs.validate_release.outputs.source_commit }}" in workflow
    assert "COMMIT=${{ needs.validate_release.outputs.source_commit }}" in workflow


def test_workflow_publish_job_requires_protected_release_environment():
    document = _workflow_document()

    publish_job = document["jobs"]["publish_release"]
    assert publish_job["environment"]["name"] == "hummingbot-image-release"


def test_workflow_pins_runner_qemu_buildx_and_buildkit_in_every_image_job():
    document = _workflow_document()
    for job_name in ("verify_pr", "publish_release"):
        job = document["jobs"][job_name]
        qemu = _workflow_step_by_action(job_name, "docker/setup-qemu-action")
        buildx = _workflow_step_by_action(job_name, "docker/setup-buildx-action")

        assert job["runs-on"] == PINNED_RUNNER
        assert qemu["with"]["image"] == PINNED_QEMU_IMAGE
        assert ":latest" not in qemu["with"]["image"]
        assert re.fullmatch(r".+:[^@]+@sha256:[0-9a-f]{64}", qemu["with"]["image"])
        assert buildx["with"]["version"] == PINNED_BUILDX_VERSION
        assert buildx["with"]["driver-opts"] == f"image={PINNED_BUILDKIT_IMAGE}"
        assert ":latest" not in buildx["with"]["driver-opts"]
        assert re.fullmatch(r"image=.+:[^@]+@sha256:[0-9a-f]{64}", buildx["with"]["driver-opts"])


def test_workflow_uses_checked_out_commit_timestamps_not_event_timestamps(tmp_path: Path):
    workflow = _workflow()
    assert "github.event.pull_request.updated_at" not in workflow
    assert "github.event.repository.updated_at" not in workflow

    metadata_script = _workflow_step("verify_pr", "source_metadata")["run"]
    repo = _make_context(tmp_path, "timestamp")
    expected = subprocess.run(
        ["git", "show", "-s", "--format=%cI", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()
    first_output = tmp_path / "first-output"
    second_output = tmp_path / "second-output"

    for output in (first_output, second_output):
        result = subprocess.run(
            ["bash", "-euo", "pipefail", "-c", metadata_script],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "GITHUB_OUTPUT": str(output)},
        )
        assert result.returncode == 0, result.stderr

    assert first_output.read_text().strip() == f"build_date={expected}"
    assert second_output.read_text() == first_output.read_text()


def test_workflow_publish_is_digest_only_without_a_semantic_tag():
    publish_job = _workflow_document()["jobs"]["publish_release"]
    image_step = _workflow_step("publish_release", "image")
    digest_step = _workflow_step("publish_release", "validate_digest")

    assert "tags" not in image_step["with"]
    assert image_step["with"]["push"] == "true"
    assert "name=hummingbot/hummingbot" in image_step["with"]["outputs"]
    assert "push-by-digest=true" in image_step["with"]["outputs"]
    assert "name-canonical=true" in image_step["with"]["outputs"]
    assert publish_job["outputs"]["image_digest"] == "${{ steps.validate_digest.outputs.image_digest }}"
    assert digest_step["env"]["IMAGE_DIGEST"] == "${{ steps.image.outputs.digest }}"
    assert "steps.image.outputs.digest" not in digest_step["run"]


@pytest.mark.parametrize(
    ("name", "digest", "expected_success"),
    [
        ("valid", f"sha256:{'a' * 64}", True),
        ("malformed", "sha256:not-a-digest", False),
        ("empty", "", False),
        ("shell_metacharacters", "sha256:$(touch should-not-run)", False),
    ],
)
def test_workflow_digest_validation_is_env_only_and_fail_closed(
    tmp_path: Path, name: str, digest: str, expected_success: bool
):
    step = _workflow_step("publish_release", "validate_digest")
    marker = tmp_path / "should-not-run"
    output = tmp_path / f"{name}-output"
    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", step["run"]],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "IMAGE_DIGEST": digest, "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(tmp_path / "summary")},
    )

    assert result.returncode == (0 if expected_success else 1), name
    assert not marker.exists()
    if expected_success:
        assert output.read_text().splitlines() == [f"image_digest={digest}"]
    else:
        assert "must be an exact sha256 digest" in result.stderr


def test_workflow_validation_rejects_the_old_unhardened_6a8_source_commit(tmp_path: Path):
    producer_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    assert producer_commit != SOURCE_SHA

    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", _release_validation_script()],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "CONFIRM_RELEASE": "true",
            "REQUESTED_SOURCE_COMMIT": SOURCE_SHA,
            "WORKFLOW_COMMIT": producer_commit,
            "WORKFLOW_REF": "refs/heads/master",
            "GITHUB_OUTPUT": str(tmp_path / "github-output"),
        },
    )

    assert result.returncode != 0
    assert "source_commit must equal the checked-out hardened workflow commit" in result.stderr


@pytest.mark.parametrize(
    "workflow_ref",
    [
        "",
        "master",
        "refs/heads/development",
        "refs/heads/feature/image-release",
        "refs/tags/v2.0.0",
        "refs/pull/42/merge",
        "refs/heads/master/extra",
    ],
)
def test_workflow_validation_rejects_every_non_master_release_ref(tmp_path: Path, workflow_ref: str):
    producer_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()

    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", _release_validation_script()],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "CONFIRM_RELEASE": "true",
            "REQUESTED_SOURCE_COMMIT": producer_commit,
            "WORKFLOW_COMMIT": producer_commit,
            "WORKFLOW_REF": workflow_ref,
            "GITHUB_OUTPUT": str(tmp_path / "github-output"),
        },
    )

    assert result.returncode != 0, workflow_ref
    assert "Release publication requires the exact reviewed default ref refs/heads/master" in result.stderr


def test_workflow_validation_accepts_only_the_checked_out_producer_commit(tmp_path: Path):
    producer_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    output = tmp_path / "github-output"

    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", _release_validation_script()],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "CONFIRM_RELEASE": "true",
            "REQUESTED_SOURCE_COMMIT": producer_commit,
            "WORKFLOW_COMMIT": producer_commit,
            "WORKFLOW_REF": "refs/heads/master",
            "GITHUB_OUTPUT": str(output),
        },
    )

    assert result.returncode == 0, result.stderr
    assert output.read_text().splitlines() == [
        "confirmed=true",
        f"source_commit={producer_commit}",
        "source_ref=refs/heads/master",
    ]


def test_make_build_command_binds_one_exact_head_sha_to_both_metadata_arguments():
    makefile = (ROOT / "Makefile").read_text()

    assert "source_commit=\"$$(git rev-parse --verify HEAD)\"" in makefile
    assert "git archive --format=tar \"$$source_commit\"" in makefile
    assert "--build-arg \"SOURCE_COMMIT=$$source_commit\"" in makefile
    assert "--build-arg \"COMMIT=$$source_commit\"" in makefile
    assert "--build-arg \"BUILD_DATE=$$build_date\"" in makefile
    assert "SOURCE_COMMIT ?=" not in makefile
    assert "BUILD_DATE ?=" not in makefile
    assert "unexport SOURCE_COMMIT BUILD_DATE" in makefile
    assert "git clean" not in makefile


def test_make_build_requires_a_clean_context_before_labeling_head():
    makefile = (ROOT / "Makefile").read_text()
    assert "build: verify-image-context" in makefile


@pytest.mark.parametrize("state", ["staged", "unstaged", "untracked"])
def test_make_context_gate_rejects_every_dirty_git_state(tmp_path: Path, state: str):
    repo = _make_context(tmp_path, state)
    if state == "staged":
        (repo / "tracked.txt").write_text("staged change\n")
        subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    elif state == "unstaged":
        (repo / "tracked.txt").write_text("unstaged change\n")
    else:
        (repo / "untracked.txt").write_text("untracked change\n")

    result = subprocess.run(
        ["make", "verify-image-context"], cwd=repo, check=False, capture_output=True, text=True
    )

    assert result.returncode != 0, state
    assert "Image build requires a clean Git worktree." in result.stderr


def test_make_context_gate_accepts_clean_git_state(tmp_path: Path):
    repo = _make_context(tmp_path, "clean")

    result = subprocess.run(
        ["make", "verify-image-context"], cwd=repo, check=False, capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr


def test_make_build_streams_a_canonical_git_archive_without_ignored_artifacts(tmp_path: Path):
    repo = _make_context(tmp_path, "archive")
    (repo / "ignored-native.so").write_bytes(b"ignored native artifact")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    context = tmp_path / "docker-context.tar"
    arguments = tmp_path / "docker-arguments.txt"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$@\" > \"$FAKE_DOCKER_ARGUMENTS\"\n"
        "cat > \"$FAKE_DOCKER_CONTEXT\"\n"
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        ["make", "build"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_CONTEXT": str(context),
            "FAKE_DOCKER_ARGUMENTS": str(arguments),
        },
    )

    assert result.returncode == 0, result.stderr
    with tarfile.open(context) as archive:
        names = archive.getnames()
    assert "Dockerfile" in names
    assert "ignored-native.so" not in names
    assert arguments.read_text().splitlines()[-1] == "-"


def test_make_build_archives_the_resolved_source_commit_not_a_racing_head(tmp_path: Path):
    repo = _make_context(tmp_path, "archive-race")
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()
    source_date = subprocess.run(
        ["git", "show", "-s", "--format=%cI", source_commit],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (repo / "tracked.txt").write_text("later head bytes\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "advance head"], cwd=repo, check=True)
    advanced_head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()
    subprocess.run(["git", "checkout", "--quiet", "--detach", source_commit], cwd=repo, check=True)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    context = tmp_path / "docker-context.tar"
    arguments = tmp_path / "docker-arguments.txt"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$@\" > \"$FAKE_DOCKER_ARGUMENTS\"\n"
        "cat > \"$FAKE_DOCKER_CONTEXT\"\n"
    )
    fake_docker.chmod(0o755)
    fake_git = fake_bin / "git"
    fake_git.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = rev-parse ] && [ \"$2\" = --verify ] && [ \"$3\" = HEAD ]; then\n"
        "  \"$REAL_GIT\" rev-parse --verify HEAD\n"
        "  \"$REAL_GIT\" checkout --quiet --detach \"$RACE_ADVANCED_HEAD\"\n"
        "  exit 0\n"
        "fi\n"
        "exec \"$REAL_GIT\" \"$@\"\n"
    )
    fake_git.chmod(0o755)

    result = subprocess.run(
        ["make", "build"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_CONTEXT": str(context),
            "FAKE_DOCKER_ARGUMENTS": str(arguments),
            "REAL_GIT": shutil.which("git") or "git",
            "RACE_ADVANCED_HEAD": advanced_head,
        },
    )

    assert result.returncode == 0, result.stderr
    with tarfile.open(context) as archive:
        tracked = archive.extractfile("tracked.txt")
        assert tracked is not None
        assert tracked.read().decode() == "clean\n"
    arguments_text = arguments.read_text()
    assert f"SOURCE_COMMIT={source_commit}" in arguments_text
    assert f"COMMIT={source_commit}" in arguments_text
    assert f"BUILD_DATE={source_date}" in arguments_text


def test_make_ignores_recursive_and_shell_metacharacter_identity_overrides(tmp_path: Path):
    repo = _make_context(tmp_path, "override-injection")
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()
    source_date = subprocess.run(
        ["git", "show", "-s", "--format=%cI", source_commit],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    context = tmp_path / "docker-context.tar"
    arguments = tmp_path / "docker-arguments.txt"
    marker = tmp_path / "override-executed"
    shell_metacharacter_marker = tmp_path / "shell-metacharacter-executed"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$@\" > \"$FAKE_DOCKER_ARGUMENTS\"\n"
        "cat > \"$FAKE_DOCKER_CONTEXT\"\n"
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        [
            "make",
            "build",
            f"SOURCE_COMMIT=$(shell touch {marker})",
            f"BUILD_DATE=2026-08-25T00:00:00Z;touch {shell_metacharacter_marker}",
        ],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_CONTEXT": str(context),
            "FAKE_DOCKER_ARGUMENTS": str(arguments),
        },
    )

    assert result.returncode == 0, result.stderr
    assert not marker.exists()
    assert not shell_metacharacter_marker.exists()
    with tarfile.open(context) as archive:
        assert "Dockerfile" in archive.getnames()
    arguments_text = arguments.read_text()
    assert f"SOURCE_COMMIT={source_commit}" in arguments_text
    assert f"COMMIT={source_commit}" in arguments_text
    assert f"BUILD_DATE={source_date}" in arguments_text


def test_workflow_actions_are_pinned_to_immutable_commits():
    workflow = _workflow()

    for action, revision in PINNED_ACTIONS.items():
        assert f"uses: {action}@{revision}" in workflow
    action_references = re.findall(r"^\s*uses:\s*([^\s#]+)", workflow, flags=re.MULTILINE)
    assert action_references
    for reference in action_references:
        action, revision = reference.split("@", maxsplit=1)
        assert action
        assert re.fullmatch(r"[0-9a-f]{40}", revision), reference
