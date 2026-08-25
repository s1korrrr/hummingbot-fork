import importlib.util
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier, Event
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
THIRD_PARTY_NOTICES = ROOT / "THIRD_PARTY_NOTICES.md"
IMAGE_LICENSE_GATE_DOC = ROOT / "docs/release/image-license-gate.md"
RELEASE_RUNTIME_DEPENDENCY_SMOKE = ROOT / "scripts/verify_release_eip712_runtime.py"
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
IMMUTABLE_REGISTRY_IMAGE = f"hummingbot/hummingbot@sha256:{'a' * 64}"
IMMUTABLE_LOCAL_IMAGE_ID = f"sha256:{'b' * 64}"
SHUTDOWN_PARSE_IMAGE_ID = f"sha256:{'0' * 64}"


def _dockerfile() -> str:
    return DOCKERFILE.read_text()


def _dockerfile_stage(name: str) -> str:
    match = re.search(
        rf"^FROM\s+[^\n]+\s+AS\s+{re.escape(name)}\s*$\n(.*?)(?=^FROM\s|\Z)",
        _dockerfile(),
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"missing Dockerfile stage {name}"
    stage = re.sub(r"\\\s*\n\s*", " ", match.group(1))
    return re.sub(r"\s+", " ", stage).strip()


def _workflow() -> str:
    return WORKFLOW.read_text()


def _workflow_document() -> dict:
    return yaml.load(_workflow(), Loader=yaml.BaseLoader)


def _release_validation_script() -> str:
    workflow = _workflow_document()
    return next(
        step["run"]
        for step in workflow["jobs"]["validate_release"]["steps"]
        if step.get("id") == "confirm"
    )


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
                ["docker", "image", "rm", "--force", image_tag],
                check=False,
                capture_output=True,
                text=True,
            )
            remaining_tag = subprocess.run(
                ["docker", "image", "inspect", image_tag],
                check=False,
                capture_output=True,
                text=True,
            )
            build_exit = "not-started" if result is None else str(result.returncode)
            if remaining_tag.returncode == 0:
                raise RuntimeError(
                    f"Isolated Docker test tag {image_tag} still exists after exact cleanup "
                    f"(build exit {build_exit}; remove exit {cleanup.returncode}; inspect exit 0): "
                    f"{cleanup.stderr}"
                )
            inspect_output = f"{remaining_tag.stdout}\n{remaining_tag.stderr}"
            recognized_absence = any(
                marker in inspect_output for marker in ("No such image", "No such object")
            )
            if not recognized_absence:
                raise RuntimeError(
                    f"Could not verify exact Docker test tag {image_tag} absence "
                    f"(build exit {build_exit}; remove exit {cleanup.returncode}; "
                    f"inspect exit {remaining_tag.returncode}): {inspect_output.strip()}"
                )
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


def _make_fake_docker(tmp_path: Path) -> tuple[Path, Path, Path]:
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    removed_tags = tmp_path / "removed-tags"
    docker_calls = tmp_path / "docker-calls"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_DOCKER_CALLS\"\n"
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
        "  target=$3\n"
        "  if [ \"$target\" = --force ]; then target=$4; fi\n"
        "  printf '%s\\n' \"$target\" >> \"$FAKE_DOCKER_REMOVED_TAGS\"\n"
        "  exit \"${FAKE_DOCKER_REMOVE_EXIT:-0}\"\n"
        "fi\n"
        "if [ \"$1\" = image ] && [ \"$2\" = inspect ]; then\n"
        "  inspect_exit=\"${FAKE_DOCKER_INSPECT_EXIT:-1}\"\n"
        "  if [ \"$inspect_exit\" -ne 0 ]; then\n"
        "    if [ -n \"${FAKE_DOCKER_INSPECT_STDERR:-}\" ]; then\n"
        "      printf '%s\\n' \"$FAKE_DOCKER_INSPECT_STDERR\" >&2\n"
        "    else\n"
        "      printf 'Error: No such image: %s\\n' \"$3\" >&2\n"
        "    fi\n"
        "  fi\n"
        "  exit \"$inspect_exit\"\n"
        "fi\n"
        "exit 99\n"
    )
    fake_docker.chmod(0o755)
    return fake_bin, removed_tags, docker_calls


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
    fake_bin, removed_tags, docker_calls = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", f"sha256:{'a' * 64}")
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "7")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))
    monkeypatch.setenv("FAKE_DOCKER_CALLS", str(docker_calls))

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
    fake_bin, removed_tags, docker_calls = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", malformed_iid)
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "0")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))
    monkeypatch.setenv("FAKE_DOCKER_CALLS", str(docker_calls))

    with pytest.raises(ValueError, match="exact sha256 image ID"):
        with _build_metadata(tmp_path):
            pytest.fail("malformed image ID must fail before the helper yields")

    assert list(tmp_path.glob("*.iid")) == []
    cleaned_tags = removed_tags.read_text().splitlines()
    assert len(cleaned_tags) == 1
    assert cleaned_tags[0].startswith("hummingbot:qte-image-identity-metadata-test-")


def test_docker_build_helpers_use_collision_proof_tags_and_leave_zero_iidfiles(tmp_path: Path, monkeypatch):
    fake_bin, removed_tags, docker_calls = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", f"sha256:{'b' * 64}")
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "0")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))
    monkeypatch.setenv("FAKE_DOCKER_CALLS", str(docker_calls))

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


def test_failed_build_raises_when_exact_tag_remains_after_forced_cleanup(tmp_path: Path, monkeypatch):
    fake_bin, removed_tags, docker_calls = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", f"sha256:{'c' * 64}")
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "7")
    monkeypatch.setenv("FAKE_DOCKER_REMOVE_EXIT", "9")
    monkeypatch.setenv("FAKE_DOCKER_INSPECT_EXIT", "0")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))
    monkeypatch.setenv("FAKE_DOCKER_CALLS", str(docker_calls))

    with pytest.raises(RuntimeError, match="still exists after exact cleanup"):
        with _build_environment(tmp_path) as image:
            assert image.result.returncode == 7

    assert list(tmp_path.glob("*.iid")) == []
    cleaned_tag = removed_tags.read_text().strip()
    assert cleaned_tag.startswith("hummingbot:qte-condaforge-environment-test-")
    assert f"image rm --force {cleaned_tag}" in docker_calls.read_text().splitlines()
    assert f"image inspect {cleaned_tag}" in docker_calls.read_text().splitlines()


def test_failed_build_raises_when_inspect_cannot_verify_exact_tag_absence(tmp_path: Path, monkeypatch):
    fake_bin, removed_tags, docker_calls = _make_fake_docker(tmp_path)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_IID", f"sha256:{'d' * 64}")
    monkeypatch.setenv("FAKE_DOCKER_BUILD_EXIT", "7")
    monkeypatch.setenv("FAKE_DOCKER_REMOVE_EXIT", "9")
    monkeypatch.setenv("FAKE_DOCKER_INSPECT_EXIT", "125")
    monkeypatch.setenv("FAKE_DOCKER_INSPECT_STDERR", "Cannot connect to the Docker daemon")
    monkeypatch.setenv("FAKE_DOCKER_REMOVED_TAGS", str(removed_tags))
    monkeypatch.setenv("FAKE_DOCKER_CALLS", str(docker_calls))

    with pytest.raises(
        RuntimeError,
        match=r"build exit 7.*remove exit 9.*inspect exit 125.*Cannot connect to the Docker daemon",
    ):
        with _build_environment(tmp_path) as image:
            assert image.result.returncode == 7

    assert list(tmp_path.glob("*.iid")) == []
    cleaned_tag = removed_tags.read_text().strip()
    assert cleaned_tag.startswith("hummingbot:qte-condaforge-environment-test-")
    assert f"image rm --force {cleaned_tag}" in docker_calls.read_text().splitlines()
    assert f"image inspect {cleaned_tag}" in docker_calls.read_text().splitlines()


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


def test_concurrent_same_content_builds_clean_only_their_exact_tag_from_shared_image_id(tmp_path: Path):
    assert shutil.which("docker"), "BuildKit is required for shared image identity verification"
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    build_args = (
        "--build-arg",
        f"SOURCE_COMMIT={source_commit}",
        "--build-arg",
        f"COMMIT={source_commit}",
        "--build-arg",
        "BRANCH=shared-identity",
        "--build-arg",
        "BUILD_DATE=2026-08-25T00:00:00Z",
    )
    both_builds_finished = Barrier(2)
    first_tag_cleaned = Event()

    def clean_first() -> tuple[str, str]:
        try:
            with _build_metadata(tmp_path, *build_args) as image:
                assert image.result.returncode == 0, image.result.stderr
                assert image.image_id is not None
                both_builds_finished.wait(timeout=30)
                return image.image_id, image.image_tag
        finally:
            first_tag_cleaned.set()

    def preserve_second() -> tuple[str, str]:
        with _build_metadata(tmp_path, *build_args) as image:
            assert image.result.returncode == 0, image.result.stderr
            assert image.image_id is not None
            both_builds_finished.wait(timeout=30)
            assert first_tag_cleaned.wait(timeout=30)
            own_tag = subprocess.run(
                ["docker", "image", "inspect", image.image_tag],
                check=False,
                capture_output=True,
                text=True,
            )
            assert own_tag.returncode == 0, own_tag.stderr
            return image.image_id, image.image_tag

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(clean_first)
        second = executor.submit(preserve_second)
        first_image_id, first_image_tag = first.result()
        second_image_id, second_image_tag = second.result()

    assert first_image_id == second_image_id
    assert first_image_tag != second_image_tag
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

    assert "/opt/conda/envs/hummingbot/bin/python -m pip install" in dockerfile
    assert "conda run -n hummingbot --no-capture-output python setup.py build_ext" in dockerfile
    assert "RUN python3 -m pip install" not in dockerfile
    assert "RUN python3 setup.py build_ext" not in dockerfile


def test_lock_validation_ancestry_fails_before_builder_bootstrap_installation():
    dockerfile = _dockerfile()

    validation_from = "FROM identity-check AS lock-validation"
    bootstrap_from = "FROM lock-validation AS builder-bootstrap"
    environment_from = "FROM builder-bootstrap AS builder-environment"
    assert validation_from in dockerfile
    assert bootstrap_from in dockerfile
    assert environment_from in dockerfile
    assert dockerfile.index(validation_from) < dockerfile.index(bootstrap_from) < dockerfile.index(environment_from)

    validation = _dockerfile_stage("lock-validation")
    bootstrap = _dockerfile_stage("builder-bootstrap")
    for source in (
        "COPY setup/environment.yml /tmp/release-sources/setup/environment.yml",
        "COPY setup/pip_packages.txt /tmp/release-sources/setup/pip_packages.txt",
        "COPY setup/locks/ /tmp/release-locks/",
        "COPY scripts/validate_release_dependency_locks.py /tmp/validate_release_dependency_locks.py",
    ):
        assert source in validation
    assert 'case "${TARGETPLATFORM}" in' in validation
    assert "/tmp/validate_release_dependency_locks.py" in validation
    assert "apt-get" not in validation
    assert "apt-get update" in bootstrap


def test_builder_environment_maps_each_supported_platform_to_its_exact_lock_pair_and_fails_closed():
    stage = _dockerfile_stage("builder-environment")

    assert 'case "${TARGETPLATFORM}" in' in stage
    assert 'linux/amd64)' in stage
    assert 'CONDA_LOCK=/tmp/release-locks/linux-64.explicit.txt' in stage
    assert 'INJECTIVE_PIP_LOCK=/tmp/release-locks/pip-injective-linux-64.lock.txt' in stage
    assert 'linux/arm64)' in stage
    assert 'CONDA_LOCK=/tmp/release-locks/linux-aarch64.explicit.txt' in stage
    assert 'INJECTIVE_PIP_LOCK=/tmp/release-locks/pip-injective-linux-aarch64.lock.txt' in stage
    assert 'Unsupported TARGETPLATFORM: ${TARGETPLATFORM:-<empty>}' in stage
    assert re.search(r"\*\)\s+.*exit\s+(?:1|64);;", stage, flags=re.DOTALL)


def test_lock_validation_checks_canonical_sources_before_builder_environment_exact_conda_create():
    validation = _dockerfile_stage("lock-validation")
    environment = _dockerfile_stage("builder-environment")

    expected_copies = (
        "COPY setup/environment.yml /tmp/release-sources/setup/environment.yml",
        "COPY setup/pip_packages.txt /tmp/release-sources/setup/pip_packages.txt",
        "COPY setup/locks/ /tmp/release-locks/",
        "COPY scripts/validate_release_dependency_locks.py /tmp/validate_release_dependency_locks.py",
    )
    for instruction in expected_copies:
        assert instruction in validation

    bootstrap_setup = "from ruamel.yaml import YAML"
    bootstrap_probe = (
        "PYTHONPATH=/tmp/validator-bootstrap "
        "/opt/conda/bin/python -c 'import yaml; from conda.models.version import VersionSpec'"
    )
    validator = (
        "PYTHONPATH=/tmp/validator-bootstrap "
        "/opt/conda/bin/python /tmp/validate_release_dependency_locks.py"
    )
    exact_create = 'conda create --prefix /opt/conda/envs/hummingbot --file "${CONDA_LOCK}" --yes'
    assert bootstrap_setup in validation
    assert "from ruamel.yaml.error import YAMLError" in validation
    assert "def safe_load(stream):" in validation
    assert bootstrap_probe in validation
    assert validator in validation
    assert "--lock-dir /tmp/release-locks" in validation
    assert "--environment /tmp/release-sources/setup/environment.yml" in validation
    assert "--pip-packages /tmp/release-sources/setup/pip_packages.txt" in validation
    assert validation.index(bootstrap_setup) < validation.index(bootstrap_probe) < validation.index(validator)
    assert exact_create in environment
    for forbidden in ("conda env create", "conda env update", "conda install", "conda update"):
        assert forbidden not in validation
        assert forbidden not in environment


def test_builder_environment_installs_both_pip_locks_with_fail_closed_flags_and_distinct_reports():
    stage = _dockerfile_stage("builder-environment")

    pip_commands = re.findall(
        r"/opt/conda/envs/hummingbot/bin/python -m pip install .*?(?=\s+&&)",
        stage,
        flags=re.DOTALL,
    )
    assert len(pip_commands) == 2
    for command in pip_commands:
        for flag in (
            "--isolated",
            "--no-cache-dir",
            "--no-build-isolation",
            "--require-hashes",
            "--no-deps",
            "--index-url https://pypi.org/simple",
        ):
            assert flag in command
    assert "--report /opt/conda/image-evidence/hummingbot-environment-pip-install-report.json" in pip_commands[0]
    assert '-r "${INJECTIVE_PIP_LOCK}"' in pip_commands[0]
    assert "--report /opt/conda/image-evidence/hummingbot-pip-install-report.json" in pip_commands[1]
    assert "-r /tmp/release-locks/pip-eip712-direct.lock.txt" in pip_commands[1]
    assert "pip check" not in stage


def test_builder_environment_preserves_selected_lock_and_inventory_evidence():
    stage = _dockerfile_stage("builder-environment")

    for evidence_path in (
        "/opt/conda/conda-meta/hummingbot-package-urls.txt",
        "/opt/conda/conda-meta/hummingbot-python-version.txt",
        "/opt/conda/image-evidence/base-final-conda-list.json",
        "/opt/conda/image-evidence/hummingbot-final-conda-list.json",
        "/opt/conda/image-evidence/hummingbot-environment-source.yml",
        "/opt/conda/image-evidence/hummingbot-pip-source-requirements.txt",
        "/opt/conda/image-evidence/hummingbot-selected-conda-lock.txt",
        "/opt/conda/image-evidence/hummingbot-environment-pip-requirements.txt",
        "/opt/conda/image-evidence/hummingbot-pip-requirements.txt",
        "/opt/conda/image-evidence/release-lock-provenance.json",
        "/opt/conda/image-evidence/release-lock-target.txt",
    ):
        assert evidence_path in stage


def test_builder_environment_runs_a_deterministic_eip712_runtime_reversal_gate_after_install():
    stage = _dockerfile_stage("builder-environment")

    assert "COPY scripts/verify_release_eip712_runtime.py /tmp/verify_release_eip712_runtime.py" in stage
    smoke_command = "/opt/conda/envs/hummingbot/bin/python /tmp/verify_release_eip712_runtime.py"
    eip_report = "/opt/conda/image-evidence/hummingbot-pip-install-report.json"
    assert smoke_command in stage
    assert stage.index(eip_report) < stage.index(smoke_command)
    assert RELEASE_RUNTIME_DEPENDENCY_SMOKE.is_file()
    smoke = RELEASE_RUNTIME_DEPENDENCY_SMOKE.read_text()
    assert "import sha3" in smoke
    assert "from eip712_structs import EIP712Struct, String, make_domain" in smoke
    assert "class HummingbotRuntimeProbe(EIP712Struct):" in smoke
    assert "message = String()" in smoke
    assert "name=\"Hummingbot\"" in smoke
    assert "version=\"1\"" in smoke
    assert "chainId=1" in smoke
    assert 'verifyingContract="0x0000000000000000000000000000000000000001"' in smoke  # noqa: mock
    assert "signable_bytes" in smoke
    assert "1901de57171073f97873bc93388ae5e87afd47d803e4b4a59a7a402a0eb71b7aa2e6" in smoke  # noqa: mock
    assert "64e15bdc268f69b1f35c21c9559e329a2f3587fe22a9800badf545aec280d842" in smoke  # noqa: mock
    assert "358eb3432f7a6fdfb6bbec6fcc85bf845d3feea114fb95722bbbf854a934453e" in smoke  # noqa: mock
    assert "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470" in smoke  # noqa: mock


def test_runtime_dependency_smoke_rejects_wrong_vector_under_optimized_python(tmp_path: Path):
    smoke = RELEASE_RUNTIME_DEPENDENCY_SMOKE.read_text()
    wrong_smoke = smoke.replace(
        "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",  # noqa: mock
        "0" * 64,
    )
    assert wrong_smoke != smoke
    candidate = tmp_path / RELEASE_RUNTIME_DEPENDENCY_SMOKE.name
    candidate.write_text(wrong_smoke)

    result = subprocess.run(
        [sys.executable, "-O", str(candidate)],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode != 0, result.stdout + result.stderr


def _load_runtime_dependency_smoke():
    spec = importlib.util.spec_from_file_location(
        "release_runtime_dependency_smoke_under_test",
        RELEASE_RUNTIME_DEPENDENCY_SMOKE,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_dependency_smoke_rejects_wrong_signable_body_prefix_and_length():
    smoke = _load_runtime_dependency_smoke()
    accepted = bytes.fromhex(smoke.EXPECTED_SIGNABLE_HEX)
    wrong_values = (
        accepted[:-1] + bytes([accepted[-1] ^ 1]),
        b"\x18" + accepted[1:],
        accepted[:-1],
    )

    for wrong in wrong_values:
        with pytest.raises(RuntimeError, match="unexpected EIP-712 signable bytes"):
            smoke.verify_signable(wrong)


def test_runtime_dependency_smoke_rejects_wrong_expected_signable_keccak(monkeypatch):
    smoke = _load_runtime_dependency_smoke()
    accepted = bytes.fromhex(smoke.EXPECTED_SIGNABLE_HEX)
    monkeypatch.setattr(smoke, "EXPECTED_SIGNABLE_KECCAK", "0" * 64)

    with pytest.raises(RuntimeError, match="unexpected EIP-712 signable Keccak-256"):
        smoke.verify_signable(accepted)


def test_release_preserves_license_texts_and_generates_solved_package_inventory():
    dockerfile = _dockerfile()

    assert THIRD_PARTY_NOTICES.is_file()
    notices = THIRD_PARTY_NOTICES.read_text()
    assert "/home/hummingbot/LICENSE" in notices
    assert "/opt/conda/LICENSE.txt" in notices
    assert "/usr/share/licenses/hummingbot/third-party-license-inventory.json" in notices
    assert "does not establish legal clearance" in notices
    assert "COPY LICENSE THIRD_PARTY_NOTICES.md ./" in dockerfile
    assert (
        "COPY licenses/miniforge-images-BSD-3-Clause.txt "
        "licenses/miniforge-images-BSD-3-Clause.txt"
    ) in dockerfile
    assert "COPY licenses/ licenses/" not in dockerfile
    assert "--report /opt/conda/image-evidence/hummingbot-pip-install-report.json" in dockerfile
    assert "scripts/generate_third_party_license_inventory.py" in dockerfile
    assert "--conda-prefix /opt/conda" in dockerfile
    assert "--conda-prefix /opt/conda/envs/hummingbot" in dockerfile
    assert "--report /opt/conda/image-evidence/hummingbot-environment-pip-install-report.json" in dockerfile
    assert "--pip-report /opt/conda/image-evidence/hummingbot-environment-pip-install-report.json" in dockerfile
    assert "--pip-report /opt/conda/image-evidence/hummingbot-pip-install-report.json" in dockerfile
    assert "--conda-list /opt/conda/image-evidence/base-final-conda-list.json" in dockerfile
    assert "--conda-list /opt/conda/image-evidence/hummingbot-final-conda-list.json" in dockerfile
    assert "--python-prefix /opt/conda/envs/hummingbot" in dockerfile
    assert "--dpkg-root /" in dockerfile
    assert "--source-commit \"${SOURCE_COMMIT}\"" in dockerfile
    assert "--platform \"${TARGETPLATFORM}\"" in dockerfile
    assert "--output /usr/share/licenses/hummingbot/third-party-license-inventory.json" in dockerfile


def test_image_license_gate_document_preserves_the_authority_boundary():
    document = IMAGE_LICENSE_GATE_DOC.read_text()

    assert "HOLD_FOR_EVIDENCE" in document
    assert "legal determination" in document
    assert "unresolved artifact sources" in document
    assert "linux/amd64" in document
    assert "linux/arm64" in document
    assert "operator or counsel approval" in document
    assert "does not include `source_commit`" in document
    assert "must not be cited as source-review" in document


def test_manual_publication_is_fail_closed_on_the_reviewed_license_policy():
    workflow = _workflow_document()
    validation_steps = workflow["jobs"]["validate_release"]["steps"]
    checkout_index = next(
        index
        for index, step in enumerate(validation_steps)
        if step.get("uses", "").startswith("actions/checkout@")
    )
    policy_index = next(
        index
        for index, step in enumerate(validation_steps)
        if "validate_image_license_policy.py" in step.get("run", "")
    )
    confirmation_index = next(
        index for index, step in enumerate(validation_steps) if step.get("id") == "confirm"
    )

    assert checkout_index < policy_index < confirmation_index
    assert workflow["jobs"]["publish_release"]["needs"] == "validate_release"
    policy_step = validation_steps[policy_index]
    assert policy_step["id"] == "license_policy"
    assert "--github-output \"${GITHUB_OUTPUT}\"" in policy_step["run"]
    publish_build = next(
        step
        for step in workflow["jobs"]["publish_release"]["steps"]
        if step.get("id") == "image"
    )
    build_args = publish_build["with"]["build-args"]
    assert "REQUIRE_APPROVED_LICENSE_EVIDENCE=true" in build_args
    assert "EXPECTED_EVIDENCE_SET_SHA256_AMD64=${{ needs.validate_release.outputs.evidence_set_sha256_amd64 }}" in build_args
    assert "EXPECTED_EVIDENCE_SET_SHA256_ARM64=${{ needs.validate_release.outputs.evidence_set_sha256_arm64 }}" in build_args


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


def test_compose_requires_an_image_and_preserves_make_validated_identity():
    missing_environment = {key: value for key, value in os.environ.items() if key != "HUMMINGBOT_IMAGE"}
    missing = subprocess.run(
        ["docker", "compose", "-f", str(ROOT / "docker-compose.yml"), "config"],
        check=False,
        capture_output=True,
        text=True,
        env=missing_environment,
    )
    assert missing.returncode != 0
    assert "HUMMINGBOT_IMAGE" in missing.stderr

    for immutable_image in (IMMUTABLE_REGISTRY_IMAGE, IMMUTABLE_LOCAL_IMAGE_ID):
        configured = subprocess.run(
            ["docker", "compose", "-f", str(ROOT / "docker-compose.yml"), "config"],
            check=False,
            capture_output=True,
            text=True,
            env={**missing_environment, "HUMMINGBOT_IMAGE": immutable_image},
        )
        assert configured.returncode == 0, configured.stderr
        document = yaml.safe_load(configured.stdout)
        assert document["services"]["hummingbot"]["image"] == immutable_image


@pytest.mark.parametrize(
    "image",
    [
        None,
        "",
        "hummingbot/hummingbot:latest",
        "hummingbot/hummingbot:development",
        "hummingbot/hummingbot@sha256:not-a-digest",
        f"sha256:{'a' * 63}",
        "sha256:$(touch should-not-run)",
        f"hummingbot/hummingbot:latest\n{IMMUTABLE_LOCAL_IMAGE_ID}",
        f"/@sha256:{'a' * 64}",
        f"localhost:5000/hummingbot/hummingbot@sha256:{'a' * 64}",
    ],
)
def test_make_deploy_rejects_non_immutable_image_before_compose(tmp_path: Path, image: str | None):
    repo = _make_context(tmp_path, "deploy-reject")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    marker = tmp_path / "docker-invoked"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text("#!/bin/sh\ntouch \"$FAKE_DOCKER_MARKER\"\nexit 0\n")
    fake_docker.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "FAKE_DOCKER_MARKER": str(marker),
    }
    environment.pop("HUMMINGBOT_IMAGE", None)
    if image is not None:
        environment["HUMMINGBOT_IMAGE"] = image

    result = subprocess.run(
        ["make", "deploy"], cwd=repo, check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "immutable registry digest or local image ID" in result.stderr
    assert not marker.exists()


@pytest.mark.parametrize("image", [IMMUTABLE_REGISTRY_IMAGE, IMMUTABLE_LOCAL_IMAGE_ID])
def test_make_deploy_passes_only_validated_immutable_image_to_compose(tmp_path: Path, image: str):
    repo = _make_context(tmp_path, "deploy-accept")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    invocation = tmp_path / "docker-invocation"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$HUMMINGBOT_IMAGE\" > \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        ["make", "deploy"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_INVOCATION": str(invocation),
            "HUMMINGBOT_IMAGE": image,
        },
    )

    assert result.returncode == 0, result.stderr
    assert invocation.read_text().splitlines() == [
        image,
        "compose --project-name hummingbot -f ./docker-compose.yml up -d",
    ]


@pytest.mark.parametrize(
    "compose_environment",
    [
        'HUMMINGBOT_IMAGE=sha256:$(touch "$SHELL_MARKER"; printf "%064d" 0)\n',
        'COMPOSE_FILE=$(touch "$SHELL_MARKER"; printf "alternate.yml")\n',
        'COMPOSE_PROFILES=gateway\nCOMPOSE_FILE=$(touch "$SHELL_MARKER"; printf "alternate.yml")\n',
        "COMPOSE_PROFILES=gateway\nCOMPOSE_FILE=alternate.yml",
        "COMPOSE_PROFILES=gate\x00way\n",
    ],
)
def test_make_deploy_treats_compose_environment_as_allowlisted_data(
    tmp_path: Path, compose_environment: str
):
    repo = _make_context(tmp_path, "deploy-compose-environment")
    shell_marker = tmp_path / "shell-executed"
    (repo / ".compose.env").write_text(compose_environment)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    marker = tmp_path / "docker-invoked"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text("#!/bin/sh\ntouch \"$FAKE_DOCKER_MARKER\"\nexit 0\n")
    fake_docker.chmod(0o755)

    result = subprocess.run(
        ["make", "deploy"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_MARKER": str(marker),
            "SHELL_MARKER": str(shell_marker),
            "HUMMINGBOT_IMAGE": IMMUTABLE_REGISTRY_IMAGE,
        },
    )

    assert result.returncode != 0
    assert ".compose.env must contain exactly one COMPOSE_PROFILES entry" in result.stderr
    assert not shell_marker.exists()
    assert not marker.exists()


@pytest.mark.parametrize(
    ("compose_profiles", "expected_arguments"),
    [
        ("", "compose --project-name hummingbot -f ./docker-compose.yml up -d"),
        (
            "gateway",
            "compose --project-name hummingbot -f ./docker-compose.yml --profile gateway up -d",
        ),
    ],
)
def test_make_deploy_passes_allowlisted_compose_environment_as_data(
    tmp_path: Path, compose_profiles: str, expected_arguments: str
):
    repo = _make_context(tmp_path, "deploy-compose-environment-accepted")
    (repo / ".compose.env").write_text(f"COMPOSE_PROFILES={compose_profiles}\n")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    invocation = tmp_path / "docker-invocation"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text("#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$FAKE_DOCKER_INVOCATION\"\n")
    fake_docker.chmod(0o755)

    result = subprocess.run(
        ["make", "deploy"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_INVOCATION": str(invocation),
            "HUMMINGBOT_IMAGE": IMMUTABLE_REGISTRY_IMAGE,
        },
    )

    assert result.returncode == 0, result.stderr
    assert invocation.read_text().strip() == expected_arguments


def test_make_deploy_ignores_ambient_compose_routing_controls(tmp_path: Path):
    repo = _make_context(tmp_path, "deploy-ambient-compose-controls")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    invocation = tmp_path / "docker-invocation"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf 'COMPOSE_FILE=%s\\n' \"${COMPOSE_FILE-unset}\" > \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf 'COMPOSE_PROFILES=%s\\n' \"${COMPOSE_PROFILES-unset}\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf 'COMPOSE_ENV_FILES=%s\\n' \"${COMPOSE_ENV_FILES-unset}\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf 'COMPOSE_DISABLE_ENV_FILE=%s\\n' \"${COMPOSE_DISABLE_ENV_FILE-unset}\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf 'COMPOSE_PROJECT_NAME=%s\\n' \"${COMPOSE_PROJECT_NAME-unset}\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        ["make", "deploy"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_INVOCATION": str(invocation),
            "HUMMINGBOT_IMAGE": IMMUTABLE_REGISTRY_IMAGE,
            "COMPOSE_FILE": "alternate.yml",
            "COMPOSE_PROFILES": "unapproved",
            "COMPOSE_ENV_FILES": "alternate.env",
            "COMPOSE_DISABLE_ENV_FILE": "0",
            "COMPOSE_PROJECT_NAME": "alternate",
        },
    )

    assert result.returncode == 0, result.stderr
    assert invocation.read_text().splitlines() == [
        "COMPOSE_FILE=unset",
        "COMPOSE_PROFILES=unset",
        "COMPOSE_ENV_FILES=unset",
        "COMPOSE_DISABLE_ENV_FILE=1",
        "COMPOSE_PROJECT_NAME=unset",
        "compose --project-name hummingbot -f ./docker-compose.yml up -d",
    ]


def test_compose_disable_env_file_prevents_untracked_gateway_profile(tmp_path: Path):
    shutil.copy(ROOT / "docker-compose.yml", tmp_path / "docker-compose.yml")
    (tmp_path / ".env").write_text("COMPOSE_PROFILES=gateway\n")
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"COMPOSE_FILE", "COMPOSE_PROFILES", "COMPOSE_ENV_FILES"}
    }
    environment.update(
        {
            "COMPOSE_DISABLE_ENV_FILE": "1",
            "HUMMINGBOT_IMAGE": IMMUTABLE_REGISTRY_IMAGE,
        }
    )

    result = subprocess.run(
        ["docker", "compose", "-f", "./docker-compose.yml", "config", "--services"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["hummingbot"]


def test_make_down_does_not_require_a_runtime_image_identity(tmp_path: Path):
    repo = _make_context(tmp_path, "down-without-runtime-image")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    invocation = tmp_path / "docker-invocation"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$HUMMINGBOT_IMAGE\" > \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf 'COMPOSE_ENV_FILES=%s\\n' \"${COMPOSE_ENV_FILES-unset}\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf 'COMPOSE_DISABLE_ENV_FILE=%s\\n' \"${COMPOSE_DISABLE_ENV_FILE-unset}\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf 'COMPOSE_PROJECT_NAME=%s\\n' \"${COMPOSE_PROJECT_NAME-unset}\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_DOCKER_INVOCATION\"\n"
    )
    fake_docker.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "FAKE_DOCKER_INVOCATION": str(invocation),
        "COMPOSE_FILE": "alternate.yml",
        "COMPOSE_PROFILES": "unapproved",
        "COMPOSE_ENV_FILES": "alternate.env",
        "COMPOSE_DISABLE_ENV_FILE": "0",
        "COMPOSE_PROJECT_NAME": "alternate",
    }
    environment.pop("HUMMINGBOT_IMAGE", None)

    result = subprocess.run(
        ["make", "down"], cwd=repo, check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 0, result.stderr
    assert invocation.read_text().splitlines() == [
        SHUTDOWN_PARSE_IMAGE_ID,
        "COMPOSE_ENV_FILES=unset",
        "COMPOSE_DISABLE_ENV_FILE=1",
        "COMPOSE_PROJECT_NAME=unset",
        "compose --project-name hummingbot -f ./docker-compose.yml --profile gateway down",
    ]


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
