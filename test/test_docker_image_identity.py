import json
import os
import re
import shutil
import subprocess
import tarfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "Dockerfile"
WORKFLOW = ROOT / ".github/workflows/docker_buildx_workflow.yml"
SOURCE_SHA = "6a8b5c37ea5bdfbdfc11127e3fa0a05a35e386ca"  # noqa: mock
PINNED_MINICONDA = "continuumio/miniconda3@sha256:1808b31ef43e9c521cde5884ba4df9ec26d8d503a314cea787590f8550358a63"  # noqa: mock
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
class _MetadataImage:
    result: subprocess.CompletedProcess[str]
    image_id: str | None
    image_tag: str


@contextmanager
def _build_metadata(artifact_dir: Path, *build_args: str) -> Iterator[_MetadataImage]:
    invocation = uuid4().hex
    image_tag = f"hummingbot:qte-image-identity-metadata-test-{invocation}"
    image_id_file = artifact_dir / f"metadata-image-{invocation}.iid"
    result = subprocess.run(
        [
            "docker",
            "buildx",
            "build",
            "--progress=quiet",
            "--file",
            str(DOCKERFILE),
            "--target",
            "image-metadata",
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
    try:
        yield _MetadataImage(result=result, image_id=image_id, image_tag=image_tag)
    finally:
        if image_id is not None:
            subprocess.run(
                ["docker", "image", "rm", image_tag],
                check=True,
                capture_output=True,
                text=True,
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


def test_concurrent_metadata_builds_keep_labels_bound_to_their_immutable_image_ids(tmp_path: Path):
    assert shutil.which("docker"), "BuildKit is required for image identity verification"
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()

    def build_and_inspect(branch: str) -> tuple[str, str, dict[str, str]]:
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
            return image.image_id, image.image_tag, _image_labels(image.image_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(build_and_inspect, "isolation-first")
        second = executor.submit(build_and_inspect, "isolation-second")
        first_image_id, first_image_tag, first_labels = first.result()
        second_image_id, second_image_tag, second_labels = second.result()

    assert first_image_id != second_image_id
    assert first_image_tag != second_image_tag
    assert first_labels["branch"] == "isolation-first"
    assert second_labels["branch"] == "isolation-second"
    for image_tag in (first_image_tag, second_image_tag):
        cleanup_check = subprocess.run(
            ["docker", "image", "inspect", image_tag],
            check=False,
            capture_output=True,
            text=True,
        )
        assert cleanup_check.returncode != 0


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
    assert "FROM image-metadata AS release" in dockerfile
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


def test_dockerfile_pins_every_miniconda_base_image_to_the_recorded_digest():
    dockerfile = _dockerfile()

    from_images = re.findall(r"^FROM\s+([^\s]+)", dockerfile, flags=re.MULTILINE)
    assert [image for image in from_images if image.startswith("continuumio/")] == [PINNED_MINICONDA]
    assert not re.search(r"^FROM\s+[^\s]*:latest", dockerfile, flags=re.MULTILINE)


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
