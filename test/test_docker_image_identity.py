import json
import os
import re
import shutil
import subprocess
import tarfile
from pathlib import Path

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


METADATA_IMAGE = "hummingbot:qte-image-identity-metadata-test"


def _build_metadata(*build_args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
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
            "--tag",
            METADATA_IMAGE,
            *build_args,
            str(ROOT),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _image_labels() -> dict[str, str]:
    result = subprocess.run(
        ["docker", "image", "inspect", METADATA_IMAGE, "--format", "{{json .Config.Labels}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


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
def test_buildkit_metadata_target_rejects_invalid_source_identity(name: str, build_args: tuple[str, ...]):
    assert shutil.which("docker"), "BuildKit is required for image identity verification"

    result = _build_metadata(*build_args)

    assert result.returncode != 0, name
    assert 'process "/bin/sh -c test -n' in f"{result.stdout}\n{result.stderr}"


def test_buildkit_metadata_target_emits_matching_oci_and_legacy_labels():
    assert shutil.which("docker"), "BuildKit is required for image identity verification"
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()

    result = _build_metadata(
        "--build-arg",
        f"SOURCE_COMMIT={source_commit}",
        "--build-arg",
        f"COMMIT={source_commit}",
        "--build-arg",
        "BRANCH=metadata-test",
        "--build-arg",
        "BUILD_DATE=2026-08-25T00:00:00Z",
    )

    assert result.returncode == 0, result.stderr
    labels = _image_labels()
    assert labels["org.opencontainers.image.revision"] == source_commit
    assert labels["commit"] == source_commit
    assert labels["branch"] == "metadata-test"
    assert labels["date"] == "2026-08-25T00:00:00Z"


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

    assert "workflow_dispatch:" in workflow
    assert "confirm_release:" in workflow
    assert "source_commit:" in workflow
    assert "needs.validate_release.outputs.confirmed == 'true'" in workflow
    assert "=~ ^[0-9a-f]{40}$" in workflow
    assert "WORKFLOW_COMMIT: ${{ github.sha }}" in workflow
    assert '[[ "${REQUESTED_SOURCE_COMMIT}" != "${WORKFLOW_COMMIT}" ]]' in workflow
    assert "source_commit=${WORKFLOW_COMMIT}" in workflow
    assert "ref: ${{ needs.validate_release.outputs.source_commit }}" in workflow
    assert "SOURCE_COMMIT=${{ needs.validate_release.outputs.source_commit }}" in workflow
    assert "COMMIT=${{ needs.validate_release.outputs.source_commit }}" in workflow


def test_workflow_publish_job_requires_protected_release_environment():
    document = _workflow_document()

    publish_job = document["jobs"]["publish_release"]
    assert publish_job["environment"]["name"] == "hummingbot-image-release"


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

    assert "tags" not in image_step["with"]
    assert image_step["with"]["push"] == "true"
    assert "push-by-digest=true" in image_step["with"]["outputs"]
    assert "name-canonical=true" in image_step["with"]["outputs"]
    assert publish_job["outputs"]["image_digest"] == "${{ steps.image.outputs.digest }}"
    assert "steps.image.outputs.digest" in _workflow_step("publish_release", "record_digest")["run"]


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
            "GITHUB_OUTPUT": str(tmp_path / "github-output"),
        },
    )

    assert result.returncode != 0
    assert "source_commit must equal the checked-out hardened workflow commit" in result.stderr


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
            "GITHUB_OUTPUT": str(output),
        },
    )

    assert result.returncode == 0, result.stderr
    assert output.read_text().splitlines() == ["confirmed=true", f"source_commit={producer_commit}"]


def test_make_build_command_binds_one_exact_head_sha_to_both_metadata_arguments():
    result = subprocess.run(
        ["make", "-n", "build"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    build_date = subprocess.run(
        ["git", "show", "-s", "--format=%cI", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()

    assert f"SOURCE_COMMIT={head}" in result.stdout
    assert f"COMMIT={head}" in result.stdout
    assert f"BUILD_DATE={build_date}" in result.stdout
    assert "git clean" not in result.stdout


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
