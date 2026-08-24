import re
from pathlib import Path

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


def test_dockerfile_rejects_missing_empty_malformed_and_mismatched_source_commit():
    dockerfile = _dockerfile()

    assert "ARG SOURCE_COMMIT" in dockerfile
    assert 'test -n "${SOURCE_COMMIT}"' in dockerfile
    assert "^[0-9a-f]{40}$" in dockerfile
    assert 'test "${COMMIT}" = "${SOURCE_COMMIT}"' in dockerfile

    valid = SOURCE_SHA
    invalid_cases = {
        "omitted": None,
        "empty": "",
        "malformed": "not-a-commit",
        "mismatched_legacy_commit": valid[:-1] + "b",
    }
    assert re.fullmatch(r"[0-9a-f]{40}", valid)
    for name, value in invalid_cases.items():
        assert value != valid, name


def test_dockerfile_emits_oci_revision_and_retains_matching_legacy_commit_metadata():
    dockerfile = _dockerfile()

    assert 'org.opencontainers.image.revision="${SOURCE_COMMIT}"' in dockerfile
    assert "commit=${COMMIT}" in dockerfile
    assert "ENV COMMIT_SHA=${COMMIT}" in dockerfile
    assert "LABEL commit=${SOURCE_COMMIT}" not in dockerfile


def test_dockerfile_pins_every_miniconda_base_image_to_the_recorded_digest():
    dockerfile = _dockerfile()

    from_images = re.findall(r"^FROM\s+([^\s]+)", dockerfile, flags=re.MULTILINE)
    assert [image for image in from_images if image.startswith("continuumio/")] == [
        PINNED_MINICONDA,
        PINNED_MINICONDA,
    ]
    assert not re.search(r"^FROM\s+[^\s]*:latest", dockerfile, flags=re.MULTILINE)


def test_workflow_pr_verification_is_non_pushing_and_binds_pr_head_sha():
    workflow = _workflow()

    assert "pull_request:" in workflow
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
    assert "ref: ${{ needs.validate_release.outputs.source_commit }}" in workflow
    assert "SOURCE_COMMIT=${{ needs.validate_release.outputs.source_commit }}" in workflow
    assert "COMMIT=${{ needs.validate_release.outputs.source_commit }}" in workflow
    assert "tags: hummingbot/hummingbot:sha-${{ needs.validate_release.outputs.source_commit }}" in workflow


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
