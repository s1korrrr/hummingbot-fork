# Pinned from the docker.io/condaforge/miniforge3:26.5.3-0 OCI index on 2026-08-25.
# linux/amd64: sha256:1f7cb6c2968ab71c66346669d57d1b952273b6aa7a28b3d8dee489b3a89bf0c3
# linux/arm64: sha256:0d488aad6aec4534ec679b22e1d6105a0d78203b57c78876d0808bfaba1a0562
FROM docker.io/condaforge/miniforge3:26.5.3-0@sha256:b230e51ddd0ebc4b08bb44d7442c20f9443973e3ddd09cf7f621b04fe449ca66 AS identity-check

ARG TARGETPLATFORM

ARG SOURCE_COMMIT
ARG COMMIT

# Image identity is a source boundary: accept only the exact lowercase Git SHA
# and keep the legacy commit value unable to diverge from the OCI revision.
RUN test -n "${SOURCE_COMMIT}" && \
    printf '%s\n' "${SOURCE_COMMIT}" | grep -Eq '^[0-9a-f]{40}$' && \
    test "${COMMIT}" = "${SOURCE_COMMIT}"

# Validate the complete dependency lock inventory before any apt, Conda, or pip
# installation. Unsupported and empty target platforms fail in this stage.
FROM identity-check AS lock-validation

ARG TARGETPLATFORM

COPY setup/environment.yml /tmp/release-sources/setup/environment.yml
COPY setup/pip_packages.txt /tmp/release-sources/setup/pip_packages.txt
COPY setup/locks/ /tmp/release-locks/
COPY scripts/validate_release_dependency_locks.py /tmp/validate_release_dependency_locks.py

# Miniforge's base environment ships ruamel.yaml rather than PyYAML. Expose the
# validator's two required PyYAML-compatible names without installing or
# solving any bootstrap package.
RUN install -d /tmp/validator-bootstrap && \
    printf '%s\n' \
        'from ruamel.yaml import YAML' \
        'from ruamel.yaml.error import YAMLError' \
        '' \
        'def safe_load(stream):' \
        '    return YAML(typ="safe").load(stream)' \
        > /tmp/validator-bootstrap/yaml.py

RUN set -eu; \
    case "${TARGETPLATFORM}" in \
        linux/amd64|linux/arm64) ;; \
        *) echo "Unsupported TARGETPLATFORM: ${TARGETPLATFORM:-<empty>}" >&2; exit 64;; \
    esac; \
    PYTHONPATH=/tmp/validator-bootstrap /opt/conda/bin/python \
        -c 'import yaml; from conda.models.version import VersionSpec' && \
    PYTHONPATH=/tmp/validator-bootstrap /opt/conda/bin/python \
        /tmp/validate_release_dependency_locks.py \
        --lock-dir /tmp/release-locks \
        --environment /tmp/release-sources/setup/environment.yml \
        --pip-packages /tmp/release-sources/setup/pip_packages.txt

# This target is deliberately lightweight: it inherits the exact identity gate
# and declares the metadata inherited by the final runtime image.
FROM identity-check AS image-metadata

# Dockerfile author / maintainer
LABEL maintainer="Fede Cardoso @dardonacci <federico@hummingbot.org>"

# Build arguments
ARG BRANCH
ARG COMMIT
ARG SOURCE_COMMIT
ARG BUILD_DATE
LABEL branch=${BRANCH}
LABEL commit=${COMMIT}
LABEL date=${BUILD_DATE}
LABEL org.opencontainers.image.title="Hummingbot"
LABEL org.opencontainers.image.description="Open-source framework for designing and deploying automated trading strategies."
LABEL org.opencontainers.image.url="https://hummingbot.org"
LABEL org.opencontainers.image.source="https://github.com/hummingbot/hummingbot"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.version="${SOURCE_COMMIT}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.revision="${SOURCE_COMMIT}"

# Set ENV variables
ENV COMMIT_SHA=${COMMIT}
ENV COMMIT_BRANCH=${BRANCH}
ENV BUILD_DATE=${BUILD_DATE}
ENV INSTALLATION_TYPE=docker

# Keep build-tool assumptions explicit and testable after lock validation.
FROM lock-validation AS builder-bootstrap

# Install system dependencies
RUN apt-get update && \
    apt-get install -y sudo libusb-1.0 gcc g++ python3-dev git wget && \
    rm -rf /var/lib/apt/lists/*

# Create the canonical environment from independently validated, target-specific
# explicit locks. This stage contains the dependency runtime reversal gate and
# never solves the broad source recipes.
FROM builder-bootstrap AS builder-environment

ARG TARGETPLATFORM

WORKDIR /home/hummingbot

# The validated sources and lock inventory are inherited from lock-validation.
COPY scripts/verify_release_eip712_runtime.py /tmp/verify_release_eip712_runtime.py

RUN set -eu; \
    case "${TARGETPLATFORM}" in \
        linux/amd64) \
            CONDA_LOCK=/tmp/release-locks/linux-64.explicit.txt; \
            INJECTIVE_PIP_LOCK=/tmp/release-locks/pip-injective-linux-64.lock.txt;; \
        linux/arm64) \
            CONDA_LOCK=/tmp/release-locks/linux-aarch64.explicit.txt; \
            INJECTIVE_PIP_LOCK=/tmp/release-locks/pip-injective-linux-aarch64.lock.txt;; \
        *) echo "Unsupported TARGETPLATFORM: ${TARGETPLATFORM:-<empty>}" >&2; exit 64;; \
    esac; \
    install -d /opt/conda/image-evidence && \
    conda list --json > /opt/conda/image-evidence/base-final-conda-list.json && \
    conda create --prefix /opt/conda/envs/hummingbot --file "${CONDA_LOCK}" --yes && \
    /opt/conda/envs/hummingbot/bin/python -m pip install \
        --isolated \
        --no-cache-dir \
        --no-build-isolation \
        --require-hashes \
        --no-deps \
        --index-url https://pypi.org/simple \
        --report /opt/conda/image-evidence/hummingbot-environment-pip-install-report.json \
        -r "${INJECTIVE_PIP_LOCK}" && \
    /opt/conda/envs/hummingbot/bin/python -m pip install \
        --isolated \
        --no-cache-dir \
        --no-build-isolation \
        --require-hashes \
        --no-deps \
        --index-url https://pypi.org/simple \
        --report /opt/conda/image-evidence/hummingbot-pip-install-report.json \
        -r /tmp/release-locks/pip-eip712-direct.lock.txt && \
    /opt/conda/envs/hummingbot/bin/python /tmp/verify_release_eip712_runtime.py && \
    conda list -n hummingbot --explicit > /opt/conda/conda-meta/hummingbot-package-urls.txt && \
    conda list -n hummingbot --json \
        > /opt/conda/image-evidence/hummingbot-final-conda-list.json && \
    if grep -Fq 'repo.anaconda.com' /opt/conda/conda-meta/hummingbot-package-urls.txt; then \
        echo "The hummingbot environment must not contain defaults-channel packages." >&2; \
        exit 1; \
    fi && \
    /opt/conda/envs/hummingbot/bin/python -c 'import platform; print(platform.python_version())' \
        > /opt/conda/conda-meta/hummingbot-python-version.txt && \
    install -m 0644 /tmp/release-sources/setup/environment.yml \
        /opt/conda/image-evidence/hummingbot-environment-source.yml && \
    install -m 0644 /tmp/release-sources/setup/pip_packages.txt \
        /opt/conda/image-evidence/hummingbot-pip-source-requirements.txt && \
    install -m 0644 "${CONDA_LOCK}" \
        /opt/conda/image-evidence/hummingbot-selected-conda-lock.txt && \
    install -m 0644 "${INJECTIVE_PIP_LOCK}" \
        /opt/conda/image-evidence/hummingbot-environment-pip-requirements.txt && \
    install -m 0644 /tmp/release-locks/pip-eip712-direct.lock.txt \
        /opt/conda/image-evidence/hummingbot-pip-requirements.txt && \
    install -m 0644 /tmp/release-locks/provenance.json \
        /opt/conda/image-evidence/release-lock-provenance.json && \
    printf 'target_platform=%s\nconda_lock=%s\ninjective_pip_lock=%s\n' \
        "${TARGETPLATFORM}" "${CONDA_LOCK##*/}" "${INJECTIVE_PIP_LOCK##*/}" \
        > /opt/conda/image-evidence/release-lock-target.txt && \
    conda clean -afy

# Build from the checked environment stage so all normal builds fail before
# source compilation when identity or dependency provenance is invalid.
FROM builder-environment AS builder

# Copy remaining files
COPY bin/ bin/
COPY hummingbot/ hummingbot/
COPY scripts/ scripts/
COPY controllers/ controllers/
COPY scripts/ scripts-copy/
COPY setup.py .
COPY LICENSE THIRD_PARTY_NOTICES.md ./
COPY licenses/miniforge-images-BSD-3-Clause.txt licenses/miniforge-images-BSD-3-Clause.txt
COPY README.md .

RUN conda run -n hummingbot --no-capture-output python setup.py build_ext --inplace -j 8 && \
    rm -rf build/ && \
    find . -type f -name "*.cpp" -delete


# Keep the legacy runtime tool contract explicit on the official base image.
FROM image-metadata AS runtime-bootstrap

# Install system dependencies
RUN apt-get update && \
    apt-get install -y sudo libusb-1.0 git wget && \
    rm -rf /var/lib/apt/lists/*

# Build final image using artifacts from builder and identity-bound metadata.
FROM runtime-bootstrap AS release

ARG SOURCE_COMMIT
ARG TARGETPLATFORM
ARG REQUIRE_APPROVED_LICENSE_EVIDENCE=false
ARG EXPECTED_EVIDENCE_SET_SHA256_AMD64
ARG EXPECTED_EVIDENCE_SET_SHA256_ARM64

# Create mount points
RUN mkdir -p /home/hummingbot/conf /home/hummingbot/conf/connectors /home/hummingbot/conf/strategies /home/hummingbot/conf/controllers /home/hummingbot/conf/scripts /home/hummingbot/logs /home/hummingbot/data /home/hummingbot/certs /home/hummingbot/scripts /home/hummingbot/controllers

WORKDIR /home/hummingbot

# Copy all build artifacts from builder image
COPY --from=builder /opt/conda/ /opt/conda/
COPY --from=builder /home/ /home/

# Preserve readable upstream notices and bind the exact final package set to
# source, platform, artifact hashes where available, and installed license
# evidence. This is mechanical provenance, not legal approval.
RUN install -d /usr/share/licenses/hummingbot && \
    install -m 0644 LICENSE THIRD_PARTY_NOTICES.md \
        licenses/miniforge-images-BSD-3-Clause.txt \
        /usr/share/licenses/hummingbot/ && \
    install -m 0644 /opt/conda/LICENSE.txt \
        /usr/share/licenses/hummingbot/Miniforge-BSD-3-Clause.txt && \
    /opt/conda/envs/hummingbot/bin/python \
        scripts/generate_third_party_license_inventory.py \
        --conda-prefix /opt/conda \
        --conda-prefix /opt/conda/envs/hummingbot \
        --pip-report /opt/conda/image-evidence/hummingbot-environment-pip-install-report.json \
        --pip-report /opt/conda/image-evidence/hummingbot-pip-install-report.json \
        --conda-list /opt/conda/image-evidence/base-final-conda-list.json \
        --conda-list /opt/conda/image-evidence/hummingbot-final-conda-list.json \
        --python-prefix /opt/conda/envs/hummingbot \
        --dpkg-root / \
        --notice /usr/share/licenses/hummingbot/LICENSE \
        --notice /usr/share/licenses/hummingbot/Miniforge-BSD-3-Clause.txt \
        --notice /usr/share/licenses/hummingbot/THIRD_PARTY_NOTICES.md \
        --notice /usr/share/licenses/hummingbot/miniforge-images-BSD-3-Clause.txt \
        --source-commit "${SOURCE_COMMIT}" \
        --base-image "docker.io/condaforge/miniforge3:26.5.3-0@sha256:b230e51ddd0ebc4b08bb44d7442c20f9443973e3ddd09cf7f621b04fe449ca66" \
        --platform "${TARGETPLATFORM}" \
        --output /usr/share/licenses/hummingbot/third-party-license-inventory.json && \
    /opt/conda/envs/hummingbot/bin/python \
        scripts/verify_generated_image_license_inventory.py \
        /usr/share/licenses/hummingbot/third-party-license-inventory.json \
        --platform "${TARGETPLATFORM}" \
        --require-approved-binding "${REQUIRE_APPROVED_LICENSE_EVIDENCE}" \
        --expected-amd64 "${EXPECTED_EVIDENCE_SET_SHA256_AMD64}" \
        --expected-arm64 "${EXPECTED_EVIDENCE_SET_SHA256_ARM64}"

# Preserve the pinned Miniforge base's tini contract explicitly. The shell uses
# exec so the Hummingbot process becomes tini's direct child and receives
# termination signals without an activation wrapper.
ENTRYPOINT ["/usr/bin/tini", "--"]

# Set the default command to run when starting the container
CMD ["/bin/bash", "-lc", "source /opt/conda/etc/profile.d/conda.sh && conda activate hummingbot && exec python ./bin/hummingbot_quickstart.py 2>> ./logs/errors.log"]
