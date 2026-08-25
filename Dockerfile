# Pinned from the docker.io/condaforge/miniforge3:26.5.3-0 OCI index on 2026-08-25.
# linux/amd64: sha256:1f7cb6c2968ab71c66346669d57d1b952273b6aa7a28b3d8dee489b3a89bf0c3
# linux/arm64: sha256:0d488aad6aec4534ec679b22e1d6105a0d78203b57c78876d0808bfaba1a0562
FROM docker.io/condaforge/miniforge3:26.5.3-0@sha256:b230e51ddd0ebc4b08bb44d7442c20f9443973e3ddd09cf7f621b04fe449ca66 AS identity-check

ARG SOURCE_COMMIT
ARG COMMIT

# Image identity is a source boundary: accept only the exact lowercase Git SHA
# and keep the legacy commit value unable to diverge from the OCI revision.
RUN test -n "${SOURCE_COMMIT}" && \
    printf '%s\n' "${SOURCE_COMMIT}" | grep -Eq '^[0-9a-f]{40}$' && \
    test "${COMMIT}" = "${SOURCE_COMMIT}"

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

# Keep build-tool assumptions explicit and testable on the pinned base.
FROM identity-check AS builder-bootstrap

# Install system dependencies
RUN apt-get update && \
    apt-get install -y sudo libusb-1.0 gcc g++ python3-dev git wget && \
    rm -rf /var/lib/apt/lists/*

# Solve the canonical environment in its own inspectable stage. The explicit
# package list is retained in the final prefix as dependency provenance, and
# any defaults-channel package fails the build.
FROM builder-bootstrap AS builder-environment

WORKDIR /home/hummingbot

# Create conda environment
COPY setup/environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml && \
    conda list -n hummingbot --explicit > /opt/conda/conda-meta/hummingbot-package-urls.txt && \
    if grep -Fq 'repo.anaconda.com' /opt/conda/conda-meta/hummingbot-package-urls.txt; then \
        echo "The hummingbot environment must not contain defaults-channel packages." >&2; \
        exit 1; \
    fi && \
    conda run -n hummingbot python -c 'import platform; print(platform.python_version())' \
        > /opt/conda/conda-meta/hummingbot-python-version.txt && \
    conda clean -afy && \
    rm /tmp/environment.yml

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
COPY LICENSE .
COPY README.md .

COPY setup/pip_packages.txt /tmp/pip_packages.txt
RUN conda run -n hummingbot python -m pip install --no-deps -r /tmp/pip_packages.txt && \
    rm /tmp/pip_packages.txt


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

# Create mount points
RUN mkdir -p /home/hummingbot/conf /home/hummingbot/conf/connectors /home/hummingbot/conf/strategies /home/hummingbot/conf/controllers /home/hummingbot/conf/scripts /home/hummingbot/logs /home/hummingbot/data /home/hummingbot/certs /home/hummingbot/scripts /home/hummingbot/controllers

WORKDIR /home/hummingbot

# Copy all build artifacts from builder image
COPY --from=builder /opt/conda/ /opt/conda/
COPY --from=builder /home/ /home/

# Preserve the pinned Miniforge base's tini contract explicitly. The shell uses
# exec so the Hummingbot process becomes tini's direct child and receives
# termination signals without an activation wrapper.
ENTRYPOINT ["/usr/bin/tini", "--"]

# Set the default command to run when starting the container
CMD ["/bin/bash", "-lc", "source /opt/conda/etc/profile.d/conda.sh && conda activate hummingbot && exec python ./bin/hummingbot_quickstart.py 2>> ./logs/errors.log"]
