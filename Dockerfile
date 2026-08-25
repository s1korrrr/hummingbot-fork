# Pinned from the docker.io/anaconda/miniconda:26.5.3 OCI index on 2026-08-25.
FROM anaconda/miniconda@sha256:d722abd5acde70c181c16cd4129696e08501cd7e0a1316705953b162d9054cfb AS identity-check

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

# The official image does not inherit git or wget like the retired
# continuumio image did. Keep those build assumptions explicit and testable.
FROM identity-check AS builder-bootstrap

# Install system dependencies
RUN apt-get update && \
    apt-get install -y sudo libusb-1.0 gcc g++ python3-dev git wget && \
    rm -rf /var/lib/apt/lists/*

# Build from the checked bootstrap stage so all normal builds fail before doing
# expensive dependency installation when the required source identity is absent.
FROM builder-bootstrap AS builder

WORKDIR /home/hummingbot

# Create conda environment
COPY setup/environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml && \
    conda clean -afy && \
    rm /tmp/environment.yml

# Copy remaining files
COPY bin/ bin/
COPY hummingbot/ hummingbot/
COPY scripts/ scripts/
COPY controllers/ controllers/
COPY scripts/ scripts-copy/
COPY setup.py .
COPY LICENSE .
COPY README.md .

# activate hummingbot env when entering the CT
SHELL [ "/bin/bash", "-lc" ]
RUN echo "conda activate hummingbot" >> ~/.bashrc

COPY setup/pip_packages.txt /tmp/pip_packages.txt
RUN python3 -m pip install --no-deps -r /tmp/pip_packages.txt && \
    rm /tmp/pip_packages.txt


RUN python3 setup.py build_ext --inplace -j 8 && \
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
COPY --from=builder /opt/miniconda3/ /opt/miniconda3/
COPY --from=builder /home/ /home/

# Setting bash as default shell because we have .bashrc with customized PATH (setting SHELL affects RUN, CMD and ENTRYPOINT, but not manual commands e.g. `docker run image COMMAND`!)
SHELL [ "/bin/bash", "-lc" ]

# Set the default command to run when starting the container

CMD conda activate hummingbot && ./bin/hummingbot_quickstart.py 2>> ./logs/errors.log
