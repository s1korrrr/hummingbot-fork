# Build context is this service directory. Building does not deploy a service.
FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --jobs 2

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home candle
COPY --from=builder /build/target/release/okx-candle-service /usr/local/bin/okx-candle-service
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/okx-candle-service"]
