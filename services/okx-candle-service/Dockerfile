# Build context is this service directory. Building does not deploy a service.
FROM rust:1.88-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --jobs 2

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home candle
COPY --from=builder /build/target/release/okx-candle-service /usr/local/bin/okx-candle-service
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/okx-candle-service"]
