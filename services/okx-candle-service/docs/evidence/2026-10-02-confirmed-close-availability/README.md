# Confirmed close availability repair

This local source change is based on owner source commit
`27d35bd529d13febf40ae0daab3036954e41e00d`, associated by the parent task with
image tag `rsibot-v2-candles:hardened-27d35bd52`. The association is a source
identity clue; no rebuilt binary or deployed image was produced or applied by
this task.

Before the change, `parse_row_tolerant`, REST page validation and seed/history
validation accepted a confirmed candle up to two seconds before its close. The
deferred queue also released at `close - 2 s` and discarded the row's original
receipt time. The new behavior classifies every pre-close confirmed row as
deferred, retains its original receipt timestamp, and releases it only when the
local clock reaches `open_time + 60`. The release event carries the original
`received_at` and its actual tick time as `published_at`. A post-close same-open
row supersedes any queued early value before ordinary ingestion; a different
same-open correction received while early replaces the queued row and receipt.
Existing two-lane, sequence, status, cache and wire-field contracts are retained.

`producer-events.json` contains a sanitized 60-row snapshot and the following
actual `State` deferred-candle event. It contains only deterministic test prices
and generated timestamps, with no runtime market data, credentials or network
responses. The event's original receive time precedes close; its publication is
after close and after the snapshot publication.

Validation run from this service directory:

```sh
cargo test --offline --locked
cargo fmt --check
cargo clippy --offline --locked --all-targets -- -D warnings
cargo build --offline --locked --release
```

The full test suite passed (68 passed, 3 ignored); `cargo fmt --check` and
clippy passed. The offline release build succeeded for the host target. Its
SHA-256 is
`14504e4e0daddf399f74d9e8a0db4a8df2bcfde5592c7529de41ec3330f1227a`; `file`
identified it as a Mach-O arm64 executable, so it is not a Linux deployment
artifact. A Linux image build was not attempted:
the local Rust installation has only `aarch64-apple-darwin`, and neither
`rust:1.88-bookworm` nor `debian:bookworm-slim` is cached. No base images,
candle data or dependencies were downloaded. No runtime containers, mounts,
cache files, credentials or external services were changed.


Later qualification supersedes the initial host-only build limitation above. The final producer and cache recovery source passed 72 tests/4 ignored; the explicit ignored read-only test against all five preserved caches also passed with unchanged file hashes. Four caches retained11,520 validated rows after excluding their single109–312ms early row; XRP retained11,521. Cache alone remained unavailable. Independent review accepted the final at-most-one/2,000ms gate. The parent regenerated this producer wire fixture from the final source and all three exact deployed native consumers accepted it offline. The parent built a Linux arm64 image with Cargo.lock and exact verified Rust/Debian base digests; immutable image/binary/source receipts are in the root unified-finish evidence. No runtime deployment is established by these source/build tests.
