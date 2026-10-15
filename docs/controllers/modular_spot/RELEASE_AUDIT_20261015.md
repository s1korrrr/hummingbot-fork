# Modular spot closure reseal for PR #128 — 15 October 2026

The modular spot closure (`controllers.generic.modular_spot`: 39 controller files plus 28 shared components, 67 hashed) changed
on master on 5 October 2026 in
[PR #128](https://github.com/s1korrrr/hummingbot/pull/128) (merge `f3e602162`),
but its two pinned digests were not resealed. Both tests failed on master
`77d660445`. This record reseals them; no source changes.

| Commit | Closure files changed | Change |
| --- | --- | --- |
| `f3c96d972` | `modular_spot_core/state.py`, `bag.py`, `sell_only.py`, `native_recovery.py` | SQLAlchemy storage unavailability (for example SQLite `database is locked`) raises `DataUnavailable`, the existing retryable hold, instead of `PolicyError`, which latched a controller fault until a restart. Integrity and compare-and-swap conflicts still refuse and latch. |
| `a6bfde2b6` | `modular_spot_core/native_funding.py` | Only `NativeFundingStore`'s peer capacity projection passes `allow_unacknowledged_tail=True` to `validate_acquisition_chain`. Executor reconciliation and release proofs stay strict. |

The PR #128 description records the cause, fail-before/pass-after regressions,
an independent review at `a6bfde2b6` (recommendations applied in `1adeb1506`)
and a broad local test run. That run did not include `test/crash_recovery_v2`
or `test/spot_lifecycle_v2`, so the pins were not updated. Both commits are
ancestors of the deployed engine commit `1623e69c5`.

## Source identity

The closure digest was recomputed from `git show` content at each commit:

| Revision | Digest |
| --- | --- |
| `d7af881f8` (last reseal, RELEASE_AUDIT_20261005.md) | `b6a9dd1193d42fdb42637b1a8ea1bd7222bddf9bb40b820c54276133cd0c8321` |
| `f3c96d972` | `b94446886426c0df729fa544ae99c860dd9d325daa6e40c02b5a1948aa2d5555` |
| `a6bfde2b6`, `f3e602162`, `1623e69c5`, `77d660445` | `92a3ed2e644a6b671a24fe1f6907acda917c203878325e103021429ad9a66aa7` |

The closure changes from `b6a9dd11…8321` to `92a3ed2e…6aa7`. Only the five
files above change; no path is added or removed. Crash and V3-only dust modules
remain outside the closure.

`test/spot_lifecycle_v2/test_seal_blast_radius.py` hashes the same closure (67 files: 39 controller files plus 28 shared components)
path-only. Its pin `14e76708f297834597beaa7b7eb38b89aa00e5ce1f781b01a406a8264c625224`
was stale since `0a3b7538b` (RELEASE_AUDIT_20260930.md), which resealed only the
isolation test; it was missed at every later reseal too. It is resealed to the
same digest here.

Historical receipts, checkpoints and deployed bindings keep their actual
identities and are not rewritten. Nothing is deployed by this record.
