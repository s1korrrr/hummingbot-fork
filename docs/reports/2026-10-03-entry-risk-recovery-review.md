# Native recovery and daily-risk enrollment review

Executable source: `85bd99c6fab22a8144f25420c2c8b59176f6001f`, continuing
`52724143dbc253454a2dd962c2461f11401222fb` on Hummingbot PR 120. The PR base
is master `479b2d34b214573f82cb51623b99201f56c19469`. Earlier dust, inventory,
fee and reporting corrections remain covered by the full PR review and tests.
Root release/restart integration is separately owned by
[rsibot PR 343](https://github.com/s1korrrr/rsibot/pull/343).

The operator selected **1,000 USDC per bot**, retaining the daily entry guard.
That choice does not clear V2's recorded breach/pause history or authorize a
first historical baseline, source migration, deployment or activation. This
continuation changed isolated source branches only. No live risk setting,
database, container, order, credential or shared service was changed.

## Confirmed paths and corrections

| Path and failure | Cause | Repair and retained invariant |
| --- | --- | --- |
| V1 recovery rejected 13 historical fills | Six-decimal TradeFill rows lacked additive exact fields although unique complete native held-ledger order/fill receipts retained the exact values | Validate the entire retained receipt against script, order, pair, ownership, fees, aggregate fill and truncation bounds before using its exact quantity; retain separate snapshot provenance and reject conflicts, incomplete coverage and orphans |
| Valid exact companion rows failed during detached ORM recovery | Exact values were lost when the session attachment disappeared | Carry verified exact values on the detached native row; the regression fails without the correction |
| Approved first daily enrollment could not restore | Existing startup supports restoring a daily rail, but cannot invent a missing prior baseline or missing historical peaks | Add an explicit pending enrollment and atomic native transition archive; preserve existing peaks or identify a new prospective scope with unknown history; all entry fences remain in place |
| V1 terminal recovery could not coexist with the new pending rail | Its strict version-1/no-daily-risk preflight equality rejected a legitimate reviewed transition | Admit only the archived prospective enrollment derived from the untouched original preflight; recheck original history, native digest, exact source/config, limit and archive at prepare, pending validation and claim |
| Risk archive transaction was not durable in Python autocommit mode | Python `Connection.commit()` and `rollback()` are no-ops when `autocommit=True` | Use explicit SQL COMMIT/ROLLBACK for the owned BEGIN IMMEDIATE transaction; injected archive failures leave neither partial risk state nor archive |
| Pending startup logged a reconciliation exception while reports initialized | Missing initial reports were treated as a corrupt initialized PnL report | Wait while report initialization is incomplete, after verifying the limit and every entry fence; missing/nonfinite PnL in a valid report still refuses |

Independent review also found a missing mandatory enrollment schema version;
the model now requires a strict integer version and rejects omission, booleans,
nonfinite numbers and inconsistent baseline/clock bindings.

This is an accounting/recovery correction and an explicit policy enrollment
mechanism, not strategy optimization. Hummingbot retains execution ownership.
Condor and reporting remain projections. No stale data, fabricated financial
values, relaxed ownership tests or lowered log severity are used.

## Executed qualification

Frozen test image:
`sha256:09c92ad2dd6c73d0ccb4b8a8a12a77bb6c256550447b193e384445e0aac8cda2`.
Source manifest:
`d16b226552a4d37fd08e73054ce53966344ed5639f2213f3b1430c28ad3ec6e5`.
The image contains the exact nine changed Python files over the previously
qualified compiled native image. File hashes match the executable commit.
It is a verification image, not a qualified per-owner live release image.

Environment: compiled ARM native Hummingbot, Python 3.13.15, SQLite 3.53.4,
Pydantic 2.13.5 and pytest 9.1.1. Tests used credential-free containers with
`--network none --read-only`, bounded CPU/memory and disposable writable tmpfs.
Sealed native copies were mounted read-only; rehearsals wrote disposable copies.

- All pytest targets from `.github/workflows/modular-native.yml`: **2,008 passed,
  21 skipped, 76 subtests**. Twenty skips need optional PyArrow; one needs the
  external candle cache. The 3,054 dependency warnings match the preceding
  qualified run's categories/count and remain visible.
- Scoped accounting, real native paper lifecycle, recorder reload, risk,
  acquisition chain and recovery: **560 passed, 15 subtests**. Existing NumPy/
  hmmlearn deprecation warnings remain visible. Suites overlap; counts are not
  combined into a unique-test total.
- Full recovery/Meridian selection: **586 passed, one skipped, 18 subtests**.
  The skipped fresh-process native-configuration test requires an explicit
  disposable-security environment; it passed in the scoped run above, which
  supplied that environment and synthetic credentials on tmpfs.
- Changed-line coverage against the complete PR base, including newly added
  source files: **89% of 1,746 lines**, passing the workflow's 80% threshold.
- Required compiled/native imports and new recovery/risk imports: passed.
  Root V3 launch tests against this exact image: **21 passed**; root's separate
  host suite passed **309 tests, with three native-import skips**.
- Pinned pre-commit formatting, Flake8, isort, private-key detectors and diff
  checks passed. Independent source reviewers audited the full preceding
  repair and this continuation; the final V1 transition review found no material
  defect. Reviewer inspection is separate from the executed tests.

RED/GREEN evidence is retained for missing schema version, lost exact companion
values, initial risk restore, SQLite autocommit/rollback, collecting startup
reports and the combined V1 archive/prepare/claim path. Adversarial cases include
changed peaks, wrong staged limit, deleted or rehashed archives, unrelated
Metadata drift, duplicate/replayed commands and native financial-state drift.
The original preflight remains unchanged, including its noncanonical historical
JSON serialization. Financial digest normalization is limited to strategy-risk
JSON; unrelated stored values retain exact drift checks.

The realistic enrollment case uses an actual NativePaperTradeExchange fill,
executor and native performance report with an independently calculated bid
mark. It does not constitute a full application launch or live exchange test.
The cold-start regression mocks the base tick; that limitation is explicit.

## Native-copy and production evidence

Consistent Backup API snapshots were captured at 13:06 UTC on 3 October.
Using fresh successful public instrument rules, native inventory proof covered
**198 V1 fills, 56 V2 fills and 25 V3 fills**. V2/V3 exact residual derivation
passed. V1's held, active, dust and separately selected BNB quantities conserve
the native fill inventory. Historical precision is no longer an unresolved
source gate; actual live suspension and venue checks are still required.

Disposable-copy risk rehearsal preserved every non-Metadata row: 20 V1 tables,
19 V2 tables and 18 V3 tables, with integrity checks passing. Actual V1 archived
pre-state reconstruction passed. V2 changed only the limit while retaining its
daily baseline and history. V3's real identity migration carried all five
decision states and one grant before an explicitly simulated new risk scope.
Simulation preconditions are not observations of stopped or fenced live owners.

At **14:45:50 UTC**, all 13 live services retained their original container,
image and start identities, with zero restarts/OOMs and all 11 configured probes
healthy. Native API reports were current and identity-verified (0.12–1.57 seconds
old); all five candle feeds were online/ready. All current entry-pause flags were
false. V1 ETH still reported `exposure_unavailable`; V3 still held on
`NATIVE_INVENTORY_MISMATCH:BTC-USDC`. V2's historical ETH/SOL pause labels do not
override its current open flags. Its live daily limit remains 50; V1/V3 still
have no daily rail. None has been changed to 1,000 yet.

Since 13:00 UTC, V1 had one recovered price timeout and V2 had two. V3's public
book WebSocket closed at 14:18:44 and resubscribed about 0.23 seconds later.
These are retained recovered transients, not evidence of clean/error-free logs.
Direct post-reconnect order-book freshness was not independently measured.
The broker's repetitive localhost INFO pairs match its 15-second TCP probe;
Telegram remains retired. The host Data volume has 23 GiB free at 98% usage.

## Remaining gates and rollback

The PR stays draft. Prospective first-mark enrollment for V1/V3 and V3's explicit
new historical-risk scope still need the pending operator policy decision.
Per-owner release images, fresh maintenance/fence/stop proof, funding and venue
reconciliation, V1 BNB plus both SOL suspension receipts, V3 BNB continuity and
source-identity admission must be qualified for a specific approved cutover.
Risk request booleans assert those conditions; they cannot establish them.

No economic backtest was run: signal thresholds, sizing and strategy economics
were not tuned. Full live process startup, authenticated submission/cancel flows,
physical device behavior, alternate architecture and a clean native compilation
were not requalified. Unchanged Rust jobs were not rerun. Prior broad-suite
collection failures remain separately documented; full-suite/global coverage is
not claimed. Hosted Actions are not evidence and are not a budget-only blocker.
V3's complete per-pair valuation projection remains separately unverified.

Version-2 risk records require a compatible runner. Increasing the daily amount
does not clear a breach or resume entries. The rail gates new entries rather
than guaranteeing a loss ceiling or forcing liquidation. Preserve backups,
archives and current order truth; never restore an old database after trading
may have changed native state. A failed cutover gate must preserve fencing and
receipts for a reviewed forward repair.

Contract and primary references: [entry-risk enrollment](../strategy_v2/entry-risk-enrollment.md),
[Python 3.13 SQLite transactions](https://docs.python.org/3.13/library/sqlite3.html#transaction-control-via-the-autocommit-attribute),
[SQLite transaction semantics](https://www.sqlite.org/lang_transaction.html),
and [OKX V5 spot quantity/fee contracts](https://app.okx.com/docs-v5/en/).

## Addendum (2026-10-03 16:00 UTC): terminal planned suspensions (superseded)

The V3 live cutover exposed a restore gap: an older planned suspension of a
previously restored executor had been checkpointed `TERMINATED` with no close
type and a `restart_suspended` marker. Commit `cc0daba46` temporarily admitted
marker-only terminal restoration. Its historical in-image checks passed 167
tests/1 skipped (restart continuity, planned resume, recovery inventory) and
375 tests (Meridian and recovery selection); V3's BNB executor was restored at
both restarts under that deployed version. Root record: rsibot
`docs/reports/2026-10-03-v3-entry-recovery-cutover.md`.

The marker-only terminal restoration described above was superseded by the
subsequent lifecycle and ownership correction. The current restart path keeps
new planned suspensions RUNNING while their control loops stop. An older
TERMINATED row without a close type remains an unresolved ownership obligation;
its `restart_suspended` marker alone cannot authorize restoration. V1's older
BNB row therefore requires the sealed planned-position manifest, exact original
preflight and stopped-state receipts, native order/fill and peer-continuity proof,
and a successful claim before the executor can resume. A failure leaves entries
fenced for reviewed forward repair. The V3 restoration reported above describes
that historical deployment, not a standing authorization for marker-only replay.
