# Fleet recovery and PR integration audit

## Contract

Objective: review all open fleet-related PRs and current native runtime, correct confirmed gaps that could admit incomplete ownership during recovery, and publish reviewed owner/source updates. Completion requires regression RED/GREEN, exact-source isolated execution, independent final review, truthful current bot permission/hold report and draft PR updates. No promise of fills or zero defects.

Owner: Hummingbot source in this isolated `feat/andrzej_terminal_dust_audit_20261003` branch; base master479b2d, existing PR120d23f and imported exact PR118eeb33677. Root restart correction remains in its own PR343/worktree. Root/API/Condor/quants dirty work is preserved. Research and physical mobile acceptance are outside the live-execution audit.

Authority: live-adjacent observation plus local code/tests/commits/push/PR. The latest user explicitly prohibits live restart, deployment, merge, order/cancel/fund, production-record or risk-setting actions. No such action will be used to verify controls. Missing V1/V3 daily-loss choices remain unresolved, without guessed limits.

Evidence: exact PR diffs and base/head metadata, retained native backups, fresh authenticated API GET and bounded read-only venue probes, native file/stdout logs, mounts/images/restarts/OOM, existing tests and official OKX/native accounting contracts. Prior reports are historical; current observations have timestamps.

Validation: reproduce fleet ownership omission with native terminal fill records, validate complete per-asset conservation and invalid/missing proof refusal, exercise actual native SQLite reader in network-disabled read-only Docker, run planned-resume/orchestrator/terminal-dust/accounting/native lifecycle suites, repository lint and complete diff review. No imports/application tests on a credentialed host environment. Immutable mode is only for standalone sealed backups.

Rollback: this source branch and test artifacts only; retain all production state/images, old source commits and private receipts. No force push, bulk cleanup or stale DB restore.

## Confirmed follow-up defects

- PR118 ownership derivation ignores physical terminal dust with zero managed remainder for V2/V3, and V1 venue coverage omits older closed residues. An HMAC over incomplete totals does not establish full ownership. Missing/erased dust receipts must be caught through complete native-fill conservation.
- PR343 excludes terminal/null-close rows without a restart_suspended marker; parent delegates smallest fail-closed root fix/tests in that PR's owner worktree.

## Repair plan

1. Integrate exact PR118 source with PR120 without reverting the current public-price routing fix.
2. Share strict terminal dust proof between native portfolio and stopped-database ownership projection; require exact native Order/TradeFill and lot evidence, unique attribution and complete per-asset reconciliation. Missing proof fails closed. No new sell authority.
3. Bind inputs/source to recovery evidence; old unqualified payloads cannot become successful by omission.
4. Review current fleet/reporting PRs and logs; preserve unconfirmed transport causes separately.
5. Execute local matrix, final independent review and updated draft PRs. Document remaining production prerequisites.

## Review-driven additions

The broad modular matrix caught accidental V2 source expansion from putting the
shared dust helper under modular_spot_core. Moving the helper into its V3 owner
restored the old frozen digest without weakening the isolation test.

Independent final review then reproduced a different, intentional shared-source
change: three native funding fee parsers refused real signed maker rebates.
Correcting these existing parsers preserves exact native accounting, finite and
supported-currency guards, nonnegative inventory cost, and fee ceilings. It
changes only native_funding.py in the same 67-file V2 closure. A second review
reproduced contradictory valued-fee acceptance in the sign-only correction;
requiring equality with actual base/quote fees qualified the final digest
9803842e (superseding intermediate e86f4e08 and historical 17cba31). The deliberate
reseal and V2/V3 migration prerequisite are recorded in
docs/controllers/modular_spot/RELEASE_AUDIT_20261003.md. No live bindings change.

The original API-password exposure mentioned by root PR333 is a historical
incident; current credential rotation status remains to be established without
printing or rotating secrets. Root PR343 source correction was independently
reviewed and pushed as a draft, with216 tests passing. Other PR source/metadata
review does not imply economic or physical-device acceptance.

## Final source result and remaining gates

All five repair steps are complete for source qualification. Native instrument
reporting additionally uses the executor/held-position pair rather than the
controller identifier; actual StrategyV2Base method regressions cover the change.
The root consumer still cannot produce complete V3 per-pair money without a
qualified native coverage/mark/dust contract. This remains explicitly unavailable,
not inferred from partial rows or shared-wallet balances.

Frozen executable owner source: 3030513324e2192e8263b1fd794f0783afb179d4.
Independent final review found no remaining new-source correctness blocker.
Final isolated image ff7d001a07578c40645b9121ea916e057ae39eea768550be3eca7dfda9f6857e:
161 focused tests plus13subtests; 490 scoped tests plus15subtests; 1961 native
workflow tests plus76subtests,21skips; 76 optional Arrow candle tests resolve20
of those skips; 89% changed-line coverage. The unchanged base and final both have
20 broad stable collection errors; global suite/coverage and clean release-image
matrix are not qualified. Pinned pre-commit and root216tests/Ruff pass.

Native accounting replay preserves V1's three ETH residues and all five V3 pairs.
Full historical recovery conserves25exactV3fills but correctly refuses V1 legacy
fills lacking exact precision. Production remains unchanged: 12:43UTC native API
has all entry-pause flags false, V1ETH exposure_unavailable and V3BTC inventory
mismatch. V2 conditional admission has no current reason. Live deployment remains
HOLD for fresh venue/owner continuity, identity migration and V1/V3 daily-loss
decisions; authority excludes restart/merge/deployment. No test trade was used.

The source and helper remain separate draft PR120 and rootPR343. This completed
source-audit plan is archived with final evidence, contract limitations and rollback
in docs/reports/2026-10-03-fleet-recovery-review.md and its sanitized verification
JSON. Publication/remote-head receipts are retained with the private evidence;
publication does not clear any production gate. Root PLAN/TODO and memory stores
are untouched.
