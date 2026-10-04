# Terminal dust ownership and admission audit

## Goal

Repair the complete native accounting path behind V1 ETH infinite exposure and V3 global inventory mismatch, preserving owned base, exact fill/fee conservation, bounded capital admission, lifecycle ownership, recovery and truthful reporting. Create and review an owner PR after safe qualification. No strategy optimization or invented risk policy.

## Current State

Owning repository: `s1korrrr/hummingbot`, base `master` at `479b2d34b214573f82cb51623b99201f56c19469`; isolated branch `feat/andrzej_terminal_dust_audit_20261003`. Only accounting corrections `71dfc50` and `9d72aaa` were transplanted; draft V1/V3 recovery/adapter branches and unrelated dirty root/nested work are preserved. Original live owners run different exact images; source, offline image and deployed behavior are separate claims.

V1's first correction incorrectly erased proven terminal dust and used only bounded controller history; accumulated and historical residue must remain in capital exposure. V3 must retain quantity/basis, strict native lineage and exact reconciliation across complete execution paths. Current scoped runtime holds remain until separately qualified procedures pass.

## Target State

Native receipts/fills remain authoritative. Aggregate proven terminal dust from complete native owner history with unique order/trade attribution. Dust remains valued owned inventory but creates no duplicate commitment, held lot, grant, trailing executor or sell authority. Missing or contradictory proof fails closed and is observable. Legitimate partial-fill/cancelled-entry completed exits must retain their exact accounting.

## Risks and Failure Modes

- Clearing Infinity by zeroing actual inventory or dropping history older than twenty reports.
- Duplicate venue/order/trade claims, wrong market/side/unit, incomplete fills, invalid fee signs or precision.
- Treating cancelled empty orders as no-fill without positive terminal evidence.
- Dust leaking into DCA/spacing/trailing/sell authorization, or gross/config entry price corrupting basis.
- Unsafe live recovery, source identity mismatch, missing durable risk history or unspecified loss limits.

## Milestones

1. Audit base, callers, native receipt producer, recorder/report cache, portfolio/grant/symbol paths, source seals and restart entry points; preserve dirty inventories.
2. Reproduce confirmed failures on original source, implement bounded owner fixes, retain adversarial regressions.
3. Build a tracked-source, credential-free native image and run mapped local workflow, accounting, real native paper lifecycle/restart and source-seal checks; independently verify native replay arithmetic.
4. Fresh independent final-diff review, intentional commits only, sanitized verification matrix and draft/ready PR with rollout prerequisites. No merge under this task.
5. Prepare current guarded native recovery artifacts where safe. Later user steering authorizes a scoped V1/V3 restart if needed, but unresolved owner proof, identity migration, effective fence, risk-history and operator loss-policy gates cannot be bypassed. Do not execute a restart with unmet gates.

## Verification

Docker checks use `--network none --read-only`, synthetic/native paper fixtures and no production credential/data/socket mounts. Source builds copy only tracked owning files over a documented compiled environment. Compare baseline and corrected regression results; inspect logs and assert source/image identity. Inspect actual native state only through readonly SQLite transactions or Backup API and bounded readonly endpoints; publish sanitized counts/booleans rather than actual order identifiers.

Map `.github/workflows/modular-native.yml` and affected stable tests to local checks. Record executed/unavailable separately; hosted Actions budget is not a blocker. No economic backtest is needed for this accounting correction; synthetic lifecycle tests do not establish strategy economics or live readiness.

## Decision Log

- 3 October: latest explicit authority permits code, safe tests and PR; later steering prioritizes two defects and authorizes restart only if required and qualified. No merge, capital-policy guess, checkpoint rewrite or order action.
- Preserve per-executor managed remainder separately from physical terminal residue; controller report cache is not complete inventory history.

## Progress Log

Completed source repair and isolated qualification at f8e21a484. Independent production-diff and fixture review found no blocking issue. See docs/reports/2026-10-03-terminal-dust-capital-audit.md for exact revisions, baseline regression failures, passing local workflow/accounting/native replay results and unavailable checks. Live rollout remains HOLD for the documented owner, continuity, source migration and loss-policy prerequisites; no live action occurred. Owner draft PR publication is the final source handoff.

## Rollback / Recovery

Keep changes on the repair branch and retain existing images/state. An offline failure never changes production. Future failed guarded stop/launch leaves owner stopped with receipts, not stale DB restored. Source publication does not activate a strategy or certify safe restart.
