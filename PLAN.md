# PLAN

## Task
- Objective: repair PR #26 so episode parity binds actual quants-lab artifact bytes and compares complete episode economics, including terminal insolvency evidence.
- Owner repo(s): `hummingbot/` only.
- Capital impact: research-only, read-only parity evidence; all paper/canary/live/capital/Risk/execution/trading authority remains false.

## Context
- The v1 runner accepts a caller-supplied SHA-256-shaped string without reading the artifact it claims to identify.
- It evaluates one terminal snapshot and five NAV-breakdown values only, so identical terminal NAV with different reward attribution can report `MATCH`.
- `liquidatable_nav()` raises for zero or negative NAV, suppressing terminal liquidation/insolvency evidence.

## Assumptions
- Raw file-byte identity is the correct provenance boundary for the quants-lab parity artifact.
- Episode parity requires both before/after snapshots, full attribution, settlement mode, exact fill IDs, and snapshot/fill timestamps.
- A zero terminal NAV is `LIQUIDATED`; a negative terminal NAV is `INSOLVENT`; neither grants authority or constitutes promotion evidence.

## Constraints
- Technical: strict exact-key schemas, canonical decimal text, regular non-symlink inputs, deterministic atomic result publication.
- Operational: no connector, executor, controller runtime, Docker, credential, account, service, or order mutation.
- Risk/capital: no paper/canary/live promotion; all authority booleans fail closed and remain false.
- Costs: compare gross market PnL, funding, fee, spread, slippage, impact, borrow, liquidation penalty, close-cost reserve change, and external cashflow.

## Options Considered
1. Extend v1 with more caller-supplied expectation fields and another hash string.
   - Pros: smallest wire diff.
   - Cons: still does not prove which artifact bytes produced the expectations; preserves the audited provenance defect.
2. Add a strict v2 two-file boundary and hash the exact artifact bytes read by the runner while preserving v1 as explicitly legacy, authority-false evidence.
   - Pros: binds provenance and economics at one read-only boundary; enables independent tamper probes.
   - Cons: intentional wire/CLI version change for this draft-only contract.

## Chosen Approach
- Choice: option 2. Add a v2 path with a required `--quants-artifact` regular file whose raw SHA-256 must equal the input declaration and whose exact schema contains the complete expected episode. Preserve v1 parsing and verification as an explicitly legacy, authority-false path; only v2 is eligible for new parity-readiness evidence.
- Why: only this makes artifact identity a property of bytes actually consumed and prevents attribution-equivalent-NAV false matches.

## Execution Plan
1. Add RED regression tests for raw-byte substitution, attribution-only drift, identity/settlement drift, zero/negative terminal outcomes, and authority invariants.
2. Implement explicit episode observation, terminal outcome/reward, complete expectation/result comparison, and strict v2 I/O.
3. Update persisted-result and pair verification plus CLIs to require/recheck the exact artifact.
4. Run focused tests, exact tamper probes, compileall, pre-commit, and shadow diff checks.

## Test Plan
- Unit: accounting classification/reward and mismatch-field reconstruction.
- Integration: runner/verifier/pair CLIs with real temporary input, artifact, and result files.
- Smoke/regression: exact artifact-byte, attribution, settlement, fill/timestamp, and result tamper probes; focused four-file pytest suite.

## Risks and Rollback
- Risks: v2 is intentionally incompatible with v1 at the wire/CLI boundary because it requires a second artifact file and complete episode semantics. V1 remains readable/verifiable for historical evidence but cannot satisfy the new parity-readiness contract.
- Rollback: revert only the local Conventional Commit(s); no external state or runtime is changed.

## Memory Impact
- No durable repo memory update expected unless a stable command or boundary beyond this PR-specific contract emerges.

## Final Notes
- What changed: added an explicitly v2, raw-artifact-byte-bound parity path alongside legacy v1; compares complete episode economics, settlement, canonical quants fill/timestamp identity, and explicit terminal outcomes; both v2 match-producing verifiers require exact input/artifact/result re-evaluation; all authority remains false.
- Validation run: observed RED for missing terminal evidence/v2 implementation, raw-byte substitution, exact per-fill timestamp identity, missing standalone `--input`, and coordinated semantic rehash; final conda suite `44 passed`; adversarial subset `20 passed, 7 deselected`; compileall passed; touched-file pre-commit passed; canonical quants fixture SHA `4a1f997ffdd7bcaa258488b8e0f31b713931b57ead794d4bdbff14d01a6201bf` produced pair `MATCH` with all-false authority.
- Follow-ups: independent parity assurance re-review at the final local head before any push. Producer/runtime cross-fixture proof is local contract evidence only, not research quality or promotion evidence.
