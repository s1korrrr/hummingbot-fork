# Thin-scout exit after a protected first stage

## Outcome and authority

Repair the native state invariant that an active retained runner cannot coexist
with EXIT and a zero inventory target. Research-only controller correction;
no configuration, capital, deployment, paper/live, external or order authority.
Keep prices, quantities, targets and existing loss/profit rails unchanged.

## Evidence and hypothesis

Both V19/V21 BNB 10-bps May–August replays failed their first-target audit.
The exact failing replay ledgers are being captured without bypassing the
original auditor. A separate two-transition native reproduction proves:

1. A protected trailing breach below stage one leaves the full target intact,
   sets phase DISTRIBUTE and structure_runner_active=True, with no sell.
2. A subsequent recovery earns stage one and qualifies for thin-scout release.
   The native request correctly targets zero but retains the active-runner flag.

The research thin-scout auditor also assumes that the prior full target must
be in MARKUP; the native protected stage can validly use DISTRIBUTE without
having reduced the target or sold inventory. Confirm the historical receipt
before attributing the BNB failure to that exact path.

## Implementation boundary

Add regressions for V19, next-stage confirmation, and V21, including state
round-trip before the transition. Clear the active-runner flag when a
thin-scout release changes the target to zero. Do not weaken target, revision,
classification, profitability or prior-sell checks. Quants Lab owns the
corresponding receipt validation and failure-evidence capture.

Do not edit native/runner source during the frozen cost cohort or original
failure capture. Once those finish, preserve their receipts and implement the
correction. New-source replays must be reported separately; do not overwrite
failed cells or present old-source economics as validation of the repair.

## Completion and validation

- [x] Capture the failing BNB ledger and identify the exact prior/current state.
- [x] Observe the native regression failing, then passing with the focused fix.
- [x] Prove invalid prior target/revision/classification paths remain rejected.
- [x] Run the relevant native episode suite and Quants receipt regressions.
- [x] Run a paired BNB 10-bps post-fix native replay with exact common sources.
- [x] Seal the old-cohort status and clearly disclose any incomplete new-source
  economic matrix. No strategy or execution promotion follows from this fix.

Rollback is the single flag-clear change plus its corresponding auditor fix;
retain all failure evidence. The default-off lifecycle feature remains opt-in.


## Historical receipt correction and validation

The captured BNB first-target failure came from a profit-guard delay after
arming EXIT, not from a protected DISTRIBUTE origin. Native runner flags in
that historical trace were valid. The synthetic regression still proves the
separate native flag inconsistency, so retain both fixes with separate evidence.
Quants now binds the first order to its revision origin and checks every
profit-guard-delayed state; it also admits an unchanged protected first target.
No profitability gate is relaxed.

Three new native regressions were observed failing on the stale flag before
the one-line fix. Afterward the nine-file native controller/episode/entry/wallet
suite passed: 322 tests and 19 subtests in 6.92 seconds. Quants runner/receipt
suite: 73 tests passed. Paired native BNB 10-bps replays are underway with the
new source fingerprints; preserve the original blocked cohort separately.


## Completed paired replay

Both repaired-source BNB 10-bps May–August native runs completed with status
DIAGNOSTIC_COMPLETED_NOT_ADMITTED. Exact before/after V19 debug-trace parity:
49 actions, 88 events, 177120 decisions, 177121 balances; every value equal.
The source comparison differs only in episode.py and the Quants receipt auditor.
Both policies have 20 logical buy roots, 19 filled roots, 6313.2947901 USDC
filled quote, zero pending orders, underwater sells and reconciliation errors.
V19 PnL 160.950104694; V21 174.7073902374; delta 13.7572855434 USDC.
V21 drawdown is worse by 0.0558015 percentage points but within this pair's
frozen local tolerance. The original BTC 20-bps failure still rejects V21.

Repair receipt is in the Quants worktree under output/backtests/
ok_rsi_earned_stage_risk_skim_v21_20260904/thin_exit_repair/verification.json.
Native episode.py SHA256: 8c03b38d6014f7f7cc890d0fd54bcc9d4a40ca67d74577f8ae530207aedf243b.
Independent hq_reviewer found no blocking defects. No broader new-source matrix
or promotion is claimed; no additional economic reruns are needed to preserve
the already failed cost-gate decision. All planned repair validation is complete.
