# t9 — Reviewer: spec + quality on the consolidated candidate

You are the reviewer in grok-orchestra. Read-only. Two verdicts required: spec compliance and quality.

## Inputs

- Brief/spec: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/spec.md`
- Ledger: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/ledger.md`
- Implementer reports: `reports/t4-merge-bcd.md`, `t5-ag-native-order.md`, `t6-capital.md`, `t7-fees.md`
- Audit: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/audit.md`
- Diff file: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reviews/review-4a4372bc-worktree.diff`
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Head: uncommitted working tree on `feat/andrzej_ok_rsi_audit_consolidation` (HEAD equals base; the diff file is WT vs base)

Read the diff file once. Do not mutate git state. Treat implementer reports as claims. Do not re-run the full suite; the audit file is the test evidence.

## Global constraints (verbatim)

- Authority: repository-process-only. No merge, deploy, restart, or live orders.
- Do not change approved trading policy.
- Preserve chronological compatibility helper, ordinary LIMIT/taker classification, cancel-source once persisted, useful entry lifecycle/recovery after corrections.
- Keep legitimate late TradeUpdates and cumulative-fill reconciliation processable. Do not suppress all messages merely because an order is terminal.
- Explicit prohibitive venue reasons win over local replacement intent. OKX 32 is STP, not post-only 31. Bound identity is not cause.
- Evaluate deadline and revocation regardless of wait state. Expiry prevents new children but does not prove the current submission absent.
- Check current owner state and venue/pair execution-data at the final child boundary. Do not rerun one-shot parent admission or abandon owned fills when entry inputs fail.
- Separate owned inventory, unresolved acquisition, and report labels. Release only reconciled commitment.
- Estimate actual next quantized close quantity for fees.
- Persist cancel_source through native JSON round-trip. Persisting evidence must not authorize restart replay of an entry.
- Keep #50 typed `_live_book() -> Optional[tuple[Decimal, Decimal]]`. Locked/crossed book waits inside remaining intent age; do not terminal `input_unavailable` on that path.
- Bound + missing cancel_source may still confirm local reprice. FILLED-after-CANCELED OrderUpdate is unspecified; do not over-block.

## Return

- Spec: PASS or FAIL, with missing/extra items
- Quality: APPROVED or NOT APPROVED
- Findings: Critical / Important / Minor, each with evidence
- Production-ready for this scope: yes or no

Also write the same verdicts to:
`/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reports/t9-review.md`
