# Native paper reconciliation follow-up — 21 September 2026

PR #69 already descends from current owner master
`0f19b464ccd112b21ac69c6de0e5515250239aac`. This continuation changes only
`NativePaperSession._drive_executors` in production. It preserves native
controller admission, the first control pass, task exception/cancellation
semantics, order identity, recorder, close handling and stale-book gates.

The original PR head `3d87520e9322f84f3b8f179a3fffc37f588b1b88` re-controlled all
fresh executors after any reconciliation task. An unrelated filled executor
could therefore evaluate another transition on the same injected snapshot.
Now only owners of pending reconciliation tasks receive a follow-up, once
per executor, in original traversal order. Closed or removed owners are not
re-driven and replacement instances are not mistaken for the old owner.

This is an in-process scheduler selection, not Python-object order identity.
Native client/exchange order reconciliation remains unchanged. Failed tasks
remain available for their own native executor to handle, as before; the
patch does not reinterpret failed reconciliation as terminal proof.

## Verification actually performed

- Reconstructed the entire 30,282-byte original source and matched its Git
  blob `34791bd147ebecbb04dca1486486ad12bf836d9b`.
- Added ten source-method control-flow tests before modifying production.
  Original result: 7 failures, 3 passes. Candidate: all 10 pass.
- Tests execute the actual methods extracted from the checked source with
  explicit executor/awaitable fixtures. They do not simulate fills, accounting,
  recorder writes or the compiled NativePaperSession constructor.
- Cover unrelated/sibling executors, two tasks per owner, traversal order,
  closure/removal/replacement during await, task failure, cancellation and
  unchanged stale/closed gates.
- AST comparison confirms every production node outside `_drive_executors`
  remains unchanged. Compilation and scoped whitespace checks passed.

```bash
python -m unittest discover -s test/paper_step_contract -v
```

Full native owner/compiled suites and joint validation with PR #68 were NOT
RUN in this conversation container. Run the existing native paper session,
connector/executor, breakout and expiry suites before merge qualification.
No independent review, economic run, deployed observation or promotion is
claimed. The original PR's native test receipt is historical, not rerun here.

## Remaining gates

Unbounded terminal-order retention, durable terminal-proof completeness,
stale-pair maintenance, full recorder/orchestrator restart and load qualification
remain separate unresolved work. This patch does not claim to complete root
#200 or make native paper an admitted economic adapter.

Source identity changes when the installed paper engine changes; do not bypass
restore guards, replace a running image, reseal old state or restart execution
owners from this source publication. Rollback is a reviewed revert retaining
all later source and recorder evidence. No runtime state was touched.
