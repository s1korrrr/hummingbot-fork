# Momentum native boundary consolidation — 2026-09-22

The H70 host now consumes the canonical H67 B0/C1 policy and its ordered native
1m/5m/15m publication-bound candle contract. The pure policy files are unchanged.
Native adapters live in `controllers/generic/momentum_fusion_v2_runtime`, outside
the pure policy and the frozen shared strategy-component source closure.

Observation and shadow use the OKX data identity. Paper observation requires the
exact native `okx_paper_trade` mapping. Every mode rejects execution enablement,
initial inventory adoption, and native executor validation. No example with
invented initialization/loss/exit/sizing/capital policy digests is shipped: a
caller must supply a fully bound canonical `MomentumRecipe`.

The local native paper session supplies real publication timestamps and supports
three-stream observation with no submitted orders. `get_custom_info()` produces
`momentum_fusion_v2` with controller identity, execution mode, false execution
flag, exact current source digest (null after drift), bound recipe digest, and
`observed_at` captured only during an update. Missing publication metadata stays
unavailable. Reading status cannot refresh old evidence.

The paper-only admission helper reads all balances, inventory marks, and pending
BUY collateral from the exact isolated native paper session and its recorder.
Unknown asset valuation, unresolved terminal events, a stopped session or pending
connector work fails closed. It charges the whole isolated account to the
program conservatively; this is not shared live-wallet ownership evidence.
Admission holds a file lock across reload, account assessment and durable claim
reservation. Independent writers cannot overwrite each other. Historical claims
are released only after durable native executor and exact terminal order/fill
receipts are present; disappearance and an `is_done` flag are insufficient.
The claim records a digest of those receipts. It is a reservation journal, never
an alternative order or fill ledger.

## Remaining qualification gates

The host deliberately does not call the acquisition helper. Mapping and native
account/claim tests do not establish a complete executing host. Execution remains
blocked until the owner connects canonical opportunity/prerequisite identity to
native admission, supplies durable initialization and ownership evidence, and
qualifies native fee/trailing/exit arbitration plus crash recovery end to end.
`plan_management` explicitly returns `native_exit_admission_unqualified`; it
cannot manufacture ownership or fee evidence from a position dictionary.
No live, paper, canary, deployment, research or promotion authority follows from
these repository tests. Neither running rsibot stack is changed.
