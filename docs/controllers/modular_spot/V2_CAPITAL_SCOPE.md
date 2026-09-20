# V2 native capital and daily entry pause

The opt-in `funding.scope: strategy` policy binds every genuine modular
controller in one native runner to one recorder-backed admission envelope.
Every peer must have the same funding policy, a unique controller ID and pair,
and the same connector and execution mode. A changed owner set, configuration,
source identity, or ambiguous native recovery evidence requires reconciliation.
The default `scope: controller` retains the existing one-controller behavior.

The V2 launch policy is:

```yaml
funding:
  scope: strategy
  allocation_quote: 50
  max_position_quote: 25
  fee_reserve_fraction: 0.003
```

The per-pair limit includes the complete owned position, additional DCA entries,
and pending native intents. It is not a fresh 25 USDC allowance for each
executor. Both limits include configured entry fee reserves and actual native
fill fees. The deployment receipt must verify that the reserve covers the
account's applicable fee schedule. The native wallet availability check runs
before durable funding admission, including fee reserves for already pending
funded orders. Insufficient available cash rejects the request without
creating a funding claim.

Native `TradeFill`, exact order receipts, executor history and `PositionHold`
remain accounting truth. The envelope stores admission commitments and their
reconciliation evidence; it is not a second balance or fill ledger. A canceled
or rejected entry retains uncertain unused capacity until the existing native
terminal-status and complete-fill reconciliation proves release safe. Partial
fills, native acquisition basis, and prior losses continue consuming capacity.
Profits do not enlarge the configured envelope. Restoration requires the same
owner identities and native evidence; changing the deployment does not reset
capital history.

This scope does not coordinate independent V1 recorders or reserve the shared
OKX wallet against V1 orders. The local 50 USDC ceiling bounds these V2 owners'
admission; available wallet cash can still change independently. Never describe
this as an account-wide lock. No V1 configuration or process change is part of
this implementation.

The runner's `max_daily_loss_quote: 5` adds a durable UTC entry pause to the
existing emergency drawdown rails. It reads native reported PnL and saves its
baseline, last trusted observation, breach day and latch in the existing native
strategy risk record. A breach pauses new BUY admission through the existing
operator entry-pause path, while already-running protective executors and SELL
admission continue subject to the other native guards. It does not issue
`StopExecutorAction` or stop a controller.

UTC rollover uses the last trusted observation as the next baseline, so losses
through an observation gap are retained conservatively. Rollover alone never
resumes a breached owner. After rollover, a fresh, correctly addressed
`global_resume_entries` command without a controller ID explicitly acknowledges
the runner-wide latch; a new breach in that UTC day still rejects the command.
The existing command identity, freshness, durable replay rejection and
persistence rules apply. Missing or malformed saved risk state, a changed loss
limit, unavailable native PnL, or a regressing clock fail closed. The actual
runtime status producer exposes `daily_entry_risk` for operator inspection.

## Paper owner isolation

Native orchestrator report, reconciliation and persistence methods accept an
optional keyword `controller_ids`. `None` preserves all-owner behavior; an
empty set selects nobody. A scoped call never marks an excluded executor or
position and never clears or zeroes its saved position. Previously observed
wallet-sale receipts belonging to excluded owners remain intact. Ownership
ambiguity for a selected pair still fails closed.

The paper session selects owners with fresh books, advances their native
executors and publishes only their current reports. A stale peer's valuation is
explicitly unavailable and its prior native state is retained. A fresh peer's
protective exit can continue while another pair is stale. These APIs do not
claim portfolio valuation is complete when any exposed owner is unavailable.
Paper restart remains a reconciliation hold; it does not promote simulated
inventory to live inventory or grant automatic resume.
