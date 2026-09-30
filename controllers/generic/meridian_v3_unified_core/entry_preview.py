"""Read-only symbol entry preview before scarce portfolio capital is allocated.

The shared planner remains the entry-policy owner. Its revised checkpoint is
intentionally discarded here: only the normal runtime may persist and dispatch.
Store handles may be initialized, but this path never reserves, writes a claim,
freezes a selection, or consumes an executor/setup identity.
"""
from controllers.generic.modular_spot_core.bag import HeldBag
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.planner import plan
from controllers.generic.modular_spot_core.runtime import ModularRuntime
from controllers.generic.modular_spot_core.state import DecisionState


def preview_entry(runtime, market, *, now, first_observed_cut, entry_hold=None):
    """Return a complete native entry config or its admission hold, without committing it.

    Meridian's fixed symbol universe permits the shared runtime's pre-grant checks
    without the SymbolRuntime grant-required override (which would be circular).
    Dispatch later repeats the planner and native permission checks against fresh
    inputs; a preview is neither a grant nor a pending executor.

    Disabled execution has no native seeding tick. For observation only, an empty
    owner may preview from its first observed cut without persisting that seed.
    Any native history/hold or pending entry forbids this substitution; enabling
    execution still requires the real runtime to initialize its own checkpoint.
    """
    c = runtime.controller.config
    if entry_hold is not None:
        return None, entry_hold
    if runtime.fault:
        return None, 'symbol_fault'
    if c.fingerprint() != runtime.fingerprint:
        raise PolicyError('implementation/configuration changed')
    if c.universe.mode != 'fixed':
        raise PolicyError('Meridian entry preview requires its fixed symbol universe')
    previous_reason = runtime.reason
    try:
        # Fixed-universe admission reads topology/selection only. Its diagnostic
        # must not overwrite the eventual symbol decision's reason.
        if not ModularRuntime.acquisition_permitted(runtime):
            return None, runtime.reason
    finally:
        runtime.reason = previous_reason
    state, _ = runtime.ensure_store().load()
    held = HeldBag.from_positions(runtime.controller.positions_held, connector=c.connector_name,
                                  pair=c.trading_pair)
    inventory = runtime.inventory()
    observational = not c.execution_enabled and state is None
    if observational:
        if inventory or held.amount > 0 or runtime.pending:
            return None, 'observation_checkpoint_required'
        state = DecisionState(fingerprint=runtime.fingerprint, candle=first_observed_cut)
    _, proposal, reason = plan(
        state, inventory, fingerprint=runtime.fingerprint, budget=c.total_amount_quote,
        now=now, market=market, signal=c.signal, acquisition=c.acquisition, risk=c.risk,
        allow_acquisition=True, bag=held, capital=c.capital, buy_gap=c.buy_gap,
        depth_budget=c.depth_budget, **runtime.snapshot())
    if proposal is None:
        return None, reason
    config = (runtime.bindings.config(c, proposal, market=market) if c.adaptive_trailing is not None
              else runtime.bindings.config(c, proposal))
    if not runtime.bindings.fees_permit(runtime.controller, config):
        return None, 'fee_reserve_exceeded'
    return config, 'observational_entry_proposed' if observational else reason
