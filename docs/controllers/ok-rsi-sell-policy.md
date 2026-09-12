# OK RSI sell sizing and owned cost protection

The normal RSI sale and held-position recovery sale use the same configured
pair target. In `buy_sell` mode this is `usd_per_entry` multiplied by
`signal_sell_inventory_slice_multiplier`, divided by the maker reference price.
The Main profile's 25 × 2 therefore targets 50 quote currency per sale.
In `sell_only` mode the target is `sell_only_inventory_fraction` of sellable
inventory: 10% for SUI. Below `sell_all_below_quote` (20 quote for SUI), the
existing policy targets the entire sellable remainder.

Recovery still requires its existing arming and reversal conditions. It proposes
at most one sale per pair in an evaluation, including when several held positions
qualify. That sale is limited to the selected owned position and available balance.
This does not change trailing exits owned by active position executors, which
continue to manage their own purchased quantity. The legacy HL RSI recovery
fraction and multiple-position behavior remain unchanged.

These are targets, not hard quote caps or daily budgets. Exchange minimum-notional
uplift, quantization and avoiding an untradeable dust remainder retain their existing
behavior. Repeated qualifying signals may sell more inventory later.

With `sell_only_if_profitable: true`, a normal sell requires a complete, finite
owned acquisition basis and historical fee record. Its quantity cannot exceed
inventory covered by that basis. Missing basis or fees prevents construction.
The order carries a minimum sell price covering aggregate acquisition cost,
configured minimum profit and remaining historical fees. The native OrderExecutor
adds estimated exit fees and rounds the floor upwards to the venue price tick,
including when its reference market price falls before placement.

The committed SUI profile retains its kill switch and enables profitable-only
selling. It must remain disabled until actual acquisition basis and fees are
verified and activation is authorized. No wallet balance or current price is
an acquisition-cost substitute.

This policy does not import inventory, establish account-wide reconciliation,
authorize activation, or change episode strategy execution.
