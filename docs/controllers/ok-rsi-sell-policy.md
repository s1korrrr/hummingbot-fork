# OK RSI sell sizing and owned cost protection

The normal RSI sale and held-position recovery sale use the same configured
pair target. In `buy_sell` mode this is `usd_per_entry` multiplied by
`signal_sell_inventory_slice_multiplier`, divided by the maker reference price.
The Main profile's 25 × 2 therefore targets 50 quote currency per sale.
In `sell_only` mode the target is `sell_only_inventory_fraction` of sellable
inventory. The SUI profile uses 10% with `sell_only_strict_fraction: true`: each
sale stays at or below that fraction, including recovery proposals. Quantization
must not raise the quantity, and a fraction below venue minimums is skipped.
The fraction is rechecked against available wallet inventory at executor admission.
Strict fraction mode ignores the full-remainder shortcut and requires restart to
change. Other profiles retain their existing optional remainder and minimum rules.

Recovery still requires its existing arming and reversal conditions. It proposes
at most one sale per pair in an evaluation, including when several held positions
qualify. That sale is limited to the selected owned position and available balance.
This does not change trailing exits owned by active position executors, which
continue to manage their own purchased quantity. The legacy HL RSI recovery
fraction and multiple-position behavior remain unchanged.

Main uses a quote target, with its existing exchange minimum-notional uplift and
dust behavior. SUI has a strict per-sale fraction; neither setting is a daily
selling budget. Repeated qualifying signals may sell more inventory later.

With `sell_only_if_profitable: true`, a normal sell requires a complete, finite
owned acquisition basis and historical fee record. Its quantity cannot exceed
inventory covered by that basis. Missing basis or fees prevents construction.
The order carries a minimum sell price covering aggregate acquisition cost,
configured minimum profit and remaining historical fees. The native OrderExecutor
adds estimated exit fees and rounds the floor upwards to the venue price tick,
including when its reference market price falls before placement.

The prepared SUI profile follows the owner's wallet-sale policy without a known
acquisition basis or profitability gate. Its kill switch remains enabled; the
owner must perform final live activation. The full-remainder threshold is zero
and buying remains forbidden. These SUI sales may realize a loss; Main's
profitable-only protection is unchanged.

This policy does not import inventory, establish account-wide reconciliation,
or change episode strategy execution.
