# RSI Modular native profiles

New native recipe exports use one public controller entrypoint,
`controllers.generic.rsi_modular`, named `rsi_modular` with display name RSI Modular.
The required `profile` is either `ok_rsi` or `rsi_v5`; missing, unknown and blended
profiles reject. Profile-specific native fields/defaults/validators remain exact.
There is no merged strategy and no second delegated controller lifecycle.

```yaml
controller_type: generic
controller_name: rsi_modular
profile: ok_rsi
id: modular_spot_v2_candidate
execution_enabled: false
```

This partial illustration is not an activated or fully qualified configuration.
CLI creation prompts for the profile first and generates a fresh owner ID using
the existing `modular_spot_` namespace. Public naming does not rename database keys.

## Loader and identity contract

- `rsi_modular.CONFIG_PROFILES` is an immutable mapping to genuine private native
  config types. `get_config_class(profile)` returns the selected type; there is
  deliberately no ambiguous `CONFIG_CLASS` for the public module.
- `hummingbot.strategy_v2.controllers.controller_loader` exposes
  `resolve_controller_config_class(module, config_data=None)` and
  `load_controller_config(config_data, controllers_module='controllers', module=None)`.
  Runtime YAML, CLI creation and backtest loaders use this same resolution contract.
  Older controllers retain explicit `CONFIG_CLASS` or existing discovery behavior.
- Each profile config explicitly returns its genuine native-backed controller class.
  One selected profile creates one controller instance and action queue, sharing
  NativeComposition, candle transport, funding, recovery and native executors.
- Backtesting keys class selection by genuine config type, profile and resolved
  controller class, preventing collisions when both profiles run in one process.
  This fixes dispatch; it does not admit generic simulated economics as native truth.
- Accounting/runner policy predicates accept only genuine exact types and the
  matching immutable profile value. RSI V5 does not inherit OK RSI wallet-sale
  exceptions. Paper sessions retain their dedicated simulated connector boundary.

Profile/type changes require restart and explicit state reconciliation. Both new
profile implementations prohibit implementation hot reload. Native funding and
held recovery bind complete normalized configuration (including profile), owner,
source and native history. Existing old-state keys/receipts are never relabeled.
`modular_ok_rsi` and `modular_rsi_v5` remain compatibility entrypoints.

## Source, recipe and parity

`rsi_modular.source_root()` and `source_files(profile)` declare the real controller
source closure, including the public module and its imported dependencies. Shared
entrypoint imports can legitimately include both policy implementations; the
manifest must not omit imported code to appear profile-specific. The engine
package/image separately binds native executor/connector/loader dependencies.

New native recipe compilation emits `controller_name=rsi_modular` and the selected
profile. The profile is reserved against arbitrary recipe-parameter overrides.
Old exported YAML remains loadable through its original module/type. Existing
source/config receipts remain tied to their old bytes and do not qualify new ones.

Per-profile comparisons preserve native signal calculations, sizing, prices,
retry/single-attempt behavior and exits. The previously documented funded-only
`require_terminal_entry_reconciliation` flag remains the sole executor-factory
operational divergence. Public configuration/report identity differs explicitly.
Each public profile has native factory/action, source binding, loader/cache,
negative identity and actual paper partial-fill/cancel/restart coverage.

No live account request, order, activation, service restart, inventory adoption or
legacy-stack configuration change is needed to expose this entrypoint. Candidate
package/image, consumer and full-window research qualification are separate steps.
Rollback before activation is the candidate source/image revert. Preserve any
candidate database and reconcile its native state before a later runtime rollback;
never erase funding/recovery receipts to bypass changed source or profile identity.
