# Native strategy components

This package contains reusable native-engine components. Importing the package
starts no services and imports no controller family. Individual components keep
their existing explicit state, input validation and I/O contracts.

| Modules | Responsibility |
| --- | --- |
| `candle_data`, `candle_transport`, `candle_stream`, `data_reader` | Validated datasets and snapshots, bounded readers, explicit authenticated transport and shared subscription ownership |
| `candle_aggregation` | Complete consecutive UTC candle aggregation, including existing integer and Arrow precision behavior |
| `feature_cache`, `feature_registry` | Bounded revision-sensitive caching and source-bound feature dependencies |
| `decision_protocol` | Immutable proposal, veto and composition records |
| `capital_policy`, `portfolio`, `program_budget` | Read-only exposure assessment, bounded sizing and existing recorder-scoped reservations |
| `exit_policy`, `trailing`, `acquisition`, `risk` | Pure existing exit, floor, schedule and loss-limit calculations |
| `universe`, `screener`, `selection_state` | Explicit symbol selection, eligibility and persisted selection state |
| `config`, `domain`, `recipe_contract`, `strict_json` | Strict neutral models, values/errors, binding identities and unambiguous JSON |

These components do not create a second order manager. Native connectors,
executors and recorder remain execution/accounting authorities. Recorder-scoped
budget reservations are not shared-account spending authority. Capital assessments
remain proposals requiring native admission.

RSI signals, strategy recipes, native funding/recovery adapters, controller
lifecycles and planner remain outside this package. `config` intentionally has no
`SignalPolicy`; `recipe_contract` intentionally has no `StrategyRecipe`. Native
RSI composition consumes canonical candle components through its existing adapter.
A different controller can import the same library without importing RSI policies.

The old `controllers.generic.modular_spot_core` paths remain compatibility imports.
Fully extracted modules alias the canonical module object, retaining shared caches,
class identity and monkeypatch targets. Partial config/recipe exports reference the
same canonical objects while their strategy-specific definitions stay in adapters.
`controllers.directional_trading.ok_rsi_core.candles` similarly aliases canonical
aggregation. Both old and new candle-service module entrypoints preserve `--help`
and explicit startup behavior; importing either does not start it.

Extraction preserves existing algorithms, defaults and persistence keys. Consumers
must bind the component source closure and native package identity as well as their
controller source. A passing component suite or source-bound recipe does not grant
paper/live activation or establish economic validity.
