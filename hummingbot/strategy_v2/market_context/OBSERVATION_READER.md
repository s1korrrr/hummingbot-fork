# Native OKX observation reader

`ModularSpotObservationsV1Controller` is disabled unless its exact candidate
config sets `observation_enabled=true`. An enabled config requires explicit
`observation_source_id`, `observation_source_contract_hash`, regional profile,
reader capacities, and every joint-entry policy threshold. No source, socket,
credential, or order is created by the reader. The native modular controller
still owns the signal, reservation, executor, inventory, and recovery.

The owner constructs one `ObservationReader` for one `okx:spot:PAIR` and attaches
it with `controller.attach_observation_reader(reader)`. The reader must match the
candidate's source ID/hash, profile, pair, and capacities exactly. Before any
entry can be admitted, an independent source reviewer must provide a
`SourceQualification` and a verification callback that validates its immutable
review receipt, `review_available_at_ms`, and source contract. The callback's result must be the boolean
`True`. A subscription ACK, health snapshot, and producer `quality` label do
not replace this review. The reviewer also supplies a `JointCoverageReceipt`
and an independent verifier. Its trade and quote epochs are separate; its
selection epoch binds the two channels for this candidate.

Feed the authenticated, pair-scoped Rust update stream into
`controller.ingest_observation_update(update)` in received order. The stream's
`sequence` is global and may have holes after pair filtering. Pending updates
have no durable cursor and never enter a feature window. Durable ACKs for both
channels must precede the claimed coverage start; durable record cursors must
advance within each channel. Durable source updates need producer
`accepted_at_ms`, `durable_acknowledged_at_ms`, and `published_at_ms` in causal
order. The controller records its own provider clock at receipt; it does not
trust a receipt time from the wire. `attach_verified_coverage` also requires
`received_at_ms` from the independent coverage consumer. Feature construction
uses each record only after its source availability, publication, and consumer
receipt, and only after the source review and coverage receipt were available.
A lag reset, disconnect, gap, channel epoch
change, malformed update, or bounded-buffer overrun holds new entries. Reopen
the stream with a new reader and reverify both receipts after a reset, source
status loss, or metadata drift. The
live broadcast cannot replay a cursor. The reader never qualifies itself from
the stream or starts a capture.

At native pre-reservation admission, the reader selects three causal BBO
anchors and individual trades. A release requires a qualified gap-free window
with exact configured thresholds for selling imbalance, adverse midpoint
response, and lack of stabilization. Missing or stale evidence holds. A
source change between queued creation and actual dispatch holds that create;
native claim, reserved funds, DCA, exits, and reconciliation remain native.
The programmatic `publish_entry_observation` method requires an attached reader
and checks the complete rebuilt observation against that reader's independently
verified source and coverage. It cannot publish a caller-authored receipt by
itself. Tests can use synthetic receipts through an explicit test verifier;
those tests do not qualify a real source.

For isolated owner replay use `ModularSpotObservationsV1PaperConfig` with an
exact `NativePaperTradeExchange` and `NativePaperSession`. Attach the reader to
`session.controllers[config.id]`, feed updates available before the step, then
call the ordinary native `session.step` with separately sourced execution book,
trade, and candle inputs. This exercises the candidate's native admission and
executor boundary without converting one-level BBO into execution depth or
assuming passive fills. The exact paper config permits an explicit
`received_at_ms` on `controller.ingest_observation_update` while the replay
feeds an upcoming step. Supply this from the trusted replay driver, and call
`reader.attach_verified_coverage(..., received_at_ms=step_ms)` only after that
independent receipt is available. Ordinary controller configs use the native
provider clock and reject explicit receipt-time overrides. Actual cached source coverage, the economic adapter,
and fill assumptions need separate qualification before any backtest claim.

After a reset, attach a new matching reader through `controller.attach_observation_reader(new_reader)`. Replacement is allowed only after the prior reader has revoked its source qualification and coverage; an active reader cannot be silently replaced. Replacement clears cached entry evidence and transient eligible attempts while preserving durable opportunity state, queued native claims, reservations and management. The new reader starts its own sequence fence and needs fresh independent receipts. Consumer receipt clocks must be nondecreasing in stream order; a regressed clock or BBO event/sequence invalidates qualification rather than replacing a newer quote with a stale image.
