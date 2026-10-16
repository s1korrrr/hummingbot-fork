"""Isolated, disabled-by-default observation candidate for modular spot.

No observation service is started here. An owner may present a qualified entry
snapshot only for a proposal that native planning has already produced.
"""

from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Literal

from pydantic import Field, StrictBool, model_validator

from controllers.generic import modular_spot as base
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.recipe_contract import digest, verify_binding
from hummingbot.strategy_v2.components.source_identity import checked_source
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.market_context.observation_budget import (
    NativeEntryGate,
    ObservationBudgetProxy,
    ObservationGateStore,
)
from hummingbot.strategy_v2.market_context.observation_entry import EntryObservation
from hummingbot.strategy_v2.market_context.observation_entry_builder import (
    BuiltEntryObservation,
    EntryFeaturePolicy,
    build_entry_observation,
)
from hummingbot.strategy_v2.market_context.observation_reader import ObservationReader
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

CONFIG_CLASS = "ModularSpotObservationsV1Config"
_HERE = Path(__file__)
_OBSERVATION_SOURCES = (
    _HERE,
    _HERE.parents[2] / "hummingbot/strategy_v2/market_context/observation_adapter.py",
    _HERE.parents[2] / "hummingbot/strategy_v2/market_context/observation_budget.py",
    _HERE.parents[2] / "hummingbot/strategy_v2/market_context/observation_entry.py",
    _HERE.parents[2] / "hummingbot/strategy_v2/market_context/observation_entry_builder.py",
    _HERE.parents[2] / "hummingbot/strategy_v2/market_context/observation_reader.py",
    _HERE.parents[2] / "hummingbot/strategy_v2/market_context/market_picture/top_of_book.py",
)


def source_root():
    """Exact native owner root for sealed paper-session replay receipts."""
    return _HERE.parents[2]


def source_files():
    root = source_root()
    return sorted(
        set(
            (
                *base.ModularSpotController.implementation_source_files(),
                *_OBSERVATION_SOURCES,
                root / "hummingbot/strategy_v2/paper_session.py",
                root / "hummingbot/connector/exchange/paper_trade/native_paper_trade_exchange.py",
            )
        )
    )


class ModularSpotObservationsV1Config(base.ModularSpotControllerConfig):
    controller_name: Literal["modular_spot_observations_v1"] = "modular_spot_observations_v1"
    id: str = Field(pattern=r"^modular_spot_observations_[A-Za-z0-9_-]+$")
    observation_enabled: StrictBool = False
    observation_source_profile: Literal["okx_eea_public_v1", "okx_global_public_v1"] = "okx_eea_public_v1"
    observation_source_id: str | None = None
    observation_source_contract_hash: str | None = None
    observation_max_trades: int | None = Field(default=None, ge=1, le=100_000, strict=True)
    observation_max_quotes: int | None = Field(default=None, ge=3, le=100_000, strict=True)
    observation_max_wait_ms: int = Field(default=60_000, ge=1, le=1_200_000, strict=True)
    observation_window_ms: int | None = Field(default=None, ge=1, le=300_000, strict=True)
    observation_stabilization_window_ms: int | None = Field(default=None, ge=1, le=300_000, strict=True)
    observation_max_quote_age_ms: int | None = Field(default=None, ge=1, le=60_000, strict=True)
    observation_max_feature_age_ms: int | None = Field(default=None, ge=1, le=60_000, strict=True)
    observation_max_spread_bps: Decimal | None = None
    observation_minimum_buy_imbalance: Decimal | None = None
    observation_minimum_adverse_drop_bps: Decimal | None = None
    observation_maximum_late_decline_bps: Decimal | None = None

    @model_validator(mode="after")
    def validate_composition(self):
        # The inherited validator seals only the base owner. Reuse its semantic
        # checks, then bind this candidate's wider source closure explicitly.
        unchecked = self.model_copy(update={"recipe_binding": None})
        base.ModularSpotControllerConfig.validate_composition(unchecked)
        if self.recipe_binding is not None:
            verify_binding(
                self.recipe_binding, self.model_dump(mode="json"), source_sha256=digest(self.source_manifest())
            )
        if self.observation_enabled and self.budget is None:
            raise ValueError("observation gate requires the native budget reservation boundary")
        paper = getattr(self, "execution_mode", None) == "paper"
        expected_connector = "okx_paper_trade" if paper else "okx"
        if self.observation_enabled and (
            self.connector_name != expected_connector or not self.trading_pair.endswith("-USDC")
        ):
            raise ValueError("observation gate requires exact OKX spot and a USDC instrument")
        if paper and (
            self.data.mode != "native"
            or self.universe.mode != "fixed"
            or getattr(self, "candles_connector", None) != "okx"
        ):
            raise ValueError("observation paper replay requires injected native OKX candles")
        if self.observation_enabled:
            if "observation_source_profile" not in self.model_fields_set:
                raise ValueError("enabled observations require an explicit regional source profile")
            if (
                not isinstance(self.observation_source_id, str)
                or not isinstance(self.observation_source_contract_hash, str)
                or self.observation_max_trades is None
                or self.observation_max_quotes is None
            ):
                raise ValueError("enabled observations require explicit source identity and bounded reader capacity")
            ObservationReader(
                instrument_id=f"okx:spot:{self.trading_pair}",
                source_id=self.observation_source_id,
                source_contract_hash=self.observation_source_contract_hash,
                source_profile=self.observation_source_profile,
                max_trades=self.observation_max_trades,
                max_quotes=self.observation_max_quotes,
            )
            self.feature_policy()
        return self

    def feature_policy(self) -> EntryFeaturePolicy:
        return EntryFeaturePolicy(
            window_ms=self.observation_window_ms,
            stabilization_window_ms=self.observation_stabilization_window_ms,
            max_quote_age_ms=self.observation_max_quote_age_ms,
            max_feature_age_ms=self.observation_max_feature_age_ms,
            max_spread_bps=self.observation_max_spread_bps,
            minimum_buy_imbalance=self.observation_minimum_buy_imbalance,
            minimum_adverse_drop_bps=self.observation_minimum_adverse_drop_bps,
            maximum_late_decline_bps=self.observation_maximum_late_decline_bps,
        )

    def source_manifest(self):
        records = base.ModularSpotControllerConfig.source_manifest(self)
        records.extend(
            (
                "observations_v1/" + path.relative_to(_HERE.parents[2]).as_posix(),
                sha256(checked_source(path)).hexdigest(),
            )
            for path in _OBSERVATION_SOURCES
        )
        return sorted(records)


class ModularSpotObservationsV1PaperConfig(ModularSpotObservationsV1Config):
    """Explicit isolated paper identity; no serialized callback or source permit."""

    research_only: Literal[True] = True
    execution_mode: Literal["paper"] = "paper"
    connector_name: Literal["okx_paper_trade"] = "okx_paper_trade"
    candles_connector: Literal["okx"] = "okx"
    observation_enabled: Literal[True] = True


class ObservationPaperBindings(NativeBindings):
    @staticmethod
    def spot_connector(controller):
        connector = (getattr(controller.market_data_provider, "connectors", None) or {}).get(
            controller.config.connector_name
        )
        if not is_native_paper_connector(connector):
            raise ValueError("observation replay requires the exact isolated native paper connector")
        return connector


class ObservationRuntime(base.ModularRuntime):
    def __init__(self, controller, bindings=None):
        super().__init__(controller, bindings=bindings)
        c = controller.config
        self.observation_gate = NativeEntryGate(
            max_wait_ms=c.observation_max_wait_ms, source_profile=c.observation_source_profile
        )
        self.entry_observations: dict[str, EntryObservation] = {}
        self._reservation_context: tuple[str, int | None, int] | None = None
        self.current_source_epoch: str | None = None
        self.observation_reader: ObservationReader | None = None

    def attach_observation_reader(self, reader: ObservationReader) -> None:
        c = self.controller.config
        if (
            not c.observation_enabled
            or not isinstance(reader, ObservationReader)
            or (
                reader.instrument_id,
                reader.source_id,
                reader.source_contract_hash,
                reader.source_profile,
                reader.max_trades,
                reader.max_quotes,
            )
            != (
                f"okx:spot:{c.trading_pair}",
                c.observation_source_id,
                c.observation_source_contract_hash,
                c.observation_source_profile,
                c.observation_max_trades,
                c.observation_max_quotes,
            )
        ):
            raise ValueError("reader must match the enabled candidate source contract")
        previous = self.observation_reader
        if previous is not None and (
            reader is previous or previous.qualification is not None or previous.coverage is not None
        ):
            raise ValueError("active observation reader cannot be replaced; revoke its source first")
        self.observation_reader = reader
        self.observation_gate._eligible_attempts.clear()
        self.entry_observations.clear()
        self.current_source_epoch = None

    def ingest_observation_update(self, message, *, received_at_ms: int | None = None) -> None:
        if self.observation_reader is None:
            raise ValueError("no qualified observation reader is attached")
        if received_at_ms is not None and type(self.controller.config) is not ModularSpotObservationsV1PaperConfig:
            raise ValueError("explicit receipt time is reserved for isolated paper replay")
        if received_at_ms is None:
            received_at_ms = int(self.controller.market_data_provider.time() * 1000)
        try:
            self.observation_reader.ingest(message, received_at_ms=received_at_ms)
        except (TypeError, ValueError):
            self.observation_reader.invalidate("invalid_observation_update")
            self.current_source_epoch = None
            raise
        permit = self.observation_reader.qualification
        self.current_source_epoch = (
            permit.selection_epoch if permit is not None and self.observation_reader.coverage is not None else None
        )

    @staticmethod
    def entry_stage(config) -> int | None:
        level = getattr(config, "level_id", None)
        if not isinstance(level, str):
            return None
        parts = level.split(":")
        if len(parts) != 4 or parts[:2] != ["modular_spot", "v1"]:
            return None
        if not all(part.isascii() and part.isdigit() for part in parts[2:]):
            return None
        return int(parts[3])

    def reservation(self, config):
        reservation = super().reservation(config)
        stage = self.entry_stage(config)
        candle_ms = -1 if self.market is None else int(Decimal(str(self.market.candle_open)) * 1000)
        self._reservation_context = (config.id, stage, candle_ms)
        return reservation

    def publish_entry_observation(self, built: BuiltEntryObservation) -> None:
        if self.observation_reader is None:
            raise ValueError("independently verified observation reader is required")
        if not isinstance(built, BuiltEntryObservation) or built.policy != self.controller.config.feature_policy():
            raise ValueError("entry observation must be built from the configured source feature policy")
        rebuilt = build_entry_observation(
            opportunity_id=built.observation.opportunity_id,
            as_of_ms=built.as_of_ms,
            trades=built.trades,
            quotes=built.quotes,
            coverage=built.coverage,
            policy=built.policy,
        )
        if rebuilt != built:
            raise ValueError("entry observation differs from its parsed source evidence")
        observation = built.observation
        if observation.source_profile != self.controller.config.observation_source_profile:
            raise ValueError("observation profile differs from controller configuration")
        state = self.observation_gate.state.get(observation.opportunity_id)
        if state is None or state.phase not in {"WAIT", "QUEUED"}:
            raise ValueError("native proposal must first exist at the admission boundary")
        if observation.instrument_id != f"okx:spot:{state.trading_pair}":
            raise ValueError("observation instrument differs from native proposal")
        self.observation_reader.check_active(observation, now_ms=built.as_of_ms)
        if built != self.observation_reader.build(
            opportunity_id=observation.opportunity_id, as_of_ms=built.as_of_ms, policy=built.policy
        ):
            raise ValueError("entry observation differs from verified reader evidence")
        if (
            observation.opportunity_id not in self.entry_observations
            and len(self.entry_observations) >= self.observation_gate.max_states
        ):
            raise ValueError("bounded entry observation capacity exceeded")
        self.entry_observations[observation.opportunity_id] = observation
        self.current_source_epoch = observation.source_epoch

    def publish_source_epoch(self, source_epoch: str) -> None:
        if self.observation_reader is not None:
            raise ValueError("attached reader owns the source selection epoch")
        if not isinstance(source_epoch, str) or not 1 <= len(source_epoch) <= 128:
            raise ValueError("bounded source epoch required")
        self.current_source_epoch = source_epoch

    def ensure_store(self):
        store = super().ensure_store()
        if self.controller.config.observation_enabled:
            if self.observation_gate.checkpoint is None:
                self.observation_gate.checkpoint = ObservationGateStore(
                    store.recorder, store.key, metadata_model=self.bindings.metadata_model
                )
                self.observation_gate.refresh()
            if self.budget is not None and not isinstance(self.budget, ObservationBudgetProxy):
                self.budget = ObservationBudgetProxy(self.budget, self._admit_observation)
        return store

    def _admit_observation(self, reservation) -> bool:
        context = self._reservation_context
        if context is None or context[0] != reservation.executor_id:
            self.observation_gate.reason = "native_reservation_context_unavailable"
            return False
        if context[1] is not None and context[1] > 0:
            self.observation_gate.reason = "dca_uses_native_admission"
            return True
        now_ms = int(self.controller.market_data_provider.time() * 1000)
        observation = self.entry_observations.get(reservation.executor_id)
        reader_hold = None
        if self.observation_reader is not None:
            try:
                built = self.observation_reader.build(
                    opportunity_id=reservation.executor_id,
                    as_of_ms=now_ms,
                    policy=self.controller.config.feature_policy(),
                )
                observation = built.observation
                self.current_source_epoch = observation.source_epoch
            except ValueError as exc:
                reader_hold = f"observation_reader_hold:{str(exc)[:96]}"
                observation = None
        admitted = self.observation_gate.evaluate(
            reservation, observation, now_ms=now_ms, decision_candle_ms=context[2]
        )
        if reader_hold is not None:
            self.observation_gate.reason = reader_hold
        if admitted and self.observation_reader is not None:
            self.entry_observations[reservation.executor_id] = observation
        self.entry_observations = {
            key: value
            for key, value in self.entry_observations.items()
            if key in self.observation_gate.state and self.observation_gate.state[key].phase in {"WAIT", "QUEUED"}
        }
        return admitted

    def observation_dispatch_permitted(self, config) -> bool:
        observation = self.entry_observations.get(config.id)
        if self.observation_reader is not None:
            try:
                self.observation_reader.check_active(
                    observation, now_ms=int(self.controller.market_data_provider.time() * 1000)
                )
            except ValueError as exc:
                self.observation_gate.reason = f"observation_reader_hold:{str(exc)[:96]}"
                return False
        if observation is None or observation.source_epoch != self.current_source_epoch:
            self.observation_gate.reason = "source_epoch_changed_or_unavailable"
            return False
        now_ms = int(self.controller.market_data_provider.time() * 1000)
        return self.observation_gate.dispatch_permitted(config.id, observation, pair=config.trading_pair, now_ms=now_ms)

    def actions(self):
        actions = super().actions()
        if not self.controller.config.observation_enabled:
            return actions
        for action in actions:
            if isinstance(action, CreateExecutorAction):
                identifier = action.executor_config.id
                state = self.observation_gate.state.get(identifier)
                if identifier in self.observation_gate._eligible_attempts and state is not None:
                    self.observation_gate.mark_queued(identifier, now_ms=state.last_clock_ms)
        return actions


class ModularSpotObservationsV1Controller(base.ModularSpotController):
    @classmethod
    def implementation_source_files(cls):
        return [*base.ModularSpotController.implementation_source_files(), *_OBSERVATION_SOURCES]

    def __init__(self, config: ModularSpotObservationsV1Config, *args, **kwargs):
        if type(config) is ModularSpotObservationsV1PaperConfig:
            provider = args[0] if args else kwargs.get("market_data_provider")
            connector = (getattr(provider, "connectors", None) or {}).get(config.connector_name)
            if not is_native_paper_connector(connector):
                raise ValueError("paper candidate requires the isolated native paper connector")
        # Avoid constructing two runtimes or attaching the candle reader twice.
        ControllerBase.__init__(self, config, *args, **kwargs)
        self.runtime = ObservationRuntime(self, bindings=self._runtime_bindings())
        self._data_close_task = None
        self._data_close_error = None
        self._operator_entry_paused = False
        self._operator_state = {
            "entry_paused": False,
            "reason": None,
            "scope": None,
            "updated_at": None,
            "last_command_id": None,
        }

    def _runtime_bindings(self):
        return ObservationPaperBindings() if type(self.config) is ModularSpotObservationsV1PaperConfig else None

    def get_candles_config(self):
        if type(self.config) is ModularSpotObservationsV1PaperConfig:
            return [
                CandlesConfig(
                    connector="okx",
                    trading_pair=self.config.trading_pair,
                    interval=self.config.interval,
                    max_records=self.config.max_records,
                )
            ]
        return super().get_candles_config()

    def publish_entry_observation(self, built: BuiltEntryObservation) -> None:
        self.runtime.publish_entry_observation(built)

    def attach_observation_reader(self, reader: ObservationReader) -> None:
        self.runtime.attach_observation_reader(reader)

    def ingest_observation_update(self, message, *, received_at_ms: int | None = None) -> None:
        self.runtime.ingest_observation_update(message, received_at_ms=received_at_ms)

    def publish_source_epoch(self, source_epoch: str) -> None:
        self.runtime.publish_source_epoch(source_epoch)

    def validate_executor_config(self, config):
        if (
            self.config.observation_enabled
            and self.runtime.entry_stage(config) == 0
            and not self.runtime.observation_dispatch_permitted(config)
        ):
            return False
        return super().validate_executor_config(config)

    def get_executor_class_for_config(self, config):
        stage = self.runtime.entry_stage(config)
        if self.config.observation_enabled and stage == 0:
            if not self.runtime.observation_dispatch_permitted(config):
                raise RuntimeError("observation entry evidence expired before native dispatch")
        executor_class = super().get_executor_class_for_config(config)
        if self.config.observation_enabled and stage == 0:
            now_ms = int(self.market_data_provider.time() * 1000)
            if not self.runtime.observation_gate.mark_dispatched(config.id, now_ms=now_ms):
                raise RuntimeError("observation dispatch checkpoint could not be committed")
            self.runtime.entry_observations.pop(config.id, None)
        return executor_class

    def get_custom_info(self) -> dict:
        info = super().get_custom_info()
        admission_error = (
            self.runtime.budget.last_error if isinstance(self.runtime.budget, ObservationBudgetProxy) else None
        )
        info["observation_entry"] = {
            "enabled": self.config.observation_enabled,
            "status": ("observation_admission_error" if admission_error else self.runtime.observation_gate.reason)
            if self.config.observation_enabled
            else "disabled",
            "admission_error": admission_error,
            "waiting": sum(state.phase == "WAIT" for state in self.runtime.observation_gate.state.values()),
        }
        return info
