"""Genuine native candidate admission seams; no paper service or order submission."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from test.modular_spot.observation_fixture import fixture_stream, paper_config
from types import SimpleNamespace as NS
from unittest.mock import patch

import pytest
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from controllers.generic import modular_spot as base, modular_spot_observations_v1 as observation_owner
from controllers.generic.modular_spot_core.execution import native_config
from controllers.generic.modular_spot_observations_v1 import (
    ModularSpotObservationsV1Config,
    ModularSpotObservationsV1Controller,
    ModularSpotObservationsV1PaperConfig,
)
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, configuration_digest, digest, source_digest
from hummingbot.strategy_v2.market_context.market_picture.top_of_book import TopOfBookObservation
from hummingbot.strategy_v2.market_context.observation_adapter import parse_rust_trade
from hummingbot.strategy_v2.market_context.observation_budget import (
    NativeEntryGate,
    ObservationBudgetProxy,
    ObservationGateStore,
)
from hummingbot.strategy_v2.market_context.observation_entry_builder import (
    DurableCursor,
    EntryFeaturePolicy,
    JointCoverageReceipt,
    build_entry_observation,
)
from hummingbot.strategy_v2.market_context.observation_reader import ObservationReader, SourceQualification
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

ID = "a" * 64
SOURCE_ID = "local_test_source"
SOURCE_HASH = "a" * 64


def config(**changes):
    fields = dict(
        id="modular_spot_observations_test",
        connector_name="okx",
        trading_pair="BTC-USDC",
        budget={"group": "observations", "max_quote": "100", "max_assets": 1},
        observation_enabled=True,
        observation_source_profile="okx_eea_public_v1",
        observation_source_id=SOURCE_ID,
        observation_source_contract_hash=SOURCE_HASH,
        observation_max_trades=100,
        observation_max_quotes=100,
        observation_max_wait_ms=200,
        observation_window_ms=100,
        observation_stabilization_window_ms=50,
        observation_max_quote_age_ms=100,
        observation_max_feature_age_ms=100,
        observation_max_spread_bps=D("30"),
        observation_minimum_buy_imbalance=D("0"),
        observation_minimum_adverse_drop_bps=D("10"),
        observation_maximum_late_decline_bps=D("0"),
    )
    fields.update(changes)
    return ModularSpotObservationsV1Config(**fields)


def built(*, epoch="e1", quote_epoch="q1", selection="s1", quality="qualified", profile="okx_eea_public_v1"):
    region = "eea" if profile == "okx_eea_public_v1" else "global"
    trade = parse_rust_trade(
        dict(
            schema_version="okx.observations.v1",
            venue="okx",
            environment="public",
            region=region,
            instrument_id="okx:spot:BTC-USDC",
            channel="trades-all",
            trade_id="123",
            price="10",
            base_quantity="4",
            quote_notional="40",
            side="buy",
            aggregate_count=1,
            event_at_ms=1150,
            available_at_ms=1160,
            metadata_revision="meta1",
            source_epoch=epoch,
            quality=quality,
            connection_id="c1",
            receive_sequence=1,
            raw_sha256="b" * 64,
            reason_codes=[] if quality == "qualified" else ["MISSING_CAPTURE_LINEAGE"],
        ),
        source_profile=profile,
    )

    def quote(event, sequence):
        return TopOfBookObservation(
            instrument_id="okx:spot:BTC-USDC",
            region=region,
            environment="public",
            source_profile=profile,
            metadata_revision="meta1",
            source_epoch=quote_epoch,
            selection_epoch=selection,
            event_at_ms=event,
            available_at_ms=event,
            sequence=sequence,
            bid_price=D("9.99"),
            bid_quantity=D("2"),
            ask_price=D("10.01"),
            ask_quantity=D("3"),
            quality="QUALIFIED",
            reason_codes=(),
        )

    quotes = (quote(1100, 1), quote(1150, 2), quote(1190, 3))
    coverage = JointCoverageReceipt(
        instrument_id="okx:spot:BTC-USDC",
        source_id=SOURCE_ID,
        source_contract_hash=SOURCE_HASH,
        source_profile=profile,
        metadata_revision="meta1",
        selection_epoch=selection,
        trade_source_epoch=epoch,
        quote_source_epoch=quote_epoch,
        trade_coverage_start_ms=1100,
        trade_coverage_end_ms=1200,
        quote_coverage_start_ms=1100,
        quote_coverage_end_ms=1200,
        available_at_ms=1200,
        receipt_sha256="c" * 64,
        quality="qualified",
        side_verified=True,
        trade_cursor=DurableCursor(1, 6, "d" * 64),
        quote_cursor=DurableCursor(1, 7, "e" * 64),
    )
    policy = EntryFeaturePolicy(
        window_ms=100,
        stabilization_window_ms=50,
        max_quote_age_ms=100,
        max_feature_age_ms=100,
        max_spread_bps=D("30"),
        minimum_buy_imbalance=D("0"),
        minimum_adverse_drop_bps=D("10"),
        maximum_late_decline_bps=D("0"),
    )
    return build_entry_observation(
        opportunity_id=ID, as_of_ms=1200, trades=(trade,), quotes=quotes, coverage=coverage, policy=policy
    )


class MemoryCheckpoint:
    def __init__(self):
        self.states, self.clock, self.revision = {}, 0, None

    def load(self):
        return deepcopy(self.states), self.clock, self.revision

    def compare_and_swap(self, expected, states, max_clock_ms):
        if expected != self.revision:
            return False
        self.states, self.clock = deepcopy(states), max_clock_ms
        self.revision = 1 if self.revision is None else self.revision + 1
        return True


def candidate(*, start=1.2):
    clock = [start]
    value = config()
    controller = ModularSpotObservationsV1Controller(value, NS(ready=False, time=lambda: clock[0]), asyncio.Queue())
    controller.runtime.observation_gate.checkpoint = MemoryCheckpoint()
    native = native_config(value, identifier=ID, timestamp=start, price=D("10"), amount=D("1"), cycle=0, stage=0)
    reservation = NS(executor_id=ID, trading_pair="BTC-USDC", expires_at=D("2"))
    gate = controller.runtime.observation_gate
    assert not gate.evaluate(reservation, None, now_ms=1200, decision_candle_ms=1100)
    controller.attach_observation_reader(ready_reader())
    controller.runtime._reservation_context = (ID, 0, 1100)
    assert controller.runtime._admit_observation(reservation)
    with patch.object(
        base.ModularRuntime,
        "actions",
        return_value=[CreateExecutorAction(controller_id=value.id, executor_config=native)],
    ):
        assert controller.runtime.actions()[0].executor_config.id == ID
    assert gate.state[ID].phase == "QUEUED"
    return controller, native, clock


def test_config_scope_defaults_and_candidate_recipe_source_binding():
    disabled = ModularSpotObservationsV1Config(
        id="modular_spot_observations_test", connector_name="okx", trading_pair="BTC-USDC"
    )
    assert not disabled.observation_enabled
    assert disabled.get_controller_class() is ModularSpotObservationsV1Controller
    for changes in ({"connector_name": "binance"}, {"trading_pair": "BTC-USDT"}, {"observation_max_spread_bps": None}):
        with pytest.raises(ValueError):
            config(**changes)
    with pytest.raises(ValueError, match="explicit regional"):
        ModularSpotObservationsV1Config(
            id="modular_spot_observations_test",
            connector_name="okx",
            trading_pair="BTC-USDC",
            budget={"group": "observations", "max_quote": "100", "max_assets": 1},
            observation_enabled=True,
            observation_window_ms=100,
            observation_max_quote_age_ms=100,
            observation_max_feature_age_ms=100,
            observation_max_spread_bps=D("30"),
            observation_minimum_buy_imbalance=D("0"),
        )
    value = config()
    binding = RecipeBinding(
        recipe_id="observations",
        recipe_sha256="d" * 64,
        source_sha256=digest(value.source_manifest()),
        configuration_sha256=configuration_digest(value.model_dump(mode="json")),
    )
    sealed = ModularSpotObservationsV1Config.model_validate(
        {**value.model_dump(mode="json"), "recipe_binding": binding.model_dump(mode="json")}
    )
    assert sealed.recipe_binding == binding
    assert sealed.fingerprint() != value.fingerprint()
    with pytest.raises(ValueError, match="implementation source changed"):
        ModularSpotObservationsV1Config.model_validate(
            {
                **value.model_dump(mode="json"),
                "recipe_binding": {**binding.model_dump(mode="json"), "source_sha256": "e" * 64},
            }
        )


def test_builder_requires_qualified_parsed_rust_trade_and_matching_source():
    result = built()
    assert result.observation.qualified and not result.observation.adverse_flow
    assert result.flow.confirmation_imbalance == 1 and result.top.qualified
    controller, _, _ = candidate()
    forged = replace(result, observation=replace(result.observation, content_sha256="f" * 64))
    with pytest.raises(ValueError, match="differs"):
        controller.publish_entry_observation(forged)
    with pytest.raises(ValueError, match="unqualified"):
        built(quality="unqualified")
    with pytest.raises(ValueError, match="coverage is unqualified"):
        build_entry_observation(
            opportunity_id=ID,
            as_of_ms=1200,
            trades=result.trades,
            quotes=result.quotes,
            coverage=replace(result.coverage, quality="unqualified"),
            policy=result.policy,
        )
    with pytest.raises(ValueError, match="lineage"):
        build_entry_observation(
            opportunity_id=ID,
            as_of_ms=1200,
            trades=result.trades,
            quotes=(replace(result.quotes[0], source_epoch="different"), *result.quotes[1:]),
            coverage=result.coverage,
            policy=result.policy,
        )


def test_delayed_native_queue_and_epoch_change_hold_without_releasing_claim():
    controller, native, clock = candidate()
    with patch.object(base.ModularSpotController, "validate_executor_config", return_value=True):
        assert controller.validate_executor_config(native)
        clock[0] = 1.30  # BBO evidence expired at 1.29 seconds.
        assert not controller.validate_executor_config(native)
    assert controller.runtime.observation_gate.state[ID].phase == "QUEUED"
    assert ID in controller.runtime.entry_observations
    with pytest.raises(RuntimeError, match="expired"):
        controller.get_executor_class_for_config(native)
    controller, native, clock = candidate()
    controller.ingest_observation_update(
        {"type": "source_status", "instrument_id": "okx:spot:BTC-USDC", "channel": "trades-all", "reason": "SOURCE_GAP"}
    )
    with patch.object(base.ModularSpotController, "validate_executor_config", return_value=True):
        assert not controller.validate_executor_config(native)
    assert controller.runtime.observation_gate.reason.startswith("observation_reader_hold:")
    with pytest.raises(ValueError, match="independently verified observation reader"):
        detached = ModularSpotObservationsV1Controller(config(), NS(ready=False, time=lambda: 1.2), asyncio.Queue())
        detached.publish_entry_observation(built(selection="s2"))


def test_eligible_dispatch_consumes_once_and_dca_uses_native_admission():
    controller, native, _ = candidate()
    with (
        patch.object(base.ModularSpotController, "validate_executor_config", return_value=True),
        patch.object(base.ModularSpotController, "get_executor_class_for_config", return_value=object),
    ):
        assert controller.validate_executor_config(native)
        assert controller.get_executor_class_for_config(native) is object
        assert not controller.validate_executor_config(native)
        dca = native_config(
            controller.config, identifier="f" * 64, timestamp=1.2, price=D("10"), amount=D("1"), cycle=0, stage=1
        )
        controller.runtime._reservation_context = (dca.id, 1, 1100)
        assert controller.runtime._admit_observation(NS(executor_id=dca.id, trading_pair="BTC-USDC", expires_at=D("2")))
        assert dca.id not in controller.runtime.observation_gate.state
        assert controller.validate_executor_config(dca)
        assert controller.get_executor_class_for_config(dca) is object
    assert controller.runtime.observation_gate.state[ID].phase == "DISPATCHED"


def test_disabled_candidate_preserves_native_action_result_and_budget_identity():
    value = ModularSpotObservationsV1Config(
        id="modular_spot_observations_test", connector_name="okx", trading_pair="BTC-USDC"
    )
    controller = ModularSpotObservationsV1Controller(value, NS(ready=False, time=lambda: 1.2), asyncio.Queue())
    native_action = object()
    with patch.object(base.ModularRuntime, "actions", return_value=[native_action]):
        assert controller.runtime.actions() == [native_action]
    assert controller.runtime.budget is None
    assert controller.get_custom_info()["observation_entry"]["status"] == "disabled"


def test_sqlite_checkpoint_survives_restart_and_prunes_only_after_native_expiry():
    model_base = declarative_base()

    class Metadata(model_base):
        __tablename__ = "observation_test_metadata"
        key = Column(String, primary_key=True)
        value = Column(String, nullable=False)

    engine = create_engine("sqlite+pysqlite:///:memory:")
    model_base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    recorder = NS(sql_manager=NS(get_new_session=sessions))
    store = ObservationGateStore(recorder, "native-owner", metadata_model=Metadata)
    first = NativeEntryGate(max_wait_ms=100, max_states=1, checkpoint=store)
    a = NS(executor_id=ID, trading_pair="BTC-USDC", expires_at=D("2"))
    b = NS(executor_id="b" * 64, trading_pair="BTC-USDC", expires_at=D("3"))
    assert not first.evaluate(a, None, now_ms=1000, decision_candle_ms=900)
    assert store.load()[0][ID].deadline_ms == 1100
    # Native planner recomputes the same proposal's config timestamp on each tick.
    refreshed_native_expiry = NS(executor_id=ID, trading_pair="BTC-USDC", expires_at=D("2.1"))
    assert not first.evaluate(refreshed_native_expiry, None, now_ms=1050, decision_candle_ms=900)
    assert store.load()[0][ID].deadline_ms == 1100
    restarted = NativeEntryGate(max_wait_ms=100, max_states=1, checkpoint=store)
    assert not restarted.evaluate(a, None, now_ms=1150, decision_candle_ms=900)
    assert restarted.state[ID].phase == "EXPIRED"
    assert not restarted.evaluate(b, None, now_ms=1150, decision_candle_ms=900)
    assert restarted.reason == "gate_capacity_exceeded"
    assert not restarted.evaluate(a, None, now_ms=2000, decision_candle_ms=900)
    assert restarted.state[ID].phase == "EXPIRED"
    assert not restarted.evaluate(b, None, now_ms=2000, decision_candle_ms=2000)
    assert ID not in restarted.state and "b" * 64 in restarted.state
    rollback = NativeEntryGate(max_wait_ms=100, max_states=1, checkpoint=store)
    assert not rollback.evaluate(a, None, now_ms=1500, decision_candle_ms=900)
    assert rollback.reason == "gate_clock_regressed"


def test_actual_native_runtime_attaches_budget_proxy_and_recorder_checkpoint():
    model_base = declarative_base()

    class Metadata(model_base):
        __tablename__ = "observation_runtime_metadata"
        key = Column(String, primary_key=True)
        value = Column(String, nullable=False)

    engine = create_engine("sqlite+pysqlite:///:memory:")
    model_base.metadata.create_all(engine)
    recorder = NS(
        config_file_path="offline-observations.yml",
        sql_manager=NS(get_new_session=sessionmaker(bind=engine)),
        get_all_executors=lambda: [],
        get_executors_by_controller=lambda _: [],
    )
    controller = ModularSpotObservationsV1Controller(config(), NS(ready=False, time=lambda: 1.2), asyncio.Queue())
    controller.runtime.bindings = NS(recorder=lambda: recorder, guard_resume=lambda *_: None, metadata_model=Metadata)
    store = controller.runtime.ensure_store()
    assert store.key and isinstance(controller.runtime.budget, ObservationBudgetProxy)
    assert controller.runtime.observation_gate.checkpoint.key == store.key + ":observations-v1"
    assert controller.runtime.ensure_store() is store
    controller.runtime.market = NS(candle_open=1.1)
    native = native_config(
        controller.config, identifier=ID, timestamp=1.2, price=D("10"), amount=D("1"), cycle=0, stage=0
    )
    assert controller.runtime.reservation(native).executor_id == ID
    assert controller.runtime._reservation_context == (ID, 0, 1100)
    gate = controller.runtime.observation_gate
    assert not gate.evaluate(
        NS(executor_id=ID, trading_pair="BTC-USDC", expires_at=D("2")), None, now_ms=1200, decision_candle_ms=1100
    )
    assert gate.checkpoint.load()[0][ID].deadline_ms == 1400
    engine.dispose()


def test_admission_checkpoint_error_is_visible_and_clears_after_valid_retry():
    controller = ModularSpotObservationsV1Controller(config(), NS(ready=False, time=lambda: 1.2), asyncio.Queue())

    def failed_checkpoint(_):
        raise OSError("synthetic checkpoint write failed")

    native = NS(reserve=lambda *args, **kwargs: True)
    proxy = ObservationBudgetProxy(native, failed_checkpoint)
    controller.runtime.budget = proxy
    assert not proxy.reserve(NS(), native_executors=[])
    info = controller.get_custom_info()["observation_entry"]
    assert info["status"] == "observation_admission_error"
    assert info["admission_error"] == "OSError:synthetic checkpoint write failed"
    proxy.admission = lambda _: False
    assert not proxy.reserve(NS(), native_executors=[])
    assert controller.get_custom_info()["observation_entry"]["admission_error"] is None


def reader_fixture(*, max_trades=3, max_quotes=4):
    reader = ObservationReader(
        instrument_id="okx:spot:BTC-USDC",
        source_id=SOURCE_ID,
        source_contract_hash=SOURCE_HASH,
        source_profile="okx_eea_public_v1",
        max_trades=max_trades,
        max_quotes=max_quotes,
    )
    source = SourceQualification(
        instrument_id=reader.instrument_id,
        source_id=SOURCE_ID,
        source_contract_hash=SOURCE_HASH,
        source_profile=reader.source_profile,
        metadata_revision="meta1",
        metadata_observed_sha256="d" * 64,
        selection_epoch="s1",
        qualified_at_ms=1000,
        review_available_at_ms=1000,
        expires_at_ms=1400,
        review_receipt_sha256="a" * 64,
    )
    reader.attach_verified_source(source, verify=lambda receipt: receipt == source)
    return reader


def wire(*, sequence, cursor, channel, kind, epoch, at, payload):
    return {
        "schema_version": "okx.observations.update.v1",
        "sequence": sequence,
        "epoch": epoch,
        "record_kind": kind,
        "channel": channel,
        "instrument_id": "okx:spot:BTC-USDC",
        "available_at_ms": at,
        "accepted_at_ms": at,
        "durable_acknowledged_at_ms": at,
        "computed_at_ms": None,
        "published_at_ms": at,
        "quality": "qualified",
        "reason_codes": [],
        "payload": payload,
        "durable_cursor": None if cursor is None else {"segment": 1, "sequence": cursor, "hash": f"{cursor:064x}"},
    }


def ingest_wire(reader, message):
    reader.ingest(message, received_at_ms=message["published_at_ms"])


def ready_updates(*, include_pending=False):
    messages = []
    original = built()
    for seq, cursor, channel, epoch in ((1, 1, "trades-all", "e1"), (3, 2, "bbo-tbt", "q1")):
        messages.append(
            wire(
                sequence=seq,
                cursor=cursor,
                channel=channel,
                kind="subscription_ack",
                epoch=epoch,
                at=1100,
                payload={"event": "subscription_ack", "inst_id": "BTC-USDC", "channel": channel},
            )
        )
    trade = dict(
        schema_version="okx.observations.v1",
        venue="okx",
        environment="public",
        region="eea",
        instrument_id="okx:spot:BTC-USDC",
        channel="trades-all",
        trade_id="123",
        price="10",
        base_quantity="4",
        quote_notional="40",
        side="buy",
        aggregate_count=1,
        event_at_ms=1150,
        available_at_ms=1160,
        metadata_revision="meta1",
        source_epoch="e1",
        quality="qualified",
        connection_id="c1",
        receive_sequence=1,
        raw_sha256="b" * 64,
        reason_codes=[],
    )
    messages.append(
        wire(
            sequence=5,
            cursor=3,
            channel="trades-all",
            kind="trade",
            epoch="e1",
            at=1160,
            payload={"observation": trade},
        )
    )
    for seq, cursor, quote in zip((7, 9, 11), (4, 5, 6), original.quotes):
        bbo = dict(
            schema_version="okx.observations.v1",
            venue="okx",
            channel="bbo-tbt",
            snapshot=True,
            depth_scope="single_level",
            instrument_id=quote.instrument_id,
            environment="public",
            region="eea",
            metadata_revision="meta1",
            source_epoch="q1",
            event_at_ms=quote.event_at_ms,
            available_at_ms=quote.available_at_ms,
            sequence=str(quote.sequence),
            bid_price=str(quote.bid_price),
            bid_quantity=str(quote.bid_quantity),
            ask_price=str(quote.ask_price),
            ask_quantity=str(quote.ask_quantity),
            quality="qualified",
            reason_codes=[],
        )
        messages.append(
            wire(
                sequence=seq,
                cursor=cursor,
                channel="bbo-tbt",
                kind="bbo",
                epoch="q1",
                at=quote.available_at_ms,
                payload={"observation": bbo},
            )
        )
    for seq, cursor, channel, epoch in ((13, 7, "trades-all", "e1"), (15, 8, "bbo-tbt", "q1")):
        messages.append(
            wire(
                sequence=seq,
                cursor=cursor,
                channel=channel,
                kind="timer",
                epoch=epoch,
                at=1200,
                payload={"event": "timer", "channel": channel, "qualified_source": False},
            )
        )
    if include_pending:
        pending = deepcopy(next(row for row in messages if row["record_kind"] == "trade"))
        pending["durable_cursor"] = None
        messages.insert(next(i for i, row in enumerate(messages) if row["record_kind"] == "trade"), pending)
    messages.sort(key=lambda row: row["published_at_ms"])
    cursor = 0
    for index, row in enumerate(messages, 1):
        row["sequence"] = index * 2 - 1
        if row["durable_cursor"] is not None:
            cursor += 1
            row["durable_cursor"] = {"segment": 1, "sequence": cursor, "hash": f"{cursor:064x}"}
    return messages


def ready_reader(*, max_trades=100, max_quotes=100):
    reader = reader_fixture(max_trades=max_trades, max_quotes=max_quotes)
    original = built()
    for row in ready_updates():
        ingest_wire(reader, row)
    receipt = replace(
        original.coverage, trade_cursor=DurableCursor(1, 7, f"{7:064x}"), quote_cursor=DurableCursor(1, 8, f"{8:064x}")
    )
    reader.attach_verified_coverage(receipt, verify=lambda item: item == receipt, received_at_ms=1200)
    return reader


def test_reader_requires_two_durable_channel_acks_and_independent_receipt():
    reader = reader_fixture()
    original = built()
    for row in ready_updates(include_pending=True):
        ingest_wire(reader, row)
        if row["durable_cursor"] is None:
            assert not reader.trades  # Queue admission is not durable evidence.
    receipt = replace(
        original.coverage, trade_cursor=DurableCursor(1, 7, f"{7:064x}"), quote_cursor=DurableCursor(1, 8, f"{8:064x}")
    )
    with pytest.raises(ValueError, match="independently verified"):
        reader.attach_verified_coverage(receipt, verify=lambda _: False, received_at_ms=1200)
    reader.attach_verified_coverage(receipt, verify=lambda item: item == receipt, received_at_ms=1200)
    result = reader.build(opportunity_id=ID, as_of_ms=1200, policy=original.policy)
    assert result.observation.qualified and result.coverage.trade_source_epoch == "e1"
    assert result.coverage.quote_source_epoch == "q1"
    reader.check_active(result.observation, now_ms=1200)
    value = config(observation_max_trades=3, observation_max_quotes=4)
    controller = ModularSpotObservationsV1Controller(value, NS(ready=False, time=lambda: 1.2), asyncio.Queue())
    controller.runtime.observation_gate.checkpoint = MemoryCheckpoint()
    controller.attach_observation_reader(reader)
    controller.runtime._reservation_context = (ID, 0, 1100)
    reservation = NS(executor_id=ID, trading_pair="BTC-USDC", expires_at=D("2"))
    assert controller.runtime._admit_observation(reservation)
    assert controller.runtime.observation_gate.mark_queued(ID, now_ms=1200)
    native = native_config(value, identifier=ID, timestamp=1.2, price=D("10"), amount=D("1"), cycle=0, stage=0)
    with patch.object(base.ModularSpotController, "validate_executor_config", return_value=True):
        assert controller.validate_executor_config(native)
    reader.ingest(
        {"type": "source_status", "instrument_id": reader.instrument_id, "channel": "bbo-tbt", "reason": "SOURCE_GAP"}
    )
    assert reader.qualification is None
    with pytest.raises(ValueError, match="coverage is unavailable"):
        reader.check_active(result.observation, now_ms=1200)
    with patch.object(base.ModularSpotController, "validate_executor_config", return_value=True):
        assert not controller.validate_executor_config(native)
    assert controller.runtime.observation_gate.state[ID].phase == "QUEUED"


def test_joint_veto_needs_selling_adverse_price_and_no_stabilization():
    ordinary = built()
    sell = replace(ordinary.trades[0], trade=replace(ordinary.trades[0].trade, side="sell"))

    def quote(event, bid, ask, sequence):
        return replace(
            ordinary.quotes[0],
            event_at_ms=event,
            available_at_ms=event,
            bid_price=D(bid),
            ask_price=D(ask),
            sequence=sequence,
        )

    falling = (quote(1100, "9.99", "10.01", 1), quote(1150, "9.89", "9.91", 2), quote(1190, "9.79", "9.81", 3))
    result = build_entry_observation(
        opportunity_id=ID,
        as_of_ms=1200,
        trades=(sell,),
        quotes=falling,
        coverage=ordinary.coverage,
        policy=ordinary.policy,
    )
    assert result.observation.adverse_flow
    stabilized = (falling[0], falling[1], quote(1190, "9.89", "9.91", 3))
    result = build_entry_observation(
        opportunity_id=ID,
        as_of_ms=1200,
        trades=(sell,),
        quotes=stabilized,
        coverage=ordinary.coverage,
        policy=ordinary.policy,
    )
    assert not result.observation.adverse_flow
    result = build_entry_observation(
        opportunity_id=ID,
        as_of_ms=1200,
        trades=ordinary.trades,
        quotes=falling,
        coverage=ordinary.coverage,
        policy=ordinary.policy,
    )
    assert not result.observation.adverse_flow


def test_exact_paper_candidate_binds_isolated_native_session(tmp_path):
    from test.hummingbot.connector.exchange.paper_trade.test_native_paper_trade_exchange import paper

    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.strategy_v2.paper_session import NativePaperSession

    value = ModularSpotObservationsV1PaperConfig.model_validate(
        {
            **config().model_dump(mode="json"),
            "connector_name": "okx_paper_trade",
            "research_only": True,
            "execution_mode": "paper",
            "candles_connector": "okx",
        }
    )
    assert value.get_controller_class() is ModularSpotObservationsV1Controller

    async def exercise_session():
        with patch.object(MarketsRecorder, "_shared_instance", None):
            session = NativePaperSession(paper(), [value], tmp_path / "observations.sqlite")
            try:
                controller = session.controllers[value.id]
                assert type(controller.config) is ModularSpotObservationsV1PaperConfig
                assert controller.get_candles_config()[0].connector == "okx"
                assert controller.runtime.observation_reader is None
                assert controller.runtime.observation_gate.reason == "passive"
            finally:
                await session.close()

    asyncio.run(exercise_session())
    with pytest.raises(ValueError, match="isolated native paper connector"):
        ModularSpotObservationsV1Controller(value, NS(connectors={}, ready=False), asyncio.Queue())


def test_reusable_two_epoch_synthetic_rust_schema_fixture_is_causal():
    value = paper_config()
    root, sources = observation_owner.source_root(), observation_owner.source_files()
    assert value.get_controller_class().__module__ == observation_owner.__name__
    assert root / "hummingbot/strategy_v2/market_context/observation_reader.py" in sources
    assert root / "hummingbot/strategy_v2/paper_session.py" in sources
    assert len(source_digest(root, sources)) == 64
    reader, updates, receipt, verify = fixture_stream()
    assert value.observation_source_id == receipt.source_id
    assert value.observation_source_contract_hash == receipt.source_contract_hash
    assert receipt.trade_source_epoch != receipt.quote_source_epoch
    for item in updates:
        reader.ingest(item, received_at_ms=item["published_at_ms"])
    reader.attach_verified_coverage(receipt, verify=verify, received_at_ms=1_060_000)
    result = reader.build(opportunity_id=ID, as_of_ms=1_060_000, policy=value.feature_policy())
    assert result.observation.qualified and not result.observation.adverse_flow
    assert len(result.quotes) == 3 and len(result.trades) == 1
    with pytest.raises(ValueError, match="independently verified"):
        reader.attach_verified_coverage(
            replace(receipt, source_contract_hash="f" * 64), verify=verify, received_at_ms=1_060_000
        )


def test_metadata_projection_and_derived_rows_do_not_reset_trade_quote_epochs():
    reader = ready_reader()
    observation = reader.build(opportunity_id=ID, as_of_ms=1200, policy=config().feature_policy()).observation
    reader.ingest(
        {
            "schema_version": "okx.observations.update.v1",
            "sequence": 17,
            "epoch": "capture-global",
            "record_kind": "metadata_available",
            "channel": None,
            "instrument_id": reader.instrument_id,
            "available_at_ms": 1200,
            "accepted_at_ms": 1200,
            "durable_acknowledged_at_ms": 1200,
            "published_at_ms": 1200,
            "payload": {"event": "metadata_available", "metadata_revision": "meta1"},
            "durable_cursor": {"segment": 1, "sequence": 9, "hash": f"{9:064x}"},
        },
        received_at_ms=1200,
    )
    observed = {
        "schema_version": "okx.observations.update.v1",
        "sequence": 19,
        "epoch": "capture-global",
        "record_kind": "exchange_metadata_observed",
        "channel": "metadata",
        "instrument_id": reader.instrument_id,
        "available_at_ms": 1200,
        "accepted_at_ms": 1200,
        "durable_acknowledged_at_ms": 1200,
        "published_at_ms": 1200,
        "payload": {
            "event": "exchange_metadata_observed",
            "metadata_revision": "meta1",
            "observed_revision_sha256": "d" * 64,
        },
        "durable_cursor": {"segment": 1, "sequence": 10, "hash": f"{10:064x}"},
    }
    reader.ingest(observed, received_at_ms=1200)
    reader.ingest(
        {
            "schema_version": "okx.observations.update.v1",
            "sequence": 21,
            "epoch": "e1",
            "record_kind": "time_bar",
            "channel": "trades-all",
            "instrument_id": reader.instrument_id,
            "available_at_ms": None,
            "payload": {"record_type": "time_bar"},
            "durable_cursor": {"segment": 1, "sequence": 11, "hash": f"{11:064x}"},
        }
    )
    reader.check_active(observation, now_ms=1200)
    reader.ingest(
        {**observed, "sequence": 23, "payload": {**observed["payload"], "observed_revision_sha256": "e" * 64}},
        received_at_ms=1200,
    )
    assert reader.qualification is None
    with pytest.raises(ValueError, match="coverage is unavailable"):
        reader.check_active(observation, now_ms=1200)


def test_new_transport_snapshot_revokes_old_window_without_replay():
    reader = ready_reader()
    assert reader.coverage is not None
    reader.ingest(
        {"type": "snapshot", "schema_version": "okx.observations.health.v1", "instrument_ids": [reader.instrument_id]}
    )
    assert reader.coverage is None and reader.qualification is None


def test_future_publication_and_missing_timing_revoke_durable_source():
    reader = ready_reader()
    later = wire(
        sequence=17,
        cursor=9,
        channel="trades-all",
        kind="timer",
        epoch="e1",
        at=1201,
        payload={"event": "timer", "channel": "trades-all"},
    )
    with pytest.raises(ValueError, match="causal durable publication"):
        reader.ingest(later, received_at_ms=1200)
    assert reader.qualification is None and reader.coverage is None

    reader = ready_reader()
    missing = wire(
        sequence=17,
        cursor=9,
        channel="trades-all",
        kind="timer",
        epoch="e1",
        at=1200,
        payload={"event": "timer", "channel": "trades-all"},
    )
    del missing["published_at_ms"]
    with pytest.raises(ValueError, match="causal durable publication"):
        reader.ingest(missing, received_at_ms=1200)
    assert reader.qualification is None


def test_observations_and_coverage_visible_only_after_consumer_receipt():
    reader, updates, receipt, verify = fixture_stream()
    delayed = None
    for item in updates:
        if item["record_kind"] == "bbo" and item["available_at_ms"] == 1_059_990:
            delayed = deepcopy(item)
        else:
            reader.ingest(item, received_at_ms=item["published_at_ms"])
    reader.attach_verified_coverage(receipt, verify=verify, received_at_ms=1_060_000)
    delayed.update(
        sequence=17,
        available_at_ms=1_060_001,
        accepted_at_ms=1_060_001,
        durable_acknowledged_at_ms=1_060_001,
        published_at_ms=1_060_001,
        durable_cursor={"segment": 1, "sequence": 9, "hash": f"{9:064x}"},
    )
    delayed["payload"]["observation"]["available_at_ms"] = 1_060_001
    reader.ingest(delayed, received_at_ms=1_060_001)
    result = reader.build(opportunity_id=ID, as_of_ms=1_060_000, policy=paper_config().feature_policy())
    assert result.quotes[-1].event_at_ms == 1_010_000
    reader.attach_verified_coverage(receipt, verify=verify, received_at_ms=1_060_001)
    with pytest.raises(ValueError, match="joint coverage unavailable"):
        reader.build(opportunity_id=ID, as_of_ms=1_060_000, policy=paper_config().feature_policy())


def test_revoked_reader_can_be_replaced_without_releasing_native_claim():
    controller, native, clock = candidate()
    original = controller.runtime.observation_reader
    before = deepcopy(controller.runtime.observation_gate.state[ID])
    with pytest.raises(ValueError, match="reader"):
        controller.attach_observation_reader(ready_reader())
    controller.ingest_observation_update({"type": "reset", "reason": "MISSED_UPDATES"})
    assert original.qualification is None and original.coverage is None
    replacement = ready_reader()
    controller.attach_observation_reader(replacement)
    assert controller.runtime.observation_reader is replacement
    assert controller.runtime.observation_gate.state[ID] == before
    assert controller.runtime.entry_observations == {}
    assert replacement.build(
        opportunity_id="b" * 64, as_of_ms=1200, policy=controller.config.feature_policy()
    ).observation.qualified
    with patch.object(base.ModularSpotController, "validate_executor_config", return_value=True):
        assert not controller.validate_executor_config(native)
    assert controller.runtime.observation_gate.state[ID].phase == "QUEUED"


def test_consumer_receipt_clock_cannot_regress_in_producer_sequence():
    reader = ready_reader()
    message = wire(
        sequence=17,
        cursor=9,
        channel="trades-all",
        kind="timer",
        epoch="e1",
        at=1199,
        payload={"event": "timer", "channel": "trades-all"},
    )
    with pytest.raises(ValueError, match="receipt clock regressed"):
        reader.ingest(message, received_at_ms=1199)
    assert reader.qualification is None and reader.coverage is None


def test_later_receipt_cannot_replace_newer_bbo_snapshot_with_old_sequence():
    reader = ready_reader()
    quote = dict(
        schema_version="okx.observations.v1",
        venue="okx",
        environment="public",
        region="eea",
        instrument_id="okx:spot:BTC-USDC",
        channel="bbo-tbt",
        snapshot=True,
        depth_scope="single_level",
        metadata_revision="meta1",
        source_epoch="q1",
        event_at_ms=1180,
        available_at_ms=1201,
        sequence="2",
        bid_price="9.99",
        bid_quantity="2",
        ask_price="10.01",
        ask_quantity="3",
        quality="qualified",
        reason_codes=[],
    )
    message = wire(
        sequence=17, cursor=9, channel="bbo-tbt", kind="bbo", epoch="q1", at=1201, payload={"observation": quote}
    )
    with pytest.raises(ValueError, match="BBO snapshot order"):
        reader.ingest(message, received_at_ms=1201)
    assert reader.qualification is None and reader.coverage is None
