import json
from decimal import Decimal
from pathlib import Path

import pytest


def runtime_store_types():
    from controllers.directional_trading.rsi_v5_runtime_store import (
        RSIV5RuntimeEvent,
        RSIV5RuntimeReplayError,
        RSIV5RuntimeStore,
    )

    return RSIV5RuntimeEvent, RSIV5RuntimeReplayError, RSIV5RuntimeStore


def test_append_is_idempotent_for_the_same_event_id_and_payload():
    Event, _, Store = runtime_store_types()
    store = Store.in_memory()
    event = Event(
        controller_id="controller-1",
        event_id="event-1",
        episode_id="episode-1",
        event_type="EPISODE_OPENED",
        payload={"started_at": 1.0},
        created_at=1.0,
    )

    first = store.append(event)
    duplicate = store.append(event)

    assert first.sequence == 1
    assert duplicate == first
    assert store.load("controller-1") == [first]


def test_changed_duplicate_event_id_is_rejected():
    Event, ReplayError, Store = runtime_store_types()
    store = Store.in_memory()
    original = Event(
        controller_id="controller-1",
        event_id="event-1",
        episode_id="episode-1",
        event_type="EPISODE_OPENED",
        payload={"started_at": 1.0},
        created_at=1.0,
    )
    store.append(original)

    with pytest.raises(ReplayError, match="event_id"):
        store.append(
            Event(
                controller_id="controller-1",
                event_id="event-1",
                episode_id="episode-1",
                event_type="EPISODE_OPENED",
                payload={"started_at": 2.0},
                created_at=1.0,
            )
        )


def test_explicit_sequence_conflict_fails_without_partial_append():
    Event, ReplayError, Store = runtime_store_types()
    store = Store.in_memory()

    with pytest.raises(ReplayError, match="sequence"):
        store.append_many(
            [
                Event(
                    "controller-1",
                    "event-1",
                    "episode-1",
                    "EPISODE_OPENED",
                    {"started_at": 1.0},
                    1.0,
                ),
                Event(
                    "controller-1",
                    "event-2",
                    "episode-1",
                    "EXECUTOR_REGISTERED",
                    {"executor_id": "executor-1", "planned_notional": "100"},
                    1.1,
                    sequence=3,
                ),
            ]
        )

    assert store.load("controller-1") == []


def test_replay_restores_open_episode_settlement_and_pending_exit():
    Event, _, Store = runtime_store_types()
    store = Store.in_memory()
    events = [
        Event("controller-1", "open", "episode-1", "EPISODE_OPENED", {"started_at": 1.0}, 1.0),
        Event(
            "controller-1",
            "register",
            "episode-1",
            "EXECUTOR_REGISTERED",
            {"executor_id": "executor-1", "planned_notional": "100"},
            1.1,
        ),
        Event(
            "controller-1",
            "exit",
            "episode-1",
            "EXIT_REQUESTED",
            {"executor_id": "executor-1", "requested_at": 2.0, "reason": "signal"},
            2.0,
        ),
        Event(
            "controller-1",
            "settlement",
            "episode-1",
            "EXECUTOR_SETTLED",
            {
                "executor_id": "executor-1",
                "status": "PROVISIONAL",
                "net_pnl_quote": "-2",
                "revision": 1,
            },
            3.0,
        ),
    ]
    store.append_many(events)

    replay = store.replay("controller-1")

    assert replay.episode_tracker.episode_id == "episode-1"
    assert replay.exit_request_timestamps == {"executor-1": 2.0}
    settlement = replay.episode_tracker.settlement_for_executor("executor-1")
    assert settlement.net_pnl_quote == Decimal("-2")
    assert settlement.status.value == "PROVISIONAL"


def test_replay_restores_closed_episode_loss_streak():
    Event, _, Store = runtime_store_types()
    store = Store.in_memory()
    store.append_many(
        [
            Event("controller-1", "open", "episode-1", "EPISODE_OPENED", {"started_at": 1.0}, 1.0),
            Event(
                "controller-1",
                "register",
                "episode-1",
                "EXECUTOR_REGISTERED",
                {"executor_id": "executor-1", "planned_notional": "100"},
                1.1,
            ),
            Event(
                "controller-1",
                "settlement",
                "episode-1",
                "EXECUTOR_SETTLED",
                {
                    "executor_id": "executor-1",
                    "status": "FINAL",
                    "net_pnl_quote": "-5",
                    "revision": 1,
                },
                2.0,
            ),
            Event(
                "controller-1",
                "closed",
                "episode-1",
                "EPISODE_CLOSED",
                {
                    "ended_at": 3.0,
                    "executor_ids": ["executor-1"],
                    "net_pnl_quote": "-5",
                    "total_planned_notional": "100",
                },
                3.0,
            ),
        ]
    )

    replay = store.replay("controller-1")

    assert replay.episode_tracker.is_open is False
    assert replay.consecutive_losses == 1
    assert replay.last_loss_timestamp == 3.0
    assert replay.last_episode_outcome is not None
    assert replay.last_episode_outcome.net_pnl_quote == Decimal("-5")
    assert replay.historical_final_results == {"executor-1": Decimal("-5")}


def test_replay_restores_durable_reconciliation_hold():
    Event, _, Store = runtime_store_types()
    store = Store.in_memory()
    store.append(
        Event(
            "controller-1",
            "hold-unexpected-short",
            "runtime:controller-1",
            "RECONCILIATION_HOLD",
            {"reason": "unexpected_short"},
            2.0,
        )
    )

    replay = store.replay("controller-1")

    assert {reason.value for reason in replay.runtime_holds} == {"unexpected_short"}


def test_replay_rejects_sequence_gaps_and_corrupt_payloads():
    Event, ReplayError, Store = runtime_store_types()
    store = Store.in_memory()
    stored = store.append(
        Event("controller-1", "open", "episode-1", "EPISODE_OPENED", {"started_at": 1.0}, 1.0)
    )
    store._memory_events[0] = stored.model_copy(update={"sequence": 2})

    with pytest.raises(ReplayError, match="sequence"):
        store.replay("controller-1")

    corrupt = Store.in_memory()
    corrupt.append(
        Event("controller-1", "open", "episode-1", "EPISODE_OPENED", {"started_at": "bad"}, 1.0)
    )
    with pytest.raises(ReplayError, match="EPISODE_OPENED"):
        corrupt.replay("controller-1")


def test_payload_serialization_is_canonical_and_rejects_non_finite_numbers():
    Event, _, _ = runtime_store_types()
    event = Event(
        "controller-1",
        "event-1",
        "episode-1",
        "EXIT_REQUESTED",
        {"z": 1, "a": "value"},
        1.0,
    )

    assert event.payload_json == json.dumps(
        {"a": "value", "z": 1},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )

    with pytest.raises(ValueError):
        Event("controller-1", "bad", "episode-1", "EPISODE_OPENED", {"started_at": float("nan")}, 1.0)


def test_recorder_persists_ordered_events_and_rejects_changed_duplicate(tmp_path: Path):
    from hummingbot.client.config.client_config_map import ClientConfigMap
    from hummingbot.client.config.config_helpers import ClientConfigAdapter
    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.model.rsi_v5_runtime_event import RSIV5RuntimeEvent as RuntimeEventRow
    from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType

    manager = SQLConnectionManager(
        ClientConfigAdapter(ClientConfigMap()),
        SQLConnectionType.TRADE_FILLS,
        db_path=str(tmp_path / "runtime.sqlite"),
    )
    recorder = object.__new__(MarketsRecorder)
    recorder._sql_manager = manager
    store = runtime_store_types()[2](recorder)

    opened = store.append(
        runtime_store_types()[0](
            "controller-1",
            "open",
            "episode-1",
            "EPISODE_OPENED",
            {"started_at": 1.0},
            1.0,
        )
    )
    registered = store.append(
        runtime_store_types()[0](
            "controller-1",
            "register",
            "episode-1",
            "EXECUTOR_REGISTERED",
            {"executor_id": "executor-1", "planned_notional": "100"},
            1.1,
        )
    )

    assert [opened.sequence, registered.sequence] == [1, 2]
    assert [event.event_id for event in store.load("controller-1")] == ["open", "register"]
    with manager.get_new_session() as session:
        assert session.query(RuntimeEventRow).count() == 2

    assert store.append(opened) == opened
    with pytest.raises(RuntimeError, match="event_id"):
        store.append(opened.model_copy(update={"payload": {"started_at": 2.0}, "sequence": None}))

    restarted_manager = SQLConnectionManager(
        ClientConfigAdapter(ClientConfigMap()),
        SQLConnectionType.TRADE_FILLS,
        db_path=str(tmp_path / "runtime.sqlite"),
    )
    restarted_recorder = object.__new__(MarketsRecorder)
    restarted_recorder._sql_manager = restarted_manager
    restarted_store = runtime_store_types()[2](restarted_recorder)
    assert [event.event_id for event in restarted_store.load("controller-1")] == ["open", "register"]
