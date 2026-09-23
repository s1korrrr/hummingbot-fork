"""Durable, fail-closed admission for one BUY attempt per cleared setup."""
import hashlib
import json
import math

from sqlalchemy import update

from hummingbot.model.metadata import Metadata


def executor_fingerprint(config):
    payload = json.dumps(config.model_dump(mode="json"), sort_keys=True, allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


class MetadataEntryDecisionStore:
    def __init__(self, manager, script, controller, connector, pair, interval):
        scope = [script, controller, connector, pair, interval]
        if any(not isinstance(value, str) or not value.strip() for value in scope):
            raise ValueError("Entry decision scope is incomplete")
        self.manager = manager
        self.key = "ok-rsi-entry-decision:" + json.dumps(scope, separators=(",", ":"))

    def load(self):
        with self.manager.get_new_session() as session:
            row = session.get(Metadata, self.key)
            return json.loads(row.value) if row else None

    def save(self, previous, current):
        payload = json.dumps(current, sort_keys=True, allow_nan=False)
        with self.manager.get_new_session() as session:
            with session.begin():
                if previous is None:
                    session.add(Metadata(key=self.key, value=payload))
                else:
                    old = json.dumps(previous, sort_keys=True, allow_nan=False)
                    result = session.execute(update(Metadata).where(
                        Metadata.key == self.key, Metadata.value == old,
                    ).values(value=payload))
                    if result.rowcount != 1:
                        raise RuntimeError("Entry decision changed concurrently; admission blocked")


class EntryDecisionGate:
    """Consume a candle only after an Executors row exists for that attempt."""

    def __init__(self, store, executor_exists=None):
        self.store = store
        self._executor_exists = executor_exists
        self._eligible_candle = None
        self._local_authorization = None
        self._awaiting_commit = None

    def _load(self):
        state = self.store.load()
        if state is None:
            return None
        required = {"version", "candle", "armed", "consumed_candle", "authorization"}
        if not isinstance(state, dict) or set(state) != required or type(state["version"]) is not int or state["version"] != 1:
            raise ValueError("Invalid entry decision state version or fields")
        if (type(state["candle"]) is not int or state["candle"] <= 0
                or type(state["consumed_candle"]) is not int
                or not 0 <= state["consumed_candle"] <= state["candle"]
                or type(state["armed"]) is not bool):
            raise ValueError("Invalid entry decision state values")
        auth = state["authorization"]
        if auth is not None:
            if (not isinstance(auth, dict) or set(auth) != {"executor_id", "fingerprint", "candle", "dispatched"}
                    or any(not isinstance(auth[key], str) or not auth[key] for key in ("executor_id", "fingerprint"))
                    or type(auth["candle"]) is not int or auth["candle"] <= 0
                    or type(auth["dispatched"]) is not bool):
                raise ValueError("Invalid entry authorization")
            expected = state["consumed_candle"] if state["consumed_candle"] else state["candle"]
            if auth["candle"] != expected:
                raise ValueError("Invalid entry authorization")
        elif state["consumed_candle"]:
            raise ValueError("Consumed decision lacks authorization")
        return state

    def _exists(self, executor_id):
        if self._executor_exists is None:
            return None
        return self._executor_exists(executor_id)

    def _reconcile(self, previous):
        if not previous or not previous["authorization"]:
            return previous
        if self._awaiting_commit is not None or self._local_authorization is not None:
            return previous
        exists = self._exists(previous["authorization"]["executor_id"])
        if exists is True:
            candle = previous["authorization"]["candle"]
            if previous["consumed_candle"] == candle:
                return previous
            current = dict(previous, armed=False, consumed_candle=candle)
            self.store.save(previous, current)
            return current
        if exists is False:
            current = dict(previous, armed=True, consumed_candle=0, authorization=None)
            self.store.save(previous, current)
            return current
        return previous

    def observe(self, data):
        self._eligible_candle = None
        timestamp = data.get("candle_timestamp")
        signal_state = data.get("signal_state", {})
        if (not isinstance(signal_state, dict)
                or data.get("signal_suppression_reason")
                or data.get("data_health", {}).get("ready") is not True
                or signal_state.get("condition_ok") is not True
                or type(signal_state.get("raw_buy_candidate")) is not bool
                or isinstance(timestamp, bool) or not isinstance(timestamp, (int, float))
                or not math.isfinite(timestamp) or timestamp <= 0 or int(timestamp) != timestamp):
            return False
        candle = int(timestamp)
        previous = self._reconcile(self._load())
        if previous is not None and candle < previous["candle"]:
            return False
        # An unconsumed authorization still needs an executor-row verdict.
        # Advancing candles must not erase it before commit or reconciliation.
        if (previous is not None and candle > previous["candle"]
                and previous["authorization"] is not None
                and previous["consumed_candle"] == 0):
            return False
        if previous is not None and candle == previous["candle"]:
            if previous["consumed_candle"] == candle:
                return False
            eligible = previous["armed"] and data.get("signal") == 1
            self._eligible_candle = candle if eligible else None
            return eligible
        current = dict(previous) if previous else {
            "version": 1, "candle": candle, "armed": False,
            "consumed_candle": 0, "authorization": None,
        }
        current["candle"] = candle
        if current["authorization"] is not None and current["consumed_candle"] == 0:
            current["authorization"] = None
        candidate = signal_state["raw_buy_candidate"]
        if not candidate and data.get("signal") != 1 and not signal_state.get("buy_confirmation_active", False):
            current["armed"] = True
        self.store.save(previous, current)
        eligible = current["armed"] and data.get("signal") == 1
        self._eligible_candle = candle if eligible else None
        return eligible

    def reserve(self, executor_id, fingerprint, candle):
        previous = self._load()
        if (not previous or not previous["armed"] or previous["candle"] != candle
                or previous["consumed_candle"] == candle
                or self._eligible_candle != candle):
            return False
        auth = {"executor_id": executor_id, "fingerprint": fingerprint,
                "candle": candle, "dispatched": False}
        # The previous candle's consumed marker belongs to its authorization.
        # Replace both atomically when a later, cleared setup is admitted.
        current = dict(previous, armed=False, consumed_candle=0, authorization=auth)
        self.store.save(previous, current)
        self._eligible_candle = None
        self._local_authorization = (executor_id, fingerprint, candle)
        return True

    def dispatch(self, executor_id, fingerprint, candle):
        identity = (executor_id, fingerprint, candle)
        if self._local_authorization != identity:
            return False
        previous = self._load()
        auth = previous["authorization"] if previous else None
        if (not auth or previous["candle"] != candle or auth["dispatched"]
                or (auth["executor_id"], auth["fingerprint"], auth["candle"]) != identity):
            return False
        self.store.save(previous, dict(previous, authorization=dict(auth, dispatched=True)))
        self._local_authorization = None
        self._awaiting_commit = (executor_id, fingerprint, auth["candle"])
        return True

    def commit(self, executor_id, fingerprint, candle):
        identity = (executor_id, fingerprint, candle)
        previous = self._load()
        auth = previous["authorization"] if previous else None
        if (not auth or previous["candle"] != candle
                or (auth["executor_id"], auth["fingerprint"], auth["candle"]) != identity):
            return False
        current = dict(previous, armed=False, consumed_candle=candle,
                       authorization=dict(auth, dispatched=True))
        self.store.save(previous, current)
        self._local_authorization = None
        self._awaiting_commit = None
        return True

    def commit_dispatched(self, executor_id, fingerprint):
        """Commit the queued attempt even if processed market data has advanced."""
        identity = self._awaiting_commit
        if identity is None or identity[:2] != (executor_id, fingerprint):
            return False
        return self.commit(*identity)

    def release(self):
        previous = self._load()
        if not previous or previous["authorization"] is None:
            self._local_authorization = None
            self._awaiting_commit = None
            return False
        exists = self._exists(previous["authorization"]["executor_id"])
        if exists is True:
            return False
        current = dict(previous, armed=True, consumed_candle=0, authorization=None)
        self.store.save(previous, current)
        self._local_authorization = None
        self._awaiting_commit = None
        return True

    def abandon(self):
        previous = self._load()
        if not previous or (previous["authorization"] is None and previous["consumed_candle"] == 0):
            self._local_authorization = None
            self._awaiting_commit = None
            return False
        current = dict(previous, armed=True, consumed_candle=0, authorization=None)
        self.store.save(previous, current)
        self._local_authorization = None
        self._awaiting_commit = None
        return True
