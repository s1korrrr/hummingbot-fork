"""Durable shortlist hysteresis in native Metadata; membership is not capital authority."""
import json

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from .domain import PolicyError, identity
from .universe import pairs


class SelectionStore:
    def __init__(self, recorder, *, group, policy_hash, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        self.recorder, self.model = recorder, metadata_model
        self.key = 'modular-shortlist:v1:' + identity([recorder.config_file_path, group])
        self.freeze_key = 'modular-admitted-policy:v1:' + identity([recorder.config_file_path, group])
        self.policy_hash = policy_hash

    def choose(self, *, candle, evidence_hash, select):
        try:
            for _ in range(16):
                with self.recorder.sql_manager.get_new_session() as session:
                    row = session.get(self.model, self.key)
                    old = row.value if row is not None else None
                previous = ()
                if old is not None:
                    data = json.loads(old)
                    if (set(data) != {'schema_version', 'policy_hash', 'candle', 'evidence_hash', 'entries'}
                            or data['schema_version'] != 1 or data['policy_hash'] != self.policy_hash
                            or type(data['candle']) is not int or candle < data['candle']):
                        raise PolicyError('shortlist identity or observation clock changed')
                    previous = pairs(data['entries'])
                    if data['evidence_hash'] == evidence_hash and data['candle'] == candle:
                        return previous
                selected = pairs(select(previous))
                payload = json.dumps({'schema_version': 1, 'policy_hash': self.policy_hash, 'candle': candle,
                                      'evidence_hash': evidence_hash, 'entries': selected}, sort_keys=True, allow_nan=False)
                try:
                    with self.recorder.sql_manager.get_new_session() as session:
                        with session.begin():
                            if old is None:
                                session.add(self.model(key=self.key, value=payload))
                                session.flush()
                                return selected
                            result = session.execute(update(self.model).where(self.model.key == self.key,
                                                                              self.model.value == old).values(value=payload))
                            if result.rowcount == 1:
                                return selected
                except IntegrityError:
                    continue
            raise PolicyError('shortlist CAS contention')
        except (SQLAlchemyError, ValueError, TypeError, KeyError) as exc:
            raise PolicyError('shortlist state unavailable or invalid') from exc

    def freeze(self, *, executor_id, trading_pair, policy_hash):
        if not isinstance(executor_id, str) or not executor_id or not isinstance(policy_hash, str) or not policy_hash:
            raise PolicyError('admitted policy identity is invalid')
        pairs((trading_pair,))
        try:
            for _ in range(16):
                with self.recorder.sql_manager.get_new_session() as session:
                    row = session.get(self.model, self.freeze_key)
                    old = None if row is None else row.value
                policies = {}
                if old is not None:
                    data = json.loads(old)
                    if (not isinstance(data, dict) or set(data) != {'schema_version', 'policies'}
                            or data['schema_version'] != 1 or not isinstance(data['policies'], dict)
                            or len(data['policies']) > 4096):
                        raise PolicyError('admitted policy schema invalid')
                    policies = dict(data['policies'])
                    current = policies.get(executor_id)
                    if current is not None:
                        if (not isinstance(current, dict) or set(current) != {'trading_pair', 'policy_hash'}
                                or current['trading_pair'] != trading_pair or current['policy_hash'] != policy_hash):
                            raise PolicyError('admitted behavioral policy is frozen')
                        return
                policies[executor_id] = {'trading_pair': trading_pair, 'policy_hash': policy_hash}
                payload = json.dumps({'schema_version': 1, 'policies': policies}, sort_keys=True, allow_nan=False)
                try:
                    with self.recorder.sql_manager.get_new_session() as session:
                        with session.begin():
                            if old is None:
                                session.add(self.model(key=self.freeze_key, value=payload))
                                session.flush()
                                return
                            result = session.execute(update(self.model).where(
                                self.model.key == self.freeze_key, self.model.value == old).values(value=payload))
                            if result.rowcount == 1:
                                return
                except IntegrityError:
                    continue
            raise PolicyError('admitted policy CAS contention')
        except PolicyError:
            raise
        except (SQLAlchemyError, ValueError, TypeError, KeyError) as exc:
            raise PolicyError('admitted policy state unavailable or invalid') from exc
