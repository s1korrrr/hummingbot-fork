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
