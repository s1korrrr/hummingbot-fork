"""Atomic modular-program commitments in native Metadata, not a fill/PnL ledger.

Scope is one native recorder and strategy configuration. This is deliberately not
advertised as coordination across independently owned account databases.
"""
import json
from decimal import Decimal

from pydantic import Field
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from .candle_data import PAIR_PATTERN
from .config import PolicyModel
from .domain import PolicyError, decimal, identity


class BudgetPolicy(PolicyModel):
    group: str = Field(pattern=r'^[A-Za-z0-9_-]{1,80}$')
    max_quote: Decimal = Field(gt=0)
    max_assets: int = Field(ge=1, le=1000, strict=True)


class Reservation(PolicyModel):
    executor_id: str = Field(min_length=1, max_length=128)
    controller_id: str = Field(pattern=r'^modular_spot_[A-Za-z0-9_-]+$', max_length=128)
    trading_pair: str = Field(pattern=PAIR_PATTERN, max_length=80)
    reserved_quote: Decimal = Field(gt=0)


class BudgetStore:
    def __init__(self, recorder, policy: BudgetPolicy, *, connector: str, quote: str, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        self.model, self.recorder, self.policy = metadata_model, recorder, policy
        self.connector, self.quote = connector, quote
        self.key = 'modular-program-budget:v1:' + identity([recorder.config_file_path, connector, quote, policy.group])
        self.policy_hash = identity(policy.model_dump(mode='json'))

    def _parse(self, raw):
        if raw is None:
            return {}, {}
        try:
            value = json.loads(raw)
            if (not isinstance(value, dict) or set(value) != {'schema_version', 'policy_hash', 'reservations', 'members'}
                    or type(value['schema_version']) is not int or value['schema_version'] != 1
                    or value['policy_hash'] != self.policy_hash or not isinstance(value['reservations'], list)
                    or not isinstance(value['members'], dict) or len(value['members']) > 1000):
                raise PolicyError('program budget schema or policy changed')
            members = value['members']
            for owner, pair in members.items():
                Reservation(executor_id='member-validation', controller_id=owner, trading_pair=pair, reserved_quote=1)
                if pair.split('-')[1] != self.quote:
                    raise PolicyError('program member quote identity mismatch')
            reservations = [Reservation.model_validate(item) for item in value['reservations']]
            if len(reservations) > 4096 or len({r.executor_id for r in reservations}) != len(reservations):
                raise PolicyError('duplicate or unbounded program commitments')
            if any(members.get(r.controller_id) != r.trading_pair for r in reservations):
                raise PolicyError('program member identity mismatch')
            if sum((r.reserved_quote for r in reservations), Decimal('0')) > self.policy.max_quote:
                raise PolicyError('persisted program overcommitted')
            if len({r.trading_pair for r in reservations}) > self.policy.max_assets:
                raise PolicyError('persisted asset limit violated')
            return {r.executor_id: r for r in reservations}, members
        except (ValueError, TypeError, KeyError) as exc:
            raise PolicyError('program budget checkpoint invalid') from exc

    def _encode(self, reservations, members):
        return json.dumps({'schema_version': 1, 'policy_hash': self.policy_hash, 'members': members,
                           'reservations': [r.model_dump(mode='json') for _, r in sorted(reservations.items())]},
                          sort_keys=True, separators=(',', ':'), allow_nan=False)

    def _load(self):
        with self.recorder.sql_manager.get_new_session() as session:
            row = session.get(self.model, self.key)
            return None if row is None else row.value

    def _write(self, old, new):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    if old is None:
                        session.add(self.model(key=self.key, value=new))
                        session.flush()
                        return True
                    changed = session.execute(update(self.model).where(
                        self.model.key == self.key, self.model.value == old).values(value=new))
                    return changed.rowcount == 1
        except IntegrityError:
            return False

    def contains(self, claim: Reservation, *, native_executors=()) -> bool:
        try:
            reservations, members = self._parse(self._load())
            self._check_unreserved(reservations, members, self._evidence(native_executors))
            return reservations.get(claim.executor_id) == claim
        except (SQLAlchemyError, AttributeError) as exc:
            raise PolicyError('program reservation store unavailable') from exc

    def _flat(self, owner, pair, info):
        if (info.controller_id != owner or info.config.controller_id != owner
                or info.config.connector_name != self.connector or info.config.trading_pair != pair):
            raise PolicyError('program receipt ownership differs')
        if info.is_done is not True or info.is_active is not False:
            return False
        receipt = info.custom_info.get('execution_receipt')
        if not isinstance(receipt, dict) or receipt.get('accounting_owner') != 'executor':
            return False
        return decimal(receipt.get('remaining_owned_base'), 'remaining native inventory') == 0

    @staticmethod
    def _evidence(native_executors):
        evidence = {}
        for info in native_executors:
            if not isinstance(info.id, str) or info.id in evidence:
                raise PolicyError('duplicate or missing native executor receipt')
            evidence[info.id] = info
        return evidence

    def _check_unreserved(self, reservations, members, evidence):
        for key, info in evidence.items():
            pair = members.get(info.controller_id)
            if pair is not None and key not in reservations and not self._flat(info.controller_id, pair, info):
                raise PolicyError('unreserved or reappearing native inventory requires reconciliation')

    def reserve(self, claim: Reservation, *, native_executors) -> bool:
        if claim.trading_pair.split('-')[1] != self.quote:
            raise PolicyError('reservation quote differs from program')
        try:
            evidence = self._evidence(native_executors)
            if claim.executor_id in evidence:
                return False  # Native ownership already exists; this is not a new admission.
            for _ in range(32):
                raw = self._load()
                current, members = self._parse(raw)
                if raw is None and any(
                        str(info.controller_id).startswith('modular_spot_')
                        and info.config.connector_name == self.connector
                        and info.config.trading_pair.split('-')[1] == self.quote for info in evidence.values()):
                    raise PolicyError('native modular history without a program checkpoint')
                if claim.controller_id in members and members[claim.controller_id] != claim.trading_pair:
                    raise PolicyError('program member cannot change instrument ownership')
                members = {**members, claim.controller_id: claim.trading_pair}
                if len(members) > 1000:
                    raise PolicyError('program membership capacity exceeded')
                existing = current.get(claim.executor_id)
                if existing is not None and existing != claim:
                    raise PolicyError('program reservation identity mutated')
                self._check_unreserved(current, members, evidence)
                revised = {key: value for key, value in current.items()
                           if key not in evidence or not self._flat(value.controller_id, value.trading_pair, evidence[key])}
                revised[claim.executor_id] = claim
                total = sum((r.reserved_quote for r in revised.values()), Decimal('0'))
                if total > self.policy.max_quote or len({r.trading_pair for r in revised.values()}) > self.policy.max_assets:
                    return False
                if len(revised) > 4096:
                    raise PolicyError('program commitment capacity exceeded')
                if self._write(raw, self._encode(revised, members)):
                    return True
            raise PolicyError('program reservation CAS contention; no dispatch')
        except (SQLAlchemyError, AttributeError, IndexError) as exc:
            raise PolicyError('program reservation evidence or store unavailable') from exc
