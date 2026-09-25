"""Atomic modular-program commitments in native Metadata, not a fill/PnL ledger.

Scope is one native recorder and strategy configuration. This is deliberately not
advertised as coordination across independently owned account databases.
"""
import json
from decimal import Decimal

from pydantic import Field, model_validator
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
    reserved_base: Decimal = Field(gt=0)
    account: str = Field(pattern=r'^[A-Za-z0-9_-]{1,80}$')
    connector: str = Field(min_length=1, max_length=80)
    program: str = Field(pattern=r'^[A-Za-z0-9_-]{1,80}$')
    slot: str = Field(pattern=r'^[A-Z0-9]+$', max_length=40)
    expires_at: Decimal = Field(ge=0)

    @model_validator(mode='after')
    def slot_is_base_asset(self):
        if self.slot != self.trading_pair.split('-')[0]:
            raise ValueError('position slot must be the base asset')
        return self


BAG_RECOVERY_LEVEL = 'modular_spot:v1:bag:recovery'
MAX_RELEASED = 65536


class BudgetStore:
    """Program commitments. Schema 1 holds reservations and members. Schema 2 adds the
    append-only ``released`` executor ids: held (bagged) tranches and bag recovery sells a
    member proved flat through :meth:`release_held`. A checkpoint with nothing released
    keeps the schema 1 bytes."""

    def __init__(self, recorder, policy: BudgetPolicy, *, connector: str, quote: str, account: str,
                 metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        self.model, self.recorder, self.policy = metadata_model, recorder, policy
        self.connector, self.quote, self.account = connector, quote, account
        # ponytail: one Metadata row per recorder/connector/quote/group. Account is bound on
        # each claim; a second namespace would not segregate exchange quote cash.
        self.key = 'modular-program-budget:v1:' + identity([recorder.config_file_path, connector, quote, policy.group])
        self.policy_hash = identity(policy.model_dump(mode='json'))

    def _member_probe(self, owner, pair):
        return Reservation(
            executor_id='member-validation', controller_id=owner, trading_pair=pair,
            reserved_quote=1, reserved_base=1, account=self.account, connector=self.connector,
            program=self.policy.group, slot=pair.split('-')[0], expires_at=0)

    def _parse(self, raw):
        if raw is None:
            return {}, {}, frozenset()
        try:
            value = json.loads(raw)
            base_keys = {'schema_version', 'policy_hash', 'reservations', 'members'}
            version = value.get('schema_version') if isinstance(value, dict) else None
            if (not isinstance(value, dict) or type(version) is not int
                    or set(value) != (base_keys if version == 1 else base_keys | {'released'} if version == 2 else None)
                    or value['policy_hash'] != self.policy_hash or not isinstance(value['reservations'], list)
                    or not isinstance(value['members'], dict) or len(value['members']) > 1000):
                raise PolicyError('program budget schema or policy changed')
            released = value.get('released', [])
            if (not isinstance(released, list) or (version == 2 and not released) or len(released) > MAX_RELEASED
                    or any(not isinstance(x, str) or not x for x in released)
                    or released != sorted(set(released))):
                raise PolicyError('program released executors invalid')
            members = value['members']
            for owner, pair in members.items():
                self._member_probe(owner, pair)
                if pair.split('-')[1] != self.quote:
                    raise PolicyError('program member quote identity mismatch')
            reservations = [Reservation.model_validate(item) for item in value['reservations']]
            if len(reservations) > 4096 or len({r.executor_id for r in reservations}) != len(reservations):
                raise PolicyError('duplicate or unbounded program commitments')
            if any(members.get(r.controller_id) != r.trading_pair for r in reservations):
                raise PolicyError('program member identity mismatch')
            if any(r.account != self.account or r.connector != self.connector or r.program != self.policy.group
                   for r in reservations):
                raise PolicyError('wrong program member account or connector')
            if sum((r.reserved_quote for r in reservations), Decimal('0')) > self.policy.max_quote:
                raise PolicyError('persisted program overcommitted')
            if len({r.slot for r in reservations}) > self.policy.max_assets:
                raise PolicyError('persisted asset limit violated')
            if len({(r.slot, r.trading_pair) for r in reservations}) != len({r.slot for r in reservations}):
                raise PolicyError('persisted base-asset concentration violated')
            if any(r.executor_id in released for r in reservations):
                raise PolicyError('released executor still reserved')
            return {r.executor_id: r for r in reservations}, members, frozenset(released)
        except (ValueError, TypeError, KeyError) as exc:
            raise PolicyError('program budget checkpoint invalid') from exc

    def _encode(self, reservations, members, released=frozenset()):
        value = {'schema_version': 1, 'policy_hash': self.policy_hash, 'members': members,
                 'reservations': [r.model_dump(mode='json') for _, r in sorted(reservations.items())]}
        if released:
            value.update(schema_version=2, released=sorted(released))
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)

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

    def _bind(self, claim: Reservation):
        if claim.connector != self.connector:
            raise PolicyError('reservation connector differs from program')
        if claim.program != self.policy.group:
            raise PolicyError('wrong program')
        if claim.account != self.account:
            raise PolicyError('wrong program member account')
        if claim.account == 'unassigned':
            raise PolicyError('account topology is unproven')
        if claim.trading_pair.split('-')[1] != self.quote:
            raise PolicyError('reservation quote differs from program')

    def contains(self, claim: Reservation, *, native_executors=()) -> bool:
        self._bind(claim)
        try:
            reservations, members, released = self._parse(self._load())
            evidence = self._evidence(native_executors)
            self._reject_foreign(evidence)
            self._check_unreserved(reservations, members, evidence, released)
            return reservations.get(claim.executor_id) == claim
        except (SQLAlchemyError, AttributeError) as exc:
            raise PolicyError('program reservation store unavailable') from exc

    def occupied_pairs(self, *, native_executors=()):
        try:
            reservations, members, released = self._parse(self._load())
            evidence = self._evidence(native_executors)
            self._reject_foreign(evidence)
            self._check_unreserved(reservations, members, evidence, released)
            pairs = {r.trading_pair for r in reservations.values()}
            pairs.update(self._active_modular_pairs(evidence, released))
            return tuple(sorted(pairs))
        except (SQLAlchemyError, AttributeError) as exc:
            raise PolicyError('program reservation store unavailable') from exc

    def _flat(self, owner, pair, info, released=frozenset()):
        if (info.controller_id != owner or info.config.controller_id != owner
                or info.config.connector_name != self.connector or info.config.trading_pair != pair):
            raise PolicyError('program receipt ownership differs')
        # A member's own bag recovery sell only reduces held inventory: it never spends quote.
        return self._is_member_bag_sell(info) or self._is_terminal_flat(info, released)

    @staticmethod
    def _is_member_bag_sell(info):
        config = info.config
        return (isinstance(info.controller_id, str) and info.controller_id.startswith('modular_spot_')
                and getattr(config, 'type', None) == 'order_executor'
                and getattr(getattr(config, 'side', None), 'name', None) == 'SELL'
                and getattr(getattr(config, 'position_action', None), 'name', None) == 'CLOSE'
                and str(getattr(config, 'level_id', '')).startswith(BAG_RECOVERY_LEVEL + ':')
                and getattr(config, 'min_sell_price', None) is not None)

    def _is_terminal_flat(self, info, released=frozenset()):
        if info.id in released:
            return True
        if info.is_done is not True or info.is_active is not False:
            return False
        if self._is_member_bag_sell(info):
            return True  # a finished bag sell no longer occupies its pair
        try:
            receipt = info.custom_info.get('execution_receipt')
            if not isinstance(receipt, dict) or receipt.get('accounting_owner') != 'executor':
                return False
            return decimal(receipt.get('remaining_owned_base'), 'remaining native inventory') == 0
        except (PolicyError, AttributeError, TypeError):
            return False

    def _active_modular_pairs(self, evidence, released=frozenset()):
        pairs = set()
        for info in evidence.values():
            cid = info.controller_id
            if not isinstance(cid, str) or not cid.startswith('modular_spot_'):
                continue
            pair = getattr(info.config, 'trading_pair', None)
            if isinstance(pair, str) and '-' in pair and not self._is_terminal_flat(info, released):
                pairs.add(pair)
        return pairs

    def _reject_foreign(self, evidence):
        for info in evidence.values():
            cid = info.controller_id
            if isinstance(cid, str) and cid.startswith('modular_spot_'):
                continue
            cfg = info.config
            if getattr(cfg, 'connector_name', None) != self.connector:
                continue
            pair = getattr(cfg, 'trading_pair', '')
            if not isinstance(pair, str) or '-' not in pair or pair.split('-')[1] != self.quote:
                continue
            if self._is_terminal_flat(info):
                continue
            raise PolicyError('foreign spender on this connector quote; topology unproven')

    @staticmethod
    def _evidence(native_executors):
        evidence = {}
        for info in native_executors:
            if not isinstance(info.id, str) or info.id in evidence:
                raise PolicyError('duplicate or missing native executor receipt')
            evidence[info.id] = info
        return evidence

    def _check_unreserved(self, reservations, members, evidence, released=frozenset()):
        for key, info in evidence.items():
            pair = members.get(info.controller_id)
            if pair is not None and key not in reservations and not self._flat(info.controller_id, pair, info, released):
                raise PolicyError('unreserved or reappearing native inventory requires reconciliation')

    def _concentration_blocked(self, revised, claim, evidence, released=frozenset()):
        if any(r.slot == claim.slot and r.trading_pair != claim.trading_pair for r in revised.values()):
            return True
        for pair in self._active_modular_pairs(evidence, released):
            if pair.split('-')[0] == claim.slot and pair != claim.trading_pair:
                return True
        return False

    def reserve(self, claim: Reservation, *, native_executors) -> bool:
        self._bind(claim)
        try:
            evidence = self._evidence(native_executors)
            self._reject_foreign(evidence)
            if claim.executor_id in evidence:
                return False  # Native ownership already exists; this is not a new admission.
            for _ in range(32):
                raw = self._load()
                current, members, released = self._parse(raw)
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
                self._check_unreserved(current, members, evidence, released)
                # Expiry, process absence, recovery labels and unconfirmed cancels never drop rows here.
                revised = {key: value for key, value in current.items()
                           if key not in evidence
                           or not self._flat(value.controller_id, value.trading_pair, evidence[key], released)}
                if self._concentration_blocked(revised, claim, evidence, released):
                    return False
                revised[claim.executor_id] = claim
                total = sum((r.reserved_quote for r in revised.values()), Decimal('0'))
                if total > self.policy.max_quote or len({r.slot for r in revised.values()}) > self.policy.max_assets:
                    return False
                if len(revised) > 4096:
                    raise PolicyError('program commitment capacity exceeded')
                if self._write(raw, self._encode(revised, members, released)):
                    return True
            raise PolicyError('program reservation CAS contention; no dispatch')
        except (SQLAlchemyError, AttributeError, IndexError) as exc:
            raise PolicyError('program reservation evidence or store unavailable') from exc

    def released(self):
        """Executor ids already proven flat through :meth:`release_held`."""
        try:
            return self._parse(self._load())[2]
        except (SQLAlchemyError, AttributeError) as exc:
            raise PolicyError('program reservation store unavailable') from exc

    def release_held(self, controller_id: str, executor_ids, *, native_executors) -> frozenset:
        """Durably release a member's terminal held tranches and bag recovery sells.

        The caller proves its held position is empty and that these receipts account
        for it. Released executors are flat for every member and every later call;
        their reservations are dropped in the same CAS write."""
        ids = frozenset(executor_ids)
        if not ids or any(not isinstance(x, str) or not x for x in ids):
            raise PolicyError('release requires explicit executor ids')
        try:
            evidence = self._evidence(native_executors)
            for _ in range(32):
                raw = self._load()
                current, members, released = self._parse(raw)
                pair = members.get(controller_id)
                if pair is None:
                    raise PolicyError('release requires a program member')
                for key in ids - released:
                    info = evidence.get(key)
                    if (info is None or info.controller_id != controller_id or info.config.controller_id != controller_id
                            or info.config.connector_name != self.connector or info.config.trading_pair != pair
                            or info.is_done is not True or info.is_active is not False):
                        raise PolicyError('release target is not a terminal executor of this member')
                    receipt = (info.custom_info or {}).get('execution_receipt')
                    held = isinstance(receipt, dict) and receipt.get('accounting_owner') == 'held_position'
                    if not held and not self._is_member_bag_sell(info):
                        raise PolicyError('only held tranches and bag recovery sells can be released')
                merged = released | ids
                if merged == released:
                    return released
                if len(merged) > MAX_RELEASED:
                    raise PolicyError('program released capacity exceeded')
                revised = {key: value for key, value in current.items() if key not in merged}
                if self._write(raw, self._encode(revised, members, merged)):
                    return merged
            raise PolicyError('program release CAS contention')
        except (SQLAlchemyError, AttributeError, TypeError) as exc:
            raise PolicyError('program release evidence or store unavailable') from exc
