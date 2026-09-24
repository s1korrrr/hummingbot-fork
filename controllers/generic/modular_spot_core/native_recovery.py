"""Persist native held-recovery policy state; native recorder remains inventory truth."""
import json
from dataclasses import asdict
from decimal import Decimal, ROUND_DOWN
from types import SimpleNamespace
from typing import Literal

from pydantic import Field, StrictBool, model_validator
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata

from .config import PolicyModel
from .domain import DataUnavailable, PolicyError, decimal, identity


class HeldIdentity(PolicyModel):
    position_key: str = Field(min_length=1, max_length=256)
    amount: Decimal = Field(gt=0)
    breakeven_price: Decimal = Field(gt=0)


class NativeTrail(PolicyModel):
    position_key: str = Field(min_length=1, max_length=256)
    armed_timestamp: float = Field(ge=0)
    arm_price: Decimal = Field(gt=0)
    peak_price: Decimal = Field(gt=0)
    tracked_amount: Decimal = Field(gt=0)
    peak_rsi: float | None = Field(default=None, ge=0, le=100)
    target_profit_pct: Decimal = Field(ge=0)
    last_reason: str = Field(min_length=1, max_length=128)
    partial_exit_done: StrictBool

    @model_validator(mode='after')
    def monotonic_arm(self):
        if self.peak_price < self.arm_price:
            raise ValueError('native recovery peak precedes its arm price')
        return self


class PendingReduction(PolicyModel):
    position_key: str = Field(min_length=1, max_length=256)
    executor_id: str = Field(min_length=1, max_length=128)
    configuration_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    prior_peak: Decimal = Field(gt=0)


class RecoveryCheckpoint(PolicyModel):
    version: Literal[1] = 1
    family: Literal['ok_rsi', 'rsi_v5']
    configuration_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    source_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    sequence: int = Field(ge=1, strict=True)
    positions: tuple[HeldIdentity, ...] = Field(max_length=128)
    trails: tuple[NativeTrail, ...] = Field(max_length=128)
    pending_reductions: tuple[PendingReduction, ...] = Field(default=(), max_length=128)

    @model_validator(mode='after')
    def native_scope(self):
        positions = {item.position_key for item in self.positions}
        trails = {item.position_key for item in self.trails}
        pending = {item.position_key for item in self.pending_reductions}
        if (len(positions) != len(self.positions) or len(trails) != len(self.trails) or not trails <= positions
                or len(pending) != len(self.pending_reductions) or not pending <= trails):
            raise ValueError('unattributed or duplicate native recovery trail')
        return self


def encode(checkpoint):
    value = RecoveryCheckpoint.model_validate(checkpoint.model_dump(mode='json')).model_dump_json()
    if len(value) > 65536:
        raise PolicyError('native recovery checkpoint exceeds bound')
    return value


def decode(raw):
    if len(raw) > 65536:
        raise PolicyError('native recovery checkpoint exceeds bound')
    try:
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) != set(RecoveryCheckpoint.model_fields):
            raise ValueError('incomplete recovery checkpoint')
        return RecoveryCheckpoint.model_validate(data)
    except (ValueError, TypeError) as exc:
        raise PolicyError('native held recovery checkpoint is corrupt') from exc


class NativeRecoveryStore:
    POSITION_REPORT_SCALE = Decimal('0.000001')

    def __init__(self, owner, recorder, source_hash):
        self.owner, self.recorder = owner, recorder
        self.source_hash = source_hash
        self.key = 'modular-native-held:v1:' + identity([
            recorder.config_file_path, owner.config.id, owner.config.connector_name, owner.config.trading_pair])
        self.previous = self.raw = None
        self.restored = False

    def _positions(self, positions):
        result = {}
        for item in positions:
            side = getattr(item.side, 'name', item.side)
            amount = decimal(item.amount, 'native held amount')
            if amount == 0:
                continue
            if (item.connector_name != self.owner.config.connector_name
                    or item.trading_pair != self.owner.config.trading_pair or side != 'BUY'):
                raise PolicyError('native held recovery received foreign inventory')
            basis = decimal(item.breakeven_price, 'native held basis', positive=True)
            value = SimpleNamespace(connector_name=item.connector_name, trading_pair=item.trading_pair,
                                    side=TradeType.BUY, breakeven_price=basis)
            key = self.owner._recovery_position_key(value)
            if key in result:
                old = result[key]
                if old.breakeven_price != basis:
                    raise PolicyError('native recovery key conflates different cost bases')
                amount += old.amount
            result[key] = HeldIdentity(position_key=key, amount=amount, breakeven_price=basis)
        held = []
        for key in sorted(result):
            item = result[key]
            if self._below_venue_minimum(item.amount, item.breakeven_price):
                continue
            held.append(item)
        return tuple(held)

    def _below_venue_minimum(self, amount, price):
        try:
            rules = self.owner.market_data_provider.get_trading_rules(
                self.owner.config.connector_name, self.owner.config.trading_pair)
            min_size = decimal(rules.min_order_size, 'venue min order size', positive=True)
            raw_notional = getattr(rules, 'min_notional_size', None) or getattr(rules, 'min_order_value', None) or 0
            min_notional = decimal(raw_notional, 'venue min notional')
        except (PolicyError, AttributeError, TypeError, ValueError):
            return False
        return amount < min_size or min_notional > 0 and amount * price < min_notional

    def inventory(self):
        saved = self._positions(self.recorder.get_positions_by_controller(self.owner.config.id))
        current = self._positions(self.owner.positions_held)
        # Position.amount/breakeven_price use SqliteDecimal(6). The owner is
        # reconstructed from exact native fills and must retain that precision;
        # compare only the lossy recorder projection at its actual storage scale.
        def projected(items):
            rows = []
            for item in items:
                basis = item.breakeven_price.quantize(self.POSITION_REPORT_SCALE, rounding=ROUND_DOWN)
                key = self.owner._recovery_position_key(SimpleNamespace(
                    connector_name=self.owner.config.connector_name,
                    trading_pair=self.owner.config.trading_pair, side=TradeType.BUY,
                    breakeven_price=basis))
                rows.append((key, item.amount.quantize(self.POSITION_REPORT_SCALE, rounding=ROUND_DOWN), basis))
            if len({row[0] for row in rows}) != len(rows):
                raise DataUnavailable('native held inventory report has conflated position keys')
            return tuple(sorted(rows))

        if projected(current) != projected(saved):
            raise DataUnavailable('native held inventory report and recorder have not reconciled')
        return current

    def _load(self):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                row = session.get(Metadata, self.key)
                self.raw = None if row is None else row.value
            self.previous = None if self.raw is None else decode(self.raw)
        except SQLAlchemyError as exc:
            raise PolicyError('native recovery checkpoint store unavailable') from exc

    def prepare(self):
        positions = self.inventory()
        if self.restored:
            self._verify_pending()
            return positions
        self._load()
        if self.previous is None:
            if positions:
                raise PolicyError('held inventory has no authoritative recovery checkpoint; explicit reconciliation required')
            if self.owner._recovery_trails:
                raise PolicyError('unpersisted native recovery trails cannot initialize a new checkpoint')
        else:
            previous = self.previous
            if previous.family != self.owner._native_family:
                raise PolicyError('native held recovery source/configuration changed; explicit reconciliation required')
            if previous.positions != positions:
                raise PolicyError('native inventory changed across restart; explicit held recovery reconciliation required')
            source_changed = (previous.source_sha256 != self.source_hash
                              or previous.configuration_sha256 != self.owner._composition_digest)
            if source_changed and positions:
                raise PolicyError('native held recovery source/configuration changed; explicit reconciliation required')
            if not source_changed:
                self._verify_pending()
                if self.owner._recovery_trails:
                    raise PolicyError('native recovery state was mutated before checkpoint restoration')
                if self.owner._native_family == 'ok_rsi':
                    from controllers.directional_trading.hl_rsi import RecoveryTrailState
                else:
                    from controllers.directional_trading.rsi_v5 import RecoveryTrailState
                self.owner._recovery_trails = {
                    trail.position_key: RecoveryTrailState(**trail.model_dump()) for trail in previous.trails}
        self.restored = True
        return positions

    def _verify_pending(self):
        if self.previous is None or not self.previous.pending_reductions:
            return
        executors = {info.id: info for info in self.recorder.get_executors_by_controller(self.owner.config.id)}
        for pending in self.previous.pending_reductions:
            info = executors.get(pending.executor_id)
            if info is None:
                raise DataUnavailable('native recovery reduction receipt pending; peak reset is not reusable')
            fingerprint = identity(info.config.model_dump(mode='json', exclude={'controller_id'}))
            if fingerprint != pending.configuration_sha256:
                raise PolicyError('native recovery reduction receipt differs from the checkpoint intent')

    def persist(self, positions, actions=()):
        if self.inventory() != positions:
            raise DataUnavailable('native held inventory changed during the decision')
        trails = tuple(NativeTrail.model_validate(asdict(value))
                       for _, value in sorted(self.owner._recovery_trails.items()))
        previous_trails = {} if self.previous is None else {item.position_key: item for item in self.previous.trails}
        reductions = {}
        for action in actions:
            config = getattr(action, 'executor_config', None)
            level = getattr(config, 'level_id', None)
            if config is not None and config.side == TradeType.SELL and level and level.startswith('recovery_exit:'):
                key = level[len('recovery_exit:'):]
                if key in reductions or key not in previous_trails:
                    raise PolicyError('unattributed or duplicate native recovery reduction')
                reductions[key] = PendingReduction(
                    position_key=key, executor_id=config.id,
                    configuration_sha256=identity(config.model_dump(mode='json', exclude={'controller_id'})),
                    prior_peak=previous_trails[key].peak_price)
        value = RecoveryCheckpoint(family=self.owner._native_family,
                                   configuration_sha256=self.owner._composition_digest, source_sha256=self.source_hash,
                                   sequence=1 if self.previous is None else self.previous.sequence + 1,
                                   positions=positions, trails=trails,
                                   pending_reductions=tuple(reductions[key] for key in sorted(reductions)))
        if self.previous:
            previous = {item.position_key: item for item in self.previous.trails}
            for trail in trails:
                old = previous.get(trail.position_key)
                if old and (trail.armed_timestamp != old.armed_timestamp
                            or trail.peak_price < old.peak_price and trail.position_key not in reductions
                            or old.partial_exit_done and not trail.partial_exit_done):
                    raise PolicyError('native held recovery high-water state regressed')
        payload = encode(value)
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    if self.raw is None:
                        session.add(Metadata(key=self.key, value=payload))
                        session.flush()
                    else:
                        changed = session.execute(update(Metadata).where(
                            Metadata.key == self.key, Metadata.value == self.raw).values(value=payload))
                        if changed.rowcount != 1:
                            raise PolicyError('native held recovery concurrent writer; dispatch refused')
        except (IntegrityError, SQLAlchemyError) as exc:
            raise PolicyError('native held recovery persistence failed; dispatch refused') from exc
        self.previous, self.raw = value, payload
