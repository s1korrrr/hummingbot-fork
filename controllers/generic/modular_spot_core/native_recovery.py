"""Persist native held-recovery policy state; native recorder remains inventory truth."""
import hashlib
import json
from dataclasses import asdict
from decimal import ROUND_DOWN, Decimal
from types import SimpleNamespace
from typing import Literal

from pydantic import Field, StrictBool, model_validator
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from hummingbot.core.data_type.common import TradeType
from hummingbot.model.metadata import Metadata
from hummingbot.model.position import Position
from hummingbot.model.position_hold_ledger import PositionHoldLedger
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator, PositionHold

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


def verify_pending_reductions(checkpoint, recorder, controller_id):
    """Keep a proposed held reduction bound to its actual native SELL intent."""
    if checkpoint is None or not checkpoint.pending_reductions:
        return
    executors = {info.id: info for info in recorder.get_executors_by_controller(controller_id)}
    for pending in checkpoint.pending_reductions:
        info = executors.get(pending.executor_id)
        if info is None:
            raise DataUnavailable('native recovery reduction receipt pending; peak reset is not reusable')
        fingerprint = identity(info.config.model_dump(mode='json', exclude={'controller_id'}))
        if fingerprint != pending.configuration_sha256:
            raise PolicyError('native recovery reduction receipt differs from the checkpoint intent')


def allocation_transition_held(session, owner, recorder, previous_config_sha256, previous_source_sha256):
    """Prove and carry one held checkpoint during a stopped, allocation-only rebind.

    Funding has already proved exact fill/order ownership in this transaction.
    Reconstruct held inventory with its native exact-fill accounting, and check
    the recorder's lossy six-decimal Position report just as runtime recovery
    does. Only the checkpoint's top-level owner identity may change.
    """
    config = owner.config
    key = 'modular-native-held:v1:' + identity([
        recorder.config_file_path, config.id, config.connector_name, config.trading_pair])
    row = session.get(Metadata, key)
    raw = None if row is None else row.value
    checkpoint = None if raw is None else decode(raw)
    family = config._native_family
    if checkpoint is not None and (checkpoint.family != family
                                   or checkpoint.configuration_sha256 != previous_config_sha256
                                   or checkpoint.source_sha256 != previous_source_sha256):
        raise PolicyError('Allocation-only held recovery prior source, configuration, or family differs')
    verify_pending_reductions(checkpoint, recorder, config.id)

    fills = recorder.get_trades_for_config(recorder.config_file_path)
    ledger = session.query(PositionHoldLedger).filter(PositionHoldLedger.controller_id == config.id).all()
    held_position = PositionHold(config.connector_name, config.trading_pair, TradeType.BUY)
    ledger_orders = set()
    try:
        for entry in sorted(ledger, key=lambda item: (item.timestamp, item.client_order_id)):
            if (entry.connector_name != config.connector_name or entry.trading_pair != config.trading_pair
                    or entry.position_side not in (None, '', 'None')
                    or entry.client_order_id in ledger_orders):
                raise ValueError('Foreign or duplicate native held ledger order')
            ledger_orders.add(entry.client_order_id)
            order_fills = [fill for fill in fills if fill.order_id == entry.client_order_id]
            record = ExecutorOrchestrator._corroborated_ledger_order(entry, order_fills)
            held_position.add_order_record(record)
        native = held_position.get_position_summary(Decimal('1'))
    except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
        raise PolicyError('Allocation-only held recovery native ledger differs from exact fills') from exc
    if native.side != TradeType.BUY and native.amount > 0:
        raise PolicyError('Allocation-only held recovery has unowned native inventory')
    recorded = session.query(Position).filter(Position.controller_id == config.id).all()
    scale = NativeRecoveryStore.POSITION_REPORT_SCALE
    actual = []
    for item in recorded:
        if (item.connector_name != config.connector_name or item.trading_pair != config.trading_pair
                or item.side != 'BUY' or item.amount < 0):
            raise PolicyError('Allocation-only held recovery recorder scope differs')
        if item.amount > 0:
            actual.append((item.amount, item.breakeven_price))
    # A native PositionHold has one aggregate BUY bag for this controller/pair.
    # Historical zero recorder rows are harmless, but extra positive bags are
    # ambiguous and require explicit reconciliation.
    if len(actual) > 1 or checkpoint is not None and len(checkpoint.positions) > 1:
        raise PolicyError('Allocation-only held recovery has ambiguous inventory')
    positions = () if checkpoint is None else checkpoint.positions
    if positions:
        held = positions[0]
        position_key = config.get_controller_class()._recovery_position_key(SimpleNamespace(
            connector_name=config.connector_name, trading_pair=config.trading_pair,
            side=TradeType.BUY, breakeven_price=held.breakeven_price))
        if (held.position_key != position_key or held.amount != native.amount
                or held.breakeven_price != native.breakeven_price):
            raise PolicyError('Allocation-only held recovery differs from native exact-fill inventory')
        projected = (held.amount.quantize(scale, rounding=ROUND_DOWN),
                     held.breakeven_price.quantize(scale, rounding=ROUND_DOWN))
        if actual != [projected]:
            raise PolicyError('Allocation-only held recovery differs from recorder inventory')
    elif actual or (native.amount != 0 if checkpoint is None
                    else native.amount.quantize(scale, rounding=ROUND_DOWN) != 0):
        raise PolicyError('Allocation-only held recovery has uncheckpointed native inventory')
    # A missing checkpoint is acceptable only for a proven empty scope. An
    # existing empty checkpoint retains its sequence and raw policy state.
    target_config = owner._composition_digest
    target_source = owner._native_source_hash
    updated = (None if checkpoint is None else encode(checkpoint.model_copy(update={
        'configuration_sha256': target_config, 'source_sha256': target_source})))
    return {'key': key, 'before_raw': raw, 'after_raw': updated,
            'before_sha256': None if raw is None else hashlib.sha256(raw.encode()).hexdigest(),
            'after_sha256': None if updated is None else hashlib.sha256(updated.encode()).hexdigest(),
            'previous_configuration_sha256': previous_config_sha256,
            'target_configuration_sha256': target_config,
            'previous_source_sha256': previous_source_sha256, 'target_source_sha256': target_source}


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
            if self._below_venue_min_size(item.amount):
                continue
            held.append(item)
        return tuple(held)

    def _reconcile_checkpoint_positions(self, previous, positions):
        """Accept inventory differences that are explained; refuse anything else.

        A bag that vanished (sold, or now below the venue minimum size) has
        nothing left to sell, so its trail and pending reduction are finished.
        A bag without a trail that the previous checkpoint excluded because it
        was below the venue minimum at cost is legitimately new to it. A changed
        amount, or any other new bag, is unexplained and still needs explicit
        reconciliation.
        """
        before = {item.position_key: item for item in previous.positions}
        trailed = {item.position_key for item in previous.trails}
        for item in positions:
            prior = before.get(item.position_key)
            if prior == item:
                continue
            legacy_excluded = (prior is None and item.position_key not in trailed
                               and self._below_venue_minimum(item.amount, item.breakeven_price))
            if not legacy_excluded:
                raise PolicyError('native inventory changed across restart; explicit held recovery reconciliation required')
        kept = {item.position_key for item in positions}
        self.owner.logger().warning(
            'Native held recovery reconciled checkpoint positions across restart | controller=%s dropped=%s added=%s',
            self.owner.config.id, sorted(set(before) - kept), sorted(kept - set(before)))
        return previous.model_copy(update={
            'positions': tuple(positions),
            'trails': tuple(item for item in previous.trails if item.position_key in kept),
            'pending_reductions': tuple(item for item in previous.pending_reductions if item.position_key in kept),
        })

    def _below_venue_min_size(self, amount):
        """Price-independent: a remainder below the venue minimum order size can never be sold alone."""
        try:
            rules = self.owner.market_data_provider.get_trading_rules(
                self.owner.config.connector_name, self.owner.config.trading_pair)
            min_size = decimal(rules.min_order_size, 'venue min order size', positive=True)
        except (PolicyError, AttributeError, TypeError, ValueError):
            return False
        return amount < min_size

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
            raise DataUnavailable('native recovery checkpoint store unavailable') from exc

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
                previous = self._reconcile_checkpoint_positions(previous, positions)
                self.previous = previous
            configuration_changed = previous.configuration_sha256 != self.owner._composition_digest
            if configuration_changed and positions:
                raise PolicyError('native held recovery source/configuration changed; explicit reconciliation required')
            if previous.source_sha256 != self.source_hash and positions:
                # Same owner, family, configuration and exact inventory: a code
                # upgrade carries the recovery trails forward (the next persist
                # records the new source hash). Held bags keep their exits.
                self.owner.logger().warning(
                    'Native held recovery carried across code upgrade | controller=%s source=%s->%s positions=%s',
                    self.owner.config.id, previous.source_sha256, self.source_hash, len(positions))
            if not configuration_changed:
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
        verify_pending_reductions(self.previous, self.recorder, self.owner.config.id)

    def persist(self, positions, actions=()):
        if self.inventory() != positions:
            raise DataUnavailable('native held inventory changed during the decision')
        held_keys = {item.position_key for item in positions}
        orphaned = sorted(key for key in self.owner._recovery_trails if key not in held_keys)
        if orphaned:
            # A trail whose bag is gone (sold down to dust) is finished; never let it block.
            self.owner.logger().warning('Native held recovery dropped trails without a bag | controller=%s bags=%s',
                                        self.owner.config.id, orphaned)
            for key in orphaned:
                self.owner._recovery_trails.pop(key, None)
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
        except IntegrityError as exc:  # Another writer created the checkpoint first.
            raise PolicyError('native held recovery concurrent writer; dispatch refused') from exc
        except SQLAlchemyError as exc:  # Rolled back: nothing persisted, the next cycle decides again.
            raise DataUnavailable('native held recovery persistence unavailable; dispatch refused') from exc
        self.previous, self.raw = value, payload
