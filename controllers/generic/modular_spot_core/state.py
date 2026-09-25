"""Decision-only CAS state in the existing recorder Metadata table."""
import json
from decimal import Decimal
from typing import Literal

from pydantic import Field, StrictBool, ValidationError, model_validator
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from .config import PolicyModel
from .domain import PolicyError, identity


class DecisionState(PolicyModel):
    schema_version: Literal[1] = 1
    fingerprint: str = Field(pattern=r'^[a-f0-9]{64}$')
    candle: int = Field(ge=0, strict=True)
    cycle: int = Field(default=0, ge=0, strict=True)
    claims: tuple[str, ...] = ()
    committed_quote: Decimal = Field(default=Decimal('0'), ge=0)
    last_entry_time: Decimal = Field(default=Decimal('0'), ge=0)
    risk_peak: Decimal | None = None
    risk_day: int | None = Field(default=None, ge=0, strict=True)
    risk_day_start: Decimal | None = None
    risk_paused: StrictBool = False

    @model_validator(mode='after')
    def valid_claims(self):
        if len(set(self.claims)) != len(self.claims) or any(not x.strip() for x in self.claims):
            raise ValueError('invalid decision claims')
        if not self.claims and self.committed_quote != 0:
            raise ValueError('unattributed commitment')
        if self.claims and self.committed_quote <= 0:
            raise ValueError('claim without commitment')
        return self


def encode(state: DecisionState) -> str:
    state = DecisionState.model_validate(state.model_dump())
    return json.dumps(state.model_dump(mode='json'), sort_keys=True, separators=(',', ':'), allow_nan=False)


def decode(raw: str) -> DecisionState:
    try:
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) != set(DecisionState.model_fields):
            raise PolicyError('incomplete decision checkpoint')
        return DecisionState.model_validate(data)
    except (TypeError, ValueError, ValidationError) as exc:
        raise PolicyError('invalid decision checkpoint') from exc


class DecisionStore:
    def __init__(self, recorder, controller_id: str, connector: str, pair: str, *, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        self.model = metadata_model
        self.recorder = recorder
        self.key = 'modular-spot:v1:' + identity([recorder.config_file_path, controller_id, connector, pair])

    def load(self):
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                row = session.get(self.model, self.key)
                raw = None if row is None else row.value
            return (None, None) if raw is None else (decode(raw), raw)
        except SQLAlchemyError as exc:
            raise PolicyError('decision store unavailable') from exc

    def compare_and_swap(self, expected: str | None, state: DecisionState) -> bool:
        payload = encode(state)
        if expected is not None:
            previous = decode(expected)
            if state.fingerprint != previous.fingerprint or state.candle < previous.candle:
                raise PolicyError('checkpoint identity or time regression')
            if state.cycle < previous.cycle or state.cycle > previous.cycle + 1:
                raise PolicyError('checkpoint cycle regression')
            if state.last_entry_time < previous.last_entry_time:
                raise PolicyError('entry clock regression')
            if state.cycle == previous.cycle:
                if (state.claims[:len(previous.claims)] != previous.claims
                        or state.committed_quote < previous.committed_quote):
                    raise PolicyError('claims cannot be released inside a campaign')
            elif not previous.claims or state.claims or state.committed_quote:
                raise PolicyError('invalid campaign reset')
            if previous.risk_paused and not state.risk_paused:
                raise PolicyError('risk latch cannot be cleared by a decision')
        try:
            with self.recorder.sql_manager.get_new_session() as session:
                with session.begin():
                    if expected is None:
                        session.add(self.model(key=self.key, value=payload))
                        session.flush()
                        return True
                    changed = session.execute(update(self.model).where(
                        self.model.key == self.key, self.model.value == expected).values(value=payload))
                    return changed.rowcount == 1
        except IntegrityError:
            return False
        except SQLAlchemyError as exc:
            raise PolicyError('decision persistence failed; no dispatch') from exc


def reconcile_claims(state: DecisionState, inventory, *, bag_amount=None):
    """Legacy mode holds on any held inventory. In bag mode (``bag_amount`` given) held
    claims are bag members; the campaign is flat only when every claim is terminal and
    the controller's held position is empty."""
    if any(identifier not in inventory for identifier in state.claims):
        return state, 'execution_receipt_missing'
    claimed = [inventory[x] for x in state.claims]
    if bag_amount is not None:
        if claimed and all(x.terminal for x in claimed) and bag_amount == 0:
            return state.model_copy(update={'claims': (), 'committed_quote': Decimal('0'),
                                            'cycle': state.cycle + 1}), 'campaign_flat'
        return state, 'reconciled'

    if any(x.held or (x.terminal and x.remaining_base > 0) for x in claimed):
        return state, 'held_inventory_requires_reconciliation'
    if claimed and all(x.terminal and x.remaining_base == 0 for x in claimed):
        return state.model_copy(update={'claims': (), 'committed_quote': Decimal('0'), 'cycle': state.cycle + 1}), 'campaign_flat'
    return state, 'reconciled'
