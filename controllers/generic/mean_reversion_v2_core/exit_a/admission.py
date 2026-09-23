"""Persist EXIT-A claims on the native Metadata recorder before any order effect."""
import json
from typing import Literal

from pydantic import Field, model_validator
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from hummingbot.strategy_v2.components.config import PolicyModel
from hummingbot.strategy_v2.components.decision_protocol import HASH, DecisionCursor
from hummingbot.strategy_v2.components.domain import PolicyError
from hummingbot.strategy_v2.components.exit_policy import (
    ExitAssessment,
    NativeExitView,
    NativeProfitFloor,
    arbitrate_exit,
)
from hummingbot.strategy_v2.components.recipe_contract import digest

from .contracts import ExitAResultV2

ClaimState = Literal['ADMITTED', 'CANCEL_PENDING', 'LATE_FILL_RECOVERED']
_TRANSITIONS = {
    ('ADMITTED', 'ADMITTED'),
    ('ADMITTED', 'CANCEL_PENDING'),
    ('ADMITTED', 'LATE_FILL_RECOVERED'),
    ('CANCEL_PENDING', 'CANCEL_PENDING'),
    ('CANCEL_PENDING', 'LATE_FILL_RECOVERED'),
    ('LATE_FILL_RECOVERED', 'LATE_FILL_RECOVERED'),
}


class ExitAClaimV2(PolicyModel):
    claim_id: str = Field(pattern=HASH)
    state: ClaimState
    cursor: DecisionCursor
    assessment: ExitAssessment
    receipt_sha256: str = Field(pattern=HASH)
    inventory_released: Literal[False] = False
    fill_receipt_sha256: str | None = Field(default=None, pattern=HASH)

    @model_validator(mode='after')
    def inventory_stays_owned(self):
        if self.inventory_released:
            raise ValueError('optional admission cannot release inventory')
        if self.state == 'LATE_FILL_RECOVERED' and self.fill_receipt_sha256 is None:
            raise ValueError('late-fill recovery requires the native fill receipt')
        if self.state != 'LATE_FILL_RECOVERED' and self.fill_receipt_sha256 is not None:
            raise ValueError('fill receipt is only valid after late-fill recovery')
        if self.assessment.execution_authorized or self.assessment.market_fallback:
            raise ValueError('assessment cannot authorize execution or a market fallback')
        return self


class ExitAAdmissionStore:
    def __init__(self, recorder, *, owner_id: str, metadata_model=None):
        if metadata_model is None:
            from hummingbot.model.metadata import Metadata
            metadata_model = Metadata
        if any(token in owner_id for token in ('ok_rsi', 'rsi_v5', 'rsi_modular')):
            raise PolicyError('EXIT-A admission cannot use a legacy or compatibility owner')
        self.model, self.recorder, self.owner_id = metadata_model, recorder, owner_id
        self.key = 'exit_a_v2:claims:' + owner_id

    def _load_raw(self):
        with self.recorder.sql_manager.get_new_session() as session:
            row = session.get(self.model, self.key)
            return None if row is None else row.value

    def _parse(self, raw):
        if raw is None:
            return {}
        try:
            value = json.loads(raw)
            if (not isinstance(value, dict) or value.get('schema_version') != 1
                    or value.get('owner_id') != self.owner_id or not isinstance(value.get('claims'), list)):
                raise PolicyError('exit-a claim checkpoint invalid')
            claims = {item['claim_id']: ExitAClaimV2.model_validate(item) for item in value['claims']}
        except (ValueError, TypeError, KeyError) as exc:
            raise PolicyError('exit-a claim checkpoint invalid') from exc
        if len(claims) != len(value['claims']):
            raise PolicyError('duplicate exit-a claim identity')
        return claims

    def _encode(self, claims):
        return json.dumps({'schema_version': 1, 'owner_id': self.owner_id,
                           'claims': [claim.model_dump(mode='json') for _, claim in sorted(claims.items())]},
                          sort_keys=True, separators=(',', ':'), allow_nan=False)

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

    def persist(self, claim: ExitAClaimV2) -> ExitAClaimV2:
        claim = ExitAClaimV2.model_validate(claim.model_dump(mode='json'))
        raw = self._load_raw()
        claims = self._parse(raw)
        existing = claims.get(claim.claim_id)
        if existing is not None:
            if (existing.cursor, existing.assessment, existing.receipt_sha256) != (
                    claim.cursor, claim.assessment, claim.receipt_sha256):
                raise PolicyError('immutable exit-a claim identity changed')
            if existing == claim:
                return existing
            if (existing.state, claim.state) not in _TRANSITIONS:
                raise PolicyError('illegal exit-a claim transition')
        elif claim.state != 'ADMITTED':
            raise PolicyError('new claims start admitted')
        claims[claim.claim_id] = claim
        encoded = self._encode(claims)
        if encoded == raw:
            return claim
        if not self._write(raw, encoded):
            raise PolicyError('exit-a claim persist lost the compare-and-swap')
        return claim

    def load(self, claim_id: str) -> ExitAClaimV2 | None:
        return self._parse(self._load_raw()).get(claim_id)


def admit(result: ExitAResultV2, view: NativeExitView, floor: NativeProfitFloor | None,
          store: ExitAAdmissionStore, *, now_ms: int, max_age_ms: int, place=None) -> ExitAClaimV2 | None:
    result = ExitAResultV2.model_validate(result.model_dump(mode='json'))
    if result.execution_authorized:
        raise PolicyError('policy result cannot authorize execution')
    if result.status != 'PROPOSE_REDUCTION' or result.proposal is None or result.cursor is None:
        return None
    assessment = arbitrate_exit(view, (result.proposal,), floor, now_ms=now_ms, max_age_ms=max_age_ms)
    if assessment.status != 'PROPOSED_REDUCTION':
        return None
    claim = ExitAClaimV2(claim_id=digest([result.cursor, assessment, view.receipt_sha256]),
                         state='ADMITTED', cursor=result.cursor, assessment=assessment,
                         receipt_sha256=view.receipt_sha256)
    persisted = store.persist(claim)
    if store.load(claim.claim_id) != persisted:
        raise PolicyError('exit-a claim missing after persist')
    if place is not None:
        place(persisted)
    return persisted


def request_cancel(claim: ExitAClaimV2, store: ExitAAdmissionStore) -> ExitAClaimV2:
    current = store.load(claim.claim_id)
    if current is None or current.claim_id != claim.claim_id:
        raise PolicyError('cancel requires the persisted claim')
    if current.state == 'CANCEL_PENDING':
        return current
    if current.state != 'ADMITTED':
        raise PolicyError('only an admitted claim can be cancelled')
    updated = ExitAClaimV2.model_validate(current.model_dump(mode='json') | {'state': 'CANCEL_PENDING'})
    return store.persist(updated)


def recover_late_fill(claim: ExitAClaimV2, fill_receipt_sha256: str, store: ExitAAdmissionStore) -> ExitAClaimV2:
    current = store.load(claim.claim_id)
    if current is None:
        raise PolicyError('late-fill recovery requires a persisted claim')
    if current.state == 'LATE_FILL_RECOVERED':
        if current.fill_receipt_sha256 != fill_receipt_sha256:
            raise PolicyError('late-fill receipt changed')
        return current
    if current.state not in ('ADMITTED', 'CANCEL_PENDING'):
        raise PolicyError('late-fill recovery is not available')
    updated = ExitAClaimV2.model_validate(current.model_dump(mode='json') | {
        'state': 'LATE_FILL_RECOVERED', 'fill_receipt_sha256': fill_receipt_sha256,
    })
    return store.persist(updated)
