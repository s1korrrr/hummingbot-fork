"""One proposal writer in an existing native SQLite recorder.

This stores targets only, never balances, orders, fill accounting or funding
grants. It does not coordinate another recorder or sharing-wallet execution
owner. Writer/recipe changes require a separate reviewed migration; there is
no timeout takeover, implicit reset or automatic release.

That migration (`carry_forward`) hands the slot to the next writer of the same
owner without deleting anything: the head becomes a carry record naming the
last published target, whose immutable receipt stays in history, and the next
writer's first target must extend that chain.
"""
import json
from typing import Literal

from pydantic import Field, field_validator, model_validator

from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.components.decision_protocol import HASH, NAME
from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.portfolio_v3.contracts import Identified, TargetPortfolio


class ProposalWriter(Identified):
    portfolio_id: str = Field(pattern=NAME)
    account_id: str = Field(pattern=NAME)
    writer_id: str = Field(pattern=NAME)
    epoch: str = Field(pattern=NAME)
    fence: int = Field(ge=1, strict=True)
    mandate_sha256: str = Field(pattern=HASH)
    recipe_sha256: str = Field(pattern=HASH)


class TargetReceipt(Identified):
    schema_version: Literal['native_target_receipt_v3_1'] = 'native_target_receipt_v3_1'
    writer: ProposalWriter
    target: TargetPortfolio
    target_sha256: str = Field(pattern=HASH)
    previous_sha256: str | None = Field(default=None, pattern=HASH)
    published_at_ms: int = Field(ge=0, strict=True)
    scope: Literal['RECORDER_LOCAL_PROPOSAL_ONLY'] = 'RECORDER_LOCAL_PROPOSAL_ONLY'
    execution_authorized: Literal[False] = False

    @field_validator('execution_authorized', mode='before')
    @classmethod
    def no_authority(cls, value):
        if value is not False:
            raise ValueError('proposal receipt cannot grant execution')
        return value

    @model_validator(mode='after')
    def bound_receipt(self):
        if (self.target.identity != self.target_sha256 or self.target.recipe_sha256 != self.writer.recipe_sha256
                or self.target.mandate_sha256 != self.writer.mandate_sha256
                or self.target.portfolio_id != self.writer.portfolio_id
                or self.target.epoch != self.writer.epoch or self.target.fence != self.writer.fence
                or not self.target.decision_time_ms <= self.published_at_ms < self.target.expires_at_ms):
            raise ValueError('native proposal receipt identity or lifetime differs')
        return self


class TargetCarry(Identified):
    """Head of a slot handed to `writer` by an explicit identity migration.

    `carried_writer` published `target_sha256`; that receipt stays in history.
    """
    schema_version: Literal['native_target_carry_v3_1'] = 'native_target_carry_v3_1'
    writer: ProposalWriter
    carried_writer: ProposalWriter
    target_sha256: str = Field(pattern=HASH)
    sequence: int = Field(ge=0, strict=True)
    decision_time_ms: int = Field(ge=0, strict=True)
    published_at_ms: int = Field(ge=0, strict=True)
    migration_sha256: str = Field(pattern=HASH)
    scope: Literal['RECORDER_LOCAL_PROPOSAL_ONLY'] = 'RECORDER_LOCAL_PROPOSAL_ONLY'
    execution_authorized: Literal[False] = False

    @field_validator('execution_authorized', mode='before')
    @classmethod
    def no_authority(cls, value):
        if value is not False:
            raise ValueError('a carried proposal head cannot grant execution')
        return value

    @model_validator(mode='after')
    def same_owner(self):
        mine, theirs = self.writer, self.carried_writer
        if (mine == theirs or (mine.portfolio_id, mine.account_id, mine.writer_id)
                != (theirs.portfolio_id, theirs.account_id, theirs.writer_id)):
            raise ValueError('a proposal slot is carried only between identities of one writer')
        return self


def _chain(value):
    """(target_sha256, sequence, decision_time_ms, published_at_ms) of a decoded head."""
    if value.get('schema_version') == 'native_target_carry_v3_1':
        return value['target_sha256'], value['sequence'], value['decision_time_ms'], value['published_at_ms']
    return (value['target_sha256'], value['target']['sequence'], value['target']['decision_time_ms'],
            value['published_at_ms'])


class NativeTargetStore:
    def __init__(self, session_factory, writer: ProposalWriter):
        self._sessions = session_factory
        self.writer = ProposalWriter.model_validate(writer.model_dump(mode='json'))
        self.key = 'portfolio-target:v3:' + digest([self.writer.account_id, self.writer.portfolio_id])

    def _decode(self, session, row, writer=None):
        writer = self.writer if writer is None else writer
        if json.loads(row.value).get('schema_version') == 'native_target_carry_v3_1':
            carry = TargetCarry.model_validate_json(row.value)
            if carry.writer != writer:
                raise ValueError('another writer or recipe owns this recorder proposal slot')
            history = session.get(Metadata, self.key + ':receipt:' + carry.target_sha256)
            receipt = None if history is None else TargetReceipt.model_validate_json(history.value)
            if (receipt is None or receipt.writer != carry.carried_writer
                    or _chain(receipt.model_dump(mode='json')) != _chain(carry.model_dump(mode='json'))):
                raise ValueError('carried native proposal head and history differ')
            return carry.model_dump(mode='json')
        receipt = TargetReceipt.model_validate_json(row.value)
        if receipt.writer != writer:
            raise ValueError('another writer or recipe owns this recorder proposal slot')
        history = session.get(Metadata, self.key + ':receipt:' + receipt.target_sha256)
        if history is None or history.value != row.value:
            raise ValueError('native proposal history and head differ')
        return receipt.model_dump(mode='json')

    def publish(self, target: TargetPortfolio, *, previous_sha256: str | None, now_ms: int):
        target = TargetPortfolio.model_validate(target.model_dump(mode='json'))
        if type(now_ms) is not int or not target.decision_time_ms <= now_ms < target.expires_at_ms:
            raise ValueError('target is expired or publish clock is invalid')
        if (target.portfolio_id != self.writer.portfolio_id or target.mandate_sha256 != self.writer.mandate_sha256
                or target.recipe_sha256 != self.writer.recipe_sha256
                or target.epoch != self.writer.epoch or target.fence != self.writer.fence):
            raise ValueError('target writer scope differs')
        with self._sessions() as session, session.begin():
            if session.bind.dialect.name != 'sqlite':
                raise ValueError('only the native SQLite recorder target adapter is supported')
            session.connection().exec_driver_sql('BEGIN IMMEDIATE')
            row = session.get(Metadata, self.key)
            prior = None if row is None else self._decode(session, row)
            if prior is not None:
                if prior['writer'] != self.writer.model_dump(mode='json'):
                    raise ValueError('another writer or recipe owns this recorder proposal slot')
                last_sha256, last_sequence, last_decision_ms, last_published_ms = _chain(prior)
                if now_ms < last_published_ms:
                    raise ValueError('publish clock regressed')
                if last_sha256 == target.identity:
                    return prior  # Idempotent exact replay; no renewed lifetime.
                if (previous_sha256 != last_sha256 or target.sequence < last_sequence
                        or target.decision_time_ms <= last_decision_ms):
                    raise ValueError('stale proposal compare-and-swap or snapshot sequence')
            elif previous_sha256 is not None:
                raise ValueError('restore references a missing native proposal')
            receipt = TargetReceipt(writer=self.writer, target=target, target_sha256=target.identity,
                                    previous_sha256=previous_sha256, published_at_ms=now_ms).model_dump(mode='json')
            encoded = json.dumps(receipt, sort_keys=True, separators=(',', ':'))
            session.add(Metadata(key=self.key + ':receipt:' + target.identity, value=encoded))
            if row is None:
                session.add(Metadata(key=self.key, value=encoded))
            else:
                row.value = encoded
            return receipt

    def read(self):
        """The head: a published receipt, or the carry record of an identity migration."""
        with self._sessions() as session:
            row = session.get(Metadata, self.key)
            return None if row is None else self._decode(session, row)

    def head(self):
        """The chain position the next target must extend, or None for an empty slot."""
        value = self.read()
        if value is None:
            return None
        sha256, sequence, decision_ms, published_ms = _chain(value)
        return {'target_sha256': sha256, 'sequence': sequence, 'decision_time_ms': decision_ms,
                'published_at_ms': published_ms}

    def carry_forward(self, session, previous: ProposalWriter, *, migration_sha256: str):
        """Hand this slot from `previous` to this writer inside the caller's transaction.

        Only for an explicit, verified identity migration of the same writer. The
        previous head must decode under `previous`; nothing is deleted. Returns the
        (old, new) head values, or None for an empty slot.
        """
        previous = ProposalWriter.model_validate(previous.model_dump(mode='json'))
        row = session.get(Metadata, self.key)
        if row is None:
            return None
        prior = self._decode(session, row, previous)
        sha256, sequence, decision_ms, published_ms = _chain(prior)
        carried = prior['carried_writer'] if 'carried_writer' in prior else prior['writer']
        if carried == self.writer.model_dump(mode='json'):
            # Back to the identity that published the head target: its own receipt is the head again.
            old, row.value = row.value, session.get(Metadata, self.key + ':receipt:' + sha256).value
            self._decode(session, row)
            return old, row.value
        carry = TargetCarry(writer=self.writer, carried_writer=carried, target_sha256=sha256, sequence=sequence,
                            decision_time_ms=decision_ms, published_at_ms=published_ms,
                            migration_sha256=migration_sha256)
        old, row.value = row.value, json.dumps(carry.model_dump(mode='json'), sort_keys=True, separators=(',', ':'))
        self._decode(session, row)
        return old, row.value
