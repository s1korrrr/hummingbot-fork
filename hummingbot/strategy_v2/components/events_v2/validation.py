"""Pure event-contract checks. No I/O, credentials, eligibility or trading authority."""
from hummingbot.strategy_v2.components.events_v2.contracts import EventEvidenceV2, QualifiedEventSnapshotV2, utc_ms
from hummingbot.strategy_v2.components.strict_json import strict_json


def validate_preexisting_market(*, first_public_ms: int, listed_ms: int) -> None:
    utc_ms(first_public_ms, 'first_public_ms')
    utc_ms(listed_ms, 'listed_ms')
    if listed_ms >= first_public_ms:
        raise ValueError('OKX_MARKET_NOT_PREEXISTING')


def validate_strict_payload(raw: str):
    if type(raw) is not str or not raw:
        raise ValueError('event payload must be nonempty text')
    return strict_json(raw)


def validate_freshness(evidence: EventEvidenceV2, *, asof_ms: int, max_age_ms: int) -> None:
    utc_ms(asof_ms, 'asof_ms')
    if type(max_age_ms) is not int or not 1 <= max_age_ms <= 86_400_000:
        raise ValueError('freshness bound must be UTC milliseconds')
    if evidence.event_available_ms > asof_ms:
        raise ValueError('label or mapping is not yet known')
    if asof_ms - evidence.received_ms > max_age_ms:
        raise ValueError('event receipt is stale relative to the admitted freshness bound')


def validate_generation(*, generation: int, admitted_generation: int) -> None:
    if type(generation) is not int or type(admitted_generation) is not int:
        raise ValueError('generation must be an integer')
    if generation < 1 or admitted_generation < 1:
        raise ValueError('generation must be positive')
    if generation > admitted_generation:
        raise ValueError('FUTURE_GENERATION')


def validate_snapshot_sequence(previous: QualifiedEventSnapshotV2 | None, current: QualifiedEventSnapshotV2) -> None:
    current = QualifiedEventSnapshotV2.model_validate(current.model_dump(mode='json'))
    if previous is None:
        if current.sequence != 1:
            raise ValueError('the first snapshot must use sequence 1')
        return
    previous = QualifiedEventSnapshotV2.model_validate(previous.model_dump(mode='json'))
    if current.sequence != previous.sequence + 1:
        raise ValueError('snapshot sequence must increase by one')
    if current.predecessor_sha256 != previous.dataset_sha256:
        raise ValueError('predecessor hash does not match the prior snapshot')
    if current.asof_ms < previous.asof_ms:
        raise ValueError('snapshot clock regressed')
    if current.generation < previous.generation:
        raise ValueError('snapshot generation regressed')
