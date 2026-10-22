"""Pure event-policy contract. Decisions are research observations, never orders."""
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

SCHEMA = 'cross_venue_event.snapshot.v1'
HOSTS = {
    'BINANCE': ('www.binance.com', 'binance.com', 'api.binance.com'),
    'COINBASE': ('www.coinbase.com', 'coinbase.com', 'api.exchange.coinbase.com', 'ws-feed.exchange.coinbase.com'),
    'OKX': ('www.okx.com', 'okx.com', 'my.okx.com', 'eea.okx.com', 'ws.okx.com'),
    'HYPERLIQUID': ('api.hyperliquid.xyz', 'app.hyperliquid.xyz'),
    'BITHUMB': ('feed.bithumb.com', 'www.bithumb.com'),
    'UPBIT': ('upbit.com', 'www.upbit.com'),
}
KINDS = frozenset(('SPOT_LISTING_ANNOUNCED', 'ROADMAP', 'TRADING_LAUNCH', 'PERP_LISTING',
                   'PAIR_REMOVAL', 'TOKEN_DELISTING', 'SECURITY_INCIDENT', 'TRADING_HALT',
                   'RISK_WARNING', 'MIGRATION', 'STATUS', 'OTHER'))
NEGATIVE = frozenset(('TOKEN_DELISTING', 'SECURITY_INCIDENT', 'TRADING_HALT', 'RISK_WARNING'))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False,
                      default=lambda v: str(v) if isinstance(v, Decimal) else asdict(v))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def timestamp(value, name):
    if type(value) is not int or not 946684800000 <= value < 4102444800000:
        raise ValueError(f'{name}: expected positive UTC milliseconds')


def sha(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('expected lowercase SHA256')


def number(value, name, minimum=None):
    if isinstance(value, bool):
        raise ValueError(f'{name}: boolean is not a number')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f'{name}: invalid number') from None
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise ValueError(f'{name}: invalid range')
    return result


@dataclass(frozen=True)
class Event:
    source: str
    source_event_id: str
    asset_id: str
    source_url: str
    raw_sha256: str
    kind: str
    published_ms: int | None
    received_ms: int
    parsed_ms: int
    label_known_ms: int
    identity_known_ms: int
    novel: bool = False
    revision: bool = False

    def __post_init__(self):
        u = urlparse(self.source_url)
        if self.source not in HOSTS or u.scheme not in ('https', 'wss') or u.hostname not in HOSTS[self.source]:
            raise ValueError('untrusted issuer URL')
        if u.username or u.password or u.port not in (None, 443) or u.query or u.fragment:
            raise ValueError('issuer URL must not carry credentials, query or fragment')
        if not isinstance(self.source_event_id, str) or not 1 <= len(self.source_event_id) <= 256:
            raise ValueError('invalid source event identity')
        if not isinstance(self.asset_id, str) or ':' not in self.asset_id or '/' not in self.asset_id or len(self.asset_id) > 256:
            raise ValueError('canonical network/asset identity required, not ticker')
        sha(self.raw_sha256)
        if self.kind not in KINDS or type(self.novel) is not bool or type(self.revision) is not bool:
            raise ValueError('invalid event classification')
        for name in ('received_ms', 'parsed_ms', 'label_known_ms', 'identity_known_ms'):
            timestamp(getattr(self, name), name)
        if self.published_ms is not None:
            timestamp(self.published_ms, 'published_ms')
        if self.parsed_ms < self.received_ms or self.label_known_ms < self.received_ms:
            raise ValueError('parsing/classification cannot predate local receipt')

    @property
    def available_ms(self):
        return max(self.received_ms, self.parsed_ms, self.label_known_ms, self.identity_known_ms)

    @property
    def key(self):
        return digest([self.source, self.source_event_id, self.asset_id])


@dataclass(frozen=True)
class Instrument:
    asset_id: str
    pair: str
    venue: str
    region: str
    capital_model: str
    listed_ms: int
    known_ms: int
    valid_until_ms: int
    account_eligible: bool
    state: str
    evidence_sha256: str

    def __post_init__(self):
        for name in ('listed_ms', 'known_ms', 'valid_until_ms'):
            timestamp(getattr(self, name), name)
        if self.valid_until_ms <= self.known_ms or type(self.account_eligible) is not bool:
            raise ValueError('invalid instrument evidence')
        if not re.fullmatch(r'[A-Z0-9]+-[A-Z0-9]+', self.pair):
            raise ValueError('expected native spot pair')
        sha(self.evidence_sha256)


@dataclass(frozen=True)
class Quote:
    pair: str
    exchange_ms: int
    received_ms: int
    bid: Decimal | str
    ask: Decimal | str
    ask_size: Decimal | str
    reference_mid: Decimal | str
    reference_known_ms: int
    feature_known_ms: int
    buy_imbalance: Decimal | str | None = None
    relative_turnover: Decimal | str | None = None

    def __post_init__(self):
        for name in ('exchange_ms', 'received_ms', 'reference_known_ms', 'feature_known_ms'):
            timestamp(getattr(self, name), name)
        for name in ('bid', 'ask', 'reference_mid'):
            v = number(getattr(self, name), name, Decimal('0'))
            if v == 0:
                raise ValueError(f'{name}: zero price')
            object.__setattr__(self, name, v)
        object.__setattr__(self, 'ask_size', number(self.ask_size, 'ask_size', Decimal('0')))
        if self.bid > self.ask:
            raise ValueError('crossed book')
        for name in ('buy_imbalance', 'relative_turnover'):
            if getattr(self, name) is not None:
                object.__setattr__(self, name, number(getattr(self, name), name))
        if self.buy_imbalance is not None and not -1 <= self.buy_imbalance <= 1:
            raise ValueError('imbalance outside [-1,1]')
        if self.relative_turnover is not None and self.relative_turnover < 0:
            raise ValueError('negative turnover')


@dataclass(frozen=True)
class Policy:
    variant: str = 'delayed_entry'
    delay_ms: int = 60000
    event_ttl_ms: int = 1200000
    max_source_delay_ms: int = 300000
    quote_ttl_ms: int = 2000
    allowed_quotes: tuple[str, ...] = ('USDC', 'EUR')
    max_spread_bps: str = '50'
    max_move_bps: str = '300'
    min_move_bps: str = '5'
    min_imbalance: str = '0.1'
    min_turnover: str = '1.5'

    def __post_init__(self):
        if self.variant not in ('delayed_entry', 'reaction_filter'):
            raise ValueError('unknown preregistered variant')
        for name in ('delay_ms', 'event_ttl_ms', 'max_source_delay_ms', 'quote_ttl_ms'):
            value = getattr(self, name)
            if type(value) is not int or value < 0 or value > 86400000:
                raise ValueError('invalid policy duration')
        if not 0 <= self.delay_ms < self.event_ttl_ms or self.quote_ttl_ms == 0:
            raise ValueError('delay must be shorter than event lifetime')
        if type(self.allowed_quotes) is not tuple or not self.allowed_quotes:
            raise ValueError('immutable quote allowlist required')
        for name in ('max_spread_bps', 'max_move_bps', 'min_move_bps', 'min_imbalance', 'min_turnover'):
            number(getattr(self, name), name, Decimal('0'))
        if number(self.min_move_bps, 'minimum') > number(self.max_move_bps, 'maximum'):
            raise ValueError('inverted move bounds')
        if number(self.min_imbalance, 'imbalance') > 1:
            raise ValueError('invalid imbalance threshold')


def evaluate(event, instrument, quote, policy, now_ms):
    timestamp(now_ms, 'now_ms')
    result = dict(event_key=event.key, asset_id=event.asset_id, event_available_ms=event.available_ms,
                  decision_ms=now_ms, policy_sha256=digest(asdict(policy)), execution_allowed=False)

    def reject(reason):
        return dict(result, reason=reason)

    if event.available_ms > now_ms:
        return reject('EVENT_NOT_AVAILABLE')
    if event.kind in NEGATIVE:
        return reject('NEGATIVE_EVENT_LATCH')
    if event.kind != 'SPOT_LISTING_ANNOUNCED':
        return reject('EVENT_TYPE_NOT_ADMITTED')
    if event.source == 'OKX':
        return reject('NOT_CROSS_VENUE')
    if not event.novel or event.revision:
        return reject('NOT_NOVEL')
    if event.published_ms is None:
        return reject('PUBLICATION_TIME_UNAVAILABLE')
    if event.published_ms > event.received_ms:
        return reject('SOURCE_CLOCK_FAULT')
    if event.received_ms - event.published_ms > policy.max_source_delay_ms:
        return reject('SOURCE_DELAY_EXCEEDED')
    if now_ms - event.published_ms > policy.event_ttl_ms:
        return reject('EVENT_EXPIRED')
    if now_ms < event.available_ms + policy.delay_ms:
        return reject('WAIT_DELAY')
    if instrument is None:
        return reject('INSTRUMENT_UNAVAILABLE')
    if event.asset_id != instrument.asset_id:
        return reject('ASSET_IDENTITY_MISMATCH')
    if not instrument.known_ms <= now_ms < instrument.valid_until_ms:
        return reject('INSTRUMENT_NOT_AVAILABLE')
    if (instrument.venue, instrument.region, instrument.capital_model, instrument.state) != ('OKX', 'EEA', 'SPOT_CASH', 'live') or not instrument.account_eligible:
        return reject('INSTRUMENT_INELIGIBLE')
    if instrument.listed_ms >= event.received_ms:
        return reject('NOT_PREEXISTING_OKX_MARKET')
    if instrument.pair.split('-')[1] not in policy.allowed_quotes:
        return reject('QUOTE_NOT_ADMITTED')
    if quote is None or quote.pair != instrument.pair:
        return reject('QUOTE_UNAVAILABLE')
    if max(quote.received_ms, quote.exchange_ms, quote.feature_known_ms) > now_ms:
        return reject('MARKET_NOT_AVAILABLE')
    if now_ms - min(quote.received_ms, quote.exchange_ms) > policy.quote_ttl_ms:
        return reject('QUOTE_STALE')
    if quote.reference_known_ms > event.received_ms:
        return reject('REFERENCE_NOT_PRE_EVENT')
    spread = (quote.ask / quote.bid - 1) * 10000
    move = (quote.ask / quote.reference_mid - 1) * 10000
    result.update(spread_bps=str(spread), move_bps=str(move), visible_ask_notional=str(quote.ask * quote.ask_size))
    if spread > Decimal(policy.max_spread_bps):
        return reject('SPREAD_TOO_WIDE')
    if move > Decimal(policy.max_move_bps):
        return reject('MOVE_ALREADY_EXTENDED')
    if quote.ask_size <= 0:
        return reject('NO_VISIBLE_LIQUIDITY')
    if policy.variant == 'reaction_filter':
        if quote.buy_imbalance is None or quote.relative_turnover is None:
            return reject('FEATURES_UNAVAILABLE')
        if move < Decimal(policy.min_move_bps) or quote.buy_imbalance < Decimal(policy.min_imbalance) or quote.relative_turnover < Decimal(policy.min_turnover):
            return reject('REACTION_NOT_CONFIRMED')
    return reject('ELIGIBLE_RESEARCH_ONLY')


def evaluate_batch(events, instruments, quotes, policy, now_ms):
    if max(len(events), len(instruments), len(quotes)) > 10000:
        raise ValueError('snapshot capacity exceeded; do not silently drop events')
    if len({e.key for e in events}) != len(events):
        raise ValueError('duplicate event identity: resolve revisions explicitly')
    by_asset = {i.asset_id: i for i in instruments}
    by_pair = {q.pair: q for q in quotes}
    if len(by_asset) != len(instruments) or len(by_pair) != len(quotes):
        raise ValueError('ambiguous instrument or quote identity')
    blocked = {e.asset_id for e in events if e.available_ms <= now_ms and e.kind in NEGATIVE}
    rows = []
    for event in events:
        instrument = by_asset.get(event.asset_id)
        row = evaluate(event, instrument, by_pair.get(instrument.pair) if instrument else None, policy, now_ms)
        if event.available_ms <= now_ms and event.asset_id in blocked:
            row['reason'] = 'NEGATIVE_EVENT_LATCH'
        rows.append(row)
    return rows


def read_snapshot(path, now_ms, max_age_ms=5000):
    """Read a bounded projection, rejecting stale, partial and malformed evidence."""
    import os
    import stat
    import sys
    from pathlib import Path
    path = Path(path).absolute()
    if sys.platform == 'darwin':
        for name in ('var', 'tmp'):
            alias, real = Path('/') / name, Path('/private') / name
            if path == alias or alias in path.parents:
                if alias.is_symlink() and alias.resolve() == real:
                    path = real.joinpath(*path.parts[2:])
                break
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise OSError('symlink snapshot parent forbidden')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    with os.fdopen(fd, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError('snapshot is not a regular file')
        raw = stream.read(2_000_001)
    if len(raw) > 2_000_000:
        raise ValueError('snapshot exceeds size limit')

    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate JSON key')
            value[key] = item
        return value

    obj = json.loads(raw, object_pairs_hook=unique, parse_constant=lambda x: (_ for _ in ()).throw(ValueError('non-finite JSON')))
    if set(obj) != {'schema', 'generated_ms', 'risk_history_complete', 'events', 'instruments', 'quotes'} or obj['schema'] != SCHEMA:
        raise ValueError('unsupported snapshot contract')
    timestamp(obj['generated_ms'], 'generated_ms')
    if not 0 <= now_ms - obj['generated_ms'] <= max_age_ms or obj['risk_history_complete'] is not True:
        raise ValueError('stale, future or incomplete snapshot')
    for name in ('events', 'instruments', 'quotes'):
        if type(obj[name]) is not list or len(obj[name]) > 10000:
            raise ValueError('invalid snapshot collection')
    return ([Event(**row) for row in obj['events']], [Instrument(**row) for row in obj['instruments']],
            [Quote(**row) for row in obj['quotes']])
