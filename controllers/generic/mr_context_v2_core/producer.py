"""Map okx-candle-service cache bytes into ContextSnapshotV2. Not a Market Pulse client."""
from decimal import Decimal, localcontext
from hashlib import sha256
from pathlib import Path

from hummingbot.strategy_v2.components.recipe_contract import digest
from hummingbot.strategy_v2.components.strict_json import strict_json

from .snapshot import ContextSnapshotV2, exact_decimal

PARAMETERS = {
    'weight': 'equal',
    'return_1m_bars': 1,
    'return_5m_bars': 5,
    'vol_bars': 15,
    'volume_bars': 5,
    'ttl_ms': 60000,
}
MAX_PAIRS = 16
MAX_ROWS = 10000
MAX_FILE_BYTES = 2 * 1024 * 1024
SOURCE = 'okx-public-spot'


def _used_content_sha256(members: list[dict]) -> str:
    payload = []
    for item in sorted(members, key=lambda row: row['pair']):
        payload.append({
            'pair': item['pair'],
            'rows': [{'open_time': open_time,
                      'close': str(item['bars'][open_time]['close']),
                      'quote_volume': str(item['bars'][open_time]['quote_volume'])}
                     for open_time in item['opens']],
        })
    return digest(payload)


def implementation_sha256() -> str:
    return sha256(Path(__file__).read_bytes()).hexdigest()


def parameters_sha256() -> str:
    return digest(PARAMETERS)


def snapshot_from_candle_cache(payloads: tuple[bytes, ...], *, now_ms: int,
                               as_of_ms: int | None = None,
                               replica_ready: bool = False) -> ContextSnapshotV2:
    if type(payloads) is not tuple or not 1 <= len(payloads) <= MAX_PAIRS:
        raise ValueError('candle cache needs 1-16 pair payloads')
    if type(now_ms) is not int or now_ms < 0:
        raise ValueError('now_ms must be a UTC millisecond integer')
    if type(replica_ready) is not bool:
        raise ValueError('replica_ready must be a boolean')
    cutoff = now_ms if as_of_ms is None else as_of_ms
    if type(cutoff) is not int or cutoff > now_ms:
        raise ValueError('as-of cannot be in the future')
    members = []
    for raw in payloads:
        if type(raw) is not bytes or not 2 <= len(raw) <= MAX_FILE_BYTES:
            raise ValueError('candle payload exceeds the admitted size')
        members.append(_parse_pair(raw, now_ms))
    pairs = tuple(sorted(item['pair'] for item in members))
    if len(set(pairs)) != len(pairs):
        raise ValueError('duplicate pair in candle cache')
    known = _common_opens(members)
    usable = tuple(open_time for open_time in known if (open_time + 60) * 1000 <= cutoff)
    if not usable:
        raise ValueError('no confirmed common one-minute bar at this as-of')
    bar_open = usable[-1]
    bar_close_ms = (bar_open + 60) * 1000
    observed = (
        replica_ready is True
        and as_of_ms is None
        and bar_open == known[-1]
        and all(item['received_at_ms'] >= bar_close_ms for item in members)
    )
    received_at_ms = max(item['received_at_ms'] for item in members)
    if received_at_ms > now_ms:
        raise ValueError('cache receipt is in the future')
    available_time_ms = max(bar_close_ms, received_at_ms) if observed else bar_close_ms
    clipped = tuple({**item, 'opens': tuple(open_time for open_time in item['opens'] if open_time <= bar_open),
                     'bars': {open_time: item['bars'][open_time] for open_time in item['opens'] if open_time <= bar_open}}
                    for item in members)
    components = _components(clipped, usable)
    dataset = digest({'source': SOURCE, 'interval': '1m', 'pairs': list(pairs)})
    universe = digest({'pairs': list(pairs)})
    with localcontext() as ctx:
        ctx.prec = 80
        return ContextSnapshotV2(
            source='okx', market_type='spot', quote_asset='USDC', interval='1m',
            dataset_sha256=dataset, epoch='okx-public-spot-1m', revision=1,
            implementation_sha256=implementation_sha256(),
            parameters_sha256=parameters_sha256(),
            initialization_sha256=digest('closed-bar-complete-window'),
            source_content_sha256=_used_content_sha256(clipped),
            universe_sha256=universe, universe_revision=1,
            bar_open_ms=bar_open * 1000, bar_close_ms=bar_close_ms, confirmed=True,
            available_time_ms=available_time_ms, received_at_ms=received_at_ms,
            expires_at_ms=available_time_ms + PARAMETERS['ttl_ms'],
            expected_members=len(members), ready_members=len(members),
            fresh_members=len(members) if observed else 0,
            history_bars=len(usable), missing_required=(),
            evidence_kind='observed' if observed else 'reconstructed',
            components=components,
        )


def _parse_pair(raw: bytes, cutoff_ms: int) -> dict:
    data = strict_json(raw.decode('utf-8'))
    if not isinstance(data, dict):
        raise ValueError('candle cache must be an object')
    if data.get('v') != 1 or data.get('source') != SOURCE or data.get('interval') != '1m':
        raise ValueError('unsupported candle cache source or interval')
    pair = data.get('pair')
    if type(pair) is not str or not pair.endswith('-USDC'):
        raise ValueError('candle cache pair must be OKX USDC spot')
    rows = data.get('rows')
    if type(rows) is not list or not 1 <= len(rows) <= MAX_ROWS:
        raise ValueError('candle cache rows are missing or oversized')
    bars = {}
    for row in rows:
        if not isinstance(row, dict) or row.get('confirmed') is not True:
            continue
        open_time = row.get('open_time')
        if type(open_time) is not int or open_time % 60 or open_time < 0:
            raise ValueError('candle open_time must be an aligned unix second')
        close_ms = (open_time + 60) * 1000
        if close_ms > cutoff_ms:
            continue
        bars[open_time] = {
            'close': _price(row.get('close')),
            'quote_volume': _price(row.get('quote_volume')),
        }
    opens = tuple(sorted(bars))
    return {'pair': pair, 'bars': bars, 'opens': opens, 'received_at_ms': _receipt_ms(data.get('saved_at'))}


def _receipt_ms(value) -> int:
    if isinstance(value, bool) or value is None:
        raise ValueError('invalid cache receipt time')
    if type(value) is int:
        return value * 1000
    return int(Decimal(str(value)) * 1000)


def _price(value) -> Decimal:
    return Decimal(str(exact_decimal(value)))


def _common_opens(members: list[dict]) -> tuple[int, ...]:
    shared = None
    for item in members:
        keys = set(item['opens'])
        shared = keys if shared is None else shared & keys
    if not shared:
        return ()
    ordered = tuple(sorted(shared))
    trailing = [ordered[-1]]
    for previous, current in zip(reversed(ordered[:-1]), reversed(ordered[1:])):
        if current - previous != 60:
            break
        trailing.append(previous)
    return tuple(reversed(trailing))


def _components(members: list[dict], common: tuple[int, ...]) -> dict:
    with localcontext() as ctx:
        ctx.prec = 80
        end = common[-1]
        series = []
        for open_time in common:
            series.append(_mean_return(members, open_time - 60, open_time))
        valid = [item for item in series if item is not None]
        vol = _stdev(valid[-PARAMETERS['vol_bars']:])
        return {
            'return_1m_fraction': _fmt(_mean_return(members, end - 60, end)),
            'return_5m_fraction': _fmt(_mean_return(members, end - 60 * PARAMETERS['return_5m_bars'], end)),
            'breadth_fraction': _fmt(_breadth(members, end)),
            'quote_volume_5m': _fmt(_quote_volume(members, common[-PARAMETERS['volume_bars']:])),
            'realized_vol_15m_fraction': _fmt(vol),
        }


def _mean_return(members: list[dict], start: int, end: int):
    returns = []
    for item in members:
        bars = item['bars']
        if start not in bars or end not in bars:
            return None
        first = bars[start]['close']
        if first <= 0:
            raise ValueError('non-positive close cannot form a return')
        returns.append(bars[end]['close'] / first - 1)
    return sum(returns) / len(returns)


def _stdev(window: list):
    if len(window) < 2 or any(item is None for item in window):
        return None
    mean = sum(window) / len(window)
    return (sum((item - mean) ** 2 for item in window) / (len(window) - 1)).sqrt()


def _breadth(members: list[dict], open_time: int):
    counted = []
    for item in members:
        bars = item['bars']
        prev = open_time - 60
        start = open_time - 60 * PARAMETERS['return_5m_bars']
        if start not in bars or open_time not in bars or prev not in bars:
            continue
        first = bars[start]['close']
        if first <= 0:
            raise ValueError('non-positive close cannot form breadth')
        counted.append(bars[open_time]['close'] / first - 1 > 0)
    if not counted:
        return None
    return Decimal(sum(counted)) / len(counted)


def _quote_volume(members: list[dict], opens: tuple[int, ...]):
    total = Decimal('0')
    for open_time in opens:
        for item in members:
            bar = item['bars'].get(open_time)
            if bar is None:
                return None
            total += bar['quote_volume']
    return total


def _fmt(value):
    if value is None:
        return None
    text = format(value.quantize(Decimal('1e-18')), 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text or '0'
