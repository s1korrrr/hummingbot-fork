from .config import AcquisitionPolicy
from .domain import PolicyError, decimal


def next_stage(policy: AcquisitionPolicy, *, stage, bid, last_fill_price, fresh_signal,
               entry_terminal, distribution_started):
    if type(stage) is not int or stage < 0:
        raise PolicyError('invalid acquisition stage')
    if any(type(x) is not bool for x in (fresh_signal, entry_terminal, distribution_started)):
        raise PolicyError('invalid acquisition evidence')
    price = decimal(bid, 'bid', positive=True)
    if stage >= len(policy.fractions) or not fresh_signal or not entry_terminal or distribution_started:
        return None
    if stage:
        reference = decimal(last_fill_price, 'last actual fill price', positive=True)
        if price > reference * (1 - policy.minimum_drop):
            return None
    return policy.fractions[stage]
