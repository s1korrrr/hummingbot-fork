from decimal import ROUND_CEILING

from .config import TrailingPolicy
from .domain import decimal


def protected_sell_price(native_no_loss_price, policy: TrailingPolicy, price_step):
    """Tighten the native after-cost floor; do not create another fill ledger."""
    native_floor = decimal(native_no_loss_price, 'native after-cost floor', positive=True)
    tick = decimal(price_step, 'price increment', positive=True)
    floor = native_floor * (1 + policy.minimum_profit)
    return (floor / tick).to_integral_value(rounding=ROUND_CEILING) * tick
