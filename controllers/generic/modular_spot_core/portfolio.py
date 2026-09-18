from decimal import ROUND_FLOOR

from .domain import Allocation, PolicyError, decimal


def allocate(*, stage_quote, remaining_quote, available_quote, price, amount_step,
             minimum_amount, minimum_notional, fee_buffer, cash_reserve):
    stage = decimal(stage_quote, 'stage budget')
    remaining = decimal(remaining_quote, 'remaining commitment')
    available = decimal(available_quote, 'available quote')
    reserve = decimal(cash_reserve, 'cash reserve')
    price = decimal(price, 'price', positive=True)
    step = decimal(amount_step, 'amount step', positive=True)
    minimum_amount = decimal(minimum_amount, 'minimum amount', positive=True)
    minimum_notional = decimal(minimum_notional, 'minimum notional')
    fees = decimal(fee_buffer, 'fee reserve')
    if fees >= 1:
        raise PolicyError('fee reserve must be below one')
    budget = min(stage, remaining, max(available - reserve, 0))
    quantity = (budget / (price * (1 + fees)) / step).to_integral_value(rounding=ROUND_FLOOR) * step
    if quantity <= 0 or quantity < minimum_amount or quantity * price < minimum_notional:
        return None
    return Allocation(quantity, quantity * price * (1 + fees))
