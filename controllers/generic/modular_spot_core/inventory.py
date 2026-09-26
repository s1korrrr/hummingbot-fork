"""Read-only native receipts. No fill ledger or wallet-inventory adoption."""
from dataclasses import dataclass
from decimal import Decimal

from .domain import PolicyError, decimal


@dataclass(frozen=True)
class InventoryView:
    remaining_base: Decimal
    entry_quote: Decimal
    entry_price: Decimal
    sold_base: Decimal
    terminal: bool
    entry_terminal: bool
    held: bool
    planned_quote: Decimal = Decimal('0')  # config amount * entry price; capital a live tranche may still use


def inventory_view(executors, positions, *, controller_id: str, connector: str, pair: str, bag_mode: bool = False):
    """Owned BUY tranche receipts. In bag mode the controller's held position is its bag
    (read separately) and its own recovery SELL executors are not tranches."""
    from .bag import is_recovery_sell

    result = {}
    for position in positions:
        if decimal(getattr(position, 'amount', None), 'held amount') <= 0:
            continue
        if not bag_mode:
            raise PolicyError('held_inventory_requires_reconciliation')
        native_side = getattr(position, 'side', None)
        side = getattr(native_side, 'name', native_side)
        if (getattr(position, 'connector_name', None) != connector
                or getattr(position, 'trading_pair', None) != pair or side != 'BUY'):
            raise PolicyError('held inventory scope mismatch')
    for info in executors:
        try:
            config = info.config
            if bag_mode and is_recovery_sell(config):
                if (info.controller_id != controller_id or config.connector_name != connector
                        or config.trading_pair != pair):
                    raise PolicyError('native executor ownership mismatch')
                continue
            if (info.controller_id != controller_id or config.controller_id != controller_id
                    or config.connector_name != connector or config.trading_pair != pair
                    or config.side.name != 'BUY' or not str(config.level_id).startswith('modular_spot:v1:')):
                raise PolicyError('native executor ownership mismatch')
            if info.id in result or not isinstance(info.id, str) or not info.id:
                raise PolicyError('duplicate or missing native executor identity')
            receipt = info.custom_info['execution_receipt']
            remaining = decimal(receipt['remaining_owned_base'], 'remaining base')
            spent = decimal(receipt['entry_gross_quote'], 'entry quote')
            sold = decimal(receipt['exit_gross_base'], 'sold base')
            entry_price = decimal(info.custom_info['entry_price'], 'entry price', positive=True)
            owner = receipt['accounting_owner']
            lifecycle = info.custom_info['entry_lifecycle']['state']
            if owner not in {'executor', 'held_position'} or type(info.is_done) is not bool:
                raise PolicyError('invalid native accounting owner/status')
            if remaining > 0 and spent <= 0:
                raise PolicyError('inventory without entry cost')
            result[info.id] = InventoryView(
                remaining, spent, entry_price, sold, info.is_done,
                info.is_done or lifecycle in {'managing_fill', 'ended_no_fill', 'held_residual'},
                owner == 'held_position' and remaining > 0,
                (decimal(config.amount, 'planned amount') * decimal(config.entry_price, 'planned price'))
                if bag_mode else Decimal('0'),
            )
        except (KeyError, TypeError, AttributeError) as exc:
            raise PolicyError('native execution receipt unavailable') from exc
    return result
