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


def inventory_view(executors, positions, *, controller_id: str, connector: str, pair: str):
    result = {}
    for position in positions:
        if decimal(position.amount, 'held amount') > 0:
            raise PolicyError('held_inventory_requires_reconciliation')
    for info in executors:
        try:
            config = info.config
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
            )
        except (KeyError, TypeError, AttributeError) as exc:
            raise PolicyError('native execution receipt unavailable') from exc
    return result
