"""In-container driver for the real process-kill matrix.

``drive`` runs the bounded chain in the offline paper session and terminates
the interpreter with ``os._exit`` at one boundary. ``verify`` starts a fresh
process against the same database directory and reports what a restart sees.
Both modes print one JSON line. Nothing here touches live state.
"""
import argparse
import asyncio
import json
import os
import sys
from decimal import Decimal as D
from pathlib import Path

KILL_CODE = 137

PHASES = {
    # after the venue accepted the replacement child, before the executor row was synced
    'replacement_accepted_before_sync': {'expect_step_receipt': True, 'expect_open_orders': 1, 'expect_fills': 0},
    # after the venue applied the first replacement fill (recorder wrote TradeFill), before sync
    'first_fill_before_sync': {'expect_step_receipt': True, 'expect_open_orders': 1, 'expect_fills': 1},
    # after the step that closed the chain fully completed (clean boundary), before any later step
    'closed_chain_between_steps': {'expect_step_receipt': False, 'expect_open_orders': 0, 'expect_fills': 2},
}


def _emit(payload):
    print('KILL_MATRIX ' + json.dumps(payload, sort_keys=True, default=str), flush=True)


async def drive(db, phase):
    from test.acquisition_chain_v2.test_paper_lifecycle_restart import ChainRun, chain_configs, two_pair_paper

    from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import NativePaperTradeExchange
    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.core.data_type.common import TradeType

    MarketsRecorder._shared_instance = None
    connector = two_pair_paper()
    session = __import__('hummingbot.strategy_v2.paper_session', fromlist=['NativePaperSession']).NativePaperSession(
        connector, chain_configs(), db)
    (db.parent / 'snapshots').mkdir(exist_ok=True)
    run = ChainRun(session, db.parent / 'snapshots')
    buy_opens = []
    original_open = NativePaperTradeExchange._open
    original_fill = NativePaperTradeExchange._fill

    async def killing_open(self, order):
        await original_open(self, order)
        if order.trade_type is TradeType.BUY:
            buy_opens.append(order.client_order_id)
            if phase == 'replacement_accepted_before_sync' and len(buy_opens) == 2:
                _emit({'mode': 'drive', 'phase': phase, 'killed_after': 'venue_open', 'order': order.client_order_id})
                os._exit(KILL_CODE)

    async def killing_fill(self, order, amount, price, maker, trade_id):
        await original_fill(self, order, amount, price, maker, trade_id)
        if phase == 'first_fill_before_sync' and order.trade_type is TradeType.BUY:
            _emit({'mode': 'drive', 'phase': phase, 'killed_after': 'venue_fill', 'order': order.client_order_id,
                   'amount': str(amount)})
            os._exit(KILL_CODE)

    NativePaperTradeExchange._open = killing_open
    NativePaperTradeExchange._fill = killing_fill
    await run.run_to_closed_chain()
    if phase == 'closed_chain_between_steps':
        _emit({'mode': 'drive', 'phase': phase, 'killed_after': 'step_complete',
               'chain': run.executor.get_custom_info()['acquisition_chain']})
        os._exit(KILL_CODE)
    raise SystemExit('kill phase never reached: ' + phase)


async def verify(db, phase):
    from test.acquisition_chain_v2.test_paper_lifecycle_restart import books, chain_configs, two_pair_paper

    from hummingbot.connector.markets_recorder import MarketsRecorder
    from hummingbot.core.data_type.common import TradeType
    from hummingbot.strategy_v2.executors.acquisition_chain import validate_acquisition_chain
    from hummingbot.strategy_v2.paper_session import NativePaperSession

    expect = PHASES[phase]
    MarketsRecorder._shared_instance = None
    report = {'mode': 'verify', 'phase': phase, 'checks': {}, 'ok': True}

    def check(name, condition, detail=None):
        report['checks'][name] = bool(condition) if detail is None else {'ok': bool(condition), 'detail': detail}
        if not condition:
            report['ok'] = False

    receipt_path = db.with_suffix(db.suffix + '.paper-step.json')
    check('step_receipt_present_matches_expectation', receipt_path.exists() == expect['expect_step_receipt'],
          {'exists': receipt_path.exists()})
    restored = NativePaperSession(two_pair_paper(), chain_configs(), db, restore=True)
    try:
        connector = restored._connector
        rows = restored.recorder.get_all_executors()
        check('one_executor_row', len(rows) == 1, [row.id for row in rows])
        row = rows[0] if rows else None
        fills = restored.recorder.get_trades_for_config('native-paper-session')
        check('fills_persisted', len(fills) == expect['expect_fills'], len(fills))
        open_buys = [o for o in connector.in_flight_orders.values() if o.trade_type is TradeType.BUY]
        check('open_orders_restored', len(open_buys) == expect['expect_open_orders'],
              [o.client_order_id for o in open_buys])
        orchestrator = restored.executor_orchestrator
        check('recovery_required_latched',
              row is not None and orchestrator.recovery_required.get(row.controller_id) == [row.id],
              orchestrator.recovery_required)
        check('no_active_executor_recreated', all(not v for v in orchestrator.active_executors.values()),
              {k: [e.config.id for e in v] for k, v in orchestrator.active_executors.items()})
        if row is not None:
            chain = row.custom_info.get('acquisition_chain')
            check('chain_persisted', isinstance(chain, dict) and chain.get('parent_id') == row.id, chain)
            if phase == 'closed_chain_between_steps':
                check('chain_closed', chain.get('membership_closed') is True)
                try:
                    validate_acquisition_chain(row.config, row.custom_info, row.custom_info.get('entry_reconciliation'))
                    check('chain_validates_closed', True)
                except ValueError as error:
                    check('chain_validates_closed', False, str(error))
            else:
                # Persisted executor state is older than venue state: it must not claim the fill/order.
                gross = D(chain.get('gross_filled_base', '0')) if isinstance(chain, dict) else None
                venue_gross = sum((o.executed_amount_base for o in connector._order_tracker.all_orders.values()
                                   if o.trade_type is TradeType.BUY), D('0'))
                check('executor_row_not_ahead_of_venue', gross is not None and gross <= venue_gross,
                      {'row_gross': str(gross), 'venue_gross': str(venue_gross)})
        buys_before = len([o for o in connector._order_tracker.all_orders.values() if o.trade_type is TradeType.BUY])
        try:
            receipt = await restored.step(700000, books=books(700000, 90))
            stepped = True
            ready = receipt['readiness']
            actions = receipt['actions']
        except RuntimeError as error:
            stepped = False
            ready = {'ready': False, 'reasons': [str(error)]}
            actions = []
        check('step_held_or_not_ready', not ready['ready'], ready)
        check('held_when_receipt_present', stepped != expect['expect_step_receipt'], {'stepped': stepped})
        check('no_actions', actions == [], actions)
        buys_after = len([o for o in connector._order_tracker.all_orders.values() if o.trade_type is TradeType.BUY])
        check('no_new_buy', buys_after == buys_before, {'before': buys_before, 'after': buys_after})
        check('balances_finite', all(v.is_finite() and v >= 0 for v in connector.get_all_balances().values()),
              {k: str(v) for k, v in connector.get_all_balances().items()})
    finally:
        await restored.close()
    _emit(report)
    return report['ok']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('drive', 'verify'))
    parser.add_argument('--db', required=True)
    parser.add_argument('--phase', required=True, choices=sorted(PHASES))
    args = parser.parse_args()
    db = Path(args.db)
    if args.mode == 'drive':
        asyncio.run(drive(db, args.phase))
        sys.exit(1)
    sys.exit(0 if asyncio.run(verify(db, args.phase)) else 2)


if __name__ == '__main__':
    main()
