"""Exercise actual stepping methods without claiming compiled engine integration.

The methods are read from production source, not copied implementations. Fixtures
supply executor transitions and awaitables only; they do not simulate fills,
accounting, recorder persistence or the NativePaperSession constructor.
"""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest


def stepping_class():
    path = Path(__file__).resolve().parents[2] / 'hummingbot/strategy_v2/paper_session.py'
    tree = ast.parse(path.read_text(), filename=str(path))
    owner = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'NativePaperSession')
    names = {'_control_open_executors', '_drive_executors'}
    methods = [node for node in owner.body if isinstance(node, ast.AsyncFunctionDef) and node.name in names]
    if {node.name for node in methods} != names:
        raise AssertionError('Expected actual native stepping methods')
    selected = ast.Module(body=methods, type_ignores=[])
    namespace = {'asyncio': asyncio}
    exec(compile(ast.fix_missing_locations(selected), str(path), 'exec'), namespace)
    return type('SteppingMethods', (), {name: namespace[name] for name in names})


class Connector:
    def __init__(self):
        self.drains = 0

    async def drain(self):
        self.drains += 1


class Executor:
    def __init__(self, name, events, reconcile=None, two_tasks=False):
        self.name, self.events, self.reconcile = name, events, reconcile
        self.two_tasks = two_tasks
        self.passes = 0
        self.is_closed = False
        self._entry_expiry_reconciliation_task = None
        self._funding_entry_reconciliation_task = None

    async def control_task(self):
        self.passes += 1
        self.events.append(self.name)
        if self.passes == 1 and self.reconcile is not None:
            self._entry_expiry_reconciliation_task = asyncio.create_task(self.reconcile())
            if self.two_tasks:
                self._funding_entry_reconciliation_task = asyncio.create_task(self.reconcile())


class ReconciliationFairness(unittest.IsolatedAsyncioTestCase):
    def session(self, groups):
        session = stepping_class()()
        session.executor_orchestrator = SimpleNamespace(active_executors=groups)
        session._connector = Connector()
        return session

    async def settle(self):
        await asyncio.sleep(0)

    async def test_no_pending_task_does_not_add_a_control_pass(self):
        events = []
        first, second = Executor('first', events), Executor('second', events)
        session = self.session({'a': [first], 'b': [second]})
        await session._drive_executors({'a', 'b'})
        self.assertEqual(events, ['first', 'second'])
        self.assertEqual(session._connector.drains, 2)

    async def test_unrelated_executors_do_not_advance_again(self):
        events = []
        owner = Executor('owner', events, self.settle)
        sibling, other = Executor('sibling', events), Executor('other', events)
        session = self.session({'a': [owner, sibling], 'b': [other]})
        await session._drive_executors({'a', 'b'})
        self.assertEqual(events, ['owner', 'sibling', 'other', 'owner'])
        self.assertEqual((owner.passes, sibling.passes, other.passes), (2, 1, 1))
        self.assertEqual(session._connector.drains, 4)

    async def test_two_tasks_for_one_executor_cause_only_one_followup(self):
        events = []
        owner = Executor('owner', events, self.settle, two_tasks=True)
        other = Executor('other', events)
        session = self.session({'a': [owner, other]})
        await session._drive_executors({'a'})
        self.assertEqual(events, ['owner', 'other', 'owner'])
        self.assertTrue(owner._entry_expiry_reconciliation_task.done())
        self.assertTrue(owner._funding_entry_reconciliation_task.done())

    async def test_multiple_owners_preserve_original_traversal_order(self):
        events = []
        first, second = Executor('first', events, self.settle), Executor('second', events, self.settle)
        other = Executor('other', events)
        session = self.session({'a': [first, other], 'b': [second]})
        await session._drive_executors({'a', 'b'})
        self.assertEqual(events, ['first', 'other', 'second', 'first', 'second'])

    async def test_closed_owner_is_not_driven_after_await(self):
        events = []
        owner, other = Executor('owner', events), Executor('other', events)

        async def close_owner():
            await self.settle()
            owner.is_closed = True

        owner.reconcile = close_owner
        session = self.session({'a': [owner, other]})
        await session._drive_executors({'a'})
        self.assertEqual(events, ['owner', 'other'])

    async def test_removed_owner_and_replacement_do_not_get_a_followup(self):
        events = []
        owner, other = Executor('owner', events), Executor('other', events)
        replacement = Executor('replacement', events)
        session = self.session({'a': [owner, other]})

        async def replace_owner():
            await self.settle()
            session.executor_orchestrator.active_executors['a'] = [replacement, other]

        owner.reconcile = replace_owner
        await session._drive_executors({'a'})
        self.assertEqual(events, ['owner', 'other'])
        self.assertEqual(replacement.passes, 0)

    async def test_removed_controller_is_not_recreated(self):
        events = []
        owner = Executor('owner', events)
        other = Executor('other', events)
        session = self.session({'a': [owner], 'b': [other]})

        async def remove_controller():
            await self.settle()
            del session.executor_orchestrator.active_executors['a']

        owner.reconcile = remove_controller
        await session._drive_executors({'a', 'b'})
        self.assertEqual(events, ['owner', 'other'])
        self.assertNotIn('a', session.executor_orchestrator.active_executors)

    async def test_failed_reconciliation_remains_for_its_owner_to_handle(self):
        events = []

        async def fail():
            await self.settle()
            raise RuntimeError('synthetic reconciliation failure')

        owner, other = Executor('owner', events, fail), Executor('other', events)
        session = self.session({'a': [owner, other]})
        await session._drive_executors({'a'})
        self.assertEqual(events, ['owner', 'other', 'owner'])
        self.assertIsInstance(owner._entry_expiry_reconciliation_task.exception(), RuntimeError)

    async def test_stale_and_already_closed_executors_keep_existing_gates(self):
        events = []
        owner = Executor('owner', events, self.settle)
        stale, closed = Executor('stale', events), Executor('closed', events)
        closed.is_closed = True
        session = self.session({'a': [owner, closed], 'stale': [stale]})
        await session._drive_executors({'a'})
        self.assertEqual(events, ['owner', 'owner'])
        self.assertEqual((stale.passes, closed.passes), (0, 0))

    async def test_cancellation_propagates_without_a_second_pass(self):
        events, started = [], asyncio.Event()

        async def wait():
            started.set()
            await asyncio.Event().wait()

        owner, other = Executor('owner', events, wait), Executor('other', events)
        session = self.session({'a': [owner, other]})
        driving = asyncio.create_task(session._drive_executors({'a'}))
        await started.wait()
        driving.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await driving
        self.assertEqual(events, ['owner', 'other'])
        self.assertTrue(owner._entry_expiry_reconciliation_task.cancelled())


if __name__ == '__main__':
    unittest.main()
