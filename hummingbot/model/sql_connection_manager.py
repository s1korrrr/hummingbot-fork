import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from os.path import join
from typing import TYPE_CHECKING, Any, Callable, Optional

from sqlalchemy import MetaData, create_engine, event, inspect
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Query, Session, sessionmaker
from sqlalchemy.schema import DropConstraint, ForeignKeyConstraint, Table

from hummingbot import data_path
from hummingbot.logger.logger import HummingbotLogger
from hummingbot.model import get_declarative_base
from hummingbot.model.metadata import Metadata as LocalMetadata
from hummingbot.model.transaction_base import TransactionBase

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class SQLConnectionType(Enum):
    TRADE_FILLS = 1


@dataclass
class _SQLWriteRequest:
    operation: Callable[[], Any]
    completion: asyncio.Future


class SQLWriteQueueFull(RuntimeError):
    """A bounded SQL writer cannot accept another producer right now."""


class SQLWriteCancelledAfterCommit(asyncio.CancelledError):
    """The caller was cancelled after an admitted write reached commit."""

    def __init__(self, result: Any):
        super().__init__("SQL write committed before caller cancellation")
        self.result = result


class BoundedSQLWriteQueue:
    """Run acknowledged database write operations serially off the event loop.

    The queue retains at most ``max_pending`` queued operations, one running
    operation, and ``max_waiters`` producers waiting for admission. Further
    submissions fail before acceptance with ``SQLWriteQueueFull``. Callers must
    pass operations that use only database state and immutable inputs; collect
    mutable runtime state before submission. A successful return means the
    operation itself returned after its database commit.
    """

    def __init__(self, max_pending: int = 1, max_waiters: int = 1,
                 thread_name_prefix: str = "hummingbot-sql-write"):
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be a positive integer")
        if type(max_waiters) is not int or max_waiters < 0:
            raise ValueError("max_waiters must be a non-negative integer")
        self._max_pending = max_pending
        self._max_waiters = max_waiters
        self._thread_name_prefix = thread_name_prefix
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: Optional[asyncio.Queue] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._worker: Optional[asyncio.Task] = None
        self._admissions_idle: Optional[asyncio.Event] = None
        self._active_admissions = 0
        self._waiting_admissions = 0
        self._closing = False
        self._closed = False
        self._close_task: Optional[asyncio.Task] = None

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
            self._queue = asyncio.Queue(maxsize=self._max_pending)
            self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=self._thread_name_prefix)
            self._admissions_idle = asyncio.Event()
            self._admissions_idle.set()
            self._worker = loop.create_task(self._run())
        elif self._loop is not loop:
            raise RuntimeError("SQL write queue is bound to a different event loop")
        return loop

    async def write(self, operation: Callable[[], Any]) -> Any:
        """Submit a database operation and return only after it completes."""
        if not callable(operation):
            raise TypeError("SQL write operation must be callable")
        loop = self._bind_loop()
        if self._closing or self._closed:
            raise RuntimeError("SQL write queue is closing or closed")
        completion = loop.create_future()
        request = _SQLWriteRequest(operation=operation, completion=completion)
        self._active_admissions += 1
        assert self._admissions_idle is not None
        self._admissions_idle.clear()
        admitted = False
        try:
            assert self._queue is not None
            if self._queue.full():
                if self._waiting_admissions >= self._max_waiters:
                    raise SQLWriteQueueFull("SQL write queue and bounded admission waiters are full")
                self._waiting_admissions += 1
                try:
                    await self._queue.put(request)
                finally:
                    self._waiting_admissions -= 1
            else:
                await self._queue.put(request)
            admitted = True
        finally:
            self._active_admissions -= 1
            if self._active_admissions == 0:
                self._admissions_idle.set()
            if not admitted and not completion.done():
                completion.cancel()

        cancelled = False
        while not completion.done():
            try:
                await asyncio.shield(completion)
            except asyncio.CancelledError:
                cancelled = True
                # Once admitted, a write must reach a known durable outcome
                # before cancellation is returned to the caller. Repeated
                # cancellation requests do not cancel the database operation.
                if completion.done():
                    break
        result = completion.result()
        if cancelled:
            raise SQLWriteCancelledAfterCommit(result)
        return result

    async def flush(self) -> None:
        self._bind_loop()
        assert self._admissions_idle is not None
        await self._admissions_idle.wait()
        if self._queue is not None:
            await self._queue.join()

    async def close(self) -> None:
        """Stop admission and wait for admitted writes to reach their outcome."""
        loop = self._bind_loop()
        if self._close_task is None:
            self._closing = True
            self._close_task = loop.create_task(self._close_after_drain())
        # A caller cancellation or shutdown timeout cancels only its wait. The
        # worker and accepted transaction continue to their known outcome.
        await asyncio.shield(self._close_task)

    async def _close_after_drain(self) -> None:
        assert self._admissions_idle is not None
        await self._admissions_idle.wait()
        await self.flush()
        assert self._queue is not None and self._worker is not None and self._executor is not None
        await self._queue.put(None)
        await self._worker
        self._executor.shutdown(wait=True)
        self._closed = True

    async def _run(self) -> None:
        assert self._queue is not None and self._executor is not None
        loop = asyncio.get_running_loop()
        while True:
            request = await self._queue.get()
            try:
                if request is None:
                    return
                try:
                    result = await loop.run_in_executor(self._executor, request.operation)
                except Exception as error:
                    if not request.completion.done():
                        request.completion.set_exception(error)
                else:
                    if not request.completion.done():
                        request.completion.set_result(result)
            finally:
                self._queue.task_done()


class SQLConnectionManager(TransactionBase):
    _scm_logger: Optional[HummingbotLogger] = None
    _scm_trade_fills_instance: Optional["SQLConnectionManager"] = None

    LOCAL_DB_VERSION_KEY = "local_db_version"
    LOCAL_DB_VERSION_VALUE = "20261019"
    SQLITE_BUSY_TIMEOUT_MS = 2_000

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._scm_logger is None:
            cls._scm_logger = logging.getLogger(__name__)
        return cls._scm_logger

    @classmethod
    def get_declarative_base(cls):
        return get_declarative_base()

    @classmethod
    def get_trade_fills_instance(
        cls, client_config_map: "ClientConfigAdapter", db_name: Optional[str] = None
    ) -> "SQLConnectionManager":
        if cls._scm_trade_fills_instance is None:
            cls._scm_trade_fills_instance = SQLConnectionManager(
                client_config_map, SQLConnectionType.TRADE_FILLS, db_name=db_name
            )
        elif cls.create_db_path(db_name=db_name) != cls._scm_trade_fills_instance.db_path:
            cls._scm_trade_fills_instance = SQLConnectionManager(
                client_config_map, SQLConnectionType.TRADE_FILLS, db_name=db_name
            )
        return cls._scm_trade_fills_instance

    @classmethod
    def create_db_path(cls, db_path: Optional[str] = None, db_name: Optional[str] = None) -> str:
        if db_path is not None:
            return db_path
        if db_name is not None:
            return join(data_path(), f"{db_name}.sqlite")
        else:
            return join(data_path(), "hummingbot_trades.sqlite")

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 connection_type: SQLConnectionType,
                 db_path: Optional[str] = None,
                 db_name: Optional[str] = None,
                 called_from_migrator = False):
        db_path = self.create_db_path(db_path, db_name)
        self.db_path = db_path
        self._write_queue = BoundedSQLWriteQueue()

        if connection_type is SQLConnectionType.TRADE_FILLS:
            database_url = client_config_map.db_mode.get_url(self.db_path)
            engine_kwargs = {}
            if database_url.startswith("sqlite"):
                engine_kwargs["connect_args"] = {"timeout": self.SQLITE_BUSY_TIMEOUT_MS / 1000.0}
            self._engine: Engine = create_engine(database_url, **engine_kwargs)
            self._configure_sqlite_pragmas()
            self._metadata: MetaData = self.get_declarative_base().metadata
            self._metadata.create_all(self._engine)

            # SQLite does not enforce foreign key constraint, but for others engines, we need to drop it.
            # See: `hummingbot/market/markets_recorder.py`, at line 213.
            with self._engine.begin() as conn:
                inspector = inspect(conn)

                for tname, fkcs in reversed(
                        inspector.get_sorted_table_and_fkc_names()):
                    if fkcs:
                        if not self._engine.dialect.supports_alter:
                            continue
                        for fkc in fkcs:
                            fk_constraint = ForeignKeyConstraint((), (), name=fkc)
                            Table(tname, MetaData(), fk_constraint)
                            conn.execute(DropConstraint(fk_constraint))

        self._session_cls = sessionmaker(bind=self._engine)

        if connection_type is SQLConnectionType.TRADE_FILLS and (not called_from_migrator):
            self.check_and_migrate_db(client_config_map)

    def _configure_sqlite_pragmas(self):
        if not str(self._engine.url).startswith("sqlite"):
            return

        @event.listens_for(self._engine, "connect")
        def set_sqlite_pragma(dbapi_connection, _connection_record):
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT_MS}")
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
            finally:
                cursor.close()

    @property
    def engine(self) -> Engine:
        return self._engine

    def get_new_session(self) -> Session:
        return self._session_cls()

    @property
    def write_queue(self) -> BoundedSQLWriteQueue:
        return self._write_queue

    async def close_write_queue(self) -> None:
        await self._write_queue.close()

    def get_local_db_version(self, session: Session):
        query: Query = (session.query(LocalMetadata)
                        .filter(LocalMetadata.key == self.LOCAL_DB_VERSION_KEY))
        result: Optional[LocalMetadata] = query.one_or_none()
        return result

    def check_and_migrate_db(self, client_config_map: "ClientConfigAdapter"):
        from hummingbot.model.db_migration.migrator import Migrator
        local_db_version_value = None
        with self.get_new_session() as session:
            with session.begin():
                local_db_version = self.get_local_db_version(session=session)
                if local_db_version is None:
                    version_info: LocalMetadata = LocalMetadata(key=self.LOCAL_DB_VERSION_KEY,
                                                                value=self.LOCAL_DB_VERSION_VALUE)
                    session.add(version_info)
                else:
                    local_db_version_value = local_db_version.value

        # End the version-read transaction before the migration acquires its writer reservation.
        # Schema changes and the version update then commit together in the same database file.
        if local_db_version_value is not None and local_db_version_value < self.LOCAL_DB_VERSION_VALUE:
            migrated = Migrator().migrate_db_to_version(
                client_config_map, self, int(local_db_version_value), int(self.LOCAL_DB_VERSION_VALUE)
            )
            if not migrated:
                raise RuntimeError("Database migration failed; startup cannot use an outdated schema")
