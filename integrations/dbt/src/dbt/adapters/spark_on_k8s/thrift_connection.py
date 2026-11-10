"""Bound and serialize the pinned PyHive client's RPCs without serializing SQL execution."""

from __future__ import annotations

import time
from contextlib import contextmanager
from decimal import Decimal
from threading import Event, Lock, RLock
from typing import Any

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.spark.connections import PyhiveConnectionWrapper, _is_retryable_error
from dbt_common.exceptions import DbtRuntimeError
from pyhive import hive
from TCLIService.ttypes import TCloseSessionReq
from thrift.transport.TSocket import TSocket
from thrift_sasl import TSaslClientTransport

SOCKET_TIMEOUT_SECONDS = 15
logger = AdapterLogger("Spark")


class _RpcGuard:
    def __init__(self, transport, timeout=SOCKET_TIMEOUT_SECONDS):
        self.transport = transport
        self.timeout = timeout
        self.lock = RLock()
        self.cancelled = Event()
        self.broken = Event()
        self._state_lock = Lock()
        self._close_lock = Lock()
        self._transport_closed = False

    def abort(self):
        self.broken.set()
        # Local transport closure, never another RPC on a possibly desynchronized stream.
        with self._close_lock:
            if not self._transport_closed:
                self._transport_closed = True
                self.transport.close()

    def begin_cancel(self):
        with self._state_lock:
            self.cancelled.set()

    def close_unless_cancelling(self):
        # Atomically transfer local closure ownership to the cancelling thread.
        # A worker that started normal close earlier must not abort its RPC.
        with self._state_lock:
            if not self.cancelled.is_set():
                self.abort()

    @contextmanager
    def exchange(self, *, cancelling=False):
        if self.broken.is_set():
            raise DbtRuntimeError("The managed Thrift transport is closed or unusable")
        if self.cancelled.is_set() and not cancelling:
            raise DbtRuntimeError("The managed dbt connection was cancelled")
        if not self.lock.acquire(timeout=self.timeout):
            if self.cancelled.is_set() and not cancelling:
                raise DbtRuntimeError("The managed dbt connection was cancelled")
            self.abort()
            raise DbtRuntimeError("Timed out acquiring the managed Thrift RPC lock; transport closed")
        try:
            if self.broken.is_set():
                raise DbtRuntimeError("The managed Thrift transport is closed or unusable")
            if self.cancelled.is_set() and not cancelling:
                raise DbtRuntimeError("The managed dbt connection was cancelled")
            yield
        finally:
            self.lock.release()


class _SerializedClient:
    """PyHive 0.7.0's generated client has no send/receive synchronization.

    Its public Connection.client property reads _client. Replace only that backing
    object, preserving its generated RPC implementation and message framing. The
    pinned-version regression tests cover this otherwise private integration seam.
    """

    def __init__(self, client, guard):
        self.client = client
        self.guard = guard

    def __getattr__(self, name):
        method = getattr(self.client, name)
        if not callable(method):
            return method

        def call(*args, **kwargs):
            with self.guard.exchange(cancelling=name == "CancelOperation"):
                try:
                    return method(*args, **kwargs)
                except BaseException:
                    # A failed or interrupted exchange may leave unread reply bytes.
                    # Neither polling nor CloseOperation/CloseSession may reuse it.
                    self.guard.abort()
                    raise

        return call


class _GuardedCursor:
    """Keep operation-handle changes atomic with cancellation, not the polling loop."""

    def __init__(self, cursor, guard):
        self.cursor = cursor
        self.guard = guard
        self.submitted = False

    def execute(self, *args, **kwargs):
        with self.guard.exchange():
            self.submitted = False
            result = self.cursor.execute(*args, **kwargs)
            self.submitted = True
            return result

    def cancel(self):
        with self.guard.exchange(cancelling=True):
            if self.submitted:
                self.cursor.cancel()

    def close(self):
        # The cancelling thread must retain the operation until CancelOperation
        # finishes. It closes the transport afterward; worker cleanup sends no RPC.
        if self.guard.cancelled.is_set() or self.guard.broken.is_set():
            return
        with self.guard.exchange():
            self.cursor.close()
            self.submitted = False

    def __getattr__(self, name):
        return getattr(self.cursor, name)


class ManagedPyhiveConnectionWrapper(PyhiveConnectionWrapper):
    """Preserve seed precision and cancel with bounded, serialized transport access."""

    def __init__(self, handle, *args, transport=None, lock_timeout=SOCKET_TIMEOUT_SECONDS, **kwargs):
        super().__init__(handle, *args, **kwargs)
        self._guard = _RpcGuard(transport if transport is not None else handle._transport, lock_timeout)
        handle._client = _SerializedClient(handle.client, self._guard)

    def cursor(self):
        with self._guard.exchange():
            self._cursor = _GuardedCursor(self.handle.cursor(), self._guard)
        return self

    def cancel(self):
        self._guard.begin_cancel()
        try:
            if self._cursor is not None:
                self._cursor.cancel()
        finally:
            # Interrupt subsequent worker RPCs and avoid network cleanup on a
            # cancelled stream. The invocation owner deletes the server separately.
            self._guard.abort()

    def close(self):
        if self._guard.cancelled.is_set():
            return
        if self._guard.broken.is_set():
            self._guard.abort()
            return
        try:
            if self._cursor is not None:
                try:
                    self._cursor.close()
                except Exception as error:
                    # execute() already reported the statement's outcome. Like
                    # dbt-spark, a failed CloseOperation must not fail the node.
                    logger.debug(f"Exception while closing cursor: {error}")
                    if self._guard.cancelled.is_set() or self._guard.broken.is_set():
                        # An aborted stream cannot carry CloseSession. The local
                        # transport is closed instead; compute is deleted at exit.
                        return
            # Match pinned PyHive Connection.close's RPC, but keep ALL local
            # transport closure in the once-only guard. Calling raw.close would
            # perform an unguarded close after releasing the RPC lock.
            try:
                response = self.handle.client.CloseSession(
                    TCloseSessionReq(sessionHandle=self.handle.sessionHandle)
                )
                hive._check_status(response)
            except Exception as error:
                # Every statement already reported its outcome and the server is
                # deleted at exit. A stream that ends while the session closes
                # ("TSocket read 0 bytes") must not fail the node either.
                logger.debug(f"Exception while closing session: {error}")
        finally:
            self._guard.close_unless_cancelling()

    def execute(self, sql, bindings=None):
        if self._guard.cancelled.is_set():
            raise DbtRuntimeError("The managed dbt connection was cancelled")
        super().execute(sql, bindings)

    @classmethod
    def _fix_binding(cls, value: Any) -> Any:
        # PyHive has no Decimal escaper. Spark's seed macro supplies CAST.
        return str(value) if isinstance(value, Decimal) else value

    def fetchone(self):
        assert self._cursor, "Cursor not available"
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        assert self._cursor, "Cursor not available"
        return self._cursor.fetchmany() if size is None else self._cursor.fetchmany(size)


def open_thrift_connection(credentials):
    """Use PyHive's supported custom transport path; bound waits before OpenSession."""
    for attempt in range(credentials.connect_retries + 1):
        socket = TSocket(credentials.host, credentials.port)
        socket.setTimeout(SOCKET_TIMEOUT_SECONDS * 1000)
        transport = TSaslClientTransport(
            lambda: hive.get_installed_sasl(
                host=credentials.host, sasl_auth="PLAIN", username=credentials.user, password="x"
            ),
            "PLAIN",
            socket,
        )
        try:
            raw = hive.Connection(
                thrift_transport=transport,
                username=credentials.user,
                configuration=credentials.server_side_parameters,
            )
            return ManagedPyhiveConnectionWrapper(
                raw,
                transport=transport,
                poll_interval=credentials.poll_interval,
                query_timeout=credentials.query_timeout,
                query_retries=0,
            )
        except Exception as error:
            transport.close()
            if (
                isinstance(error, EOFError)
                or attempt == credentials.connect_retries
                or not (credentials.retry_all or _is_retryable_error(error))
            ):
                raise FailedToConnectError(f"Failed to open managed Thrift connection: {error}") from error
            time.sleep(credentials.connect_timeout)
        except BaseException:
            transport.close()
            raise
    raise AssertionError("Connection retry loop ended without a connection or error")
