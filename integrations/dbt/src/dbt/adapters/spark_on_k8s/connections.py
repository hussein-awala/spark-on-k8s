"""Reuse Spark SQL behavior while keeping compute ownership invocation-scoped."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from threading import Event
from typing import Any

from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.spark.connections import PyhiveConnectionWrapper, SparkConnectionManager
from dbt.adapters.spark_on_k8s.connect_cursor import ConnectConnectionWrapper
from dbt_common.exceptions import DbtRuntimeError
from spark_on_k8s_dbt.lifecycle import get_invocation_owner


class ManagedPyhiveConnectionWrapper(PyhiveConnectionWrapper):
    """Preserve seed precision and support dbt's limited result fetching."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        super().cancel()

    def execute(self, sql, bindings=None):
        if self._cancelled:
            raise DbtRuntimeError("The managed dbt connection was cancelled")
        super().execute(sql, bindings)

    @classmethod
    def _fix_binding(cls, value: Any) -> Any:
        # PyHive has no Decimal escaper. Spark's seed macro supplies CAST, so a
        # quoted decimal string keeps all digits without conversion to float.
        return str(value) if isinstance(value, Decimal) else value

    def fetchone(self):
        assert self._cursor, "Cursor not available"
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        assert self._cursor, "Cursor not available"
        return self._cursor.fetchmany() if size is None else self._cursor.fetchmany(size)


class SparkOnK8SConnectionManager(SparkConnectionManager):
    TYPE = "spark_on_k8s"

    def __init__(self, profile, mp_context):
        super().__init__(profile, mp_context)
        self.owner = get_invocation_owner()
        self._cancelled = Event()
        if self.owner is None:
            raise DbtRuntimeError(
                "This experimental adapter requires invocation ownership. "
                "Run spark-on-k8s-dbt run -- <dbt command>, for example: "
                "spark-on-k8s-dbt run -- build"
            )

    def open(self, connection):
        if connection.state == ConnectionState.OPEN:
            return connection
        credentials = connection.credentials
        try:
            self._ensure_not_cancelled()
            endpoint = self.owner.ensure_endpoint(credentials.method, credentials.compute)
            self._ensure_not_cancelled()
            if credentials.method == "thrift":
                # dbt initially shares profile.credentials across worker threads.
                # Clone before writing the invocation's private endpoint.
                connection.credentials = deepcopy(credentials)
                connection.credentials.host = endpoint.host
                connection.credentials.port = endpoint.port
                super().open(connection)
                connection.handle = ManagedPyhiveConnectionWrapper(
                    connection.handle.handle,
                    poll_interval=credentials.poll_interval,
                    query_timeout=credentials.query_timeout,
                    query_retries=0,
                )
            else:
                if endpoint.connect_profile is None:
                    raise DbtRuntimeError("The invocation did not provide a Spark Connect profile")
                connection.handle = ConnectConnectionWrapper(
                    endpoint.connect_profile, credentials.server_side_parameters
                )
                connection.state = ConnectionState.OPEN
            return connection
        except BaseException:
            # A failed LazyHandle must not resolve again when dbt closes it.
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise

    def _ensure_not_cancelled(self):
        if self._cancelled.is_set():
            raise DbtRuntimeError("The managed dbt invocation was cancelled")

    def cancel_open(self):
        self._cancelled.set()
        names = []
        this_connection = self.get_if_exists()
        with self.lock:
            for connection in self.thread_connections.values():
                if connection is this_connection:
                    continue
                # Reading an INIT connection's handle would resolve LazyHandle.
                if connection.state == ConnectionState.OPEN and connection.handle is not None:
                    self.cancel(connection)
                if connection.name is not None:
                    names.append(connection.name)
        return names
