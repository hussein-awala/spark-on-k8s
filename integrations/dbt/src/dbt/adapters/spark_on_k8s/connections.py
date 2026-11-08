"""Reuse Spark SQL behavior while keeping compute ownership invocation-scoped."""

from __future__ import annotations

from copy import deepcopy
from threading import Event

from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.spark.connections import SparkConnectionManager
from dbt.adapters.spark_on_k8s.connect_cursor import ConnectConnectionWrapper
from dbt.adapters.spark_on_k8s.thrift_connection import (
    ManagedPyhiveConnectionWrapper as ManagedPyhiveConnectionWrapper,
    open_thrift_connection,
)
from dbt_common.exceptions import DbtRuntimeError
from spark_on_k8s_dbt.lifecycle import get_invocation_owner

logger = AdapterLogger("Spark")


class SparkOnK8SConnectionManager(SparkConnectionManager):
    TYPE = "spark_on_k8s"

    def __init__(self, profile, mp_context):
        super().__init__(profile, mp_context)
        self.owner = get_invocation_owner()
        # One cancellation flag per open still in progress, guarded by self.lock.
        self._opening = set()
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
        pending = Event()
        handle = None
        with self.lock:
            self._opening.add(pending)
        try:
            self._ensure_not_cancelled(pending)
            endpoint = self.owner.ensure_endpoint(credentials.method, credentials.compute)
            self._ensure_not_cancelled(pending)
            if credentials.method == "thrift":
                # dbt initially shares profile.credentials across worker threads.
                # Clone before writing the invocation's private endpoint.
                connection.credentials = deepcopy(credentials)
                connection.credentials.host = endpoint.host
                connection.credentials.port = endpoint.port
                handle = open_thrift_connection(connection.credentials)
            else:
                if endpoint.connect_profile is None:
                    raise DbtRuntimeError("The invocation did not provide a Spark Connect profile")
                handle = ConnectConnectionWrapper(
                    endpoint.connect_profile, credentials.server_side_parameters
                )
            # Publish atomically with cancel_open's scan: it either cancels this
            # OPEN handle, or this open observes the cancellation and discards it.
            with self.lock:
                self._ensure_not_cancelled(pending)
                connection.handle = handle
                connection.state = ConnectionState.OPEN
            return connection
        except BaseException:
            if handle is not None:
                try:
                    handle.close()
                except Exception as error:
                    logger.debug(f"Exception while closing an unpublished connection: {error}")
            # A failed LazyHandle must not resolve again when dbt closes it.
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise
        finally:
            with self.lock:
                self._opening.discard(pending)

    def _ensure_not_cancelled(self, pending):
        # dbt cancels open connections on --fail-fast and on interrupts. After
        # fail-fast it still opens a connection for on-run-end hooks, so stop only
        # opens already in progress; an interrupt stops every later open.
        if pending.is_set() or self.owner.cancelled:
            raise DbtRuntimeError("The managed dbt invocation was cancelled")

    def cancel_open(self):
        names = []
        this_connection = self.get_if_exists()
        with self.lock:
            for pending in self._opening:
                pending.set()
            for connection in self.thread_connections.values():
                if connection is this_connection:
                    continue
                # Reading an INIT connection's handle would resolve LazyHandle.
                if connection.state == ConnectionState.OPEN and connection.handle is not None:
                    try:
                        self.cancel(connection)
                    except Exception as error:
                        # As dbt-spark does, keep cancelling the others: dbt must still
                        # join its workers and clean up. Compute is deleted at exit.
                        logger.warning(f"Could not cancel connection {connection.name}: {error}")
                if connection.name is not None:
                    names.append(connection.name)
        return names
