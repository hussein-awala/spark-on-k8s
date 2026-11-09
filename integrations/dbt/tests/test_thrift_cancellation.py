from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.spark_on_k8s import thrift_connection as module
from dbt_common.exceptions import DbtRuntimeError
from pyhive.hive import Cursor
from TCLIService import ttypes
from thrift.transport.TTransport import TBufferedTransport, TTransportException


def success():
    return ttypes.TStatus(statusCode=ttypes.TStatusCode.SUCCESS_STATUS)


class FakeClient:
    def __init__(self, blocked=None):
        self.blocked = blocked
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = []
        self.active = 0
        self.max_active = 0
        self.error = None
        self.state = ttypes.TOperationState.RUNNING_STATE
        self.error_message = None
        self.close_operation_failure = None
        self.operation = ttypes.TOperationHandle(
            operationId=ttypes.THandleIdentifier(guid=b"g" * 16, secret=b"s" * 16),
            operationType=ttypes.TOperationType.EXECUTE_STATEMENT,
            hasResultSet=False,
        )

    @contextmanager
    def rpc(self, name):
        self.calls.append(name)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if name == self.blocked:
                self.entered.set()
                assert self.release.wait(3), "Test RPC barrier was not released"
            yield
        finally:
            self.active -= 1

    def ExecuteStatement(self, request):
        with self.rpc("execute"):
            return ttypes.TExecuteStatementResp(status=success(), operationHandle=self.operation)

    def GetOperationStatus(self, request):
        with self.rpc("poll"):
            if self.error is not None:
                raise self.error
            return ttypes.TGetOperationStatusResp(
                status=success(), operationState=self.state, errorMessage=self.error_message
            )

    def CancelOperation(self, request):
        with self.rpc("cancel"):
            assert request.operationHandle is self.operation
            return ttypes.TCancelOperationResp(status=success())

    def CloseOperation(self, request):
        with self.rpc("close_operation"):
            if self.close_operation_failure == "transport":
                raise TTransportException(TTransportException.END_OF_FILE, "stream closed by server")
            if self.close_operation_failure == "status":
                return ttypes.TCloseOperationResp(
                    status=ttypes.TStatus(
                        statusCode=ttypes.TStatusCode.ERROR_STATUS, errorMessage="Invalid OperationHandle"
                    )
                )
            return ttypes.TCloseOperationResp(status=success())

    def CloseSession(self, request):
        with self.rpc("close_session"):
            return ttypes.TCloseSessionResp(status=success())


class FakeConnection:
    def __init__(self, client, transport):
        self._client = client
        self._transport = transport
        self.sessionHandle = object()

    @property
    def client(self):
        return self._client

    def cursor(self):
        return Cursor(self)

    def close(self):
        self.client.CloseSession(None)
        self._transport.close()


def wrapper_for(client, timeout=1):
    transport = Mock()
    raw = FakeConnection(client, transport)
    wrapper = module.ManagedPyhiveConnectionWrapper(
        raw, transport=transport, poll_interval=0.001, query_retries=0, lock_timeout=timeout
    ).cursor()
    return wrapper, transport


def start_thread(call, errors):
    def invoke():
        try:
            call()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=invoke)
    thread.start()
    return thread


@pytest.mark.parametrize("blocked", ["execute", "poll"])
def test_cancel_serializes_actual_pyhive_cursor_rpc_and_operation_assignment(blocked):
    client = FakeClient(blocked)
    wrapper, transport = wrapper_for(client)
    errors = []
    worker = start_thread(lambda: wrapper.execute("SELECT 1"), errors)
    canceller = None
    try:
        assert client.entered.wait(1)
        canceller = start_thread(wrapper.cancel, errors)
        assert wrapper._guard.cancelled.wait(1)
        assert "cancel" not in client.calls
        client.release.set()
    finally:
        client.release.set()
        worker.join(3)
        if canceller is not None:
            canceller.join(3)
    assert not worker.is_alive() and not canceller.is_alive()
    assert client.max_active == 1 and client.calls.count("cancel") == 1
    assert client.calls.count("execute") == 1
    assert errors and all(isinstance(error, DbtRuntimeError) for error in errors)
    wrapper.close()
    assert "close_operation" not in client.calls and "close_session" not in client.calls
    transport.close.assert_called_once()


def test_timeout_abandons_transport_without_replay_or_network_cleanup():
    client = FakeClient()
    client.error = TTransportException(TTransportException.TIMED_OUT, "stalled read")
    wrapper, transport = wrapper_for(client)
    with pytest.raises(DbtRuntimeError, match="Connection lost"):
        wrapper.execute("INSERT INTO t VALUES (1)")
    wrapper.close()
    with pytest.raises(DbtRuntimeError, match="unusable"):
        wrapper.execute("INSERT INTO t VALUES (1)")
    assert client.calls == ["execute", "poll"]
    transport.close.assert_called_once()


def test_cancel_lock_acquisition_is_bounded_when_an_rpc_stalls():
    client = FakeClient("poll")
    wrapper, transport = wrapper_for(client, timeout=0.03)
    errors = []
    worker = start_thread(lambda: wrapper.execute("SELECT 1"), errors)
    try:
        assert client.entered.wait(1)
        started = time.monotonic()
        with pytest.raises(DbtRuntimeError, match="RPC lock"):
            wrapper.cancel()
        assert time.monotonic() - started < 0.5
        assert "cancel" not in client.calls
        transport.close.assert_called_once()
    finally:
        client.release.set()
        worker.join(3)
    assert not worker.is_alive()
    assert "close_operation" not in client.calls


def test_close_started_before_cancellation_cannot_abort_pending_cancel_rpc():
    client = FakeClient("cancel")
    wrapper, transport = wrapper_for(client)
    wrapper._cursor.execute("SELECT 1", async_=True)
    close_entered = threading.Event()
    release_close = threading.Event()
    original_close = wrapper._cursor.close

    def paused_close():
        close_entered.set()
        assert release_close.wait(3)
        original_close()

    wrapper._cursor.close = paused_close
    errors = []
    closer = start_thread(wrapper.close, errors)
    canceller = None
    try:
        assert close_entered.wait(1)
        canceller = start_thread(wrapper.cancel, errors)
        assert client.entered.wait(1)
        release_close.set()
        closer.join(1)
        assert not closer.is_alive()
        transport.close.assert_not_called()
        assert "close_operation" not in client.calls and "close_session" not in client.calls
    finally:
        release_close.set()
        client.release.set()
        closer.join(3)
        if canceller is not None:
            canceller.join(3)
    assert not canceller.is_alive()
    assert client.calls == ["execute", "cancel"]
    transport.close.assert_called_once()


def test_concurrent_abort_closes_local_transport_once():
    entered, release = threading.Event(), threading.Event()
    transport = Mock()

    def close():
        entered.set()
        assert release.wait(3)

    transport.close.side_effect = close
    guard = module._RpcGuard(transport)
    errors = []
    first = start_thread(guard.abort, errors)
    second = None
    try:
        assert entered.wait(1)
        second = start_thread(guard.abort, errors)
    finally:
        release.set()
        first.join(3)
        if second is not None:
            second.join(3)
    assert not errors and not first.is_alive() and not second.is_alive()
    transport.close.assert_called_once()


def credentials(**updates):
    return SimpleNamespace(
        **{
            "host": "127.0.0.1",
            "port": 10000,
            "user": "dbt",
            "server_side_parameters": {"key": "value"},
            "poll_interval": 0.001,
            "query_timeout": None,
            "connect_retries": 0,
            "connect_timeout": 0,
            "retry_all": False,
            **updates,
        }
    )


def test_socket_timeout_applies_before_open_session_against_silent_local_server(monkeypatch):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(2)
    release = threading.Event()
    observed = []

    def serve():
        with listener.accept()[0] as connection:
            connection.settimeout(2)
            assert connection.recv(4096), "OpenSession request was not sent"
            release.wait(2)

    thread = threading.Thread(target=serve)
    thread.start()
    monkeypatch.setattr(module, "SOCKET_TIMEOUT_SECONDS", 0.05)

    def transport(factory, mechanism, raw_socket):
        observed.append(raw_socket._timeout)
        return TBufferedTransport(raw_socket)

    monkeypatch.setattr(module, "TSaslClientTransport", transport)
    started = time.monotonic()
    try:
        with pytest.raises(FailedToConnectError, match="timeout|timed out"):
            module.open_thrift_connection(credentials(port=listener.getsockname()[1]))
        assert time.monotonic() - started < 1
        assert observed == [0.05]
    finally:
        release.set()
        thread.join(3)
        listener.close()
    assert not thread.is_alive()


def test_custom_open_preserves_retry_username_and_session_parameters(monkeypatch):
    transport = Mock()
    socket_factory = Mock()
    client = FakeClient()
    raw = FakeConnection(client, transport)
    connect = Mock(side_effect=[OSError("temporary connection failure"), raw])
    monkeypatch.setattr(module, "TSocket", socket_factory)
    monkeypatch.setattr(module, "TSaslClientTransport", lambda *args: transport)
    monkeypatch.setattr(module.hive, "Connection", connect)
    wrapper = module.open_thrift_connection(credentials(connect_retries=1, retry_all=True))
    assert connect.call_count == 2
    assert connect.call_args.kwargs == {
        "thrift_transport": transport,
        "username": "dbt",
        "configuration": {"key": "value"},
    }
    assert socket_factory.return_value.setTimeout.call_args.args == (15000,)
    assert wrapper.query_retries == 0


def test_normal_close_uses_session_rpc_and_only_guarded_local_transport_close():
    client = FakeClient()
    wrapper, transport = wrapper_for(client)
    wrapper._cursor.execute("SELECT 1", async_=True)
    wrapper.handle.close = Mock(side_effect=AssertionError("unguarded upstream close must not run"))
    wrapper.close()
    wrapper.close()
    assert client.calls == ["execute", "close_operation", "close_session"]
    transport.close.assert_called_once()
    wrapper.handle.close.assert_not_called()


@pytest.mark.parametrize("failure", ["transport", "status"])
def test_close_operation_failure_after_a_successful_statement_does_not_fail_the_node(failure):
    client = FakeClient()
    client.state = ttypes.TOperationState.FINISHED_STATE
    client.close_operation_failure = failure
    wrapper, transport = wrapper_for(client)
    wrapper.execute("INSERT INTO t VALUES (1)")
    # dbt's release() closes the handle; raising here would mark the already
    # successful node as failed. dbt-spark also logs cursor-close errors.
    wrapper.close()
    # A failed exchange aborts the stream, so no CloseSession may follow it; an
    # error status leaves the stream usable for the normal session close.
    session = ["close_session"] if failure == "status" else []
    assert client.calls == ["execute", "poll", "close_operation", *session]
    transport.close.assert_called_once()


def test_failed_statement_is_still_reported_when_close_operation_also_fails():
    from dbt_common.exceptions import DbtDatabaseError

    client = FakeClient()
    client.state = ttypes.TOperationState.ERROR_STATE
    client.error_message = "Table or view not found"
    client.close_operation_failure = "transport"
    wrapper, transport = wrapper_for(client)
    with pytest.raises(DbtDatabaseError, match="Table or view not found"):
        wrapper.execute("INSERT INTO missing VALUES (1)")
    wrapper.close()
    assert client.calls == ["execute", "poll", "close_operation"]
    transport.close.assert_called_once()
