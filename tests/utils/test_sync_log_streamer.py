from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call

import pytest
from spark_on_k8s.utils import app_manager


@pytest.fixture
def log_case(monkeypatch):
    api = Mock()
    api.read_namespaced_pod.return_value = SimpleNamespace(
        status=SimpleNamespace(phase="Running"),
        spec=SimpleNamespace(
            containers=[SimpleNamespace(name="spark-driver"), SimpleNamespace(name="metrics")]
        ),
    )
    api.list_namespaced_pod.return_value = SimpleNamespace(
        items=[SimpleNamespace(metadata=SimpleNamespace(name="selected-driver"))]
    )
    watcher = Mock()
    watcher.stream.return_value = iter(["ready"])
    monkeypatch.setattr(app_manager.k8s, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(app_manager.watch, "Watch", Mock(return_value=watcher))
    sleep = Mock()
    monkeypatch.setattr(app_manager.time, "sleep", sleep)
    clients = MagicMock()
    clients.client.return_value.__exit__.return_value = False
    manager = app_manager.SparkAppManager(k8s_client_manager=clients)
    manager.log = Mock()
    return SimpleNamespace(api=api, watcher=watcher, sleep=sleep, manager=manager, clients=clients)


def test_logs_resolve_application_id_before_streaming(log_case):
    log_case.manager.stream_logs(namespace="spark", app_id="report-123", should_print=True)

    log_case.api.list_namespaced_pod.assert_called_once_with(
        namespace="spark", label_selector="spark-app-id=report-123"
    )
    log_case.api.read_namespaced_pod.assert_called_once_with(namespace="spark", name="selected-driver")
    log_case.watcher.stream.assert_called_once_with(
        log_case.api.read_namespaced_pod_log,
        namespace="spark",
        name="selected-driver",
        container="spark-driver",
    )
    log_case.manager.log.assert_called_once_with(msg="ready", level=20, should_print=True)
    log_case.watcher.stop.assert_called_once()


@pytest.mark.parametrize(
    "target,message",
    [
        ({}, "Either pod_name or app_id must be specified"),
        ({"app_id": "absent"}, "No pods found for app absent"),
    ],
)
def test_logs_reject_missing_target_before_opening_a_watch(log_case, target, message):
    log_case.api.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    with pytest.raises(ValueError, match=message):
        log_case.manager.stream_logs(namespace="spark", **target)
    log_case.watcher.stream.assert_not_called()


def test_log_connection_retries_before_first_output_then_emits_once(log_case):
    error = app_manager.ApiException(status=500, reason="TLS handshake failed")
    log_case.watcher.stream.side_effect = [error, error, iter(["first", "second"])]

    log_case.manager.stream_logs(namespace="spark", pod_name="driver")

    assert log_case.watcher.stream.call_count == 3
    assert log_case.sleep.call_args_list == [call(5), call(5)]
    assert [
        args.kwargs["msg"] for args in log_case.manager.log.call_args_list if args.kwargs["level"] == 20
    ] == ["first", "second"]
    log_case.watcher.stop.assert_called_once()


def test_log_connection_retries_are_bounded(log_case):
    error = app_manager.ApiException(status=500, reason="TLS handshake failed")
    log_case.watcher.stream.side_effect = error

    with pytest.raises(app_manager.ApiException) as raised:
        log_case.manager.stream_logs(namespace="spark", pod_name="driver")

    assert raised.value is error
    assert log_case.watcher.stream.call_count == 4
    assert log_case.sleep.call_args_list == [call(5)] * 3
    log_case.clients.client.return_value.__exit__.assert_called_once()
    assert log_case.clients.client.return_value.__exit__.call_args.args[1] is error


def test_logs_do_not_replay_after_partial_output(log_case):
    error = app_manager.ApiException(status=500, reason="Connection lost")

    def interrupted_stream():
        yield "already delivered"
        raise error

    log_case.watcher.stream.return_value = interrupted_stream()

    with pytest.raises(app_manager.ApiException) as raised:
        log_case.manager.stream_logs(namespace="spark", pod_name="driver")

    assert raised.value is error
    log_case.watcher.stream.assert_called_once()
    log_case.sleep.assert_not_called()
    log_case.manager.log.assert_called_once_with(msg="already delivered", level=20, should_print=False)


def test_unexpected_log_errors_are_not_retried(log_case):
    error = RuntimeError("invalid log response")
    log_case.watcher.stream.side_effect = error

    with pytest.raises(RuntimeError) as raised:
        log_case.manager.stream_logs(namespace="spark", pod_name="driver")

    assert raised.value is error
    log_case.watcher.stream.assert_called_once()
    log_case.sleep.assert_not_called()


def test_pending_driver_timeout_does_not_start_streaming(log_case, monkeypatch):
    log_case.api.read_namespaced_pod.return_value = SimpleNamespace(status=SimpleNamespace(phase="Pending"))
    monkeypatch.setattr(app_manager.time, "time", Mock(side_effect=[100, 106]))

    with pytest.raises(TimeoutError, match="App startup timeout"):
        log_case.manager.stream_logs(namespace="spark", pod_name="driver", startup_timeout=5)

    log_case.watcher.stream.assert_not_called()
    log_case.sleep.assert_not_called()
    log_case.clients.client.return_value.__exit__.assert_called_once()
