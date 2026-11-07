from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from spark_on_k8s.utils import app_manager, run
from spark_on_k8s.utils.spark_app_status import SparkAppStatus


def test_overall_wait_deadline_covers_running_jobs_and_caps_sleep(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(run.time, "monotonic", lambda: clock[0])
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(run.time, "sleep", sleep)
    manager = app_manager.SparkAppManager(k8s_client_manager=MagicMock())
    status = Mock(return_value=SparkAppStatus.Running)
    monkeypatch.setattr(manager, "app_status", status)
    with pytest.raises(TimeoutError, match="wait timeout"):
        manager.wait_for_app(namespace="spark", pod_name="driver", timeout=3, poll_interval=10)
    assert slept == [3]
    assert status.call_args.kwargs["request_timeout"] == 3


def test_quiet_log_watch_is_cancelled_and_closed_at_deadline(monkeypatch):
    api = Mock()
    api.read_namespaced_pod.return_value = SimpleNamespace(
        status=SimpleNamespace(phase="Running"),
        spec=SimpleNamespace(containers=[SimpleNamespace(name="driver")]),
    )
    monkeypatch.setattr(app_manager.k8s, "CoreV1Api", Mock(return_value=api))
    stopped = threading.Event()
    closed = threading.Event()

    class QuietWatch:
        def stream(self, *args, **kwargs):
            assert 0 < kwargs["_request_timeout"] <= 0.05
            assert kwargs["container"] == "driver"
            try:
                assert stopped.wait(2), "The deadline failed to cancel a silent stream"
                return
                yield "unreachable"
            finally:
                closed.set()

        def stop(self):
            stopped.set()

    monkeypatch.setattr(app_manager.watch, "Watch", QuietWatch)
    manager = app_manager.SparkAppManager(k8s_client_manager=MagicMock())
    before = time.monotonic()
    with pytest.raises(TimeoutError, match="wait timeout"):
        manager.stream_logs(namespace="spark", pod_name="driver", timeout=0.05)
    assert time.monotonic() - before < 1.5
    assert stopped.is_set() and closed.is_set()


def test_log_stream_closes_generator_on_keyboard_interrupt(monkeypatch):
    api = Mock()
    api.read_namespaced_pod.return_value = SimpleNamespace(
        status=SimpleNamespace(phase="Running"),
        spec=SimpleNamespace(containers=[SimpleNamespace(name="driver")]),
    )
    monkeypatch.setattr(app_manager.k8s, "CoreV1Api", Mock(return_value=api))
    closed = []

    def stream(*args, **kwargs):
        try:
            yield "line"
            raise KeyboardInterrupt()
        finally:
            closed.append(True)

    watch = Mock()
    watch.stream.side_effect = stream
    monkeypatch.setattr(app_manager.watch, "Watch", Mock(return_value=watch))
    with pytest.raises(KeyboardInterrupt):
        app_manager.SparkAppManager(k8s_client_manager=MagicMock()).stream_logs(
            namespace="spark", pod_name="driver"
        )
    assert closed == [True]
    watch.stop.assert_called_once()
