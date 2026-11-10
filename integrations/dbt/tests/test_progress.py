from __future__ import annotations

import json
from argparse import Namespace
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import dbt.flags
import pytest
from click.testing import CliRunner
from spark_on_k8s_dbt import lifecycle, progress
from spark_on_k8s_dbt.cli import main
from spark_on_k8s_dbt.lifecycle import InvocationOwner, get_invocation_owner
from spark_on_k8s_dbt.progress import StartupProgress, dbt_allows_progress, driver_state


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(progress.time, "monotonic", clock)
    return clock


def pod(phase="Pending", node=None, conditions=(), waiting=None, uid="driver"):
    containers = []
    if waiting:
        containers.append(
            SimpleNamespace(
                name="spark-kubernetes-driver",
                state=SimpleNamespace(waiting=SimpleNamespace(reason=waiting, message="registry text")),
            )
        )
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, deletion_timestamp=None),
        spec=SimpleNamespace(node_name=node),
        status=SimpleNamespace(
            phase=phase,
            conditions=list(conditions),
            init_container_statuses=None,
            container_statuses=containers,
        ),
    )


def unschedulable(message):
    return SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable", message=message)


def lines(capsys):
    captured = capsys.readouterr()
    assert captured.out == ""  # dbt's stdout, including machine-readable output, is never used.
    return captured.err.splitlines()


def test_driver_state_reports_scheduling_and_container_waits_without_event_text():
    assert driver_state(pod()) == ("unscheduled", "waiting for the driver pod to be scheduled")
    state, message = driver_state(pod(conditions=[unschedulable("0/3 nodes are available: " + "x" * 300)]))
    assert state == "unscheduled:Unschedulable"
    assert message.startswith("waiting for the driver pod to be scheduled (Unschedulable: 0/3 nodes")
    assert message.endswith("...)") and len(message) < 220
    assert driver_state(pod(node="worker", waiting="ContainerCreating")) == (
        "waiting:ContainerCreating",
        "driver pod scheduled on worker; container spark-kubernetes-driver is ContainerCreating",
    )
    state, message = driver_state(pod(node="worker", waiting="ImagePullBackOff"))
    assert message.endswith("is ImagePullBackOff; check compute.image and image pull access")
    assert "registry text" not in message
    assert driver_state(pod(node="worker")) == (
        "scheduled",
        "driver pod scheduled on worker; starting its containers",
    )
    assert driver_state(pod(phase="Running", node="worker")) == ("running", "driver container running")


def test_progress_prints_new_states_and_rate_limits_repeats(clock, capsys):
    reporter = StartupProgress(allowed=lambda: True, heartbeat=30)
    reporter.start()
    reporter.update("a", "first")
    clock.now += 5
    reporter.update("a", "first again")
    reporter.update("b", "second")
    clock.now += 1
    # Alternating states (for example ErrImagePull and ImagePullBackOff) do not flood.
    reporter.update("a", "first flapping")
    reporter.update("b", "second flapping")
    clock.now += 30
    reporter.update("a", "still first")
    assert lines(capsys) == [
        "Managed Spark +0s: first",
        "Managed Spark +5s: second",
        "Managed Spark +36s: still first",
    ]
    reporter.start()
    reporter.update("a", "new operation")
    assert lines(capsys) == ["Managed Spark +0s: new operation"]


@pytest.mark.parametrize(
    "flags, shown",
    [
        (Namespace(USE_COLORS=True), True),
        (Namespace(QUIET=False, LOG_LEVEL="info"), True),
        (Namespace(QUIET=False, LOG_LEVEL="debug"), True),
        (Namespace(QUIET=True, LOG_LEVEL="info"), False),
        (Namespace(QUIET=False, LOG_LEVEL="warn"), False),
        (Namespace(QUIET=False, LOG_LEVEL="error"), False),
        (Namespace(QUIET=False, LOG_LEVEL="none"), False),
    ],
)
def test_progress_follows_dbt_quiet_and_log_level(monkeypatch, capsys, flags, shown):
    monkeypatch.setattr(dbt.flags, "GLOBAL_FLAGS", flags)
    assert dbt_allows_progress() is shown
    StartupProgress().update("state", "message")
    assert bool(lines(capsys)) is shown


def test_closed_stderr_never_fails_startup(monkeypatch):
    stream = Mock()
    stream.write.side_effect = ValueError("I/O operation on closed file")
    monkeypatch.setattr(progress.sys, "stderr", stream)
    StartupProgress(allowed=lambda: True).update("state", "message")


def compute(tmp_path):
    return {
        "in_cluster": True,
        "image": "spark:test",
        "server_jar": "local:///opt/spark/server.jar",
        "namespace": "test",
        "service_account": "spark",
        "poll_interval": 0.01,
        "spark_conf": {
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": "hdfs://h/warehouse",
            "spark.hadoop.hive.metastore.uris": "thrift://h:9083",
        },
    }


def test_connect_startup_and_cleanup_report_each_phase_on_stderr(tmp_path, monkeypatch, capsys):
    from spark_on_k8s.connect_server import ConnectServerHandle
    from spark_on_k8s.utils.run import RunHandle

    handle = ConnectServerHandle(
        run=RunHandle(
            namespace="test", app_id="dbt-connect-1", driver_pod="dbt-connect-1-driver", driver_uid="d"
        ),
        service_name="dbt-connect-1-connect",
        service_uid="s",
        driver_service_uid="h",
        port=15002,
    )
    sdk = Mock()
    sdk.k8s_client_manager.client.side_effect = lambda: nullcontext(object())
    sdk.create_connect_server.return_value = handle
    monkeypatch.setattr(lifecycle, "RecordingSDK", lambda manager, record: sdk)
    api = Mock()
    api.read_namespace.return_value = SimpleNamespace(metadata=SimpleNamespace(uid="namespace"))
    api.read_namespaced_pod.side_effect = [
        pod(conditions=[unschedulable("0/1 nodes are available: 1 Insufficient cpu.")], uid="d"),
        pod(conditions=[unschedulable("0/1 nodes are available: 1 Insufficient cpu.")], uid="d"),
        pod(node="worker", waiting="ContainerCreating", uid="d"),
        pod(phase="Running", node="worker", uid="d"),
    ]
    monkeypatch.setattr(lifecycle.k8s, "CoreV1Api", lambda raw: api)
    owner = InvocationOwner(tmp_path / "receipt", progress=StartupProgress(allowed=lambda: True))
    endpoint = owner.ensure_endpoint("connect", compute(tmp_path))
    assert endpoint.host == "dbt-connect-1-connect.test.svc"
    messages = [line.split(": ", 1)[1] for line in lines(capsys)]
    assert messages == [
        "requesting a Spark Connect server in namespace test (startup timeout 180s)",
        "driver pod dbt-connect-1-driver created",
        "waiting for the driver pod to be scheduled (Unschedulable: 0/1 nodes are available: "
        "1 Insufficient cpu.)",
        "driver pod scheduled on worker; container spark-kubernetes-driver is ContainerCreating",
        "driver container running",
        "waiting for the Spark Connect endpoint to answer a query",
        "Spark Connect server ready (application dbt-connect-1)",
    ]

    owner._namespace = Mock(return_value=False)
    owner.close(dbt_exit_code=0)
    assert [line.split(": ", 1)[1] for line in lines(capsys)] == [
        "deleting managed Spark compute and observing its removal",
        "managed Spark compute deleted",
    ]


def test_wrapper_reports_progress_that_follows_dbt_flags(tmp_path, monkeypatch):
    import dbt.cli.main

    observed = []

    def invoke(args):
        observed.append(get_invocation_owner().progress.allowed)
        return SimpleNamespace(success=True, exception=None)

    monkeypatch.setattr(dbt.cli.main, "dbtRunner", lambda: SimpleNamespace(invoke=invoke))
    result = CliRunner().invoke(main, ["run", "--receipt-dir", str(tmp_path / "run"), "--", "build"])
    assert result.exit_code == 0
    assert observed == [dbt_allows_progress]
    assert json.loads((tmp_path / "run/receipt.json").read_text())["stage"] == "not_started"
