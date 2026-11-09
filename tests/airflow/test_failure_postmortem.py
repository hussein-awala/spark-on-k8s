from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")

NAMESPACE = "spark"
DRIVER = "application-driver"
XCOM_KEY = "spark_failure_diagnosis"


class RecordingHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records: list[tuple[str, str]] = []

    def emit(self, record):
        self.records.append((record.levelname, record.getMessage()))

    @property
    def messages(self) -> list[str]:
        return [message for _, message in self.records]

    def text(self) -> str:
        return "\n".join(self.messages)


class FakeLogResponse:
    """The raw urllib3 response returned for _preload_content=False."""

    def __init__(self, data: bytes):
        self.data = data
        self.closed = False

    def read(self, amount):
        chunk, self.data = self.data[:amount], self.data[amount:]
        return chunk

    def close(self):
        self.closed = True


def container_status(name="driver", *, state=None, last_state=None):
    from kubernetes import client as k8s

    return k8s.V1ContainerStatus(
        name=name,
        image="spark:test",
        image_id="",
        ready=False,
        restart_count=0,
        state=state,
        last_state=last_state,
    )


def terminated(reason="Error", exit_code=1):
    from kubernetes import client as k8s

    return k8s.V1ContainerState(terminated=k8s.V1ContainerStateTerminated(reason=reason, exit_code=exit_code))


def driver_pod(*, phase="Failed", statuses=None, conditions=None, annotations=None, containers=("driver",)):
    from kubernetes import client as k8s

    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=DRIVER,
            namespace=NAMESPACE,
            uid="driver-uid",
            labels={"spark-role": "driver", "spark-app-id": "application"},
            annotations=annotations,
        ),
        spec=k8s.V1PodSpec(containers=[k8s.V1Container(name=name) for name in containers]),
        status=k8s.V1PodStatus(phase=phase, container_statuses=statuses, conditions=conditions),
    )


def oom_driver():
    return driver_pod(statuses=[container_status(state=terminated("OOMKilled", 137))])


def unscheduled_driver():
    from kubernetes import client as k8s

    message = "0/3 nodes are available: 3 Insufficient memory."
    condition = k8s.V1PodCondition(
        type="PodScheduled", status="False", reason="Unschedulable", message=message
    )
    return driver_pod(phase="Pending", conditions=[condition])


def event(reason, message, uid="driver-uid"):
    from kubernetes import client as k8s

    return k8s.CoreV1Event(
        metadata=k8s.V1ObjectMeta(name=f"event-{reason}"),
        involved_object=k8s.V1ObjectReference(uid=uid),
        reason=reason,
        message=message,
    )


@pytest.fixture
def cluster():
    """A fake Kubernetes API reached only through the task's SparkAppManager client manager."""
    from kubernetes import client as k8s
    from spark_on_k8s.utils.app_manager import SparkAppManager

    timeline: list[str] = []
    api = mock.create_autospec(k8s.CoreV1Api, instance=True)
    api.read_namespaced_pod.return_value = oom_driver()
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[], metadata=k8s.V1ListMeta())
    api.list_namespaced_event.return_value = k8s.CoreV1EventList(
        items=[event("BackOff", "Back-off restarting failed container")], metadata=k8s.V1ListMeta()
    )
    lines = [f"driver line {index}" for index in range(1, 151)]
    lines[-2] = "Traceback: connect jdbc://etl:hunter2@db/warehouse password=hunter2"

    def read_log(**kwargs):
        # Kubernetes applies tail_lines before sending.
        return FakeLogResponse("\n".join(lines[-kwargs["tail_lines"] :]).encode() + b"\n")

    for name in (
        "read_namespaced_pod",
        "list_namespaced_pod",
        "list_namespaced_event",
        "read_namespaced_pod_log",
    ):
        method = getattr(api, name)
        method.side_effect = recorder(timeline, name, method, read_log if name.endswith("_log") else None)

    manager = mock.create_autospec(SparkAppManager, instance=True)
    # A named mock keeps client calls out of the manager's own call list.
    manager.k8s_client_manager = mock.MagicMock(name="k8s_client_manager")
    manager.delete_app.side_effect = lambda **_: timeline.append("delete_app")
    manager.kill_app.side_effect = lambda **_: timeline.append("kill_app")
    factory = mock.Mock(return_value=manager)
    handler = RecordingHandler()
    logger = logging.Logger("test.spark_failure_postmortem", level=logging.DEBUG)
    logger.addHandler(handler)
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    with (
        mock.patch("spark_on_k8s.airflow.operators.SparkAppManager", factory),
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager", factory),
        mock.patch("spark_on_k8s.airflow.postmortem.k8s.CoreV1Api", return_value=api),
        mock.patch.object(SparkOnK8SOperator, "log", new_callable=mock.PropertyMock, return_value=logger),
    ):
        yield SimpleNamespace(api=api, manager=manager, log=handler, timeline=timeline)


def recorder(timeline, name, method, effect=None):
    """Record the call order; return the method's current return value unless given an effect."""

    def call(*args, **kwargs):
        timeline.append(name)
        return effect(*args, **kwargs) if effect else method.return_value

    return call


@pytest.fixture
def task_context():
    return {"ti": mock.Mock(xcom_pull=mock.Mock(return_value=None))}


def make_operator(operator_class="SparkOnK8SOperator", **kwargs):
    from spark_on_k8s.airflow import operators

    defaults = {"task_id": "postmortem", "image": "spark:test", "namespace": NAMESPACE}
    if operator_class == "SparkSqlOnK8SOperator":
        defaults["sql"] = "SELECT 1"
    else:
        defaults["app_path"] = "local:///job.py"
    return getattr(operators, operator_class)(**(defaults | kwargs))


def diagnosis_pushes(context):
    return [
        call.kwargs["value"]
        for call in context["ti"].xcom_push.call_args_list
        if call.kwargs["key"] == XCOM_KEY
    ]


def run_sync(operator, context, status):
    from airflow.exceptions import AirflowException

    with mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value=DRIVER):
        if status == "Succeeded":
            return operator.execute(context)
        with pytest.raises(AirflowException, match=f"^The job finished with status: {status}$") as failure:
            operator.execute(context)
    return failure.value


@pytest.mark.parametrize("operator_class", ["SparkOnK8SOperator", "SparkSqlOnK8SOperator"])
def test_failed_run_logs_diagnosis_and_driver_tail_before_deleting(cluster, task_context, operator_class):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    operator = make_operator(operator_class, on_finish_action="delete")
    run_sync(operator, task_context, "Failed")

    assert cluster.timeline == [
        "read_namespaced_pod",
        "list_namespaced_pod",
        "list_namespaced_event",
        "read_namespaced_pod_log",
        "delete_app",
    ]
    log = cluster.log.text()
    assert f"Spark failure postmortem for {NAMESPACE}/{DRIVER}, which finished with status Failed:" in log
    assert "Diagnosis: 1 finding(s) from 1 pod(s) and 1 event(s)." in log
    assert any(level == "ERROR" and "[error] oom_killed" in m for level, m in cluster.log.records)
    assert "on pod application-driver, container driver" in log
    messages = cluster.log.messages
    header = messages.index("Last 100 driver log line(s) from container driver:")
    tail = messages[header + 1 : messages.index("End of the driver log tail.")]
    assert len(tail) == 100
    assert tail[:2] == ["driver line 51", "driver line 52"]
    assert tail[-2:] == [
        "Traceback: connect jdbc://<redacted>@db/warehouse password=<redacted>",
        "driver line 150",
    ]
    assert "hunter2" not in log
    request = cluster.api.read_namespaced_pod_log.call_args.kwargs
    assert request["container"] == "driver"
    assert request["tail_lines"] == 100
    assert request["limit_bytes"] == 1024 * 1024
    assert request["_preload_content"] is False

    (summary,) = diagnosis_pushes(task_context)
    assert json.loads(json.dumps(summary)) == {
        "version": 1,
        "status": "Failed",
        "namespace": NAMESPACE,
        "app_id": "application",
        "driver_pod": DRIVER,
        "incomplete": False,
        "findings": [{"code": "oom_killed", "severity": "error", "resource": DRIVER, "container": "driver"}],
        "omitted_findings": 0,
    }


@pytest.mark.parametrize("finish_action", ["keep", "keep_if_failed"])
def test_retained_failure_still_gets_a_postmortem(cluster, task_context, finish_action):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    run_sync(make_operator(on_finish_action=finish_action), task_context, "Failed")
    assert cluster.timeline[-1] == "read_namespaced_pod_log"
    cluster.manager.delete_app.assert_not_called()
    assert len(diagnosis_pushes(task_context)) == 1


@pytest.mark.parametrize("waiter", ["wait", "log"])
@pytest.mark.parametrize("deferrable", [False, True])
def test_success_records_no_postmortem(cluster, task_context, waiter, deferrable):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.manager.app_status.return_value = SparkAppStatus.Succeeded
    operator = make_operator(app_waiter=waiter, on_finish_action="delete", deferrable=deferrable)
    if deferrable:
        event = {"namespace": NAMESPACE, "pod_name": DRIVER, "status": "Succeeded"}
        assert operator.execute_complete(task_context, event) == "Succeeded"
    else:
        assert run_sync(operator, task_context, "Succeeded") == SparkAppStatus.Succeeded
    assert cluster.timeline == ["delete_app"]
    cluster.manager.k8s_client_manager.client.assert_not_called()
    assert diagnosis_pushes(task_context) == []
    assert "postmortem" not in cluster.log.text()


def test_no_wait_records_no_postmortem(cluster, task_context):
    operator = make_operator(app_waiter="no_wait", on_finish_action="delete")
    assert run_sync(operator, task_context, "Succeeded") is None
    assert cluster.timeline == []


def kubernetes_failure():
    from kubernetes.client import ApiException

    error = ApiException(status=403, reason="Forbidden")
    error.body = '{"message": "pods is forbidden", "token": "do-not-log"}'
    return error


def run_failure_path(path, operator, context):
    """Run one non-success path to its end, returning the raised task error (if any)."""
    from airflow.exceptions import AirflowException
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    manager = operator_manager()
    if path == "killed":
        operator._driver_pod_name = DRIVER
        with mock.patch("spark_on_k8s.airflow.operators.get_current_context", return_value=context):
            operator.on_kill()
        return None
    if path == "sync_timeout":
        manager.wait_for_app.side_effect = TimeoutError("still pending")
        expected = "^Spark application startup timeout exceeded$"
    elif path == "sync_failed":
        manager.app_status.return_value = SparkAppStatus.Failed
        expected = "^The job finished with status: Failed$"
    else:
        event = {"namespace": NAMESPACE, "pod_name": DRIVER, "status": path.removeprefix("deferred_")}
        if event["status"] == "error":
            event |= {"error": "Kubernetes connection lost", "stacktrace": "Traceback (trigger)"}
        expected = {
            "startup_timeout": "^Spark application startup timeout exceeded$",
            "error": "SparkOnK8STrigger failed: with error: Kubernetes connection lost",
            "Failed": "^The job finished with status: Failed$",
            "Unknown": "^The job finished with status: Unknown$",
        }[event["status"]]
        with pytest.raises(AirflowException, match=expected) as failure:
            operator.execute_complete(context, event)
        return failure.value
    with (
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value=DRIVER),
        pytest.raises(AirflowException, match=expected) as failure,
    ):
        operator.execute(context)
    return failure.value


def operator_manager():
    from spark_on_k8s.airflow import operators

    return operators.SparkAppManager.return_value


FAILURE_PATHS = [
    "sync_failed",
    "sync_timeout",
    "deferred_Failed",
    "deferred_Unknown",
    "deferred_error",
    "deferred_startup_timeout",
    "killed",
]


@pytest.mark.parametrize("path", FAILURE_PATHS)
def test_every_deleting_failure_path_records_a_postmortem_before_cleanup(cluster, task_context, path):
    operator = make_operator(on_finish_action="delete", deferrable=path.startswith("deferred"))
    run_failure_path(path, operator, task_context)
    assert cluster.timeline[0] == "read_namespaced_pod"
    assert cluster.timeline[-1] == "delete_app"
    assert cluster.timeline.count("delete_app") == 1
    assert "Spark failure postmortem for spark/application-driver" in cluster.log.text()
    # on_kill logs only: it may run from a signal handler, so it does not write XCom.
    assert len(diagnosis_pushes(task_context)) == (0 if path == "killed" else 1)


@pytest.mark.parametrize("path", FAILURE_PATHS)
@pytest.mark.parametrize("failing", ["client", "read_pod", "xcom"])
def test_collection_failure_never_changes_the_outcome_or_error(cluster, task_context, path, failing):
    error = kubernetes_failure()
    if failing == "client":
        cluster.manager.k8s_client_manager.client.side_effect = error
    elif failing == "read_pod":
        cluster.api.read_namespaced_pod.side_effect = error
    else:

        def push(key, value):
            if key == XCOM_KEY:
                raise RuntimeError("XCom backend unavailable")

        task_context["ti"].xcom_push.side_effect = push
    operator = make_operator(on_finish_action="delete", deferrable=path.startswith("deferred"))
    failure = run_failure_path(path, operator, task_context)

    # The original error is raised unchanged, and cleanup still happens.
    assert failure is None or failure.__cause__ is None
    assert cluster.timeline[-1] == "delete_app"
    expected = "RuntimeError" if failing == "xcom" else "ApiException 403"
    if failing == "xcom" and path == "killed":
        expected = None  # on_kill never writes XCom.
    lines = [
        m for m in cluster.log.messages if m.startswith("Could not complete the Spark failure postmortem")
    ]
    if expected is None:
        assert lines == []
    else:
        assert lines == [
            f"Could not complete the Spark failure postmortem for spark/{DRIVER} ({expected}); continuing."
        ]
    log = cluster.log.text()
    assert "do-not-log" not in log
    assert "pods is forbidden" not in log


@pytest.mark.parametrize("path", ["deferred_Failed", "deferred_startup_timeout"])
def test_cleanup_failure_still_wins_over_a_successful_postmortem(cluster, task_context, path):
    cleanup_error = RuntimeError("delete request failed")
    cluster.manager.delete_app.side_effect = cleanup_error
    operator = make_operator(on_finish_action="delete", deferrable=True)
    failure = run_failure_path(path, operator, task_context)
    assert failure.__cause__ is cleanup_error
    assert "Spark failure postmortem" in cluster.log.text()


def test_partial_diagnosis_and_tail_failures_are_reported_separately(cluster, task_context):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.api.list_namespaced_pod.side_effect = kubernetes_failure()
    cluster.api.read_namespaced_pod_log.side_effect = kubernetes_failure()
    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    run_sync(make_operator(on_finish_action="delete"), task_context, "Failed")

    log = cluster.log.text()
    assert "Diagnosis: 2 finding(s) from 1 pod(s) and 1 event(s); collection was incomplete." in log
    assert "[warning] collection_incomplete" in log
    assert "Driver log tail unavailable (ApiException 403)." in log
    assert "do-not-log" not in log
    assert cluster.timeline[-1] == "delete_app"
    assert diagnosis_pushes(task_context)[0]["incomplete"] is True


def test_reads_share_one_bounded_budget(cluster, task_context):
    from spark_on_k8s.airflow.postmortem import POSTMORTEM_TIMEOUT
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    run_sync(make_operator(), task_context, "Failed")
    calls = [
        *cluster.api.read_namespaced_pod.call_args_list,
        *cluster.api.list_namespaced_pod.call_args_list,
        *cluster.api.list_namespaced_event.call_args_list,
        *cluster.api.read_namespaced_pod_log.call_args_list,
    ]
    assert len(calls) == 4
    for call in calls:
        assert 0 < call.kwargs["_request_timeout"] <= POSTMORTEM_TIMEOUT
    # The diagnosis leaves part of the budget for the log tail.
    assert cluster.api.list_namespaced_pod.call_args.kwargs["_request_timeout"] <= 3


@pytest.mark.parametrize("deferrable", [False, True])
def test_expired_budget_stops_reading_and_cleanup_proceeds(cluster, task_context, deferrable):
    clock = [1000.0]
    read_pod = cluster.api.read_namespaced_pod.side_effect

    def slow_read(**kwargs):
        clock[0] += 60  # The driver read used up the budget.
        return read_pod(**kwargs)

    cluster.api.read_namespaced_pod.side_effect = slow_read
    operator = make_operator(on_finish_action="delete", deferrable=deferrable)
    with mock.patch("spark_on_k8s.utils.run.time.monotonic", side_effect=lambda: clock[0]):
        run_failure_path("deferred_Failed" if deferrable else "sync_timeout", operator, task_context)

    assert cluster.timeline == ["read_namespaced_pod", "delete_app"]
    log = cluster.log.text()
    assert "Diagnosis unavailable (TimeoutError)." in log
    assert "Driver log tail unavailable (TimeoutError)." in log


def test_log_read_stopping_at_the_deadline_keeps_received_lines(cluster, task_context):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    clock = [1000.0]
    chunks = iter([b"first line\nsecond line\n", b"never read\n"])

    class SlowResponse(FakeLogResponse):
        def read(self, amount):
            clock[0] += 6  # This read outlasts the budget.
            return next(chunks)

    response = SlowResponse(b"")
    cluster.api.read_namespaced_pod_log.side_effect = None
    cluster.api.read_namespaced_pod_log.return_value = response
    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    operator = make_operator(on_finish_action="delete", failure_diagnostics=False)
    with mock.patch("spark_on_k8s.utils.run.time.monotonic", side_effect=lambda: clock[0]):
        run_sync(operator, task_context, "Failed")

    messages = cluster.log.messages
    header = "Last 2 driver log line(s) from container driver (the read stopped early (TimeoutError)):"
    assert messages[messages.index(header) + 1 :][:3] == [
        "first line",
        "second line",
        "End of the driver log tail.",
    ]
    assert response.closed
    assert cluster.timeline[-1] == "delete_app"


@pytest.mark.parametrize(
    "diagnostics,tail_lines,expected_reads",
    [
        (False, 0, []),
        (False, 25, ["read_namespaced_pod", "read_namespaced_pod_log"]),
        (True, 0, ["read_namespaced_pod", "list_namespaced_pod", "list_namespaced_event"]),
    ],
)
def test_arguments_disable_parts_of_the_postmortem(
    cluster, task_context, diagnostics, tail_lines, expected_reads
):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    operator = make_operator(
        on_finish_action="delete", failure_diagnostics=diagnostics, failure_log_tail_lines=tail_lines
    )
    run_sync(operator, task_context, "Failed")
    assert cluster.timeline == [*expected_reads, "delete_app"]
    if not expected_reads:
        cluster.manager.k8s_client_manager.client.assert_not_called()
        assert "postmortem" not in cluster.log.text()
    if tail_lines:
        assert cluster.api.read_namespaced_pod_log.call_args.kwargs["tail_lines"] == tail_lines
    assert len(diagnosis_pushes(task_context)) == int(diagnostics)


@pytest.mark.parametrize("value", [-1, 1001, True, 10.5, "100"])
def test_invalid_tail_line_count_is_rejected(value):
    with pytest.raises(ValueError, match="failure_log_tail_lines must be an integer from 0 to 1000"):
        make_operator(failure_log_tail_lines=value)


@pytest.mark.parametrize("deferrable", [False, True])
def test_streamed_driver_log_is_not_repeated(cluster, task_context, deferrable):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    operator = make_operator(app_waiter="log", on_finish_action="delete", deferrable=deferrable)
    run_failure_path("deferred_Failed" if deferrable else "sync_failed", operator, task_context)
    cluster.manager.stream_logs.assert_called_once()
    assert cluster.timeline == [
        "read_namespaced_pod",
        "list_namespaced_pod",
        "list_namespaced_event",
        "delete_app",
    ]
    assert "Diagnosis: 1 finding(s)" in cluster.log.text()


def test_trigger_error_includes_the_tail_because_logs_were_not_replayed(cluster, task_context):
    operator = make_operator(app_waiter="log", on_finish_action="delete", deferrable=True)
    failure = run_failure_path("deferred_error", operator, task_context)
    assert "Traceback (trigger)" in str(failure)
    cluster.manager.stream_logs.assert_not_called()
    assert cluster.timeline[-2:] == ["read_namespaced_pod_log", "delete_app"]
    assert diagnosis_pushes(task_context)[0]["status"] == "error"


@pytest.mark.parametrize("deferrable", [False, True])
def test_startup_timeout_explains_scheduling_without_reading_logs(cluster, task_context, deferrable):
    from kubernetes import client as k8s

    cluster.api.read_namespaced_pod.return_value = unscheduled_driver()
    cluster.api.list_namespaced_event.return_value = k8s.CoreV1EventList(
        items=[event("FailedScheduling", "0/3 nodes are available: 3 Insufficient memory.")],
        metadata=k8s.V1ListMeta(),
    )
    operator = make_operator(on_finish_action="keep", deferrable=deferrable, startup_timeout=30)
    run_failure_path("deferred_startup_timeout" if deferrable else "sync_timeout", operator, task_context)

    assert "read_namespaced_pod_log" not in cluster.timeline
    assert cluster.timeline[-1] == "delete_app"
    log = cluster.log.text()
    assert "which exceeded its startup timeout:" in log
    assert "[error] unschedulable" in log
    assert "[error] insufficient_resources" in log
    assert "Driver container driver has not started; no driver log is available." in log
    (summary,) = diagnosis_pushes(task_context)
    assert summary["status"] == "startup_timeout"
    assert [finding["code"] for finding in summary["findings"]] == ["unschedulable", "insufficient_resources"]


@pytest.mark.parametrize("kill_action", ["keep", "kill"])
def test_kill_actions_that_keep_the_driver_record_no_postmortem(cluster, task_context, kill_action):
    operator = make_operator(on_kill_action=kill_action)
    run_failure_path("killed", operator, task_context)
    assert "read_namespaced_pod" not in cluster.timeline
    assert "postmortem" not in cluster.log.text()


def test_kill_after_streaming_logs_skips_the_tail(cluster, task_context):
    operator = make_operator(app_waiter="log")
    operator._driver_log_streamed = True
    run_failure_path("killed", operator, task_context)
    assert cluster.timeline == [
        "read_namespaced_pod",
        "list_namespaced_pod",
        "list_namespaced_event",
        "delete_app",
    ]
    assert "which is being stopped because the task was killed:" in cluster.log.text()


def test_driver_tail_uses_the_selected_driver_container(cluster, task_context):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    pod = driver_pod(
        statuses=[container_status("spark-kubernetes-driver", state=terminated())],
        annotations={"kubectl.kubernetes.io/default-container": "spark-kubernetes-driver"},
        containers=("istio-proxy", "spark-kubernetes-driver"),
    )
    cluster.api.read_namespaced_pod.return_value = pod
    cluster.manager.app_status.return_value = SparkAppStatus.Failed
    run_sync(make_operator(), task_context, "Failed")
    assert cluster.api.read_namespaced_pod_log.call_args.kwargs["container"] == "spark-kubernetes-driver"


def run_tail(data: bytes, tail_lines=100):
    from spark_on_k8s.airflow.postmortem import _read_tail
    from spark_on_k8s.utils.run import WaitDeadline

    api = mock.Mock()
    # Like Kubernetes, stop sending at limit_bytes.
    api.read_namespaced_pod_log.side_effect = lambda **kwargs: FakeLogResponse(data[: kwargs["limit_bytes"]])
    return _read_tail(api, DRIVER, NAMESPACE, "driver", tail_lines, WaitDeadline(5))


def test_tail_bounds_long_lines_total_size_and_transfer():
    from spark_on_k8s.airflow import postmortem

    lines, notes = run_tail(b"short\n" + b"x" * 5000 + b"\n")
    assert lines[0] == "short"
    assert lines[1] == "x" * 2000 + " ... [3000 characters omitted]"
    assert notes == []

    lines, notes = run_tail(b"".join(b"%04d" % index + b"y" * 1996 + b"\n" for index in range(100)))
    assert sum(len(line) + 1 for line in lines) <= postmortem._LOG_TAIL_CHARS
    assert lines[-1].startswith("0099")
    assert notes == [f"{100 - len(lines)} earlier line(s) omitted by the size limit"]

    with mock.patch.object(postmortem, "_LOG_TAIL_TRANSFER_BYTES", 16):
        lines, notes = run_tail(b"0123456789\n" * 3)
    assert lines == ["0123456789", "01234"]
    assert notes == ["the transfer limit was reached, so the most recent lines may be missing"]


def test_tail_decodes_invalid_utf8_and_a_split_character():
    lines, notes = run_tail(b"caf\xc3\xa9\nbinary \xff\xfe\nsplit \xc3")
    replacement = "\N{REPLACEMENT CHARACTER}"
    assert lines == ["caf\u00e9", f"binary {replacement * 2}", f"split {replacement}"]
    assert notes == []
