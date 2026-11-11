from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner
from kubernetes import client as k8s
from kubernetes.client import ApiException
from spark_on_k8s import client
from spark_on_k8s.cli.app import app_cli
from spark_on_k8s.cli.apps import apps_cli
from spark_on_k8s.utils import app_manager, run as run_module
from spark_on_k8s.utils.run import RunHandle
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, NewConnectionError, ReadTimeoutError


@pytest.fixture
def cluster(monkeypatch):
    api = Mock()
    pod = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="existing-driver", labels={"spark-app-id": "existing"}),
        spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="driver"), k8s.V1Container(name="metrics")]),
        status=k8s.V1PodStatus(phase="Succeeded"),
    )
    api.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])
    api.read_namespaced_pod.return_value = pod

    def create(namespace, body):
        body.metadata.uid = "test-uid"
        return body

    api.create_namespaced_pod.side_effect = create
    monkeypatch.setattr(k8s, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(client.KubernetesClientManager, "client", MagicMock())
    watcher = Mock()
    watcher.stream.return_value = iter(["driver output", "<literal> data"])
    monkeypatch.setattr(app_manager.watch, "Watch", Mock(return_value=watcher))
    return SimpleNamespace(api=api, pod=pod, watcher=watcher)


def submit_args(*flags):
    return [
        "submit",
        "--namespace",
        "spark",
        "--image",
        "spark:test",
        "--path",
        "local:///app.py",
        "--name",
        "automation",
        *flags,
    ]


@pytest.mark.parametrize("phase,code", [("Succeeded", 0), ("Failed", 3), ("Unknown", 6)])
@pytest.mark.parametrize("mode", ["wait", "submit-wait", "submit-logs"])
def test_terminal_workload_status_controls_exit_code(cluster, phase, code, mode):
    cluster.pod.status.phase = phase
    args = (
        ["wait", "--namespace", "spark", "--app-id", "existing"]
        if mode == "wait"
        else submit_args("--wait" if mode == "submit-wait" else "--logs")
    )
    result = CliRunner().invoke(app_cli, [*args, "--output", "json"])
    assert result.exit_code == code, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 1
    assert payload["status"] == phase
    assert payload["error"] is None
    if mode == "submit-logs":
        assert "driver output" in result.stderr
        assert "driver output" not in result.stdout
        cluster.watcher.stop.assert_called_once()


def test_submission_json_is_a_stable_receipt_without_secrets(cluster):
    result = CliRunner().invoke(
        app_cli, submit_args("--output", "json", "--secret-env-var", "TOKEN=private-value")
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    pod = cluster.api.create_namespaced_pod.call_args.kwargs["body"]
    assert payload == {
        "schema_version": 1,
        "operation": "submit",
        "namespace": "spark",
        "error": None,
        "status": None,
        "submission_phase": "resources_created",
        "run": {
            "namespace": "spark",
            "app_id": pod.metadata.labels["spark-app-id"],
            "driver_pod": pod.metadata.name,
            "driver_uid": "test-uid",
        },
    }
    assert "private-value" not in result.stdout + result.stderr
    cluster.api.read_namespaced_pod.assert_not_called()


def test_json_list_and_status_preserve_identity(cluster):
    listing = CliRunner().invoke(apps_cli, ["list", "--namespace", "spark", "--output", "json"])
    assert listing.exit_code == 0
    assert json.loads(listing.stdout) == {
        "schema_version": 1,
        "operation": "list",
        "namespace": "spark",
        "error": None,
        "items": [{"app_id": "existing", "namespace": "spark"}],
    }
    cluster.pod.status.phase = "Failed"
    status = CliRunner().invoke(
        app_cli, ["status", "--namespace", "spark", "--app-id", "existing", "--output", "json"]
    )
    assert status.exit_code == 0  # Inspecting a failed run is a successful query.
    assert json.loads(status.stdout)["status"] == "Failed"


@pytest.mark.parametrize(
    "error,code,error_code",
    [
        (ApiException(status=404, reason="Missing"), 4, "not_found"),
        (ApiException(status=403, reason="Forbidden"), 1, "api_error"),
        (TimeoutError("budget elapsed"), 5, "timeout"),
        # The Kubernetes client surfaces request timeouts wrapped by urllib3's retry layer.
        (
            MaxRetryError(None, "/api/v1/pods", reason=ReadTimeoutError(None, "/api/v1/pods", "slow")),
            5,
            "timeout",
        ),
        (MaxRetryError(None, "/api/v1/pods", reason=ConnectTimeoutError("connect timed out")), 5, "timeout"),
        (
            MaxRetryError(None, "/api/v1/pods", reason=NewConnectionError(None, "refused")),
            1,
            "operation_error",
        ),
        (KeyboardInterrupt(), 130, "interrupted"),
    ],
)
def test_wait_reports_distinct_runtime_failures(cluster, monkeypatch, error, code, error_code):
    cluster.api.list_namespaced_pod.side_effect = error
    # Connection failures are retried a few times at the poll interval before they are reported.
    monkeypatch.setattr(run_module.time, "sleep", Mock())
    result = CliRunner().invoke(
        app_cli, ["wait", "--app-id", "existing", "--namespace", "spark", "--output", "json"]
    )
    assert result.exit_code == code, result.output
    assert json.loads(result.stdout)["error"]["code"] == error_code


@pytest.mark.parametrize("phase,code", [("Succeeded", 0), ("Failed", 3), ("Running", 5)])
@pytest.mark.parametrize("mode", ["wait", "submit-wait"])
def test_final_status_read_within_the_deadline_is_not_reported_as_timeout(
    cluster, monkeypatch, phase, code, mode
):
    clock = [0.0]
    monkeypatch.setattr(run_module, "time", SimpleNamespace(monotonic=lambda: clock[0], sleep=Mock()))
    cluster.pod.status.phase = phase
    # The first read takes half the budget; later reads end exactly as the remaining budget expires.
    durations = iter([0.5] + [10.0] * 10)

    def respond(result):
        def read(*args, **kwargs):
            clock[0] += min(next(durations), kwargs["_request_timeout"])
            return result

        return read

    cluster.api.list_namespaced_pod.side_effect = respond(SimpleNamespace(items=[cluster.pod]))
    cluster.api.read_namespaced_pod.side_effect = respond(cluster.pod)
    args = (
        ["wait", "--namespace", "spark", "--app-id", "existing"] if mode == "wait" else submit_args("--wait")
    )
    result = CliRunner().invoke(app_cli, [*args, "--timeout", "1", "--output", "json"])
    assert result.exit_code == code, result.output
    payload = json.loads(result.stdout)
    if code == 5:
        assert payload["error"]["code"] == "timeout"
        assert payload["status"] is None
    else:
        assert payload["error"] is None
        assert payload["status"] == phase


def test_waiting_submission_keeps_receipt_on_timeout(cluster, monkeypatch):
    monkeypatch.setattr(
        app_manager.SparkAppManager, "wait_for_app", Mock(side_effect=TimeoutError("budget elapsed"))
    )
    result = CliRunner().invoke(app_cli, submit_args("--wait", "--timeout", "1", "--output", "json"))
    assert result.exit_code == 5
    payload = json.loads(result.stdout)
    assert (
        payload["run"]["driver_pod"]
        == cluster.api.create_namespaced_pod.call_args.kwargs["body"].metadata.name
    )
    assert payload["error"]["code"] == "timeout"
    cluster.api.delete_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
def test_invalid_timeout_cannot_create_a_pod(cluster, value):
    result = CliRunner().invoke(app_cli, submit_args("--wait", "--timeout", value))
    assert result.exit_code != 0
    cluster.api.create_namespaced_pod.assert_not_called()


def test_submit_run_is_additive_and_receipt_is_immutable(cluster):
    spark = client.SparkOnK8S()
    assert spark.last_submitted_app_id is None
    assert spark.last_submitted_run is None
    run = spark.submit_run(
        image="spark:test",
        app_path="local:///app.py",
        namespace="spark",
        app_name="receipt",
        app_waiter="no_wait",
    )
    assert isinstance(run, RunHandle)
    assert run == spark.last_submitted_run
    assert run.app_id == spark.last_submitted_app_id
    assert run.driver_pod == cluster.api.create_namespaced_pod.call_args.kwargs["body"].metadata.name
    with pytest.raises(FrozenInstanceError):
        run.namespace = "other"
    previous = run.to_dict()
    pod_name = spark.submit_app(
        image="spark:test",
        app_path="local:///app.py",
        namespace="other",
        app_name="other",
        app_waiter="no_wait",
    )
    assert isinstance(pod_name, str)
    assert run.to_dict() == previous
    assert spark.last_submitted_run.namespace == "other"
    assert spark.last_submitted_app_id == spark.last_submitted_run.app_id
    assert spark.last_submitted_app_id != run.app_id
    cluster.api.create_namespaced_pod.side_effect = ApiException(status=403)
    with pytest.raises(ApiException):
        spark.submit_run(image="spark:test", app_path="local:///app.py", namespace="other")
    assert spark.last_submitted_run is None
    assert spark.last_submitted_app_id is None


def test_submit_run_rejects_dry_run_without_creating_resources(cluster):
    with pytest.raises(ValueError, match="real submission"):
        client.SparkOnK8S().submit_run(dry_run=True)
    cluster.api.create_namespaced_pod.assert_not_called()


def test_text_wait_reports_failed_workload_with_nonzero_exit(cluster):
    cluster.pod.status.phase = "Failed"
    result = CliRunner().invoke(app_cli, ["wait", "--namespace", "spark", "--app-id", "existing"])
    assert result.exit_code == 3
    assert "terminated with status Failed" in result.stdout


def test_submission_logs_eof_does_not_imply_success(cluster, monkeypatch):
    cluster.pod.status.phase = "Running"
    final = k8s.V1Pod(status=k8s.V1PodStatus(phase="Failed"))
    cluster.api.read_namespaced_pod.side_effect = [cluster.pod, cluster.pod, final, final]
    monkeypatch.setattr(app_manager.time, "sleep", Mock())
    result = CliRunner().invoke(app_cli, submit_args("--logs", "--output", "json"))
    assert result.exit_code == 3
    assert json.loads(result.stdout)["status"] == "Failed"
    assert cluster.api.read_namespaced_pod.call_count == 4


def test_api_error_json_does_not_expose_kubernetes_response_body(cluster):
    error = ApiException(status=403, reason="Forbidden")
    error.body = '{"submitted_secret":"do-not-print-this"}'
    cluster.api.list_namespaced_pod.side_effect = error
    result = CliRunner().invoke(apps_cli, ["list", "--namespace", "spark", "--output", "json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["items"] is None
    assert "do-not-print-this" not in result.stdout + result.stderr


@pytest.mark.parametrize("output", ["json", "text"])
def test_unexpected_cluster_errors_are_reported_without_false_success(cluster, output):
    cluster.api.list_namespaced_pod.side_effect = RuntimeError("Cluster transport unavailable")
    result = CliRunner().invoke(
        app_cli, ["status", "--namespace", "spark", "--app-id", "existing", "--output", output]
    )
    assert result.exit_code == 1
    if output == "json":
        payload = json.loads(result.stdout)
        assert payload["error"] == {"code": "operation_error", "message": "Cluster transport unavailable"}
        assert payload["status"] is None
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert result.stderr == "Error: Cluster transport unavailable\n"


def test_submission_timeout_requires_observation_before_any_write():
    import click
    from spark_on_k8s.cli.automation import execute_submission

    spark = Mock()
    with pytest.raises(click.UsageError, match="requires a wait or log waiter"):
        execute_submission(spark, {"app_waiter": "no_wait"}, timeout=1)
    spark.submit_app.assert_not_called()


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_shared_submission_helper_rejects_invalid_deadlines_before_any_write(timeout):
    import click
    from spark_on_k8s.cli.automation import execute_submission

    spark = Mock()
    # Job submission integrations call this helper directly, bypassing Click's option validation.
    with pytest.raises(click.BadParameter, match="positive finite"):
        execute_submission(spark, {"app_waiter": "wait"}, timeout=timeout)
    spark.submit_app.assert_not_called()


def test_cli_does_not_claim_acceptance_without_a_run_receipt(cluster, monkeypatch):
    submit = Mock(return_value="untracked-driver")
    monkeypatch.setattr(client.SparkOnK8S, "submit_app", submit)
    result = CliRunner().invoke(app_cli, submit_args("--output", "json"))
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["run"] is None
    assert payload["status"] is None
    assert payload["error"] == {
        "code": "operation_error",
        "message": "Submission completed without a run identity",
    }
    assert "Submitted" not in result.stderr
    submit.assert_called_once()
    cluster.api.create_namespaced_pod.assert_not_called()


def test_sdk_does_not_return_a_handle_when_custom_submission_omits_the_receipt(cluster, monkeypatch):
    spark = client.SparkOnK8S()
    submit = Mock(return_value="untracked-driver")
    monkeypatch.setattr(spark, "submit_app", submit)
    with pytest.raises(RuntimeError, match="without a run identity"):
        spark.submit_run(image="spark:test", app_path="local:///app.py")
    assert spark.last_submitted_run is None
    submit.assert_called_once_with(image="spark:test", app_path="local:///app.py")
    cluster.api.create_namespaced_pod.assert_not_called()


def test_text_waiting_submission_reports_identity_and_success(cluster):
    result = CliRunner().invoke(app_cli, submit_args("--wait"))
    assert result.exit_code == 0, result.output
    actual = cluster.api.create_namespaced_pod.call_args.kwargs["body"]
    app_id = actual.metadata.labels["spark-app-id"]
    assert f"Submitted {app_id} in spark (driver {actual.metadata.name})" in result.stdout
    assert f"App {app_id} has terminated with status Succeeded" in result.stdout
    assert result.stderr == ""


_LIST_MANY_APPLICATIONS = """
import sys
from unittest import mock
from spark_on_k8s.utils.app_manager import SparkAppManager

applications = [f"app-{index:06d}" for index in range(50000)]
with mock.patch.object(SparkAppManager, "list_apps", return_value=applications):
    from spark_on_k8s.cli import main

    sys.argv = ["spark-on-k8s", "apps", "list", "--namespace", "spark"]
    main()
"""


def test_text_output_ends_quietly_when_the_reader_closes_the_pipe():
    # `spark-on-k8s apps list | head -1`: the reader leaves after the first line.
    environment = {key: value for key, value in os.environ.items() if not key.startswith("SPARK_ON_K8S_")}
    process = subprocess.Popen(
        [sys.executable, "-c", _LIST_MANY_APPLICATIONS],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    try:
        assert process.stdout.readline() == b"app-000000\n"
        process.stdout.close()
        _, stderr = process.communicate(timeout=60)
    finally:
        process.kill()
        process.wait()
    # Click's convention for a closed pipe, as `apps list --output json` and `app logs` already follow.
    assert (process.returncode, stderr) == (1, b"")


def test_a_driver_that_vanishes_during_the_wait_is_not_found_not_finished(cluster):
    from spark_on_k8s.utils.spark_app_status import SparkAppDeletedError

    deleted = SparkAppDeletedError(
        "App existing was deleted, or does not exist, before a final status was observed"
    )
    with patch("spark_on_k8s.utils.app_manager.SparkAppManager.wait_for_app", side_effect=deleted):
        result = CliRunner().invoke(
            app_cli, ["wait", "--app-id", "existing", "--namespace", "spark", "--output", "json"]
        )
    assert result.exit_code == 4, result.output
    document = json.loads(result.stdout)
    assert document["error"] == {"code": "not_found", "message": str(deleted)}
    assert document.get("status") is None
