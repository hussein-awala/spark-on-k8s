from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from click.testing import CliRunner
from kubernetes import client as k8s
from kubernetes.client import ApiException
from spark_on_k8s import client
from spark_on_k8s.cli.app import app_cli
from spark_on_k8s.cli.apps import apps_cli
from spark_on_k8s.utils import app_manager
from spark_on_k8s.utils.run import RunHandle


@pytest.fixture
def cluster(monkeypatch):
    api = Mock()
    pod = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="existing-driver", labels={"spark-app-id": "existing"}),
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
        "run": {
            "namespace": "spark",
            "app_id": pod.metadata.labels["spark-app-id"],
            "driver_pod": pod.metadata.name,
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
        (KeyboardInterrupt(), 130, "interrupted"),
    ],
)
def test_wait_reports_distinct_runtime_failures(cluster, error, code, error_code):
    cluster.api.list_namespaced_pod.side_effect = error
    result = CliRunner().invoke(
        app_cli, ["wait", "--app-id", "existing", "--namespace", "spark", "--output", "json"]
    )
    assert result.exit_code == code, result.output
    assert json.loads(result.stdout)["error"]["code"] == error_code


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
