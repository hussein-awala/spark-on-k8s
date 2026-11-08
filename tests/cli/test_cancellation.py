from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from click.testing import CliRunner
from kubernetes import client as k8s
from kubernetes.client import ApiException
from spark_on_k8s.cli.app import app_cli
from spark_on_k8s.cli.receipts import receipt_writer
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.run import RunHandle


@pytest.fixture
def cancellation(tmp_path, monkeypatch):
    run = RunHandle(
        namespace="analytics", app_id="daily-job", driver_pod="daily-job-driver", driver_uid="original-uid"
    )
    receipt = tmp_path / "run.json"
    with receipt_writer(receipt) as save:
        save(run)
    api = Mock()
    api.read_namespaced_pod.return_value = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=run.driver_pod,
            uid=run.driver_uid,
            labels={"spark-app-id": run.app_id, "spark-role": "driver"},
        ),
        status=k8s.V1PodStatus(phase="Running"),
    )
    factory = Mock(return_value=api)
    monkeypatch.setattr(k8s, "CoreV1Api", factory)
    monkeypatch.setattr(KubernetesClientManager, "client", MagicMock())
    return SimpleNamespace(run=run, receipt=receipt, api=api, factory=factory)


@pytest.mark.parametrize("output", ["json", "text"])
@pytest.mark.parametrize("force", [False, True])
def test_cancel_cli_uses_saved_uid_through_sdk_and_only_requests_deletion(cancellation, output, force):
    args = ["cancel", "--receipt", str(cancellation.receipt), "--output", output]
    result = CliRunner().invoke(app_cli, [*args, *(["--force"] if force else [])])
    assert result.exit_code == 0, result.output
    cancellation.api.read_namespaced_pod.assert_called_once_with(
        name=cancellation.run.driver_pod,
        namespace=cancellation.run.namespace,
    )
    cancellation.api.delete_namespaced_pod.assert_called_once()
    request = cancellation.api.delete_namespaced_pod.call_args.kwargs
    assert request["name"] == cancellation.run.driver_pod
    assert request["namespace"] == cancellation.run.namespace
    assert request["body"].preconditions.uid == cancellation.run.driver_uid
    assert request["body"].propagation_policy == "Foreground"
    assert request["body"].grace_period_seconds == (0 if force else None)
    if output == "json":
        assert json.loads(result.stdout) == {
            "schema_version": 1,
            "operation": "cancel",
            "namespace": "analytics",
            "error": None,
            "run": cancellation.run.to_dict(),
            "accepted": True,
        }
    else:
        assert "Cancellation accepted; resource cleanup may still be in progress." in result.stdout
    cancellation.api.list_namespaced_pod.assert_not_called()
    cancellation.api.delete_namespaced_service.assert_not_called()
    cancellation.api.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("mismatch", ["uid", "application", "role"])
def test_cancel_cli_rejects_stale_or_misdirected_receipt_without_mutation(cancellation, mismatch):
    metadata = cancellation.api.read_namespaced_pod.return_value.metadata
    if mismatch == "uid":
        metadata.uid = "replacement-uid"
    elif mismatch == "application":
        metadata.labels["spark-app-id"] = "other-job"
    else:
        metadata.labels["spark-role"] = "executor"
    result = CliRunner().invoke(
        app_cli, ["cancel", "--receipt", str(cancellation.receipt), "--output", "json"]
    )
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["run"] == cancellation.run.to_dict()
    assert payload["error"]["code"] == "operation_error"
    assert "identity does not match" in payload["error"]["message"]
    assert "accepted" not in payload
    cancellation.api.delete_namespaced_pod.assert_not_called()


def test_cancel_cli_not_found_is_a_structured_failure_without_claiming_acceptance(cancellation):
    cancellation.api.read_namespaced_pod.side_effect = ApiException(
        status=404,
        reason="Not Found",
        http_resp=None,
    )
    cancellation.api.read_namespaced_pod.side_effect.body = "private upstream content"
    result = CliRunner().invoke(
        app_cli, ["cancel", "--receipt", str(cancellation.receipt), "--output", "json"]
    )
    assert result.exit_code == 4
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "not_found"
    assert payload["run"] == cancellation.run.to_dict()
    assert "accepted" not in payload
    assert "private upstream content" not in result.output
    cancellation.api.delete_namespaced_pod.assert_not_called()


def test_cancel_cli_missing_receipt_is_a_usage_error(cancellation):
    cancellation.receipt.unlink()
    result = CliRunner().invoke(
        app_cli, ["cancel", "--receipt", str(cancellation.receipt), "--output", "json"]
    )
    assert result.exit_code == 2
    assert result.stdout == ""
    assert "does not exist" in result.stderr
    cancellation.factory.assert_not_called()


@pytest.mark.parametrize("output", ["json", "text"])
@pytest.mark.parametrize(
    "invalid,expected",
    [
        ("corrupt", "Expecting property name"),
        ("oversize", "exceeds 64 KiB"),
        ("no_uid", "driver_uid"),
        ("unreadable", "receipt read denied"),
    ],
)
def test_cancel_cli_rejects_unusable_receipts_before_cluster_access(
    cancellation, monkeypatch, invalid, expected, output
):
    if invalid == "corrupt":
        cancellation.receipt.write_text("{invalid")
    elif invalid == "oversize":
        cancellation.receipt.write_bytes(b" " * 65537)
    elif invalid == "no_uid":
        run = cancellation.run.to_dict()
        run.pop("driver_uid")
        cancellation.receipt.write_text(json.dumps({"schema_version": 1, "run": run}))
    else:
        monkeypatch.setattr(
            "spark_on_k8s.cli.receipts.Path.open", Mock(side_effect=PermissionError("receipt read denied"))
        )
    result = CliRunner().invoke(
        app_cli, ["cancel", "--receipt", str(cancellation.receipt), "--output", output]
    )
    assert result.exit_code == 1
    if output == "json":
        # Like connect server receipt errors, a runtime receipt failure is one versioned document.
        payload = json.loads(result.stdout)
        error = payload.pop("error")
        assert payload == {"schema_version": 1, "operation": "cancel", "namespace": "", "run": None}
        assert error["code"] == "operation_error"
        assert expected in error["message"]
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert expected in result.stderr
    cancellation.factory.assert_not_called()
    cancellation.api.delete_namespaced_pod.assert_not_called()


def test_sdk_refuses_name_only_legacy_handle_before_cluster_access(cancellation):
    legacy = RunHandle(namespace="analytics", app_id="daily-job", driver_pod="daily-job-driver")
    with pytest.raises(ValueError, match="driver UID"):
        SparkOnK8S().cancel_run(legacy)
    cancellation.factory.assert_not_called()
