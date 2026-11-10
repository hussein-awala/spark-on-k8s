from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from click.testing import CliRunner
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.cli import connect_server as cli
from spark_on_k8s.cli.connect import connect_cli
from spark_on_k8s.connect_server import ConnectServerCreateError, ConnectServerHandle, ConnectServerTimeout
from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.run import RunHandle


@pytest.fixture
def server(tmp_path, monkeypatch):
    handle = ConnectServerHandle(
        run=RunHandle(
            namespace="spark", app_id="notebook-123", driver_pod="notebook-123-driver", driver_uid="pod-uid"
        ),
        service_name="notebook-123-connect",
        service_uid="grpc-uid",
        driver_service_uid="rpc-uid",
        port=15002,
    )
    client = Mock()
    client.create_connect_server.return_value = handle
    client.last_submitted_run = None
    client.last_submission_phase = None
    client.connect_server_status.return_value = {
        "status": "running",
        "driver_phase": "Running",
        "query_ready": None,
    }
    client.wait_connect_server_ready.return_value = {
        "status": "ready",
        "query_ready": True,
        "probe": {"spark_version": "4.3.0", "app_id": handle.run.app_id},
    }
    client.delete_connect_server.return_value = {"status": "deletion_requested", "requested": []}
    factory = Mock(return_value=client)
    monkeypatch.setattr(cli, "_client", factory)
    receipt = tmp_path / "server.json"
    profile = tmp_path / "profile.toml"
    profile.write_text('[profiles.notebook]\nendpoint="sc://localhost:15002"\ntls=false\n')
    return handle, client, factory, receipt, profile


def create_args(receipt, output="json"):
    return [
        "server",
        "create",
        "--image",
        "apache/spark:4.3.0",
        "--server-jar",
        "local:///opt/spark/jars/connect.jar",
        "--namespace",
        "spark",
        "--service-account",
        "spark",
        "--receipt-file",
        str(receipt),
        "--output",
        output,
    ]


def save(handle, receipt):
    receipt.write_text(json.dumps({"schema_version": 1, "server": handle.to_dict()}))


@pytest.mark.parametrize("output", ["json", "text"])
def test_create_persists_identity_and_does_not_claim_ready(server, output):
    handle, client, factory, receipt, _ = server

    def submit(config):
        print("submission progress")
        assert config.namespace == "spark"
        assert config.executor_instances.initial == 2
        assert config.spark_conf == {}
        assert config.packages == []
        return handle

    client.create_connect_server.side_effect = submit
    result = CliRunner().invoke(connect_cli, create_args(receipt, output))
    assert result.exit_code == 0, result.output
    assert json.loads(receipt.read_text()) == {"schema_version": 1, "server": handle.to_dict()}
    assert not list(receipt.parent.glob(".spark-receipt-*"))
    if output == "json":
        value = json.loads(result.stdout)
        assert value["operation"] == "connect_server_create"
        assert value["server"] == handle.to_dict()
        assert value["endpoint"] == handle.endpoint
        assert value["query_ready"] is False
        assert value["error"] is None
        assert "submission progress" in result.stderr
    else:
        assert "query readiness has not been checked" in result.stdout
    client.wait_connect_server_ready.assert_not_called()


def test_create_maps_explicit_resources_and_configuration(server):
    _, client, _, receipt, _ = server
    result = CliRunner().invoke(
        connect_cli,
        create_args(receipt)
        + [
            "--driver-memory",
            "512",
            "--executor-memory",
            "768",
            "--executors",
            "1",
            "--port",
            "16000",
            "--conf",
            "spark.sql.shuffle.partitions=4",
            "--package",
            "example:connector:1.0",
            "--image-pull-policy",
            "Never",
            "--max-runtime",
            "14400",
        ],
    )
    assert result.exit_code == 0, result.output
    config = client.create_connect_server.call_args.args[0]
    assert (
        config.driver_resources.memory,
        config.executor_resources.memory,
        config.executor_instances.initial,
    ) == (512, 768, 1)
    assert config.port == 16000
    assert config.spark_conf == {"spark.sql.shuffle.partitions": "4"}
    assert config.packages == ["example:connector:1.0"]
    assert config.image_pull_policy == "Never"
    assert config.max_runtime == 14400


def test_create_sets_no_max_runtime_by_default(server):
    _, client, _, receipt, _ = server
    result = CliRunner().invoke(connect_cli, create_args(receipt))
    assert result.exit_code == 0, result.output
    assert client.create_connect_server.call_args.args[0].max_runtime is None


def test_existing_receipt_prevents_any_cluster_access(server):
    _, client, factory, receipt, _ = server
    receipt.write_text("keep me")
    result = CliRunner().invoke(connect_cli, create_args(receipt))
    assert result.exit_code == 1
    assert json.loads(result.stdout)["server"] is None
    factory.assert_not_called()
    client.create_connect_server.assert_not_called()
    assert receipt.read_text() == "keep me"


@pytest.mark.parametrize("output", ["json", "text"])
def test_receipt_publication_race_preserves_server_identity_and_competing_file(server, output):
    handle, client, _, receipt, _ = server

    def submit(config):
        receipt.write_text("another writer")
        return handle

    client.create_connect_server.side_effect = submit
    result = CliRunner().invoke(connect_cli, create_args(receipt, output))
    assert result.exit_code == 1
    assert receipt.read_text() == "another writer"
    assert not list(receipt.parent.glob(".spark-receipt-*"))
    if output == "json":
        assert json.loads(result.stdout)["server"] == handle.to_dict()
    else:
        assert handle.run.driver_uid in result.stderr
    client.delete_connect_server.assert_not_called()


@pytest.mark.parametrize("full_handle", [False, True])
def test_create_failure_reports_partial_identity_and_cleanup(server, full_handle):
    handle, client, _, receipt, _ = server
    cleanup = {"status": "failed", "reason": "Inspect the recorded driver"}
    client.create_connect_server.side_effect = ConnectServerCreateError(
        "Server creation failed",
        handle=handle if full_handle else None,
        run=handle.run,
        phase="grpc_service_creation",
        cleanup=cleanup,
    )
    result = CliRunner().invoke(connect_cli, create_args(receipt))
    value = json.loads(result.stdout)
    assert result.exit_code == 1
    assert value["partial_run"] == handle.run.to_dict()
    assert value["phase"] == "grpc_service_creation"
    assert value["cleanup"] == cleanup
    assert receipt.exists() is full_handle
    if full_handle:
        assert json.loads(receipt.read_text())["server"] == handle.to_dict()


@pytest.mark.parametrize("confirmed", [False, True])
@pytest.mark.parametrize("output", ["json", "text"])
def test_interrupt_retains_confirmed_partial_identity_without_cleanup(server, confirmed, output):
    handle, client, _, receipt, _ = server
    client.last_submitted_run = handle.run if confirmed else None
    client.last_submission_phase = "driver_created" if confirmed else "creating_resources"
    client.create_connect_server.side_effect = KeyboardInterrupt()
    result = CliRunner().invoke(connect_cli, create_args(receipt, output))
    assert result.exit_code == 130
    if output == "json":
        value = json.loads(result.stdout)
        assert value["error"]["code"] == "interrupted"
        assert ("partial_run" in value) is confirmed
        if confirmed:
            assert value["partial_run"] == handle.run.to_dict()
        assert value["cleanup"]["status"] == "not_attempted"
    elif confirmed:
        assert json.dumps(handle.run.to_dict()) in result.stderr
        assert "not_attempted" in result.stderr
    else:
        assert handle.run.driver_uid not in result.stderr
    assert not receipt.exists()
    client.delete_connect_server.assert_not_called()


@pytest.mark.parametrize("command", ["status", "wait", "delete"])
def test_lifecycle_uses_receipt_and_explicit_cluster_without_overwriting_it(server, command, tmp_path):
    handle, client, factory, receipt, profile = server
    save(handle, receipt)
    original = receipt.read_bytes()
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("only a CLI fixture")
    args = [
        "server",
        command,
        str(receipt),
        "--kubeconfig",
        str(kubeconfig),
        "--context",
        "chosen",
        "--timeout",
        "17",
        "--output",
        "json",
    ]
    if command == "wait":
        args += ["--profile-file", str(profile), "--profile", "notebook"]
    result = CliRunner().invoke(connect_cli, args)
    assert result.exit_code == 0, result.output
    value = json.loads(result.stdout)
    assert value["server"] == handle.to_dict()
    factory.assert_called_once_with(kubeconfig, "chosen")
    method = getattr(
        client,
        {
            "status": "connect_server_status",
            "wait": "wait_connect_server_ready",
            "delete": "delete_connect_server",
        }[command],
    )
    assert method.call_args.args[0] == handle
    assert method.call_args.kwargs["timeout"] == 17
    if command == "wait":
        assert method.call_args.kwargs["profile"].endpoint == "sc://localhost:15002"
        assert value["query_ready"] is True
    assert receipt.read_bytes() == original


def test_readiness_timeout_retains_receipt_and_reports_not_ready(server):
    handle, client, _, receipt, profile = server
    save(handle, receipt)
    original = receipt.read_bytes()
    client.wait_connect_server_ready.side_effect = ConnectServerTimeout(handle)
    result = CliRunner().invoke(
        connect_cli,
        [
            "server",
            "wait",
            str(receipt),
            "--profile-file",
            str(profile),
            "--profile",
            "notebook",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 5
    value = json.loads(result.stdout)
    assert value["query_ready"] is False
    assert value["server"] == handle.to_dict()
    assert value["error"]["code"] == "timeout"
    assert receipt.read_bytes() == original
    client.delete_connect_server.assert_not_called()


@pytest.mark.parametrize(
    "contents",
    [
        None,
        "{bad",
        " " * 65537,
        "[]",
        '{"schema_version":true,"server":{}}',
        '{"schema_version":1,"server":{}}',
    ],
)
def test_invalid_receipt_never_contacts_cluster(server, contents):
    _, _, factory, receipt, _ = server
    if contents is not None:
        receipt.write_text(contents)
    result = CliRunner().invoke(connect_cli, ["server", "delete", str(receipt), "--output", "json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] is not None
    factory.assert_not_called()


@pytest.mark.parametrize("code", [403, 404])
def test_kubernetes_errors_do_not_expose_response_bodies(server, code):
    handle, client, _, receipt, _ = server
    save(handle, receipt)
    error = ApiException(status=code, reason="Forbidden" if code == 403 else "Not Found")
    error.body = "PRIVATE_TOKEN"
    client.connect_server_status.side_effect = error
    result = CliRunner().invoke(connect_cli, ["server", "status", str(receipt), "--output", "json"])
    assert result.exit_code == (4 if code == 404 else 1)
    assert "PRIVATE_TOKEN" not in result.output
    assert json.loads(result.stdout)["error"] is not None


@pytest.mark.parametrize(
    "command, method", [("status", "connect_server_status"), ("delete", "delete_connect_server")]
)
def test_bounded_kubernetes_request_timeout_uses_timeout_exit_code(server, command, method):
    from urllib3.exceptions import MaxRetryError, ReadTimeoutError

    handle, client, _, receipt, _ = server
    save(handle, receipt)
    # Bounded helper clients disable urllib3 retries, so a request timeout is wrapped once.
    reason = ReadTimeoutError(None, "/api/v1/pods", "slow")
    getattr(client, method).side_effect = MaxRetryError(None, "/api/v1/pods", reason=reason)
    result = CliRunner().invoke(connect_cli, ["server", command, str(receipt), "--output", "json"])
    assert result.exit_code == 5, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "timeout"
    assert payload["server"] == handle.to_dict()


def test_nonfinite_timeout_is_usage_error_without_cluster_access(server):
    _, _, factory, receipt, _ = server
    result = CliRunner().invoke(connect_cli, ["server", "status", str(receipt), "--timeout", "nan"])
    assert result.exit_code == 2
    factory.assert_not_called()


@pytest.mark.parametrize("explicit", ["file", "context", "both", "neither"])
def test_explicit_cluster_selection_overrides_in_cluster_default(monkeypatch, tmp_path, explicit):
    from spark_on_k8s.utils.configuration import Configuration

    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_IN_CLUSTER", True)
    monkeypatch.setattr(cli, "SparkOnK8S", Mock())
    kubeconfig = tmp_path / "kubeconfig" if explicit in {"file", "both"} else None
    context = "selected" if explicit in {"context", "both"} else None
    cli._client(kubeconfig, context)
    manager = cli.SparkOnK8S.call_args.kwargs["k8s_client_manager"]
    assert manager.in_cluster is (explicit == "neither")
    if kubeconfig is not None:
        assert manager.config_file == str(kubeconfig)
    if context is not None:
        assert manager.context == context


def test_text_creation_failure_retains_complete_partial_identity(server):
    handle, client, _, receipt, _ = server
    client.create_connect_server.side_effect = ConnectServerCreateError(
        "Server creation failed",
        run=handle.run,
        phase="grpc_service_creation",
        cleanup={"status": "failed", "reason": "Inspect the recorded driver"},
    )
    result = CliRunner().invoke(connect_cli, create_args(receipt, "text"))
    assert result.exit_code == 1
    assert handle.run.driver_uid in result.stderr
    assert handle.run.app_id in result.stderr
    assert not receipt.exists()


def test_absent_deletion_reports_no_new_request(server):
    handle, client, _, receipt, _ = server
    save(handle, receipt)
    client.delete_connect_server.return_value = {"status": "absent", "requested": []}
    result = CliRunner().invoke(connect_cli, ["server", "delete", str(receipt)])
    assert result.exit_code == 0
    assert "already absent" in result.stdout
    assert "Deletion requested" not in result.stdout


def test_interrupted_rollback_preserves_uncertain_cleanup_outcome(server):
    handle, client, _, receipt, _ = server
    interrupted = KeyboardInterrupt()
    interrupted.run = handle.run
    interrupted.handle = handle
    interrupted.phase = "rollback"
    interrupted.cleanup = {"status": "unknown", "reason": "Deletion may already have been requested"}
    client.create_connect_server.side_effect = interrupted
    result = CliRunner().invoke(connect_cli, create_args(receipt))
    assert result.exit_code == 130
    value = json.loads(result.stdout)
    assert value["phase"] == "rollback"
    assert value["cleanup"]["status"] == "unknown"
    assert value["partial_run"] == handle.run.to_dict()
    assert json.loads(receipt.read_text())["server"] == handle.to_dict()
    assert "not stopped" not in value["error"]["message"]
    client.delete_connect_server.assert_not_called()


def test_text_lifecycle_distinguishes_observation_readiness_and_requested_deletion(server):
    handle, client, _, receipt, profile = server
    save(handle, receipt)
    original = receipt.read_bytes()
    runner = CliRunner()
    status = runner.invoke(connect_cli, ["server", "status", str(receipt)])
    assert status.exit_code == 0, status.output
    observation = json.loads(status.stdout)
    assert observation["status"] == "running"
    assert observation["query_ready"] is None
    client.wait_connect_server_ready.assert_not_called()

    ready = runner.invoke(
        connect_cli,
        ["server", "wait", str(receipt), "--profile-file", str(profile), "--profile", "notebook"],
    )
    assert ready.exit_code == 0, ready.output
    assert f"Server {handle.run.app_id} accepted a query" in ready.stdout
    assert client.wait_connect_server_ready.call_args.kwargs["profile"].endpoint == "sc://localhost:15002"

    deleted = runner.invoke(connect_cli, ["server", "delete", str(receipt)])
    assert deleted.exit_code == 0, deleted.output
    assert "Deletion requested" in deleted.stdout
    assert "cleanup may still be in progress" in deleted.stdout
    assert "already absent" not in deleted.stdout
    assert receipt.read_bytes() == original


@pytest.mark.parametrize("interrupted", [False, True], ids=["failed-creation", "interrupted-rollback"])
def test_receipt_write_failure_does_not_erase_partial_identity_or_original_failure(
    server, monkeypatch, interrupted
):
    from spark_on_k8s.cli import receipts

    handle, client, _, receipt, _ = server
    cleanup = {"status": "unknown", "reason": "Deletion outcome is uncertain"}
    if interrupted:
        error = KeyboardInterrupt()
        error.handle = handle
        error.run = handle.run
        error.phase = "rollback"
        error.cleanup = cleanup
    else:
        error = ConnectServerCreateError(
            "Original server creation failure", handle=handle, phase="rollback", cleanup=cleanup
        )
    client.create_connect_server.side_effect = error
    publish = Mock(side_effect=OSError("private publication failure details"))
    monkeypatch.setattr(receipts.os, "link", publish)
    result = CliRunner().invoke(connect_cli, create_args(receipt))
    assert result.exit_code == (130 if interrupted else 1)
    value = json.loads(result.stdout)
    assert value["server"] == handle.to_dict()
    assert value["partial_run"] == handle.run.to_dict()
    assert value["phase"] == "rollback"
    assert value["cleanup"] == cleanup
    assert value["receipt_error"]["code"] == "receipt_write_error"
    assert value["error"]["code"] == ("interrupted" if interrupted else "operation_error")
    if not interrupted:
        assert value["error"]["message"] == "Original server creation failure"
    assert "private publication failure details" not in result.output
    publish.assert_called_once()
    assert not receipt.exists()
    assert not list(receipt.parent.glob(".spark-receipt-*"))
    client.delete_connect_server.assert_not_called()


def listed(handle, **updates):
    return {
        "app_id": handle.run.app_id,
        "name": "notebook",
        "namespace": handle.run.namespace,
        "driver_pod": handle.run.driver_pod,
        "driver_uid": handle.run.driver_uid,
        "status": "running",
        "driver_phase": "Running",
        "spark_version": None,
        "image": "apache/spark:4.3.0",
        "created_at": "2026-11-06T08:00:00Z",
        "age_seconds": 3 * 86400 + 4 * 3600 + 59,
        **updates,
    }


def test_list_is_read_only_and_uses_explicit_cluster_namespace_and_timeout(server, tmp_path):
    handle, client, factory, _, _ = server
    client.list_connect_servers.return_value = [listed(handle)]
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("only a CLI fixture")
    result = CliRunner().invoke(
        connect_cli,
        [
            "server",
            "list",
            "--namespace",
            "spark",
            "--kubeconfig",
            str(kubeconfig),
            "--context",
            "chosen",
            "--timeout",
            "17",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    assert json.loads(result.stdout) == {
        "schema_version": 1,
        "operation": "connect_server_list",
        "namespace": "spark",
        "error": None,
        "items": [listed(handle)],
    }
    factory.assert_called_once_with(kubeconfig, "chosen")
    client.list_connect_servers.assert_called_once_with("spark", timeout=17)
    client.delete_connect_server.assert_not_called()
    client.create_connect_server.assert_not_called()
    client.wait_connect_server_ready.assert_not_called()


def test_list_text_shows_age_and_versions_or_an_empty_namespace(server):
    handle, client, _, _, _ = server
    client.list_connect_servers.return_value = [
        listed(handle),
        listed(handle, app_id="other-1", status="deleting", age_seconds=None, created_at=None, image=None),
    ]
    result = CliRunner().invoke(connect_cli, ["server", "list", "--namespace", "spark"])
    assert result.exit_code == 0, result.output
    header, first, second = result.stdout.splitlines()
    assert header.split() == ["APP", "ID", "STATUS", "AGE", "CREATED", "SPARK", "IMAGE"]
    assert first.split() == [
        handle.run.app_id,
        "running",
        "3d4h",
        "2026-11-06T08:00:00Z",
        "-",
        "apache/spark:4.3.0",
    ]
    assert second.split() == ["other-1", "deleting", "-", "-", "-", "-"]
    client.list_connect_servers.return_value = []
    empty = CliRunner().invoke(connect_cli, ["server", "list", "--namespace", "spark"])
    assert empty.exit_code == 0, empty.output
    assert "No Spark Connect server drivers created by spark-on-k8s were found in spark." in empty.stdout


@pytest.mark.parametrize(
    "seconds, text",
    [(None, "-"), (0, "0s"), (59, "59s"), (60, "1m"), (3599, "59m"), (3600, "1h0m"), (90061, "1d1h")],
)
def test_list_age_is_compact(seconds, text):
    assert cli._age(seconds) == text


def test_list_requires_a_namespace_before_cluster_access(server):
    _, _, factory, _, _ = server
    result = CliRunner().invoke(connect_cli, ["server", "list", "--output", "json"])
    assert result.exit_code == 2
    assert result.stdout == ""
    factory.assert_not_called()


@pytest.mark.parametrize("code", [403, 404])
def test_list_errors_are_versioned_without_response_bodies(server, code):
    _, client, _, _, _ = server
    error = ApiException(status=code, reason="Forbidden" if code == 403 else "Not Found")
    error.body = "PRIVATE_TOKEN"
    client.list_connect_servers.side_effect = error
    result = CliRunner().invoke(connect_cli, ["server", "list", "--namespace", "spark", "--output", "json"])
    assert result.exit_code == (4 if code == 404 else 1)
    assert "PRIVATE_TOKEN" not in result.output
    value = json.loads(result.stdout)
    assert value["items"] is None
    assert value["error"]["code"] == ("not_found" if code == 404 else "api_error")


@pytest.mark.parametrize("output", ["json", "text"])
def test_wait_reports_version_skew_warnings_without_failing(server, output):
    from spark_on_k8s.connect import version_warnings

    handle, client, _, receipt, profile = server
    save(handle, receipt)
    warnings = version_warnings("4.3.0", "4.0.5")
    client.wait_connect_server_ready.return_value = {
        "status": "ready",
        "query_ready": True,
        "spark_version": "4.0.5",
        "client_version": "4.3.0",
        "warnings": warnings,
    }
    result = CliRunner().invoke(
        connect_cli,
        [
            "server",
            "wait",
            str(receipt),
            "--profile-file",
            str(profile),
            "--profile",
            "notebook",
            "--output",
            output,
        ],
    )
    assert result.exit_code == 0, result.output
    if output == "json":
        value = json.loads(result.stdout)
        assert value["query_ready"] is True
        assert value["warnings"] == warnings
        assert value["observation"]["client_version"] == "4.3.0"
        assert value["error"] is None
    else:
        assert f"Server {handle.run.app_id} accepted a query" in result.stdout
        assert "Warning: Spark Connect client 4.3 and server 4.0 differ" in result.stderr


def test_failed_wait_keeps_an_empty_warning_list(server):
    handle, client, _, receipt, profile = server
    save(handle, receipt)
    client.wait_connect_server_ready.side_effect = ConnectServerTimeout(handle)
    result = CliRunner().invoke(
        connect_cli,
        [
            "server",
            "wait",
            str(receipt),
            "--profile-file",
            str(profile),
            "--profile",
            "notebook",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 5
    assert json.loads(result.stdout)["warnings"] == []


@pytest.mark.parametrize("namespace", ["Uppercase", "invalid/name", "a" * 64])
def test_invalid_namespace_rejected_before_receipt_or_cluster_access(server, namespace):
    _, client, factory, receipt, _ = server
    args = create_args(receipt)
    args[args.index("--namespace") + 1] = namespace
    result = CliRunner().invoke(connect_cli, args)
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"]["code"] == "operation_error"
    assert not receipt.exists()
    assert not list(receipt.parent.glob(".spark-receipt-*"))
    factory.assert_not_called()
    client.create_connect_server.assert_not_called()


POD_FLAGS = [
    "--image-pull-secret",
    "private-registry",
    "--image-pull-secret",
    "mirror-registry",
    "--driver-env-from-secret",
    "API_TOKEN=notebook-credentials:api-token",
    "--executor-env-from-secret",
    "API_TOKEN=notebook-credentials:api-token",
    "--driver-node-selector",
    "kubernetes.io/os=linux",
    "--executor-node-selector",
    "example.com/pool=gpu",
    "--driver-toleration",
    "dedicated=ml:NoSchedule",
    "--executor-toleration",
    "nvidia.com/gpu:NoSchedule",
    "--executor-toleration",
    "spot",
]


def test_create_maps_repeatable_pod_settings(server):
    _, client, _, receipt, _ = server
    result = CliRunner().invoke(connect_cli, create_args(receipt) + POD_FLAGS)
    assert result.exit_code == 0, result.output
    config = client.create_connect_server.call_args.args[0]
    assert config.image_pull_secrets == ["private-registry", "mirror-registry"]
    assert config.driver_env_from_secrets == {"API_TOKEN": "notebook-credentials:api-token"}
    assert config.executor_env_from_secrets == {"API_TOKEN": "notebook-credentials:api-token"}
    assert config.driver_node_selector == {"kubernetes.io/os": "linux"}
    assert config.executor_node_selector == {"example.com/pool": "gpu"}
    assert config.driver_tolerations == [
        {"key": "dedicated", "operator": "Equal", "value": "ml", "effect": "NoSchedule"}
    ]
    assert config.executor_tolerations == [
        {"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"},
        {"key": "spot", "operator": "Exists"},
    ]


@pytest.mark.parametrize("value", ["", ":NoSchedule", "pool:", "=ml:NoSchedule"])
def test_malformed_toleration_is_a_usage_error_before_cluster_access(server, value):
    _, client, factory, receipt, _ = server
    result = CliRunner().invoke(connect_cli, create_args(receipt) + ["--executor-toleration", value])
    assert result.exit_code == 2
    assert "KEY[=VALUE][:EFFECT]" in result.output
    factory.assert_not_called()
    client.create_connect_server.assert_not_called()


@pytest.mark.parametrize(
    "flags",
    [
        ["--driver-env-from-secret", "SPARK_HOME=credentials:home"],
        ["--executor-env-from-secret", "TOKEN=credentials"],
        ["--image-pull-secret", "Not_A_Secret"],
        ["--executor-node-selector", "pool=not valid"],
        ["--driver-toleration", "pool=ml:Never"],
    ],
)
def test_invalid_pod_settings_fail_before_receipt_or_cluster_access(server, flags):
    _, client, factory, receipt, _ = server
    result = CliRunner().invoke(connect_cli, create_args(receipt) + flags)
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"]["code"] == "operation_error"
    assert not receipt.exists()
    factory.assert_not_called()
    client.create_connect_server.assert_not_called()


def plan_args(output):
    args = create_args(Path("unused.json"), output)
    index = args.index("--receipt-file")
    del args[index : index + 2]
    args[1] = "plan"
    return args


@pytest.mark.parametrize("output", ["json", "text"])
def test_plan_is_offline_and_lists_pod_settings_as_references(server, monkeypatch, output):
    _, client, factory, _, _ = server

    def no_cluster(self):
        raise AssertionError("Planning must not open a Kubernetes connection")

    monkeypatch.setattr(KubernetesClientManager, "client", no_cluster)
    result = CliRunner().invoke(connect_cli, plan_args(output) + POD_FLAGS + ["--name", "notebook"])
    assert result.exit_code == 0, result.output
    factory.assert_not_called()
    client.create_connect_server.assert_not_called()
    value = json.loads(result.stdout) if output == "json" else yaml.safe_load(result.stdout)
    if output == "json":
        assert value["operation"] == "connect_server_plan"
        assert value["namespace"] == "spark"
        assert value["error"] is None
    plan = value["plan"]
    assert plan["preview"] is True and plan["app_id"] == "notebook-preview"
    assert [item["kind"] for item in plan["resources"]] == ["ConfigMap", "Pod", "Service", "Service"]
    assert value["pod_settings"]["image_pull_secrets"] == ["private-registry", "mirror-registry"]
    assert value["pod_settings"]["executor_env_from_secrets"] == {
        "API_TOKEN": "notebook-credentials:api-token"
    }
    assert value["pod_settings"]["executor_tolerations"][1] == {"key": "spot", "operator": "Exists"}
    driver = next(item for item in plan["resources"] if item["kind"] == "Pod")["spec"]["containers"][0]
    assert {
        "name": "API_TOKEN",
        "valueFrom": {"secretKeyRef": {"name": "notebook-credentials", "key": "api-token"}},
    } in driver["env"]


def test_plan_reports_invalid_settings_with_the_automation_contract(server):
    _, _, factory, _, _ = server
    result = CliRunner().invoke(connect_cli, plan_args("json") + ["--driver-env-from-secret", "TOKEN=bad"])
    assert result.exit_code == 1
    value = json.loads(result.stdout)
    assert value["operation"] == "connect_server_plan"
    assert value["plan"] is None
    assert "SECRET:KEY" in value["error"]["message"]
    factory.assert_not_called()
