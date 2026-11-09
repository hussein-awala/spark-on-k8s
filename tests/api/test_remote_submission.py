from __future__ import annotations

import copy
import hmac
import json
import logging
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.api import remote_auth, submission
from spark_on_k8s.api.remote_auth import RemoteAccessMiddleware
from spark_on_k8s.api.submission_policy import (
    AdmissionError,
    SettingsSource,
    SubmissionPolicy,
    SubmissionSettings,
)
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.utils.configuration import Configuration
from starlette.websockets import WebSocketDisconnect

TOKEN = "a-test-token-with-at-least-32-characters"
NEXT_TOKEN = "b-next-token-with-at-least-32-characters"


def policy_document():
    return {
        "schema_version": 1,
        "namespaces": {"analytics": ["spark-worker"]},
        "applications": [
            {
                "image": "spark@sha256:" + "a" * 64,
                "app_path": "local:///jobs/report.py",
                "arguments": [[], ["2026-11-07"]],
            }
        ],
        "limits": {
            "driver_cpu": 2,
            "driver_memory_mb": 2048,
            "executor_cpu": 2,
            "executor_memory_mb": 2048,
            "executors": 4,
        },
        "spark_conf": {"spark.sql.shuffle.partitions": [8, 16]},
    }


def job_document():
    return {
        "apiVersion": "spark-on-k8s/v1",
        "kind": "SparkJob",
        "metadata": {"name": "daily-report"},
        "spec": {
            "image": "spark@sha256:" + "a" * 64,
            "app_path": "local:///jobs/report.py",
            "namespace": "analytics",
            "service_account": "spark-worker",
        },
    }


@pytest.fixture
def server(monkeypatch):
    settings = SubmissionSettings(True, TOKEN.encode(), SubmissionPolicy.from_mapping(policy_document()))
    monkeypatch.setattr(remote_auth, "get_submission_settings", lambda: settings)
    monkeypatch.setattr(submission, "get_submission_settings", lambda: settings)
    manager = Mock()
    manager.client.return_value = MagicMock()
    api = Mock()

    def create_pod(**kwargs):
        pod = kwargs["body"]
        pod.metadata.uid = "driver-uid"
        return pod

    api.create_namespaced_pod.side_effect = create_pod
    monkeypatch.setattr("spark_on_k8s.client.k8s.CoreV1Api", Mock(return_value=api))
    factory = Mock(side_effect=lambda: SparkOnK8S(k8s_client_manager=manager))
    monkeypatch.setattr(submission, "create_submission_client", factory)
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(submission.router)
    with TestClient(app, headers={"Authorization": "Bearer " + TOKEN}) as client:
        yield client, api, manager, factory


def test_plan_uses_server_client_offline_and_submit_returns_actual_run(server, monkeypatch):
    client, api, manager, factory = server
    # Explicit JobSpec defaults must prevent ambient CLI settings from adding credentials or templates.
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_SECRET_ENV_VAR", {"password": "ambient-secret"})
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_EXECUTOR_TEMPLATE", "/do/not/read.yaml")
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_SPARK_CONF", {"spark.plugins": "unapproved.Code"})
    document = job_document()
    document["spec"]["app_arguments"] = ["2026-11-07"]
    preview = client.post("/jobs/v1/plan", json=document)
    assert preview.status_code == 200, preview.text
    manager.client.assert_not_called()
    api.create_namespaced_pod.assert_not_called()
    plan = preview.json()["plan"]
    assert plan["preview"] is True
    assert plan["namespace"] == "analytics"
    assert [item["kind"] for item in plan["resources"]] == ["Pod", "Service"]
    assert "ambient-secret" not in preview.text
    assert "2026-11-07" not in preview.text  # Plans redact positional arguments.
    result = client.post("/jobs/v1/submit", json=document)
    assert result.status_code == 201, result.text
    receipt = result.json()["run"]
    actual = api.create_namespaced_pod.call_args.kwargs["body"]
    assert receipt == {
        "namespace": "analytics",
        "driver_pod": actual.metadata.name,
        "driver_uid": "driver-uid",
        "app_id": actual.metadata.labels["spark-app-id"],
    }
    assert actual.spec.service_account_name == "spark-worker"
    assert actual.spec.containers[0].args[-2:] == ["local:///jobs/report.py", "2026-11-07"]
    assert actual.spec.containers[0].env_from == []
    assert actual.spec.volumes == []
    api.create_namespaced_secret.assert_not_called()
    api.create_namespaced_config_map.assert_not_called()
    api.create_namespaced_service.assert_called_once()
    assert factory.call_count == 2
    assert result.headers["cache-control"] == "no-store"


def test_repeated_long_names_create_distinct_valid_driver_identities(server):
    client, api, _, _ = server
    document = job_document()
    document["metadata"]["name"] = "a" * 63
    runs = [client.post("/jobs/v1/submit", json=document).json()["run"] for _ in range(2)]
    assert runs[0]["app_id"] != runs[1]["app_id"]
    assert all(len(run["driver_pod"]) <= 63 for run in runs)
    assert api.create_namespaced_pod.call_count == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("namespace", "kube-system"),
        ("service_account", "administrator"),
        ("image", "unapproved:latest"),
        ("app_path", "https://attacker.invalid/code.py"),
        ("app_path", "local:///jobs/other.py"),
        ("class_name", "arbitrary.Class"),
        ("app_arguments", ["-c", "arbitrary-code"]),
        ("packages", ["unapproved:code:1"]),
        ("driver_pod_template", {"kind": "Pod", "spec": {"hostNetwork": True}}),
        ("executor_pod_template", "/etc/private.yml"),
        ("executor_pod_template_path", "file:///etc/private.yml"),
        ("driver_env_vars_from_secrets", ["cluster-secret"]),
        ("secret_values", {"a": "b"}),
        ("driver_annotations", {"sidecar.inject": "true"}),
        ("driver_labels", {"cost-center": "cc-1001"}),
        ("executor_labels", {"cost-center": "cc-1001"}),
        ("executor_node_selector", {"node": "privileged"}),
        ("spark_conf", {"spark.kubernetes.executor.volumes.hostPath.escape.mount.path": "/"}),
        ("spark_conf", {"spark.plugins": "arbitrary.Code"}),
        ("spark_conf", {"spark.kubernetes.driverEnv.JAVA_TOOL_OPTIONS": "-javaagent:/unapproved.jar"}),
        ("spark_conf", {"spark.executor.instances": 1000}),
        ("spark_conf", {"spark.sql.shuffle.partitions": "8"}),
        ("spark_conf", {"spark.sql.shuffle.partitions": 100000}),
        ("driver_resources", {"cpu": 3}),
        ("executor_resources", {"memory": 2048, "memory_overhead": 1}),
        ("executor_instances", {"min": 1}),
        ("executor_instances", {"initial": 5}),
        ("executor_instances", {"min": 0, "max": 5}),
        ("app_waiter", "log"),
    ],
)
def test_denied_submission_never_builds_or_touches_kubernetes(server, field, value):
    client, api, manager, factory = server
    document = job_document()
    document["spec"][field] = value
    result = client.post("/jobs/v1/submit", json=document)
    assert result.status_code == 403, result.text
    assert result.json()["error"]["code"] == "policy_denied"
    factory.assert_not_called()
    manager.client.assert_not_called()
    api.create_namespaced_pod.assert_not_called()


def test_admitted_namespace_outside_the_api_scope_is_still_denied(server, monkeypatch):
    from spark_on_k8s.api.configuration import APIConfiguration

    # Startup rejects such a policy; each operation still enforces the scope on its own.
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", "batch")
    client, api, manager, factory = server
    for operation in ("plan", "submit"):
        result = client.post(f"/jobs/v1/{operation}", json=job_document())
        assert result.status_code == 403, result.text
        assert result.json()["error"] == {
            "code": "policy_denied",
            "message": "The namespace is outside this API's namespace scope.",
        }
    factory.assert_not_called()
    api.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [("kubernetes", {"config_file": "/etc/private/kubeconfig"}), ("profiles", {"production": {"spec": {}}})],
)
def test_remote_document_cannot_choose_client_credentials_or_profiles(server, field, value):
    client, _, _, factory = server
    document = job_document()
    document[field] = value
    assert client.post("/jobs/v1/plan", json=document).status_code == 403
    factory.assert_not_called()


def test_admitted_tuning_and_bounded_dynamic_allocation(server):
    client, api, _, _ = server
    document = job_document()
    document["spec"].update(
        spark_conf={"spark.sql.shuffle.partitions": 8}, executor_instances={"min": 0, "max": 4, "initial": 1}
    )
    assert client.post("/jobs/v1/submit", json=document).status_code == 201
    args = api.create_namespaced_pod.call_args.kwargs["body"].spec.containers[0].args
    assert "spark.dynamicAllocation.maxExecutors=4" in args
    assert "spark.sql.shuffle.partitions=8" in args


def test_invalid_schema_and_request_formats_fail_without_effects(server):
    client, _, _, factory = server
    wrong_schema = job_document()
    wrong_schema["apiVersion"] = "v900"
    assert client.post("/jobs/v1/submit", json=wrong_schema).status_code == 422
    assert client.post("/jobs/v1/submit", content=b"{}").status_code == 415
    for invalid in (b'{"spec":{},"spec":{}}', b'{"n":NaN}', b"\xff"):
        result = client.post("/jobs/v1/submit", content=invalid, headers={"Content-Type": "application/json"})
        assert result.status_code == 400
    result = client.post(
        "/jobs/v1/submit", content=b" " * (1024 * 1024 + 1), headers={"Content-Type": "application/json"}
    )
    assert result.status_code == 413
    result = client.post("/jobs/v1/submit", json=job_document(), headers={"Idempotency-Key": "attempt"})
    assert result.status_code == 400
    assert result.json()["error"]["code"] == "unsupported_idempotency"
    deep = client.post(
        "/jobs/v1/submit", content=b"[" * 10000 + b"]" * 10000, headers={"Content-Type": "application/json"}
    )
    assert deep.status_code in {400, 403}
    factory.assert_not_called()


def test_kubernetes_failures_are_sanitized_and_never_retried(server):
    client, api, _, _ = server
    api.create_namespaced_pod.side_effect = ApiException(status=403, reason="token secret")
    result = client.post("/jobs/v1/submit", json=job_document())
    assert result.status_code == 502
    assert "secret" not in result.text
    assert "partial" in result.json()["error"]["message"]
    api.create_namespaced_pod.assert_called_once()


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/apps/list_apps/analytics"),
        ("GET", "/webserver/api/apps?namespace=analytics"),
        ("GET", "/webserver/api/apps/analytics/pods/run/diagnostics"),
        ("PATCH", "/app/analytics/run"),
        ("DELETE", "/app/analytics/run"),
        ("GET", "/webserver/logs/analytics/run"),
        ("GET", "/webserver/ui/analytics/run/"),
        ("GET", "/webserver/history/analytics/run/"),
        ("GET", "/static/ui/console.js"),
        ("GET", "/openapi.json"),
        ("POST", "/jobs/v1/submit"),
    ],
)
def test_remote_mode_authenticates_every_existing_http_surface(server, method, path):
    from spark_on_k8s.api.main import app

    result = TestClient(app).request(method, path)
    assert result.status_code == 401
    assert result.headers["www-authenticate"] == "Bearer"


def test_websocket_requires_header_not_query_token_and_default_ui_stays_public(monkeypatch):
    settings = SubmissionSettings(True, TOKEN.encode(), SubmissionPolicy.from_mapping(policy_document()))
    monkeypatch.setattr(remote_auth, "get_submission_settings", lambda: settings)
    calls = []
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)

    @app.websocket("/webserver/ws/logs/ns/run")
    async def socket(websocket: WebSocket):
        calls.append("socket")
        await websocket.accept()
        await websocket.send_text("literal logs")
        await websocket.close()

    @app.get("/webserver/apps")
    async def dashboard():
        return {"page": "dashboard"}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        for url in ("/webserver/ws/logs/ns/run", "/webserver/ws/logs/ns/run?token=" + TOKEN):
            with pytest.raises(WebSocketDisconnect) as error, client.websocket_connect(url):
                pass
            assert error.value.code == 1008
        assert calls == []
        with client.websocket_connect(
            "/webserver/ws/logs/ns/run", headers={"Authorization": "Bearer " + TOKEN}
        ) as ws:
            assert ws.receive_text() == "literal logs"
        settings = SubmissionSettings()
        assert client.get("/webserver/apps").json() == {"page": "dashboard"}
        with client.websocket_connect("/webserver/ws/logs/ns/run") as ws:
            assert ws.receive_text() == "literal logs"


def test_disabled_remote_routes_are_unavailable(monkeypatch):
    monkeypatch.setattr(submission, "get_submission_settings", SubmissionSettings)
    app = FastAPI()
    app.include_router(submission.router)
    response = TestClient(app).post("/jobs/v1/submit", json=job_document())
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "remote_disabled"


def test_settings_fail_closed_and_do_not_expose_token(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "true")
    monkeypatch.delenv("SPARK_ON_K8S_SUBMISSION_TOKEN_FILE", raising=False)
    with pytest.raises(ValueError, match="requires readable"):
        SubmissionSettings.from_env()
    token = tmp_path / "token"
    token.write_text(TOKEN + "\n")
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps(policy_document()))
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_TOKEN_FILE", str(token))
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_POLICY_FILE", str(policy))
    settings = SubmissionSettings.from_env()
    assert settings.enabled and settings.token == TOKEN.encode()
    assert TOKEN not in repr(settings)
    token.write_text("weak")
    with pytest.raises(ValueError, match="32"):
        SubmissionSettings.from_env()
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "false")
    assert not SubmissionSettings.from_env().enabled


@pytest.mark.parametrize(
    "key",
    [
        "spark.plugins",
        "spark.kubernetes.namespace",
        "spark.executor.extraJavaOptions",
        "spark.kubernetes.driverEnv.JAVA_TOOL_OPTIONS",
        "spark.sql.extensions",
        "spark.jars",
        "spark.files",
        "spark.master",
    ],
)
def test_policy_cannot_admit_executable_or_credential_configuration(key):
    document = copy.deepcopy(policy_document())
    document["spark_conf"][key] = ["escape"]
    with pytest.raises(ValueError, match="Spark configuration allowlist"):
        SubmissionPolicy.from_mapping(document)


@pytest.mark.parametrize("change", ["host_volume", "extra_env", "command"])
def test_generated_plan_cannot_introduce_unapproved_runtime_settings(server, change):
    from spark_on_k8s.planning import ApplicationPlan

    client, api, manager, factory = server
    real = SparkOnK8S(k8s_client_manager=manager)
    original = real.plan_app

    def altered_plan(**kwargs):
        plan = original(**kwargs)
        resources = copy.deepcopy(plan.resources)
        pod = resources[0]["spec"]
        if change == "host_volume":
            pod["volumes"] = [{"name": "host", "hostPath": {"path": "/"}}]
        elif change == "extra_env":
            pod["containers"][0]["env"].append({"name": "JAVA_TOOL_OPTIONS", "value": "unapproved"})
        else:
            pod["containers"][0]["command"] = ["sh", "-c", "unapproved"]
        return ApplicationPlan(plan.namespace, plan.app_id, plan.driver_pod, resources)

    real.plan_app = altered_plan
    factory.side_effect = lambda: real
    response = client.post("/jobs/v1/submit", json=job_document())
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "policy_denied"
    manager.client.assert_not_called()
    api.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "headers",
    [
        [("Authorization", "Bearer wrong")],
        [("Authorization", "Basic " + TOKEN)],
        [("Authorization", "Bearer " + TOKEN), ("Authorization", "Bearer wrong")],
        [("X-Forwarded-User", "administrator")],
    ],
)
def test_auth_rejects_wrong_ambiguous_and_untrusted_gateway_headers(server, headers):
    from spark_on_k8s.api.main import app

    response = TestClient(app).post("/jobs/v1/submit", content=b"invalid JSON", headers=headers)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"
    server[3].assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p.update(schema_version=True),
        lambda p: p.update(namespaces={}),
        lambda p: p.update(namespaces={"not a namespace": ["spark"]}),
        lambda p: p.update(namespaces={"spark": []}),
        lambda p: p.update(namespaces={"spark": ["BadAccount"]}),
        lambda p: p.update(applications=[]),
        lambda p: p["applications"][0].update(image="image with whitespace"),
        lambda p: p["applications"][0].update(app_path="https://example.test/job.py"),
        lambda p: p["applications"][0].update(app_path="local:///jobs/../private.py"),
        lambda p: p["applications"][0].update(class_name=""),
        lambda p: p["applications"][0].update(arguments=[["allowed"], [42]]),
        lambda p: p["limits"].update(executors=True),
        lambda p: p["spark_conf"].update({"spark.sql.shuffle.partitions": [1.5]}),
    ],
)
def test_invalid_administrator_policies_fail_closed(change):
    policy = policy_document()
    change(policy)
    with pytest.raises(ValueError):
        SubmissionPolicy.from_mapping(policy)


def test_invalid_enabled_flag_and_oversized_policy_are_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "maybe")
    with pytest.raises(ValueError, match="must be true or false"):
        SubmissionSettings.from_env()
    token = tmp_path / "token"
    token.write_text(TOKEN)
    policy = tmp_path / "policy.json"
    policy.write_bytes(b" " * (1024 * 1024 + 1))
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "true")
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_TOKEN_FILE", str(token))
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_POLICY_FILE", str(policy))
    with pytest.raises(ValueError, match="exceeds 1 MiB"):
        SubmissionSettings.from_env()


@pytest.mark.parametrize("change", ["namespace", "foreign_resource", "extra_resource", "driver_missing"])
def test_generated_plan_identity_and_resource_boundaries_are_rechecked(server, change):
    from spark_on_k8s.planning import ApplicationPlan

    client, api, manager, factory = server
    real = SparkOnK8S(k8s_client_manager=manager)
    original = real.plan_app

    def altered(**kwargs):
        plan = original(**kwargs)
        resources = list(copy.deepcopy(plan.resources))
        namespace = plan.namespace
        if change == "namespace":
            namespace = "another-namespace"
        elif change == "foreign_resource":
            resources[0]["metadata"]["namespace"] = "another-namespace"
        elif change == "extra_resource":
            resources.append({"kind": "Secret", "metadata": {"namespace": namespace}})
        else:
            resources = [r for r in resources if r["kind"] != "Pod"]
        return ApplicationPlan(namespace, plan.app_id, plan.driver_pod, resources)

    real.plan_app = altered
    factory.side_effect = lambda: real
    response = client.post("/jobs/v1/submit", json=job_document())
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "policy_denied"
    manager.client.assert_not_called()
    assert not api.mock_calls


def test_stalled_request_body_is_rejected_before_admission(server, monkeypatch):
    from types import SimpleNamespace

    async def expire_body(awaitable, *, timeout):
        assert timeout == 15
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(submission, "asyncio", SimpleNamespace(wait_for=expire_body))
    client, _, _, factory = server
    response = client.post("/jobs/v1/submit", json=job_document())
    assert response.status_code == 408
    assert response.json()["error"]["code"] == "request_timeout"
    factory.assert_not_called()


def test_unexpected_submission_failure_does_not_disclose_details_or_retry(server):
    client, api, _, factory = server
    factory.side_effect = RuntimeError("private internal credential")
    response = client.post("/jobs/v1/submit", json=job_document())
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "operation_error"
    assert "private internal credential" not in response.text
    factory.assert_called_once()
    assert not api.mock_calls


def test_server_clients_do_not_reuse_submission_receipts_or_connect_during_creation(monkeypatch):
    def unexpected_connection(*args, **kwargs):
        raise AssertionError("Creating a submission client must not contact Kubernetes")

    monkeypatch.setattr(
        "spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client", unexpected_connection
    )
    first = submission.create_submission_client()
    first.last_submitted_app_id = "previous-request"
    second = submission.create_submission_client()
    assert first is not second
    assert second.last_submitted_app_id is None and second.last_submitted_run is None


def use_policy(monkeypatch, document):
    settings = SubmissionSettings(True, TOKEN.encode(), SubmissionPolicy.from_mapping(document))
    monkeypatch.setattr(submission, "get_submission_settings", lambda: settings)


def placement_policy():
    policy = policy_document()
    policy["namespaces"] = {"analytics": ["spark-worker"], "finance": ["spark-worker", "spark-finance"]}
    report = policy["applications"][0]
    policy["applications"] = [
        {**report, "namespaces": ["analytics"]},
        {**report, "namespaces": ["finance"], "service_accounts": ["spark-finance"]},
        {**report, "image": "spark@sha256:" + "b" * 64, "service_accounts": ["spark-finance"]},
        {**report, "image": "spark@sha256:" + "c" * 64},
    ]
    return policy


PLACEMENT_DENIED = "The application is not approved for this namespace and service-account pair."


@pytest.mark.parametrize(
    "image, namespace, account, denial",
    [
        ("a", "analytics", "spark-worker", None),
        ("a", "finance", "spark-finance", None),
        ("a", "finance", "spark-worker", PLACEMENT_DENIED),
        ("b", "finance", "spark-finance", None),
        ("b", "finance", "spark-worker", PLACEMENT_DENIED),
        ("b", "analytics", "spark-worker", PLACEMENT_DENIED),
        # Entries without a placement keep today's behavior: every approved pair.
        ("c", "analytics", "spark-worker", None),
        ("c", "finance", "spark-worker", None),
        ("c", "analytics", "spark-finance", "The namespace and service-account pair is not approved."),
    ],
)
def test_application_placement_narrows_the_approved_pairs(image, namespace, account, denial):
    policy = SubmissionPolicy.from_mapping(placement_policy())
    document = job_document()
    document["spec"].update(image="spark@sha256:" + image * 64, namespace=namespace, service_account=account)
    if denial is None:
        assert policy.admit(document)["namespace"] == namespace
    else:
        with pytest.raises(AdmissionError) as error:
            policy.admit(document)
        assert str(error.value) == denial


def test_placement_denial_is_a_policy_denied_response_without_effects(server, monkeypatch):
    client, api, manager, factory = server
    use_policy(monkeypatch, placement_policy())
    document = job_document()
    document["spec"]["namespace"] = "finance"
    response = client.post("/jobs/v1/submit", json=document)
    assert response.status_code == 403, response.text
    assert response.json()["error"] == {"code": "policy_denied", "message": PLACEMENT_DENIED}
    factory.assert_not_called()
    api.create_namespaced_pod.assert_not_called()


def label_policy():
    policy = policy_document()
    policy["labels"] = {
        "required": {
            "cost-center": {"values": ["cc-1001", "cc-1002"]},
            "example.com/team": {"pattern": "[a-z][a-z0-9-]*"},
            "owner": {},
        },
        "fixed": {"submitted-via": "remote-api"},
    }
    return policy


COST_LABELS = {"cost-center": "cc-1001", "example.com/team": "data-eng", "owner": "Jane.Doe"}


def test_required_and_fixed_labels_reach_the_driver_and_executors(server, monkeypatch):
    client, api, _, _ = server
    use_policy(monkeypatch, label_policy())
    document = job_document()
    document["spec"]["driver_labels"] = COST_LABELS
    expected = {**COST_LABELS, "submitted-via": "remote-api"}
    preview = client.post("/jobs/v1/plan", json=document)
    assert preview.status_code == 200, preview.text
    assert preview.json()["plan"]["resources"][0]["metadata"]["labels"].items() >= expected.items()
    result = client.post("/jobs/v1/submit", json=document)
    assert result.status_code == 201, result.text
    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    assert pod.metadata.labels == {
        **expected,
        "spark-app-name": "daily-report",
        "spark-app-id": result.json()["run"]["app_id"],
        "spark-role": "driver",
    }
    args = pod.spec.containers[0].args
    for key, value in expected.items():
        assert f"spark.kubernetes.executor.label.{key}={value}" in args


def test_policy_without_labels_keeps_driver_labels_closed(server):
    client, api, _, _ = server
    document = job_document()
    document["spec"]["driver_labels"] = {"cost-center": "cc-1001"}
    response = client.post("/jobs/v1/submit", json=document)
    assert response.json()["error"]["message"] == (
        "Remote jobs contain a setting that is not admitted by the submission API."
    )
    assert client.post("/jobs/v1/submit", json=job_document()).status_code == 201
    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    assert set(pod.metadata.labels) == {"spark-app-name", "spark-app-id", "spark-role"}
    assert not [arg for arg in pod.spec.containers[0].args if ".executor.label." in arg]


ONLY_REQUIRED = "Remote jobs can set only the driver labels"


@pytest.mark.parametrize(
    "labels, message",
    [
        (None, "The submission policy requires the driver labels: cost-center, example.com/team, owner."),
        (
            {"cost-center": "cc-1001"},
            "The submission policy requires the driver labels: example.com/team, owner.",
        ),
        ({**COST_LABELS, "project": "x"}, ONLY_REQUIRED),
        # Reserved ownership labels and the server's fixed labels cannot be supplied by a job.
        ({**COST_LABELS, "spark-role": "executor"}, ONLY_REQUIRED),
        ({**COST_LABELS, "spark-app-id": "another-run"}, ONLY_REQUIRED),
        ({**COST_LABELS, "spark-ui-proxy": "true"}, ONLY_REQUIRED),
        ({**COST_LABELS, "spark-on-k8s.io/connect-server": "true"}, ONLY_REQUIRED),
        ({**COST_LABELS, "spark-on-k8s.io/airflow-task-key": "x"}, ONLY_REQUIRED),
        ({**COST_LABELS, "submitted-via": "laptop"}, ONLY_REQUIRED),
        ({**COST_LABELS, "cost-center": "cc-9999"}, "does not approve the value of driver label cost-center"),
        ({**COST_LABELS, "example.com/team": "Data-Eng"}, "value of driver label example.com/team"),
        ({**COST_LABELS, "example.com/team": "data eng"}, "value of driver label example.com/team"),
        ({**COST_LABELS, "owner": ""}, "does not approve the value of driver label owner"),
        ({**COST_LABELS, "owner": "-leading-dash"}, "does not approve the value of driver label owner"),
        ({**COST_LABELS, "owner": "x" * 64}, "does not approve the value of driver label owner"),
    ],
)
def test_label_policy_denials_never_build_or_touch_kubernetes(server, monkeypatch, labels, message):
    client, api, manager, factory = server
    use_policy(monkeypatch, label_policy())
    document = job_document()
    if labels is not None:
        document["spec"]["driver_labels"] = labels
    response = client.post("/jobs/v1/submit", json=document)
    assert response.status_code == 403, response.text
    assert response.json()["error"]["code"] == "policy_denied"
    assert message in response.json()["error"]["message"]
    factory.assert_not_called()
    api.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("field", ["executor_labels", "driver_annotations"])
def test_label_policy_admits_no_other_metadata_fields(server, monkeypatch, field):
    client, _, _, factory = server
    use_policy(monkeypatch, label_policy())
    document = job_document()
    document["spec"].update({"driver_labels": COST_LABELS, field: {"cost-center": "cc-1001"}})
    assert client.post("/jobs/v1/plan", json=document).status_code == 403
    factory.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        "changed_label",
        "extra_label",
        "role_label",
        "annotation",
        "extra_executor_label",
        "missing_executor_label",
    ],
)
def test_generated_plan_cannot_change_admitted_labels(server, monkeypatch, change):
    from spark_on_k8s.planning import ApplicationPlan

    client, api, manager, factory = server
    use_policy(monkeypatch, label_policy())
    real = SparkOnK8S(k8s_client_manager=manager)
    original = real.plan_app

    def altered_plan(**kwargs):
        plan = original(**kwargs)
        resources = copy.deepcopy(plan.resources)
        metadata, args = resources[0]["metadata"], resources[0]["spec"]["containers"][0]["args"]
        if change == "changed_label":
            metadata["labels"]["cost-center"] = "cc-1002"
        elif change == "extra_label":
            metadata["labels"]["sidecar.istio.io/inject"] = "true"
        elif change == "role_label":
            metadata["labels"]["spark-role"] = "executor"
        elif change == "annotation":
            metadata["annotations"] = {"sidecar.istio.io/inject": "true"}
        elif change == "extra_executor_label":
            args[-1:-1] = ["--conf", "spark.kubernetes.executor.label.team=unapproved"]
        else:
            index = args.index("spark.kubernetes.executor.label.submitted-via=remote-api")
            del args[index - 1 : index + 1]
        return ApplicationPlan(plan.namespace, plan.app_id, plan.driver_pod, resources)

    real.plan_app = altered_plan
    factory.side_effect = lambda: real
    document = job_document()
    document["spec"]["driver_labels"] = COST_LABELS
    response = client.post("/jobs/v1/submit", json=document)
    assert response.status_code == 403, response.text
    assert response.json()["error"] == {
        "code": "policy_denied",
        "message": "The generated driver pod contains unapproved labels or annotations.",
    }
    manager.client.assert_not_called()
    api.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "key",
    [
        "spark-role",
        "spark-app-id",
        "spark-app-name",
        "spark-ui-proxy",
        "spark-app-selector",
        "spark-on-k8s.io/connect-server",
        "spark-on-k8s.io/airflow-task-key",
        "kubernetes.io/metadata.name",
        "node.kubernetes.io/instance-type",
        "k8s.io/owner",
    ],
)
@pytest.mark.parametrize("section", ["required", "fixed"])
def test_policy_cannot_require_or_fix_reserved_labels(key, section):
    policy = policy_document()
    policy["labels"] = {section: {key: {} if section == "required" else "value"}}
    with pytest.raises(ValueError, match="reserved"):
        SubmissionPolicy.from_mapping(policy)


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p["applications"][0].update(namespaces=["finance"]),
        lambda p: p["applications"][0].update(namespaces=[]),
        lambda p: p["applications"][0].update(namespaces="analytics"),
        lambda p: p["applications"][0].update(service_accounts=["administrator"]),
        lambda p: p["applications"][0].update(service_accounts=[""]),
        lambda p: (
            p["namespaces"].update(finance=["spark-finance"]),
            p["applications"][0].update(namespaces=["analytics"], service_accounts=["spark-finance"]),
        ),
        lambda p: p.update(labels={}),
        lambda p: p.update(labels={"required": {}}),
        lambda p: p.update(labels={"fixed": {}}),
        lambda p: p.update(labels={"optional": {"team": {}}}),
        lambda p: p.update(labels={"required": {"Bad Key": {}}}),
        lambda p: p.update(labels={"required": {"a/b/c": {}}}),
        lambda p: p.update(labels={"required": {"Example.com/team": {}}}),
        lambda p: p.update(labels={"required": {"team": {"values": []}}}),
        lambda p: p.update(labels={"required": {"team": {"values": ["bad value"]}}}),
        lambda p: p.update(labels={"required": {"team": {"values": ["a"], "pattern": "a"}}}),
        lambda p: p.update(labels={"required": {"team": {"pattern": "("}}}),
        lambda p: p.update(labels={"required": {"team": {"pattern": ""}}}),
        lambda p: p.update(labels={"required": {"team": {"other": True}}}),
        lambda p: p.update(labels={"required": {"team": {}}, "fixed": {"team": "data"}}),
        lambda p: p.update(labels={"fixed": {"team": ""}}),
        lambda p: p.update(labels={"fixed": {"team": 1}}),
    ],
)
def test_invalid_placement_and_label_policies_fail_closed(change):
    policy = policy_document()
    change(policy)
    with pytest.raises(ValueError):
        SubmissionPolicy.from_mapping(policy)


def test_schema_version_1_policies_keep_their_meaning():
    policy = SubmissionPolicy.from_mapping(policy_document())
    assert policy.required_labels == {} and policy.fixed_labels == {}
    options = policy.admit(job_document())
    assert options["driver_labels"] == {} and options["executor_labels"] == {}


def test_rotation_overlap_accepts_either_token_and_compares_every_token(monkeypatch):
    settings = SubmissionSettings(True, TOKEN.encode(), None, (NEXT_TOKEN.encode(),))
    compared = []
    real = hmac.compare_digest
    monkeypatch.setattr(
        hmac, "compare_digest", lambda left, right: compared.append(right) or real(left, right)
    )
    assert settings.accepts(TOKEN.encode()) and settings.accepts(NEXT_TOKEN.encode())
    assert not settings.accepts(b"wrong") and not settings.accepts(b"")
    # A match on the first token still compares the second, so timing does not reveal which matched.
    assert compared == [TOKEN.encode(), NEXT_TOKEN.encode()] * 4
    assert TOKEN not in repr(settings) and NEXT_TOKEN not in repr(settings)


def write_policy_files(directory, token):
    token_file, policy_file = directory / "token", directory / "policy.json"
    token_file.write_text(token)
    policy_file.write_text(json.dumps(policy_document()))
    return token_file, policy_file


@pytest.mark.parametrize(
    "content, accepted",
    [
        (TOKEN + "\n", [TOKEN]),
        (f"{TOKEN}\r\n{NEXT_TOKEN}\r\n", [TOKEN, NEXT_TOKEN]),
        (f"\n  {TOKEN}\n\n{NEXT_TOKEN}", [TOKEN, NEXT_TOKEN]),
    ],
)
def test_token_file_holds_one_token_or_a_rotation_pair(tmp_path, content, accepted):
    settings = SubmissionSettings.read(*write_policy_files(tmp_path, content))
    assert [token.decode() for token in (settings.token, *settings.extra_tokens)] == accepted


THIRD_TOKEN = "c-third-token-with-at-least-32-characters"


@pytest.mark.parametrize(
    "content", ["", "\n\n", f"{TOKEN}\n{NEXT_TOKEN}\n{THIRD_TOKEN}", f"{TOKEN}\nweak", TOKEN + " x"]
)
def test_invalid_token_files_fail_closed(tmp_path, content):
    with pytest.raises(ValueError, match="token"):
        SubmissionSettings.read(*write_policy_files(tmp_path, content))


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def replace_file(path, content):
    # Like an atomic configuration update: a new file is renamed over the old one.
    staged = path.with_name(path.name + ".new")
    staged.write_text(content)
    os.replace(staged, path)


@pytest.fixture
def remote_files(tmp_path, monkeypatch):
    token_file, policy_file = write_policy_files(tmp_path, TOKEN + "\n")
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "true")
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_POLICY_FILE", str(policy_file))
    monkeypatch.delenv("SPARK_ON_K8S_SUBMISSION_RELOAD_INTERVAL", raising=False)
    return token_file, policy_file


def test_token_rotation_takes_effect_without_restart(remote_files, monkeypatch):
    token_file, _ = remote_files
    clock = Clock()
    source = SettingsSource(clock)
    monkeypatch.setattr(remote_auth, "get_submission_settings", source.get)
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)

    @app.get("/probe")
    async def probe():
        return {}

    client = TestClient(app)

    def statuses():
        return [
            client.get("/probe", headers={"Authorization": "Bearer " + token}).status_code
            for token in (TOKEN, NEXT_TOKEN)
        ]

    assert source.get().reload_interval == 10
    assert statuses() == [200, 401]
    # 1. Add the new token next to the current one, in place.
    token_file.write_text(f"{TOKEN}\n{NEXT_TOKEN}\n")
    clock.now = 9
    assert statuses() == [200, 401]  # Files are checked at most once per interval.
    clock.now = 10
    assert statuses() == [200, 200]
    # 2. After every client uses the new token, remove the old one.
    replace_file(token_file, NEXT_TOKEN)
    clock.now = 20
    assert statuses() == [401, 200]


def kubernetes_secret_update(directory, version, files):
    """Update a directory as the kubelet updates a Secret volume: swap the ..data symlink atomically."""
    data = directory / f"..{version}"
    data.mkdir()
    for name, content in files.items():
        (data / name).write_text(content)
    (directory / "..data_tmp").symlink_to(data.name)
    os.replace(directory / "..data_tmp", directory / "..data")
    for name in files:
        if not (directory / name).is_symlink():
            (directory / name).symlink_to(Path("..data") / name)
    return data


def test_reload_follows_kubernetes_secret_symlink_swaps(tmp_path, monkeypatch):
    policy = json.dumps(policy_document())
    first = kubernetes_secret_update(tmp_path, "2026_11_09_1", {"token": TOKEN, "policy.json": policy})
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "true")
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_TOKEN_FILE", str(tmp_path / "token"))
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_POLICY_FILE", str(tmp_path / "policy.json"))
    clock = Clock()
    source = SettingsSource(clock)
    assert source.get().accepts(TOKEN.encode())
    # Same sizes: only the identities of the swapped files differ.
    kubernetes_secret_update(tmp_path, "2026_11_09_2", {"token": NEXT_TOKEN, "policy.json": policy})
    shutil.rmtree(first)
    clock.now = 10
    settings = source.get()
    assert settings.accepts(NEXT_TOKEN.encode()) and not settings.accepts(TOKEN.encode())


def outside_scope_policy():
    policy = policy_document()
    policy["namespaces"]["kube-system"] = ["spark-worker"]
    return json.dumps(policy)


@pytest.mark.parametrize(
    "change",
    [
        lambda token, policy: replace_file(policy, "{not json"),
        lambda token, policy: replace_file(policy, json.dumps({**policy_document(), "schema_version": 3})),
        lambda token, policy: replace_file(policy, outside_scope_policy()),
        lambda token, policy: replace_file(token, f"{TOKEN}\n{NEXT_TOKEN}\n{THIRD_TOKEN}"),
        lambda token, policy: replace_file(token, "weak"),
        lambda token, policy: token.unlink(),
    ],
)
def test_invalid_change_keeps_the_last_valid_settings_and_logs_once(
    remote_files, monkeypatch, caplog, change
):
    from spark_on_k8s.api.configuration import APIConfiguration

    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", "analytics")
    token_file, policy_file = remote_files
    clock = Clock()
    source = SettingsSource(clock)
    original = source.get()
    change(token_file, policy_file)
    with caplog.at_level(logging.WARNING, logger="spark_on_k8s.api.submission_policy"):
        for clock.now in (10, 20, 30):
            assert source.get() is original
        errors = [record for record in caplog.records if record.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "Kept the previous remote submission token and policy" in errors[0].getMessage()
        # A corrected change is picked up at the next check.
        replace_file(token_file, NEXT_TOKEN)
        replace_file(policy_file, json.dumps(policy_document()))
        clock.now = 40
        updated = source.get()
    assert updated.accepts(NEXT_TOKEN.encode()) and not updated.accepts(TOKEN.encode())
    assert "Reloaded the remote submission token and policy files; accepted tokens: 1." in caplog.text
    assert TOKEN not in caplog.text and NEXT_TOKEN not in caplog.text


def test_unchanged_files_are_not_read_again(remote_files, monkeypatch):
    clock = Clock()
    source = SettingsSource(clock)
    original = source.get()
    monkeypatch.setattr(SubmissionSettings, "read", Mock(side_effect=AssertionError("re-read")))
    clock.now = 100
    assert source.get() is original


def test_zero_reload_interval_reads_the_files_once(remote_files, monkeypatch):
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_RELOAD_INTERVAL", "0")
    clock = Clock()
    source = SettingsSource(clock)
    original = source.get()
    replace_file(remote_files[0], NEXT_TOKEN)
    clock.now = 10_000
    assert source.get() is original


@pytest.mark.parametrize("value", ["-1", "1.5", "3601", "soon", ""])
def test_invalid_reload_interval_fails_closed(remote_files, monkeypatch, value):
    monkeypatch.setenv("SPARK_ON_K8S_SUBMISSION_RELOAD_INTERVAL", value)
    with pytest.raises(ValueError, match="SPARK_ON_K8S_SUBMISSION_RELOAD_INTERVAL"):
        SubmissionSettings.from_env()
