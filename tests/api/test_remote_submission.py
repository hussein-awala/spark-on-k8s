from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock, Mock

import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.api import remote_auth, submission
from spark_on_k8s.api.remote_auth import RemoteAccessMiddleware
from spark_on_k8s.api.submission_policy import SubmissionPolicy, SubmissionSettings
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.utils.configuration import Configuration
from starlette.websockets import WebSocketDisconnect

TOKEN = "a-test-token-with-at-least-32-characters"


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
        ("executor_node_selector", {"node": "privileged"}),
        ("spark_conf", {"spark.kubernetes.executor.volumes.hostPath.escape.mount.path": "/"}),
        ("spark_conf", {"spark.plugins": "arbitrary.Code"}),
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
