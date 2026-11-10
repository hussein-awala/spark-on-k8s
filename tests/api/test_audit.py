from __future__ import annotations

import json
import logging
import re
import sys
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from kubernetes.client.exceptions import ApiException
from kubernetes_asyncio.client import ApiException as AsyncApiException
from spark_on_k8s.api import KubernetesClientSingleton, audit, remote_auth, run_history, submission
from spark_on_k8s.api.audit import AuditSettings
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.main import app
from spark_on_k8s.api.submission_policy import SubmissionPolicy, SubmissionSettings
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.history.settings import HistorySettings
from starlette.websockets import WebSocketDisconnect

TOKEN = "an-audit-test-operator-token-with-48-characters"
IMAGE = "spark@sha256:" + "b" * 64
FIELDS = {
    "audit_version",
    "time",
    "event",
    "outcome",
    "code",
    "status",
    "request_id",
    "method",
    "route",
    "client",
    "unverified_identity",
    "namespace",
    "app_id",
    "driver_pod",
    "driver_uid",
}


def job(**spec):
    return {
        "apiVersion": "spark-on-k8s/v1",
        "kind": "SparkJob",
        "metadata": {"name": "daily-report"},
        "spec": {
            "image": IMAGE,
            "app_path": "local:///jobs/report.py",
            "namespace": "analytics",
            "service_account": "spark-worker",
            **spec,
        },
    }


@pytest.fixture
def events(caplog):
    caplog.set_level(logging.INFO, logger="spark_on_k8s.audit")

    def read():
        return [json.loads(r.getMessage()) for r in caplog.records if r.name == "spark_on_k8s.audit"]

    return read


@pytest.fixture
def remote(monkeypatch):
    policy = SubmissionPolicy.from_mapping(
        {
            "schema_version": 1,
            "namespaces": {"analytics": ["spark-worker"]},
            "applications": [
                {"image": IMAGE, "app_path": "local:///jobs/report.py", "arguments": [[], ["private-arg"]]}
            ],
            "limits": {
                "driver_cpu": 2,
                "driver_memory_mb": 2048,
                "executor_cpu": 2,
                "executor_memory_mb": 2048,
                "executors": 4,
            },
            "spark_conf": {"spark.sql.session.timeZone": ["Private/Zone"]},
        }
    )
    settings = SubmissionSettings(True, TOKEN.encode(), policy)
    monkeypatch.setattr(remote_auth, "get_submission_settings", lambda: settings)
    monkeypatch.setattr(submission, "get_submission_settings", lambda: settings)
    api = mock.Mock()

    def create_pod(**kwargs):
        kwargs["body"].metadata.uid = "driver-uid"
        return kwargs["body"]

    api.create_namespaced_pod.side_effect = create_pod
    monkeypatch.setattr("spark_on_k8s.client.k8s.CoreV1Api", mock.Mock(return_value=api))
    manager = mock.Mock()
    manager.client.return_value = mock.MagicMock()
    monkeypatch.setattr(
        submission, "create_submission_client", lambda: SparkOnK8S(k8s_client_manager=manager)
    )
    client = TestClient(app, headers={"Authorization": "Bearer " + TOKEN}, raise_server_exceptions=False)
    return client, api


def test_submission_records_the_created_run_without_the_document_or_token(remote, events, caplog):
    client, api = remote
    document = job(app_arguments=["private-arg"], spark_conf={"spark.sql.session.timeZone": "Private/Zone"})
    response = client.post("/jobs/v1/submit?trace=private-query", json=document)
    assert response.status_code == 201, response.text
    run = response.json()["run"]
    [event] = events()
    assert set(event) == FIELDS
    assert event == {
        "audit_version": 1,
        "time": event["time"],
        "event": "job.submit",
        "outcome": "succeeded",
        "code": "ok",
        "status": 201,
        "request_id": response.headers["x-request-id"],
        "method": "POST",
        "route": "/jobs/v1/submit",
        "client": "testclient",
        "unverified_identity": None,
        **run,
    }
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", event["time"])
    # Job documents, Spark configuration, query strings and credentials never reach the record.
    for private in ("private-arg", "Private/Zone", "private-query", IMAGE, "Bearer"):
        assert private not in json.dumps(event)
    assert TOKEN not in caplog.text


def test_plans_and_policy_denials_are_recorded_without_a_run_identity(remote, events):
    client, api = remote
    assert client.post("/jobs/v1/plan", json=job()).status_code == 200
    assert client.post("/jobs/v1/submit", json=job(namespace="kube-system")).status_code == 403
    assert client.post("/jobs/v1/submit", content=b"{}").status_code == 415
    plan, denied, invalid = events()
    assert (plan["event"], plan["outcome"], plan["code"], plan["status"]) == (
        "job.plan",
        "succeeded",
        "ok",
        200,
    )
    # A plan's generated identity is illustrative, not a run; only its namespace is recorded.
    assert (plan["namespace"], plan["app_id"], plan["driver_pod"]) == ("analytics", None, None)
    assert (denied["event"], denied["outcome"], denied["code"]) == ("job.submit", "denied", "policy_denied")
    assert denied["namespace"] is None and denied["app_id"] is None
    assert (invalid["outcome"], invalid["code"]) == ("rejected", "invalid_content_type")
    api.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("status, http_status", [(500, 502), (409, 409)])
def test_ambiguous_kubernetes_failure_records_the_intended_run_as_unknown(
    remote, events, caplog, status, http_status
):
    client, api = remote
    api.create_namespaced_pod.side_effect = ApiException(status=status, reason="private Kubernetes body")
    response = client.post("/jobs/v1/submit", json=job())
    assert response.status_code == http_status
    [event] = events()
    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    # Resources may exist: the record names the identity to inspect, not an absence.
    assert (event["outcome"], event["code"]) == ("unknown", "submission_error")
    assert event["namespace"] == "analytics"
    assert event["app_id"] == pod.metadata.labels["spark-app-id"]
    assert event["driver_pod"] == pod.metadata.name
    assert event["driver_uid"] is None
    assert "private Kubernetes body" not in caplog.text
    assert f'spark_on_k8s_api_kubernetes_errors_total{{status_class="{status // 100}xx"}} 1' in (
        audit.metric_lines()
    )


@pytest.mark.parametrize(
    "error, status, outcome, code",
    [
        (None, 200, "succeeded", "ok"),
        (AsyncApiException(status=404, reason="private body"), 404, "rejected", "application_not_found"),
        (AsyncApiException(status=403, reason="private body"), 403, "denied", "forbidden"),
        (ValueError("Pod private-driver is not running"), 409, "rejected", "application_state"),
        # The signal or deletion may have reached Kubernetes before the failure.
        (AsyncApiException(status=503, reason="private body"), 500, "unknown", "internal_error"),
        (RuntimeError("private failure"), 500, "unknown", "internal_error"),
    ],
)
@pytest.mark.parametrize(
    "method, manager_method, event_type",
    [("PATCH", "kill_app", "app.kill"), ("DELETE", "delete_app", "app.delete")],
)
def test_kill_and_delete_record_target_and_outcome(
    events, caplog, error, status, outcome, code, method, manager_method, event_type
):
    client = TestClient(app, raise_server_exceptions=False)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        setattr(manager_class.return_value, manager_method, mock.AsyncMock(side_effect=error))
        response = client.request(method, "/app/analytics/app-1?force=true")
    assert response.status_code == status
    [event] = events()
    assert {key: event[key] for key in ("event", "outcome", "code", "status", "method", "route")} == {
        "event": event_type,
        "outcome": outcome,
        "code": code,
        "status": status,
        "method": method,
        "route": "/app/{namespace}/{app_id}",
    }
    assert (event["namespace"], event["app_id"]) == ("analytics", "app-1")
    assert event["request_id"] == response.headers["x-request-id"]
    assert "private" not in json.dumps(event) and "force" not in json.dumps(event)
    errors = [line for line in audit.metric_lines() if line.startswith("spark_on_k8s_api_kubernetes")]
    if isinstance(error, AsyncApiException):
        assert errors == [
            f'spark_on_k8s_api_kubernetes_errors_total{{status_class="{error.status // 100}xx"}} 1'
        ]
    else:
        assert errors == []


@pytest.mark.parametrize(
    "method, path, event_type, route, namespace",
    [
        ("PATCH", "/app/analytics/run", "app.kill", "/app/{namespace}/{app_id}", "analytics"),
        ("POST", "/jobs/v1/submit", "job.submit", "/jobs/v1/submit", None),
        ("DELETE", "/app/kube-system/run", "app.delete", "/app/{namespace}/{app_id}", "kube-system"),
        # Authentication precedes routing: other refused requests are recorded without a route.
        ("GET", "/webserver/apps", "access", None, None),
        ("GET", "/metrics", "access", None, None),
        ("GET", "/no-such-route?token=private-query", "access", None, None),
    ],
)
def test_authentication_failures_are_recorded_without_credentials(
    remote, events, caplog, method, path, event_type, route, namespace
):
    response = TestClient(app).request(method, path, headers={"Authorization": "Bearer private-wrong-token"})
    assert response.status_code == 401
    [event] = events()
    assert (event["event"], event["outcome"], event["code"], event["status"]) == (
        event_type,
        "denied",
        "unauthorized",
        401,
    )
    assert (event["method"], event["route"], event["namespace"]) == (method, route, namespace)
    assert event["request_id"] == response.headers["x-request-id"]
    assert "private" not in caplog.text and TOKEN not in caplog.text


def test_public_probes_and_authorized_reads_are_not_audited(remote, events, monkeypatch):
    client, _ = remote
    unauthenticated = TestClient(app)
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", None)
    assert unauthenticated.get("/health").status_code == 200
    assert client.get("/webserver/api/capabilities").status_code == 200
    assert client.get("/no-such-route").status_code == 404
    assert events() == []
    assert audit.metric_lines() == []


def test_namespace_scope_refusals_are_recorded_with_the_refused_namespace(events, monkeypatch):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", "analytics")
    client = TestClient(app)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        assert client.delete("/app/kube-system/app-1").status_code == 403
        manager_class.assert_not_called()
    assert client.get("/webserver/ui/kube-system/kube-dns/").status_code == 403
    assert client.get("/webserver/api/apps", params={"namespace": "kube-system"}).status_code == 403
    deleted, proxied, listed = events()
    assert (deleted["event"], deleted["outcome"], deleted["code"]) == (
        "app.delete",
        "denied",
        "namespace_forbidden",
    )
    assert (deleted["namespace"], deleted["app_id"]) == ("kube-system", "app-1")
    assert (proxied["event"], proxied["route"], proxied["namespace"]) == (
        "access",
        "/webserver/ui/{path:path}",
        "kube-system",
    )
    assert (listed["event"], listed["route"], listed["namespace"]) == (
        "access",
        "/webserver/api/apps",
        "kube-system",
    )
    assert all(event["code"] == "namespace_forbidden" and event["status"] == 403 for event in events())


def test_refused_log_websockets_are_recorded(remote, events, monkeypatch):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", "analytics")
    for headers in ({}, {"Authorization": "Bearer " + TOKEN}):
        with (
            pytest.raises(WebSocketDisconnect),
            TestClient(app).websocket_connect("/webserver/ws/logs/kube-system/app-1", headers=headers),
        ):
            pass
    unauthenticated, outside = events()
    for event, code in ((unauthenticated, "unauthorized"), (outside, "namespace_forbidden")):
        assert (event["event"], event["outcome"], event["code"], event["status"]) == (
            "access",
            "denied",
            code,
            403,
        )
        assert event["method"] == "WEBSOCKET"
    assert (unauthenticated["route"], unauthenticated["namespace"]) == (None, None)
    assert outside["route"] == "/webserver/ws/logs/{namespace}/{app_id}"
    assert (outside["namespace"], outside["app_id"]) == ("kube-system", "app-1")


def test_every_response_carries_a_fresh_server_generated_request_id(monkeypatch, events):
    client = TestClient(app, raise_server_exceptions=False)
    first = client.get("/health", headers={"X-Request-ID": "client-chosen"})
    second = client.get("/health")
    assert first.headers["x-request-id"] != second.headers["x-request-id"]
    assert all(re.fullmatch(r"[0-9a-f]{32}", r.headers["x-request-id"]) for r in (first, second))
    assert first.headers.get_list("x-request-id") == [first.headers["x-request-id"]]
    # Unhandled exceptions are answered outside the middleware stack and still carry the ID.
    monkeypatch.setattr(
        KubernetesClientSingleton, "client", mock.AsyncMock(side_effect=AsyncApiException(status=500))
    )
    failed = client.get("/apps/list_apps/analytics")
    assert failed.status_code == 500
    assert re.fullmatch(r"[0-9a-f]{32}", failed.headers["x-request-id"])
    assert audit.metric_lines()[-1] == 'spark_on_k8s_api_kubernetes_errors_total{status_class="5xx"} 1'
    assert events() == []


def test_disabled_audit_log_still_counts_operations(monkeypatch, events):
    monkeypatch.setattr(audit, "get_audit_settings", lambda: AuditSettings(enabled=False))
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.kill_app = mock.AsyncMock()
        response = TestClient(app).patch("/app/analytics/app-1")
    assert response.status_code == 200
    assert response.headers["x-request-id"]
    assert events() == []
    assert 'spark_on_k8s_api_audit_events_total{event="app.kill",outcome="succeeded",code="ok"} 1' in (
        audit.metric_lines()
    )


@pytest.mark.parametrize(
    "configured, headers, identity",
    [
        (None, [("X-Auth-Request-User", "alice")], None),
        ("x-auth-request-user", [("X-Auth-Request-User", "alice")], "alice"),
        ("x-auth-request-user", [], None),
        # A repeated header means the gateway did not replace the client's value.
        ("x-auth-request-user", [("X-Auth-Request-User", "alice"), ("X-Auth-Request-User", "bob")], None),
        ("x-auth-request-user", [("X-Auth-Request-User", "x" * 1000)], "x" * 256),
    ],
)
def test_gateway_identity_is_recorded_only_when_configured_and_unverified(
    monkeypatch, events, configured, headers, identity
):
    monkeypatch.setattr(audit, "get_audit_settings", lambda: AuditSettings(identity_header=configured))
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.kill_app = mock.AsyncMock()
        TestClient(app).patch("/app/analytics/app-1", headers=headers)
    [event] = events()
    assert event["unverified_identity"] == identity


def test_settings_default_to_enabled_and_refuse_credential_headers(monkeypatch):
    monkeypatch.delenv("SPARK_ON_K8S_AUDIT_LOG_ENABLED", raising=False)
    monkeypatch.delenv("SPARK_ON_K8S_AUDIT_IDENTITY_HEADER", raising=False)
    assert AuditSettings.from_env() == AuditSettings(enabled=True, identity_header=None)
    monkeypatch.setenv("SPARK_ON_K8S_AUDIT_LOG_ENABLED", "false")
    monkeypatch.setenv("SPARK_ON_K8S_AUDIT_IDENTITY_HEADER", " X-Auth-Request-Email ")
    assert AuditSettings.from_env() == AuditSettings(enabled=False, identity_header="x-auth-request-email")
    monkeypatch.setenv("SPARK_ON_K8S_AUDIT_LOG_ENABLED", "maybe")
    with pytest.raises(ValueError, match="true or false"):
        AuditSettings.from_env()
    monkeypatch.setenv("SPARK_ON_K8S_AUDIT_LOG_ENABLED", "true")
    for header in ("Authorization", "cookie", "Proxy-Authorization", "x user", "x-user:"):
        monkeypatch.setenv("SPARK_ON_K8S_AUDIT_IDENTITY_HEADER", header)
        with pytest.raises(ValueError, match="not a credential header"):
            AuditSettings.from_env()


def test_audit_log_writes_one_json_line_per_event_to_stdout(monkeypatch, capsys):
    isolated = logging.Logger("isolated-audit")
    monkeypatch.setattr(audit, "logger", isolated)
    monkeypatch.setattr(audit, "get_audit_settings", lambda: AuditSettings(enabled=False))
    audit.configure_audit_log()
    assert isolated.handlers == []
    monkeypatch.setattr(audit, "get_audit_settings", AuditSettings)
    audit.configure_audit_log()
    audit.configure_audit_log()
    assert [type(handler) for handler in isolated.handlers] == [logging.StreamHandler]
    assert isolated.handlers[0].stream is sys.stdout
    assert isolated.level == logging.INFO
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.delete_app = mock.AsyncMock()
        TestClient(app).delete("/app/analytics/app-1")
    [line] = capsys.readouterr().out.splitlines()
    assert json.loads(line)["event"] == "app.delete"


def test_metrics_expose_bounded_per_process_counters(monkeypatch):
    monkeypatch.setattr(run_history, "get_history_settings", lambda: HistorySettings(metrics_enabled=True))
    client = TestClient(app)
    assert "spark_on_k8s_api" not in client.get("/metrics").text
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.kill_app = mock.AsyncMock(side_effect=[None, None, AsyncApiException(404)])
        for namespace in ("analytics", "batch", "analytics"):
            client.patch(f"/app/{namespace}/app-{namespace}")
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", "analytics")
    client.get("/webserver/ui/kube-system/kube-dns/")
    text = client.get("/metrics").text
    assert text.endswith("\n")
    lines = [line for line in text.splitlines() if "spark_on_k8s_api_" in line]
    assert lines == [
        "# HELP spark_on_k8s_api_audit_events_total Audited API operations and access refusals "
        "by event, outcome and code, in this worker process.",
        "# TYPE spark_on_k8s_api_audit_events_total counter",
        'spark_on_k8s_api_audit_events_total{event="access",outcome="denied",code="namespace_forbidden"} 1',
        'spark_on_k8s_api_audit_events_total{event="app.kill",outcome="rejected",'
        'code="application_not_found"} 1',
        'spark_on_k8s_api_audit_events_total{event="app.kill",outcome="succeeded",code="ok"} 2',
        "# HELP spark_on_k8s_api_kubernetes_errors_total Kubernetes API error responses while serving "
        "API requests, by status class, in this worker process.",
        "# TYPE spark_on_k8s_api_kubernetes_errors_total counter",
        'spark_on_k8s_api_kubernetes_errors_total{status_class="4xx"} 1',
    ]
    # Labels never carry namespaces, application IDs or other request-controlled values.
    assert "analytics" not in text and "kube-system" not in text and "app-" not in text


def test_refused_operations_are_named_below_a_deployment_prefix(remote, events):
    client = TestClient(app, root_path="/platform/spark")
    assert client.patch("/platform/spark/app/analytics/run").status_code == 401
    assert client.patch("/app/analytics/run").status_code == 401
    prefixed, direct = events()
    assert (prefixed["event"], prefixed["route"], prefixed["namespace"]) == (
        "app.kill",
        "/app/{namespace}/{app_id}",
        "analytics",
    )
    assert direct["event"] == "app.kill"
