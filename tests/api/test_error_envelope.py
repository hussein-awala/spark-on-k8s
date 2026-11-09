from __future__ import annotations

from unittest import mock

import pytest
from fastapi.testclient import TestClient
from spark_on_k8s.api import KubernetesClientSingleton
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.main import app


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", None)
    # No lifespan: these requests never need the real Kubernetes or HTTP clients.
    return TestClient(app, raise_server_exceptions=False)


def envelope(response):
    body = response.json()
    assert set(body) == {"schema_version", "error"}, body
    assert body["schema_version"] == 1
    assert set(body["error"]) == {"code", "message"}
    return body["error"]


@pytest.mark.parametrize(
    "method, path, status, code, message",
    [
        ("get", "/no-such-route", 404, "not_found", "Not Found"),
        ("post", "/health", 405, "method_not_allowed", "Method Not Allowed"),
        # Structured route errors keep their specific code.
        (
            "get",
            "/webserver/api/history",
            412,
            "history_not_configured",
            "Spark History Server is not configured.",
        ),
        # Plain-text route errors gain a code derived from their status.
        (
            "get",
            "/webserver/ui-history/app",
            412,
            "not_configured",
            "SPARK_ON_K8S_API_SPARK_HISTORY_HOST is not set.",
        ),
        ("get", "/webserver/ui/analytics", 404, "not_found", "A namespace and Spark service are required."),
    ],
)
def test_every_route_error_uses_the_versioned_envelope(client, method, path, status, code, message):
    response = getattr(client, method)(path)
    assert response.status_code == status
    assert envelope(response) == {"code": code, "message": message}


def test_request_validation_errors_name_the_invalid_field(client):
    response = client.get("/webserver/api/apps", params={"limit": 0})
    assert response.status_code == 422
    error = envelope(response)
    assert error["code"] == "invalid_request"
    assert error["message"].startswith("query.limit: ")


def test_unexpected_failures_return_an_opaque_reference(client, monkeypatch, caplog):
    monkeypatch.setattr(
        KubernetesClientSingleton, "client", mock.AsyncMock(side_effect=RuntimeError("secret"))
    )
    response = client.get("/apps/list_apps/analytics")
    assert response.status_code == 500
    error = envelope(response)
    assert error["code"] == "internal_error"
    assert "secret" not in error["message"]
    reference = error["message"].removeprefix("The query failed with exception ").split(",")[0]
    assert any(reference in message for message in caplog.messages)


def test_kill_failures_share_the_envelope(client):
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager:
        manager.return_value.kill_app = mock.AsyncMock(side_effect=RuntimeError("boom"))
        response = client.patch("/app/analytics/app-1")
    assert response.status_code == 500
    assert envelope(response)["code"] == "internal_error"


def test_remote_job_errors_already_use_the_same_envelope(client):
    response = client.post("/jobs/v1/plan", json={})
    assert response.status_code == 404
    assert envelope(response) == {"code": "remote_disabled", "message": "Remote submission is disabled."}
