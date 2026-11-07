from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from spark_on_k8s.api.app import router


@pytest.mark.parametrize("query, force", [("", False), ("?force=false", False), ("?force=true", True)])
def test_delete_app_forwards_force(query, force):
    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.delete_app = mock.AsyncMock()

        response = TestClient(app).delete(f"/app/test-namespace/test-app{query}")

    assert response.status_code == 200
    manager_class.return_value.delete_app.assert_awaited_once_with(
        namespace="test-namespace", app_id="test-app", force=force
    )


def test_kill_app_forwards_the_target_and_returns_success():
    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.kill_app = mock.AsyncMock()
        response = TestClient(app).patch("/app/analytics/app-1")
    assert response.status_code == 200
    assert response.content == b""
    manager_class.return_value.kill_app.assert_awaited_once_with(namespace="analytics", app_id="app-1")


@pytest.mark.parametrize("method, manager_method", [("PATCH", "kill_app"), ("DELETE", "delete_app")])
def test_failed_app_action_returns_a_safe_error_and_logs_details(caplog, method, manager_method):
    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        action = mock.AsyncMock(side_effect=RuntimeError("private Kubernetes failure details"))
        setattr(manager_class.return_value, manager_method, action)
        response = TestClient(app).request(method, "/app/analytics/app-1")
    assert response.status_code == 500
    assert "please contact the administrator" in response.text
    assert "private Kubernetes failure details" not in response.text
    assert "private Kubernetes failure details" in caplog.text
    action.assert_awaited_once()
