"""Kubernetes refusing a caller-supplied identifier or continuation token is a 400, audited as rejected."""

from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes_asyncio import client as k8s
from kubernetes_asyncio.client import ApiException
from spark_on_k8s.api import app as app_routes, apps as apps_routes
from spark_on_k8s.api.audit import _outcome
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.utils import install_error_handlers


@pytest.fixture(autouse=True)
def any_namespace(monkeypatch):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", "*")


@pytest.mark.parametrize("status", [400, 422])
@pytest.mark.parametrize("method, manager_method", [("PATCH", "kill_app"), ("DELETE", "delete_app")])
def test_a_target_kubernetes_rejects_is_a_client_error_and_not_an_unknown_outcome(
    method, manager_method, status
):
    # "spark-app-id=a b,spark-role=driver" is not a label selector; Kubernetes answers 400 and
    # nothing was changed, yet the caller sees internal_error and the audit line says "unknown".
    api = FastAPI()
    install_error_handlers(api)
    api.include_router(app_routes.router)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        error = ApiException(status=status, reason="Bad Request")
        setattr(manager_class.return_value, manager_method, mock.AsyncMock(side_effect=error))
        response = TestClient(api, raise_server_exceptions=False).request(method, "/app/analytics/a%20b")
    code = response.json()["error"]["code"]
    event = "app.kill" if method == "PATCH" else "app.delete"
    assert (response.status_code, _outcome(event, response.status_code, code)) == (400, "rejected")


@pytest.mark.parametrize("status", [400, 410])
def test_a_continuation_token_kubernetes_rejects_is_a_client_error(status):
    # Any string is forwarded as the continue token; an invalid one is 400 and an expired one 410.
    api = FastAPI()
    install_error_handlers(api)
    api.include_router(apps_routes.router)
    with (
        mock.patch("spark_on_k8s.api.apps.KubernetesClientSingleton.client", new_callable=mock.AsyncMock),
        mock.patch("spark_on_k8s.api.apps.CoreV1Api") as api_class,
    ):
        api_class.return_value.list_namespaced_pod = mock.AsyncMock(
            side_effect=ApiException(status=status, reason="Expired"),
            return_value=k8s.V1PodList(items=[], metadata=k8s.V1ListMeta()),
        )
        response = TestClient(api, raise_server_exceptions=False).get(
            "/apps/list_apps/analytics", params={"continue_from": "not-a-token"}
        )
    assert response.status_code in {400, 410}


@pytest.mark.parametrize("continue_from,status", [("", 400), ("a-token", 503)])
def test_other_listing_errors_keep_their_handling(continue_from, status):
    # Only a refused continuation token is the caller's mistake; other errors are not reworded.
    api = FastAPI()
    install_error_handlers(api)
    api.include_router(apps_routes.router)
    with (
        mock.patch("spark_on_k8s.api.apps.KubernetesClientSingleton.client", new_callable=mock.AsyncMock),
        mock.patch("spark_on_k8s.api.apps.CoreV1Api") as api_class,
    ):
        api_class.return_value.list_namespaced_pod = mock.AsyncMock(
            side_effect=ApiException(status=status, reason="Refused")
        )
        response = TestClient(api, raise_server_exceptions=False).get(
            "/apps/list_apps/analytics", params={"continue_from": continue_from}
        )
    assert "continuation token" not in response.text
