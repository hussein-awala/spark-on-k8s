from __future__ import annotations

from importlib.metadata import version
from unittest import mock

from fastapi.testclient import TestClient
from kubernetes_asyncio import client as k8s
from spark_on_k8s.api import AsyncHttpClientSingleton, KubernetesClientSingleton
from spark_on_k8s.api.main import app


def test_real_application_lifecycle_routes_and_assets(monkeypatch):
    kubernetes_client = mock.Mock(close=mock.AsyncMock())
    create_client = mock.AsyncMock(return_value=kubernetes_client)
    http_client = mock.Mock(aclose=mock.AsyncMock())
    monkeypatch.setattr(KubernetesClientSingleton, "_client", None)
    monkeypatch.setattr(AsyncHttpClientSingleton, "_client", None)
    monkeypatch.setattr(KubernetesClientSingleton.client_manager, "create_client", create_client)
    with (
        mock.patch("spark_on_k8s.api.AsyncClient", return_value=http_client) as http_factory,
        mock.patch("spark_on_k8s.api.apps.CoreV1Api") as api_class,
    ):
        api_class.return_value.list_namespaced_pod = mock.AsyncMock(
            return_value=k8s.V1PodList(items=[], metadata=k8s.V1ListMeta())
        )
        with TestClient(app) as client:
            assert client.get("/").json() == {"message": "Welcome to Spark on Kubernetes!"}
            assert client.get("/health").json() == {"status": "ok"}
            assert client.head("/health").status_code == 200
            assert client.head("/health").content == b""
            schema = client.get("/openapi.json").json()
            assert schema["info"]["version"] == version("spark-on-k8s")
            assert "/app/{namespace}/{app_id}" in schema["paths"]
            assert "/apps/list_apps/{namespace}" in schema["paths"]
            assert client.get("/apps/list_apps/analytics").json() == []
            assert client.get("/apps/list_apps/analytics").json() == []
            logs = client.get("/webserver/logs/analytics/app-1")
            assert logs.status_code == 200
            assert "app-1" in logs.text
            stylesheet = client.get("/static/ui/console.css")
            assert stylesheet.status_code == 200
            assert stylesheet.headers["content-type"].startswith("text/css")
            assert ".spark-console" in stylesheet.text
            bundle = client.get("/static/ui/console.js")
            assert bundle.status_code == 200
            assert len(bundle.content) > 1000
            create_client.assert_awaited_once()
            http_factory.assert_called_once_with()
            kubernetes_client.close.assert_not_awaited()
            http_client.aclose.assert_not_awaited()
        create_client.assert_awaited_once()
        http_factory.assert_called_once_with()
    kubernetes_client.close.assert_awaited_once_with()
    http_client.aclose.assert_awaited_once_with()
