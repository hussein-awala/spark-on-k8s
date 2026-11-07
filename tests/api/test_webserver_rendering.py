from __future__ import annotations

from unittest import mock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from spark_on_k8s.api.apps import SparkApp
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.webserver import current_dir, router
from spark_on_k8s.utils.spark_app_status import SparkAppStatus
from starlette.staticfiles import StaticFiles


@pytest.fixture
def web_client():
    app = FastAPI()
    app.include_router(router)
    app.mount("/static", StaticFiles(directory=current_dir / "static"), name="static")
    with TestClient(app) as client:
        yield client


def test_app_list_renders_status_and_links(web_client, monkeypatch):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", None)
    application = SparkApp(
        app_id="app-1", status=SparkAppStatus.Running, driver_logs=True, spark_ui_proxy=True
    )
    with mock.patch("spark_on_k8s.api.webserver.list_apps", return_value=[application]) as list_apps:
        response = web_client.get("/webserver/apps?namespace=test-namespace")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "app-1" in response.text
    assert "Running" in response.text
    assert "/webserver/ui/test-namespace/app-1" in response.text
    assert "/webserver/logs/test-namespace/app-1?tail=1000" in response.text
    assert "http://testserver/static/apps.css" in response.text
    list_apps.assert_awaited_once_with(response=mock.ANY, namespace="test-namespace", max_per_page=-1)


def test_log_page_renders_websocket_parameters(web_client):
    response = web_client.get("/webserver/logs/test-namespace/app-1?tail=25")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "const namespace = 'test-namespace';" in response.text
    assert "const appId = 'app-1';" in response.text
    assert "const tail = '25';" in response.text
    assert "http://testserver/static/app_logs.css" in response.text


@pytest.mark.parametrize("history_host", ["history:18080", "https://history.example"])
def test_history_apps_merge_without_losing_live_status_or_links(web_client, monkeypatch, history_host):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", history_host)
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_DEFAULT_NAMESPACE", "analytics")
    live_apps = [
        SparkApp(app_id="overlap", status=SparkAppStatus.Running, driver_logs=True, spark_ui_proxy=True),
        SparkApp(app_id="live-only", status=SparkAppStatus.Pending, driver_logs=True),
    ]
    history_client = mock.Mock()
    history_client.get = mock.AsyncMock(
        return_value=httpx.Response(200, json=[{"id": "overlap"}, {"id": "history-only"}])
    )
    with (
        mock.patch("spark_on_k8s.api.webserver.list_apps", return_value=live_apps),
        mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=history_client),
    ):
        response = web_client.get("/webserver/apps")
    assert response.status_code == 200
    rendered_apps = list(response.context["apps_list"])
    assert len(rendered_apps) == 3
    by_id = {app.app_id: app for app in rendered_apps}
    assert by_id["overlap"].status == SparkAppStatus.Running
    assert by_id["overlap"].driver_logs
    assert by_id["overlap"].spark_ui_proxy
    assert by_id["overlap"].spark_history_proxy
    assert by_id["live-only"].status == SparkAppStatus.Pending
    assert not by_id["live-only"].spark_history_proxy
    assert by_id["history-only"].status == SparkAppStatus.Unknown
    assert not by_id["history-only"].driver_logs
    assert not by_id["history-only"].spark_ui_proxy
    assert by_id["history-only"].spark_history_proxy
    assert response.text.count('href="/webserver/ui-history/history/overlap"') == 1
    assert 'href="/webserver/ui/analytics/overlap"' in response.text
    assert 'href="/webserver/logs/analytics/overlap?tail=1000"' in response.text
    assert 'href="/webserver/ui-history/history/history-only"' in response.text
    assert 'href="/webserver/logs/analytics/history-only' not in response.text
    expected_host = history_host if history_host.startswith("https://") else "http://" + history_host
    history_client.get.assert_awaited_once_with(expected_host + "/api/v1/applications?status=completed")
