from __future__ import annotations

from unittest import mock

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
