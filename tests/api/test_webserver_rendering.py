from __future__ import annotations

import json
import re
from importlib.metadata import version
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.webserver import current_dir, router
from starlette.staticfiles import StaticFiles


def make_client(root_path=""):
    app = FastAPI(root_path=root_path)
    app.include_router(router)
    app.mount("/static", StaticFiles(directory=current_dir / "static"), name="static")
    return TestClient(app)


def bootstrap(response):
    return json.loads(
        re.search(
            r'<script id="spark-console-config" type="application/json">(.*?)</script>',
            response.text,
            re.DOTALL,
        ).group(1)
    )


def test_app_console_bootstraps_without_waiting_for_kubernetes(monkeypatch):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON", False)
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL", 15)
    with mock.patch("spark_on_k8s.api.KubernetesClientSingleton.client") as kubernetes:
        response = make_client().get("/webserver/apps?namespace=analytics")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"
    config = bootstrap(response)
    assert config["namespace"] == "analytics"
    assert config["version"] == version("spark-on-k8s")
    assert config["view"] == "apps"
    assert config["refreshInterval"] == 15
    assert config["historyEnabled"] is True
    assert config["allowKill"] is False
    assert 'src="/static/ui/console.js"' in response.text
    assert 'href="/static/ui/console.css"' in response.text
    kubernetes.assert_not_called()


@pytest.mark.parametrize(
    "value,expected",
    [(None, True), ("true", True), ("True", True), ("TRUE", True), ("false", False), ("False", False)],
)
def test_console_action_settings_parse_booleans_like_other_true_false_settings(monkeypatch, value, expected):
    import runpy

    from spark_on_k8s.api import configuration

    names = ("SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON", "SPARK_ON_K8S_API_WEBSERVER_APPS_DELETE_BUTTON")
    for name in names:
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    settings = runpy.run_path(configuration.__file__)["APIConfiguration"]
    assert [getattr(settings, name) for name in names] == [expected, expected]


def test_log_page_bootstraps_target_and_tail():
    response = make_client().get("/webserver/logs/analytics/app-1?tail=25")
    assert response.status_code == 200
    config = bootstrap(response)
    assert (config["namespace"], config["appId"], config["tail"], config["view"]) == (
        "analytics",
        "app-1",
        25,
        "logs",
    )


@pytest.mark.parametrize("tail", ["invalid", "-2", "10001"])
def test_log_page_rejects_invalid_tail(tail):
    assert make_client().get("/webserver/logs/analytics/app-1", params={"tail": tail}).status_code == 422


def test_console_respects_deployment_prefix_and_default_namespace(monkeypatch):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_DEFAULT_NAMESPACE", "default-spark")
    response = make_client("/platform/spark").get("/webserver/apps")
    assert response.status_code == 200
    assert bootstrap(response)["basePath"] == "/platform/spark"
    assert bootstrap(response)["namespace"] == "default-spark"
    assert 'src="/platform/spark/static/ui/console.js"' in response.text


@pytest.mark.parametrize("root_path", ["", "/platform/spark"])
def test_console_assets_ignore_request_host_and_scheme(root_path):
    # An absolute URL built from Host would let a spoofed or cached Host choose the script
    # origin, and a TLS-terminating gateway would turn assets into blocked mixed content.
    response = make_client(root_path).get(
        "/webserver/apps", headers={"Host": "attacker.example", "X-Forwarded-Host": "attacker.example"}
    )
    assert response.status_code == 200
    assert "attacker.example" not in response.text
    assert f'src="{root_path}/static/ui/console.js"' in response.text
    assert f'href="{root_path}/static/ui/console.css"' in response.text


def test_untrusted_namespace_cannot_escape_json_script():
    payload = "</script><script>window.compromised=true</script><img src=x onerror=alert(1)>"
    response = make_client().get("/webserver/apps", params={"namespace": payload})
    assert response.status_code == 200
    assert bootstrap(response)["namespace"] == payload
    assert payload not in response.text
    assert response.text.count("<script") == 2
