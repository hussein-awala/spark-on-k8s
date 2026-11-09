from __future__ import annotations

import asyncio
import json
import re
from unittest import mock

import httpx
import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes_asyncio import client as k8s
from spark_on_k8s.api import AsyncHttpClientSingleton, KubernetesClientSingleton, main, namespace_scope
from spark_on_k8s.api.app import router as app_router
from spark_on_k8s.api.apps import router as apps_router
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.console import router as console_router
from spark_on_k8s.api.namespace_scope import current_namespace as resolve_current_namespace
from spark_on_k8s.api.submission_policy import SubmissionPolicy, SubmissionSettings
from spark_on_k8s.api.webserver import current_dir, router as webserver_router
from spark_on_k8s.history.settings import HistorySettings
from spark_on_k8s.utils.configuration import Configuration
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocketDisconnect


def scoped(monkeypatch, namespaces, default=""):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", namespaces)
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_DEFAULT_NAMESPACE", default)


@pytest.fixture
def real_current_namespace(monkeypatch):
    monkeypatch.setattr(namespace_scope, "current_namespace", resolve_current_namespace)
    resolve_current_namespace.cache_clear()
    yield resolve_current_namespace
    resolve_current_namespace.cache_clear()


@pytest.mark.parametrize(
    "value, expected",
    [
        ("", ("default",)),
        ("  ", ("default",)),
        ("*", None),
        (" analytics , batch,analytics ", ("analytics", "batch")),
    ],
)
def test_scope_accepts_current_all_or_an_explicit_list(value, expected):
    assert namespace_scope.parse_namespaces(value) == expected


@pytest.mark.parametrize("value", ["analytics,*", "Analytics", "analytics,,batch", "a" * 64, "-batch"])
def test_scope_rejects_wildcards_in_lists_and_invalid_names(value):
    with pytest.raises(ValueError, match="SPARK_ON_K8S_API_NAMESPACES"):
        namespace_scope.parse_namespaces(value)


@pytest.mark.parametrize(
    "namespaces, default, expected_default",
    [
        ("", "", "default"),
        ("*", "", "default"),
        ("analytics,batch", "", "analytics"),
        ("analytics,batch", "batch", "batch"),
        ("*", "spark", "spark"),
    ],
)
def test_default_namespace_follows_the_scope(monkeypatch, namespaces, default, expected_default):
    scoped(monkeypatch, namespaces, default)
    assert namespace_scope.namespace_scope().default == expected_default


@pytest.mark.parametrize("namespaces", ["", "analytics,batch"])
def test_default_namespace_outside_the_scope_is_a_configuration_error(monkeypatch, namespaces):
    scoped(monkeypatch, namespaces, "kube-system")
    with pytest.raises(ValueError, match="SPARK_ON_K8S_API_DEFAULT_NAMESPACE"):
        namespace_scope.namespace_scope()


def test_current_namespace_in_cluster_is_the_pod_namespace(monkeypatch, tmp_path, real_current_namespace):
    path = tmp_path / "namespace"
    path.write_text("spark-control\n")
    monkeypatch.setattr(namespace_scope, "SERVICE_ACCOUNT_NAMESPACE", path)
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_IN_CLUSTER", True)
    assert real_current_namespace() == "spark-control"


def test_current_namespace_in_cluster_requires_a_mounted_service_account(
    monkeypatch, tmp_path, real_current_namespace
):
    monkeypatch.setattr(namespace_scope, "SERVICE_ACCOUNT_NAMESPACE", tmp_path / "missing")
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_IN_CLUSTER", True)
    with pytest.raises(ValueError, match="SPARK_ON_K8S_API_NAMESPACES"):
        real_current_namespace()


def test_current_namespace_in_cluster_rejects_an_invalid_mounted_namespace(
    monkeypatch, tmp_path, real_current_namespace
):
    path = tmp_path / "namespace"
    path.write_text("Not A Namespace\n")
    monkeypatch.setattr(namespace_scope, "SERVICE_ACCOUNT_NAMESPACE", path)
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_IN_CLUSTER", True)
    with pytest.raises(ValueError, match="not a valid namespace name"):
        real_current_namespace()


@pytest.mark.parametrize(
    "context, expected",
    [(None, "analytics"), ("batch-context", "batch"), ("no-namespace", "default"), ("unknown", "default")],
)
def test_current_namespace_outside_a_cluster_follows_the_kubeconfig_context(
    monkeypatch, tmp_path, real_current_namespace, context, expected
):
    kubeconfig = tmp_path / "config"
    cluster = {"name": "test", "cluster": {"server": "https://127.0.0.1:6443"}}
    contexts = [
        {"name": "analytics-context", "context": {"cluster": "test", "user": "u", "namespace": "analytics"}},
        {"name": "batch-context", "context": {"cluster": "test", "user": "u", "namespace": "batch"}},
        {"name": "no-namespace", "context": {"cluster": "test", "user": "u"}},
    ]
    kubeconfig.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [cluster],
                "users": [{"name": "u", "user": {}}],
                "contexts": contexts,
                "current-context": "analytics-context",
            }
        )
    )
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_IN_CLUSTER", False)
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_CONFIG_FILE", str(kubeconfig))
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_CONTEXT", context)
    assert real_current_namespace() == expected


def test_current_namespace_without_a_kubeconfig_is_default(monkeypatch, tmp_path, real_current_namespace):
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_IN_CLUSTER", False)
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_CONFIG_FILE", str(tmp_path / "missing"))
    assert real_current_namespace() == "default"


@pytest.fixture
def client():
    app = FastAPI()
    for router in (app_router, apps_router, console_router, webserver_router):
        app.include_router(router)
    app.mount("/static", StaticFiles(directory=current_dir / "static"), name="static")
    return TestClient(app)


@pytest.mark.parametrize(
    "method, path",
    [
        ("patch", "/app/kube-system/app-1"),
        ("delete", "/app/kube-system/app-1"),
        ("get", "/apps/list_apps/kube-system"),
        ("get", "/webserver/api/apps?namespace=kube-system"),
        ("get", "/webserver/api/apps/kube-system/pods/driver"),
        ("get", "/webserver/api/apps/kube-system/pods/driver/diagnostics"),
        ("get", "/webserver/ui/kube-system/kube-dns/"),
        ("get", "/webserver/ui/kube-system/app-svc/api/v1/applications"),
        ("get", "/webserver/apps?namespace=kube-system"),
        ("get", "/webserver/logs/kube-system/app-1"),
    ],
)
def test_out_of_scope_namespaces_are_refused_before_any_cluster_access(monkeypatch, client, method, path):
    scoped(monkeypatch, "analytics,batch")
    kubernetes = mock.AsyncMock(side_effect=AssertionError("Kubernetes was contacted"))
    http = mock.Mock(side_effect=AssertionError("A Spark UI was contacted"))
    monkeypatch.setattr(KubernetesClientSingleton, "client", kubernetes)
    monkeypatch.setattr(AsyncHttpClientSingleton, "client", http)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager:
        response = getattr(client, method)(path)
    assert response.status_code == 403, response.text
    assert response.json()["detail"]["code"] == "namespace_forbidden"
    manager.assert_not_called()
    kubernetes.assert_not_called()
    http.assert_not_called()


def test_spark_ui_proxy_still_reaches_namespaces_in_scope(monkeypatch, client):
    scoped(monkeypatch, "analytics")
    upstream = mock.AsyncMock()
    upstream.send.return_value = mock.Mock(
        status_code=200, headers=httpx.Headers({"content-type": "text/plain"})
    )
    upstream.send.return_value.aiter_raw = lambda: _chunks(b"ok")
    upstream.send.return_value.aclose = mock.AsyncMock()
    upstream.build_request = mock.Mock(return_value="request")
    monkeypatch.setattr(AsyncHttpClientSingleton, "client", mock.Mock(return_value=upstream))
    response = client.get("/webserver/ui/analytics/app-1-ui-svc/jobs/")
    assert response.status_code == 200
    assert str(upstream.build_request.call_args.kwargs["url"]).startswith(
        "http://app-1-ui-svc.analytics.svc.cluster.local:4040/jobs/"
    )


async def _chunks(*chunks):
    for chunk in chunks:
        yield chunk


def test_log_stream_outside_the_scope_closes_without_reading_logs(monkeypatch, client):
    scoped(monkeypatch, "analytics")
    with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager:
        with pytest.raises(WebSocketDisconnect) as error:
            with client.websocket_connect("/webserver/ws/logs/kube-system/app-1"):
                pytest.fail("An out-of-scope log stream was accepted")
    assert error.value.code == 1008
    manager.assert_not_called()


def test_default_listing_uses_the_scope_default(monkeypatch, client):
    scoped(monkeypatch, "analytics,batch")
    api = mock.Mock()
    api.list_namespaced_pod = mock.AsyncMock(return_value=k8s.V1PodList(items=[], metadata=k8s.V1ListMeta()))
    monkeypatch.setattr(KubernetesClientSingleton, "client", mock.AsyncMock())
    with (
        mock.patch("spark_on_k8s.api.apps.CoreV1Api", return_value=api),
        mock.patch("spark_on_k8s.api.console.k8s.CoreV1Api", return_value=api),
    ):
        assert client.get("/apps/list_apps").status_code == 200
        assert client.get("/webserver/api/apps").status_code == 200
        assert client.get("/webserver/api/apps?namespace=batch").status_code == 200
    namespaces = [call.kwargs["namespace"] for call in api.list_namespaced_pod.await_args_list]
    assert namespaces == ["analytics", "analytics", "batch"]


def _console_config(html):
    return json.loads(re.search(r'id="spark-console-config"[^>]*>(.*?)</script>', html, re.S).group(1))


@pytest.mark.parametrize(
    "namespaces, picker, editable",
    [("*", None, True), ("analytics,batch", ["analytics", "batch"], True), ("", ["default"], False)],
)
def test_console_receives_the_namespace_choices(monkeypatch, client, namespaces, picker, editable):
    scoped(monkeypatch, namespaces)
    page = client.get("/webserver/apps")
    assert page.status_code == 200
    config = _console_config(page.text)
    assert config["namespaces"] == picker
    assert config["namespaceEditable"] is editable


def _run(coroutine):
    return asyncio.run(coroutine)


@pytest.mark.parametrize(
    "submission, history, message",
    [
        (
            SubmissionSettings(
                True,
                b"t" * 32,
                SubmissionPolicy.from_mapping(
                    {
                        "schema_version": 1,
                        "namespaces": {"analytics": ["spark"], "kube-system": ["spark"]},
                        "applications": [{"image": "spark:4", "app_path": "local:///app.py"}],
                        "limits": {
                            "driver_cpu": 1,
                            "driver_memory_mb": 1024,
                            "executor_cpu": 1,
                            "executor_memory_mb": 1024,
                            "executors": 1,
                        },
                        "spark_conf": {},
                    }
                ),
            ),
            HistorySettings(),
            "The submission policy names namespaces outside SPARK_ON_K8S_API_NAMESPACES: kube-system",
        ),
        (
            SubmissionSettings(),
            HistorySettings(enabled=True, namespaces=("analytics", "batch")),
            "SPARK_ON_K8S_RUN_HISTORY_NAMESPACES names namespaces outside SPARK_ON_K8S_API_NAMESPACES: batch",
        ),
    ],
)
def test_startup_rejects_settings_that_name_namespaces_outside_the_scope(
    monkeypatch, submission, history, message
):
    scoped(monkeypatch, "analytics")
    monkeypatch.setattr(main, "get_submission_settings", lambda: submission)
    monkeypatch.setattr(main, "get_history_settings", lambda: history)
    kubernetes = mock.AsyncMock()
    monkeypatch.setattr(KubernetesClientSingleton, "client", kubernetes)
    with pytest.raises(ValueError, match=re.escape(message)):
        _run(main.on_start_up())
    kubernetes.assert_not_called()


def test_startup_rejects_an_invalid_scope(monkeypatch):
    scoped(monkeypatch, "analytics,*")
    kubernetes = mock.AsyncMock()
    monkeypatch.setattr(KubernetesClientSingleton, "client", kubernetes)
    with pytest.raises(ValueError, match="SPARK_ON_K8S_API_NAMESPACES"):
        _run(main.on_start_up())
    kubernetes.assert_not_called()
