from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes_asyncio import client as k8s
from spark_on_k8s.api.apps import router
from spark_on_k8s.api.configuration import APIConfiguration


def pod(name, phase, labels):
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name=name, labels=labels),
        status=k8s.V1PodStatus(phase=phase),
    )


@pytest.mark.parametrize("path", ["/apps/list_apps", "/apps/list_apps/analytics"])
def test_pagination_preserves_app_status_and_continuation(monkeypatch, path):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_DEFAULT_NAMESPACE", "analytics")
    pages = [
        k8s.V1PodList(
            metadata=k8s.V1ListMeta(_continue="page-token"),
            items=[
                pod("driver-1", "Running", {"spark-app-id": "app-1", "spark-ui-proxy": "true"}),
                pod("driver-2", "Pending", {"spark-app-id": "app-2"}),
            ],
        ),
        k8s.V1PodList(
            metadata=k8s.V1ListMeta(),
            items=[pod("driver-without-app-label", "Failed", {})],
        ),
    ]
    app = FastAPI()
    app.include_router(router)
    with (
        mock.patch("spark_on_k8s.api.apps.KubernetesClientSingleton.client", new_callable=mock.AsyncMock),
        mock.patch("spark_on_k8s.api.apps.CoreV1Api") as api_class,
        TestClient(app) as client,
    ):
        api = api_class.return_value
        api.list_namespaced_pod = mock.AsyncMock(side_effect=pages)
        first = client.get(path, params={"max_per_page": 2})
        second = client.get(path, params={"max_per_page": 2, "continue_from": first.headers["X-Continue"]})
        terminal = client.get(path, params={"continue_from": second.headers["X-Continue"]})

    assert first.status_code == second.status_code == terminal.status_code == 200
    assert first.json() == [
        {
            "app_id": "app-1",
            "status": "Running",
            "driver_logs": True,
            "spark_ui_proxy": True,
            "spark_history_proxy": False,
        },
        {
            "app_id": "app-2",
            "status": "Pending",
            "driver_logs": True,
            "spark_ui_proxy": False,
            "spark_history_proxy": False,
        },
    ]
    assert second.json() == [
        {
            "app_id": "driver-without-app-label",
            "status": "Failed",
            "driver_logs": True,
            "spark_ui_proxy": False,
            "spark_history_proxy": False,
        },
    ]
    assert second.headers["X-Continue"] == terminal.headers["X-Continue"] == "no-more-items"
    assert terminal.json() == []
    assert api.list_namespaced_pod.await_args_list == [
        mock.call(namespace="analytics", label_selector="spark-role=driver", limit=2),
        mock.call(namespace="analytics", label_selector="spark-role=driver", limit=2, _continue="page-token"),
    ]


@pytest.mark.parametrize("max_per_page", [0, -1])
def test_unlimited_app_listing_omits_kubernetes_limit(max_per_page):
    app = FastAPI()
    app.include_router(router)
    with (
        mock.patch("spark_on_k8s.api.apps.KubernetesClientSingleton.client", new_callable=mock.AsyncMock),
        mock.patch("spark_on_k8s.api.apps.CoreV1Api") as api_class,
        TestClient(app) as client,
    ):
        api_class.return_value.list_namespaced_pod = mock.AsyncMock(
            return_value=k8s.V1PodList(items=[], metadata=k8s.V1ListMeta())
        )
        response = client.get("/apps/list_apps/analytics", params={"max_per_page": max_per_page})
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Continue"] == "no-more-items"
    api_class.return_value.list_namespaced_pod.assert_awaited_once_with(
        namespace="analytics", label_selector="spark-role=driver"
    )


def test_terminal_page_does_not_acquire_a_kubernetes_client():
    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.apps.KubernetesClientSingleton.client") as acquire:
        response = TestClient(app).get("/apps/list_apps/analytics?continue_from=no-more-items")
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers["X-Continue"] == "no-more-items"
    acquire.assert_not_called()
