from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, call

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes_asyncio import client as k8s
from spark_on_k8s.api import console
from spark_on_k8s.api.configuration import APIConfiguration


def driver_pod(name="report-driver", app_id="report-123", phase="Running"):
    labels = {"spark-role": "driver", "spark-app-name": "daily-report", "spark-version": "3.5.9"}
    if app_id is not None:
        labels["spark-app-id"] = app_id
    labels["spark-ui-proxy"] = "true"
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name,
            namespace="analytics",
            labels=labels,
            creation_timestamp=datetime(2026, 11, 7, 12, 30, tzinfo=UTC),
            annotations={"private-setting": "do-not-expose"},
        ),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="driver",
                    image="registry.example/report:v1",
                    env=[k8s.V1EnvVar(name="TOKEN", value="do-not-expose")],
                )
            ],
            node_name="worker-1",
        ),
        status=k8s.V1PodStatus(phase=phase),
    )


@pytest.fixture
def console_client(monkeypatch):
    app = FastAPI()
    app.include_router(console.router)
    api = Mock()
    api.list_namespaced_pod = AsyncMock()
    api.read_namespaced_pod = AsyncMock()
    acquire = AsyncMock()
    monkeypatch.setattr(console.KubernetesClientSingleton, "client", acquire)
    monkeypatch.setattr(console.k8s, "CoreV1Api", Mock(return_value=api))
    with TestClient(app) as client:
        yield client, api, acquire


def test_live_pages_preserve_metadata_and_opaque_continuation(console_client):
    client, api, _ = console_client
    token = "opaque/token+="
    api.list_namespaced_pod.side_effect = [
        k8s.V1PodList(items=[driver_pod()], metadata=k8s.V1ListMeta(_continue=token)),
        k8s.V1PodList(items=[], metadata=k8s.V1ListMeta()),
    ]

    first = client.get("/webserver/api/apps", params={"namespace": "analytics", "limit": 1})
    second = client.get(
        "/webserver/api/apps",
        params={"namespace": "analytics", "limit": 1, "continue_from": first.json()["next_cursor"]},
    )

    assert first.status_code == second.status_code == 200
    assert first.json() == {
        "items": [
            {
                "app_id": "report-123",
                "name": "daily-report",
                "namespace": "analytics",
                "status": "Running",
                "created_at": "2026-11-07T12:30:00Z",
                "driver_pod": "report-driver",
                "image": "registry.example/report:v1",
                "spark_version": "3.5.9",
                "node_name": "worker-1",
                "driver_logs": True,
                "spark_ui_proxy": True,
                "spark_history_proxy": False,
                "source": "kubernetes",
            }
        ],
        "next_cursor": token,
    }
    assert "do-not-expose" not in first.text
    assert second.json() == {"items": [], "next_cursor": None}
    assert api.list_namespaced_pod.await_args_list == [
        call(namespace="analytics", label_selector="spark-role=driver", limit=1),
        call(namespace="analytics", label_selector="spark-role=driver", limit=1, _continue=token),
    ]


def test_live_listing_uses_configured_namespace_and_default_limit(console_client, monkeypatch):
    client, api, _ = console_client
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_DEFAULT_NAMESPACE", "configured")
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[], metadata=k8s.V1ListMeta())

    assert client.get("/webserver/api/apps").json() == {"items": [], "next_cursor": None}
    api.list_namespaced_pod.assert_awaited_once_with(
        namespace="configured", label_selector="spark-role=driver", limit=50
    )


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": -1},
        {"limit": 201},
        {"limit": "abc"},
        {"namespace": ""},
        {"namespace": "Invalid"},
        {"namespace": "a" * 64},
    ],
)
def test_live_listing_rejects_unbounded_or_invalid_requests_before_access(console_client, params):
    client, _, acquire = console_client
    assert client.get("/webserver/api/apps", params=params).status_code == 422
    acquire.assert_not_awaited()


def test_terminal_cursor_does_not_contact_kubernetes(console_client):
    client, _, acquire = console_client
    response = client.get("/webserver/api/apps", params={"continue_from": "no-more-items"})
    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}
    acquire.assert_not_awaited()


def test_sparse_driver_metadata_does_not_invent_versions_or_working_links(console_client):
    client, api, _ = console_client
    pod = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name="legacy-driver", labels={"spark-role": "driver", "spark-ui-proxy": "unexpected"}
        )
    )
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[pod], metadata=k8s.V1ListMeta())

    response = client.get("/webserver/api/apps", params={"namespace": "analytics", "limit": 200})
    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "app_id": "legacy-driver",
            "name": "legacy-driver",
            "namespace": "analytics",
            "status": "Unknown",
            "created_at": None,
            "driver_pod": "legacy-driver",
            "image": None,
            "spark_version": None,
            "node_name": None,
            "driver_logs": False,
            "spark_ui_proxy": False,
            "spark_history_proxy": False,
            "source": "kubernetes",
        }
    ]


@pytest.mark.parametrize(
    "route,method",
    [
        ("/webserver/api/apps?namespace=analytics", "list_namespaced_pod"),
        ("/webserver/api/apps/analytics/pods/report-driver", "read_namespaced_pod"),
    ],
)
@pytest.mark.parametrize(
    "status,expected", [(403, 403), (404, 404), (410, 410), (429, 429), (500, 502), (None, 502)]
)
def test_kubernetes_failures_return_safe_structured_errors(console_client, route, method, status, expected):
    client, api, _ = console_client
    getattr(api, method).side_effect = k8s.ApiException(status=status, reason="private upstream detail")

    response = client.get(route)

    assert response.status_code == expected
    assert response.json()["detail"]["code"] == "kubernetes_error"
    assert response.json()["detail"]["message"]
    assert "private upstream detail" not in response.text


@pytest.mark.parametrize("route", ["/webserver/api/apps", "/webserver/api/apps/analytics/pods/report-driver"])
def test_client_configuration_failure_has_a_stable_error(console_client, route):
    client, _, acquire = console_client
    acquire.side_effect = RuntimeError("private kubeconfig location")
    response = client.get(route)
    assert response.status_code == 502
    assert response.json() == {
        "detail": {"code": "kubernetes_unavailable", "message": "Kubernetes is unavailable."}
    }


def test_driver_lookup_preserves_actual_long_application_id(console_client):
    client, api, _ = console_client
    app_id = "a" * 63
    pod_name = app_id[:56] + "-driver"
    api.read_namespaced_pod.return_value = driver_pod(pod_name, app_id)

    response = client.get(f"/webserver/api/apps/analytics/pods/{pod_name}")

    assert response.status_code == 200
    assert response.json()["app_id"] == app_id
    assert response.json()["driver_pod"] == pod_name
    assert response.json()["source"] == "kubernetes"
    api.read_namespaced_pod.assert_awaited_once_with(namespace="analytics", name=pod_name)
    api.list_namespaced_pod.assert_not_awaited()


@pytest.mark.parametrize("labels", [{}, {"spark-role": "executor"}])
def test_driver_lookup_rejects_non_driver_pods(console_client, labels):
    client, api, _ = console_client
    pod = driver_pod()
    pod.metadata.labels = labels
    api.read_namespaced_pod.return_value = pod
    response = client.get("/webserver/api/apps/analytics/pods/report-driver")
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "application_not_found"


def test_driver_lookup_without_application_label_only_returns_display_metadata(console_client):
    client, api, _ = console_client
    api.read_namespaced_pod.return_value = driver_pod(app_id=None)
    response = client.get("/webserver/api/apps/analytics/pods/report-driver")
    assert response.status_code == 200
    assert response.json()["app_id"] == "report-driver"
    assert response.json()["driver_logs"] is False
    assert response.json()["spark_ui_proxy"] is False


def history_response(payload, status=200):
    return httpx.Response(
        status, json=payload, request=httpx.Request("GET", "http://history/api/v1/applications")
    )


@pytest.mark.parametrize(
    "host,expected_url",
    [
        ("history:18080", "http://history:18080/api/v1/applications"),
        ("https://history.example/prefix/", "https://history.example/prefix/api/v1/applications"),
    ],
)
def test_history_metadata_retains_real_id_without_inventing_namespace_or_success(
    console_client, monkeypatch, host, expected_url
):
    client, _, acquire = console_client
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", host)
    http = Mock(
        get=AsyncMock(
            return_value=history_response(
                [
                    {
                        "id": "application-real-id",
                        "name": "Report from history",
                        "attempts": [
                            {
                                "startTime": "2026-11-07T12:30:00.000GMT",
                                "completed": True,
                                "appSparkVersion": "3.5.9",
                            }
                        ],
                    },
                    {"id": "minimal-history-record"},
                ]
            )
        )
    )
    monkeypatch.setattr(console.AsyncHttpClientSingleton, "client", Mock(return_value=http))

    response = client.get("/webserver/api/history")

    assert response.status_code == 200
    first, second = response.json()["items"]
    assert first == {
        "app_id": "application-real-id",
        "name": "Report from history",
        "namespace": None,
        "status": "Unknown",
        "created_at": "2026-11-07T12:30:00Z",
        "driver_pod": None,
        "image": None,
        "spark_version": "3.5.9",
        "node_name": None,
        "driver_logs": False,
        "spark_ui_proxy": False,
        "spark_history_proxy": True,
        "source": "history",
    }
    assert second["name"] == second["app_id"] == "minimal-history-record"
    assert second["created_at"] is None
    http.get.assert_awaited_once_with(expected_url, params={"status": "completed"})
    acquire.assert_not_awaited()


@pytest.mark.parametrize(
    "attempt,expected",
    [
        ({"startTimeEpoch": 0}, "1970-01-01T00:00:00Z"),
        ({"startTime": "2026-11-07T12:30:00+02:00"}, "2026-11-07T12:30:00+02:00"),
        ({"startTime": "not a date"}, None),
        ({"startTimeEpoch": 10**30}, None),
    ],
)
def test_history_start_time_is_iso_or_null(console_client, monkeypatch, attempt, expected):
    client, _, _ = console_client
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    http = Mock(get=AsyncMock(return_value=history_response([{"id": "app-1", "attempts": [attempt]}])))
    monkeypatch.setattr(console.AsyncHttpClientSingleton, "client", Mock(return_value=http))
    response = client.get("/webserver/api/history")
    assert response.status_code == 200
    assert response.json()["items"][0]["created_at"] == expected


@pytest.mark.parametrize("host", [None, ""])
def test_unconfigured_history_is_explicit_and_does_not_open_a_client(console_client, monkeypatch, host):
    client, _, _ = console_client
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", host)
    acquire = Mock()
    monkeypatch.setattr(console.AsyncHttpClientSingleton, "client", acquire)
    response = client.get("/webserver/api/history")
    assert response.status_code == 412
    assert response.json()["detail"]["code"] == "history_not_configured"
    acquire.assert_not_called()


@pytest.mark.parametrize(
    "upstream",
    [
        history_response([], 503),
        history_response({"error": "private server detail"}),
        history_response([{"name": "missing ID"}]),
        httpx.Response(200, text="not JSON", request=httpx.Request("GET", "http://history")),
        httpx.ConnectError("private connection detail"),
        httpx.ReadTimeout("private timeout detail"),
        httpx.InvalidURL("private malformed history URL"),
    ],
)
def test_history_failures_are_independent_of_live_listing(console_client, monkeypatch, upstream):
    client, api, _ = console_client
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    get = (
        AsyncMock(side_effect=upstream)
        if isinstance(upstream, Exception)
        else AsyncMock(return_value=upstream)
    )
    monkeypatch.setattr(console.AsyncHttpClientSingleton, "client", Mock(return_value=Mock(get=get)))
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[driver_pod()], metadata=k8s.V1ListMeta())

    history = client.get("/webserver/api/history")
    live = client.get("/webserver/api/apps?namespace=analytics")

    assert history.status_code == 502
    assert history.json() == {
        "detail": {"code": "history_unavailable", "message": "Spark History Server is unavailable."}
    }
    assert "private" not in history.text
    assert live.status_code == 200
    assert live.json()["items"][0]["app_id"] == "report-123"
