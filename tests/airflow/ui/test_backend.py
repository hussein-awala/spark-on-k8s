from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from importlib.metadata import PackageNotFoundError, version
from types import SimpleNamespace
from unittest import mock
from urllib.parse import urlencode

import pytest

try:
    AIRFLOW_UI_SUPPORTED = tuple(int(x) for x in version("apache-airflow").split(".")[:2]) >= (3, 4)
except PackageNotFoundError:
    AIRFLOW_UI_SUPPORTED = False

pytestmark = pytest.mark.skipif(not AIRFLOW_UI_SUPPORTED, reason="Native task UI requires Airflow 3.4")

QUERY = {
    "dag_id": "dag/with spaces",
    "run_id": "manual__2026-11-07T00:00:00+00:00",
    "task_id": "spark.task",
    "map_index": 4,
    "try_number": 2,
}
APP_ID = "a" * 63
POD = "a" * 56 + "-driver"
APPLICATION = {
    "app_id": APP_ID,
    "name": "Spark example",
    "namespace": "spark",
    "status": "Running",
    "driver_pod": POD,
    "driver_logs": True,
    "spark_ui_proxy": True,
    "source": "kubernetes",
    "task_context": dict(QUERY),
}


@pytest.fixture
def harness(monkeypatch):
    import httpx
    from fastapi.testclient import TestClient
    from spark_on_k8s.airflow.ui import backend
    from spark_on_k8s.airflow.ui.task_binding import TaskBinding
    from spark_on_k8s.airflow.ui_config import UISettings

    @contextmanager
    def make(*, writes=False, authenticated=True, root_path="", public_url=None, snapshot_ttl=5.0):
        requests = []
        responses = {}
        binding = TaskBinding(
            version=1,
            namespace="spark",
            driver_pod=POD,
            app_id=APP_ID,
            try_number=2,
            task_context=dict(QUERY),
        )
        loaded = mock.Mock(return_value=binding)
        authorized = mock.AsyncMock()
        monkeypatch.setattr(backend, "load_task_binding", loaded)
        monkeypatch.setattr(backend, "authorize", authorized)
        monkeypatch.setattr(backend, "create_dag_bag", lambda: object())

        def response(request):
            requests.append(request)
            if request.method in {"PATCH", "DELETE"}:
                return httpx.Response(200)
            configured = responses.get(request.url.path)
            if configured:
                if isinstance(configured, Exception):
                    raise configured
                if callable(configured):
                    return configured(request)
                return configured
            if request.url.path.endswith("/capabilities"):
                return httpx.Response(200, json={"history_configured": True})
            if request.url.path.endswith("/history"):
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {
                                **APPLICATION,
                                "namespace": None,
                                "driver_pod": None,
                                "driver_logs": False,
                                "spark_ui_proxy": False,
                                "spark_history_proxy": True,
                                "source": "history",
                            },
                            {**APPLICATION, "app_id": "other-app"},
                        ]
                    },
                )
            return httpx.Response(200, json={**APPLICATION, "raw_secret": "must not be exposed"})

        original_client = httpx.AsyncClient
        monkeypatch.setattr(
            backend.httpx,
            "AsyncClient",
            lambda **kwargs: original_client(**kwargs, transport=httpx.MockTransport(response)),
        )
        app = backend.create_ui_app(
            UISettings(
                service_url="https://spark.example/service",
                allow_writes=writes,
                public_url=public_url,
                snapshot_ttl=snapshot_ttl,
            )
        )
        if authenticated:
            app.dependency_overrides[backend.get_user] = lambda: mock.sentinel.user
        with TestClient(app, base_url="https://airflow.example", root_path=root_path) as client:
            yield SimpleNamespace(
                client=client,
                app=app,
                requests=requests,
                responses=responses,
                binding=binding,
                loaded=loaded,
                authorized=authorized,
                backend=backend,
            )
        assert app.state.client.is_closed

    return make


def test_application_is_task_bound_and_does_not_forward_browser_credentials(harness):
    with harness(root_path="/airflow") as h:
        response = h.client.get(
            "/api/application",
            params={**QUERY, "namespace": "other", "app_id": "other"},
            headers={"Authorization": "Bearer browser-secret", "Cookie": "session=secret"},
        )
        assert response.status_code == 200
        assert response.json()["app_id"] == APP_ID
        assert "raw_secret" not in response.json()
        reference = h.loaded.call_args.args[0]
        assert reference.dag_id == QUERY["dag_id"]
        assert reference.run_id == QUERY["run_id"]
        assert reference.map_index == 4
        assert reference.try_number == 2
        request = h.requests[0]
        assert str(request.url) == f"https://spark.example/service/webserver/api/apps/spark/pods/{POD}"
        assert "authorization" not in request.headers
        assert "cookie" not in request.headers


def test_anonymous_request_never_reads_task_or_upstream(harness):
    with harness(authenticated=False) as h:
        response = h.client.get("/api/application", params=QUERY)
        assert response.status_code in {401, 403}
        h.loaded.assert_not_called()
        assert not h.requests


def test_dag_authorization_precedes_binding_lookup(harness):
    from fastapi import HTTPException

    with harness() as h:
        h.authorized.side_effect = HTTPException(403, "Forbidden")
        assert h.client.get("/api/application", params=QUERY).status_code == 403
        h.loaded.assert_not_called()
        assert not h.requests


@pytest.mark.parametrize(
    "service_url", ["https://different.example", "file:///etc/passwd", "https://spark.example/other"]
)
def test_task_cannot_override_admin_upstream(harness, service_url):
    with harness() as h:
        h.binding.service_url = service_url
        assert h.client.get("/api/application", params=QUERY).status_code == 409
        assert not h.requests


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Origin": "https://evil.example", "X-Spark-On-K8s-Action": "true"},
        {"Origin": "https://airflow.example"},
    ],
)
def test_writes_require_same_origin_and_action_header(harness, headers):
    with harness(writes=True) as h:
        assert h.client.post("/api/kill", params=QUERY, headers=headers).status_code == 403
        assert not h.requests


@pytest.mark.parametrize(
    "method,path,upstream_method,permission",
    [("post", "/api/kill", "PATCH", "PUT"), ("delete", "/api/application", "DELETE", "DELETE")],
)
def test_authorized_writes_resolve_actual_id_and_use_separate_permissions(
    harness, method, path, upstream_method, permission
):
    with harness(writes=True) as h:
        response = getattr(h.client, method)(
            path,
            params={**QUERY, "app_id": "unrelated"},
            headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"},
        )
        assert response.status_code == 204
        assert h.authorized.call_args.args[-1] == permission
        assert h.requests[-1].method == upstream_method
        assert str(h.requests[-1].url) == f"https://spark.example/service/app/spark/{APP_ID}"


def test_writes_disabled_by_default_and_capabilities_match(harness):
    with harness() as h:
        response = h.client.post(
            "/api/kill",
            params=QUERY,
            headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"},
        )
        assert response.status_code == 403
        assert not h.requests
        capabilities = h.client.get("/api/capabilities", params=QUERY).json()
        assert capabilities["can_kill"] is False
        assert capabilities["can_delete"] is False
        assert capabilities["spark_ui_url"] == f"https://spark.example/service/webserver/ui/spark/{APP_ID}"


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize("deleted", [False, True])
def test_history_link_requires_explicit_service_configuration(harness, configured, deleted):
    import httpx

    with harness() as h:
        h.responses["/service/webserver/api/capabilities"] = httpx.Response(
            200, json={"history_configured": configured}
        )
        if deleted:
            h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(404)
        response = h.client.get("/api/capabilities", params=QUERY)
        assert response.status_code == 200
        assert response.json()["spark_history_url"] == (
            f"https://spark.example/service/webserver/ui-history/history/{APP_ID}" if configured else None
        )
        assert bool(response.json()["spark_ui_url"]) is not deleted
        assert not any(request.url.path.endswith("/history") for request in h.requests)


@pytest.mark.parametrize("deleted", [False, True])
def test_older_service_does_not_guess_history_from_driver_logs(harness, deleted):
    import httpx

    with harness() as h:
        h.responses["/service/webserver/api/capabilities"] = httpx.Response(404)
        if deleted:
            h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(404)
        response = h.client.get("/api/capabilities", params=QUERY)
        assert response.status_code == 200
        assert response.json()["spark_history_url"] is None
        assert not any(request.url.path.endswith("/history") for request in h.requests)


@pytest.mark.parametrize("upstream_status,expected_status", [(403, 403), (503, 502)])
def test_failed_capability_lookup_is_not_reported_as_disabled(harness, upstream_status, expected_status):
    import httpx

    with harness() as h:
        h.responses["/service/webserver/api/capabilities"] = httpx.Response(
            upstream_status, text="private service details"
        )
        response = h.client.get("/api/capabilities", params=QUERY)
        assert response.status_code == expected_status
        assert response.json()["detail"]["code"] == "service_error"
        assert "spark_history_url" not in response.json()
        assert "private" not in response.text
        assert not any(request.url.path.endswith("/history") for request in h.requests)


@pytest.mark.parametrize("body", [{"history_configured": "false"}, {}, ["private detail"]])
def test_invalid_capability_response_is_sanitized(harness, body):
    import httpx

    with harness() as h:
        h.responses["/service/webserver/api/capabilities"] = httpx.Response(200, json=body)
        response = h.client.get("/api/capabilities", params=QUERY)
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "invalid_response"
        assert "private" not in response.text


def test_configured_but_unavailable_history_keeps_its_error(harness):
    import httpx

    with harness() as h:
        h.responses["/service/webserver/api/history"] = httpx.Response(502, text="private host")
        capabilities = h.client.get("/api/capabilities", params=QUERY)
        assert capabilities.status_code == 200
        assert capabilities.json()["spark_history_url"]
        history = h.client.get("/api/history", params=QUERY)
        assert history.status_code == 502
        assert history.json()["detail"]["code"] == "service_error"
        assert "private" not in history.text


def test_permission_denial_disables_only_the_corresponding_action(harness):
    from fastapi import HTTPException

    with harness(writes=True) as h:

        async def authorize(*args):
            if args[-1] == "DELETE":
                raise HTTPException(403, "Forbidden")

        h.authorized.side_effect = authorize
        capabilities = h.client.get("/api/capabilities", params=QUERY).json()
        assert capabilities["can_kill"] is True
        assert capabilities["can_delete"] is False


def test_deleted_driver_keeps_external_history_link_without_proxying_history(harness):
    import httpx

    with harness(writes=True) as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(404)
        application = h.client.get("/api/application", params=QUERY)
        assert application.status_code == 200
        assert application.json()["app_id"] == APP_ID
        assert application.json()["source"] == "history"
        assert application.json()["driver_logs"] is False
        capabilities = h.client.get("/api/capabilities", params=QUERY).json()
        assert not capabilities["can_kill"] and not capabilities["can_delete"]
        assert capabilities["spark_ui_url"] is None
        assert (
            capabilities["spark_history_url"]
            == f"https://spark.example/service/webserver/ui-history/history/{APP_ID}"
        )
        history = h.client.get("/api/history", params=QUERY).json()
        assert history["items"] == []
        assert not any(request.url.path.endswith("/history") for request in h.requests)
        response = h.client.post(
            "/api/kill",
            params=QUERY,
            headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"},
        )
        assert response.status_code == 409
        assert not any(request.method == "PATCH" for request in h.requests)


def test_reused_driver_name_cannot_target_another_application(harness):
    import httpx

    with harness(writes=True) as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            200, json={**APPLICATION, "app_id": "replacement"}
        )
        response = h.client.post(
            "/api/kill",
            params=QUERY,
            headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"},
        )
        assert response.status_code == 409
        assert len(h.requests) == 1


@pytest.mark.parametrize("status", [302, 500])
def test_upstream_errors_are_sanitized_and_redirects_are_not_followed(harness, status):
    import httpx

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            status, text="sensitive traceback", headers={"Location": "https://evil.example"}
        )
        response = h.client.get("/api/application", params=QUERY)
        assert response.status_code == 502
        assert "sensitive" not in response.text
        assert len(h.requests) == 1


def test_websocket_authenticates_cookie_and_closes_idle_upstream_on_disconnect(harness, monkeypatch):
    from airflow.api_fastapi.core_api import security
    from spark_on_k8s.airflow.ui import backend

    closed, cancelled = threading.Event(), threading.Event()
    connected = mock.Mock()

    class Source:
        async def __aiter__(self):
            yield "driver log line"
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    @asynccontextmanager
    async def connect(url, **kwargs):
        connected(url, **kwargs)
        try:
            yield Source()
        finally:
            closed.set()

    monkeypatch.setattr(backend, "connect_upstream_logs", connect)
    resolve = mock.AsyncMock(return_value=mock.sentinel.user)
    monkeypatch.setattr(security, "resolve_user_from_token", resolve)
    with harness() as h:
        with h.client.websocket_connect(
            "wss://airflow.example/api/logs?" + urlencode({**QUERY, "tail": 25}),
            headers={
                "Origin": "https://airflow.example",
                "Cookie": f"{security.COOKIE_NAME_JWT_TOKEN}=test-token",
            },
        ) as socket:
            assert socket.receive_text() == "driver log line"
            socket.close()
            assert closed.wait(1) and cancelled.wait(1)
        resolve.assert_awaited_once_with("test-token")
        assert h.authorized.call_args.args[3] == security.DagAccessEntity.TASK_LOGS
        assert connected.call_args.args == (
            f"wss://spark.example/service/webserver/ws/logs/spark/{APP_ID}?tail=25",
        )
        assert connected.call_args.kwargs == {}


def test_cross_origin_websocket_is_rejected_before_authentication(harness, monkeypatch):
    from starlette.websockets import WebSocketDisconnect

    with harness() as h:
        get_user = mock.AsyncMock()
        monkeypatch.setattr(h.backend, "get_user", get_user)
        with pytest.raises(WebSocketDisconnect) as rejected:
            with h.client.websocket_connect(
                "wss://airflow.example/api/logs?" + urlencode(QUERY),
                headers={"Origin": "https://evil.example"},
            ):
                pass
        assert rejected.value.code == 1008
        get_user.assert_not_awaited()
        assert not h.requests


def test_websocket_redirect_is_not_followed():
    from spark_on_k8s.airflow.ui.backend import connect_upstream_logs
    from websockets.datastructures import Headers
    from websockets.exceptions import InvalidStatus
    from websockets.http11 import Response

    redirect = InvalidStatus(Response(302, "Found", Headers({"Location": "wss://other.example/logs"})))
    connection = mock.Mock()
    connection.handshake = mock.AsyncMock(side_effect=redirect)
    connector = connect_upstream_logs("wss://spark.example/logs")
    with mock.patch.object(
        connector, "open_tcp_connection", new=mock.AsyncMock(return_value=connection)
    ) as opened:
        with pytest.raises(InvalidStatus):
            asyncio.run(connector.connect())
    opened.assert_awaited_once()
    connection.transport.abort.assert_called_once()
    assert connector.uri == "wss://spark.example/logs"


@pytest.mark.parametrize(
    "owner",
    [
        None,
        {**QUERY, "dag_id": "another-dag"},
        {**QUERY, "run_id": "another-run"},
        {**QUERY, "task_id": "another-task"},
        {**QUERY, "map_index": 5},
        {**QUERY, "region_id": "00000000-0000-0000-0000-000000000001"},
    ],
)
def test_tampered_binding_cannot_authorize_other_task_driver(harness, owner):
    import httpx

    with harness(writes=True) as h:
        # Even matching XCom app IDs cannot confer ownership over a different task's pod.
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            200, json={**APPLICATION, "task_context": owner}
        )
        for path in ("/api/application", "/api/capabilities", "/api/history"):
            response = h.client.get(path, params=QUERY)
            assert response.status_code == 409
            assert response.json() == {
                "detail": {
                    "code": "task_ownership_mismatch",
                    "message": "This driver is not bound to the selected Airflow task.",
                }
            }
            assert "Spark example" not in response.text
            assert "task_context" not in response.text
        response = h.client.post(
            "/api/kill",
            params=QUERY,
            headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"},
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "task_ownership_mismatch"
        assert not any(
            request.method == "PATCH" or request.url.path.endswith("/history") for request in h.requests
        )


def test_same_task_can_adopt_previous_attempt_driver(harness):
    import httpx

    with harness(writes=True) as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            200, json={**APPLICATION, "task_context": {**QUERY, "try_number": 1}}
        )
        response = h.client.post(
            "/api/kill",
            params=QUERY,
            headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"},
        )
        assert response.status_code == 204


def test_history_is_filtered_only_after_live_task_ownership_is_verified(harness):
    with harness() as h:
        history = h.client.get("/api/history", params=QUERY).json()
        assert [item["app_id"] for item in history["items"]] == [APP_ID]
        assert h.requests[0].url.path.endswith(POD)
        assert h.requests[1].url.path.endswith("/history")


def test_public_url_is_separate_from_internal_proxy_target(harness):
    with harness(public_url="https://public-spark.example/base") as h:
        h.binding.service_url = "https://public-spark.example/base/"
        response = h.client.get("/api/capabilities", params=QUERY)
        assert response.status_code == 200
        assert (
            response.json()["spark_ui_url"]
            == f"https://public-spark.example/base/webserver/ui/spark/{APP_ID}"
        )
        assert all(request.url.host == "spark.example" for request in h.requests)


def test_websocket_refuses_unowned_driver_before_upstream_connection(harness, monkeypatch):
    import httpx
    from airflow.api_fastapi.core_api import security
    from fastapi import WebSocketDisconnect

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            200, json={**APPLICATION, "task_context": None}
        )
        monkeypatch.setattr(
            security, "resolve_user_from_token", mock.AsyncMock(return_value=mock.sentinel.user)
        )
        connect = mock.Mock()
        monkeypatch.setattr(h.backend, "connect_upstream_logs", connect)
        with pytest.raises(WebSocketDisconnect):
            with h.client.websocket_connect(
                "wss://airflow.example/api/logs?" + urlencode(QUERY),
                headers={
                    "Origin": "https://airflow.example",
                    "Cookie": f"{security.COOKIE_NAME_JWT_TOKEN}=jwt",
                },
            ):
                pytest.fail("Unowned driver must be rejected before accepting the WebSocket")
        connect.assert_not_called()


def test_authorization_uses_airflow_dag_permission_helper():
    from spark_on_k8s.airflow.ui import backend
    from spark_on_k8s.airflow.ui.task_binding import TaskReference

    check = mock.Mock()
    with mock.patch.object(backend, "requires_access_dag", return_value=check) as factory:
        asyncio.run(
            backend.authorize(
                mock.sentinel.request,
                mock.sentinel.user,
                TaskReference(**QUERY),
                backend.DagAccessEntity.TASK_LOGS,
            )
        )
    factory.assert_called_once_with(
        method="GET", access_entity=backend.DagAccessEntity.TASK_LOGS, param_dag_id=QUERY["dag_id"]
    )
    check.assert_called_once_with(mock.sentinel.request, mock.sentinel.user)


def test_registered_bundle_and_mount_preserve_airflow_base_path(monkeypatch):
    import importlib
    from urllib.parse import urljoin

    monkeypatch.setenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", "https://spark.example")
    plugin = importlib.reload(importlib.import_module("spark_on_k8s.airflow.ui.plugin")).SparkTaskUIPlugin
    assert plugin.fastapi_apps[0]["url_prefix"] == "/spark-on-k8s"
    assert plugin.react_apps[0]["destination"] == "task_instance"
    assert plugin.react_apps[0]["applies_to"]["operator"] == ["SparkOnK8SOperator", "SparkSqlOnK8SOperator"]
    assert urljoin("https://airflow.example/airflow/", plugin.react_apps[0]["bundle_url"]) == (
        "https://airflow.example/airflow/spark-on-k8s/static/airflow.js"
    )
    routes = {route.path for route in plugin.fastapi_apps[0]["app"].routes}
    assert {"/static/airflow.js", "/static/console.css"} <= routes


def test_listed_operator_subclasses_show_the_tab(monkeypatch):
    import importlib

    from fastapi.testclient import TestClient

    monkeypatch.setenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", "https://spark.example")
    monkeypatch.setenv("SPARK_ON_K8S_AIRFLOW_OPERATOR_SUBCLASSES", "TeamSparkOperator,TeamSqlOperator")
    monkeypatch.setenv("SPARK_ON_K8S_AIRFLOW_SNAPSHOT_TTL", "2.5")
    plugin = importlib.reload(importlib.import_module("spark_on_k8s.airflow.ui.plugin")).SparkTaskUIPlugin
    assert plugin.react_apps[0]["applies_to"] == {
        "operator": ["SparkOnK8SOperator", "SparkSqlOnK8SOperator", "TeamSparkOperator", "TeamSqlOperator"]
    }
    app = plugin.fastapi_apps[0]["app"]
    with TestClient(app):
        assert app.state.snapshots.ttl == 2.5


@pytest.mark.parametrize("payload", [None, {"source": "invalid"}, {**APPLICATION, "namespace": "other"}])
def test_invalid_application_response_is_not_exposed(harness, payload):
    import httpx

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(200, json=payload)
        response = h.client.get("/api/application", params=QUERY)
        assert response.status_code == 502
        assert "Spark example" not in response.text
        assert "task_context" not in response.text


def test_unreachable_service_has_safe_retryable_error(harness):
    import httpx

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.ConnectError("private address")
        response = h.client.get("/api/application", params=QUERY)
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "service_unavailable"
        assert "private" not in response.text


def test_invalid_history_response_is_sanitized(harness):
    import httpx

    with harness() as h:
        h.responses["/service/webserver/api/history"] = httpx.Response(200, json={"items": ["private"]})
        response = h.client.get("/api/history", params=QUERY)
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "invalid_response"
        assert "private" not in response.text


def test_unverified_app_id_cannot_be_used_in_upstream_path(harness):
    import httpx

    with harness() as h:
        h.binding.app_id = None
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            200, json={**APPLICATION, "app_id": "../other"}
        )
        response = h.client.get("/api/application", params=QUERY)
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "invalid_application_id"
        assert len(h.requests) == 1


def test_owned_driver_without_id_cannot_open_ui_or_accept_actions(harness):
    import httpx

    with harness(writes=True) as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            200, json={**APPLICATION, "driver_logs": False}
        )
        assert h.client.get("/api/application", params=QUERY).json()["spark_ui_proxy"] is False
        response = h.client.post(
            "/api/kill",
            params=QUERY,
            headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"},
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "application_id_unavailable"
        assert not any(request.method == "PATCH" for request in h.requests)


def test_permission_service_failure_does_not_grant_action_capabilities(harness):
    from fastapi import HTTPException

    with harness(writes=True) as h:

        async def authorize(*args):
            if args[-1] == "PUT":
                raise HTTPException(503, "Authorization unavailable")

        h.authorized.side_effect = authorize
        response = h.client.get("/api/capabilities", params=QUERY)
        assert response.status_code == 503
        assert "can_kill" not in response.json()


@pytest.mark.parametrize("name,media_type", [("airflow.js", "text/javascript"), ("console.css", "text/css")])
def test_packaged_assets_are_served_without_private_data(harness, name, media_type):
    with harness(authenticated=False) as h:
        response = h.client.get(f"/static/{name}")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith(media_type)
        assert response.headers["cache-control"] == "no-cache"
        assert response.content
        assert not h.requests


def test_missing_packaged_asset_has_explicit_error(harness, monkeypatch):
    from pathlib import Path

    with harness() as h:
        monkeypatch.setattr(Path, "is_file", lambda self: False)
        response = h.client.get("/static/airflow.js")
        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "bundle_not_installed"


@pytest.mark.parametrize("broken,close_code", [(False, 1000), (True, 1011)])
def test_log_source_completion_and_failure_close_browser_stream(harness, monkeypatch, broken, close_code):
    from airflow.api_fastapi.core_api import security
    from spark_on_k8s.airflow.ui import backend
    from starlette.websockets import WebSocketDisconnect

    closed = threading.Event()

    class Source:
        async def __aiter__(self):
            yield b"not text"
            if broken:
                raise RuntimeError("private service trace")
            yield "final line"

    @asynccontextmanager
    async def connect(url):
        try:
            yield Source()
        finally:
            closed.set()

    monkeypatch.setattr(backend, "connect_upstream_logs", connect)
    monkeypatch.setattr(security, "resolve_user_from_token", mock.AsyncMock(return_value=mock.sentinel.user))
    with harness() as h:
        with h.client.websocket_connect(
            "wss://airflow.example/api/logs?" + urlencode(QUERY),
            headers={"Origin": "https://airflow.example", "Cookie": f"{security.COOKIE_NAME_JWT_TOKEN}=jwt"},
        ) as socket:
            if not broken:
                assert socket.receive_text() == "final line"
            with pytest.raises(WebSocketDisconnect) as ended:
                socket.receive_text()
            assert ended.value.code == close_code
            assert "private" not in str(ended.value)
        assert closed.wait(1)


@pytest.mark.parametrize("upstream_code,close_code", [(1008, 1008), (1011, 1011), (1001, 1011)])
def test_upstream_log_unavailability_reaches_the_browser_stream(
    harness, monkeypatch, upstream_code, close_code
):
    from airflow.api_fastapi.core_api import security
    from spark_on_k8s.airflow.ui import backend
    from starlette.websockets import WebSocketDisconnect
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close

    # The Spark API closes with 1008 when the driver's logs are unavailable (for example,
    # after deletion). The shared console only says "unavailable" for that code; a 1011
    # tells the user to reconnect, which cannot succeed.
    class Source:
        async def __aiter__(self):
            yield "last line"
            raise ConnectionClosedError(Close(upstream_code, "private upstream reason"), None)

    @asynccontextmanager
    async def connect(url):
        yield Source()

    monkeypatch.setattr(backend, "connect_upstream_logs", connect)
    monkeypatch.setattr(security, "resolve_user_from_token", mock.AsyncMock(return_value=mock.sentinel.user))
    with harness() as h:
        with h.client.websocket_connect(
            "wss://airflow.example/api/logs?" + urlencode(QUERY),
            headers={"Origin": "https://airflow.example", "Cookie": f"{security.COOKIE_NAME_JWT_TOKEN}=jwt"},
        ) as socket:
            assert socket.receive_text() == "last line"
            with pytest.raises(WebSocketDisconnect) as ended:
                socket.receive_text()
            assert ended.value.code == close_code
            assert "private" not in str(ended.value.reason)


@pytest.mark.parametrize("mutation,status", [(None, 200), ("ownership", 409), ("identity", 409)])
def test_diagnostics_requires_current_task_ownership(harness, mutation, status):
    import httpx

    payload = {
        "application": {**APPLICATION},
        "diagnosis": {
            "namespace": "spark",
            "app_id": APP_ID,
            "driver_pod": POD,
            "findings": [],
            "inspected_pods": 1,
            "inspected_events": 0,
            "incomplete": False,
        },
    }
    if mutation == "ownership":
        payload["application"]["task_context"] = {**QUERY, "map_index": 99}
    elif mutation == "identity":
        payload["diagnosis"]["app_id"] = "another-application"
    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}/diagnostics"] = httpx.Response(
            200, json=payload
        )
        response = h.client.get("/api/diagnostics", params={**QUERY, "namespace": "foreign"})
        assert response.status_code == status
        h.authorized.assert_awaited_once()
        assert h.requests[0].url.path == f"/service/webserver/api/apps/spark/pods/{POD}/diagnostics"
        assert "namespace" not in h.requests[0].url.params


def test_anonymous_diagnostics_never_resolves_task_or_calls_service(harness):
    with harness(authenticated=False) as h:
        assert h.client.get("/api/diagnostics", params=QUERY).status_code in {401, 403}
        h.loaded.assert_not_called()
        assert h.requests == []


def test_missing_pod_diagnostics_never_falls_back_to_history(harness):
    import httpx

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}/diagnostics"] = httpx.Response(404)
        assert h.client.get("/api/diagnostics", params=QUERY).status_code == 404
        assert len(h.requests) == 1


def test_invalid_upstream_diagnostics_is_not_exposed_to_task(harness):
    import httpx

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}/diagnostics"] = httpx.Response(
            200, json={"application": APPLICATION, "diagnosis": {"private": "internal trace"}}
        )
        response = h.client.get("/api/diagnostics", params=QUERY)
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "invalid_response"
        assert "internal trace" not in response.text


def test_native_log_connection_preserves_large_lines_and_following_messages():
    from spark_on_k8s.airflow.ui.backend import connect_upstream_logs
    from websockets.asyncio.server import serve
    from websockets.exceptions import ConnectionClosedOK

    # A valid driver line larger than the WebSocket client's 1 MiB default
    # previously interrupted the native tab and discarded subsequent output.
    large_line = "START " + "x" * (1200 * 1024) + " END\n"

    async def run():
        async def upstream(connection):
            await connection.send(large_line)
            await connection.send("job completed\n")

        async with serve(upstream, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect_upstream_logs(f"ws://127.0.0.1:{port}/logs") as connection:
                assert await connection.recv() == large_line
                assert await connection.recv() == "job completed\n"
                with pytest.raises(ConnectionClosedOK):
                    await connection.recv()

    asyncio.run(run())


def test_native_log_connection_keeps_a_bounded_message_limit():
    from spark_on_k8s.airflow.ui.backend import connect_upstream_logs
    from websockets.asyncio.server import serve
    from websockets.exceptions import ConnectionClosed, ConnectionClosedError

    async def run():
        async def upstream(connection):
            try:
                await connection.send("x" * (16 * 1024 * 1024 + 1))
                await connection.wait_closed()
            except ConnectionClosed:
                pass

        async with serve(upstream, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect_upstream_logs(f"ws://127.0.0.1:{port}/logs") as connection:
                with pytest.raises(ConnectionClosedError) as rejected:
                    await connection.recv()
                assert rejected.value.sent.code == 1009

    asyncio.run(run())


def driver_lookups(h) -> int:
    return sum(request.url.path == f"/service/webserver/api/apps/spark/pods/{POD}" for request in h.requests)


def test_one_poll_shares_a_single_driver_lookup_across_endpoints(harness):
    with harness() as h:
        for path in ("/api/application", "/api/capabilities", "/api/history"):
            assert h.client.get(path, params=QUERY).status_code == 200
        assert driver_lookups(h) == 1
        # Each request still resolved its own task binding after its own authorization.
        assert h.authorized.await_count == 3
        assert h.loaded.call_count == 3


def test_concurrent_viewers_share_one_driver_lookup(harness):
    import time
    from concurrent.futures import ThreadPoolExecutor

    import httpx

    released = threading.Event()

    async def slow_driver(request):
        await asyncio.to_thread(released.wait, 5)
        return httpx.Response(200, json=APPLICATION)

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = slow_driver
        snapshots = h.app.state.snapshots
        shared, joined = snapshots.get, []

        async def counted(key, load):
            joined.append(key)
            return await shared(key, load)

        snapshots.get = counted
        paths = ["/api/application", "/api/application", "/api/capabilities", "/api/history"]
        with ThreadPoolExecutor(len(paths)) as pool:
            pending = [pool.submit(h.client.get, path, params=QUERY) for path in paths]
            deadline = time.monotonic() + 5
            while len(joined) < len(paths) and time.monotonic() < deadline:
                time.sleep(0.01)
            # Every viewer joined while the first driver lookup was still in flight.
            assert len(joined) == len(paths)
            released.set()
            responses = [future.result(timeout=5) for future in pending]
        assert [response.status_code for response in responses] == [200] * len(paths)
        assert len(set(joined)) == 1
        assert driver_lookups(h) == 1


def test_shared_snapshot_never_bypasses_per_request_authorization(harness):
    from fastapi import HTTPException

    with harness() as h:
        assert h.client.get("/api/application", params=QUERY).status_code == 200
        h.authorized.side_effect = HTTPException(403, "Forbidden")
        h.loaded.reset_mock()
        for path in ("/api/application", "/api/capabilities", "/api/history"):
            response = h.client.get(path, params=QUERY)
            assert response.status_code == 403
            assert APP_ID not in response.text
        h.loaded.assert_not_called()
        assert driver_lookups(h) == 1


@pytest.mark.parametrize(
    "change", [{"try_number": 3}, {"task_id": "other.task"}, {"map_index": 5}, {"run_id": "other-run"}]
)
def test_task_attempts_never_share_snapshots(harness, change):
    import httpx

    with harness() as h:
        assert h.client.get("/api/application", params=QUERY).status_code == 200
        owner = {**QUERY, **change}
        h.loaded.return_value = h.binding.model_copy(
            update={
                "try_number": owner["try_number"],
                "task_context": h.binding.task_context.model_copy(update=change),
            }
        )
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(
            200, json={**APPLICATION, "task_context": owner}
        )
        response = h.client.get("/api/application", params={**QUERY, **change})
        assert response.status_code == 200
        assert driver_lookups(h) == 2


@pytest.mark.parametrize("method,path", [("post", "/api/kill"), ("delete", "/api/application")])
def test_actions_verify_current_driver_and_refresh_the_shared_snapshot(harness, method, path):
    with harness(writes=True) as h:
        assert h.client.get("/api/application", params=QUERY).status_code == 200
        response = getattr(h.client, method)(
            path, params=QUERY, headers={"Origin": "https://airflow.example", "X-Spark-On-K8s-Action": "true"}
        )
        assert response.status_code == 204
        assert driver_lookups(h) == 2
        assert h.client.get("/api/application", params=QUERY).status_code == 200
        assert driver_lookups(h) == 3


def test_zero_snapshot_ttl_reads_the_driver_for_every_request(harness):
    with harness(snapshot_ttl=0) as h:
        for path in ("/api/application", "/api/capabilities", "/api/history"):
            assert h.client.get(path, params=QUERY).status_code == 200
        assert driver_lookups(h) == 3


def test_failed_driver_lookup_is_retried_by_the_next_request(harness):
    import httpx

    with harness() as h:
        h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"] = httpx.Response(503)
        assert h.client.get("/api/application", params=QUERY).status_code == 502
        del h.responses[f"/service/webserver/api/apps/spark/pods/{POD}"]
        assert h.client.get("/api/application", params=QUERY).status_code == 200
        assert driver_lookups(h) == 2
