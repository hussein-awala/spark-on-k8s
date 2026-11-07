from __future__ import annotations

import asyncio
from unittest import mock

import httpx
import pytest
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.testclient import TestClient
from kubernetes_asyncio.client import ApiException
from spark_on_k8s.api import KubernetesClientSingleton
from spark_on_k8s.api.webserver import app_logs_websocket, router


@pytest.mark.parametrize("query, tail", [("", -1), ("?tail=25", 25)])
@pytest.mark.parametrize("lines", [[], ["first line", "<literal log entry>", "last line"]])
def test_websocket_sends_ordered_logs_and_closes(query, tail, lines):
    finished = []

    async def logs():
        try:
            for line in lines:
                yield line
        finally:
            finished.append(True)

    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.logs_streamer.return_value = logs()
        with TestClient(app) as client:
            with client.websocket_connect("/webserver/ws/logs/analytics/app-1" + query) as websocket:
                assert [websocket.receive_text() for _ in lines] == lines
                close = websocket.receive()
        manager_class.assert_called_once_with(k8s_client_manager=KubernetesClientSingleton.client_manager)
        manager_class.return_value.logs_streamer.assert_called_once_with(
            namespace="analytics", app_id="app-1", tail_lines=tail
        )
    assert close["type"] == "websocket.close"
    assert close["code"] == 1000
    assert finished == [True]


@pytest.mark.parametrize(
    "error,code",
    [
        (ApiException(status=403), 1008),
        (ApiException(status=404), 1008),
        (ApiException(status=500), 1011),
        (ValueError("missing"), 1008),
        (RuntimeError("private upstream information"), 1011),
    ],
)
def test_websocket_reports_stream_failure_without_rendering_it_as_logs(error, code):
    closed = []

    async def logs():
        try:
            yield "first line"
            raise error
        finally:
            closed.append(True)

    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager:
        manager.return_value.logs_streamer.return_value = logs()
        with TestClient(app) as client, client.websocket_connect("/webserver/ws/logs/spark/app") as websocket:
            assert websocket.receive_text() == "first line"
            event = websocket.receive()
    assert event["type"] == "websocket.close"
    assert event["code"] == code
    assert "private upstream information" not in event.get("reason", "")
    assert closed == [True]


def test_disconnected_browser_closes_log_generator():
    closed = []

    async def logs():
        try:
            yield "line"
        finally:
            closed.append(True)

    async def wait_for_disconnect():
        await asyncio.Event().wait()

    websocket = mock.Mock(
        headers=mock.Mock(getlist=mock.Mock(return_value=[])),
        accept=mock.AsyncMock(),
        receive=mock.AsyncMock(side_effect=wait_for_disconnect),
        send_text=mock.AsyncMock(side_effect=WebSocketDisconnect()),
        close=mock.AsyncMock(),
    )
    with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager:
        manager.return_value.logs_streamer.return_value = logs()
        asyncio.run(app_logs_websocket(websocket, "spark", "app", tail=100))
    assert closed == [True]
    websocket.close.assert_not_awaited()


@pytest.mark.parametrize("termination", ["client_disconnect", "cancel_handler"])
def test_idle_websocket_cleans_up_without_waiting_for_another_log_line(termination):
    async def check_cleanup():
        incoming = asyncio.Queue()
        await incoming.put({"type": "websocket.connect"})
        reading = asyncio.Event()
        cleaned = asyncio.Event()

        async def logs():
            try:
                reading.set()
                await asyncio.Event().wait()
                yield "unreachable"
            finally:
                cleaned.set()

        async def receive():
            return await incoming.get()

        sent = []

        async def send(message):
            sent.append(message)

        app = FastAPI()
        app.include_router(router)
        scope = {
            "type": "websocket",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "scheme": "ws",
            "path": "/webserver/ws/logs/spark/app",
            "raw_path": b"/webserver/ws/logs/spark/app",
            "root_path": "",
            "query_string": b"tail=10",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
            "subprotocols": [],
            "state": {},
        }
        with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager:
            manager.return_value.logs_streamer.return_value = logs()
            task = asyncio.create_task(app(scope, receive, send))
            try:
                await asyncio.wait_for(reading.wait(), timeout=5)
                assert sent[0]["type"] == "websocket.accept"
                if termination == "client_disconnect":
                    await incoming.put({"type": "websocket.disconnect", "code": 1000})
                    await asyncio.wait_for(task, timeout=5)
                    assert incoming.empty()
                else:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(task, timeout=5)
                assert cleaned.is_set()
                assert asyncio.all_tasks() == {asyncio.current_task()}
                manager.assert_called_once_with(k8s_client_manager=KubernetesClientSingleton.client_manager)
                manager.return_value.logs_streamer.assert_called_once_with(
                    namespace="spark", app_id="app", tail_lines=10
                )
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check_cleanup())


@pytest.mark.parametrize("remote_mode", [True, False])
@pytest.mark.parametrize("root_path", ["", "/platform/spark"])
@pytest.mark.parametrize(
    "origins,configured,allowed",
    [
        ([], "", True),
        (["http://testserver"], "", True),
        (["http://TESTSERVER:80"], "", True),
        (["https://testserver"], "", False),
        (["https://foreign.example"], "", False),
        (["null"], "", False),
        (["http://testserver", "http://testserver"], "", False),
        (["http://testserver/"], "", False),
        (["http://user@testserver"], "", False),
        (["http://testserver?"], "", False),
        (["http://testserver#"], "", False),
        (["https://console.example"], "https://console.example", True),
        (["https://console.example:443"], "https://console.example", True),
        (["http://testserver"], "https://console.example", False),
        (["https://console.example"], "https://console.example/path", False),
        (["https://console.example"], "https://console.example,invalid", False),
        ([], "https://console.example", True),
    ],
)
def test_log_websocket_origin_contract(monkeypatch, root_path, origins, configured, allowed, remote_mode):
    from spark_on_k8s.api import remote_auth
    from spark_on_k8s.api.configuration import APIConfiguration
    from spark_on_k8s.api.remote_auth import RemoteAccessMiddleware
    from spark_on_k8s.api.submission_policy import SubmissionSettings

    token = "dummy-operator-bearer-with-more-than-32-characters"
    monkeypatch.setattr(
        remote_auth, "get_submission_settings", lambda: SubmissionSettings(remote_mode, token.encode())
    )
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS", configured)
    app = FastAPI(root_path=root_path)
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(router)

    async def logs():
        yield "permitted"

    # Spoofed proxy headers must not broaden the direct-origin default.
    headers = [("origin", origin) for origin in origins] + [
        ("authorization", "Bearer " + token),
        ("x-forwarded-host", "foreign.example"),
        ("x-forwarded-proto", "https"),
        ("forwarded", "host=foreign.example;proto=https"),
    ]
    with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager:
        manager.return_value.logs_streamer.return_value = logs()
        with TestClient(app) as client:
            if allowed:
                with client.websocket_connect(
                    root_path + "/webserver/ws/logs/spark/app", headers=httpx.Headers(headers)
                ) as ws:
                    assert ws.receive_text() == "permitted"
            else:
                with pytest.raises(WebSocketDisconnect) as error:
                    with client.websocket_connect(
                        root_path + "/webserver/ws/logs/spark/app", headers=httpx.Headers(headers)
                    ):
                        pytest.fail("Foreign browser origin was accepted")
                assert error.value.code == 1008
                manager.assert_not_called()
        # Avoid an unconsumed test generator in rejected cases.
        asyncio.run(manager.return_value.logs_streamer.return_value.aclose())


@pytest.mark.parametrize("origin,allowed", [("https://testserver", True), ("http://testserver", False)])
def test_secure_direct_websocket_uses_https_origin(monkeypatch, origin, allowed):
    from spark_on_k8s.api.configuration import APIConfiguration

    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS", "")
    app = FastAPI()
    app.include_router(router)

    async def logs():
        yield "secure"

    with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager:
        manager.return_value.logs_streamer.return_value = logs()
        with TestClient(app) as client:
            if allowed:
                with client.websocket_connect(
                    "wss://testserver/webserver/ws/logs/spark/app", headers={"origin": origin}
                ) as ws:
                    assert ws.receive_text() == "secure"
            else:
                with pytest.raises(WebSocketDisconnect):
                    with client.websocket_connect(
                        "wss://testserver/webserver/ws/logs/spark/app", headers={"origin": origin}
                    ):
                        pytest.fail("HTTP origin accepted for secure direct endpoint")
                manager.assert_not_called()
        asyncio.run(manager.return_value.logs_streamer.return_value.aclose())
