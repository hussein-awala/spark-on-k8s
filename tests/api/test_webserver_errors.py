from __future__ import annotations

import asyncio
from unittest import mock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes_asyncio.client import ApiException
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.webserver import router
from starlette.responses import Response


@pytest.mark.parametrize("status, expected_status", [(403, 403), (404, 404), (500, 500), (None, 500)])
def test_webserver_preserves_kubernetes_error_status(status, expected_status):
    app = FastAPI()
    app.include_router(router)
    app.add_route("/static/{path:path}", lambda request: Response(), name="static")
    error = ApiException(status=status, reason="Kubernetes request failed")
    with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client") as http_client:
        http_client.return_value.send = mock.AsyncMock(side_effect=error)
        response = TestClient(app).get("/webserver/ui/analytics/app-1")

    assert response.status_code == expected_status
    assert response.headers["content-type"].startswith("text/html")
    assert "Kubernetes request failed" in response.text


@pytest.mark.parametrize(
    "error, expected_status",
    [
        (httpx.ConnectError, 502),
        (httpx.RemoteProtocolError, 502),
        (httpx.ConnectTimeout, 504),
        (httpx.ReadTimeout, 504),
    ],
)
@pytest.mark.parametrize(
    "path", ["/webserver/ui/analytics/app-1/jobs/", "/webserver/ui-history/history/app-1/"]
)
def test_unreachable_upstream_is_a_gateway_error_without_upstream_details(
    monkeypatch, caplog, error, expected_status, path
):
    # A finished driver's Service has no endpoints; history may be restarting.
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")

    def upstream(request):
        raise error("private upstream resolver details", request=request)

    app = FastAPI()
    app.include_router(router)
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    try:
        with mock.patch(
            "spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=http_client
        ):
            response = TestClient(app, raise_server_exceptions=False).get(path)
    finally:
        asyncio.run(http_client.aclose())
    assert response.status_code == expected_status
    assert "private upstream resolver details" not in response.text
    assert "private upstream resolver details" not in caplog.text
