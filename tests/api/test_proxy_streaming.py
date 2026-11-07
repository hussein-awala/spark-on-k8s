from __future__ import annotations

import asyncio
from unittest import mock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.webserver import router


class UpstreamBody(httpx.AsyncByteStream):
    def __init__(self):
        self.chunks = [b"first\x00chunk", b"second chunk"]
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            assert not self.closed
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize(
    "path, history_host, expected_url",
    [
        ("/webserver/ui/analytics/app-1/jobs/", None, "http://app-1.analytics.svc.cluster.local:4040/jobs/"),
        (
            "/webserver/ui-history/history/app-1/jobs/",
            "history:18080",
            "http://history:18080/history/app-1/jobs/",
        ),
        (
            "/webserver/ui-history/history/app-1/jobs/",
            "https://history.example:18443",
            "https://history.example:18443/history/app-1/jobs/",
        ),
    ],
)
def test_proxy_preserves_query_response_and_closes_upstream(monkeypatch, path, history_host, expected_url):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", history_host)
    body = UpstreamBody()
    requests = []

    async def upstream(request):
        requests.append(request)
        assert await request.aread() == b""
        return httpx.Response(
            206,
            stream=body,
            headers={
                "content-type": "application/octet-stream",
                "x-upstream": "spark",
                "cache-control": "no-cache",
            },
        )

    app = FastAPI()
    app.include_router(router)
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    try:
        with mock.patch(
            "spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=http_client
        ):
            with TestClient(app) as client:
                response = client.get(
                    path + "?stage=1&stage=2&description=a%2Bb", headers={"x-request-id": "trace-1"}
                )
        assert response.status_code == 206
        assert response.content == b"".join(body.chunks)
        assert response.headers["content-type"] == "application/octet-stream"
        assert response.headers["x-upstream"] == "spark"
        assert response.headers["cache-control"] == "no-cache"
        assert body.closed
        assert len(requests) == 1
        assert requests[0].method == "GET"
        assert str(requests[0].url) == expected_url + "?stage=1&stage=2&description=a%2Bb"
        assert requests[0].headers["x-request-id"] == "trace-1"
    finally:
        asyncio.run(http_client.aclose())


@pytest.mark.parametrize(
    "location, expected",
    [
        ("/history/app-1/", "/webserver/ui-history/history/app-1/"),
        ("/webserver/ui-history/history/app-1/", "/webserver/ui-history/history/app-1/"),
        (None, None),
    ],
)
def test_history_redirect_remains_under_the_proxy(monkeypatch, location, expected):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    body = UpstreamBody()
    headers = {"location": location} if location is not None else {}
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(302, headers=headers, stream=body))
    )
    app = FastAPI()
    app.include_router(router)
    try:
        with mock.patch(
            "spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=http_client
        ):
            response = TestClient(app).get("/webserver/ui-history/history/app-1", follow_redirects=False)
        assert response.status_code == 302
        assert response.headers.get("location") == expected
        assert response.content == b"".join(body.chunks)
        assert body.closed
    finally:
        asyncio.run(http_client.aclose())
