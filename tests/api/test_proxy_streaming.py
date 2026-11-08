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
@pytest.mark.parametrize("root_path", ["", "/platform/spark"])
def test_proxy_preserves_query_response_and_closes_upstream(
    monkeypatch, path, history_host, expected_url, root_path
):
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

    app = FastAPI(root_path=root_path)
    app.include_router(router)
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    try:
        with mock.patch(
            "spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=http_client
        ):
            with TestClient(app) as client:
                response = client.get(
                    root_path + path + "?stage=1&stage=2&description=a%2Bb",
                    headers={"x-request-id": "trace-1"},
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
        assert "x-request-id" not in requests[0].headers
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
@pytest.mark.parametrize("root_path", ["", "/platform/spark"])
def test_history_redirect_remains_under_the_proxy(monkeypatch, location, expected, root_path):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    body = UpstreamBody()
    headers = {"location": location} if location is not None else {}
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(302, headers=headers, stream=body))
    )
    app = FastAPI(root_path=root_path)
    app.include_router(router)
    try:
        with mock.patch(
            "spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=http_client
        ):
            response = TestClient(app).get(
                root_path + "/webserver/ui-history/history/app-1", follow_redirects=False
            )
        assert response.status_code == 302
        assert response.headers.get("location") == (root_path + expected if expected is not None else None)
        assert response.content == b"".join(body.chunks)
        assert body.closed
    finally:
        asyncio.run(http_client.aclose())


@pytest.mark.parametrize(
    "path",
    [
        "/webserver/ui/analytics/app%40example.invalid%3A80%23/jobs/",
        "/webserver/ui/analytics%40example.invalid%3A80%23/app/jobs/",
        "/webserver/ui/Analytics/app/jobs/",
        "/webserver/ui/analytics/app_name/jobs/",
        "/webserver/ui/analytics/-app/jobs/",
        "/webserver/ui/analytics/app-/jobs/",
        "/webserver/ui/" + "a" * 64 + "/app/jobs/",
        "/webserver/ui/analytics/" + "a" * 64 + "/jobs/",
    ],
)
def test_proxy_rejects_invalid_kubernetes_targets_before_opening_http_client(path):
    requests = []

    async def upstream(request):
        requests.append(request)
        return httpx.Response(200, stream=UpstreamBody())

    app = FastAPI()
    app.include_router(router)
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    try:
        with mock.patch(
            "spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=http_client
        ) as acquire:
            response = TestClient(app).get(path)
        assert response.status_code == 400
        acquire.assert_not_called()
        assert requests == []
    finally:
        asyncio.run(http_client.aclose())


@pytest.mark.parametrize("suffix", ["/history/app-1/", "?show=all"])
def test_history_redirect_with_deployment_prefix_is_idempotent(monkeypatch, suffix):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    prefix = "/platform/spark"
    location = prefix + "/webserver/ui-history" + suffix
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(302, headers={"location": location}, stream=UpstreamBody())
        )
    )
    app = FastAPI(root_path=prefix)
    app.include_router(router)
    try:
        with mock.patch(
            "spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=http_client
        ):
            response = TestClient(app).get(
                prefix + "/webserver/ui-history/history/app-1", follow_redirects=False
            )
        assert response.status_code == 302
        assert response.headers["location"] == location
    finally:
        asyncio.run(http_client.aclose())


@pytest.mark.parametrize("path", ["/webserver/ui/analytics/app-1/jobs/", "/webserver/ui-history/jobs/"])
@pytest.mark.parametrize("remote_mode", [True, False])
def test_remote_proxy_never_forwards_operator_or_gateway_credentials(monkeypatch, path, remote_mode):
    from spark_on_k8s.api import remote_auth
    from spark_on_k8s.api.remote_auth import RemoteAccessMiddleware
    from spark_on_k8s.api.submission_policy import SubmissionSettings

    token = "dummy-operator-bearer-with-more-than-32-characters"
    monkeypatch.setattr(
        remote_auth, "get_submission_settings", lambda: SubmissionSettings(remote_mode, token.encode())
    )
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    seen = []

    def upstream(request):
        seen.append(request)
        body = UpstreamBody()
        body.chunks = [b"range"]
        return httpx.Response(206, stream=body, headers={"content-range": "bytes 0-4/5"})

    transport = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(router)
    sensitive = {
        "Authorization": "Bearer " + token,
        "Cookie": "gateway-session=private-cookie",
        "Proxy-Authorization": "Basic private-proxy",
        "X-Auth-Request-Access-Token": "private-gateway-token",
        "X-Forwarded-Access-Token": "private-forwarded-token",
        "X-Custom-Identity": "private-vendor-identity",
    }
    functional = {
        "Accept": "text/html",
        "Accept-Language": "en",
        "Range": "bytes=0-4",
        "If-None-Match": '"etag"',
    }
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get(path + "?stage=1", headers={**sensitive, **functional})
        assert response.status_code == 206
        assert response.content == b"range"
        assert response.headers["content-range"] == "bytes 0-4/5"
        assert len(seen) == 1
        assert seen[0].url.query == b"stage=1"
        for key, value in functional.items():
            assert seen[0].headers[key] == value
        for key in sensitive:
            assert key not in seen[0].headers
        assert token not in str(seen[0].headers)
    finally:
        asyncio.run(transport.aclose())


@pytest.mark.parametrize("status_code", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("root_path", ["", "/platform/spark"])
@pytest.mark.parametrize("history", [False, True])
def test_absolute_upstream_redirect_keeps_public_proxy_origin(monkeypatch, status_code, root_path, history):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    proxy_path = "/webserver/ui-history" if history else "/webserver/ui/analytics/app-1"
    host = "http://history:18080" if history else "http://app-1.analytics.svc.cluster.local:4040"
    # Spark 4 emits an absolute internal URL containing its configured proxy base.
    location = host + proxy_path + "/jobs/?description=a%2Bb&stage=1&stage=2#job-0"
    body = UpstreamBody()
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status_code, headers={"location": location}, stream=body)
        )
    )
    app = FastAPI(root_path=root_path)
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get(
                root_path + proxy_path + ("/history/app-1" if history else ""), follow_redirects=False
            )
        assert response.status_code == status_code
        assert response.headers["location"] == (
            root_path + proxy_path + "/jobs/?description=a%2Bb&stage=1&stage=2#job-0"
        )
        assert response.content == b"".join(body.chunks)
        assert body.closed
    finally:
        asyncio.run(transport.aclose())


@pytest.mark.parametrize(
    "location, expected",
    [
        ("jobs/?show=all", "/platform/spark/webserver/ui/analytics/app-1/jobs/?show=all"),
        ("/jobs/", "/platform/spark/webserver/ui/analytics/app-1/jobs/"),
        (
            "//app-1.analytics.svc.cluster.local:4040/jobs/",
            "/platform/spark/webserver/ui/analytics/app-1/jobs/",
        ),
        (
            "/platform/spark/webserver/ui/analytics/app-1/jobs/",
            "/platform/spark/webserver/ui/analytics/app-1/jobs/",
        ),
        ("https://external.example/jobs/", "https://external.example/jobs/"),
        ("//external.example/jobs/", "//external.example/jobs/"),
        (None, None),
    ],
)
def test_driver_redirect_paths_and_external_origins(location, expected):
    headers = {"location": location} if location else {}
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(302, headers=headers, stream=UpstreamBody())
        )
    )
    app = FastAPI(root_path="/platform/spark")
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get(
                "/platform/spark/webserver/ui/analytics/app-1", follow_redirects=False
            )
        assert response.status_code == 302
        assert response.headers.get("location") == expected
    finally:
        asyncio.run(transport.aclose())


def test_malformed_upstream_location_is_not_rewritten():
    from spark_on_k8s.api.webserver import _proxy_response_headers
    from starlette.requests import Request

    location = "http://upstream:invalid/jobs/"
    response = httpx.Response(302, headers={"location": location})
    headers = _proxy_response_headers(
        response,
        httpx.URL("http://app-1.analytics.svc.cluster.local:4040/"),
        Request({"type": "http", "root_path": ""}),
        "/webserver/ui/analytics/app-1",
    )
    assert headers["location"] == location


@pytest.mark.parametrize("history", [False, True])
@pytest.mark.parametrize("encoding", [None, "gzip", "deflate"])
def test_mounted_proxy_rewrites_only_its_quoted_html_paths(monkeypatch, history, encoding):
    import gzip
    import zlib

    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", "history:18080")
    base = "/webserver/ui-history" if history else "/webserver/ui/analytics/app-1"
    prefix = "/platform/spark"
    document = (
        f"<link href=\"{base}/static/ui.css\"><a href='{base}/jobs/?id=1#stage'>café</a>"
        f"<script>setUIRoot('{base}')</script>"
        f'<a href="{prefix}{base}/jobs/">already mounted</a>'
        f'<a href="https://example.test{base}/jobs/">external</a>'
        f'<a href="//example.test{base}/jobs/">external relative scheme</a>'
        f'<a href="{base}-other/jobs/">different application</a>'
        '<a href="/unrelated/">unrelated</a>'
    ).encode()
    expected = document.replace(f'"{base}/'.encode(), f'"{prefix}{base}/'.encode())
    expected = expected.replace(f"'{base}".encode(), f"'{prefix}{base}".encode())
    wire = (
        gzip.compress(document)
        if encoding == "gzip"
        else zlib.compress(document)
        if encoding == "deflate"
        else document
    )
    body = UpstreamBody()
    body.chunks = [wire[:17], wire[17:31], wire[31:]]
    headers = {
        "content-type": "text/html" if encoding else "text/html; charset=utf-8",
        "content-length": str(len(wire)),
        "etag": '"upstream-etag"',
        "content-md5": "upstream-md5",
        "digest": "upstream-digest",
        "cache-control": "no-cache",
    }
    if encoding:
        headers["content-encoding"] = encoding
    seen = []

    def upstream(request):
        seen.append(request)
        return httpx.Response(200, headers=headers, stream=body)

    transport = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    app = FastAPI(root_path=prefix)
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get(
                prefix + base + "/jobs/", headers={"X-Forwarded-Context": "/attacker"}
            )
        assert response.status_code == 200
        assert response.content == expected
        assert response.headers["content-length"] == str(len(expected))
        for header in ("content-encoding", "etag", "content-md5", "digest"):
            assert header not in response.headers
        assert response.headers["cache-control"] == "no-cache"
        assert "x-forwarded-context" not in seen[0].headers
        assert body.closed
    finally:
        asyncio.run(transport.aclose())


@pytest.mark.parametrize("compressed", [False, True])
def test_mounted_proxy_rejects_oversized_decoded_html_before_sending_a_success(monkeypatch, compressed):
    import gzip

    from spark_on_k8s.api import webserver

    monkeypatch.setattr(webserver, "_MAX_PROXY_HTML_BYTES", 32)
    document = b'<a href="/webserver/ui/analytics/app-1/jobs/">' + b"x" * 1000
    wire = gzip.compress(document) if compressed else document
    body = UpstreamBody()
    body.chunks = [wire[:10], wire[10:]]
    headers = {"content-type": "text/html"}
    if compressed:
        headers["content-encoding"] = "gzip"
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, headers=headers, stream=body))
    )
    app = FastAPI(root_path="/platform/spark")
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get("/platform/spark/webserver/ui/analytics/app-1/jobs/")
        assert response.status_code == 502
        assert response.json() == {"detail": "Spark UI HTML exceeds the proxy size limit."}
        assert body.closed
    finally:
        asyncio.run(transport.aclose())


def test_mounted_proxy_invalid_compressed_html_is_a_closed_upstream_error():
    body = UpstreamBody()
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "text/html", "content-encoding": "gzip"}, stream=body
            )
        )
    )
    app = FastAPI(root_path="/platform/spark")
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get("/platform/spark/webserver/ui/analytics/app-1/jobs/")
        assert response.status_code == 502
        assert response.json() == {"detail": "Unable to decode Spark UI HTML."}
        assert body.closed
    finally:
        asyncio.run(transport.aclose())


@pytest.mark.parametrize(
    "root_path, content_type, status",
    [
        ("", "text/html", 200),
        ("/platform/spark", "application/javascript", 200),
        ("/platform/spark", "application/octet-stream", 200),
        ("/platform/spark", "text/html", 206),
    ],
)
def test_unmounted_html_and_mounted_assets_or_ranges_preserve_wire_bytes(root_path, content_type, status):
    import gzip

    content = b'<a href="/webserver/ui/analytics/app-1/jobs/">unchanged</a>'
    wire = gzip.compress(content)
    body = UpstreamBody()
    body.chunks = [wire[:8], wire[8:]]
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                status,
                headers={
                    "content-type": content_type,
                    "content-encoding": "gzip",
                    "content-length": str(len(wire)),
                    "etag": '"same"',
                },
                stream=body,
            )
        )
    )
    app = FastAPI(root_path=root_path)
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get(root_path + "/webserver/ui/analytics/app-1/jobs/")
        assert response.status_code == status
        assert response.content == content
        assert response.headers["content-encoding"] == "gzip"
        assert response.headers["content-length"] == str(len(wire))
        assert response.headers["etag"] == '"same"'
        assert body.closed
    finally:
        asyncio.run(transport.aclose())


def test_mounted_proxy_rejects_unknown_html_encoding_without_relabeling_compressed_bytes():
    body = UpstreamBody()
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "text/html", "content-encoding": "unknown"}, stream=body
            )
        )
    )
    app = FastAPI(root_path="/platform/spark")
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get("/platform/spark/webserver/ui/analytics/app-1/jobs/")
        assert response.status_code == 502
        assert response.json() == {"detail": "Unsupported Spark UI HTML content encoding."}
        assert body.closed
    finally:
        asyncio.run(transport.aclose())


@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
@pytest.mark.parametrize("damage", ["truncated", "trailing"])
def test_mounted_proxy_rejects_incomplete_or_trailing_compressed_html(encoding, damage):
    import gzip
    import zlib

    content = b'<a href="/webserver/ui/analytics/app-1/jobs/">bounded</a>'
    wire = gzip.compress(content) if encoding == "gzip" else zlib.compress(content)
    wire = wire[:-1] if damage == "truncated" else wire + b"unexpected trailing bytes"
    body = UpstreamBody()
    body.chunks = [wire[:10], wire[10:]]
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "text/html", "content-encoding": encoding}, stream=body
            )
        )
    )
    app = FastAPI(root_path="/platform/spark")
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get("/platform/spark/webserver/ui/analytics/app-1/jobs/")
        assert response.status_code == 502
        assert response.json() == {"detail": "Unable to decode Spark UI HTML."}
        assert body.closed
    finally:
        asyncio.run(transport.aclose())


@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_mounted_html_decoder_allocation_cannot_exceed_remaining_budget(monkeypatch, encoding):
    import zlib

    from spark_on_k8s.api import webserver

    budget = 1024
    monkeypatch.setattr(webserver, "_MAX_PROXY_HTML_BYTES", budget)
    bits = 16 + zlib.MAX_WBITS if encoding == "gzip" else zlib.MAX_WBITS
    compressor = zlib.compressobj(wbits=bits)
    # Construct a tiny wire payload representing16MiB without allocating the expanded document.
    wire = b"".join(compressor.compress(b"x" * 1024) for _ in range(16384)) + compressor.flush()
    original_factory = zlib.decompressobj
    allocations = []

    class ObservedDecoder:
        def __init__(self, *args):
            self.decoder = original_factory(*args)

        def decompress(self, data, max_length=0):
            value = self.decoder.decompress(data, max_length)
            allocations.append((max_length, len(value)))
            return value

        def __getattr__(self, name):
            return getattr(self.decoder, name)

    monkeypatch.setattr(webserver.zlib, "decompressobj", ObservedDecoder)
    body = UpstreamBody()
    body.chunks = [wire]
    transport = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "text/html", "content-encoding": encoding}, stream=body
            )
        )
    )
    app = FastAPI(root_path="/platform/spark")
    app.include_router(router)
    try:
        with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client", return_value=transport):
            response = TestClient(app).get("/platform/spark/webserver/ui/analytics/app-1/jobs/")
        assert response.status_code == 502
        assert response.json() == {"detail": "Spark UI HTML exceeds the proxy size limit."}
        assert allocations
        assert all(0 < limit <= budget + 1 and allocated <= budget + 1 for limit, allocated in allocations)
        assert body.closed
    finally:
        asyncio.run(transport.aclose())
