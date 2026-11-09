from __future__ import annotations

import asyncio
import re
import zlib
from contextlib import aclosing
from functools import wraps
from pathlib import Path
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import httpx
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from kubernetes_asyncio.client import ApiException
from starlette.background import BackgroundTask
from starlette.requests import Request  # noqa: TCH002
from starlette.responses import HTMLResponse, Response, StreamingResponse
from starlette.templating import Jinja2Templates

from spark_on_k8s import __version__
from spark_on_k8s.api import AsyncHttpClientSingleton, KubernetesClientSingleton, logger
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.namespace_scope import namespace_scope
from spark_on_k8s.api.run_history import get_history_settings
from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager

router = APIRouter(
    prefix="/webserver",
    tags=["spark-apps", "webserver"],
    include_in_schema=False,
)


_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


_PROXY_HEADERS = {
    b"accept",
    b"accept-encoding",
    b"accept-language",
    b"cache-control",
    b"content-type",
    b"if-match",
    b"if-modified-since",
    b"if-none-match",
    b"if-range",
    b"if-unmodified-since",
    b"range",
}


def _proxy_headers(request: Request) -> list[tuple[bytes, bytes]]:
    # The API/gateway identity is never a credential for application or history services.
    # An allowlist also excludes vendor-specific gateway token/identity headers.
    return [(key, value) for key, value in request.headers.raw if key.lower() in _PROXY_HEADERS]


_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def _proxy_response_headers(
    response: httpx.Response, upstream: httpx.URL, request: Request, proxy_path: str
) -> httpx.Headers:
    """Keep upstream redirects on the browser-facing proxy, including mounted deployments."""
    headers = response.headers.copy()
    # Hop-by-hop headers describe the upstream connection; the ASGI server frames this one.
    # A rewritten HTML body otherwise carries both Transfer-Encoding and a new Content-Length.
    connection_scoped = {
        token.strip().lower() for value in headers.get_list("connection") for token in value.split(",")
    }
    for name in _HOP_BY_HOP_HEADERS | connection_scoped:
        headers.pop(name, None)
    location = headers.get("location")
    if response.status_code not in {301, 302, 303, 307, 308} or not location:
        return headers
    try:
        target = httpx.URL(urljoin(str(upstream), location))
        if (target.scheme, target.host, target.port) != (upstream.scheme, upstream.host, upstream.port):
            return headers
        parsed = urlsplit(str(target))
    except (httpx.InvalidURL, ValueError):
        return headers
    prefix = request.scope.get("root_path", "").rstrip("/") + proxy_path
    path = parsed.path
    if path == prefix or path.startswith(prefix + "/"):
        pass
    elif path == proxy_path or path.startswith(proxy_path + "/"):
        path = prefix + path.removeprefix(proxy_path)
    else:
        path = prefix + path
    headers["location"] = urlunsplit(("", "", path, parsed.query, parsed.fragment))
    return headers


_MAX_PROXY_HTML_BYTES = 8 * 1024 * 1024


async def _proxy_response(
    response: httpx.Response, upstream: httpx.URL, request: Request, proxy_path: str
) -> Response:
    headers = _proxy_response_headers(response, upstream, request, proxy_path)
    root_path = request.scope.get("root_path", "").rstrip("/")
    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if root_path and response.status_code == 200 and content_type == "text/html":
        # Spark's configured proxyBase takes precedence over forwarded context headers.
        # Read/decompress a bounded document before responding, so an oversized page cannot
        # become a partially rewritten success response. Other bodies keep streaming raw.
        body = bytearray()
        try:
            encoding = headers.get("content-encoding", "").strip().lower()
            if encoding not in {"", "identity", "gzip", "deflate"}:
                raise HTTPException(502, detail="Unsupported Spark UI HTML content encoding.")
            decoder = (
                zlib.decompressobj(16 + zlib.MAX_WBITS if encoding == "gzip" else zlib.MAX_WBITS)
                if encoding in {"gzip", "deflate"}
                else None
            )
            async for raw in response.aiter_raw(chunk_size=65536):
                # Bound decoder allocation itself, not just chunks yielded after decoding.
                remaining = _MAX_PROXY_HTML_BYTES - len(body)
                chunk = decoder.decompress(raw, remaining + 1) if decoder is not None else raw
                if len(chunk) > remaining:
                    raise HTTPException(502, detail="Spark UI HTML exceeds the proxy size limit.")
                body.extend(chunk)
                if decoder is not None and decoder.unused_data:
                    raise HTTPException(502, detail="Unable to decode Spark UI HTML.")
            if decoder is not None and not decoder.eof:
                raise HTTPException(502, detail="Unable to decode Spark UI HTML.")
        except zlib.error as error:
            raise HTTPException(502, detail="Unable to decode Spark UI HTML.") from error
        finally:
            await response.aclose()
        base = proxy_path.encode("ascii")
        mounted_base = quote(root_path, safe="/%").encode("ascii") + base
        # Match HTML attributes and Spark's inline JS strings, but not external URLs,
        # already mounted paths, or another app whose name merely shares this prefix.
        pattern = re.compile(rb"([\"'])" + re.escape(base) + rb"(?=[/?#\"'])")
        content = pattern.sub(lambda match: match[1] + mounted_base, bytes(body))
        for header in ("content-length", "content-encoding", "etag", "content-md5", "digest"):
            headers.pop(header, None)
        return Response(content, status_code=response.status_code, headers=headers)
    return StreamingResponse(
        response.aiter_raw(),
        status_code=response.status_code,
        headers=headers,
        background=BackgroundTask(response.aclose),
    )


def _origin(value: str) -> tuple[str, str, int] | None:
    """Parse an HTTP origin, rejecting URL paths, credentials and opaque origins."""
    if not value or any(character.isspace() for character in value) or "\\" in value:
        return None
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or "?" in value
            or "#" in value
        ):
            return None
        port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
        return parsed.scheme, parsed.hostname.lower(), port
    except ValueError:
        return None


def _logs_origin_allowed(websocket: WebSocket) -> bool:
    origins = websocket.headers.getlist("origin")
    # Non-browser clients may omit Origin; existing authentication still applies.
    if not origins:
        return True
    if len(origins) != 1 or (origin := _origin(origins[0])) is None:
        return False
    configured = APIConfiguration.SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS
    if configured:
        allowed = [_origin(value.strip()) for value in configured.split(",")]
        # A malformed deployment list must not fall back to a broader default.
        return all(value is not None for value in allowed) and origin in allowed
    hosts = websocket.headers.getlist("host")
    if len(hosts) != 1:
        return False
    scheme = {"ws": "http", "wss": "https"}.get(websocket.scope["scheme"])
    # Never derive trust from Forwarded or X-Forwarded-* request headers.
    # An explicit public-origin list supports gateways that translate Host/TLS.
    return scheme is not None and origin == _origin(f"{scheme}://{hosts[0]}")


def handle_k8s_errors(func):
    @wraps(func)
    async def wrapper(*args, request: Request, **kwargs):
        try:
            return await func(*args, request=request, **kwargs)
        except ApiException as e:
            title = f"{e.status} {e.reason}"
            message = e.body
            return templates.TemplateResponse(
                request=request,
                name="error.html",
                context={
                    "title": title,
                    "message": message,
                },
                status_code=e.status or 500,
            )
        except httpx.HTTPError as e:
            # Finished drivers and restarting history servers are upstream failures, not API errors.
            # Exception text can contain internal resolver/upstream details; report only its type.
            logger.warning("Spark UI proxy upstream request failed: %s", type(e).__name__)
            if isinstance(e, httpx.TimeoutException):
                raise HTTPException(504, detail="The Spark UI did not respond in time.") from e
            raise HTTPException(502, detail="The Spark UI is unavailable.") from e

    return wrapper


@router.get("/ui/{path:path}")
@handle_k8s_errors
async def ui_reverse_proxy(request: Request, path: str):
    parts = path.split("/", 2)
    if len(parts) < 2 or not all(parts[:2]):
        raise HTTPException(status_code=404, detail="A namespace and Spark service are required.")
    namespace, service_name = parts[:2]
    if not all(_DNS_LABEL.fullmatch(value) for value in (namespace, service_name)):
        raise HTTPException(status_code=400, detail="Invalid Kubernetes namespace or service name.")
    # The proxy connects to Services directly, so Kubernetes RBAC cannot bound it; the scope does.
    namespace_scope().require(namespace)
    path = "/" + parts[2] if len(parts) == 3 else "/"
    async_http_client = AsyncHttpClientSingleton.client()
    url = httpx.URL(
        url=f"http://{service_name}.{namespace}.svc.cluster.local:4040",
        path=path,
        query=request.url.query.encode("utf-8"),
    )
    reverse_proxy_req = async_http_client.build_request(
        request.method, url=url, headers=_proxy_headers(request), content=request.stream()
    )
    reverse_proxy_resp = await async_http_client.send(reverse_proxy_req, stream=True)
    return await _proxy_response(
        reverse_proxy_resp, url, request, f"/webserver/ui/{namespace}/{service_name}"
    )


@router.get("/ui-history/{path:path}")
@handle_k8s_errors
async def spark_history_reverse_proxy(request: Request, path: str):
    spark_history_url = APIConfiguration.SPARK_ON_K8S_API_SPARK_HISTORY_HOST
    if not spark_history_url:
        raise HTTPException(status_code=412, detail="SPARK_ON_K8S_API_SPARK_HISTORY_HOST is not set.")
    async_http_client = AsyncHttpClientSingleton.client()
    # Match the console listing: a scheme is explicit, not inferred from a host name starting "http".
    spark_history_url = spark_history_url if "://" in spark_history_url else f"http://{spark_history_url}"
    url = httpx.URL(
        url=spark_history_url,
        path=f"/{path}",
        query=request.url.query.encode("utf-8"),
    )
    reverse_proxy_req = async_http_client.build_request(
        request.method, url=url, headers=_proxy_headers(request), content=request.stream()
    )
    reverse_proxy_resp = await async_http_client.send(reverse_proxy_req, stream=True)
    return await _proxy_response(reverse_proxy_resp, url, request, "/webserver/ui-history")


current_dir = Path(__file__).parent.absolute()
templates = Jinja2Templates(directory=str(current_dir / "templates"))


def console_page(request: Request, *, view: str, namespace: str, app_id: str | None = None, tail: int = 1000):
    """Serve the packaged UI; fetch application data without reloading the page."""
    scope = namespace_scope()
    config = {
        "basePath": request.scope.get("root_path", "").rstrip("/"),
        "namespace": namespace,
        # A listed scope becomes a picker; a single namespace is not editable at all.
        "namespaces": list(scope.namespaces) if scope.namespaces is not None else None,
        "namespaceEditable": scope.namespaces is None or len(scope.namespaces) > 1,
        "refreshInterval": APIConfiguration.SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL,
        "allowKill": APIConfiguration.SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON,
        "allowDelete": APIConfiguration.SPARK_ON_K8S_API_WEBSERVER_APPS_DELETE_BUTTON,
        "historyEnabled": bool(APIConfiguration.SPARK_ON_K8S_API_SPARK_HISTORY_HOST),
        "runsEnabled": get_history_settings().enabled,
        "version": __version__,
        "view": view,
        "appId": app_id,
        "tail": tail,
    }
    return templates.TemplateResponse(
        request=request,
        name="console.html",
        context={"config": config},
        headers={"Cache-Control": "no-cache"},
    )


@router.get("/apps", response_class=HTMLResponse)
async def apps(request: Request):
    """Display the application console, with namespace preserved in the URL."""
    scope = namespace_scope()
    namespace = scope.require(request.query_params.get("namespace") or scope.default)
    return console_page(request, view="apps", namespace=namespace)


@router.websocket("/ws/logs/{namespace}/{app_id}")
async def app_logs_websocket(
    websocket: WebSocket, namespace: str, app_id: str, tail: int = Query(default=-1, ge=-1, le=10000)
):
    """Stream literal log text and signal failures through the WebSocket close state."""
    if not _logs_origin_allowed(websocket):
        await websocket.close(code=1008, reason="Browser origin is not permitted.")
        return
    if not namespace_scope().allows(namespace):
        await websocket.close(code=1008, reason="The namespace is outside this API's namespace scope.")
        return
    await websocket.accept()
    manager = AsyncSparkAppManager(k8s_client_manager=KubernetesClientSingleton.client_manager)

    async def send_logs():
        async with aclosing(
            manager.logs_streamer(namespace=namespace, app_id=app_id, tail_lines=tail)
        ) as logs:
            async for log in logs:
                await websocket.send_text(log)

    async def wait_for_disconnect():
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                return

    producer = asyncio.create_task(send_logs())
    disconnected = asyncio.create_task(wait_for_disconnect())
    code, reason = 1000, ""
    try:
        finished, _ = await asyncio.wait({producer, disconnected}, return_when=asyncio.FIRST_COMPLETED)
        if disconnected in finished:
            await disconnected
            return
        await producer
    except WebSocketDisconnect:
        return
    except ApiException as error:
        code = 1008 if error.status in (403, 404) else 1011
        reason = "Driver logs are unavailable."
    except ValueError:
        code, reason = 1008, "The application is no longer available."
    except Exception:
        code, reason = 1011, "Unable to stream driver logs."
    finally:
        producer.cancel()
        disconnected.cancel()
        await asyncio.gather(producer, disconnected, return_exceptions=True)
    try:
        await websocket.close(code=code, reason=reason)
    except WebSocketDisconnect:
        pass


@router.get("/logs/{namespace}/{app_id}", response_class=HTMLResponse)
async def app_logs(
    request: Request, namespace: str, app_id: str, tail: int = Query(default=1000, ge=-1, le=10000)
):
    """Display a bounded, searchable driver-log stream."""
    namespace_scope().require(namespace)
    return console_page(request, view="logs", namespace=namespace, app_id=app_id, tail=tail)


@router.get("/runs", response_class=HTMLResponse)
async def runs_page(request: Request, namespace: str | None = None):
    settings = get_history_settings()
    if not settings.enabled:
        raise HTTPException(status_code=404, detail="Run history is disabled.")
    namespace = namespace or settings.namespaces[0]
    if namespace not in settings.namespaces:
        raise HTTPException(status_code=400, detail="Namespace is not recorded.")
    return console_page(request, view="runs", namespace=namespace)
