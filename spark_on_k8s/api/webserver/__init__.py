from __future__ import annotations

import asyncio
import re
from contextlib import aclosing
from functools import wraps
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from kubernetes_asyncio.client import ApiException
from starlette.background import BackgroundTask
from starlette.requests import Request  # noqa: TCH002
from starlette.responses import HTMLResponse, StreamingResponse
from starlette.templating import Jinja2Templates

from spark_on_k8s import __version__
from spark_on_k8s.api import AsyncHttpClientSingleton, KubernetesClientSingleton
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager

router = APIRouter(
    prefix="/webserver",
    tags=["spark-apps", "webserver"],
    include_in_schema=False,
)


_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


_REMOTE_PROXY_HEADERS = {
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
    if not request.scope.get("spark_on_k8s.remote_authenticated"):
        return request.headers.raw
    # The API/gateway identity is never a credential for application or history services.
    # An allowlist also excludes vendor-specific gateway token/identity headers.
    return [(key, value) for key, value in request.headers.raw if key.lower() in _REMOTE_PROXY_HEADERS]


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
    return StreamingResponse(
        reverse_proxy_resp.aiter_raw(),
        status_code=reverse_proxy_resp.status_code,
        headers=reverse_proxy_resp.headers,
        background=BackgroundTask(reverse_proxy_resp.aclose),
    )


@router.get("/ui-history/{path:path}")
@handle_k8s_errors
async def spark_history_reverse_proxy(request: Request, path: str):
    spark_history_url = APIConfiguration.SPARK_ON_K8S_API_SPARK_HISTORY_HOST
    if not spark_history_url:
        raise HTTPException(status_code=412, detail="SPARK_ON_K8S_API_SPARK_HISTORY_HOST is not set.")
    async_http_client = AsyncHttpClientSingleton.client()
    spark_history_url = (
        f"http://{spark_history_url}" if not spark_history_url.startswith("http") else spark_history_url
    )
    url = httpx.URL(
        url=spark_history_url,
        path=f"/{path}",
        query=request.url.query.encode("utf-8"),
    )
    reverse_proxy_req = async_http_client.build_request(
        request.method, url=url, headers=_proxy_headers(request), content=request.stream()
    )
    reverse_proxy_resp = await async_http_client.send(reverse_proxy_req, stream=True)
    response_headers = reverse_proxy_resp.headers
    response_status_code = reverse_proxy_resp.status_code
    if response_status_code == 302:
        # this is a workaround to fix the location header in the response
        location = response_headers.get("location")
        if location:
            proxy_prefix = request.scope.get("root_path", "").rstrip("/") + "/webserver/ui-history"
            location_path = location.split("?", 1)[0].split("#", 1)[0]
            if location_path == proxy_prefix or location_path.startswith(proxy_prefix + "/"):
                pass
            elif location_path == "/webserver/ui-history" or location_path.startswith(
                "/webserver/ui-history/"
            ):
                response_headers["location"] = proxy_prefix + location.removeprefix("/webserver/ui-history")
            elif location.startswith("/") and not location.startswith("//"):
                response_headers["location"] = proxy_prefix + location
    return StreamingResponse(
        reverse_proxy_resp.aiter_raw(),
        status_code=response_status_code,
        headers=response_headers,
        background=BackgroundTask(reverse_proxy_resp.aclose),
    )


current_dir = Path(__file__).parent.absolute()
templates = Jinja2Templates(directory=str(current_dir / "templates"))


def console_page(request: Request, *, view: str, namespace: str, app_id: str | None = None, tail: int = 1000):
    """Serve the packaged UI; fetch application data without reloading the page."""
    config = {
        "basePath": request.scope.get("root_path", "").rstrip("/"),
        "namespace": namespace,
        "refreshInterval": APIConfiguration.SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL,
        "allowKill": APIConfiguration.SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON,
        "allowDelete": APIConfiguration.SPARK_ON_K8S_API_WEBSERVER_APPS_DELETE_BUTTON,
        "historyEnabled": bool(APIConfiguration.SPARK_ON_K8S_API_SPARK_HISTORY_HOST),
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
    namespace = request.query_params.get("namespace", APIConfiguration.SPARK_ON_K8S_API_DEFAULT_NAMESPACE)
    return console_page(request, view="apps", namespace=namespace)


@router.websocket("/ws/logs/{namespace}/{app_id}")
async def app_logs_websocket(
    websocket: WebSocket, namespace: str, app_id: str, tail: int = Query(default=-1, ge=-1, le=10000)
):
    """Stream literal log text and signal failures through the WebSocket close state."""
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
    return console_page(request, view="logs", namespace=namespace, app_id=app_id, tail=tail)
