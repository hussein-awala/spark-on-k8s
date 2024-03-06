from __future__ import annotations

from pathlib import Path

import httpx
from fastapi import APIRouter, WebSocket
from starlette.background import BackgroundTask
from starlette.requests import Request  # noqa: TCH002
from starlette.responses import HTMLResponse, StreamingResponse
from starlette.templating import Jinja2Templates

from spark_on_k8s.api import AsyncHttpClientSingleton
from spark_on_k8s.api.apps import list_apps
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.utils.app_manager import AsyncSparkAppManager

router = APIRouter(
    prefix="/webserver",
    tags=["spark-apps", "webserver"],
    include_in_schema=False,
)


@router.get("/ui/{path:path}")
async def ui_reverse_proxy(request: Request):
    path = request.url.path
    path = path.replace(router.prefix + "/ui", "").lstrip("/")
    namespace = path.split("/")[0]
    service_name = path.split("/")[1]
    path = path.replace(f"{namespace}/{service_name}", "")
    async_http_client = AsyncHttpClientSingleton.client()
    url = httpx.URL(
        url=f"http://{service_name}.{namespace}.svc.cluster.local:4040",
        path=path,
        query=request.url.query.encode("utf-8"),
    )
    reverse_proxy_req = async_http_client.build_request(
        request.method, url=url, headers=request.headers.raw, content=request.stream()
    )
    reverse_proxy_resp = await async_http_client.send(reverse_proxy_req, stream=True)
    return StreamingResponse(
        reverse_proxy_resp.aiter_raw(),
        status_code=reverse_proxy_resp.status_code,
        headers=reverse_proxy_resp.headers,
        background=BackgroundTask(reverse_proxy_resp.aclose),
    )


current_dir = Path(__file__).parent.absolute()
templates = Jinja2Templates(directory=str(current_dir / "templates"))


@router.get("/apps", response_class=HTMLResponse)
async def apps(request: Request):
    """List spark apps in a namespace, and display them in a web page."""
    namespace = request.query_params.get("namespace", APIConfiguration.SPARK_ON_K8S_API_DEFAULT_NAMESPACE)
    apps_list = await list_apps(namespace)
    return templates.TemplateResponse(
        "apps.html",
        {
            "request": request,
            "apps_list": apps_list,
            "namespace": namespace,
        },
    )


@router.websocket("/ws/logs/{namespace}/{app_id}")
async def app_logs_websocket(websocket: WebSocket, namespace: str, app_id: str, tail: int = -1):
    """Websocket endpoint to stream logs of a spark app."""
    await websocket.accept()
    async_spark_app_manager = AsyncSparkAppManager()
    async for log in async_spark_app_manager.logs_streamer(
        namespace=namespace, app_id=app_id, tail_lines=tail
    ):
        await websocket.send_text(log)
    await websocket.close()


@router.get("/logs/{namespace}/{app_id}", response_class=HTMLResponse)
async def app_logs(request: Request, namespace: str, app_id: str):
    """Display logs of a spark app in a web page."""
    tail = request.query_params.get("tail", -1)
    return templates.TemplateResponse(
        "app_logs.html",
        {
            "request": request,
            "namespace": namespace,
            "app_id": app_id,
            "tail": tail,
        },
    )
