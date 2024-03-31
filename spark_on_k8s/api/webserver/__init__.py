from __future__ import annotations

from functools import wraps
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, WebSocket
from kubernetes_asyncio.client import ApiException
from starlette.background import BackgroundTask
from starlette.requests import Request  # noqa: TCH002
from starlette.responses import HTMLResponse, StreamingResponse
from starlette.templating import Jinja2Templates

from spark_on_k8s.api import AsyncHttpClientSingleton
from spark_on_k8s.api.apps import SparkApp, list_apps
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager
from spark_on_k8s.utils.spark_app_status import SparkAppStatus

router = APIRouter(
    prefix="/webserver",
    tags=["spark-apps", "webserver"],
    include_in_schema=False,
)


def handle_k8s_errors(func):
    @wraps(func)
    async def wrapper(*args, request: Request, **kwargs):
        try:
            return await func(*args, request=request, **kwargs)
        except ApiException as e:
            title = f"{e.status} {e.reason}"
            message = e.body
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": title,
                    "message": message,
                },
            )

    return wrapper


@router.get("/ui/{path:path}")
@handle_k8s_errors
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


@router.get("/ui-history/{path:path}")
@handle_k8s_errors
async def spark_history_reverse_proxy(request: Request):
    spark_history_url = APIConfiguration.SPARK_ON_K8S_API_SPARK_HISTORY_HOST
    if not spark_history_url:
        return HTTPException(status_code=412, detail="SPARK_ON_K8S_API_SPARK_HISTORY_HOST is not set.")
    path = request.url.path.replace(router.prefix, "").lstrip("/").replace("ui-history/", "")
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
        request.method, url=url, headers=request.headers.raw, content=request.stream()
    )
    reverse_proxy_resp = await async_http_client.send(reverse_proxy_req, stream=True)
    response_headers = reverse_proxy_resp.headers
    response_status_code = reverse_proxy_resp.status_code
    if response_status_code == 302:
        # this is a workaround to fix the location header in the response
        location = response_headers.get("location")
        if location:
            response_headers["location"] = (
                location.replace(path, f"webserver/ui-history/{path}")
                if "webserver/ui-history" not in location
                else location
            )
    return StreamingResponse(
        reverse_proxy_resp.aiter_raw(),
        status_code=response_status_code,
        headers=response_headers,
        background=BackgroundTask(reverse_proxy_resp.aclose),
    )


current_dir = Path(__file__).parent.absolute()
templates = Jinja2Templates(directory=str(current_dir / "templates"))


@router.get("/apps", response_class=HTMLResponse)
@handle_k8s_errors
async def apps(request: Request):
    """List spark apps in a namespace, and display them in a web page."""
    namespace = request.query_params.get("namespace", APIConfiguration.SPARK_ON_K8S_API_DEFAULT_NAMESPACE)
    apps_list = await list_apps(namespace)
    spark_history_url = APIConfiguration.SPARK_ON_K8S_API_SPARK_HISTORY_HOST
    if spark_history_url:
        spark_history_url = (
            spark_history_url if spark_history_url.startswith("http") else f"http://{spark_history_url}"
        )
        async_http_client = AsyncHttpClientSingleton.client()
        history_apps_list = [
            SparkApp(
                app_id=app["id"],
                status=SparkAppStatus.Unknown,
                driver_logs=False,
                spark_ui_proxy=False,
                spark_history_proxy=True,
            )
            for app in (
                await async_http_client.get(f"{spark_history_url}/api/v1/applications?status=completed")
            ).json()
        ]
    else:
        history_apps_list = []
    apps_dict = {app.app_id: app for app in apps_list}
    for app in history_apps_list:
        if app.app_id not in apps_dict:
            apps_dict[app.app_id] = app
        else:
            apps_dict[app.app_id].spark_history_proxy = True
    return templates.TemplateResponse(
        "apps.html",
        {
            "request": request,
            "apps_list": apps_dict.values(),
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
@handle_k8s_errors
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
