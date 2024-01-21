from __future__ import annotations

from typing import TYPE_CHECKING

import httpx
from fastapi import APIRouter
from starlette.background import BackgroundTask
from starlette.responses import StreamingResponse

from spark_on_k8s.api import AsyncHttpClientSingleton

if TYPE_CHECKING:
    from starlette.requests import Request

router = APIRouter(
    prefix="/ui",
    tags=["spark-jobs"],
    include_in_schema=False,
)


@router.get("/{path:path}")
async def reverse_proxy(request: Request):
    path = request.url.path
    path = path.replace(router.prefix, "").lstrip("/")
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
