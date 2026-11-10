from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
from urllib.parse import quote, urlencode, urlsplit

import httpx
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import create_dag_bag
from airflow.api_fastapi.core_api.security import GetUserDep, bearer_scheme, get_user, requires_access_dag
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from spark_on_k8s.airflow.ui.snapshots import SnapshotCache
from spark_on_k8s.airflow.ui.task_binding import (
    KUBERNETES_NAME,
    TaskBinding,
    TaskReference,
    failure,
    load_task_binding,
    task_reference,
)
from spark_on_k8s.airflow.ui_config import UISettings, normalize_service_url
from spark_on_k8s.utils.console_models import ConsoleApplication, ConsoleDiagnostics, ConsoleHistory
from spark_on_k8s.utils.spark_app_status import SparkAppStatus

ReferenceDep = Annotated[TaskReference, Depends(task_reference)]


async def authorize(
    request: Request, user, reference: TaskReference, entity: DagAccessEntity, method: str = "GET"
):
    await run_in_threadpool(
        requires_access_dag(method=method, access_entity=entity, param_dag_id=reference.dag_id), request, user
    )


def require_same_origin(connection):
    origin = connection.headers.get("origin")
    try:
        source = urlsplit(origin or "")
        target = urlsplit(str(connection.url))
        target_scheme = {"ws": "http", "wss": "https"}.get(target.scheme, target.scheme)
        source_port = source.port or (443 if source.scheme == "https" else 80)
        target_port = target.port or (443 if target_scheme == "https" else 80)
        valid = (
            source.scheme in {"http", "https"}
            and not source.path
            and not source.query
            and not source.fragment
            and (source.scheme, source.hostname, source_port) == (target_scheme, target.hostname, target_port)
        )
    except ValueError:
        valid = False
    if not valid:
        raise failure("invalid_origin", "This action requires the Airflow page's origin.", 403)


async def binding_for(request, reference: TaskReference) -> TaskBinding:
    binding = await run_in_threadpool(load_task_binding, reference, request.app.state.dag_bag)
    settings = request.app.state.settings
    configured_urls = {settings.service_url, settings.public_url}
    if binding.service_url:
        try:
            matches = normalize_service_url(binding.service_url) in configured_urls
        except ValueError:
            matches = False
        if not matches:
            raise failure("service_mismatch", "This task targets a different Spark service.", 409)
    return binding


async def upstream(request, method: str, path: str, **kwargs) -> httpx.Response:
    try:
        response = await request.app.state.client.request(method, path, **kwargs)
    except httpx.HTTPError as error:
        raise failure("service_unavailable", "The configured Spark service is unavailable.", 502) from error
    if response.status_code >= 300:
        status = response.status_code if response.status_code in {403, 404, 409, 412, 429} else 502
        messages = {
            403: "The Spark service denied access.",
            404: "The Spark application is unavailable.",
            409: "The Spark application cannot be changed in its current state.",
            412: "Spark history is not configured.",
            429: "The Spark service is busy.",
        }
        raise failure(
            "service_error", messages.get(status, "The configured Spark service is unavailable."), status
        )
    return response


async def history_configured(request) -> bool:
    try:
        response = await upstream(request, "GET", "webserver/api/capabilities")
    except HTTPException as error:
        if error.status_code == 404:
            # Older services do not expose this capability. Driver logs cannot
            # establish whether history is configured; do not probe history data.
            return False
        raise
    try:
        configured = response.json()["history_configured"]
        if not isinstance(configured, bool):
            raise ValueError("Expected a history capability boolean")
    except (ValueError, KeyError, TypeError) as error:
        raise failure("invalid_response", "The Spark service returned invalid capabilities.", 502) from error
    return configured


async def task_history(request, app_id: str) -> ConsoleHistory:
    response = await upstream(request, "GET", "webserver/api/history")
    try:
        result = ConsoleHistory.model_validate(response.json())
    except ValueError as error:
        raise failure(
            "invalid_response", "The Spark service returned an invalid history response.", 502
        ) from error
    return ConsoleHistory(items=[item for item in result.items if item.app_id == app_id])


async def application_for(request, binding: TaskBinding) -> ConsoleApplication:
    try:
        response = await upstream(
            request,
            "GET",
            f"webserver/api/apps/{quote(binding.namespace, safe='')}/pods/"
            f"{quote(binding.driver_pod, safe='')}",
        )
    except HTTPException as error:
        if error.status_code != 404 or not binding.app_id:
            raise
        # XCom can be edited independently of pod ownership. After deletion, retain
        # only a task-local display and an external link; do not proxy history data.
        return ConsoleApplication(
            app_id=binding.app_id,
            name=binding.app_id,
            namespace=binding.namespace,
            status=SparkAppStatus.Unknown,
            spark_history_proxy=True,
            source="history",
        )
    try:
        application = ConsoleApplication.model_validate(response.json())
    except (ValueError, ValidationError) as error:
        raise failure(
            "invalid_response", "The Spark service returned an invalid application response.", 502
        ) from error
    if application.namespace != binding.namespace or application.driver_pod != binding.driver_pod:
        raise failure("binding_mismatch", "The Spark service returned a different application.", 502)
    if binding.app_id and application.app_id != binding.app_id:
        raise failure("binding_mismatch", "The driver now belongs to a different Spark application.", 409)
    if application.driver_logs and not re.fullmatch(KUBERNETES_NAME, application.app_id):
        raise failure("invalid_application_id", "The Spark service returned an invalid application ID.", 502)
    if not owns_task(binding, application):
        raise failure(
            "task_ownership_mismatch", "This driver is not bound to the selected Airflow task.", 409
        )
    if not application.driver_logs:
        application.spark_ui_proxy = False
    return application


def snapshot_key(binding: TaskBinding) -> tuple | None:
    context = binding.task_context
    if context is None:
        return None
    # The identity comes from the authorized database task, never from editable XCom data.
    return (
        context.dag_id,
        context.run_id,
        context.task_id,
        context.map_index,
        context.region_id,
        context.region_index,
        context.try_number,
        binding.namespace,
        binding.driver_pod,
        binding.app_id,
    )


async def snapshot_for(connection, binding: TaskBinding) -> ConsoleApplication:
    """Return a recent verified application snapshot shared by this task attempt's viewers.

    Callers authorize the request and resolve its binding first. Only reads use this;
    actions always verify the current application themselves.
    """
    key = snapshot_key(binding)
    if key is None:
        return await application_for(connection, binding)
    return await connection.app.state.snapshots.get(key, lambda: application_for(connection, binding))


def owns_task(binding: TaskBinding, application: ConsoleApplication) -> bool:
    return bool(
        binding.task_context
        and application.task_context
        and binding.task_context.owns_same_task(application.task_context)
    )


def verified_app_path(binding: TaskBinding, application: ConsoleApplication) -> str:
    if not owns_task(binding, application):
        raise failure(
            "task_ownership_mismatch", "This driver is not bound to the selected Airflow task.", 409
        )
    if not application.driver_logs:
        raise failure("application_id_unavailable", "This driver has no verified Spark application ID.", 409)
    return f"{quote(binding.namespace, safe='')}/{quote(application.app_id, safe='')}"


def external_links(
    settings: UISettings, binding: TaskBinding, application: ConsoleApplication, *, history_enabled: bool
) -> dict:
    app_id = quote(application.app_id, safe="")
    live_path = f"{quote(binding.namespace, safe='')}/{app_id}"
    public_url = settings.public_url or settings.service_url
    return {
        "spark_ui_url": f"{public_url}/webserver/ui/{live_path}"
        if application.driver_logs and application.spark_ui_proxy
        else None,
        "spark_history_url": f"{public_url}/webserver/ui-history/history/{app_id}"
        if history_enabled and (application.spark_history_proxy or application.driver_logs)
        else None,
    }


def connect_upstream_logs(url: str):
    from websockets.asyncio.client import connect

    class ConfiguredServiceConnection(connect):
        def process_redirect(self, error: Exception) -> Exception:
            # A configured service must not redirect this authenticated proxy to another target.
            return error

    # Keep large driver lines intact without multiplying the larger message limit
    # by the client's default receive queue of 16 frames.
    return ConfiguredServiceConnection(
        url, proxy=None, open_timeout=5, close_timeout=5, max_size=16 * 1024 * 1024, max_queue=1
    )


def create_ui_app(settings: UISettings | None = None) -> FastAPI:
    settings = settings or UISettings.from_environment()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.dag_bag = create_dag_bag()
        app.state.snapshots = SnapshotCache(settings.snapshot_ttl)
        async with httpx.AsyncClient(
            base_url=settings.service_url + "/",
            follow_redirects=False,
            timeout=httpx.Timeout(15, connect=5),
            trust_env=False,
        ) as client:
            app.state.client = client
            try:
                yield
            finally:
                app.state.snapshots.clear()

    app = FastAPI(title="Spark task UI", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    def asset(name: str, media_type: str):
        path = Path(__file__).parent / "static" / name
        if not path.is_file():
            raise failure("bundle_not_installed", "The Spark task UI bundle is not installed.")
        return FileResponse(path, media_type=media_type, headers={"Cache-Control": "no-cache"})

    @app.get("/static/airflow.js")
    def bundle():
        return asset("airflow.js", "text/javascript")

    @app.get("/static/console.css")
    def stylesheet():
        return asset("console.css", "text/css")

    @app.get("/api/application", response_model=ConsoleApplication)
    async def application(request: Request, reference: ReferenceDep, user: GetUserDep):
        await authorize(request, user, reference, DagAccessEntity.TASK_INSTANCE)
        binding = await binding_for(request, reference)
        return await snapshot_for(request, binding)

    @app.get("/api/diagnostics", response_model=ConsoleDiagnostics)
    async def diagnostics(request: Request, reference: ReferenceDep, user: GetUserDep):
        await authorize(request, user, reference, DagAccessEntity.TASK_INSTANCE)
        binding = await binding_for(request, reference)
        response = await upstream(
            request,
            "GET",
            f"webserver/api/apps/{quote(binding.namespace, safe='')}/pods/"
            f"{quote(binding.driver_pod, safe='')}/diagnostics",
        )
        try:
            result = ConsoleDiagnostics.model_validate(response.json())
        except ValueError as error:
            raise failure(
                "invalid_response", "The Spark service returned invalid diagnostics.", 502
            ) from error
        application, report = result.application, result.diagnosis
        if (
            application.namespace != binding.namespace
            or application.driver_pod != binding.driver_pod
            or (binding.app_id and application.app_id != binding.app_id)
            or report.namespace != binding.namespace
            or report.driver_pod != binding.driver_pod
            or report.app_id != application.app_id
        ):
            raise failure(
                "binding_mismatch", "The Spark service returned diagnostics for another application.", 409
            )
        if application.source != "kubernetes" or not owns_task(binding, application):
            raise failure(
                "task_ownership_mismatch",
                "These diagnostics are not bound to the selected Airflow task.",
                409,
            )
        return result

    @app.get("/api/capabilities")
    async def capabilities(request: Request, reference: ReferenceDep, user: GetUserDep):
        await authorize(request, user, reference, DagAccessEntity.TASK_INSTANCE)
        binding = await binding_for(request, reference)
        application = await snapshot_for(request, binding)
        permissions = {"can_kill": False, "can_delete": False}
        if (
            settings.allow_writes
            and application.driver_logs
            and application.source == "kubernetes"
            and owns_task(binding, application)
        ):
            for name, method in (("can_kill", "PUT"), ("can_delete", "DELETE")):
                try:
                    await authorize(request, user, reference, DagAccessEntity.TASK_INSTANCE, method)
                except HTTPException as error:
                    if error.status_code != 403:
                        raise
                else:
                    permissions[name] = True
        return {
            "namespace": binding.namespace,
            **permissions,
            **external_links(
                settings, binding, application, history_enabled=await history_configured(request)
            ),
        }

    @app.get("/api/history", response_model=ConsoleHistory)
    async def history(request: Request, reference: ReferenceDep, user: GetUserDep):
        await authorize(request, user, reference, DagAccessEntity.TASK_INSTANCE)
        binding = await binding_for(request, reference)
        application = await snapshot_for(request, binding)
        if application.source != "kubernetes" or not owns_task(binding, application):
            return ConsoleHistory(items=[])
        return await task_history(request, application.app_id)

    async def change_application(request: Request, reference: TaskReference, user, method: str):
        require_same_origin(request)
        if request.headers.get("x-spark-on-k8s-action") != "true":
            raise failure("action_header_required", "The action confirmation header is required.", 403)
        await authorize(
            request, user, reference, DagAccessEntity.TASK_INSTANCE, "PUT" if method == "PATCH" else "DELETE"
        )
        if not settings.allow_writes:
            raise failure(
                "writes_disabled", "Spark application changes are disabled by the Airflow administrator.", 403
            )
        binding = await binding_for(request, reference)
        # Never act on a shared snapshot: verify the driver's current application first.
        application = await application_for(request, binding)
        path = verified_app_path(binding, application)
        try:
            await upstream(request, method, f"app/{path}")
        finally:
            if (key := snapshot_key(binding)) is not None:
                request.app.state.snapshots.invalidate(key)
        return Response(status_code=204)

    @app.post("/api/kill", status_code=204)
    async def kill(request: Request, reference: ReferenceDep, user: GetUserDep):
        return await change_application(request, reference, user, "PATCH")

    @app.delete("/api/application", status_code=204)
    async def delete(request: Request, reference: ReferenceDep, user: GetUserDep):
        return await change_application(request, reference, user, "DELETE")

    @app.websocket("/api/logs")
    async def logs(
        websocket: WebSocket, reference: ReferenceDep, tail: Annotated[int, Query(ge=-1, le=10000)] = 1000
    ):
        try:
            require_same_origin(websocket)
            request = Request({**websocket.scope, "type": "http"})
            user = await get_user(request, _oauth_token=None, bearer_credentials=await bearer_scheme(request))
            await authorize(request, user, reference, DagAccessEntity.TASK_LOGS)
            binding = await binding_for(websocket, reference)
            application = await snapshot_for(websocket, binding)
            path = verified_app_path(binding, application)
        except HTTPException:
            await websocket.close(code=1008)
            return
        base = settings.service_url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        url = f"{base}/webserver/ws/logs/{path}?{urlencode({'tail': tail})}"
        await websocket.accept()
        try:
            async with connect_upstream_logs(url) as source:

                async def forward():
                    async for message in source:
                        if isinstance(message, str):
                            await websocket.send_text(message)

                async def disconnected():
                    while (await websocket.receive())["type"] != "websocket.disconnect":
                        pass

                tasks = {asyncio.create_task(forward()), asyncio.create_task(disconnected())}
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except (WebSocketDisconnect, asyncio.CancelledError):
            raise
        except Exception as error:
            # Keep the Spark API's "logs unavailable" policy closure distinct from an
            # interruption; never forward the upstream reason text.
            received = getattr(error, "rcvd", None)
            await websocket.close(code=1008 if getattr(received, "code", None) == 1008 else 1011)
        else:
            if websocket.client_state.name != "DISCONNECTED":
                await websocket.close()

    return app
