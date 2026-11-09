from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

from spark_on_k8s import __version__
from spark_on_k8s.api import AsyncHttpClientSingleton, KubernetesClientSingleton
from spark_on_k8s.api.app import router as app_router
from spark_on_k8s.api.apps import router as apps_router
from spark_on_k8s.api.console import router as console_router
from spark_on_k8s.api.namespace_scope import namespace_scope
from spark_on_k8s.api.remote_auth import RemoteAccessMiddleware
from spark_on_k8s.api.run_history import (
    get_history_settings,
    router as run_history_router,
    start_history,
    stop_history,
)
from spark_on_k8s.api.submission import router as submission_router
from spark_on_k8s.api.submission_policy import get_submission_settings
from spark_on_k8s.api.utils import install_error_handlers
from spark_on_k8s.api.webserver import router as webserver_router


async def on_start_up() -> None:
    # Invalid scope or remote configuration fails before the server accepts traffic.
    scope = namespace_scope()
    if (policy := get_submission_settings().policy) is not None:
        scope.require_configured(policy.namespaces, "The submission policy")
    if (history := get_history_settings()).enabled:
        scope.require_configured(history.namespaces, "SPARK_ON_K8S_RUN_HISTORY_NAMESPACES")
    await KubernetesClientSingleton.client()
    AsyncHttpClientSingleton.client()
    await start_history()


async def on_shutdown() -> None:
    await stop_history()
    await (await KubernetesClientSingleton.client()).close()
    await AsyncHttpClientSingleton.client().aclose()


app = FastAPI(
    title="Spark on Kubernetes",
    description="Spark on Kubernetes API",
    version=__version__,
    on_startup=[on_start_up],
    on_shutdown=[on_shutdown],
)
install_error_handlers(app)
app.add_middleware(RemoteAccessMiddleware)
app.include_router(submission_router)
app.include_router(run_history_router)
app.include_router(app_router)
app.include_router(apps_router)
app.include_router(webserver_router)
app.include_router(console_router)
app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).resolve().parent.absolute() / "webserver/static"),
    name="static",
)


@app.get("/", include_in_schema=False)
async def root():
    return {"message": "Welcome to Spark on Kubernetes!"}


@app.get("/health")
@app.head("/health", include_in_schema=False)
async def health():
    return {"status": "ok"}
