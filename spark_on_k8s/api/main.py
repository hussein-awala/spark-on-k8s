from __future__ import annotations

from fastapi import FastAPI

from spark_on_k8s import __version__
from spark_on_k8s.api import AsyncHttpClientSingleton, KubernetesClientSingleton
from spark_on_k8s.api.jobs import router as jobs_router
from spark_on_k8s.api.ui import router as ui_router


async def on_start_up() -> None:
    await KubernetesClientSingleton.client()
    AsyncHttpClientSingleton.client()


async def on_shutdown() -> None:
    await (await KubernetesClientSingleton.client()).close()
    await AsyncHttpClientSingleton.client().aclose()


app = FastAPI(
    title="Spark on Kubernetes",
    description="Spark on Kubernetes API",
    version=__version__,
    on_startup=[on_start_up],
    on_shutdown=[on_shutdown],
)
app.include_router(jobs_router)
app.include_router(ui_router)


@app.get("/")
async def root():
    return {"message": "Welcome to Spark on Kubernetes!"}
