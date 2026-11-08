from __future__ import annotations

import traceback
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

from spark_on_k8s.k8s.async_client import KubernetesAsyncClientManager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _AirflowKubernetesAsyncClientManager(KubernetesAsyncClientManager):
    """A kubernetes async client manager for Airflow."""

    def __init__(self, kubernetes_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.kubernetes_conn_id = kubernetes_conn_id

    @asynccontextmanager
    async def client(self):
        from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook

        k8s_hook = AsyncKubernetesHook(conn_id=self.kubernetes_conn_id)
        async with k8s_hook.get_conn() as async_client:
            yield async_client


class SparkOnK8STrigger(BaseTrigger):
    """Watch a Spark application on Kubernetes.

    startup_deadline is an absolute Unix timestamp, preserved when a trigger restarts.
    It only limits the Pending phase; None disables the startup timeout.
    """

    def __init__(
        self,
        *,
        driver_pod_name: str,
        namespace: str = "default",
        kubernetes_conn_id: str = "kubernetes_default",
        poll_interval: int = 10,
        startup_deadline: float | None = None,
    ):
        super().__init__()
        self.driver_pod_name = driver_pod_name
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.startup_deadline = startup_deadline

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "spark_on_k8s.airflow.triggers.SparkOnK8STrigger",
            {
                "driver_pod_name": self.driver_pod_name,
                "namespace": self.namespace,
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "poll_interval": self.poll_interval,
                "startup_deadline": self.startup_deadline,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager, SparkAppStartupTimeoutError

        try:
            k8s_client_manager = _AirflowKubernetesAsyncClientManager(
                kubernetes_conn_id=self.kubernetes_conn_id,
            )
            async_spark_app_manager = AsyncSparkAppManager(
                k8s_client_manager=k8s_client_manager,
            )
            await async_spark_app_manager.wait_for_app(
                namespace=self.namespace,
                pod_name=self.driver_pod_name,
                poll_interval=self.poll_interval,
                startup_deadline=self.startup_deadline,
            )
            yield TriggerEvent(
                {
                    "namespace": self.namespace,
                    "pod_name": self.driver_pod_name,
                    "status": await async_spark_app_manager.app_status(
                        namespace=self.namespace,
                        pod_name=self.driver_pod_name,
                    ),
                }
            )
        except SparkAppStartupTimeoutError as e:
            yield TriggerEvent(
                {
                    "namespace": self.namespace,
                    "pod_name": self.driver_pod_name,
                    "status": "startup_timeout",
                    "error": str(e),
                }
            )
        except Exception as e:
            yield TriggerEvent(
                {
                    "namespace": self.namespace,
                    "pod_name": self.driver_pod_name,
                    "status": "error",
                    "error": str(e),
                    "stacktrace": traceback.format_exc(),
                }
            )
