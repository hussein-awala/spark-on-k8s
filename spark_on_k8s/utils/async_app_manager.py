from __future__ import annotations

import asyncio
import logging
import time

import aiohttp
from kubernetes.stream import stream
from kubernetes_asyncio import client as k8s_async, watch
from kubernetes_asyncio.client import ApiException
from kubernetes_asyncio.stream import WsApiClient

from spark_on_k8s.k8s.async_client import KubernetesAsyncClientManager
from spark_on_k8s.utils.logging_mixin import LoggingMixin
from spark_on_k8s.utils.spark_app_status import (
    TRANSIENT_STATUS_READ_ATTEMPTS,
    SparkAppStatus,
    driver_container_name,
    driver_pod_selector,
    get_app_status,
    is_transient_status_code,
    select_driver_pod,
)


class SparkAppStartupTimeoutError(TimeoutError):
    """A Spark driver remained Pending beyond its startup deadline."""


class AsyncSparkAppManager(LoggingMixin):
    """Manage Spark apps on Kubernetes asynchronously.

    Args:
        k8s_client_manager (KubernetesClientManager, optional): Kubernetes client manager. Defaults to None.
        logger_name (str, optional): logger name. Defaults to "SparkAppManager".
    """

    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesAsyncClientManager | None = None,
        logger_name: str | None = None,
    ):
        super().__init__(logger_name=logger_name or "SparkAppManager")
        self.k8s_client_manager = k8s_client_manager or KubernetesAsyncClientManager()

    async def app_status(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        client: k8s_async.CoreV1Api | None = None,
    ) -> SparkAppStatus:
        """Get app status asynchronously.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name. Defaults to None.
            app_id (str): App ID. Defaults to None.
            client (k8s.CoreV1Api, optional): Kubernetes client. Defaults to None.

        Returns:
            SparkAppStatus: App status.

        Raises:
            ValueError: If neither pod_name nor app_id is provided.
            ApiException: If no matching pod exists (404), or the Kubernetes request fails.
        """

        async def _app_status(_client: k8s_async.CoreV1Api) -> SparkAppStatus:
            if pod_name is None and app_id is None:
                raise ValueError("Either pod_name or app_id must be specified")
            if pod_name is not None:
                _pod = await _client.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
            else:
                pods = (
                    await _client.list_namespaced_pod(
                        namespace=namespace,
                        label_selector=f"spark-app-id={app_id}",
                    )
                ).items
                if not pods:
                    raise ApiException(status=404, reason=f"No pods found for app {app_id}")
                _pod = pods[0]
            return get_app_status(_pod)

        if client is None:
            async with self.k8s_client_manager.client() as client:
                api = k8s_async.CoreV1Api(client)
                return await _app_status(api)
        return await _app_status(client)

    async def wait_for_app(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        poll_interval: float = 10,
        should_print: bool = False,
        startup_deadline: float | None = None,
    ):
        """Wait for a Spark app to finish asynchronously.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            poll_interval (float, optional): Poll interval in seconds. Defaults to 10.
            should_print (bool, optional): Whether to print logs instead of logging them.
            startup_deadline (float, optional): Absolute Unix timestamp after which a Pending
                driver times out. Defaults to None (no startup timeout). Running applications
                are not limited by this deadline.

        Returns:
            The final status, or None when the driver was deleted.
        """
        termination_statuses = {SparkAppStatus.Succeeded, SparkAppStatus.Failed, SparkAppStatus.Unknown}
        target = f"Pod {pod_name}" if pod_name is not None else f"App {app_id}"
        async with self.k8s_client_manager.client() as client:
            api = k8s_async.CoreV1Api(client)
            failed_reads = 0
            while True:
                sleep_interval = poll_interval
                try:
                    status = await self.app_status(
                        namespace=namespace, pod_name=pod_name, app_id=app_id, client=api
                    )
                    failed_reads = 0
                    if status in termination_statuses:
                        break
                    if status == SparkAppStatus.Pending and startup_deadline is not None:
                        remaining = startup_deadline - time.time()
                        if remaining <= 0:
                            raise SparkAppStartupTimeoutError("App startup timeout")
                        sleep_interval = min(poll_interval, remaining)
                except (ApiException, aiohttp.ClientError) as e:
                    if isinstance(e, ApiException) and e.status == 404:
                        self.log(msg=f"{target} was deleted", level=logging.INFO, should_print=should_print)
                        return None
                    failed_reads += 1
                    transient = isinstance(e, aiohttp.ClientError) or is_transient_status_code(e.status)
                    if not transient or failed_reads >= TRANSIENT_STATUS_READ_ATTEMPTS:
                        raise
                    detail = f"HTTP {e.status}" if isinstance(e, ApiException) else type(e).__name__
                    self.log(
                        msg=f"Could not read the status of {target} ({detail}), retry in {sleep_interval}s",
                        level=logging.WARNING,
                        should_print=should_print,
                    )
                    await asyncio.sleep(sleep_interval)
                    continue
                self.log(
                    msg=f"{target} status is {status}, sleep {sleep_interval}s",
                    level=logging.INFO,
                    should_print=should_print,
                )
                await asyncio.sleep(sleep_interval)
            self.log(
                msg=f"{target} finished with status {status.value}",
                level=logging.INFO,
                should_print=should_print,
            )
            return status

    async def logs_streamer(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        tail_lines: int = -1,
    ):
        """Stream logs from a Spark app asynchronously.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            tail_lines (int, optional): Number of lines to tail. Defaults to -1.
        """
        if pod_name is None and app_id is None:
            raise ValueError("Either pod_name or app_id must be specified")
        if pod_name is None:
            async with self.k8s_client_manager.client() as client:
                api = k8s_async.CoreV1Api(client)
                pods = (
                    await api.list_namespaced_pod(
                        namespace=namespace,
                        label_selector=driver_pod_selector(app_id),
                    )
                ).items
                pod_name = select_driver_pod(pods, app_id).metadata.name

        async with self.k8s_client_manager.client() as client:
            api = k8s_async.CoreV1Api(client)
            while True:
                pod = await api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
                if pod.status.phase != "Pending":
                    break
                await asyncio.sleep(5)

            async with watch.Watch() as watcher:
                log_streamer = watcher.stream(
                    api.read_namespaced_pod_log,
                    namespace=namespace,
                    name=pod_name,
                    container=driver_container_name(pod),
                    tail_lines=tail_lines if tail_lines > 0 else None,
                    follow=True,
                )
                async for line in log_streamer:
                    yield line

    async def kill_app(
        self,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
    ):
        """Kill an app asynchronously.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
        """
        if pod_name is None and app_id is None:
            raise ValueError("Either pod_name or app_id must be specified")
        async with self.k8s_client_manager.client() as client:
            api = k8s_async.CoreV1Api(client)
            if pod_name is None:
                pods = (
                    await api.list_namespaced_pod(
                        namespace=namespace,
                        label_selector=driver_pod_selector(app_id),
                    )
                ).items
                pod = select_driver_pod(pods, app_id)
            else:
                pod = await api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
            container_name = driver_container_name(pod)
            if pod.status.phase != "Running":
                raise ValueError(f"Pod {pod.metadata.name} is not running")
            async with WsApiClient() as websocket_client:
                v1_ws = k8s_async.CoreV1Api(api_client=websocket_client)
                await stream(
                    v1_ws.connect_get_namespaced_pod_exec,
                    pod.metadata.name,
                    namespace,
                    command=["/bin/sh", "-c", "kill 1"],
                    container=container_name,
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                    _preload_content=True,
                )

    async def delete_app(
        self, namespace: str, pod_name: str | None = None, app_id: str | None = None, force: bool = False
    ):
        """Delete an app asynchronously.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            force (bool, optional): Whether to force delete the app. Defaults to False.
        """
        if pod_name is None and app_id is None:
            raise ValueError("Either pod_name or app_id must be specified")
        async with self.k8s_client_manager.client() as client:
            api = k8s_async.CoreV1Api(client)
            preconditions = None
            if app_id:
                # we don't use `delete_collection_namespaced_pod` to know if the app exists or not
                pods = (
                    await api.list_namespaced_pod(
                        namespace=namespace,
                        label_selector=driver_pod_selector(app_id),
                    )
                ).items
                pod = select_driver_pod(pods, app_id)
                pod_name = pod.metadata.name
                # Delete exactly the selected driver, not a replacement created after the lookup.
                preconditions = k8s_async.V1Preconditions(uid=pod.metadata.uid) if pod.metadata.uid else None
            await api.delete_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                body=k8s_async.V1DeleteOptions(
                    grace_period_seconds=0 if force else None,
                    propagation_policy="Foreground",
                    preconditions=preconditions,
                ),
            )
