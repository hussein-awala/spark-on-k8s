from __future__ import annotations

import asyncio
import logging

from kubernetes.stream import stream
from kubernetes_asyncio import client as k8s_async, watch
from kubernetes_asyncio.client import ApiException
from kubernetes_asyncio.stream import WsApiClient

from spark_on_k8s.k8s.async_client import KubernetesAsyncClientManager
from spark_on_k8s.utils.logging_mixin import LoggingMixin
from spark_on_k8s.utils.spark_app_status import SparkAppStatus, get_app_status


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
                _pod = (
                    await _client.list_namespaced_pod(
                        namespace=namespace,
                        label_selector=f"spark-app-id={app_id}",
                    )
                ).items[0]
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
    ):
        """Wait for a Spark app to finish asynchronously.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            poll_interval (float, optional): Poll interval in seconds. Defaults to 10.
            should_print (bool, optional): Whether to print logs instead of logging them.
        """
        termination_statuses = {SparkAppStatus.Succeeded, SparkAppStatus.Failed, SparkAppStatus.Unknown}
        async with self.k8s_client_manager.client() as client:
            api = k8s_async.CoreV1Api(client)
            while True:
                try:
                    status = await self.app_status(
                        namespace=namespace, pod_name=pod_name, app_id=app_id, client=api
                    )
                    if status in termination_statuses:
                        break
                except ApiException as e:
                    if e.status == 404:
                        self.log(
                            msg=f"Pod {pod_name} was deleted", level=logging.INFO, should_print=should_print
                        )
                        return
                self.log(
                    msg=f"Pod {pod_name} status is {status}, sleep {poll_interval}s",
                    level=logging.INFO,
                    should_print=should_print,
                )
                await asyncio.sleep(poll_interval)
            self.log(
                msg=f"Pod {pod_name} finished with status {status.value}",
                level=logging.INFO,
                should_print=should_print,
            )

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
                        label_selector=f"spark-app-id={app_id}",
                    )
                ).items
                if len(pods) == 0:
                    raise ValueError(f"No pods found for app {app_id}")
                pod_name = pods[0].metadata.name

        async with self.k8s_client_manager.client() as client:
            api = k8s_async.CoreV1Api(client)
            while True:
                pod = await api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
                if pod.status.phase != "Pending":
                    break

            watcher = watch.Watch()
            log_streamer = watcher.stream(
                api.read_namespaced_pod_log,
                namespace=namespace,
                name=pod_name,
                tail_lines=tail_lines if tail_lines > 0 else None,
                follow=True,
            )
            async for line in log_streamer:
                yield line
            watcher.stop()

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
                        label_selector=f"spark-app-id={app_id}",
                    )
                ).items
                if len(pods) == 0:
                    raise ValueError(f"No pods found for app {app_id}")
                pod = pods[0]
            else:
                pod = await api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
            container_name = pod.spec.containers[0].name
            if pod.status.phase != "Running":
                raise ValueError(f"Pod {pod.metadata.name} is not running")
            v1_ws = k8s_async.CoreV1Api(api_client=WsApiClient())
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
            await v1_ws.api_client.close()

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
            if app_id:
                # we don't use `delete_collection_namespaced_pod` to know if the app exists or not
                pods = (
                    await api.list_namespaced_pod(
                        namespace=namespace,
                        label_selector=f"spark-app-id={app_id}",
                    )
                ).items
                if len(pods) == 0:
                    raise ValueError(f"No pods found for app {app_id}")
                pod_name = pods[0].metadata.name
            await api.delete_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                body=k8s_async.V1DeleteOptions(
                    grace_period_seconds=0 if force else None,
                    propagation_policy="Foreground",
                ),
            )
