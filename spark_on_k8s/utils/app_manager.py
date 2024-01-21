from __future__ import annotations

import logging
from enum import Enum

from kubernetes import client as k8s, watch
from kubernetes.client import ApiException
from kubernetes.stream import stream

from spark_on_k8s.kubernetes_client import KubernetesClientManager


class SparkAppStatus(str, Enum):
    """Spark app status."""

    Pending = "Pending"
    Running = "Running"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Unknown = "Unknown"


def get_app_status(pod: k8s.V1Pod) -> SparkAppStatus:
    """Get app status."""
    if pod.status.phase == "Pending":
        return SparkAppStatus.Pending
    elif pod.status.phase == "Running":
        return SparkAppStatus.Running
    elif pod.status.phase == "Succeeded":
        return SparkAppStatus.Succeeded
    elif pod.status.phase == "Failed":
        return SparkAppStatus.Failed
    else:
        return SparkAppStatus.Unknown


class SparkAppManager:
    """Manage Spark apps on Kubernetes.

    Examples:
        >>> from spark_on_k8s.utils.app_manager import SparkAppManager
        >>> app_manager = SparkAppManager()
        >>> app_manager.stream_logs(
        ...     namespace="spark",
        ...     pod_name="20240114225118-driver",
        ...     print_logs=True,
        ... )

    Args:
        k8s_client_manager (KubernetesClientManager, optional): Kubernetes client manager. Defaults to None.
        logger (logging.Logger, optional): Logger. Defaults to None.
    """

    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
        logger: logging.Logger | None = None,
    ):
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()
        self.logger = logger or logging.getLogger(__name__)

    def app_status(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        client: k8s.CoreV1Api | None = None,
    ) -> SparkAppStatus:
        """Get app status.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name. Defaults to None.
            app_id (str): App ID. Defaults to None.
            client (k8s.CoreV1Api, optional): Kubernetes client. Defaults to None.

        Returns:
            SparkAppStatus: App status.
        """

        def _app_status(_client: k8s.CoreV1Api) -> SparkAppStatus:
            if pod_name is None and app_id is None:
                raise ValueError("Either pod_name or app_id must be specified")
            if pod_name is not None:
                _pod = _client.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
            else:
                _pod = _client.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"spark-app-id={app_id}",
                ).items[0]
            return get_app_status(_pod)

        if client is None:
            with self.k8s_client_manager.client() as client:
                api = k8s.CoreV1Api(client)
                return _app_status(api)
        return _app_status(client)

    def wait_for_app(self, *, namespace: str, pod_name: str | None = None, app_id: str | None = None):
        """Wait for a Spark app to finish.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
        """
        termination_statuses = {SparkAppStatus.Succeeded, SparkAppStatus.Failed, SparkAppStatus.Unknown}
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            while True:
                try:
                    status = self.app_status(
                        namespace=namespace, pod_name=pod_name, app_id=app_id, client=api
                    )
                    if status in termination_statuses:
                        break
                except ApiException as e:
                    if e.status == 404:
                        self.logger.info(f"Pod {pod_name} was deleted")
                        return
            self.logger.info(f"Pod {pod_name} finished with status {status.value}")

    def stream_logs(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        print_logs: bool = False,
    ):
        """Stream logs from a Spark app.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            print_logs (bool, optional): Whether to print log lines or use the logger to log them.
                Defaults to False.
        """
        if pod_name is None and app_id is None:
            raise ValueError("Either pod_name or app_id must be specified")
        if pod_name is None:
            with self.k8s_client_manager.client() as client:
                api = k8s.CoreV1Api(client)
                pods = api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"spark-app-id={app_id}",
                ).items
                if len(pods) == 0:
                    raise ValueError(f"No pods found for app {app_id}")
                pod_name = pods[0].metadata.name
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            while True:
                pod = api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
                if pod.status.phase != "Pending":
                    break
            watcher = watch.Watch()
            for line in watcher.stream(
                api.read_namespaced_pod_log,
                namespace=namespace,
                name=pod_name,
            ):
                if print_logs:
                    print(line)
                else:
                    self.logger.info(line)
            watcher.stop()

    def list_apps(self, namespace: str) -> list[str]:
        """List apps.

        Args:
            namespace (str): Namespace.

        Returns:
            list[str]: Spark apps in the namespace.
        """
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            return [
                pod.metadata.labels["spark-app-id"]
                for pod in api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector="spark-role=driver",
                ).items
            ]

    def kill_app(self, namespace: str, pod_name: str | None = None, app_id: str | None = None):
        """Kill an app.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
        """
        if pod_name is None and app_id is None:
            raise ValueError("Either pod_name or app_id must be specified")
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            if pod_name is None:
                pods = api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"spark-app-id={app_id}",
                ).items
                if len(pods) == 0:
                    raise ValueError(f"No pods found for app {app_id}")
                pod = pods[0]
            else:
                pod = api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
            container_name = pod.spec.containers[0].name
            if pod.status.phase != "Running":
                print(f"App is not running, it is {get_app_status(pod).value}")
                return
            if pod.status.container_statuses[0].state.terminated is not None:
                print("App is already terminated")
                return
            stream(
                api.connect_get_namespaced_pod_exec,
                pod.metadata.name,
                namespace,
                command=["/bin/sh", "-c", "kill 1"],
                container=container_name,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=False,
            )
        print(f"Killed app {app_id}")

    def delete_app(
        self, namespace: str, pod_name: str | None = None, app_id: str | None = None, force: bool = False
    ):
        """Delete an app.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            force (bool, optional): Whether to force delete the app. Defaults to False.
        """
        if pod_name is None and app_id is None:
            raise ValueError("Either pod_name or app_id must be specified")
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            if app_id:
                # we don't use `delete_collection_namespaced_pod` to know if the app exists or not
                pods = api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"spark-app-id={app_id}",
                ).items
                if len(pods) == 0:
                    raise ValueError(f"No pods found for app {app_id}")
                pod_name = pods[0].metadata.name
            api.delete_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                body=k8s.V1DeleteOptions(
                    grace_period_seconds=0 if force else None,
                    propagation_policy="Foreground",
                ),
            )
        print(f"Deleted app {app_id}")
