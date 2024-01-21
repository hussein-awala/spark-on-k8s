from __future__ import annotations

import logging
from enum import Enum

from kubernetes import client as k8s, watch
from kubernetes.client import ApiException

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


class SparkAppWaiter:
    """Wait for a Spark app to finish and stream its logs.

    Examples:
        >>> from spark_on_k8s.utils.app_waiter import SparkAppWaiter
        >>> app_waiter = SparkAppWaiter()
        >>> app_waiter.stream_logs(
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
                _pod = api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
            else:
                _pod = api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"spark-app-id={app_id}",
                ).items[0]
            return get_app_status(_pod)

        if client is None:
            with self.k8s_client_manager.client() as client:
                api = k8s.CoreV1Api(client)
                return _app_status(api)
        return _app_status(client)

    def wait_for_app(self, *, namespace: str, pod_name: str):
        """Wait for a Spark app to finish.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
        """
        termination_statuses = {SparkAppStatus.Succeeded, SparkAppStatus.Failed, SparkAppStatus.Unknown}
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            while True:
                try:
                    status = self.app_status(namespace=namespace, pod_name=pod_name, client=api)
                    if status in termination_statuses:
                        break
                except ApiException as e:
                    if e.status == 404:
                        self.logger.info(f"Pod {pod_name} was deleted")
                        return
            self.logger.info(f"Pod {pod_name} finished with status {status.value}")

    def stream_logs(self, *, namespace: str, pod_name: str, print_logs: bool = False):
        """Stream logs from a Spark app.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            print_logs (bool, optional): Whether to print log lines or use the logger to log them.
                Defaults to False.
        """
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
