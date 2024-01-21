from __future__ import annotations

import logging

from kubernetes import client as k8s, watch
from kubernetes.client import ApiException

from spark_on_k8s.kubernetes_client import KubernetesClientManager


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

    def wait_for_app(self, *, namespace: str, pod_name: str):
        """Wait for a Spark app to finish.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
        """
        termination_statuses = {"Succeeded", "Failed"}
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            while True:
                try:
                    pod = api.read_namespaced_pod(
                        namespace=namespace,
                        name=pod_name,
                    )
                except ApiException as e:
                    if e.status == 404:
                        self.logger.info(f"Pod {pod_name} was deleted")
                        return
                if pod.status.phase in termination_statuses:
                    break
            self.logger.info(f"Pod {pod_name} finished with status {pod.status.phase}")

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
