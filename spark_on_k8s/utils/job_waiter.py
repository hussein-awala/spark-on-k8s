from __future__ import annotations

import logging

from kubernetes import client as k8s, watch
from kubernetes.client import ApiException

from spark_on_k8s.kubernetes_client import KubernetesClientManager


class SparkJobWaiter:
    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
        logger: logging.Logger | None = None,
    ):
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()
        self.logger = logger or logging.getLogger(__name__)

    def wait_for_job(self, *, namespace: str, pod_name: str):
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
