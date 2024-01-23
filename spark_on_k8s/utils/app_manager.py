from __future__ import annotations

import logging
from enum import Enum
from typing import Literal

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

    @staticmethod
    def create_spark_pod_spec(
        *,
        app_name: str,
        app_id: str,
        image: str,
        namespace: str = "default",
        service_account: str = "spark",
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, dict[str, str]] | None = None,
        args: list[str] | None = None,
        image_pull_policy: Literal["Always", "Never", "IfNotPresent"] = "IfNotPresent",
        extra_labels: dict[str, str] | None = None,
    ) -> k8s.V1PodTemplateSpec:
        """Create a pod spec for a Spark application

        Args:
            app_name: Name of the Spark application
            app_id: ID of the Spark application
            image: Docker image to use for the Spark driver and executors
            namespace: Kubernetes namespace to use, defaults to "default"
            service_account: Kubernetes service account to use for the Spark driver, defaults to "spark"
            container_name: Name of the container, defaults to "driver"
            env_variables: Dictionary of environment variables to pass to the container
            pod_resources: Dictionary of resources to request for the container
            args: List of arguments to pass to the container
            image_pull_policy: Image pull policy for the driver and executors, defaults to "IfNotPresent"
            extra_labels: Dictionary of extra labels to add to the pod template

        Returns:
            Pod template spec for the Spark application
        """
        pod_metadata = k8s.V1ObjectMeta(
            name=f"{app_id}-driver",
            namespace=namespace,
            labels=SparkAppManager.spark_app_labels(
                app_name=app_name,
                app_id=app_id,
                extra_labels=extra_labels,
            ),
        )
        pod_spec = k8s.V1PodSpec(
            service_account_name=service_account,
            restart_policy="Never",
            containers=[
                SparkAppManager.create_driver_container(
                    image=image,
                    container_name=container_name,
                    env_variables=env_variables,
                    pod_resources=pod_resources,
                    args=args,
                    image_pull_policy=image_pull_policy,
                )
            ],
        )
        template = k8s.V1PodTemplateSpec(
            metadata=pod_metadata,
            spec=pod_spec,
        )
        return template

    @staticmethod
    def create_driver_container(
        *,
        image: str,
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, dict[str, str]] | None = None,
        args: list[str] | None = None,
        image_pull_policy: Literal["Always", "Never", "IfNotPresent"] = "IfNotPresent",
    ) -> k8s.V1Container:
        """Create a container spec for the Spark driver

        Args:
            image: Docker image to use for the Spark driver and executors
            container_name: Name of the container, defaults to "driver"
            env_variables: Dictionary of environment variables to pass to the container
            pod_resources: Dictionary of resources to request for the container
            args: List of arguments to pass to the container
            image_pull_policy: Image pull policy for the driver and executors, defaults to "IfNotPresent"

        Returns:
            Container spec for the Spark driver
        """
        return k8s.V1Container(
            name=container_name,
            image=image,
            image_pull_policy=image_pull_policy,
            env=[k8s.V1EnvVar(name=key, value=value) for key, value in (env_variables or {}).items()]
            + [
                k8s.V1EnvVar(
                    name="SPARK_DRIVER_BIND_ADDRESS",
                    value_from=k8s.V1EnvVarSource(
                        field_ref=k8s.V1ObjectFieldSelector(
                            field_path="status.podIP",
                        )
                    ),
                ),
            ],
            resources=k8s.V1ResourceRequirements(
                **(pod_resources or {}),
            ),
            args=args or [],
            ports=[
                k8s.V1ContainerPort(
                    container_port=7077,
                    name="driver-port",
                ),
                k8s.V1ContainerPort(
                    container_port=4040,
                    name="ui-port",
                ),
            ],
        )

    @staticmethod
    def spark_app_labels(
        *,
        app_name: str,
        app_id: str,
        extra_labels: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Create labels for a Spark application

        Args:
            app_name: Name of the Spark application
            app_id: ID of the Spark application

        Returns:
            Dictionary of labels for the Spark application resources
        """
        return {
            "spark-app-name": app_name,
            "spark-app-id": app_id,
            "spark-role": "driver",
            **(extra_labels or {}),
        }

    @staticmethod
    def create_headless_service_object(
        *,
        app_name: str,
        app_id: str,
        namespace: str = "default",
        pod_owner_uid: str | None = None,
        extra_labels: dict[str, str] | None = None,
    ) -> k8s.V1Service:
        """Create a headless service for a Spark application

        Args:
            app_name: Name of the Spark application
            app_id: ID of the Spark application
            namespace: Kubernetes namespace to use, defaults to "default"
            pod_owner_uid: UID of the pod to use as owner reference for the service

        Returns:
            The created headless service for the Spark application
        """
        labels = SparkAppManager.spark_app_labels(
            app_name=app_name,
            app_id=app_id,
            extra_labels=extra_labels,
        )
        owner = (
            [
                k8s.V1OwnerReference(
                    api_version="v1",
                    kind="Pod",
                    name=f"{app_id}-driver",
                    uid=pod_owner_uid,
                )
            ]
            if pod_owner_uid
            else None
        )
        return k8s.V1Service(
            metadata=k8s.V1ObjectMeta(
                name=app_id,
                labels=labels,
                namespace=namespace,
                owner_references=owner,
            ),
            spec=k8s.V1ServiceSpec(
                selector=labels,
                ports=[
                    k8s.V1ServicePort(
                        port=7077,
                        name="driver-port",
                    ),
                    k8s.V1ServicePort(
                        port=4040,
                        name="ui-port",
                    ),
                ],
                type="ClusterIP",
                cluster_ip="None",
            ),
        )
