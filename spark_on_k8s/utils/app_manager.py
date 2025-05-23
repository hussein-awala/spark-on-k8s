from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Literal

from kubernetes import client as k8s, watch
from kubernetes.client import ApiException
from kubernetes.stream import stream

from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.logging_mixin import LoggingMixin
from spark_on_k8s.utils.spark_app_status import SparkAppStatus, get_app_status
from spark_on_k8s.utils.warnings import LongAppNameWarning, WarningCache

if TYPE_CHECKING:
    from spark_on_k8s.utils.types import ConfigMap, ConfigMapSource


class SparkAppManager(LoggingMixin):
    """Manage Spark apps on Kubernetes.

    Examples:
        >>> from spark_on_k8s.utils.app_manager import SparkAppManager
        >>> app_manager = SparkAppManager()
        >>> app_manager.stream_logs(
        ...     namespace="spark",
        ...     pod_name="20240114225118-driver",
        ...     should_print=True,
        ... )

    Args:
        k8s_client_manager (KubernetesClientManager, optional): Kubernetes client manager. Defaults to None.
        logger_name (str, optional): logger name. Defaults to "SparkAppManager".
    """

    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
        logger_name: str | None = None,
    ):
        super().__init__(logger_name=logger_name or "SparkAppManager")
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()

    @staticmethod
    def _get_pod_name(*, app_id: str) -> str:
        if len(app_id) > 56:
            WarningCache.warn(
                message="The used app name or app id suffix is too long,"
                " pod name will be truncated and may not be unique",
                category=LongAppNameWarning,
                stacklevel=2,
                warning_id=app_id,
            )
        return f"{app_id[:56]}-driver"

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

    def wait_for_app(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        poll_interval: float = 10,
        should_print: bool = False,
        startup_timeout: float = 0,
    ):
        """Wait for a Spark app to finish.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            poll_interval (float, optional): Poll interval in seconds. Defaults to 10.
            startup_timeout (float, optional): Timeout in seconds to wait for the app to start.
                Defaults to 0 (no timeout).
            should_print (bool, optional): Whether to print logs instead of logging them.
        """
        start_time = time.time()
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
                    if status == SparkAppStatus.Pending:
                        if startup_timeout and start_time + startup_timeout < time.time():
                            raise TimeoutError("App startup timeout")

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
                time.sleep(poll_interval)
            self.log(
                msg=f"Pod {pod_name} finished with status {status.value}",
                level=logging.INFO,
                should_print=should_print,
            )

    def stream_logs(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        startup_timeout: float = 0,
        should_print: bool = False,
    ):
        """Stream logs from a Spark app.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            startup_timeout (float, optional): Timeout in seconds to wait for the app to start.
                Defaults to 0 (no timeout).
            should_print (bool, optional): Whether to print logs instead of logging them.
        """
        start_time = time.time()
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
                if startup_timeout and start_time + startup_timeout < time.time():
                    raise TimeoutError("App startup timeout")
                time.sleep(5)
            watcher = watch.Watch()
            # Retry to connect to the pod log stream in case of ApiException
            # (e.g. TLS handshake error while communicating with kubeapi)
            # retry only if no logs were received yet
            nb_retries = 3
            retry_delay = 5
            first_attempt = True
            while True:
                try:
                    for line in watcher.stream(
                        api.read_namespaced_pod_log,
                        namespace=namespace,
                        name=pod_name,
                    ):
                        self.log(msg=line, level=logging.INFO, should_print=should_print)
                        first_attempt = False
                    break
                except ApiException as e:
                    if not first_attempt or nb_retries == 0:
                        raise e
                    self.log(
                        msg=f"Failed to connect to the pod log stream: {e}. Retrying in {retry_delay}s",
                        level=logging.WARNING,
                        should_print=should_print,
                    )
                    nb_retries -= 1
                    time.sleep(retry_delay)
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

    def kill_app(
        self,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        should_print: bool = False,
    ):
        """Kill an app.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            should_print (bool, optional): Whether to print logs instead of logging them.
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
                self.log(
                    f"App is not running, it is {get_app_status(pod).value}",
                    level=logging.INFO,
                    should_print=should_print,
                )
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
        annotations: dict[str, str] | None = None,
        env_from_secrets: list[str] | None = None,
        volumes: list[k8s.V1Volume] | None = None,
        volume_mounts: list[k8s.V1VolumeMount] | None = None,
        node_selector: dict[str, str] | None = None,
        tolerations: list[k8s.V1Toleration] | None = None,
        init_containers: list[k8s.V1Container] | None = None,
        host_aliases: list[k8s.V1HostAlias] | None = None,
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
            annotations: Dictionary of annotations to add to the pod template
            env_from_secrets: List of secrets to load environment variables from
            volumes: List of volumes to mount in the pod
            volume_mounts: List of volume mounts to mount in the container
            node_selector: Node selector to use for the pod
            tolerations: List of tolerations to use for the pod
            init_containers: List of init containers to run before the main container
            host_aliases: List of host aliases to add to the pod

        Returns:
            Pod template spec for the Spark application
        """
        pod_metadata = k8s.V1ObjectMeta(
            name=SparkAppManager._get_pod_name(app_id=app_id),
            namespace=namespace,
            labels=SparkAppManager.spark_app_labels(
                app_name=app_name,
                app_id=app_id,
                extra_labels=extra_labels,
            ),
            annotations=annotations,
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
                    env_from_secrets=env_from_secrets,
                    volume_mounts=volume_mounts,
                )
            ],
            volumes=volumes,
            node_selector=node_selector,
            tolerations=tolerations,
            init_containers=init_containers,
            host_aliases=host_aliases,
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
        env_from_secrets: list[str] | None = None,
        volume_mounts: list[k8s.V1VolumeMount] | None = None,
    ) -> k8s.V1Container:
        """Create a container spec for the Spark driver

        Args:
            image: Docker image to use for the Spark driver and executors
            container_name: Name of the container, defaults to "driver"
            env_variables: Dictionary of environment variables to pass to the container
            pod_resources: Dictionary of resources to request for the container
            args: List of arguments to pass to the container
            image_pull_policy: Image pull policy for the driver and executors, defaults to "IfNotPresent"
            env_from_secrets: List of secrets to load environment variables from
            volume_mounts: List of volume mounts to mount in the container

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
            env_from=[
                k8s.V1EnvFromSource(
                    secret_ref=k8s.V1SecretEnvSource(
                        name=secret_name,
                    ),
                )
                for secret_name in (env_from_secrets or [])
            ],
            volume_mounts=volume_mounts,
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
            extra_labels: Dictionary of extra labels to add to the labels

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
            extra_labels: Dictionary of extra labels to add to the service

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
                    name=SparkAppManager._get_pod_name(app_id=app_id),
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

    @staticmethod
    def create_secret_object(
        *,
        app_name: str,
        app_id: str,
        secrets_values: dict[str, str],
        namespace: str = "default",
    ) -> k8s.V1Secret:
        """Create a secret for a Spark application to store secrets values

        Args:
            app_name: Name of the Spark application
            app_id: ID of the Spark application
            secrets_values: Dictionary of secrets values
            namespace: Kubernetes namespace to use, defaults to "default"

        Returns:
            The created secret for the Spark application
        """
        return k8s.V1Secret(
            metadata=k8s.V1ObjectMeta(
                name=app_id,
                namespace=namespace,
                labels=SparkAppManager.spark_app_labels(
                    app_name=app_name,
                    app_id=app_id,
                ),
            ),
            string_data=secrets_values,
        )

    @staticmethod
    def create_configmap_objects(
        *,
        app_name: str,
        app_id: str,
        configmaps: list[ConfigMap],
        namespace: str = "default",
    ) -> list[k8s.V1ConfigMap]:
        """Create configmaps for a Spark application to mount in the driver
         and executors containers as volumes

        Args:
            app_name: Name of the Spark application
            app_id: ID of the Spark application
            configmaps: List of configmaps to create
            namespace: Kubernetes namespace to use, defaults to "default"

        Returns:
            The created configmaps objects for the Spark application
        """
        k8s_configmaps = []
        configmap_names = set()
        configmap_mount_paths = set()
        for index, configmap in enumerate(configmaps):
            configmap_name = configmap.get("name", f"{app_id}-{index}")
            if configmap_name in configmap_names:
                raise ValueError(f"Configmap name {configmap_name} is duplicated")
            configmap_mount_path = configmap["mount_path"]
            if configmap_mount_path in configmap_mount_paths:
                raise ValueError(f"Configmap mount path {configmap_mount_path} is duplicated")
            configmap_names.add(configmap_name)
            configmap_mount_paths.add(configmap_mount_path)
            data = {}
            source: ConfigMapSource
            for source in configmap["sources"]:
                if "text" in source:
                    data[source["name"]] = source["text"]
                elif "text_path" in source:
                    with open(source["text_path"]) as file:
                        data[source["name"]] = file.read()
            k8s_configmaps.append(
                k8s.V1ConfigMap(
                    metadata=k8s.V1ObjectMeta(
                        name=configmap_name,
                        namespace=namespace,
                        labels=SparkAppManager.spark_app_labels(
                            app_name=app_name,
                            app_id=app_id,
                        ),
                    ),
                    data=data,
                )
            )
        return k8s_configmaps
