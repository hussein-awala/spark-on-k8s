from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Literal

from kubernetes import client as k8s

from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.app_manager import SparkAppManager
from spark_on_k8s.utils.configuration import Configuration
from spark_on_k8s.utils.logging_mixin import LoggingMixin
from spark_on_k8s.utils.types import NOTSET, ArgNotSet

# For Python 3.8 and 3.9 compatibility
KW_ONLY_DATACLASS = {"kw_only": True} if "kw_only" in dataclass.__kwdefaults__ else {}


def default_app_id_suffix() -> str:
    """Default function to generate a suffix for the application ID

    Returns:
        the current timestamp in the format %Y%m%d%H%M%S prefixed with a dash (e.g. -20240101123456)
    """
    return f"-{datetime.now().strftime('%Y%m%d%H%M%S')}"


class SparkAppWait(str, Enum):
    """Enum for the Spark app waiter options"""

    NO_WAIT = "no_wait"
    WAIT = "wait"
    LOG = "log"


@dataclass(**KW_ONLY_DATACLASS)
class PodResources:
    """Resources to request for the Spark driver and executors

    Attributes:
        cpu: Number of CPU cores to request
        memory: Amount of memory to request in MB
        memory_overhead: Amount of memory overhead to request in MB
    """

    cpu: int = 1
    memory: int = 1024
    memory_overhead: int = 512


@dataclass(**KW_ONLY_DATACLASS)
class ExecutorInstances:
    """Number of executors to request

    Attributes:
        min: Minimum number of executors. If provided, dynamic allocation is enabled
        max: Maximum number of executors. If provided, dynamic allocation is enabled
        initial: Initial number of executors. If max and min are not provided, defaults to 2,
            dynamic allocation will be disabled and the number of executors will be fixed.
    """

    min: int | None = None
    max: int | None = None
    initial: int | None = None


class SparkOnK8S(LoggingMixin):
    """Client for submitting Spark apps to Kubernetes

    Examples:
        >>> from spark_on_k8s.client import SparkOnK8S
        >>> spark = SparkOnK8S()
        >>> spark.submit_app(
        ...     image="husseinawala/spark:v3.5.0",
        ...     app_path="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
        ...     class_name="org.apache.spark.examples.SparkPi",
        ...     app_name="spark-pi",
        ...     app_arguments=["1000"],
        ...     namespace="spark",
        ...     service_account="spark",
        ...     app_waiter="log",
        ... )

    Args:
        k8s_client_manager: Kubernetes client manager to use for creating Kubernetes clients
        logger_name: Name of the logger to use for logging, defaults to "SparkOnK8S"
    """

    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
        logger_name: str | None = None,
    ):
        super().__init__(logger_name=logger_name or "SparkOnK8S")
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()
        self.app_manager = SparkAppManager(k8s_client_manager=self.k8s_client_manager)

    def submit_app(
        self,
        *,
        image: str | ArgNotSet = NOTSET,
        app_path: str | ArgNotSet = NOTSET,
        namespace: str | ArgNotSet = NOTSET,
        service_account: str | ArgNotSet = NOTSET,
        app_name: str | ArgNotSet = NOTSET,
        spark_conf: dict[str, str] | ArgNotSet = NOTSET,
        class_name: str | ArgNotSet = NOTSET,
        app_arguments: list[str] | ArgNotSet = NOTSET,
        app_id_suffix: Callable[[], str] | ArgNotSet = NOTSET,
        app_waiter: Literal["no_wait", "wait", "log"] | ArgNotSet = NOTSET,
        image_pull_policy: Literal["Always", "Never", "IfNotPresent"] | ArgNotSet = NOTSET,
        ui_reverse_proxy: bool | ArgNotSet = NOTSET,
        driver_resources: PodResources | ArgNotSet = NOTSET,
        executor_resources: PodResources | ArgNotSet = NOTSET,
        executor_instances: ExecutorInstances | ArgNotSet = NOTSET,
        should_print: bool | ArgNotSet = NOTSET,
        secret_values: dict[str, str] | ArgNotSet = NOTSET,
        driver_env_vars_from_secrets: list[str] | ArgNotSet = NOTSET,
        volumes: list[k8s.V1Volume] | ArgNotSet = NOTSET,
        driver_volume_mounts: list[k8s.V1VolumeMount] | ArgNotSet = NOTSET,
        executor_volume_mounts: list[k8s.V1VolumeMount] | ArgNotSet = NOTSET,
        driver_node_selector: dict[str, str] | ArgNotSet = NOTSET,
        executor_node_selector: dict[str, str] | ArgNotSet = NOTSET,
        driver_annotations: dict[str, str] | ArgNotSet = NOTSET,
        executor_annotations: dict[str, str] | ArgNotSet = NOTSET,
        driver_labels: dict[str, str] | ArgNotSet = NOTSET,
        executor_labels: dict[str, str] | ArgNotSet = NOTSET,
        driver_tolerations: list[k8s.V1Toleration] | ArgNotSet = NOTSET,
        executor_pod_template_path: str | ArgNotSet = NOTSET,
    ) -> str:
        """Submit a Spark app to Kubernetes

        Args:
            image: Docker image to use for the Spark driver and executors
            app_path: Path to the application JAR / Python / R file
            namespace: Kubernetes namespace to use, defaults to "default"
            service_account: Kubernetes service account to use for the Spark driver,
                defaults to "spark"
            app_name: Name of the Spark application, defaults to a generated name as
                `spark-app{app_id_suffix()}`
            spark_conf: Dictionary of spark configuration to pass to the application
            class_name: Name of the class to execute
            app_arguments: List of arguments to pass to the application
            app_id_suffix: Function to generate a suffix for the application ID, defaults to
                `default_app_id_suffix`
            app_waiter: How to wait for the app to finish. One of "no_wait", "wait", or "log"
            image_pull_policy: Image pull policy for the driver and executors, defaults to "IfNotPresent"
            ui_reverse_proxy: Whether to use a reverse proxy for the Spark UI, defaults to False
            driver_resources: Resources to request for the Spark driver. Defaults to 1 CPU core, 1Gi of
                memory and512Mi of memory overhead
            executor_resources: Resources to request for the Spark executors. Defaults to 1 CPU core, 1Gi
                of memory and 512Mi of memory overhead
            executor_instances: Number of executors to request. If max and min are not provided, dynamic
                allocation will be disabled and the number of executors will be fixed to initial or 2 if
                initial is not provided. If max or min or both are provided, dynamic allocation will be
                enabled and the number of executors will be between min and max (inclusive), and initial
                will be the initial number of executors with a default of 0.
            should_print: Whether to print logs instead of logging them, defaults to False
            secret_values: Dictionary of secret values to pass to the application as environment variables
            driver_env_vars_from_secrets: List of secret names to load environment variables from for
                the driver
            volumes: List of volumes to mount to the driver and/or executors
            driver_volume_mounts: List of volume mounts to mount to the driver
            executor_volume_mounts: List of volume mounts to mount to the executors
            driver_node_selector: Node selector for the driver
            executor_node_selector: Node selector for the executors
            driver_tolerations: List of tolerations for the driver
            executor_pod_template_path: Path to the executor pod template file

        Returns:
            Name of the Spark application pod
        """
        if image is NOTSET:
            if Configuration.SPARK_ON_K8S_DOCKER_IMAGE is None:
                raise ValueError(
                    "Docker image is not set."
                    "Please set the image argument or the environment variable SPARK_ON_K8S_DOCKER_IMAGE"
                )
            image = Configuration.SPARK_ON_K8S_DOCKER_IMAGE
        if app_path is NOTSET:
            if Configuration.SPARK_ON_K8S_APP_PATH is None:
                raise ValueError(
                    "Application path is not set."
                    "Please set the app_path argument or the environment variable SPARK_ON_K8S_APP_PATH"
                )
            app_path = Configuration.SPARK_ON_K8S_APP_PATH
        if namespace is NOTSET:
            namespace = Configuration.SPARK_ON_K8S_NAMESPACE
        if service_account is NOTSET:
            service_account = Configuration.SPARK_ON_K8S_SERVICE_ACCOUNT
        if app_name is NOTSET:
            app_name = Configuration.SPARK_ON_K8S_APP_NAME
        if spark_conf is NOTSET:
            spark_conf = Configuration.SPARK_ON_K8S_SPARK_CONF
        if class_name is NOTSET:
            class_name = Configuration.SPARK_ON_K8S_CLASS_NAME
        if app_arguments is NOTSET:
            app_arguments = Configuration.SPARK_ON_K8S_APP_ARGUMENTS
        if app_id_suffix is NOTSET:
            app_id_suffix = default_app_id_suffix
        if app_waiter is NOTSET:
            app_waiter = Configuration.SPARK_ON_K8S_APP_WAITER
        if image_pull_policy is NOTSET:
            image_pull_policy = Configuration.SPARK_ON_K8S_IMAGE_PULL_POLICY
        if ui_reverse_proxy is NOTSET:
            ui_reverse_proxy = Configuration.SPARK_ON_K8S_UI_REVERSE_PROXY
        if driver_resources is NOTSET:
            driver_resources = PodResources(
                cpu=Configuration.SPARK_ON_K8S_DRIVER_CPU,
                memory=Configuration.SPARK_ON_K8S_DRIVER_MEMORY,
                memory_overhead=Configuration.SPARK_ON_K8S_DRIVER_MEMORY_OVERHEAD,
            )
        if executor_resources is NOTSET:
            executor_resources = PodResources(
                cpu=Configuration.SPARK_ON_K8S_EXECUTOR_CPU,
                memory=Configuration.SPARK_ON_K8S_EXECUTOR_MEMORY,
                memory_overhead=Configuration.SPARK_ON_K8S_EXECUTOR_MEMORY_OVERHEAD,
            )
        if executor_instances is NOTSET:
            executor_instances = ExecutorInstances(
                min=Configuration.SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES,
                max=Configuration.SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES,
                initial=Configuration.SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES,
            )
            if (
                executor_instances.min is None
                and executor_instances.max is None
                and executor_instances.initial is None
            ):
                executor_instances.initial = 2
        app_name, app_id = self._parse_app_name_and_id(
            app_name=app_name, app_id_suffix=app_id_suffix, should_print=should_print
        )
        if secret_values is not NOTSET and secret_values:
            env_from_secrets = [app_id]
        else:
            secret_values = Configuration.SPARK_ON_K8S_SECRET_ENV_VAR
            env_from_secrets = [app_id] if secret_values else []
        if driver_env_vars_from_secrets is NOTSET:
            driver_env_vars_from_secrets = Configuration.SPARK_ON_K8S_DRIVER_ENV_VARS_FROM_SECRET
        if driver_env_vars_from_secrets:
            env_from_secrets.extend(driver_env_vars_from_secrets)
        if volumes is NOTSET or volumes is None:
            volumes = []
        if driver_volume_mounts is NOTSET or driver_volume_mounts is None:
            driver_volume_mounts = []
        if executor_volume_mounts is NOTSET or executor_volume_mounts is None:
            executor_volume_mounts = []
        if driver_node_selector is NOTSET or driver_node_selector is None:
            driver_node_selector = {}
        if executor_node_selector is NOTSET or executor_node_selector is None:
            executor_node_selector = {}
        if driver_annotations is NOTSET or driver_annotations is None:
            driver_annotations = {}
        if executor_annotations is NOTSET or executor_annotations is None:
            executor_annotations = {}
        if driver_labels is NOTSET or driver_labels is None:
            driver_labels = {}
        if executor_labels is NOTSET or executor_labels is None:
            executor_labels = {}
        if driver_tolerations is NOTSET or driver_tolerations is None:
            driver_tolerations = []
        if executor_pod_template_path is NOTSET or executor_pod_template_path is None:
            executor_pod_template_path = Configuration.SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH

        spark_conf = spark_conf or {}
        main_class_parameters = app_arguments or []

        driver_resources = driver_resources or PodResources()
        executor_resources = executor_resources or PodResources()
        executor_instances = executor_instances or ExecutorInstances(initial=2)

        basic_conf = {
            "spark.app.name": app_name,
            "spark.app.id": app_id,
            "spark.kubernetes.namespace": namespace,
            "spark.kubernetes.authenticate.driver.serviceAccountName": service_account,
            "spark.kubernetes.container.image": image,
            "spark.driver.host": app_id,
            "spark.driver.port": "7077",
            "spark.kubernetes.driver.pod.name": f"{app_id}-driver",
            "spark.kubernetes.executor.podNamePrefix": app_id,
            "spark.kubernetes.container.image.pullPolicy": image_pull_policy,
            "spark.driver.memory": f"{driver_resources.memory}m",
            "spark.executor.cores": f"{executor_resources.cpu}",
            "spark.executor.memory": f"{executor_resources.memory}m",
            "spark.executor.memoryOverhead": f"{executor_resources.memory_overhead}m",
            **self._executor_secrets_config(secret_values=secret_values, app_id=app_id),
        }
        extra_labels = {}
        if ui_reverse_proxy:
            basic_conf["spark.ui.proxyBase"] = f"/webserver/ui/{namespace}/{app_id}"
            basic_conf["spark.ui.proxyRedirectUri"] = "/"
            extra_labels["spark-ui-proxy"] = "true"
        if executor_instances.min is not None or executor_instances.max is not None:
            basic_conf["spark.dynamicAllocation.enabled"] = "true"
            basic_conf["spark.dynamicAllocation.shuffleTracking.enabled"] = "true"
            basic_conf["spark.dynamicAllocation.minExecutors"] = f"{executor_instances.min or 0}"
            if executor_instances.max is not None:
                basic_conf["spark.dynamicAllocation.maxExecutors"] = f"{executor_instances.max}"
            basic_conf["spark.dynamicAllocation.initialExecutors"] = f"{executor_instances.initial or 0}"
        else:
            basic_conf[
                "spark.executor.instances"
            ] = f"{executor_instances.initial if executor_instances.initial is not None else 2}"
        if executor_volume_mounts:
            basic_conf.update(
                self._executor_volumes_config(volumes=volumes, volume_mounts=executor_volume_mounts)
            )
        if executor_node_selector:
            basic_conf.update(self._executor_node_selector(node_selector=executor_node_selector))
        if executor_labels:
            basic_conf.update(self._executor_labels(labels=executor_labels))
        if executor_annotations:
            basic_conf.update(self._executor_annotations(annotations=executor_annotations))
        if executor_pod_template_path:
            basic_conf.update(self._executor_pod_template_path(executor_pod_template_path))
        driver_command_args = ["driver", "--master", "k8s://https://kubernetes.default.svc.cluster.local:443"]
        if class_name:
            driver_command_args.extend(["--class", class_name])
        driver_command_args.extend(
            self._spark_config_to_arguments({**basic_conf, **spark_conf}) + [app_path, *main_class_parameters]
        )
        pod = SparkAppManager.create_spark_pod_spec(
            app_name=app_name,
            app_id=app_id,
            image=image,
            image_pull_policy=image_pull_policy,
            namespace=namespace,
            args=driver_command_args,
            extra_labels={**extra_labels, **driver_labels},
            annotations=driver_annotations,
            pod_resources={
                "requests": {
                    "cpu": f"{driver_resources.cpu}",
                    "memory": f"{driver_resources.memory + driver_resources.memory_overhead}Mi",
                },
                "limits": {
                    "cpu": f"{driver_resources.cpu}",
                    "memory": f"{driver_resources.memory + driver_resources.memory_overhead}Mi",
                },
            },
            env_from_secrets=env_from_secrets,
            volumes=volumes,
            volume_mounts=driver_volume_mounts,
            node_selector=driver_node_selector,
            tolerations=driver_tolerations,
        )
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            if secret_values:
                application_secret = self.app_manager.create_secret_object(
                    app_name=app_name,
                    app_id=app_id,
                    secrets_values=secret_values,
                    namespace=namespace,
                )
                api.create_namespaced_secret(namespace=namespace, body=application_secret)
            pod = api.create_namespaced_pod(
                namespace=namespace,
                body=pod,
            )
            if secret_values:
                application_secret.metadata.owner_references = [
                    k8s.V1OwnerReference(
                        api_version="v1",
                        kind="Pod",
                        name=pod.metadata.name,
                        uid=pod.metadata.uid,
                    )
                ]
                api.patch_namespaced_secret(
                    namespace=namespace,
                    name=application_secret.metadata.name,
                    body=application_secret,
                )
            api.create_namespaced_service(
                namespace=namespace,
                body=SparkAppManager.create_headless_service_object(
                    app_name=app_name,
                    app_id=app_id,
                    namespace=namespace,
                    pod_owner_uid=pod.metadata.uid,
                    extra_labels=extra_labels,
                ),
            )
        if app_waiter == SparkAppWait.LOG:
            self.app_manager.stream_logs(
                namespace=namespace,
                pod_name=pod.metadata.name,
                should_print=should_print,
            )
        elif app_waiter == SparkAppWait.WAIT:
            self.app_manager.wait_for_app(
                namespace=namespace, pod_name=pod.metadata.name, should_print=should_print
            )
        return pod.metadata.name

    def _parse_app_name_and_id(
        self,
        *,
        app_name: str | None = None,
        app_id_suffix: Callable[[], str] = default_app_id_suffix,
        should_print: bool = False,
    ) -> tuple[str, str]:
        """Parse the application name and ID

        This function will generate a valid application name and ID from the provided application name.
            It will ensure that the application name and ID respect the Kubernetes naming conventions
            (e.g. no uppercase characters, no
        special characters, start with a letter, etc.), and they are not too long
            (less than 64 characters for service
        names and labels values).

        Args:
            app_name: Name of the Spark application
            app_id_suffix: Function to generate a suffix for the application ID,
                defaults to `default_app_id_suffix`
            should_print: Whether to print logs instead of logging them, defaults to False

        Returns:
            Tuple of the application name and ID
        """
        if not app_name:
            app_name = f"spark-app{app_id_suffix()}"
            app_id = app_name
        else:
            original_app_name = app_name
            # All to lowercase
            app_name = app_name.lower()
            app_id_suffix_str = app_id_suffix()
            if len(app_name) > (63 - len(app_id_suffix_str) + 1):
                app_name = app_name[: (63 - len(app_id_suffix_str)) + 1]
            # Replace all non-alphanumeric characters with dashes
            app_name = re.sub(r"[^0-9a-zA-Z]+", "-", app_name)
            # Remove leading non-alphabetic characters
            app_name = re.sub(r"^[^a-zA-Z]*", "", app_name)
            # Remove leading and trailing dashes
            app_name = re.sub(r"^-*", "", app_name)
            app_name = re.sub(r"-*$", "", app_name)
            app_id = app_name + app_id_suffix_str
            if app_name != original_app_name:
                self.log(
                    msg=(
                        f"Application name {original_app_name} is too long"
                        f" and will be truncated to {app_name}"
                    ),
                    level=logging.WARNING,
                    should_print=should_print,
                )
        return app_name, app_id

    @staticmethod
    def _value_to_str(value: Any) -> str:
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    @staticmethod
    def _spark_config_to_arguments(spark_conf: dict[str, str] | None) -> list[str]:
        """Convert Spark configuration to a list of arguments

        Args:
            spark_conf: Spark configuration dictionary

        Returns:
            List of arguments
        """
        if not spark_conf:
            return []
        args = []
        for key, value in spark_conf.items():
            args.extend(["--conf", f"{key}={SparkOnK8S._value_to_str(value)}"])
        return args

    @staticmethod
    def _executor_secrets_config(
        secret_values: dict[str, str] | None,
        app_id: str,
    ) -> dict[str, str]:
        """Spark configuration to load environment variables from the secret

        Args:
            secret_values: Secret values to pass to the application as environment variables
            app_id: Application ID

        Returns:
            Spark configuration dictionary
        """
        if not secret_values:
            return {}
        return {
            f"spark.kubernetes.executor.secretKeyRef.{secret_name}": f"{app_id}:{secret_name}"
            for secret_name in secret_values.keys()
        }

    @staticmethod
    def _executor_volumes_config(
        volumes: list[k8s.V1Volume] | None,
        volume_mounts: list[k8s.V1VolumeMount] | None,
    ) -> dict[str, str]:
        """Spark configuration to mount volumes to the executors

        Args:
            volumes: List of volumes to mount to the driver and/or executors
            volume_mounts: List of volume mounts to mount to the executors

        Returns:
            Spark configuration dictionary
        """
        if not volumes:
            return {}
        config = {}
        # https://spark.apache.org/docs/latest/running-on-kubernetes.html#using-kubernetes-volumes
        supported_volume_types = {
            "hostPath",
            "emptyDir",
            "nfs",
            "persistentVolumeClaim",
        }
        loaded_volumes = {}
        volumes_config = {}
        for volume in volumes:
            volume_name = volume.name
            volume_mapped_type: str | None = None
            volume_type_str: str | None = None
            for attr in k8s.V1Volume.attribute_map:
                if attr != "name" and getattr(volume, attr) is not None:
                    volume_mapped_type = k8s.V1Volume.attribute_map[attr]
                    volume_type_str = k8s.V1Volume.openapi_types[attr]
                    volume = getattr(volume, attr)
                    break
            volume_type = getattr(k8s, volume_type_str)
            if volume_mapped_type not in supported_volume_types:
                continue
            loaded_volumes[volume_name] = volume_mapped_type
            volumes_config[volume_name] = {}
            for attr in volume_type.attribute_map:
                if getattr(volume, attr) is not None:
                    option_name = volume_type.attribute_map[attr]
                    volumes_config[volume_name][
                        f"spark.kubernetes.executor.volumes.{volume_mapped_type}.{volume_name}.{option_name}"
                    ] = getattr(volume, attr)
        for volume_mount in volume_mounts:
            if volume_mount.name not in loaded_volumes:
                raise ValueError(
                    f"Volume {volume_mount.name} is not found in the volumes list or is not supported.\n"
                    "Please make sure to add the volume to the volumes list and use one of"
                    f" the supported types: {supported_volume_types}"
                )
            config.update(volumes_config[volume_mount.name])
            volume_config_prefix = (
                "spark.kubernetes.executor.volumes."
                f"{loaded_volumes[volume_mount.name]}.{volume_mount.name}.mount"
            )
            config[f"{volume_config_prefix}.path"] = volume_mount.mount_path
            if volume_mount.sub_path:
                config[f"{volume_config_prefix}.subPath"] = volume_mount.sub_path
            if volume_mount.read_only:
                config[f"{volume_config_prefix}.readOnly"] = True
        return config

    @staticmethod
    def _executor_node_selector(
        node_selector: dict[str, str] | None,
    ) -> dict[str, str]:
        """Spark configuration to set node selector for the executors

        Args:
            node_selector: Node selector for the executors

        Returns:
            Spark configuration dictionary
        """
        if not node_selector:
            return {}
        return {
            f"spark.kubernetes.executor.node.selector.{key}": value for key, value in node_selector.items()
        }

    @staticmethod
    def _executor_labels(
        labels: dict[str, str] | None,
    ) -> dict[str, str]:
        """Spark configuration to set labels for the executors

        Args:
            labels: Labels for the executors

        Returns:
            Spark configuration dictionary
        """
        if not labels:
            return {}
        return {f"spark.kubernetes.executor.label.{key}": value for key, value in labels.items()}

    @staticmethod
    def _executor_annotations(
        annotations: dict[str, str] | None,
    ) -> dict[str, str]:
        """Spark configuration to set annotations for the executors

        Args:
            annotations: Annotations for the executors

        Returns:
            Spark configuration dictionary
        """
        if not annotations:
            return {}
        return {f"spark.kubernetes.executor.annotation.{key}": value for key, value in annotations.items()}

    @staticmethod
    def _executor_pod_template_path(
        executor_pod_template_path: str | None,
    ) -> dict[str, str]:
        """Spark configuration to set the executor pod template file

        Args:
            executor_pod_template_path: Path to the executor pod template file

        Returns:
            Spark configuration dictionary
        """
        if not executor_pod_template_path:
            return {}
        return {"spark.kubernetes.executor.podTemplateFile": executor_pod_template_path}
