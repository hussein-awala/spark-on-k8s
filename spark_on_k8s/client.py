from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, Literal

from kubernetes import client as k8s

from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.app_manager import SparkAppManager
from spark_on_k8s.utils.logging_mixin import LoggingMixin


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


@dataclass(kw_only=True)
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


@dataclass(kw_only=True)
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
        image: str,
        app_path: str,
        namespace: str = "default",
        service_account: str = "spark",
        app_name: str | None = None,
        spark_conf: dict[str, str] | None = None,
        class_name: str | None = None,
        app_arguments: list[str] | None = None,
        app_id_suffix: Callable[[], str] = default_app_id_suffix,
        app_waiter: Literal["no_wait", "wait", "log"] = SparkAppWait.NO_WAIT,
        image_pull_policy: Literal["Always", "Never", "IfNotPresent"] = "IfNotPresent",
        ui_reverse_proxy: bool = False,
        driver_resources: PodResources | None = None,
        executor_resources: PodResources | None = None,
        executor_instances: ExecutorInstances | None = None,
        should_print: bool = False,
    ):
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
        """
        app_name, app_id = self._parse_app_name_and_id(
            app_name=app_name, app_id_suffix=app_id_suffix, should_print=should_print
        )

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
            extra_labels=extra_labels,
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
        )
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            pod = api.create_namespaced_pod(
                namespace=namespace,
                body=pod,
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
            args.extend(["--conf", f"{key}={value}"])
        return args