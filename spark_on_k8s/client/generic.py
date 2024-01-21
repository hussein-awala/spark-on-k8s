from __future__ import annotations

import logging
import re
from datetime import datetime
from enum import Enum
from typing import Callable, Literal

from kubernetes import client as k8s

from spark_on_k8s.kubernetes_client import KubernetesClientManager
from spark_on_k8s.utils.job_waiter import SparkJobWaiter


def default_app_id_suffix() -> str:
    """Default function to generate a suffix for the application ID

    Returns:
        the current timestamp in the format %Y%m%d%H%M%S prefixed with a dash (e.g. -20240101123456)
    """
    return f"-{datetime.now().strftime('%Y%m%d%H%M%S')}"


class SparkJobWait(str, Enum):
    """Enum for the Spark job waiter options"""

    NO_WAIT = "no_wait"
    WAIT = "wait"
    PRINT = "print"
    LOG = "log"


class SparkOnK8S:
    """Client for submitting Spark jobs to Kubernetes

    Examples:
        >>> from spark_on_k8s.client.generic import SparkOnK8S
        >>> spark = SparkOnK8S()
        >>> spark.submit_job(
        ...     image="husseinawala/spark:v3.5.0",
        ...     app_path="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
        ...     class_name="org.apache.spark.examples.SparkPi",
        ...     app_name="spark-pi",
        ...     app_arguments=["1000"],
        ...     namespace="spark",
        ...     service_account="spark",
        ...     job_waiter="print",
        ... )

    Args:
        k8s_client_manager: Kubernetes client manager to use for creating Kubernetes clients
    """

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
    ):
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()
        self.job_waiter = SparkJobWaiter(k8s_client_manager=self.k8s_client_manager)

    def submit_job(
        self,
        image: str,
        app_path: str,
        namespace: str = "default",
        service_account: str = "spark",
        app_name: str | None = None,
        spark_conf: dict[str, str] | None = None,
        class_name: str | None = None,
        app_arguments: list[str] | None = None,
        app_id_suffix: Callable[[], str] = default_app_id_suffix,
        job_waiter: Literal["no_wait", "wait", "print", "log"] = SparkJobWait.NO_WAIT,
        image_pull_policy: Literal["Always", "Never", "IfNotPresent"] = "IfNotPresent",
        ui_reverse_proxy: bool = False,
    ):
        """Submit a Spark job to Kubernetes

        Args:
            image: Docker image to use for the Spark driver and executors
            app_path: Path to the application JAR / Python / R file
            namespace: Kubernetes namespace to use, defaults to "default"
            service_account: Kubernetes service account to use for the Spark driver,
                defaults to "spark"
            app_name: Name of the Spark application, defaults to a generated name as
                `spark-job{app_id_suffix()}`
            spark_conf: Dictionary of spark configuration to pass to the application
            class_name: Name of the class to execute
            app_arguments: List of arguments to pass to the application
            app_id_suffix: Function to generate a suffix for the application ID, defaults to
                `default_app_id_suffix`
            job_waiter: How to wait for the job to finish. One of "no_wait", "wait", "print" or "log"
            image_pull_policy: Image pull policy for the driver and executors, defaults to "IfNotPresent"
        """
        app_name, app_id = self._parse_app_name_and_id(app_name=app_name, app_id_suffix=app_id_suffix)

        spark_conf = spark_conf or {}
        main_class_parameters = app_arguments or []

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
        }
        extra_labels = {}
        if ui_reverse_proxy:
            basic_conf["spark.ui.proxyBase"] = f"/ui/{namespace}/{app_id}"
            basic_conf["spark.ui.proxyRedirectUri"] = "/"
            extra_labels["spark-ui-proxy"] = "true"
        driver_command_args = ["driver", "--master", "k8s://https://kubernetes.default.svc.cluster.local:443"]
        if class_name:
            driver_command_args.extend(["--class", class_name])
        driver_command_args.extend(
            self._spark_config_to_arguments({**basic_conf, **spark_conf}) + [app_path, *main_class_parameters]
        )
        pod = self._create_spark_pod_spec(
            app_name=app_name,
            app_id=app_id,
            image=image,
            image_pull_policy=image_pull_policy,
            namespace=namespace,
            args=driver_command_args,
            extra_labels=extra_labels,
        )
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            pod = api.create_namespaced_pod(
                namespace=namespace,
                body=pod,
            )
            api.create_namespaced_service(
                namespace=namespace,
                body=self._create_headless_service_object(
                    app_name=app_name,
                    app_id=app_id,
                    namespace=namespace,
                    pod_owner_uid=pod.metadata.uid,
                    extra_labels=extra_labels,
                ),
            )
        if job_waiter in (SparkJobWait.PRINT, SparkJobWait.LOG):
            self.job_waiter.stream_logs(
                namespace=namespace, pod_name=pod.metadata.name, print_logs=job_waiter == SparkJobWait.PRINT
            )
        elif job_waiter == SparkJobWait.WAIT:
            self.job_waiter.wait_for_job(namespace=namespace, pod_name=pod.metadata.name)

    def _parse_app_name_and_id(
        self, *, app_name: str | None = None, app_id_suffix: Callable[[], str] = default_app_id_suffix
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

        Returns:
            Tuple of the application name and ID
        """
        if not app_name:
            app_name = f"spark-job{app_id_suffix()}"
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
                self.logger.warning(
                    f"Application name {original_app_name} is too long and will be truncated to {app_name}"
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

    def _create_spark_pod_spec(
        self,
        *,
        app_name: str,
        app_id: str,
        image: str,
        namespace: str = "default",
        service_account: str = "spark",
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, str] | None = None,
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

        Returns:
            Pod template spec for the Spark application
        """
        pod_metadata = k8s.V1ObjectMeta(
            name=f"{app_id}-driver",
            namespace=namespace,
            labels=self.job_labels(
                app_name=app_name,
                app_id=app_id,
                extra_labels=extra_labels,
            ),
        )
        pod_spec = k8s.V1PodSpec(
            service_account_name=service_account,
            restart_policy="Never",
            containers=[
                self._create_driver_container(
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

    def _create_driver_container(
        self,
        *,
        image: str,
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, str] | None = None,
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

    def job_labels(
        self,
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
            "spark-app-selector": app_id,
            "spark-role": "driver",
            **(extra_labels or {}),
        }

    def _create_headless_service_object(
        self,
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
        labels = self.job_labels(
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
