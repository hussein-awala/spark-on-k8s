from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml
from kubernetes import client as k8s

from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.planning import ApplicationPlan
from spark_on_k8s.utils.app_manager import SparkAppManager
from spark_on_k8s.utils.configuration import Configuration
from spark_on_k8s.utils.driver_pod_template import load_driver_pod_template, merge_driver_pod_template
from spark_on_k8s.utils.logging_mixin import LoggingMixin
from spark_on_k8s.utils.run import RunHandle, WaitDeadline, validate_max_runtime
from spark_on_k8s.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from spark_on_k8s.connect import ConnectProfile
    from spark_on_k8s.connect_server import ConnectServerConfig, ConnectServerHandle
    from spark_on_k8s.utils.types import ConfigMap


def default_app_id_suffix() -> str:
    """Default function to generate a suffix for the application ID

    Returns:
        the current timestamp in the format %Y%m%d%H%M%S prefixed with a dash (e.g. -20240101123456)
    """
    return f"-{datetime.now().strftime('%Y%m%d%H%M%S')}"


# Keep Enum.__str__ behavior for existing callers.
class SparkAppWait(str, Enum):  # noqa: UP042
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
        executor_allocation_ratio: Executor allocation ratio to use for dynamic allocation.
            Defaults to 1.0
        scheduler_backlog_timeout: Scheduler backlog timeout to use for dynamic allocation.
            Defaults to "1s"
        sustained_scheduler_backlog_timeout: Sustained scheduler backlog timeout to use for
            dynamic allocation. Defaults to sustained_scheduler_backlog_timeout.

    """

    min: int | None = None
    max: int | None = None
    initial: int | None = None
    executor_allocation_ratio: float = 1.0
    scheduler_backlog_timeout: str = "1s"
    sustained_scheduler_backlog_timeout: str | None = None


class SparkOnK8S(LoggingMixin):
    """Client for submitting Spark apps to Kubernetes

    Examples:
        >>> from spark_on_k8s.client import SparkOnK8S
        >>> spark = SparkOnK8S()
        >>> spark.submit_app(
        ...     image="apache/spark:3.5.9-scala2.12-java17-python3-ubuntu",
        ...     app_path="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.9.jar",
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
        self.last_submitted_app_id: str | None = None
        self.last_submitted_run: RunHandle | None = None
        self.last_submission_phase: str | None = None
        self.app_manager = SparkAppManager(k8s_client_manager=self.k8s_client_manager)

    def submit_run(self, **kwargs) -> RunHandle:
        """Submit using the same arguments as submit_app, returning a structured run identity."""
        if kwargs.get("dry_run"):
            raise ValueError("submit_run requires a real submission and does not support dry_run")
        self.submit_app(**kwargs)
        if self.last_submitted_run is None:
            raise RuntimeError("Submission completed without a run identity")
        return self.last_submitted_run

    def create_connect_server(self, config: ConnectServerConfig) -> ConnectServerHandle:
        """Create a private Spark Connect server; return identity before checking readiness."""
        from spark_on_k8s.connect_server import create_connect_server

        return create_connect_server(self, config)

    def connect_server_status(self, handle: ConnectServerHandle, *, timeout: float = 10) -> dict:
        """Observe exactly the recorded server resources without importing PySpark."""
        from spark_on_k8s.connect_server import connect_server_status

        return connect_server_status(self, handle, timeout=timeout)

    def list_connect_servers(self, namespace: str, *, timeout: float = 10) -> list[dict]:
        """List drivers labeled as Connect servers in one namespace; read-only and label-based."""
        from spark_on_k8s.connect_server import list_connect_servers

        return list_connect_servers(self, namespace, timeout=timeout)

    def wait_connect_server_ready(
        self,
        handle: ConnectServerHandle,
        *,
        profile: ConnectProfile,
        timeout: float = 60,
        poll_interval: float = 1,
    ) -> dict:
        """Verify a real Connect query, remote application identity and current resource UIDs."""
        from spark_on_k8s.connect_server import wait_connect_server_ready

        return wait_connect_server_ready(
            self, handle, profile=profile, timeout=timeout, poll_interval=poll_interval
        )

    def delete_connect_server(self, handle: ConnectServerHandle, *, timeout: float = 10) -> dict:
        """Request deletion only after validating all recorded server resource identities."""
        from spark_on_k8s.connect_server import delete_connect_server

        return delete_connect_server(self, handle, timeout=timeout)

    def submit_sql(
        self,
        *,
        sql: str | None = None,
        sql_file: str | Path | None = None,
        **submit_options: Any,
    ) -> str | ApplicationPlan:
        """Submit inline SQL or a local SQL file using a PySpark image.

        Exactly one of ``sql`` and ``sql_file`` is required. Other keyword arguments
        are passed to :meth:`submit_app`, including image, resources, namespace,
        service account, packages, and waiter options. SQL runs in statement order;
        the first failed statement fails the application. This is not a transaction.
        Set ``dry_run=True`` to return an offline :class:`ApplicationPlan` instead of submitting.
        """
        from spark_on_k8s.sql import prepare_sql_options

        return self.submit_app(**prepare_sql_options(sql=sql, sql_file=sql_file, **submit_options))

    def diagnose_app(
        self,
        *,
        namespace: str,
        app_id: str | None = None,
        pod_name: str | None = None,
        pod_limit: int = 20,
        event_limit: int = 100,
        timeout: float = 10,
    ):
        """Read bounded pod/event evidence without changing the application."""
        from spark_on_k8s.diagnostics import _limits, collect_diagnosis, validate_target

        if (app_id is None) == (pod_name is None):
            raise ValueError("Provide exactly one of app_id or pod_name")
        validate_target(namespace, app_id if app_id is not None else pod_name, label=app_id is not None)
        deadline = _limits(pod_limit, event_limit, timeout)
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            if pod_name is not None:
                driver = api.read_namespaced_pod(
                    namespace=namespace, name=pod_name, **deadline.request_options()
                )
            else:
                response = api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"spark-role=driver,spark-app-id={app_id}",
                    limit=2,
                    **deadline.request_options(),
                )
                if len(response.items) != 1:
                    raise ValueError("Expected one Spark driver; use pod_name to select an existing driver")
                driver = response.items[0]
            return collect_diagnosis(
                api,
                driver,
                namespace=namespace,
                pod_limit=pod_limit,
                event_limit=event_limit,
                timeout=deadline.remaining(),
            )

    def doctor(self, *, namespace: str, service_account: str = "spark", timeout: float = 10):
        """Check caller access and driver service-account existence without applying changes."""
        from spark_on_k8s.doctor import check_cluster

        with self.k8s_client_manager.client() as client:
            return check_cluster(
                client, namespace=namespace, service_account=service_account, timeout=timeout
            )

    def _process_executor_template(self, executor_template: str) -> str:
        """Process executor template string, reading from file if it's a file path.

        Args:
            executor_template: Either template content as string or path to template file

        Returns:
            Template content as string

        Raises:
            ValueError: If the resulting content is not valid YAML
        """
        # Try to read from file first, if that fails use the string as-is
        try:
            template_path = Path(executor_template).expanduser()
            content = template_path.read_text(encoding="utf-8")
        except (OSError, PermissionError):
            # Not a valid file path, use as content
            content = executor_template

        # Validate that the content is valid YAML and expected Kubernetes resource
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ValueError(f"Executor template contains invalid YAML: {e}") from e

        # Check if it's a proper Kubernetes resource structure
        if not isinstance(parsed, dict):
            raise ValueError("Executor template must be a Kubernetes resource (YAML object)")

        if parsed.get("kind") != "Pod":
            raise ValueError("Executor template must be a Kubernetes Pod resource (kind: Pod)")

        return content

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
        packages: list[str] | ArgNotSet = NOTSET,
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
        driver_ephemeral_configmaps_volumes: list[ConfigMap] | ArgNotSet = NOTSET,
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
        executor_pod_template: str | ArgNotSet = NOTSET,
        startup_timeout: int | ArgNotSet = NOTSET,
        wait_timeout: float | None = None,
        driver_init_containers: list[k8s.V1Container] | ArgNotSet = NOTSET,
        driver_host_aliases: list[k8s.V1HostAlias] | ArgNotSet = NOTSET,
        driver_pod_template: str | Path | Mapping[str, Any] | None = None,
        driver_container_name: str | None = None,
        max_runtime: int | None = None,
        dry_run: bool = False,
    ) -> str | ApplicationPlan:
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
            packages: List of maven coordinates of jars to include in the classpath
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
            driver_ephemeral_configmaps_volumes: List of ephemeral configmaps to create for the application
                driver
            driver_env_vars_from_secrets: List of secret names to load environment variables from for
                the driver
            volumes: List of volumes to mount to the driver and/or executors
            driver_volume_mounts: List of volume mounts to mount to the driver
            executor_volume_mounts: List of volume mounts to mount to the executors
            driver_node_selector: Node selector for the driver
            executor_node_selector: Node selector for the executors
            driver_tolerations: List of tolerations for the driver
            executor_pod_template_path: Path to the executor pod template file
            executor_pod_template: Executor pod template content as string or local file path. If provided,
                a ConfigMap will be created and mounted automatically
            startup_timeout: Timeout in seconds to wait for the application to start
            driver_init_containers: List of init containers to run before the driver starts
            driver_host_aliases: List of host aliases for the driver
            driver_pod_template: Driver Pod YAML text, a local file path, or a mapping.
                Generated identity and launch settings override the template; other fields are preserved.
            driver_container_name: Container to use as the driver in the template. Defaults to the
                container named "driver", or the sole container. Ambiguous templates are rejected.
            max_runtime: Maximum driver lifetime in whole seconds, set as the driver pod's
                ``activeDeadlineSeconds``. Kubernetes counts it from the pod's start, then stops the
                driver and marks it Failed with reason ``DeadlineExceeded``. Unset (None) by default.
            dry_run: Build an offline plan without opening a Kubernetes connection or creating resources.

        Returns:
            Name of the Spark application pod. The same identity is available as last_submitted_run.

        The optional wait_timeout bounds observation after resource creation, in seconds.
        It does not stop or delete the application. None preserves unlimited waiting.
        """
        if not dry_run:
            self.last_submitted_run = None
            self.last_submitted_app_id = None
            self.last_submission_phase = "preparing"
        WaitDeadline(wait_timeout)
        validate_max_runtime(max_runtime)
        if driver_container_name is not None and driver_pod_template is None:
            raise ValueError("driver_container_name requires driver_pod_template")
        template = load_driver_pod_template(driver_pod_template) if driver_pod_template is not None else None
        if image is NOTSET:
            if Configuration.SPARK_ON_K8S_DOCKER_IMAGE is None:
                raise ValueError(
                    "Docker image is not set. "
                    "Please set the image argument or the environment variable SPARK_ON_K8S_DOCKER_IMAGE"
                )
            image = Configuration.SPARK_ON_K8S_DOCKER_IMAGE
        if app_path is NOTSET:
            if Configuration.SPARK_ON_K8S_APP_PATH is None:
                raise ValueError(
                    "Application path is not set. "
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
        if packages is NOTSET:
            packages = (
                Configuration.SPARK_ON_K8S_PACKAGES.split(",") if Configuration.SPARK_ON_K8S_PACKAGES else []
            )
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
                executor_allocation_ratio=Configuration.SPARK_ON_K8S_EXECUTOR_ALLOCATION_RATIO,
                scheduler_backlog_timeout=Configuration.SPARK_ON_K8S_SCHEDULER_BACKLOG_TIMEOUT,
                sustained_scheduler_backlog_timeout=Configuration.SPARK_ON_K8S_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT,
            )
            if (
                executor_instances.min is None
                and executor_instances.max is None
                and executor_instances.initial is None
            ):
                executor_instances.initial = 2
        if should_print is NOTSET:
            should_print = False
        app_name, app_id = self._parse_app_name_and_id(
            app_name=app_name, app_id_suffix=app_id_suffix, should_print=should_print
        )
        # The driver Service is named after the app ID but is created after the driver Pod,
        # whose name accepts more characters; reject an unusable ID before creating anything.
        if len(app_id) > 63 or not re.fullmatch(r"[a-z](?:[a-z0-9-]*[a-z0-9])?", app_id):
            raise ValueError(
                f"Application ID {app_id!r} must be a DNS-1035 label of at most 63 characters "
                "(lowercase letters, digits and '-', starting with a letter); check app_id_suffix"
            )
        if secret_values is not NOTSET and secret_values is not None:
            env_from_secrets = [app_id] if secret_values else []
        else:
            secret_values = Configuration.SPARK_ON_K8S_SECRET_ENV_VAR
            env_from_secrets = [app_id] if secret_values else []
        if driver_ephemeral_configmaps_volumes is NOTSET:
            driver_ephemeral_configmaps_volumes = []
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
        volumes = list(volumes)
        driver_volume_mounts = list(driver_volume_mounts)
        executor_volume_mounts = list(executor_volume_mounts)
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
        if executor_pod_template is NOTSET or executor_pod_template is None:
            executor_pod_template = Configuration.SPARK_ON_K8S_EXECUTOR_TEMPLATE
        if startup_timeout is NOTSET:
            startup_timeout = Configuration.SPARK_ON_K8S_STARTUP_TIMEOUT
        if driver_init_containers is NOTSET:
            driver_init_containers = []
        if driver_host_aliases is NOTSET:
            driver_host_aliases = []

        spark_conf = spark_conf or {}
        driver_environment = self._driver_environment(spark_conf)
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
            "spark.kubernetes.driver.pod.name": SparkAppManager._get_pod_name(app_id=app_id),
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
            basic_conf["spark.dynamicAllocation.executorAllocationRatio"] = str(
                executor_instances.executor_allocation_ratio
            )
            basic_conf["spark.dynamicAllocation.schedulerBacklogTimeout"] = (
                executor_instances.scheduler_backlog_timeout
            )
            basic_conf["spark.dynamicAllocation.sustainedSchedulerBacklogTimeout"] = (
                executor_instances.sustained_scheduler_backlog_timeout
                or executor_instances.scheduler_backlog_timeout
            )
        else:
            basic_conf["spark.executor.instances"] = (
                f"{executor_instances.initial if executor_instances.initial is not None else 2}"
            )
        # Note: executor volume mounts will be processed after volumes are created
        if executor_node_selector:
            basic_conf.update(self._executor_node_selector(node_selector=executor_node_selector))
        if executor_labels:
            basic_conf.update(self._executor_labels(labels=executor_labels))
        if executor_annotations:
            basic_conf.update(self._executor_annotations(annotations=executor_annotations))
        # Handle executor template (either string content or local file path)
        executor_template_configmap = None
        if executor_pod_template and executor_pod_template_path:
            raise ValueError(
                "Both executor_pod_template and executor_pod_template_path are provided. "
                "Please provide only one of them."
            )

        if executor_pod_template:
            # Process the template (read from file if it's a path, otherwise use as content)
            template_content = self._process_executor_template(executor_pod_template)

            # ConfigMap names are reused as volume names, which are limited to 63 characters.
            executor_template_configmap = {
                "name": SparkAppManager._resource_name(app_id=app_id, suffix="-executor-template"),
                "mount_path": "/opt/spark/executor-template",
                "sources": [{"name": "executor-template.yaml", "text": template_content}],
            }

            # Set Spark configuration to use the mounted template
            basic_conf.update(
                self._executor_pod_template_path("/opt/spark/executor-template/executor-template.yaml")
            )

        elif executor_pod_template_path:
            # Handle external executor pod template path (S3, GCS, etc.)
            basic_conf.update(self._executor_pod_template_path(executor_pod_template_path))

        # Process executor volume mounts before building driver command args
        if executor_volume_mounts:
            basic_conf.update(
                self._executor_volumes_config(volumes=volumes, volume_mounts=executor_volume_mounts)
            )

        driver_command_args = ["driver", "--master", "k8s://https://kubernetes.default.svc.cluster.local:443"]
        if class_name:
            driver_command_args.extend(["--class", class_name])
        if packages:
            driver_command_args.extend(["--packages", ",".join(packages)])
        driver_command_args.extend(
            self._spark_config_to_arguments({**basic_conf, **spark_conf}) + [app_path, *main_class_parameters]
        )

        all_configmaps = []
        if driver_ephemeral_configmaps_volumes:
            all_configmaps.extend(driver_ephemeral_configmaps_volumes)

        # Add executor template ConfigMap if present
        if executor_template_configmap:
            all_configmaps.append(executor_template_configmap)

        if all_configmaps:
            application_configmaps_volumes = self.app_manager.create_configmap_objects(
                app_name=app_name,
                app_id=app_id,
                configmaps=all_configmaps,
                namespace=namespace,
            )
            for ind, configmap in enumerate(application_configmaps_volumes):
                volumes.append(
                    k8s.V1Volume(
                        name=configmap.metadata.name,
                        config_map=k8s.V1ConfigMapVolumeSource(name=configmap.metadata.name),
                    )
                )

                configmap_mount_path = all_configmaps[ind]["mount_path"]
                driver_volume_mounts.append(
                    k8s.V1VolumeMount(
                        name=configmap.metadata.name,
                        mount_path=configmap_mount_path,
                    )
                )
        else:
            application_configmaps_volumes = []

        pod = SparkAppManager.create_spark_pod_spec(
            app_name=app_name,
            app_id=app_id,
            image=image,
            image_pull_policy=image_pull_policy,
            namespace=namespace,
            service_account=service_account,
            args=driver_command_args,
            env_variables=driver_environment,
            extra_labels={
                **{key: value for key, value in driver_labels.items() if key != "spark-ui-proxy"},
                **extra_labels,
                "spark-app-name": app_name,
                "spark-app-id": app_id,
                "spark-role": "driver",
            },
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
            init_containers=driver_init_containers,
            host_aliases=driver_host_aliases,
            active_deadline_seconds=max_runtime,
        )
        if template is not None:
            pod = merge_driver_pod_template(pod, template, driver_container_name)
        if dry_run:
            objects = [("ConfigMap", item) for item in application_configmaps_volumes]
            if secret_values:
                objects.insert(
                    0,
                    (
                        "Secret",
                        self.app_manager.create_secret_object(
                            app_name=app_name,
                            app_id=app_id,
                            secrets_values=secret_values,
                            namespace=namespace,
                        ),
                    ),
                )
            objects.extend(
                [
                    ("Pod", pod),
                    (
                        "Service",
                        SparkAppManager.create_headless_service_object(
                            app_name=app_name, app_id=app_id, namespace=namespace, extra_labels=extra_labels
                        ),
                    ),
                ]
            )
            with k8s.ApiClient() as serializer:
                resources = tuple(
                    {**serializer.sanitize_for_serialization(item), "apiVersion": "v1", "kind": kind}
                    for kind, item in objects
                )
            pod_name = pod["metadata"]["name"] if isinstance(pod, dict) else pod.metadata.name
            from spark_on_k8s.resource_plan import resource_envelope

            planned_pod = next(item for item in resources if item["kind"] == "Pod")
            return ApplicationPlan(
                namespace,
                app_id,
                pod_name,
                resources,
                resource_envelope(planned_pod, {**basic_conf, **spark_conf}),
            )
        self.last_submission_phase = "creating_resources"
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            # Objects confirmed by this call; they have no owner until the driver exists.
            created: list[tuple[str, str, Any]] = []
            try:
                if secret_values:
                    application_secret = self.app_manager.create_secret_object(
                        app_name=app_name,
                        app_id=app_id,
                        secrets_values=secret_values,
                        namespace=namespace,
                    )
                    response = api.create_namespaced_secret(namespace=namespace, body=application_secret)
                    created.append(("secret", application_secret.metadata.name, response))
                for configmap in application_configmaps_volumes:
                    response = api.create_namespaced_config_map(namespace=namespace, body=configmap)
                    created.append(("config_map", configmap.metadata.name, response))
                pod = api.create_namespaced_pod(
                    namespace=namespace,
                    body=pod,
                )
            except k8s.ApiException as error:
                # Only a definitive rejection proves that no driver will use these objects.
                if isinstance(error.status, int) and 400 <= error.status < 500 and error.status != 408:
                    self._delete_unowned_resources(api, namespace, created, error, should_print)
                raise
            # Preserve the confirmed driver identity before later resource operations can fail.
            self.last_submitted_app_id = app_id
            self.last_submitted_run = RunHandle(
                namespace=namespace, app_id=app_id, driver_pod=pod.metadata.name, driver_uid=pod.metadata.uid
            )
            self.last_submission_phase = "driver_created"
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
            for configmap in application_configmaps_volumes:
                configmap.metadata.owner_references = [
                    k8s.V1OwnerReference(
                        api_version="v1",
                        kind="Pod",
                        name=pod.metadata.name,
                        uid=pod.metadata.uid,
                    )
                ]
                api.patch_namespaced_config_map(
                    namespace=namespace,
                    name=configmap.metadata.name,
                    body=configmap,
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
        self.last_submission_phase = "resources_created"
        deadline = WaitDeadline(wait_timeout)
        if app_waiter == SparkAppWait.LOG:
            self.app_manager.stream_logs(
                namespace=namespace,
                pod_name=pod.metadata.name,
                startup_timeout=startup_timeout,
                should_print=should_print,
                **({"timeout": deadline.remaining()} if wait_timeout is not None else {}),
            )
        if app_waiter == SparkAppWait.WAIT or (app_waiter == SparkAppWait.LOG and wait_timeout is not None):
            self.app_manager.wait_for_app(
                namespace=namespace,
                pod_name=pod.metadata.name,
                poll_interval=5,
                startup_timeout=startup_timeout,
                should_print=should_print,
                **({"timeout": deadline.remaining()} if wait_timeout is not None else {}),
            )
        return pod.metadata.name

    def cancel_run(self, run: RunHandle, *, force: bool = False) -> None:
        """Request deletion of exactly the recorded driver; does not wait for cleanup."""
        self.app_manager.cancel_run(run, force=force)

    def _delete_unowned_resources(
        self,
        api: k8s.CoreV1Api,
        namespace: str,
        created: list[tuple[str, str, Any]],
        error: Exception,
        should_print: bool,
    ) -> None:
        """Delete objects created by a submission whose driver was definitively rejected.

        Only objects whose creation response carried a UID are deleted, with that UID as a
        precondition so a replacement is never removed. Rollback failures are attached to the
        original error instead of replacing it.
        """
        for kind, name, response in created:
            description = f"{'Secret' if kind == 'secret' else 'ConfigMap'} {name}"
            uid = getattr(getattr(response, "metadata", None), "uid", None)
            if not isinstance(uid, str) or not uid:
                error.add_note(f"{description} was kept because its creation was not confirmed with a UID")
                continue
            try:
                getattr(api, f"delete_namespaced_{kind}")(
                    name=name,
                    namespace=namespace,
                    body=k8s.V1DeleteOptions(preconditions=k8s.V1Preconditions(uid=uid)),
                )
            except Exception as cleanup_error:
                if isinstance(cleanup_error, k8s.ApiException) and cleanup_error.status == 404:
                    continue
                reason = (
                    f"Kubernetes API {cleanup_error.status}"
                    if isinstance(cleanup_error, k8s.ApiException)
                    else type(cleanup_error).__name__
                )
                error.add_note(f"Could not delete {description} (uid {uid}) after the rejection: {reason}")
                self.log(
                    msg=f"Could not delete {description} created for the rejected driver: {reason}",
                    level=logging.WARNING,
                    should_print=should_print,
                )
            else:
                self.log(
                    msg=f"Deleted {description} created for the rejected driver",
                    level=logging.INFO,
                    should_print=should_print,
                )

    def plan_app(self, **kwargs) -> ApplicationPlan:
        """Render a deterministic offline preview using the same builder as submission."""
        kwargs.setdefault("app_id_suffix", lambda: "-preview")
        kwargs["dry_run"] = True
        kwargs["should_print"] = False
        result = self.submit_app(**kwargs)
        assert isinstance(result, ApplicationPlan)
        return result

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
            # Maximum length for pod labels and service names is 63 characters
            if len(app_name) > (63 - len(app_id_suffix_str)):
                app_name = app_name[: (63 - len(app_id_suffix_str))]
            # Replace all non-alphanumeric characters with dashes
            app_name = re.sub(r"[^0-9a-zA-Z]+", "-", app_name)
            # Remove leading non-alphabetic characters
            app_name = re.sub(r"^[^a-zA-Z]*", "", app_name)
            # Remove trailing dashes
            app_name = re.sub(r"-*$", "", app_name)
            # A name without letters would otherwise yield an empty or dash-prefixed ID.
            app_name = app_name or "spark-app"
            app_id = app_name + app_id_suffix_str
            if app_name != original_app_name:
                self.log(
                    msg=(
                        f"Application name {original_app_name!r} has been sanitized"
                        f" to {app_name!r} to comply with Kubernetes naming conventions"
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
    def _driver_environment(spark_conf: dict[str, Any]) -> dict[str, str]:
        """Apply driver environment properties before this client creates the Pod."""
        prefix = "spark.kubernetes.driverEnv."
        environment = {}
        for key, value in spark_conf.items():
            if not isinstance(key, str) or not key.startswith(prefix):
                continue
            name = key.removeprefix(prefix)
            if not name or any(ord(char) < 32 or ord(char) > 126 or char == "=" for char in name):
                raise ValueError(f"Invalid driver environment variable name in Spark property {key!r}")
            if name == "SPARK_DRIVER_BIND_ADDRESS":
                raise ValueError(
                    "SPARK_DRIVER_BIND_ADDRESS is managed and cannot be set via Spark properties"
                )
            if not isinstance(value, (str, bool, int, float)) or (
                isinstance(value, float) and not math.isfinite(value)
            ):
                raise ValueError(
                    f"Driver environment property {key!r} must have a string or finite scalar value"
                )
            rendered = SparkOnK8S._value_to_str(value)
            if "\0" in rendered:
                raise ValueError(f"Driver environment property {key!r} cannot contain NUL")
            environment[name] = rendered
        return environment

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
            "hostPath": {"path"},
            "emptyDir": {"medium", "sizeLimit"},
            "nfs": {"path", "server"},
            "persistentVolumeClaim": {"claimName"},
        }
        loaded_volumes = {}
        volumes_config = {}
        read_only_volumes = set()
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
            if volume_mapped_type not in supported_volume_types:
                continue
            volume_type = getattr(k8s, volume_type_str)
            loaded_volumes[volume_name] = volume_mapped_type
            if getattr(volume, "read_only", False):
                read_only_volumes.add(volume_name)
            volumes_config[volume_name] = {}
            for attr in volume_type.attribute_map:
                option_name = volume_type.attribute_map[attr]
                if (
                    option_name in supported_volume_types[volume_mapped_type]
                    and getattr(volume, attr) is not None
                ):
                    volumes_config[volume_name][
                        f"spark.kubernetes.executor.volumes.{volume_mapped_type}.{volume_name}.options.{option_name}"
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
            if volume_mount.read_only or volume_mount.name in read_only_volumes:
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
