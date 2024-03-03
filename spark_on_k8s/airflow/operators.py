from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from spark_on_k8s.airflow.triggers import SparkOnK8STrigger
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

if TYPE_CHECKING:
    from typing import Literal

    import jinja2
    from kubernetes import client as k8s

    from airflow.utils.context import Context
    from spark_on_k8s.client import ExecutorInstances, PodResources


class _AirflowKubernetesClientManager(KubernetesClientManager):
    """A Kubernetes client manager for Airflow."""

    def __init__(self, kubernetes_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.kubernetes_conn_id = kubernetes_conn_id

    def create_client(self):
        from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

        k8s_hook = KubernetesHook(conn_id=self.kubernetes_conn_id)

        return k8s_hook.get_conn()


class OnKillAction(str, Enum):
    KEEP = "keep"
    DELETE = "delete"
    KILL = "kill"


class SparkOnK8SOperator(BaseOperator):
    """Submit a Spark application on Kubernetes.

    Args:
        image (str): Spark application image.
        app_path (str): Path to the Spark application.
        namespace (str, optional): Kubernetes namespace. Defaults to "default".
        service_account (str, optional): Kubernetes service account. Defaults to "spark".
        app_name (str, optional): Spark application name. Defaults to None.
        spark_conf (dict[str, str], optional): Spark configuration. Defaults to None.
        class_name (str, optional): Spark application class name. Defaults to None.
        app_arguments (list[str], optional): Spark application arguments. Defaults to None.
        app_waiter (Literal["no_wait", "wait", "log"], optional): Spark application waiter.
            Defaults to "wait".
        image_pull_policy (Literal["Always", "Never", "IfNotPresent"], optional): Image pull policy.
            Defaults to "IfNotPresent".
        ui_reverse_proxy (bool, optional): Whether to use a reverse proxy for the Spark UI.
            Defaults to False.
        driver_resources (PodResources, optional): Driver pod resources. Defaults to None.
        executor_resources (PodResources, optional): Executor pod resources. Defaults to None.
        executor_instances (ExecutorInstances, optional): Executor instances. Defaults to None.
        secret_values (dict[str, str], optional): Dictionary of secret values to pass to the application
            as environment variables. Defaults to None.
        volumes: List of volumes to mount to the driver and/or executors.
        driver_volume_mounts: List of volume mounts to mount to the driver.
        executor_volume_mounts: List of volume mounts to mount to the executors.
        driver_node_selector: Node selector for the driver pod.
        executor_node_selector: Node selector for the executor pods.
        driver_tolerations: Tolerations for the driver pod.
        kubernetes_conn_id (str, optional): Kubernetes connection ID. Defaults to
            "kubernetes_default".
        poll_interval (int, optional): Poll interval for checking the Spark application status.
            Defaults to 10.
        deferrable (bool, optional): Whether the operator is deferrable. Defaults to False.
        on_kill_action (Literal["keep", "delete", "kill"], optional): Action to take when the
            operator is killed. Defaults to "delete".
        **kwargs: Other keyword arguments for BaseOperator.
    """

    _driver_pod_name: str | None = None

    template_fields = (
        "image",
        "app_path",
        "namespace",
        "service_account",
        "app_name",
        "spark_conf",
        "class_name",
        "app_arguments",
        "app_waiter",
        "image_pull_policy",
        "driver_resources",
        "executor_resources",
        "executor_instances",
        "secret_values",
        "kubernetes_conn_id",
    )

    def __init__(
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
        app_waiter: Literal["no_wait", "wait", "log"] = "wait",
        image_pull_policy: Literal["Always", "Never", "IfNotPresent"] = "IfNotPresent",
        ui_reverse_proxy: bool = False,
        driver_resources: PodResources | None = None,
        executor_resources: PodResources | None = None,
        executor_instances: ExecutorInstances | None = None,
        secret_values: dict[str, str] | None = None,
        volumes: list[k8s.V1Volume] | None = None,
        driver_volume_mounts: list[k8s.V1VolumeMount] | None = None,
        executor_volume_mounts: list[k8s.V1VolumeMount] | None = None,
        driver_node_selector: dict[str, str] | None = None,
        executor_node_selector: dict[str, str] | None = None,
        driver_labels: dict[str, str] | None = None,
        executor_labels: dict[str, str] | None = None,
        driver_annotations: dict[str, str] | None = None,
        executor_annotations: dict[str, str] | None = None,
        driver_tolerations: list[k8s.V1Toleration] | None = None,
        executor_pod_template_path: str | None = None,
        kubernetes_conn_id: str = "kubernetes_default",
        poll_interval: int = 10,
        deferrable: bool = False,
        on_kill_action: Literal["keep", "delete", "kill"] = OnKillAction.DELETE,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image = image
        self.app_path = app_path
        self.namespace = namespace
        self.service_account = service_account
        self.app_name = app_name
        self.spark_conf = spark_conf
        self.class_name = class_name
        self.app_arguments = app_arguments
        self.app_waiter = app_waiter
        self.image_pull_policy = image_pull_policy
        self.ui_reverse_proxy = ui_reverse_proxy
        self.driver_resources = driver_resources
        self.executor_resources = executor_resources
        self.executor_instances = executor_instances
        self.secret_values = secret_values
        self.volumes = volumes
        self.driver_volume_mounts = driver_volume_mounts
        self.executor_volume_mounts = executor_volume_mounts
        self.driver_node_selector = driver_node_selector
        self.executor_node_selector = executor_node_selector
        self.driver_labels = driver_labels
        self.executor_labels = executor_labels
        self.driver_annotations = driver_annotations
        self.executor_annotations = executor_annotations
        self.driver_tolerations = driver_tolerations
        self.executor_pod_template_path = executor_pod_template_path
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable
        self.on_kill_action = on_kill_action

    def _render_nested_template_fields(
        self,
        content: Any,
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set,
    ) -> None:
        """Render nested template fields."""
        from spark_on_k8s.client import ExecutorInstances, PodResources

        if id(content) not in seen_oids:
            template_fields: tuple | None

            if isinstance(content, PodResources):
                template_fields = ("cpu", "memory", "memory_overhead")
            elif isinstance(content, ExecutorInstances):
                template_fields = ("min", "max", "initial")
            else:
                template_fields = None

            if template_fields:
                seen_oids.add(id(content))
                self._do_render_template_fields(content, template_fields, context, jinja_env, seen_oids)
                return

        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    def execute(self, context):
        from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
        from spark_on_k8s.utils.app_manager import SparkAppManager

        # post-process template fields
        if self.driver_resources:
            self.driver_resources = PodResources(
                cpu=int(self.driver_resources.cpu) if self.driver_resources.cpu is not None else None,
                memory=int(self.driver_resources.memory)
                if self.driver_resources.memory is not None
                else None,
                memory_overhead=int(self.driver_resources.memory_overhead)
                if self.driver_resources.memory_overhead is not None
                else None,
            )
        if self.executor_resources:
            self.executor_resources = PodResources(
                cpu=int(self.executor_resources.cpu) if self.executor_resources.cpu is not None else None,
                memory=int(self.executor_resources.memory)
                if self.executor_resources.memory is not None
                else None,
                memory_overhead=int(self.executor_resources.memory_overhead),
            )
        if self.executor_instances:
            self.executor_instances = ExecutorInstances(
                min=int(self.executor_instances.min) if self.executor_instances.min is not None else None,
                max=int(self.executor_instances.max) if self.executor_instances.max is not None else None,
                initial=int(self.executor_instances.initial)
                if self.executor_instances.initial is not None
                else None,
            )

        k8s_client_manager = _AirflowKubernetesClientManager(
            kubernetes_conn_id=self.kubernetes_conn_id,
        )
        spark_client = SparkOnK8S(
            k8s_client_manager=k8s_client_manager,
        )
        self._driver_pod_name = spark_client.submit_app(
            image=self.image,
            app_path=self.app_path,
            namespace=self.namespace,
            service_account=self.service_account,
            app_name=self.app_name,
            spark_conf=self.spark_conf,
            class_name=self.class_name,
            app_arguments=self.app_arguments,
            app_waiter="no_wait",
            image_pull_policy=self.image_pull_policy,
            ui_reverse_proxy=self.ui_reverse_proxy,
            driver_resources=self.driver_resources,
            executor_resources=self.executor_resources,
            executor_instances=self.executor_instances,
            secret_values=self.secret_values,
            volumes=self.volumes,
            driver_volume_mounts=self.driver_volume_mounts,
            executor_volume_mounts=self.executor_volume_mounts,
            driver_node_selector=self.driver_node_selector,
            executor_node_selector=self.executor_node_selector,
            driver_labels=self.driver_labels,
            executor_labels=self.executor_labels,
            driver_annotations=self.driver_annotations,
            executor_annotations=self.executor_annotations,
            driver_tolerations=self.driver_tolerations,
            executor_pod_template_path=self.executor_pod_template_path,
        )
        if self.app_waiter == "no_wait":
            return
        if self.deferrable:
            self.defer(
                trigger=SparkOnK8STrigger(
                    driver_pod_name=self._driver_pod_name,
                    namespace=self.namespace,
                    kubernetes_conn_id=self.kubernetes_conn_id,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        k8s_client_manager = _AirflowKubernetesClientManager(
            kubernetes_conn_id=self.kubernetes_conn_id,
        )
        spark_app_manager = SparkAppManager(
            k8s_client_manager=k8s_client_manager,
        )
        if self.app_waiter == "wait":
            spark_app_manager.wait_for_app(
                namespace=self.namespace,
                pod_name=self._driver_pod_name,
                poll_interval=self.poll_interval,
            )
        elif self.app_waiter == "log":
            spark_app_manager.stream_logs(
                namespace=self.namespace,
                pod_name=self._driver_pod_name,
            )
        app_status = spark_app_manager.app_status(
            namespace=self.namespace,
            pod_name=self._driver_pod_name,
        )
        if app_status == "Succeeded":
            return app_status
        raise AirflowException(f"The job finished with status: {app_status}")

    def execute_complete(self, context: Context, event: dict, **kwargs):
        if self.app_waiter == "log":
            from spark_on_k8s.utils.app_manager import SparkAppManager

            k8s_client_manager = _AirflowKubernetesClientManager(
                kubernetes_conn_id=self.kubernetes_conn_id,
            )
            spark_app_manager = SparkAppManager(
                k8s_client_manager=k8s_client_manager,
            )
            spark_app_manager.stream_logs(
                namespace=event["namespace"],
                pod_name=event["pod_name"],
            )
        if event["status"] == "Succeeded":
            return event["status"]
        if event["status"] == "error":
            raise AirflowException(
                f"SparkOnK8STrigger failed: with error: {event['error']}\n"
                f"Stacktrace: {event['stacktrace']}"
            )
        raise AirflowException(f"The job finished with status: {event['status']}")

    def on_kill(self) -> None:
        if self.on_kill_action == OnKillAction.KEEP:
            return
        self.log.warning(self._driver_pod_name)
        if self._driver_pod_name:
            from spark_on_k8s.utils.app_manager import SparkAppManager

            k8s_client_manager = _AirflowKubernetesClientManager(
                kubernetes_conn_id=self.kubernetes_conn_id,
            )
            spark_app_manager = SparkAppManager(
                k8s_client_manager=k8s_client_manager,
            )
            if self.on_kill_action == OnKillAction.DELETE:
                self.log.info("Deleting Spark application...")
                spark_app_manager.delete_app(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                )
            elif self.on_kill_action == OnKillAction.KILL:
                self.log.info("Killing Spark application...")
                spark_app_manager.kill_app(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                )
            else:
                raise AirflowException(f"Invalid on_kill_action: {self.on_kill_action}")
