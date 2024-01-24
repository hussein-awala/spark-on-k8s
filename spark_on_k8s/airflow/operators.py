from __future__ import annotations

from typing import TYPE_CHECKING

from airflow import AirflowException
from airflow.models import BaseOperator
from spark_on_k8s.airflow.triggers import SparkOnK8STrigger
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

if TYPE_CHECKING:
    from typing import Literal

    from airflow.utils.context import Context
    from spark_on_k8s.client import ExecutorInstances, PodResources


class _AirflowKubernetesClientManager(KubernetesClientManager):
    """A kubernetes client manager for Airflow."""

    def __init__(self, kubernetes_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.kubernetes_conn_id = kubernetes_conn_id

    def create_client(self):
        from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

        k8s_hook = KubernetesHook(conn_id=self.kubernetes_conn_id)

        return k8s_hook.get_conn()


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
        kubernetes_conn_id (str, optional): Kubernetes connection ID. Defaults to
            "kubernetes_default".
        poll_interval (int, optional): Poll interval for checking the Spark application status.
            Defaults to 10.
        deferrable (bool, optional): Whether the operator is deferrable. Defaults to False.
        **kwargs: Other keyword arguments for BaseOperator.
    """

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
        kubernetes_conn_id: str = "kubernetes_default",
        poll_interval: int = 10,
        deferrable: bool = False,
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
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable

    def execute(self, context):
        from spark_on_k8s.client import SparkOnK8S

        k8s_client_manager = _AirflowKubernetesClientManager(
            kubernetes_conn_id=self.kubernetes_conn_id,
        )
        spark_client = SparkOnK8S(
            k8s_client_manager=k8s_client_manager,
        )
        spark_client.submit_app(
            image=self.image,
            app_path=self.app_path,
            namespace=self.namespace,
            service_account=self.service_account,
            app_name=self.app_name,
            spark_conf=self.spark_conf,
            class_name=self.class_name,
            app_arguments=self.app_arguments,
            app_waiter=self.app_waiter if not self.deferrable else "no_wait",
            image_pull_policy=self.image_pull_policy,
            ui_reverse_proxy=self.ui_reverse_proxy,
            driver_resources=self.driver_resources,
            executor_resources=self.executor_resources,
            executor_instances=self.executor_instances,
        )
        if self.app_waiter == "no_wait":
            return
        if self.deferrable:
            self.defer(
                trigger=SparkOnK8STrigger(
                    driver_pod_name=self.app_name,
                    namespace=self.namespace,
                    kubernetes_conn_id=self.kubernetes_conn_id,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

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
            return
        if event["status"] == "error":
            raise AirflowException(
                "SparkOnK8STrigger failed: with error: {event['error']}\n"
                f"Stacktrace: {event['stacktrace']}"
            )
        raise AirflowException("The job finished with status: {event['status']}")
