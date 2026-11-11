from __future__ import annotations

import contextlib
import re
import time
from dataclasses import replace
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from kubernetes.client import ApiException

from spark_on_k8s.airflow._compat import BaseOperator, get_current_context
from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink
from spark_on_k8s.airflow.postmortem import MAX_LOG_TAIL_LINES, describe_error, log_failure_postmortem
from spark_on_k8s.airflow.triggers import SparkOnK8STrigger
from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.sql import SQL_MOUNT_PATH, prepare_sql_configmap, reserves_sql_mount
from spark_on_k8s.utils.app_manager import SparkAppManager
from spark_on_k8s.utils.spark_app_status import SparkAppDeletedError, SparkAppStatus, get_app_status

if TYPE_CHECKING:
    from typing import Literal

    import jinja2
    from kubernetes import client as k8s

    from spark_on_k8s.airflow._compat import Context
    from spark_on_k8s.client import ExecutorInstances, PodResources
    from spark_on_k8s.utils.console_models import AirflowTaskContext
    from spark_on_k8s.utils.types import ConfigMap

# Characters outside a Kubernetes label value's alphabet.
_LABEL_VALUE_INVALID = re.compile(r"[^A-Za-z0-9_.-]")


def _readable_label_value(value: str) -> str:
    """Fit a display value into a label value; altered values may collide, so never select by one."""
    return _LABEL_VALUE_INVALID.sub("_", value)[:63].strip("_.-")


class _AirflowKubernetesClientManager(KubernetesClientManager):
    """A Kubernetes client manager for Airflow."""

    def __init__(self, kubernetes_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.kubernetes_conn_id = kubernetes_conn_id

    def create_client(self):
        from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

        k8s_hook = KubernetesHook(conn_id=self.kubernetes_conn_id)

        return k8s_hook.get_conn()


class OnFinishAction(StrEnum):
    KEEP = "keep"
    DELETE = "delete"
    KEEP_IF_FAILED = "keep_if_failed"


class OnKillAction(StrEnum):
    KEEP = "keep"
    DELETE = "delete"
    KILL = "kill"


class AppWaiter(StrEnum):
    NO_WAIT = "no_wait"
    WAIT = "wait"
    LOG = "log"


class SparkOnK8SOperator(BaseOperator):
    """Submit a Spark application on Kubernetes.

    Args:
        image (str): Spark application image.
        app_path (str): Path to the Spark application.
        namespace (str, optional): Kubernetes namespace. Defaults to "default".
        service_account (str, optional): Kubernetes service account. Defaults to "spark".
        app_name (str, optional): Spark application name. Defaults to None.
        app_id_suffix: A suffix for the application ID, defaults to current timestamp in
            the format %Y%m%d%H%M%S prefixed with a dash.
        spark_conf (dict[str, str], optional): Spark configuration. Defaults to None.
        class_name (str, optional): Spark application class name. Defaults to None.
        packages: List of maven coordinates of jars to include in the classpath. Defaults to None.
        app_arguments (list[str], optional): Spark application arguments. Defaults to None.
        app_waiter (AppWaiter, optional): Spark application waiter.
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
        driver_ephemeral_configmaps_volumes: List of ConfigMaps to mount as ephemeral volumes to the driver.
        driver_init_containers: List of init containers for the driver pod.
        driver_host_aliases: List of host aliases for the driver pod.
        driver_pod_template: Driver Pod YAML text, local file path, or mapping.
        driver_container_name: Driver container to select from the template.
        spark_on_k8s_service_url: URL of the Spark On K8S service. Defaults to None.
        kubernetes_conn_id (str, optional): Kubernetes connection ID. Defaults to
            "kubernetes_default".
        poll_interval (int, optional): Poll interval for checking the Spark application status.
            Defaults to 10.
        deferrable (bool, optional): Whether the operator is deferrable. Defaults to False.
        on_finish_action (OnFinishAction, optional): Action to take when the operator finishes.
            Defaults to "keep".
        on_kill_action (OnKillAction, optional): Action to take when the operator is killed.
            Defaults to "delete".
        failure_diagnostics (bool, optional): When the application does not succeed, or before
            on_kill deletes it, log a bounded diagnosis of the driver and executor pods and their
            events before any cleanup, and push a compact summary to the
            "spark_failure_diagnosis" XCom key (not from on_kill). Defaults to True.
        failure_log_tail_lines (int, optional): Also log the driver's last lines (0 to 1000) in that
            case, unless this attempt already streamed the driver log. 0 disables. Defaults to 100.
        startup_timeout (int, optional): Timeout for the Spark application to start.
            Defaults to 0 (no timeout).
        max_runtime (int, optional): Maximum driver lifetime in seconds, counted from the driver
            pod's start (Kubernetes activeDeadlineSeconds). Kubernetes then stops the driver, even if
            the task's worker was lost. Defaults to None (no limit).
        reattach_by_label (bool, optional): When task XCom does not identify a driver to adopt,
            find this task instance's earlier drivers by their task-key label and adopt a Pending
            or Running one instead of submitting another application. Defaults to True.
        reattach_succeeded (bool, optional): Also adopt a retained Succeeded driver of this task
            instance as the attempt's result instead of rerunning Spark. Defaults to False.
    """

    _XCOM_DRIVER_POD_NAMESPACE = "driver_pod_namespace"
    _XCOM_DRIVER_POD_NAME = "driver_pod_name"
    XCOM_SPARK_UI_LINK = "spark_ui_link"
    XCOM_FAILURE_DIAGNOSIS = "spark_failure_diagnosis"

    _driver_pod_name: str | None = None
    _spark_app_id: str | None = None
    _driver_log_streamed: bool = False

    operator_extra_links = (SparkOnK8SOperatorLink(),)

    template_fields = (
        "image",
        "app_path",
        "namespace",
        "service_account",
        "app_name",
        "app_id_suffix",
        "spark_conf",
        "class_name",
        "packages",
        "app_arguments",
        "app_waiter",
        "image_pull_policy",
        "driver_resources",
        "executor_resources",
        "driver_labels",
        "executor_labels",
        "driver_annotations",
        "executor_annotations",
        "executor_instances",
        "secret_values",
        "driver_env_vars_from_secrets",
        "executor_pod_template",
        "kubernetes_conn_id",
        "driver_pod_template",
        "driver_container_name",
    )

    def __init__(
        self,
        *,
        image: str,
        app_path: str,
        namespace: str = "default",
        service_account: str = "spark",
        app_name: str | None = None,
        app_id_suffix: str = None,
        spark_conf: dict[str, str] | None = None,
        class_name: str | None = None,
        packages: list[str] | None = None,
        app_arguments: list[str] | None = None,
        app_waiter: AppWaiter = AppWaiter.WAIT,
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
        driver_env_vars_from_secrets: list[str] | None = None,
        executor_pod_template: str | None = None,
        executor_pod_template_path: str | None = None,
        driver_ephemeral_configmaps_volumes: list[ConfigMap] | None = None,
        driver_init_containers: list[k8s.V1Container] | None = None,
        driver_host_aliases: list[k8s.V1HostAlias] | None = None,
        driver_pod_template: str | dict[str, Any] | None = None,
        driver_container_name: str | None = None,
        spark_on_k8s_service_url: str | None = None,
        kubernetes_conn_id: str = "kubernetes_default",
        poll_interval: int = 10,
        deferrable: bool = False,
        # TODO: Change to OnFinishAction.KEEP_IF_FAILED in the stable version
        on_finish_action: OnFinishAction = OnFinishAction.KEEP,
        on_kill_action: OnKillAction = OnKillAction.DELETE,
        failure_diagnostics: bool = True,
        failure_log_tail_lines: int = 100,
        startup_timeout: int = 0,
        max_runtime: int | None = None,
        reattach_by_label: bool = True,
        reattach_succeeded: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if (
            isinstance(failure_log_tail_lines, bool)
            or not isinstance(failure_log_tail_lines, int)
            or not 0 <= failure_log_tail_lines <= MAX_LOG_TAIL_LINES
        ):
            raise ValueError(f"failure_log_tail_lines must be an integer from 0 to {MAX_LOG_TAIL_LINES}")
        self.image = image
        self.app_path = app_path
        self.namespace = namespace
        self.service_account = service_account
        self.app_name = app_name
        self.app_id_suffix = app_id_suffix
        self.spark_conf = spark_conf
        self.class_name = class_name
        self.packages = packages
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
        self.driver_env_vars_from_secrets = driver_env_vars_from_secrets
        self.executor_pod_template = executor_pod_template
        self.driver_tolerations = driver_tolerations
        self.executor_pod_template_path = executor_pod_template_path
        self.driver_ephemeral_configmaps_volumes = driver_ephemeral_configmaps_volumes
        self.driver_init_containers = driver_init_containers
        self.driver_host_aliases = driver_host_aliases
        self.driver_pod_template = driver_pod_template
        self.driver_container_name = driver_container_name
        self.spark_on_k8s_service_url = spark_on_k8s_service_url
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable
        self.on_finish_action = on_finish_action
        self.on_kill_action = on_kill_action
        self.failure_diagnostics = failure_diagnostics
        self.failure_log_tail_lines = failure_log_tail_lines
        self.startup_timeout = startup_timeout
        self.max_runtime = max_runtime
        self.reattach_by_label = reattach_by_label
        self.reattach_succeeded = reattach_succeeded

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

    def _persist_pod_name(self, context: Context):
        context["ti"].xcom_push(key=self._XCOM_DRIVER_POD_NAMESPACE, value=self.namespace)
        context["ti"].xcom_push(key=self._XCOM_DRIVER_POD_NAME, value=self._driver_pod_name)

    def _restore_console_metadata(self, context: Context):
        from spark_on_k8s.airflow.ui_config import CONSOLE_XCOM_KEY

        if not context.get("ti"):
            return
        # Legacy tasks have no console record. Their existing links remain available.
        with contextlib.suppress(Exception):
            metadata = context["ti"].xcom_pull(
                key=CONSOLE_XCOM_KEY, task_ids=self.task_id, map_indexes=context["ti"].map_index
            )
            if (
                isinstance(metadata, dict)
                and metadata.get("version") == 1
                and metadata.get("namespace") == self.namespace
                and metadata.get("driver_pod") == self._driver_pod_name
                and isinstance(metadata.get("app_id"), str)
            ):
                self._spark_app_id = metadata["app_id"]

    def _persist_console_metadata(self, context: Context):
        from spark_on_k8s.airflow.ui_config import CONSOLE_XCOM_KEY

        try_number = getattr(context["ti"], "try_number", 1)
        context["ti"].xcom_push(
            key=CONSOLE_XCOM_KEY,
            value={
                "version": 1,
                "namespace": self.namespace,
                "driver_pod": self._driver_pod_name,
                "try_number": try_number if isinstance(try_number, int) else 1,
                "service_url": self.spark_on_k8s_service_url,
                **({"app_id": self._spark_app_id} if self._spark_app_id else {}),
            },
        )

    def _persist_spark_ui_link(self, context: Context):
        if self.spark_on_k8s_service_url:
            SparkOnK8SOperatorLink.persist_spark_ui_link(
                context,
                self,
                spark_on_k8s_service_url=self.spark_on_k8s_service_url,
                namespace=self.namespace,
                spark_app_id=self._spark_app_id or self._driver_pod_name[: -len("-driver")],
            )

    def _persist_spark_history_ui_link(self, context: Context):
        if self.spark_on_k8s_service_url:
            SparkOnK8SOperatorLink.persist_spark_history_ui_link(
                context,
                self,
                spark_on_k8s_service_url=self.spark_on_k8s_service_url,
                spark_app_id=self._spark_app_id or self._driver_pod_name[: -len("-driver")],
            )

    def _try_to_adopt_job(self, context: Context, spark_app_manager: SparkAppManager) -> bool:
        xcom_driver_namespace = context["ti"].xcom_pull(
            dag_id=context["ti"].dag_id,
            task_ids=context["ti"].task_id,
            map_indexes=context["ti"].map_index,
            key=self._XCOM_DRIVER_POD_NAMESPACE,
            include_prior_dates=False,
        )
        if not xcom_driver_namespace or xcom_driver_namespace != self.namespace:
            return False
        xcom_driver_pod_name = context["ti"].xcom_pull(
            dag_id=context["ti"].dag_id,
            task_ids=context["ti"].task_id,
            map_indexes=context["ti"].map_index,
            key=self._XCOM_DRIVER_POD_NAME,
            include_prior_dates=False,
        )
        if not xcom_driver_pod_name:
            return False
        try:
            app_status = spark_app_manager.app_status(
                namespace=xcom_driver_namespace,
                pod_name=xcom_driver_pod_name,
            )
        except ApiException as error:
            if error.status == 404:
                self.log.info(
                    "Previously recorded Spark driver %s/%s is absent", self.namespace, xcom_driver_pod_name
                )
                return False
            raise AirflowException(
                f"Cannot verify previously recorded Spark driver {self.namespace}/{xcom_driver_pod_name}; "
                "refusing to submit another application. Restore Kubernetes access before retrying."
            ) from error
        except Exception as error:
            raise AirflowException(
                f"Cannot verify previously recorded Spark driver {self.namespace}/{xcom_driver_pod_name}; "
                "refusing to submit another application. Restore Kubernetes access before retrying."
            ) from error
        if app_status in {SparkAppStatus.Pending, SparkAppStatus.Running} or (
            app_status == SparkAppStatus.Succeeded and self.reattach_succeeded
        ):
            self._driver_pod_name = xcom_driver_pod_name
            self._restore_console_metadata(context)
            self.log.info(
                "Reattaching to Spark driver %s/%s (%s)", self.namespace, xcom_driver_pod_name, app_status
            )
            return True
        if app_status in {SparkAppStatus.Succeeded, SparkAppStatus.Failed}:
            self.log.info(
                "Previously recorded Spark driver %s/%s is terminal (%s)",
                self.namespace,
                xcom_driver_pod_name,
                app_status,
            )
            return False
        raise AirflowException(
            f"Previously recorded Spark driver {self.namespace}/{xcom_driver_pod_name} "
            f"has indeterminate status {app_status}; refusing to submit another application."
        )

    def _task_identity(self, context: Context) -> AirflowTaskContext | None:
        from pydantic import ValidationError

        from spark_on_k8s.utils.console_models import AirflowTaskContext

        ti = context.get("ti")
        try:
            return AirflowTaskContext(
                dag_id=getattr(ti, "dag_id", None),
                run_id=getattr(ti, "run_id", None),
                task_id=getattr(ti, "task_id", None),
                map_index=getattr(ti, "map_index", -1),
                try_number=getattr(ti, "try_number", 1),
                region_id=getattr(ti, "region_id", None),
                region_index=getattr(ti, "region_index", None),
            )
        except ValidationError:
            # Legacy/manual operator invocations without task context remain supported; their
            # unbound pods get no task labels, and the task UI will refuse live actions on them.
            return None

    def _task_driver_annotations(self, context: Context) -> dict[str, str]:
        from spark_on_k8s.utils.console_models import AIRFLOW_TASK_ANNOTATION

        annotations = dict(self.driver_annotations or {})
        # This reserved annotation cannot be supplied through user annotations.
        annotations.pop(AIRFLOW_TASK_ANNOTATION, None)
        identity = self._task_identity(context)
        if identity is not None:
            annotations[AIRFLOW_TASK_ANNOTATION] = identity.model_dump_json(exclude_none=True)
        return annotations

    def _task_driver_labels(self, context: Context) -> dict[str, str]:
        from spark_on_k8s.utils.console_models import (
            AIRFLOW_DAG_ID_LABEL,
            AIRFLOW_TASK_ID_LABEL,
            AIRFLOW_TASK_KEY_LABEL,
        )

        reserved = {AIRFLOW_TASK_KEY_LABEL, AIRFLOW_DAG_ID_LABEL, AIRFLOW_TASK_ID_LABEL}
        # Like the task annotation, reserved task labels cannot be supplied through user labels.
        labels = {key: value for key, value in (self.driver_labels or {}).items() if key not in reserved}
        identity = self._task_identity(context)
        if identity is None:
            return labels
        labels[AIRFLOW_TASK_KEY_LABEL] = identity.task_key()
        readable = {AIRFLOW_DAG_ID_LABEL: identity.dag_id, AIRFLOW_TASK_ID_LABEL: identity.task_id}
        for key, value in readable.items():
            if label := _readable_label_value(value):
                labels[key] = label
        return labels

    def _list_task_drivers(self, spark_app_manager: SparkAppManager, task_key: str) -> list[k8s.V1Pod]:
        from kubernetes import client as k8s_client

        from spark_on_k8s.utils.console_models import AIRFLOW_TASK_KEY_LABEL

        selector = f"spark-role=driver,{AIRFLOW_TASK_KEY_LABEL}={task_key}"
        with spark_app_manager.k8s_client_manager.client() as client:
            api = k8s_client.CoreV1Api(client)
            return api.list_namespaced_pod(namespace=self.namespace, label_selector=selector).items

    def _try_to_adopt_labelled_job(self, context: Context, spark_app_manager: SparkAppManager) -> bool:
        """Adopt an earlier attempt's driver by its task-key label when XCom does not identify one."""
        identity = self._task_identity(context) if self.reattach_by_label else None
        if identity is None:
            return False
        try:
            pods = self._list_task_drivers(spark_app_manager, identity.task_key())
        except Exception as error:
            raise AirflowException(
                f"Cannot look up earlier Spark drivers of this task in namespace {self.namespace}; "
                "refusing to submit another application. Restore Kubernetes access before retrying."
            ) from error
        drivers: dict[SparkAppStatus, list[k8s.V1Pod]] = {}
        for pod in pods:
            if pod.metadata.deletion_timestamp is not None:
                # Deletion was already requested, for example after a startup timeout.
                self.log.info(
                    "Ignoring Spark driver %s/%s of this task: it is being deleted",
                    self.namespace,
                    pod.metadata.name,
                )
                continue
            status = get_app_status(pod) if pod.status else SparkAppStatus.Unknown
            drivers.setdefault(status, []).append(pod)

        def names(*statuses: SparkAppStatus) -> str:
            matches = (pod.metadata.name for status in statuses for pod in drivers.get(status, []))
            return ", ".join(sorted(matches))

        if SparkAppStatus.Unknown in drivers:
            raise AirflowException(
                f"Spark driver(s) {names(SparkAppStatus.Unknown)} of this task in namespace "
                f"{self.namespace} have indeterminate status; refusing to submit another application."
            )
        adoptable: tuple[SparkAppStatus, ...] = (SparkAppStatus.Pending, SparkAppStatus.Running)
        if not any(status in drivers for status in adoptable):
            adoptable = (SparkAppStatus.Succeeded,) if self.reattach_succeeded else ()
        candidates = [(status, pod) for status in adoptable for pod in drivers.get(status, [])]
        if len(candidates) > 1:
            raise AirflowException(
                f"Several Spark drivers of this task can be adopted in namespace {self.namespace} "
                f"({names(*adoptable)}); refusing to choose one or submit another application. "
                "Delete the extra drivers before retrying."
            )
        if candidates:
            status, pod = candidates[0]
            self._driver_pod_name = pod.metadata.name
            self._spark_app_id = (pod.metadata.labels or {}).get("spark-app-id")
            self.log.info(
                "Reattaching to Spark driver %s/%s (%s) found by its task-key label",
                self.namespace,
                self._driver_pod_name,
                status,
            )
            return True
        if SparkAppStatus.Succeeded in drivers:
            self.log.warning(
                "Spark driver(s) %s of this task already succeeded in namespace %s; submitting a new "
                "application because reattach_succeeded is False",
                names(SparkAppStatus.Succeeded),
                self.namespace,
            )
        if SparkAppStatus.Failed in drivers:
            self.log.info(
                "Not adopting failed Spark driver(s) %s of this task in namespace %s",
                names(SparkAppStatus.Failed),
                self.namespace,
            )
        return False

    def _submit_new_job(self, context: Context):
        from spark_on_k8s.client import PodResources, SparkOnK8S

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
            self.executor_instances = replace(
                self.executor_instances,
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
        submit_app_kwargs = {}
        if self.app_id_suffix:
            submit_app_kwargs["app_id_suffix"] = lambda: self.app_id_suffix
        if self.driver_init_containers is not None:
            submit_app_kwargs["driver_init_containers"] = self.driver_init_containers
        if self.driver_host_aliases is not None:
            submit_app_kwargs["driver_host_aliases"] = self.driver_host_aliases
        if self.driver_pod_template is not None:
            submit_app_kwargs["driver_pod_template"] = self.driver_pod_template
        if self.driver_container_name is not None:
            submit_app_kwargs["driver_container_name"] = self.driver_container_name
        if self.max_runtime is not None:
            submit_app_kwargs["max_runtime"] = self.max_runtime
        self._driver_pod_name = spark_client.submit_app(
            image=self.image,
            app_path=self.app_path,
            namespace=self.namespace,
            service_account=self.service_account,
            app_name=self.app_name,
            spark_conf=self.spark_conf,
            class_name=self.class_name,
            packages=self.packages,
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
            driver_labels=self._task_driver_labels(context),
            executor_labels=self.executor_labels,
            driver_annotations=self._task_driver_annotations(context),
            executor_annotations=self.executor_annotations,
            driver_env_vars_from_secrets=self.driver_env_vars_from_secrets,
            executor_pod_template=self.executor_pod_template,
            driver_tolerations=self.driver_tolerations,
            driver_ephemeral_configmaps_volumes=self.driver_ephemeral_configmaps_volumes,
            executor_pod_template_path=self.executor_pod_template_path,
            **submit_app_kwargs,
        )
        self._spark_app_id = spark_client.last_submitted_app_id

    def _clean_up(self, spark_app_manager: SparkAppManager, app_status: SparkAppStatus):
        if self.on_finish_action == OnFinishAction.KEEP or (
            self.on_finish_action == OnFinishAction.KEEP_IF_FAILED and app_status == SparkAppStatus.Failed
        ):
            return
        if self.on_finish_action == OnFinishAction.DELETE or (
            self.on_finish_action == OnFinishAction.KEEP_IF_FAILED and app_status != SparkAppStatus.Failed
        ):
            self.log.info("Deleting Spark application...")
            try:
                spark_app_manager.delete_app(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                )
            except ApiException as delete_error:
                if delete_error.status != 404:
                    raise
                # Deleted since its status was read, for example by pod garbage collection after
                # its node was removed: the requested end state already holds.
                self.log.info(
                    "Spark application %s/%s is already deleted.", self.namespace, self._driver_pod_name
                )
        else:
            self.log.error(
                f"Something went wrong: on_finish_action={self.on_finish_action}, app_status={app_status}"
            )

    def _log_failure_postmortem(
        self, spark_app_manager: SparkAppManager, status: str, context: Context | None = None
    ) -> None:
        """Log bounded failure evidence before cleanup can delete the driver and its logs.

        Best effort: it never raises, so it cannot change the task outcome or replace its error,
        and its reads share a short time budget. Never call it for a successful application.
        """
        # A streamed driver log is already in this task log; do not repeat its end.
        tail_lines = 0 if self._driver_log_streamed else self.failure_log_tail_lines
        if not self._driver_pod_name or not (self.failure_diagnostics or tail_lines):
            return
        try:
            summary = log_failure_postmortem(
                self.log,
                spark_app_manager.k8s_client_manager,
                namespace=self.namespace,
                pod_name=self._driver_pod_name,
                status=status,
                diagnose=self.failure_diagnostics,
                log_tail_lines=tail_lines,
            )
            if summary is not None and context and context.get("ti"):
                context["ti"].xcom_push(key=self.XCOM_FAILURE_DIAGNOSIS, value=summary)
        except Exception as error:
            self.log.warning(
                "Could not complete the Spark failure postmortem for %s/%s (%s); continuing.",
                self.namespace,
                self._driver_pod_name,
                describe_error(error),
            )

    def execute(self, context: Context):
        from spark_on_k8s.utils.app_manager import SparkAppManager

        # Templated and compared by value: an unknown value would silently skip or change waiting.
        if self.app_waiter not in set(AppWaiter):
            raise AirflowException(
                f"app_waiter must be one of {', '.join(AppWaiter)}; got {self.app_waiter!r}"
            )
        k8s_client_manager = _AirflowKubernetesClientManager(
            kubernetes_conn_id=self.kubernetes_conn_id,
        )
        spark_app_manager = SparkAppManager(
            k8s_client_manager=k8s_client_manager,
        )
        # Airflow clears task XCom when a new attempt starts, so a retry usually finds an
        # earlier attempt's driver only by its task-key label.
        if not self._try_to_adopt_job(context, spark_app_manager):
            if not self._try_to_adopt_labelled_job(context, spark_app_manager):
                self._submit_new_job(context)
            self._persist_pod_name(context)
        self._persist_spark_ui_link(context)
        self._persist_console_metadata(context)
        if self.app_waiter == AppWaiter.NO_WAIT:
            return
        if self.deferrable:
            self.defer(
                trigger=SparkOnK8STrigger(
                    driver_pod_name=self._driver_pod_name,
                    namespace=self.namespace,
                    kubernetes_conn_id=self.kubernetes_conn_id,
                    poll_interval=self.poll_interval,
                    startup_deadline=time.time() + self.startup_timeout if self.startup_timeout else None,
                ),
                method_name="execute_complete",
            )
        try:
            if self.app_waiter == AppWaiter.WAIT:
                spark_app_manager.wait_for_app(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                    poll_interval=self.poll_interval,
                    startup_timeout=self.startup_timeout,
                )
            elif self.app_waiter == AppWaiter.LOG:
                self._driver_log_streamed = True
                spark_app_manager.stream_logs(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                    startup_timeout=self.startup_timeout,
                )
                # wait for termination status
                spark_app_manager.wait_for_app(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                    poll_interval=1,
                )
        except SparkAppDeletedError as error:
            # Nothing is left to diagnose or clean up, and the outcome is unknown: fail rather than guess.
            raise AirflowException(f"{error}; failing the task") from None
        except TimeoutError:
            self._log_failure_postmortem(spark_app_manager, "startup_timeout", context)
            self.log.info("Deleting Spark application due to startup timeout...")
            failure = AirflowException("Spark application startup timeout exceeded")
            try:
                spark_app_manager.delete_app(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                )
            except Exception as cleanup_error:
                # As after a deferred wait: a failed deletion must not replace the timeout.
                raise failure from cleanup_error
            raise failure from None
        app_status = spark_app_manager.app_status(
            namespace=self.namespace,
            pod_name=self._driver_pod_name,
        )
        self._persist_spark_history_ui_link(context)
        if app_status == SparkAppStatus.Succeeded:
            self._clean_up_after_success(spark_app_manager, app_status)
            return app_status
        failure = AirflowException(f"The job finished with status: {app_status}")
        self._log_failure_postmortem(spark_app_manager, app_status, context)
        try:
            self._clean_up(spark_app_manager, app_status)
        except Exception as cleanup_error:
            # As after a deferred wait: a failed cleanup must not replace the Spark outcome.
            raise failure from cleanup_error
        raise failure

    def execute_complete(self, context: Context, event: dict, **kwargs):
        self.namespace = event["namespace"]
        self._driver_pod_name = event["pod_name"]
        self._restore_console_metadata(context)
        k8s_client_manager = _AirflowKubernetesClientManager(
            kubernetes_conn_id=self.kubernetes_conn_id,
        )
        spark_app_manager = SparkAppManager(
            k8s_client_manager=k8s_client_manager,
        )
        if event["status"] == "deleted":
            # Nothing is left to replay, diagnose or clean up, and the outcome is unknown.
            raise AirflowException(f"{event['error']}; failing the task")
        if event["status"] == "startup_timeout":
            self._log_failure_postmortem(spark_app_manager, "startup_timeout", context)
            self.log.info("Deleting Spark application due to startup timeout...")
            failure = AirflowException("Spark application startup timeout exceeded")
            try:
                spark_app_manager.delete_app(
                    namespace=self.namespace,
                    pod_name=self._driver_pod_name,
                )
            except Exception as cleanup_error:
                # Report the timeout; the trigger event is its only record.
                raise failure from cleanup_error
            raise failure
        if self.app_waiter == AppWaiter.LOG and event["status"] != "error":
            self._driver_log_streamed = True
            try:
                spark_app_manager.stream_logs(
                    namespace=event["namespace"],
                    pod_name=event["pod_name"],
                )
            except Exception as replay_error:
                # The trigger already reported the outcome: a failed replay must not replace it or
                # skip cleanup. The postmortem may still read the driver's last lines.
                self._driver_log_streamed = False
                self.log.warning(
                    "Could not replay the driver log of %s/%s (%s); continuing with the reported status %s.",
                    self.namespace,
                    self._driver_pod_name,
                    describe_error(replay_error),
                    event["status"],
                )
        self._persist_spark_history_ui_link(context)
        if event["status"] == "Succeeded":
            self._clean_up_after_success(spark_app_manager, event["status"])
            return event["status"]
        if event["status"] == "error":
            failure = AirflowException(
                f"SparkOnK8STrigger failed: with error: {event['error']}\nStacktrace: {event['stacktrace']}"
            )
        else:
            failure = AirflowException(f"The job finished with status: {event['status']}")
        self._log_failure_postmortem(spark_app_manager, event["status"], context)
        try:
            self._clean_up(spark_app_manager, event["status"])
        except Exception as cleanup_error:
            # A failed cleanup must not replace the trigger diagnostics or Spark outcome.
            raise failure from cleanup_error
        raise failure

    def _clean_up_after_success(self, spark_app_manager: SparkAppManager, app_status) -> None:
        try:
            self._clean_up(spark_app_manager, app_status)
        except Exception as cleanup_error:
            # Name the outcome: the bare cleanup error read as a Spark failure, and a retry reran the job.
            raise AirflowException(
                f"The job finished with status: {app_status}, but its cleanup failed; "
                "the application does not need to run again"
            ) from cleanup_error

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
                self._log_failure_postmortem(spark_app_manager, "killed")
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

            self._persist_spark_history_ui_link(get_current_context())


class SparkSqlOnK8SOperator(SparkOnK8SOperator):
    """Execute Spark SQL commands on Kubernetes.

    This operator passes the SQL commands to a Python script that runs them on Spark,
    that's why it requires a PySpark docker image. SQL is split into statements before
    submission, so semicolons inside quoted strings or comments remain intact. The
    internal runner receives a JSON statement list and does not require a SQL parser.

    Args:
        sql (str): SQL commands to execute.
    """

    template_fields = (
        "sql",
        *SparkOnK8SOperator.template_fields,
    )
    template_ext = (".sql",)
    template_fields_renderers = {
        "sql": "sql",
    }

    def __init__(self, sql: str, **kwargs):
        super().__init__(
            app_path="/configmap/spark_sql.py",
            app_arguments=["/configmap/queries.json"],
            **kwargs,
        )
        self.sql = sql

    def execute(self, context: Context):
        # Like the SDK and CLI SQL paths, keep user ConfigMaps and add the generated statements.
        configmaps = list(self.driver_ephemeral_configmaps_volumes or [])
        if any(reserves_sql_mount(configmap["mount_path"]) for configmap in configmaps):
            raise ValueError(f"SQL submission reserves the {SQL_MOUNT_PATH} mount path and its descendants")
        self.driver_ephemeral_configmaps_volumes = [
            *configmaps,
            prepare_sql_configmap(self.sql, allow_empty=True),
        ]
        return super().execute(context)
