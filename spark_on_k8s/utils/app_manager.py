from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import UTC, datetime
from threading import Event, Timer
from typing import TYPE_CHECKING, Any, Literal

from kubernetes import client as k8s, watch
from kubernetes.client import ApiException
from kubernetes.stream import stream

from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.logging_mixin import LoggingMixin
from spark_on_k8s.utils.run import RunHandle, WaitDeadline
from spark_on_k8s.utils.spark_app_status import (
    SparkAppStatus,
    driver_container_name,
    driver_pod_selector,
    get_app_status,
    select_driver_pod,
)
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
                " pod name will be shortened with a hash of the app id",
                category=LongAppNameWarning,
                stacklevel=2,
                warning_id=app_id,
            )
        # Plain truncation would drop the distinguishing suffix, so reruns could collide.
        return SparkAppManager._resource_name(app_id=app_id, suffix="-driver")

    @staticmethod
    def _resource_name(*, app_id: str, suffix: str) -> str:
        """Name an application resource within a 63-character DNS label, such as a Pod volume name."""
        name = f"{app_id}{suffix}"
        if len(name) <= 63:
            return name
        # Hash the full ID so truncating the readable prefix preserves uniqueness.
        digest = hashlib.sha256(app_id.encode("utf-8")).hexdigest()[:32]
        return f"{app_id[: 63 - len(digest) - len(suffix) - 1]}-{digest}{suffix}"

    def app_status(
        self,
        *,
        namespace: str,
        pod_name: str | None = None,
        app_id: str | None = None,
        client: k8s.CoreV1Api | None = None,
        request_timeout: float | None = None,
    ) -> SparkAppStatus:
        """Get app status.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name. Defaults to None.
            app_id (str): App ID. Defaults to None.
            client (k8s.CoreV1Api, optional): Kubernetes client. Defaults to None.

        Returns:
            SparkAppStatus: App status.

        Raises:
            ValueError: If neither pod_name nor app_id is provided.
            ApiException: If no matching pod exists (404), or the Kubernetes request fails.
        """

        def _app_status(_client: k8s.CoreV1Api) -> SparkAppStatus:
            if pod_name is None and app_id is None:
                raise ValueError("Either pod_name or app_id must be specified")
            if pod_name is not None:
                _pod = _client.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                    **({"_request_timeout": request_timeout} if request_timeout is not None else {}),
                )
            else:
                pods = _client.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"spark-app-id={app_id}",
                    **({"_request_timeout": request_timeout} if request_timeout is not None else {}),
                ).items
                if not pods:
                    raise ApiException(status=404, reason=f"No pods found for app {app_id}")
                _pod = pods[0]
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
        timeout: float | None = None,
    ):
        """Wait for a Spark app to finish.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            poll_interval (float, optional): Poll interval in seconds. Defaults to 10.
            startup_timeout (float, optional): Timeout in seconds to wait for the app to start.
                Defaults to 0 (no timeout).
            timeout (float, optional): Overall observation deadline in seconds, including running jobs.
                None disables the deadline. Expiration never stops or deletes the application.
            should_print (bool, optional): Whether to print logs instead of logging them.
        """
        deadline = WaitDeadline(timeout)
        start_time = time.time()
        termination_statuses = {SparkAppStatus.Succeeded, SparkAppStatus.Failed, SparkAppStatus.Unknown}
        target = f"Pod {pod_name}" if pod_name is not None else f"App {app_id}"
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            while True:
                try:
                    status = self.app_status(
                        namespace=namespace,
                        pod_name=pod_name,
                        app_id=app_id,
                        client=api,
                        **({"request_timeout": deadline.remaining()} if timeout is not None else {}),
                    )
                    # A final status read within the budget is the outcome, not a timeout.
                    if status in termination_statuses:
                        break
                    deadline.remaining()
                    if status == SparkAppStatus.Pending:
                        if startup_timeout and start_time + startup_timeout < time.time():
                            raise TimeoutError("App startup timeout")

                except ApiException as e:
                    if e.status == 404:
                        self.log(msg=f"{target} was deleted", level=logging.INFO, should_print=should_print)
                        return
                    raise
                self.log(
                    msg=f"{target} status is {status}, sleep {poll_interval}s",
                    level=logging.INFO,
                    should_print=should_print,
                )
                deadline.sleep(poll_interval)
            self.log(
                msg=f"{target} finished with status {status.value}",
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
        timeout: float | None = None,
        should_print: bool = False,
    ):
        """Stream logs from a Spark app.

        Args:
            namespace (str): Namespace.
            pod_name (str): Pod name.
            app_id (str): App ID.
            startup_timeout (float, optional): Timeout in seconds to wait for the app to start.
                Defaults to 0 (no timeout).
            timeout (float, optional): Overall observation deadline in seconds, including running jobs.
                None disables the deadline. Expiration never stops or deletes the application.
            should_print (bool, optional): Whether to print logs instead of logging them.
        """
        deadline = WaitDeadline(timeout)
        start_time = time.time()
        if pod_name is None and app_id is None:
            raise ValueError("Either pod_name or app_id must be specified")
        if pod_name is None:
            with self.k8s_client_manager.client() as client:
                api = k8s.CoreV1Api(client)
                pods = api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=driver_pod_selector(app_id),
                    **deadline.request_options(),
                ).items
                pod_name = select_driver_pod(pods, app_id).metadata.name
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            while True:
                pod = api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                    **deadline.request_options(),
                )
                if pod.status.phase != "Pending":
                    break
                if startup_timeout and start_time + startup_timeout < time.time():
                    raise TimeoutError("App startup timeout")
                deadline.sleep(5)
            watcher = watch.Watch()
            expired = Event()
            timer = None
            if timeout is not None:

                def expire():
                    expired.set()
                    watcher.stop()

                timer = Timer(deadline.remaining(), expire)
                timer.daemon = True
                timer.start()
            retries = 3
            received = False
            stream = None
            try:
                while True:
                    try:
                        stream = watcher.stream(
                            api.read_namespaced_pod_log,
                            namespace=namespace,
                            name=pod_name,
                            container=driver_container_name(pod),
                            **deadline.request_options(),
                        )
                        for line in stream:
                            deadline.remaining()
                            self.log(msg=line, level=logging.INFO, should_print=should_print)
                            received = True
                        deadline.remaining()
                        break
                    except ApiException as error:
                        if received or retries == 0:
                            raise
                        self.log(
                            msg=f"Failed to connect to the pod log stream: {error}. Retrying in 5s",
                            level=logging.WARNING,
                            should_print=should_print,
                        )
                        retries -= 1
                        deadline.sleep(5)
                    finally:
                        close = getattr(stream, "close", None)
                        if close is not None:
                            close()
            finally:
                if timer is not None:
                    timer.cancel()
                    timer.join()
                watcher.stop()
                if expired.is_set():
                    raise TimeoutError("Application wait timeout exceeded")

    def list_apps(self, namespace: str) -> list[str]:
        """List apps.

        Args:
            namespace (str): Namespace.

        Returns:
            list[str]: Spark apps in the namespace.
        """
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            drivers = api.list_namespaced_pod(
                namespace=namespace,
                label_selector="spark-role=driver",
            ).items
            # Other launchers also label drivers; only this client's drivers have an app ID label.
            labels = [pod.metadata.labels or {} for pod in drivers]
            return [item["spark-app-id"] for item in labels if "spark-app-id" in item]

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
                    label_selector=driver_pod_selector(app_id),
                ).items
                pod = select_driver_pod(pods, app_id)
            else:
                pod = api.read_namespaced_pod(
                    namespace=namespace,
                    name=pod_name,
                )
            container_name = driver_container_name(pod)
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

    def cancel_run(self, run, *, force: bool = False) -> None:
        """Cancel a UID-bearing run without deleting a replacement or another application."""
        if not isinstance(run.driver_uid, str) or not run.driver_uid:
            raise ValueError("Cancellation requires a receipt with a driver UID")
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            pod = api.read_namespaced_pod(name=run.driver_pod, namespace=run.namespace)
            labels = pod.metadata.labels or {}
            if (
                pod.metadata.uid != run.driver_uid
                or labels.get("spark-app-id") != run.app_id
                or labels.get("spark-role") != "driver"
            ):
                raise ValueError("The recorded driver identity does not match the current pod")
            api.delete_namespaced_pod(
                name=run.driver_pod,
                namespace=run.namespace,
                body=k8s.V1DeleteOptions(
                    grace_period_seconds=0 if force else None,
                    propagation_policy="Foreground",
                    preconditions=k8s.V1Preconditions(uid=run.driver_uid),
                ),
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
            preconditions = None
            if app_id:
                # we don't use `delete_collection_namespaced_pod` to know if the app exists or not
                pods = api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=driver_pod_selector(app_id),
                ).items
                pod = select_driver_pod(pods, app_id)
                pod_name = pod.metadata.name
                # Delete exactly the selected driver, not a replacement created after the lookup.
                preconditions = k8s.V1Preconditions(uid=pod.metadata.uid) if pod.metadata.uid else None
            api.delete_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                body=k8s.V1DeleteOptions(
                    grace_period_seconds=0 if force else None,
                    propagation_policy="Foreground",
                    preconditions=preconditions,
                ),
            )

    def find_prunable_drivers(
        self,
        *,
        namespace: str,
        older_than: float = 0,
        include_active: bool = False,
    ) -> dict[str, Any]:
        """Select drivers created by this client for cleanup, without changing anything.

        Only pods labeled ``spark-role=driver`` with a ``spark-app-id`` whose name is the driver
        name this client derives from that ID are considered. Terminal (Succeeded or Failed)
        drivers created at least ``older_than`` seconds ago are selected; ``include_active`` also
        selects Pending, Running and Unknown drivers, whose deletion stops the application.

        Args:
            namespace (str): Namespace.
            older_than (float, optional): Minimum age in seconds since pod creation. Defaults to 0.
            include_active (bool, optional): Also select non-terminal drivers. Defaults to False.

        Returns:
            dict: ``items``, one identity/status mapping per selected driver, and ``excluded``,
            the number of other matching pods by reason (``active``, ``recent``, ``deleting``,
            ``unverified``).
        """
        if isinstance(older_than, bool) or not isinstance(older_than, int | float) or not older_than >= 0:
            raise ValueError("older_than must be a nonnegative number of seconds")
        if include_active and older_than <= 0:
            raise ValueError("Selecting active drivers requires a positive older_than")
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            pods = api.list_namespaced_pod(
                namespace=namespace,
                label_selector="spark-role=driver,spark-app-id",
            ).items
        now = datetime.now(UTC)
        items = []
        excluded = dict.fromkeys(("active", "recent", "deleting", "unverified"), 0)
        for pod in pods:
            app_id = (pod.metadata.labels or {}).get("spark-app-id")
            created = pod.metadata.creation_timestamp
            # Older releases truncated long IDs instead of hashing them; both names are this client's.
            if (
                not isinstance(app_id, str)
                or not re.fullmatch(r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?", app_id)
                or pod.metadata.name
                not in {self._resource_name(app_id=app_id, suffix="-driver"), f"{app_id[:56]}-driver"}
                or not pod.metadata.uid
                or not isinstance(created, datetime)
            ):
                excluded["unverified"] += 1
                continue
            if pod.metadata.deletion_timestamp is not None:
                excluded["deleting"] += 1
                continue
            phase = (pod.status.phase if pod.status else None) or SparkAppStatus.Unknown.value
            if phase not in {SparkAppStatus.Succeeded, SparkAppStatus.Failed} and not include_active:
                excluded["active"] += 1
                continue
            age = (now - (created if created.tzinfo else created.replace(tzinfo=UTC))).total_seconds()
            if age < older_than:
                excluded["recent"] += 1
                continue
            items.append(
                {
                    "app_id": app_id,
                    "driver_pod": pod.metadata.name,
                    "driver_uid": pod.metadata.uid,
                    "phase": phase,
                    "reason": pod.status.reason if pod.status else None,
                    "created_at": created.isoformat(),
                    "age_seconds": max(int(age), 0),
                }
            )
        return {"items": items, "excluded": excluded}

    def delete_prunable_driver(self, *, namespace: str, item: dict[str, Any]) -> str:
        """Request deletion of one selected driver, guarded by its recorded identity.

        Owned executors, Services, ConfigMaps and Secrets are removed by Kubernetes garbage
        collection after the driver. Returns ``deletion_requested``, ``absent`` when the driver
        no longer exists, or ``replaced`` when a different pod now has its name (not deleted).
        Other failures raise.
        """
        run = RunHandle(
            namespace=namespace,
            app_id=item["app_id"],
            driver_pod=item["driver_pod"],
            driver_uid=item["driver_uid"],
        )
        try:
            self.cancel_run(run)
        except ApiException as error:
            if error.status == 404:
                return "absent"
            if error.status == 409:
                return "replaced"
            raise
        except ValueError:
            return "replaced"
        return "deletion_requested"

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
        active_deadline_seconds: int | None = None,
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
            active_deadline_seconds: Seconds after pod start before Kubernetes stops the pod and
                marks it Failed (DeadlineExceeded). None sets no deadline.

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
            active_deadline_seconds=active_deadline_seconds,
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
            configmap_name = (
                configmap["name"]
                if "name" in configmap
                else SparkAppManager._resource_name(app_id=app_id, suffix=f"-{index}")
            )
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
                    with open(source["text_path"], encoding="utf-8") as file:
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
