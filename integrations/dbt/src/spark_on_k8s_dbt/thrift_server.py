"""Experimental private HiveThriftServer2 lifecycle; no dbt invocation or tunnel ownership.

The caller supplies an explicitly configured SDK and durable ``record`` callback.
Creation records ``creating`` -> ``driver_created`` -> ``driver_service_confirmed``
-> ``service_creating`` -> ``created``. ``created`` is not SQL readiness.
Failures record ``creation_uncertain`` without retry or adoption. After a confirmed
driver UID, a failure (not an interrupt) requests deletion of that driver only, by
UID, and records the outcome in ``creation_cleanup``; the record stays incomplete.
Deletion records ``deleting`` -> ``deletion_requested``; a failed request records
``deletion_uncertain``. Only observed garbage collection records ``deleted``.

Every operation error retains the most recent receipt in ``error.handle``. If the
callback itself fails, that in-memory identity remains available on the exception.
Incomplete identities are preserved for inspection; deletion never guesses a UID.
Namespaces, service accounts, storage, catalogs, and network policy are never owned.
"""

from __future__ import annotations

import math
import re
from contextlib import contextmanager
from copy import deepcopy
from typing import TYPE_CHECKING
from uuid import uuid4

from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.connect_server import ConnectServerConfig
from spark_on_k8s.utils.run import RunHandle, WaitDeadline

if TYPE_CHECKING:
    from collections.abc import Callable

    from spark_on_k8s.client import SparkOnK8S

_LABEL = "spark-on-k8s.io/dbt-thrift-server"
_PORT = 10000
_CLASS = "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
_FIXED_CONF = {
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.server2.thrift.bind.host": "0.0.0.0",
    "spark.hadoop.hive.server2.thrift.port": str(_PORT),
    "spark.hadoop.hive.server2.transport.mode": "binary",
    "spark.hadoop.hive.server2.authentication": "NONE",
    "spark.hadoop.hive.server2.enable.doAs": "false",
    "spark.sql.hive.thriftServer.singleSession": "false",
}
_COMPLETE_STAGES = {
    "created",
    "creation_uncertain",
    "deleting",
    "deletion_requested",
    "deletion_uncertain",
    "deleted",
}


class ThriftServerError(RuntimeError):
    """An operation failed; ``handle`` retains all confirmed and planned identity."""

    def __init__(self, message, handle):
        super().__init__(message)
        self.handle = deepcopy(handle)


class ThriftOwnershipError(ThriftServerError):
    """Resource identity or private routing changed; do not adopt the replacement."""


def _uid(value):
    if not isinstance(value, str) or not value or len(value) > 128 or any(c.isspace() for c in value):
        raise ValueError("A recorded Kubernetes UID is required")


def _deadline(timeout):
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    return WaitDeadline(timeout)


def _options(config, token):
    """Set every ambient submission option explicitly, as the core Connect provisioner does."""
    return {
        "image": config.image,
        "app_path": config.server_jar,
        "namespace": config.namespace,
        "service_account": config.service_account,
        "app_name": config.app_name[:22].rstrip("-"),
        "app_id_suffix": lambda: "-" + token,
        "class_name": _CLASS,
        "app_arguments": [],
        "app_waiter": "no_wait",
        "should_print": False,
        "image_pull_policy": config.image_pull_policy,
        "spark_conf": {**config.spark_conf, **_FIXED_CONF},
        "packages": list(config.packages),
        "ui_reverse_proxy": False,
        "driver_resources": config.driver_resources,
        "executor_resources": config.executor_resources,
        "executor_instances": config.executor_instances,
        "secret_values": {},
        "driver_ephemeral_configmaps_volumes": [],
        "driver_env_vars_from_secrets": [],
        "volumes": [],
        "driver_volume_mounts": [],
        "executor_volume_mounts": [],
        "driver_node_selector": dict(config.driver_node_selector),
        "executor_node_selector": {},
        "driver_annotations": {},
        "executor_annotations": {},
        "driver_labels": {_LABEL: "true"},
        "executor_labels": {},
        "driver_tolerations": [],
        "executor_pod_template_path": "",
        "executor_pod_template": "",
        "startup_timeout": 0,
        "wait_timeout": None,
        "driver_init_containers": [],
        "driver_host_aliases": [],
        "driver_pod_template": None,
        "driver_container_name": None,
        "dry_run": False,
    }


def _read(api, kind, name, namespace, deadline):
    try:
        return getattr(api, f"read_namespaced_{kind}")(
            name=name, namespace=namespace, **deadline.request_options()
        )
    except ApiException as error:
        if error.status == 404:
            return None
        raise


def _identity(obj):
    _uid(obj.metadata.uid)
    return {"name": obj.metadata.name, "uid": obj.metadata.uid}


def _validate_handle(handle):
    if not isinstance(handle, dict) or handle.get("format") != 1 or handle.get("kind") != "thrift":
        raise ValueError("Invalid Thrift server receipt")
    if handle.get("stage") not in _COMPLETE_STAGES or handle.get("port") != _PORT:
        raise ValueError("Incomplete Thrift identity; inspect the retained receipt before cleanup")
    run = handle.get("run")
    if not isinstance(run, dict) or set(run) != {"namespace", "app_id", "driver_pod", "driver_uid"}:
        raise ValueError("Receipt requires complete run identity")
    run = RunHandle(**run)
    if not isinstance(run.namespace, str) or not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", run.namespace
    ):
        raise ValueError("Invalid recorded namespace")
    if not isinstance(run.app_id, str) or not re.fullmatch(
        r"[a-z](?:[a-z0-9-]{0,20}[a-z0-9])?-[0-9a-f]{32}", run.app_id
    ):
        raise ValueError("Invalid recorded application identity")
    if run.driver_pod != run.app_id + "-driver":
        raise ValueError("Recorded driver name does not match its application")
    _uid(run.driver_uid)
    _uid(handle.get("namespace_uid"))
    for key, name in (("driver_service", run.app_id), ("service", run.app_id + "-thrift")):
        identity = handle.get(key)
        if not isinstance(identity, dict) or set(identity) != {"name", "uid"} or identity["name"] != name:
            raise ValueError(f"Incomplete or invalid {key} identity")
        _uid(identity["uid"])
    owned = handle.get("owned_uids", [])
    if not isinstance(owned, list):
        raise ValueError("owned_uids must be a list")
    for uid in owned:
        _uid(uid)
    return run


def _metadata(obj, run, name, uid, handle):
    if (
        obj.metadata.name != name
        or obj.metadata.namespace != run.namespace
        or obj.metadata.uid != uid
        or not obj.metadata.resource_version
    ):
        raise ThriftOwnershipError("Recorded resource identity changed or has no resourceVersion", handle)


def _driver(pod, run, handle):
    if pod is None:
        return
    _metadata(pod, run, run.driver_pod, run.driver_uid, handle)
    expected = {"spark-app-id": run.app_id, "spark-role": "driver", _LABEL: "true"}
    if any((pod.metadata.labels or {}).get(key) != value for key, value in expected.items()):
        raise ThriftOwnershipError("Recorded Thrift driver labels changed", handle)


def _service(service, run, identity, handle, *, thrift):
    if service is None:
        return
    _metadata(service, run, identity["name"], identity["uid"], handle)
    expected = {"spark-app-id": run.app_id, "spark-role": "driver"}
    expected.update({_LABEL: "true"} if thrift else {"spark-app-name": run.app_id.rsplit("-", 1)[0]})
    owners = service.metadata.owner_references or []
    ports = service.spec.ports or []
    expected_ports = {(10000, 10000, "TCP")} if thrift else {(4040, 4040, "TCP"), (7077, 7077, "TCP")}
    actual_ports = {
        (p.port, p.target_port if p.target_port is not None else p.port, p.protocol or "TCP") for p in ports
    }
    if (
        service.spec.selector != expected
        or any((service.metadata.labels or {}).get(key) != value for key, value in expected.items())
        or len(owners) != 1
        or owners[0].uid != run.driver_uid
        or owners[0].name != run.driver_pod
        or owners[0].kind != "Pod"
        or owners[0].api_version != "v1"
        or service.spec.type != "ClusterIP"
        or service.spec.external_ips
        or service.spec.external_name
        or service.spec.load_balancer_ip
        or service.spec.load_balancer_source_ranges
        or (thrift and service.spec.cluster_ip in {None, "", "None"})
        or (not thrift and service.spec.cluster_ip != "None")
        or len(ports) != len(expected_ports)
        or actual_ports != expected_ports
        or any(p.node_port is not None for p in ports)
    ):
        raise ThriftOwnershipError(
            "Recorded Thrift Service ownership, selector, or private routing changed", handle
        )


class ThriftServer:
    """Manage only a UID-recorded driver and its two private Services.

    ``config=None`` supports recovery operations from a receipt without retaining
    original Spark configuration. The SDK's Kubernetes manager must already select
    the intended cluster explicitly; this class does not replace or mutate it.
    """

    def __init__(self, sdk: SparkOnK8S, config: ConnectServerConfig | None, record: Callable[[dict], None]):
        if not callable(record):
            raise ValueError("record must persist server identity")
        if config is not None:
            if not isinstance(config, ConnectServerConfig):
                raise ValueError("config must be ConnectServerConfig or None")
            config = deepcopy(config)
            config.__post_init__()
            if config.port != _PORT:
                raise ValueError("The experimental Thrift backend requires port 10000")
            for key, value in config.spark_conf.items():
                if (
                    key == "spark.sql.catalogImplementation"
                    or "hive.server2." in key
                    or key.startswith("spark.sql.hive.thriftServer.")
                ) and (key not in _FIXED_CONF or value != _FIXED_CONF[key]):
                    raise ValueError(f"spark_conf cannot override managed Thrift setting: {key}")
        self.sdk = sdk
        self.config = config
        self.record = record
        self._creation_started = False
        self._observed_uids = {}

    @contextmanager
    def _api(self):
        with self.sdk.k8s_client_manager.client() as raw:
            raw.rest_client.pool_manager.connection_pool_kw["retries"] = 0
            yield k8s.CoreV1Api(raw)

    def _publish(self, state, stage):
        state["stage"] = stage
        self.record(deepcopy(state))

    def _failed(self, state, stage, error):
        state["stage"] = stage
        try:
            self.record(deepcopy(state))
        except BaseException as publication_error:
            error.add_note(f"Could not persist recovery identity: {publication_error}")
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            error.handle = deepcopy(state)
            raise error
        raise ThriftServerError(
            f"Thrift server {stage}; retain the receipt and inspect before retrying", state
        ) from error

    def create(self) -> dict:
        if self.config is None:
            raise ValueError("Creating a Thrift server requires explicit config")
        if self._creation_started:
            raise RuntimeError(
                "This server creation was already attempted; inspect its receipt instead of retrying"
            )
        self._creation_started = True
        token = uuid4().hex
        options = _options(self.config, token)
        app_id = options["app_name"] + "-" + token
        planned = RunHandle(namespace=self.config.namespace, app_id=app_id, driver_pod=app_id + "-driver")
        state = {"format": 1, "kind": "thrift", "stage": "creating", "run": planned.to_dict(), "port": _PORT}
        try:
            # Persist intent before even a read, and never perform a mutation after a failed publication.
            self._publish(state, "creating")
            with self._api() as api:
                deadline = _deadline(30)
                namespace = api.read_namespace(name=planned.namespace, **deadline.request_options())
                _uid(namespace.metadata.uid)
                state["namespace_uid"] = namespace.metadata.uid
                api.read_namespaced_service_account(
                    name=self.config.service_account,
                    namespace=planned.namespace,
                    **deadline.request_options(),
                )
                self._publish(state, "creating")
            run = self.sdk.submit_run(**options)
            if (run.namespace, run.app_id, run.driver_pod) != (
                planned.namespace,
                planned.app_id,
                planned.driver_pod,
            ):
                raise ValueError("SDK returned an unrelated run identity")
            _uid(run.driver_uid)
            state["run"] = run.to_dict()
            self._publish(state, "driver_created")
            with self._api() as api:
                deadline = _deadline(30)
                pod = _read(api, "pod", run.driver_pod, run.namespace, deadline)
                driver_service = _read(api, "service", run.app_id, run.namespace, deadline)
                if pod is None or driver_service is None:
                    raise RuntimeError("Submitted driver or its Service is absent")
                _driver(pod, run, state)
                state["driver_service"] = _identity(driver_service)
                _service(driver_service, run, state["driver_service"], state, thrift=False)
                self._publish(state, "driver_service_confirmed")
                state["planned_service_name"] = run.app_id + "-thrift"
                self._publish(state, "service_creating")
                labels = {"spark-app-id": run.app_id, "spark-role": "driver", _LABEL: "true"}
                service = api.create_namespaced_service(
                    namespace=run.namespace,
                    body=k8s.V1Service(
                        metadata=k8s.V1ObjectMeta(
                            name=state["planned_service_name"],
                            namespace=run.namespace,
                            labels=labels,
                            owner_references=[
                                k8s.V1OwnerReference(
                                    api_version="v1",
                                    kind="Pod",
                                    name=run.driver_pod,
                                    uid=run.driver_uid,
                                )
                            ],
                        ),
                        spec=k8s.V1ServiceSpec(
                            type="ClusterIP",
                            selector=labels,
                            ports=[
                                k8s.V1ServicePort(
                                    name="thrift", port=_PORT, target_port=_PORT, protocol="TCP"
                                )
                            ],
                        ),
                    ),
                    **deadline.request_options(),
                )
                state["service"] = _identity(service)
                _service(service, run, state["service"], state, thrift=True)
                self._publish(state, "created")
                # Re-read all identities together before handing the server to the caller.
                self._preflight(api, state, deadline)
            return deepcopy(state)
        except BaseException as error:
            confirmed = self.sdk.last_submitted_run
            if (
                confirmed is not None
                and (confirmed.namespace, confirmed.app_id, confirmed.driver_pod)
                == (planned.namespace, planned.app_id, planned.driver_pod)
                and confirmed.driver_uid
            ):
                state["run"] = confirmed.to_dict()
            if isinstance(error, Exception):
                # As the core Connect provisioner does, never on an interrupt.
                try:
                    state["creation_cleanup"] = self._rollback(state)
                except BaseException as interrupted:
                    state["creation_cleanup"] = {
                        "status": "unknown",
                        "reason": "Rollback was interrupted; deletion may already have been requested",
                    }
                    self._failed(state, "creation_uncertain", interrupted)
            self._failed(state, "creation_uncertain", error)

    def _rollback(self, state):
        """Request deletion of only a UID-confirmed driver after a failed create.

        Both Services name that driver UID as their owner, so Kubernetes collects
        them with it. The record stays incomplete: disappearance is not observed.
        """
        if "driver_uid" not in state["run"]:
            return {
                "status": "not_attempted",
                "reason": "Driver creation is unconfirmed; resources may exist",
            }
        run = RunHandle(**state["run"])
        try:
            with self._api() as api:
                deadline = _deadline(30)
                pod = _read(api, "pod", run.driver_pod, run.namespace, deadline)
                if pod is None:
                    return {"status": "not_found", "reason": "The confirmed driver was already absent"}
                _driver(pod, run, state)
                api.delete_namespaced_pod(
                    name=run.driver_pod,
                    namespace=run.namespace,
                    body=k8s.V1DeleteOptions(
                        propagation_policy="Foreground",
                        # The UID proves ownership. A starting pod's resourceVersion
                        # changes with each status update, so it is not a precondition.
                        preconditions=k8s.V1Preconditions(uid=run.driver_uid),
                    ),
                    **deadline.request_options(),
                )
        except Exception as error:
            return {
                "status": "failed",
                "reason": "UID-checked rollback failed; inspect the recorded run before retrying cleanup",
                "error": type(error).__name__,
            }
        return {
            "status": "requested",
            "reason": "UID-checked driver deletion requested; its Services are garbage-collected with it",
        }

    def _preflight(self, api, handle, deadline):
        run = _validate_handle(handle)
        try:
            namespace = api.read_namespace(name=run.namespace, **deadline.request_options())
        except ApiException as error:
            if error.status == 404:
                return run, None, None, None, False
            raise
        if namespace.metadata.uid != handle["namespace_uid"]:
            raise ThriftOwnershipError("Recorded namespace was replaced; preserve its resources", handle)
        pod = _read(api, "pod", run.driver_pod, run.namespace, deadline)
        driver_service = _read(api, "service", handle["driver_service"]["name"], run.namespace, deadline)
        service = _read(api, "service", handle["service"]["name"], run.namespace, deadline)
        _driver(pod, run, handle)
        _service(driver_service, run, handle["driver_service"], handle, thrift=False)
        _service(service, run, handle["service"], handle, thrift=True)
        return run, pod, driver_service, service, True

    def status(self, handle: dict, *, timeout=30) -> dict:
        state = deepcopy(handle)
        try:
            with self._api() as api:
                _, pod, driver_service, service, _ = self._preflight(api, state, _deadline(timeout))
            resources = {"driver": pod, "driver_service": driver_service, "service": service}
            missing = [name for name, resource in resources.items() if resource is None]
            phase = pod.status.phase if pod is not None and pod.status else None
            deleting = any(obj is not None and obj.metadata.deletion_timestamp for obj in resources.values())
            status = (
                "absent"
                if len(missing) == 3
                else "missing"
                if missing
                else "deleting"
                if deleting
                else "terminated"
                if phase in {"Failed", "Succeeded"}
                else "running"
                if phase == "Running"
                else "starting"
                if phase == "Pending"
                else "unknown"
            )
            return {"status": status, "driver_phase": phase, "missing": missing, "query_ready": None}
        except ThriftServerError:
            raise
        except Exception as error:
            raise ThriftServerError(
                "Thrift status is uncertain; absence was not established", state
            ) from error

    def _remaining(self, api, state, deadline):
        run = _validate_handle(state)
        known = self._observed_uids.setdefault(run.driver_uid, set())
        known.update([run.driver_uid, state["driver_service"]["uid"], state["service"]["uid"]])
        known.update(state.get("owned_uids", []))
        objects = []
        for kind in ("pod", "service", "config_map"):
            objects.extend(
                getattr(api, f"list_namespaced_{kind}")(
                    namespace=run.namespace, **deadline.request_options()
                ).items
            )
        previous = None
        while previous != known:
            previous = set(known)
            for obj in objects:
                if (obj.metadata.labels or {}).get("spark-app-id") == run.app_id or any(
                    owner.uid in known for owner in obj.metadata.owner_references or []
                ):
                    known.add(obj.metadata.uid)
        state["owned_uids"] = sorted(known)
        return [obj.metadata.name for obj in objects if obj.metadata.uid in known]

    def delete(self, handle: dict, *, timeout=30) -> None:
        state = deepcopy(handle)
        try:
            with self._api() as api:
                deadline = _deadline(timeout)
                run, pod, driver_service, service, namespace_exists = self._preflight(api, state, deadline)
                if namespace_exists:
                    self._remaining(api, state, deadline)
                self._publish(state, "deleting")
                for kind, obj in (("service", service), ("service", driver_service), ("pod", pod)):
                    if obj is None:
                        continue
                    try:
                        getattr(api, f"delete_namespaced_{kind}")(
                            name=obj.metadata.name,
                            namespace=run.namespace,
                            body=k8s.V1DeleteOptions(
                                propagation_policy="Foreground",
                                preconditions=k8s.V1Preconditions(
                                    uid=obj.metadata.uid,
                                    resource_version=obj.metadata.resource_version,
                                ),
                            ),
                            **deadline.request_options(),
                        )
                    except ApiException as error:
                        if error.status != 404:
                            raise
                self._publish(state, "deletion_requested")
        except BaseException as error:
            self._failed(state, "deletion_uncertain", error)

    def wait_deleted(self, handle: dict, *, timeout=90, poll_interval=1) -> dict:
        deadline = _deadline(timeout)
        if type(poll_interval) not in {int, float} or not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError("poll_interval must be positive and finite")
        state = deepcopy(handle)
        try:
            with self._api() as api:
                while True:
                    _, _, _, _, namespace_exists = self._preflight(api, state, deadline)
                    remaining = self._remaining(api, state, deadline) if namespace_exists else []
                    if not remaining:
                        state["owned_resources_remaining"] = []
                        self._publish(state, "deleted")
                        return {"status": "deleted", "owned_resources_remaining": []}
                    deadline.sleep(poll_interval)
        except ThriftServerError:
            raise
        except Exception as error:
            raise ThriftServerError("Thrift cleanup was not observed; retain the receipt", state) from error
