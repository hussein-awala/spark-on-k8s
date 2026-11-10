from __future__ import annotations

import math
import re
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import yaml
from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import MaxRetryError, NewConnectionError, TimeoutError as TransportTimeoutError

from spark_on_k8s.client import ExecutorInstances, PodResources
from spark_on_k8s.connect import ConnectClientUnavailable, ConnectError, ConnectProfile
from spark_on_k8s.utils.run import RunHandle, WaitDeadline, validate_max_runtime
from spark_on_k8s.utils.spark_app_status import driver_container_name

if TYPE_CHECKING:
    from spark_on_k8s.client import SparkOnK8S
    from spark_on_k8s.planning import ApplicationPlan

_SERVER_LABEL = "spark-on-k8s.io/connect-server"
_SERVER_CLASS = "org.apache.spark.sql.connect.service.SparkConnectServer"
_PROTECTED_CONF = {
    "spark.master",
    "spark.submit.deployMode",
    "spark.app.id",
    "spark.app.name",
    "spark.driver.host",
    "spark.driver.port",
    "spark.driver.bindAddress",
    "spark.ui.port",
    "spark.kubernetes.namespace",
    "spark.kubernetes.driver.pod.name",
    "spark.kubernetes.authenticate.driver.serviceAccountName",
    "spark.kubernetes.container.image",
    "spark.kubernetes.driver.container.image",
    "spark.kubernetes.executor.container.image",
    "spark.kubernetes.container.image.pullPolicy",
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.executor.instances",
    "spark.connect.grpc.binding.address",
    "spark.connect.grpc.binding.port",
}
_PROTECTED_PREFIXES = (
    "spark.dynamicAllocation.",
    "spark.kubernetes.",
    "spark.kubernetes.driver.label.",
    "spark.kubernetes.executor.label.",
    "spark.kubernetes.driver.podTemplate",
    "spark.kubernetes.executor.podTemplate",
)


def _name(value: Any, field_name: str, *, max_length: int = 63) -> None:
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", value)
        or len(value) > max_length
    ):
        raise ValueError(f"{field_name} must be a Kubernetes name of at most {max_length} characters")
    if (max_length == 63 and "." in value) or any(
        not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", part) or len(part) > 63
        for part in value.split(".")
    ):
        raise ValueError(f"{field_name} contains an invalid DNS label")


def _uid(value: Any) -> None:
    if not isinstance(value, str) or not value or len(value) > 128 or any(c.isspace() for c in value):
        raise ValueError("A nonblank recorded Kubernetes UID is required")


def _port(value: Any) -> None:
    if type(value) is not int or not 1 <= value <= 65535 or value in {4040, 7077}:
        raise ValueError("Connect port must be an integer in 1..65535, excluding driver ports 4040 and 7077")


def _positive(value: Any, name: str, *, zero: bool = False) -> None:
    if type(value) is not int or value < (0 if zero else 1):
        raise ValueError(f"{name} must be a {'nonnegative' if zero else 'positive'} integer")


_LABEL_PART = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?")
_TOLERATION_FIELDS = {"key", "operator", "value", "effect", "tolerationSeconds"}
# Tuples compare without hashing, so malformed (unhashable) input gets a validation error.
_TAINT_EFFECTS = ("NoSchedule", "PreferNoSchedule", "NoExecute")


def _label_key(value: Any, name: str) -> None:
    """Kubernetes label key: an optional DNS-subdomain prefix and slash, then a 63-character name."""
    prefix, slash, label = value.rpartition("/") if isinstance(value, str) else ("", "", "")
    if (
        not _LABEL_PART.fullmatch(label)
        or len(label) > 63
        or (
            slash
            and (
                len(prefix) > 253
                or any(
                    not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", part) or len(part) > 63
                    for part in prefix.split(".")
                )
            )
        )
    ):
        raise ValueError(f"{name} key {value!r} must be a Kubernetes label key ([prefix/]name)")


def _label_value(value: Any, name: str) -> None:
    if not isinstance(value, str) or len(value) > 63 or (value and not _LABEL_PART.fullmatch(value)):
        raise ValueError(f"{name} value {value!r} must be a Kubernetes label value")


def _node_selector(values: Any, name: str) -> None:
    if not isinstance(values, dict):
        raise ValueError(f"{name} must be a mapping of node label keys to values")
    for key, value in values.items():
        _label_key(key, name)
        _label_value(value, name)


def _tolerations(values: Any, name: str) -> None:
    """Apply the Kubernetes API's toleration rules before any cluster access."""
    if not isinstance(values, list):
        raise ValueError(f"{name} must be a list of Kubernetes toleration mappings")
    for toleration in values:
        if not isinstance(toleration, dict) or not set(toleration) <= _TOLERATION_FIELDS:
            raise ValueError(
                f"{name} entries are mappings of key, operator, value, effect and tolerationSeconds"
            )
        key = toleration.get("key", "")
        operator = toleration.get("operator", "Equal")
        value = toleration.get("value", "")
        effect = toleration.get("effect", "")
        if operator not in ("Equal", "Exists"):
            raise ValueError(f"{name} operator must be Equal or Exists")
        if not isinstance(key, str):
            raise ValueError(f"{name} key must be a string")
        if key:
            _label_key(key, name)
        elif operator != "Exists":
            raise ValueError(f"{name} entries without a key must use operator Exists")
        if operator == "Exists" and value != "":
            raise ValueError(f"{name} value must be empty when operator is Exists")
        _label_value(value, name)
        if effect != "" and effect not in _TAINT_EFFECTS:  # Empty matches every effect.
            raise ValueError(f"{name} effect must be NoSchedule, PreferNoSchedule or NoExecute")
        if "tolerationSeconds" in toleration and (
            type(toleration["tolerationSeconds"]) is not int or effect != "NoExecute"
        ):
            raise ValueError(f"{name} tolerationSeconds must be an integer and requires effect NoExecute")


def _secret_environment(values: Any, name: str) -> None:
    """Validate environment variable to SECRET:KEY references; Secret values are never read."""
    if not isinstance(values, dict):
        raise ValueError(f"{name} must map environment variable names to SECRET:KEY references")
    for variable, reference in values.items():
        # Spark and this helper set SPARK_* variables; a Secret must not silently replace one.
        if (
            not isinstance(variable, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", variable)
            or variable.startswith("SPARK_")
        ):
            raise ValueError(
                f"{name} variable {variable!r} must be an identifier ([A-Za-z_][A-Za-z0-9_]*) "
                "not starting with SPARK_"
            )
        secret, colon, key = reference.partition(":") if isinstance(reference, str) else ("", "", "")
        if not colon:
            raise ValueError(f"{name} reference for {variable} must have the form SECRET:KEY")
        _name(secret, f"{name} Secret name for {variable}", max_length=253)
        if len(key) > 253 or not re.fullmatch(r"[-._a-zA-Z0-9]+", key):
            raise ValueError(f"{name} key for {variable} must be a Kubernetes Secret data key")


@dataclass(frozen=True, kw_only=True)
class ConnectServerConfig:
    """Explicit inputs for one private, single-driver Spark Connect server.

    The image must contain the specified Connect server JAR and a Spark entrypoint.
    This configuration creates neither namespaces nor service accounts. Its plaintext
    ClusterIP service requires a trusted network or a separately managed gateway.

    Pod placement and credentials reference existing Kubernetes objects only:
    ``image_pull_secrets`` names Secrets used for the driver and executors;
    ``driver_env_from_secrets`` and ``executor_env_from_secrets`` map environment
    variable names to ``SECRET:KEY`` references, resolved by the kubelet (this helper
    never reads Secret values); node selectors map node label keys to values; and
    tolerations are Kubernetes toleration mappings (``key``, ``operator``, ``value``,
    ``effect``, ``tolerationSeconds``).

    An optional max_runtime (seconds) stops the server's driver even if no client deletes it.
    """

    image: str
    server_jar: str
    namespace: str
    service_account: str
    app_name: str = "spark-connect"
    port: int = 15002
    image_pull_policy: str = "IfNotPresent"
    driver_resources: PodResources = field(default_factory=PodResources)
    executor_resources: PodResources = field(default_factory=PodResources)
    executor_instances: ExecutorInstances = field(default_factory=ExecutorInstances)
    spark_conf: dict[str, str] = field(default_factory=dict)
    packages: list[str] = field(default_factory=list)
    driver_node_selector: dict[str, str] = field(default_factory=dict)
    executor_node_selector: dict[str, str] = field(default_factory=dict)
    driver_tolerations: list[dict[str, Any]] = field(default_factory=list)
    executor_tolerations: list[dict[str, Any]] = field(default_factory=list)
    image_pull_secrets: list[str] = field(default_factory=list)
    driver_env_from_secrets: dict[str, str] = field(default_factory=dict)
    executor_env_from_secrets: dict[str, str] = field(default_factory=dict)
    max_runtime: int | None = None

    def __post_init__(self):
        if not isinstance(self.image, str) or not self.image or any(c.isspace() for c in self.image):
            raise ValueError("image must be an explicit nonblank container image")
        if (
            not isinstance(self.server_jar, str)
            or not self.server_jar.startswith("local:///")
            or not self.server_jar.endswith(".jar")
            or any(c.isspace() for c in self.server_jar)
        ):
            raise ValueError("server_jar must be an explicit local:/// path to a JAR in the image")
        _name(self.namespace, "namespace")
        _name(self.service_account, "service_account", max_length=253)
        if not isinstance(self.app_name, str) or not re.fullmatch(
            r"[a-z](?:[a-z0-9-]*[a-z0-9])?", self.app_name
        ):
            raise ValueError("app_name must start with a lowercase letter and contain DNS-label characters")
        _port(self.port)
        if self.image_pull_policy not in {"Always", "IfNotPresent", "Never"}:
            raise ValueError("image_pull_policy must be Always, IfNotPresent, or Never")
        for name, resources in (("driver", self.driver_resources), ("executor", self.executor_resources)):
            if not isinstance(resources, PodResources):
                raise ValueError(f"{name}_resources must be PodResources")
            for key in ("cpu", "memory", "memory_overhead"):
                _positive(getattr(resources, key), f"{name}.{key}", zero=key == "memory_overhead")
        instances = self.executor_instances
        if not isinstance(instances, ExecutorInstances):
            raise ValueError("executor_instances must be ExecutorInstances")
        for key in ("min", "max", "initial"):
            value = getattr(instances, key)
            if value is not None:
                _positive(value, f"executor_instances.{key}", zero=key != "max")
        if instances.min is not None and instances.max is not None and instances.min > instances.max:
            raise ValueError("Minimum executors cannot exceed maximum executors")
        if instances.initial is not None and (
            (instances.min is not None and instances.initial < instances.min)
            or (instances.max is not None and instances.initial > instances.max)
        ):
            raise ValueError("Initial executors must be within the configured minimum and maximum")
        if (
            type(instances.executor_allocation_ratio) not in {int, float}
            or not math.isfinite(instances.executor_allocation_ratio)
            or not 0 < instances.executor_allocation_ratio <= 1
        ):
            raise ValueError("Executor allocation ratio must be finite and in (0, 1]")
        for value in (instances.scheduler_backlog_timeout, instances.sustained_scheduler_backlog_timeout):
            if value is not None and (
                not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*(?:ms|s|m|h|d)?", value)
            ):
                raise ValueError("Executor backlog timeouts must be positive Spark durations")
        if not isinstance(self.spark_conf, dict) or any(
            not isinstance(k, str) or not k or not isinstance(v, str) for k, v in self.spark_conf.items()
        ):
            raise ValueError("spark_conf must be a string-to-string mapping")
        for name in ("driver_node_selector", "executor_node_selector"):
            _node_selector(getattr(self, name), name)
        for name in ("driver_tolerations", "executor_tolerations"):
            _tolerations(getattr(self, name), name)
        if not isinstance(self.image_pull_secrets, list):
            raise ValueError("image_pull_secrets must be a list of Secret names")
        for secret in self.image_pull_secrets:
            _name(secret, "image_pull_secrets entry", max_length=253)
        if len(set(self.image_pull_secrets)) != len(self.image_pull_secrets):
            raise ValueError("image_pull_secrets must not repeat a Secret")
        for name in ("driver_env_from_secrets", "executor_env_from_secrets"):
            _secret_environment(getattr(self, name), name)
        if any(f"spark.executorEnv.{name}" in self.spark_conf for name in self.executor_env_from_secrets):
            raise ValueError("executor_env_from_secrets and spark_conf cannot both set an executor variable")
        if any(
            (key in _PROTECTED_CONF or key.startswith(_PROTECTED_PREFIXES))
            and key != "spark.kubernetes.executor.request.cores"
            for key in self.spark_conf
        ):
            raise ValueError(
                "spark_conf cannot override managed identity, ports, images, resources, or allocation"
            )
        request_cores = self.spark_conf.get("spark.kubernetes.executor.request.cores")
        if request_cores is not None:
            if not re.fullmatch(r"(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)m?", request_cores):
                raise ValueError("Executor CPU request must be a positive Kubernetes CPU quantity")
            amount = float(request_cores.removesuffix("m")) / (1000 if request_cores.endswith("m") else 1)
            if not math.isfinite(amount) or amount <= 0 or amount > self.executor_resources.cpu:
                raise ValueError(
                    "Executor CPU request must be positive and no greater than configured executor cores"
                )
        if not isinstance(self.packages, list) or any(
            not isinstance(p, str) or not re.fullmatch(r"[^\s,:]+:[^\s,:]+:[^\s,:]+", p)
            for p in self.packages
        ):
            raise ValueError("packages must contain explicit Maven group:artifact:version coordinates")
        validate_max_runtime(self.max_runtime)


@dataclass(frozen=True, kw_only=True)
class ConnectServerHandle:
    """Credential-free identity for a server and both owned Kubernetes Services."""

    run: RunHandle
    service_name: str
    service_uid: str
    driver_service_uid: str
    port: int

    def __post_init__(self):
        if not isinstance(self.run, RunHandle):
            raise ValueError("run must be a RunHandle")
        _name(self.run.namespace, "namespace")
        _name(self.run.app_id, "app_id")
        _name(self.run.driver_pod, "driver_pod", max_length=253)
        _name(self.service_name, "service_name")
        if (
            self.run.driver_pod != f"{self.run.app_id}-driver"
            or self.service_name != f"{self.run.app_id}-connect"
        ):
            raise ValueError("Connect resource names must match the recorded application identity")
        for value in (self.run.driver_uid, self.service_uid, self.driver_service_uid):
            _uid(value)
        _port(self.port)

    @property
    def endpoint(self) -> str:
        """Return the private cluster DNS endpoint; no credentials are included."""
        return f"sc://{self.service_name}.{self.run.namespace}.svc:{self.port}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "run": self.run.to_dict(),
            "service_name": self.service_name,
            "service_uid": self.service_uid,
            "driver_service_uid": self.driver_service_uid,
            "port": self.port,
        }

    @classmethod
    def from_dict(cls, value: dict) -> ConnectServerHandle:
        if not isinstance(value, dict) or set(value) != {
            "run",
            "service_name",
            "service_uid",
            "driver_service_uid",
            "port",
        }:
            raise ValueError("Invalid Connect server receipt fields")
        run = value["run"]
        if not isinstance(run, dict) or set(run) != {"namespace", "app_id", "driver_pod", "driver_uid"}:
            raise ValueError("Connect server receipt requires complete run identity")
        return cls(**{**value, "run": RunHandle(**run)})


class ConnectServerError(RuntimeError):
    """Server operation failed without interpreting uncertainty as absence."""

    def __init__(self, message, *, handle=None, run=None, phase=None, cleanup=None):
        super().__init__(message)
        self.handle = handle
        self.run = handle.run if handle is not None else run
        self.phase = phase
        self.cleanup = cleanup or {"status": "not_attempted", "reason": "No rollback was requested"}


class ConnectServerCreateError(ConnectServerError):
    """Creation failed; inspect run, phase and cleanup for partial resource effects."""


class ConnectServerOwnershipError(ConnectServerError):
    """Observed resources do not match the recorded identity; no adoption is allowed."""


class ConnectServerTimeout(TimeoutError):
    """Readiness observation expired; the server was not deleted."""

    def __init__(self, handle, last_failure: str | None = None):
        message = "Spark Connect readiness timed out; the server was not stopped or deleted"
        super().__init__(message + (f". Last query check: {last_failure}" if last_failure else ""))
        self.handle = handle
        self.run = handle.run
        self.phase = "readiness"
        self.cleanup = {"status": "not_attempted", "reason": "Readiness timeout does not delete the server"}


def _pod_options(config: ConnectServerConfig) -> dict:
    """Map placement and Secret references onto existing submission mechanisms.

    The driver Pod is built by this client, so its pull secrets and Secret-backed
    variables use a driver pod template. Executors are built by Spark: they use Spark's
    pull-secret and secretKeyRef properties, and an executor pod template for
    tolerations, which Spark has no property for. Secret values are never read.
    """
    conf = {
        f"spark.kubernetes.executor.secretKeyRef.{variable}": reference
        for variable, reference in config.executor_env_from_secrets.items()
    }
    driver_template = None
    if config.image_pull_secrets:
        conf["spark.kubernetes.container.image.pullSecrets"] = ",".join(config.image_pull_secrets)
    if config.image_pull_secrets or config.driver_env_from_secrets:
        driver = {"name": "driver"}
        if config.driver_env_from_secrets:
            driver["env"] = []
            for variable, reference in config.driver_env_from_secrets.items():
                secret, key = reference.split(":", 1)
                driver["env"].append(
                    {"name": variable, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}
                )
        spec = {"containers": [driver]}
        if config.image_pull_secrets:
            spec["imagePullSecrets"] = [{"name": secret} for secret in config.image_pull_secrets]
        driver_template = {"apiVersion": "v1", "kind": "Pod", "spec": spec}
    executor_template = ""
    if config.executor_tolerations:
        tolerations = deepcopy(config.executor_tolerations)
        executor_template = yaml.safe_dump(
            {"apiVersion": "v1", "kind": "Pod", "spec": {"tolerations": tolerations}}, sort_keys=False
        )
    return {
        "spark_conf": conf,
        "driver_pod_template": driver_template,
        "executor_pod_template": executor_template,
        "driver_tolerations": [
            k8s.V1Toleration(
                key=toleration.get("key"),
                operator=toleration.get("operator"),
                value=toleration.get("value"),
                effect=toleration.get("effect"),
                toleration_seconds=toleration.get("tolerationSeconds"),
            )
            for toleration in config.driver_tolerations
        ],
    }


def _options(config: ConnectServerConfig, token: str) -> dict:
    pod = _pod_options(config)
    # Explicit values prevent unrelated SPARK_ON_K8S_* application defaults from leaking into a server.
    return {
        "image": config.image,
        "app_path": config.server_jar,
        "namespace": config.namespace,
        "service_account": config.service_account,
        # Match submission's name normalization so the planned identity equals the created one;
        # rollback only deletes a driver whose identity matches the plan.
        "app_name": re.sub(r"-+", "-", config.app_name[:22]).rstrip("-"),
        "app_id_suffix": lambda: f"-{token}",
        "class_name": _SERVER_CLASS,
        "app_arguments": [],
        "app_waiter": "no_wait",
        "should_print": False,
        "image_pull_policy": config.image_pull_policy,
        "spark_conf": {
            **config.spark_conf,
            **pod["spark_conf"],
            "spark.connect.grpc.binding.address": "0.0.0.0",
            "spark.connect.grpc.binding.port": str(config.port),
        },
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
        "executor_node_selector": dict(config.executor_node_selector),
        "driver_annotations": {},
        "executor_annotations": {},
        "driver_labels": {_SERVER_LABEL: "true"},
        "executor_labels": {},
        "driver_tolerations": pod["driver_tolerations"],
        "executor_pod_template_path": "",
        "executor_pod_template": pod["executor_pod_template"],
        "startup_timeout": 0,
        "wait_timeout": None,
        "driver_init_containers": [],
        "driver_host_aliases": [],
        "driver_pod_template": pod["driver_pod_template"],
        "driver_container_name": None,
        "max_runtime": config.max_runtime,
    }


def _deadline(timeout):
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    return WaitDeadline(timeout)


def _api(raw):
    # A retry would restart urllib3's per-request timeout with the same budget.
    # This is per ApiClient, preserving TLS/proxy/auth configuration and global defaults.
    # urllib3 includes retries in its pool key, so cached pools with retries stay untouched.
    raw.rest_client.pool_manager.connection_pool_kw["retries"] = 0
    return k8s.CoreV1Api(raw)


def _read(api, kind, name, namespace, deadline):
    try:
        return getattr(api, f"read_namespaced_{kind}")(
            name=name, namespace=namespace, **deadline.request_options()
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise


def _driver(run, pod):
    if pod is None:
        return
    labels = pod.metadata.labels or {}
    if (
        pod.metadata.uid != run.driver_uid
        or pod.metadata.name != run.driver_pod
        or pod.metadata.namespace != run.namespace
        or labels.get("spark-app-id") != run.app_id
        or labels.get("spark-role") != "driver"
        or labels.get(_SERVER_LABEL) != "true"
    ):
        raise ConnectServerOwnershipError(
            "The recorded Connect driver identity does not match the current pod", run=run
        )


def _service(run, service, *, name, uid, port=None):
    if service is None:
        return
    labels = service.metadata.labels or {}
    owners = service.metadata.owner_references or []
    selector = service.spec.selector or {}
    identity = {"spark-app-id": run.app_id, "spark-role": "driver"}
    if (
        service.metadata.name != name
        or service.metadata.namespace != run.namespace
        or (uid is not None and service.metadata.uid != uid)
        or any(labels.get(k) != v for k, v in identity.items())
        or any(selector.get(k) != v for k, v in identity.items())
        or len(owners) != 1
        or owners[0].uid != run.driver_uid
        or owners[0].name != run.driver_pod
        or owners[0].kind != "Pod"
        or owners[0].api_version != "v1"
    ):
        raise ConnectServerOwnershipError(
            "The recorded Connect Service identity or owner does not match", run=run
        )
    _uid(service.metadata.uid)
    if port is None:
        ports = service.spec.ports or []
        if (
            service.spec.type != "ClusterIP"
            or service.spec.cluster_ip != "None"
            or service.spec.external_ips
            or service.spec.external_name
            or len(ports) != 2
            or {(p.port, p.target_port or p.port, p.protocol or "TCP") for p in ports}
            != {(7077, 7077, "TCP"), (4040, 4040, "TCP")}
        ):
            raise ConnectServerOwnershipError(
                "The recorded driver Service routing or private exposure changed", run=run
            )
    if port is not None:
        expected = {**identity, _SERVER_LABEL: "true"}
        ports = service.spec.ports or []
        if (
            service.spec.type != "ClusterIP"
            or service.spec.cluster_ip == "None"
            or service.spec.external_ips
            or service.spec.external_name
            or selector != expected
            or labels.get(_SERVER_LABEL) != "true"
            or len(ports) != 1
            or ports[0].port != port
            or ports[0].target_port != port
            or (ports[0].protocol or "TCP") != "TCP"
        ):
            raise ConnectServerOwnershipError(
                "The recorded Connect Service routing or private exposure changed", run=run
            )


def _grpc_service(app_id: str, namespace: str, port: int, owner=None) -> k8s.V1Service:
    labels = {"spark-app-id": app_id, "spark-role": "driver", _SERVER_LABEL: "true"}
    return k8s.V1Service(
        metadata=k8s.V1ObjectMeta(
            name=f"{app_id}-connect",
            namespace=namespace,
            labels=labels,
            owner_references=None if owner is None else [owner],
        ),
        spec=k8s.V1ServiceSpec(
            type="ClusterIP",
            selector=labels,
            ports=[k8s.V1ServicePort(name="grpc", port=port, target_port=port, protocol="TCP")],
        ),
    )


def plan_connect_server(client: SparkOnK8S, config: ConnectServerConfig) -> ApplicationPlan:
    """Render the resources create_connect_server would request, without cluster access.

    The plan uses the same submission builder as creation plus the gRPC Service. Secret
    settings appear only as Secret names and keys: no Secret is read, so no value can
    appear. Identifiers are illustrative; nothing is created or reserved.
    """
    if not isinstance(config, ConnectServerConfig):
        raise ValueError("config must be ConnectServerConfig")
    config = deepcopy(config)
    config.__post_init__()
    plan = client.plan_app(**_options(config, "preview"))
    service = _grpc_service(plan.app_id, plan.namespace, config.port)
    with k8s.ApiClient() as serializer:
        service = {**serializer.sanitize_for_serialization(service), "apiVersion": "v1", "kind": "Service"}
    return replace(plan, resources=(*plan.resources, service))


def create_connect_server(client: SparkOnK8S, config: ConnectServerConfig) -> ConnectServerHandle:
    """Create a driver and private gRPC Service, without claiming query readiness.

    A failed create attempts rollback only after receiving a confirmed driver UID.
    A timeout with no confirmed UID is uncertain: no retry, adoption or deletion occurs.
    The raised error retains the planned/confirmed run and rollback outcome.
    """
    if not isinstance(config, ConnectServerConfig):
        raise ValueError("config must be ConnectServerConfig")
    config = deepcopy(config)
    config.__post_init__()  # Nested resource/mapping objects may have changed since construction.
    token = uuid4().hex
    options = _options(config, token)
    app_id = f"{options['app_name']}-{token}"
    planned = RunHandle(namespace=config.namespace, app_id=app_id, driver_pod=f"{app_id}-driver")
    run = None
    handle = None
    phase = "driver_submission"
    try:
        run = client.submit_run(**options)
        _uid(run.driver_uid)
        phase = "driver_service_verification"
        with client.k8s_client_manager.client() as raw:
            api = _api(raw)
            deadline = WaitDeadline(10)
            pod = _read(api, "pod", run.driver_pod, run.namespace, deadline)
            transport = _read(api, "service", run.app_id, run.namespace, deadline)
            _driver(run, pod)
            _service(run, transport, name=run.app_id, uid=None)
            if pod is None or transport is None:
                raise ConnectServerError("Submitted Connect driver or driver Service is missing", run=run)
            phase = "grpc_service_creation"
            name = f"{run.app_id}-connect"
            body = _grpc_service(
                run.app_id,
                run.namespace,
                config.port,
                owner=k8s.V1OwnerReference(
                    api_version="v1", kind="Pod", name=run.driver_pod, uid=run.driver_uid
                ),
            )
            service = api.create_namespaced_service(
                namespace=run.namespace, body=body, **deadline.request_options()
            )
            handle = ConnectServerHandle(
                run=run,
                service_name=name,
                service_uid=service.metadata.uid,
                driver_service_uid=transport.metadata.uid,
                port=config.port,
            )
            _service(run, service, name=name, uid=handle.service_uid, port=config.port)
        return handle
    except Exception as exc:
        confirmed = run or client.last_submitted_run
        if confirmed is not None and (
            confirmed.namespace != planned.namespace
            or confirmed.app_id != planned.app_id
            or confirmed.driver_pod != planned.driver_pod
        ):
            confirmed = None
        cleanup = {"status": "not_attempted", "reason": "Driver creation is unconfirmed; resources may exist"}
        if confirmed is not None and confirmed.driver_uid:
            try:
                client.cancel_run(confirmed)
                cleanup = {
                    "status": "requested",
                    "reason": (
                        "UID-checked driver deletion requested; owned resources "
                        "are garbage-collected asynchronously"
                    ),
                }
            except KeyboardInterrupt as interrupted:
                interrupted.handle = handle
                interrupted.run = confirmed
                interrupted.phase = "rollback"
                interrupted.cleanup = {
                    "status": "unknown",
                    "reason": "Rollback was interrupted; deletion may already have been requested",
                }
                raise
            except Exception:
                cleanup = {
                    "status": "failed",
                    "reason": "UID-checked rollback failed; inspect the recorded run before retrying cleanup",
                }
        raise ConnectServerCreateError(
            "Spark Connect server creation failed",
            handle=handle,
            run=confirmed or planned,
            phase=phase,
            cleanup=cleanup,
        ) from exc


def _resources(api, handle, deadline):
    run = handle.run
    pod = _read(api, "pod", run.driver_pod, run.namespace, deadline)
    transport = _read(api, "service", run.app_id, run.namespace, deadline)
    service = _read(api, "service", handle.service_name, run.namespace, deadline)
    _driver(run, pod)
    _service(run, transport, name=run.app_id, uid=handle.driver_service_uid)
    _service(run, service, name=handle.service_name, uid=handle.service_uid, port=handle.port)
    return pod, transport, service


def _driver_state(phase: str | None, deleting: bool) -> str:
    if deleting:
        return "deleting"
    return {
        "Succeeded": "terminated",
        "Failed": "terminated",
        "Running": "running",
        "Pending": "starting",
    }.get(phase, "unknown")


def _created(pod, now: datetime) -> tuple[datetime | None, int | None]:
    """Return the driver creation time in UTC and its age by the local clock, if known."""
    created = pod.metadata.creation_timestamp if pod is not None else None
    if not isinstance(created, datetime):
        return None, None
    created = created.astimezone(UTC) if created.tzinfo else created.replace(tzinfo=UTC)
    # Clock skew between this machine and the API server must not produce a negative age.
    return created, max(0, int((now - created).total_seconds()))


def _timestamp(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value is not None else None


def connect_server_status(client: SparkOnK8S, handle: ConnectServerHandle, *, timeout: float = 10) -> dict:
    """Read matching resource state. Only confirmed 404 means missing; errors propagate."""
    if not isinstance(handle, ConnectServerHandle):
        raise ValueError("handle must be ConnectServerHandle")
    deadline = _deadline(timeout)
    with client.k8s_client_manager.client() as raw:
        pod, transport, service = _resources(_api(raw), handle, deadline)
    missing = [
        name
        for name, obj in (("driver", pod), ("driver_service", transport), ("connect_service", service))
        if obj is None
    ]
    phase = pod.status.phase if pod is not None and pod.status else None
    # Kubernetes sets a pod-level reason such as DeadlineExceeded (max_runtime) or Evicted.
    reason = getattr(pod.status, "reason", None) if pod is not None and pod.status else None
    deleting = any(
        obj is not None and obj.metadata.deletion_timestamp is not None for obj in (pod, transport, service)
    )
    created, age = _created(pod, datetime.now(UTC))
    return {
        "status": "missing" if missing else _driver_state(phase, deleting),
        "driver_phase": phase,
        "driver_reason": reason if isinstance(reason, str) else None,
        "missing": missing,
        "endpoint": handle.endpoint,
        "created_at": _timestamp(created),
        "age_seconds": age,
        # Kubernetes observation cannot learn the server version; only an optional label is reported.
        "spark_version": (pod.metadata.labels or {}).get("spark-version") if pod is not None else None,
        "query_ready": None,
        "message": "Kubernetes observation only; query readiness has not been checked",
    }


def list_connect_servers(client: SparkOnK8S, namespace: str, *, timeout: float = 10) -> list[dict]:
    """List driver pods labeled as servers created by this helper in one namespace, oldest first.

    This is a read-only, label-based observation: it neither verifies receipts, Services
    or query readiness nor deletes anything. A matching label is not proof of ownership;
    deletion still requires the recorded identity checks. Errors propagate.
    """
    _name(namespace, "namespace")
    deadline = _deadline(timeout)
    with client.k8s_client_manager.client() as raw:
        pods = _api(raw).list_namespaced_pod(
            namespace=namespace,
            label_selector=f"spark-role=driver,{_SERVER_LABEL}=true",
            **deadline.request_options(),
        )
    now = datetime.now(UTC)
    servers = []
    for pod in pods.items:
        labels = pod.metadata.labels or {}
        if not labels.get("spark-app-id"):
            continue  # The helper always records an application ID on its driver.
        phase = pod.status.phase if pod.status else None
        containers = (pod.spec.containers if pod.spec else None) or []
        name = driver_container_name(pod) if containers else None
        created, age = _created(pod, now)
        servers.append(
            (
                created,
                {
                    "app_id": labels["spark-app-id"],
                    "name": labels.get("spark-app-name"),
                    "namespace": pod.metadata.namespace or namespace,
                    "driver_pod": pod.metadata.name,
                    "driver_uid": pod.metadata.uid,
                    "status": _driver_state(phase, pod.metadata.deletion_timestamp is not None),
                    "driver_phase": phase,
                    "spark_version": labels.get("spark-version"),
                    "image": next((c.image for c in containers if c.name == name), None),
                    "created_at": _timestamp(created),
                    "age_seconds": age,
                },
            )
        )
    servers.sort(key=lambda item: (item[0] is None, item[0] or now, item[1]["app_id"]))
    return [server for _, server in servers]


def wait_connect_server_ready(
    client: SparkOnK8S,
    handle: ConnectServerHandle,
    *,
    profile: ConnectProfile,
    timeout: float = 60,
    poll_interval: float = 1,
) -> dict:
    """Require a real query against this application, then revalidate Kubernetes identity."""
    if not isinstance(profile, ConnectProfile):
        raise ValueError("profile must be a reachable ConnectProfile")
    if type(poll_interval) not in {int, float} or not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("poll_interval must be positive and finite")
    deadline = _deadline(timeout)
    last_failure = None
    try:
        while True:
            status = connect_server_status(client, handle, timeout=deadline.remaining())
            if status["status"] in {"missing", "deleting", "terminated"}:
                raise ConnectServerError(
                    "Connect server is missing, terminating, or terminated", handle=handle, phase="readiness"
                )
            if status["status"] == "running":
                try:
                    # The same per-check budget as `connect check`: a slower client start is not unreadiness.
                    result = profile.probe(
                        timeout=min(15, deadline.remaining()), expected_app_id=handle.run.app_id
                    )
                except ConnectClientUnavailable:
                    # A missing local client is not a server state; polling cannot make it ready.
                    raise
                except (ConnectError, TimeoutError) as failure:
                    # Reported if the wait times out, for example when another application answers.
                    last_failure = str(failure)
                else:
                    after = connect_server_status(client, handle, timeout=deadline.remaining())
                    if after["status"] == "running":
                        return {
                            **after,
                            "status": "ready",
                            "query_ready": True,
                            # The query reports the running server's version, unlike the optional label.
                            "spark_version": result.get("spark_version"),
                            "client_version": result.get("client_version"),
                            "warnings": list(result.get("warnings") or []),
                            "probe": result,
                            "message": "Connect query and application identity verified",
                        }
            deadline.sleep(poll_interval)
    except TimeoutError:
        raise ConnectServerTimeout(handle, last_failure) from None
    except (TransportTimeoutError, MaxRetryError) as error:
        # urllib3 can wrap a request timeout once the remaining wait budget is tiny.
        # Only the enclosing deadline expiring changes that transport error into a wait timeout.
        reason = error.reason if isinstance(error, MaxRetryError) else error
        if isinstance(reason, TransportTimeoutError) and not isinstance(reason, NewConnectionError):
            try:
                deadline.remaining()
            except TimeoutError:
                raise ConnectServerTimeout(handle, last_failure) from None
        raise


def delete_connect_server(client: SparkOnK8S, handle: ConnectServerHandle, *, timeout: float = 10) -> dict:
    """Verify all three resource identities before requesting UID-conditional deletion.

    Missing resources are idempotent. Success means deletion was requested, not that
    executors or Services have already disappeared. A concurrent change can interrupt
    a partially applied deletion; UID preconditions protect replacement resources.
    """
    if not isinstance(handle, ConnectServerHandle):
        raise ValueError("handle must be ConnectServerHandle")
    deadline = _deadline(timeout)
    requested = []
    with client.k8s_client_manager.client() as raw:
        api = _api(raw)
        pod, transport, service = _resources(api, handle, deadline)
        for kind, obj in (("service", service), ("service", transport), ("pod", pod)):
            if obj is None:
                continue
            try:
                getattr(api, f"delete_namespaced_{kind}")(
                    name=obj.metadata.name,
                    namespace=handle.run.namespace,
                    body=k8s.V1DeleteOptions(
                        propagation_policy="Foreground",
                        preconditions=k8s.V1Preconditions(uid=obj.metadata.uid),
                    ),
                    **deadline.request_options(),
                )
            except ApiException as exc:
                if exc.status != 404:
                    raise
            requested.append({"kind": kind, "name": obj.metadata.name, "uid": obj.metadata.uid})
    return {
        "status": "deletion_requested" if requested else "absent",
        "requested": requested,
        "message": "Resource deletion and executor garbage collection are asynchronous",
    }
