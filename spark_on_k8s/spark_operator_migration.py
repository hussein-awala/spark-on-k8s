"""Read-only assessment of Kubeflow Spark Operator manifests for a SparkJob migration.

The assessor reads ``sparkoperator.k8s.io/v1beta2`` SparkApplication and
ScheduledSparkApplication documents and gives every field one accounting status:
``mapped``, ``approximated``, ``unsupported`` or ``ignored``. It drafts a SparkJob
document from the settings that translate and validates that draft offline. It
never contacts a cluster and never translates a field it does not recognize.
Values that look like credentials are neither shown nor copied into a draft.
"""

from __future__ import annotations

import copy
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from spark_on_k8s.jobs import API_VERSION, MAX_JOB_BYTES, JobSpec
from spark_on_k8s.planning import _SENSITIVE, redact

SCHEMA_VERSION = 1
OPERATOR_API_VERSION = "sparkoperator.k8s.io/v1beta2"
OPERATOR_KINDS = ("SparkApplication", "ScheduledSparkApplication")
MAPPED, APPROXIMATED, UNSUPPORTED, IGNORED = "mapped", "approximated", "unsupported", "ignored"
STATUSES = (MAPPED, APPROXIMATED, UNSUPPORTED, IGNORED)


class ManifestError(ValueError):
    """The input cannot be read as Spark Operator manifests."""


class _ManifestLoader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ManifestError("YAML aliases are not supported in assessed manifests.")
        return super().compose_node(parent, index)


def _construct_mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str) or key in result:
            raise ManifestError("Manifest keys must be unique strings; YAML merge keys are not supported.")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_ManifestLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)

_SECRET_NOTE = "The value looks like a credential, so it is neither shown nor copied into the draft."
_SECRET_ALT = (
    "Store it in a Kubernetes Secret and reference it: valueFrom.secretKeyRef in driver_pod_template for "
    "the driver, spark.kubernetes.executor.secretKeyRef.NAME for executors, or workload identity for "
    "cloud storage."
)
_UNKNOWN = (
    "Unrecognized Spark Operator field; it was not translated.",
    "Check what it does in your operator version and express it with SparkJob settings or pod templates.",
)
_RETRY = (
    "spark-on-k8s submits once; no controller resubmits a failed, evicted or deleted application.",
    "Retry from the caller (Airflow task retries, or a Kubernetes Job or CronJob backoffLimit around "
    "`job submit` with a wait waiter). Every retry is a new submission; keep outputs idempotent.",
)
_SCHEDULE = (
    "spark-on-k8s has no scheduler.",
    "Run `spark-on-k8s job submit` from a Kubernetes CronJob or an Airflow DAG; see the CronJob recipe.",
)
_UI = (
    "The client creates no Spark UI Service or Ingress.",
    "Use the spark-on-k8s API Spark UI proxy (spec.ui_reverse_proxy: true) or kubectl port-forward.",
)
_DRIVER_POD = (
    "Spark applies this to the driver pod only when spark-submit creates it in cluster mode; "
    "spark-on-k8s creates the driver pod directly.",
    "Use driver_pod_template or the matching SparkJob field "
    "(service_account, driver_labels, driver_annotations, driver_node_selector).",
)
_OVERHEAD_FACTOR = (
    "SparkJob always sets an explicit memory overhead, so a factor has no effect.",
    "Set memory_overhead in driver_resources and executor_resources.",
)
_EXECUTOR_IMAGE = "spark.kubernetes.executor.container.image"
_MAP_TARGETS = {
    "spark_conf",
    "driver_labels",
    "executor_labels",
    "driver_annotations",
    "executor_annotations",
    "driver_node_selector",
    "executor_node_selector",
}
_DRAFT_ORDER = (
    "image",
    "app_path",
    "class_name",
    "app_arguments",
    "namespace",
    "service_account",
    "image_pull_policy",
    "packages",
    "driver_resources",
    "executor_resources",
    "executor_instances",
    "driver_labels",
    "executor_labels",
    "driver_annotations",
    "executor_annotations",
    "driver_node_selector",
    "executor_node_selector",
    "spark_conf",
    "executor_pod_template_path",
    "driver_pod_template",
    "executor_pod_template",
)
_RESOURCE_CONF = {
    "spark.driver.memory": ("driver_resources", "memory"),
    "spark.driver.memoryOverhead": ("driver_resources", "memory_overhead"),
    "spark.driver.cores": ("driver_resources", "cpu"),
    "spark.executor.memory": ("executor_resources", "memory"),
    "spark.executor.memoryOverhead": ("executor_resources", "memory_overhead"),
    "spark.executor.cores": ("executor_resources", "cpu"),
}
_ALLOCATION_CONF = {
    "spark.executor.instances": "instances",
    "spark.dynamicAllocation.enabled": "enabled",
    "spark.dynamicAllocation.minExecutors": "min",
    "spark.dynamicAllocation.maxExecutors": "max",
    "spark.dynamicAllocation.initialExecutors": "initial",
}
_MANAGED_CONF = {
    "spark.app.name": "Set metadata.name in the SparkJob.",
    "spark.kubernetes.namespace": "Set spec.namespace.",
    "spark.kubernetes.container.image": "Set spec.image.",
    "spark.kubernetes.container.image.pullPolicy": "Set spec.image_pull_policy.",
    **dict.fromkeys(
        (
            "spark.app.id",
            "spark.master",
            "spark.submit.deployMode",
            "spark.driver.host",
            "spark.driver.port",
            "spark.kubernetes.executor.podNamePrefix",
        ),
        "Remove it; the client derives it from the generated application ID and in-cluster driver.",
    ),
}
_FACTOR_CONF = {
    "spark.kubernetes.memoryOverheadFactor",
    "spark.driver.memoryOverheadFactor",
    "spark.executor.memoryOverheadFactor",
}
_DRIVER_POD_CONF = (
    "spark.kubernetes.driver.secrets.",
    "spark.kubernetes.driver.secretKeyRef.",
    "spark.kubernetes.driver.volumes.",
    "spark.kubernetes.driver.service.",
    "spark.kubernetes.driver.pod.",
    "spark.kubernetes.driver.podTemplate",
    "spark.kubernetes.driver.request.cores",
    "spark.kubernetes.driver.limit.cores",
    "spark.kubernetes.driver.container.image",
    "spark.kubernetes.driver.scheduler.name",
    "spark.kubernetes.authenticate.driver.",
    "spark.kubernetes.authenticate.submission.",
    "spark.kubernetes.submission.",
    "spark.kubernetes.file.upload.path",
)
_DRIVER_MAP_CONF = {
    "spark.kubernetes.driver.label.": ("driver_labels",),
    "spark.kubernetes.driver.annotation.": ("driver_annotations",),
    "spark.kubernetes.driver.node.selector.": ("driver_node_selector",),
    "spark.kubernetes.node.selector.": ("driver_node_selector", "executor_node_selector"),
}
# Keys that name Secrets or credential providers rather than containing credentials.
_REFERENCE_KEY = re.compile(
    r"^spark\.kubernetes\.(?:driver|executor)\.(?:secretKeyRef|secrets)\.|credentials?\.provider$"
)
_MEMORY = re.compile(r"^(\d+)([kmgtp]?b?)$", re.IGNORECASE)
_MEMORY_KIB = {"b": 1 / 1024, "k": 1, "m": 1024, "g": 1024**2, "t": 1024**3, "p": 1024**4}
_CPU = re.compile(r"^(\d+(?:\.\d+)?)(m?)$")
_SCHEME = re.compile(r"^([a-z][a-z0-9+.-]*):", re.IGNORECASE)


@dataclass(frozen=True)
class Finding:
    """The accounting status of one source field; it covers any fields nested below it."""

    path: str
    status: str
    target: str | None = None
    note: str | None = None
    alternative: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "status": self.status,
            "target": self.target,
            "note": self.note,
            "alternative": self.alternative,
        }


def _sensitive_key(key: str) -> bool:
    return (
        bool(_SENSITIVE.search(key))
        and not key.endswith(("KeyRef", "secretRef"))
        and not _REFERENCE_KEY.search(key)
    )


def _value_secret(value: Any) -> bool:
    """Detect credentials embedded in values (URL user info, signed URLs, NAME=secret tokens)."""
    if isinstance(value, str):
        return any(redact(part) != part for part in (value, *value.split()))
    if isinstance(value, dict):
        return any(_value_secret(item) for item in value.values())
    if isinstance(value, list):
        return any(_value_secret(item) for item in value)
    return False


def _container_secret(container: dict) -> bool:
    env = container.get("env", [])
    literal = any(
        isinstance(entry, dict)
        and isinstance(entry.get("name"), str)
        and _sensitive_key(entry["name"])
        and entry.get("value")
        for entry in (env if isinstance(env, list) else [])
    )
    return literal or _value_secret(container)


def _arguments_secret(arguments: list[str]) -> bool:
    for index, argument in enumerate(arguments):
        if _value_secret(argument):
            return True
        if argument.startswith("-") and "=" not in argument and _sensitive_key(argument.lstrip("-")):
            if index + 1 < len(arguments):
                return True
    return False


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _text_list(value: Any) -> bool:
    return isinstance(value, list) and all(_text(item) for item in value)


def _string_map(value: Any) -> bool:
    return isinstance(value, dict) and all(isinstance(item, str) for item in value.values())


def _count(value: Any, minimum: int = 0) -> bool:
    return type(value) is int and value >= minimum


def _conf_string(value: Any) -> str:
    return str(value).lower() if isinstance(value, bool) else str(value)


def _mebibytes(value: Any) -> tuple[int, bool] | None:
    """Spark memory strings default to MiB; return the whole MiB value and whether it was exact."""
    match = _MEMORY.fullmatch(value.strip()) if isinstance(value, str) else None
    if match is None:
        return None
    suffix = match[2].lower()
    kib = int(match[1]) * _MEMORY_KIB[suffix[:1] or "m"]
    return math.ceil(kib / 1024), kib % 1024 == 0


def _cores(value: Any) -> float | None:
    """Kubernetes CPU quantities such as 500m or 1.5; the CRD stores them as strings."""
    match = _CPU.fullmatch(value.strip()) if isinstance(value, str) else None
    quantity = float(match[1]) / (1000 if match[2] else 1) if match else 0
    return quantity if quantity > 0 else None


def _render(target: tuple[str, ...]) -> str:
    head, *rest = target
    if head in _MAP_TARGETS:
        return f"spec.{head}" + "".join(f"[{part}]" for part in rest)
    return ".".join(("spec", head, *rest))


def _job_name(name: str) -> str:
    text = re.sub(r"[^a-z0-9-]+", "-", name.lower())
    text = re.sub(r"^[^a-z]+", "", text)[:63].rstrip("-")
    return text or "spark-app"


class _Pod:
    """Driver or executor Pod template fields collected from one SparkPodSpec."""

    def __init__(self, role: str):
        self.role = role
        self.container: dict[str, Any] = {"name": role}
        self.spec: dict[str, Any] = {}
        self.volumes: dict[str, dict] = {}
        self.sidecars: dict[str, dict] = {}

    def target(self, field: str, *, container: bool = False) -> str:
        base = f"spec.{self.role}_pod_template.spec"
        return f"{base}.containers[{self.role}].{field}" if container else f"{base}.{field}"

    def set(self, field: str, value: Any, *, container: bool = False) -> None:
        # Each template field has exactly one source field, so this never overwrites.
        (self.container if container else self.spec)[field] = copy.deepcopy(value)

    def append(self, field: str, item: Any, key: str | None, *, container: bool = False) -> str | None:
        owner = self.container if container else self.spec
        items = owner.setdefault(field, [])
        for existing in items:
            if existing == item:
                return None
            if key is not None and existing.get(key) == item.get(key):
                target = self.target(field, container=container)
                return f"{target} already has an entry with {key} {item[key]!r}."
        items.append(copy.deepcopy(item))
        return None

    def add_volume(self, volume: dict) -> str | None:
        existing = self.volumes.get(volume["name"])
        if existing is not None and existing != volume:
            return f"{self.target('volumes')} already has a different volume named {volume['name']!r}."
        self.volumes[volume["name"]] = copy.deepcopy(volume)
        return None

    def add_sidecar(self, container: dict) -> str | None:
        name = container["name"]
        if name == self.role or name in self.sidecars:
            return f"spec.{self.role}_pod_template already has a container named {name!r}."
        self.sidecars[name] = copy.deepcopy(container)
        return None

    def document(self) -> dict[str, Any] | None:
        if len(self.container) == 1 and not (self.spec or self.volumes or self.sidecars):
            return None
        spec = {"containers": [self.container, *self.sidecars.values()], **copy.deepcopy(self.spec)}
        if self.volumes:
            spec["volumes"] = list(self.volumes.values())
        return {"apiVersion": "v1", "kind": "Pod", "spec": spec}


@dataclass
class _PodContext:
    role: str
    pod: _Pod
    source: dict
    application: dict
    volumes: dict[str, dict]


class _Assessment:
    def __init__(self):
        self.findings: list[Finding] = []
        self.values: dict[tuple[str, ...], tuple[Any, str]] = {}
        self.allocation: dict[str, tuple[Any, list[str]]] = {}
        self.pods = {"driver": _Pod("driver"), "executor": _Pod("executor")}
        self.volume_users: dict[str, set[str]] = {}
        self.rejected_volumes: set[str] = set()
        self.notes: list[str] = []

    def record(self, path, status, target=None, note=None, alternative=None):
        self.findings.append(Finding(path, status, target, note, alternative))

    def unsupported(self, path, note, alternative=None):
        self.record(path, UNSUPPORTED, note=note, alternative=alternative)

    def unknown(self, path):
        self.unsupported(path, *_UNKNOWN)

    def invalid(self, path, expected):
        self.unsupported(
            path,
            f"Unexpected value; expected {expected}.",
            "Correct the source manifest and assess it again.",
        )

    def secret(self, path):
        self.unsupported(path, _SECRET_NOTE, _SECRET_ALT)

    def assign(self, path, assignments, *, status=MAPPED, note=None, extra=(), record=True) -> bool:
        """Set SparkJob values; a conflicting earlier source makes this field unsupported."""
        for target, value in assignments:
            previous = self.values.get(target)
            if previous is not None and previous[0] != value:
                self.unsupported(
                    path,
                    f"Conflicts with {previous[1]}, which sets {_render(target)} to a different value.",
                    "Keep one value in the source manifest.",
                )
                return False
        for target, value in assignments:
            self.values.setdefault(target, (copy.deepcopy(value), path))
        if record:
            targets = [_render(target) for target, _ in assignments] + list(extra)
            self.record(path, status, ", ".join(targets) or None, note)
        return True

    def assign_default(self, path, assignments) -> list[tuple[str, ...]]:
        """Set lower-precedence values that a more specific source has not already set."""
        applied = [(target, value) for target, value in assignments if target not in self.values]
        for target, value in applied:
            self.values[target] = (value, path)
        return [target for target, _ in applied]

    def template(self, path, conflict, target, *, status=MAPPED, note=None):
        if conflict:
            self.unsupported(path, conflict, "Keep one definition in the source manifest.")
        else:
            self.record(path, status, target, note)

    def allocate(self, name, value, path):
        previous = self.allocation.get(name)
        if previous is not None and previous[0] != value:
            self.unsupported(
                path,
                f"Conflicts with {previous[1][0]}, which sets a different executor count.",
                "Keep one value in the source manifest.",
            )
            return
        self.allocation.setdefault(name, (value, []))[1].append(path)


# SparkApplication spec fields.


def _application(a: _Assessment, spec: dict, prefix: str) -> None:
    volumes = {}
    for volume in spec.get("volumes") if isinstance(spec.get("volumes"), list) else []:
        if isinstance(volume, dict) and _text(volume.get("name")):
            volumes.setdefault(volume["name"], volume)
    for role in ("driver", "executor"):
        value = spec.get(role)
        if isinstance(value, dict):
            _pod_spec(a, f"{prefix}.{role}", _PodContext(role, a.pods[role], value, spec, volumes))
    for key, value in spec.items():
        path = f"{prefix}.{key}"
        if value is None:
            a.record(path, IGNORED, note="Null; treated as unset.")
        elif key in ("driver", "executor"):
            if not isinstance(value, dict):
                a.invalid(path, "an object")
        elif key in _APPLICATION_FIELDS:
            _APPLICATION_FIELDS[key](a, path, value, spec)
        else:
            a.unknown(path)
    _finish_allocation(a)


def _image(a, path, value, spec):
    if not _text(value):
        return a.invalid(path, "a string")
    driver = _pod_field(spec, "driver", "image")
    executor = _pod_field(spec, "executor", "image")
    if not driver:
        a.assign(path, [(("image",), value)])
    elif not executor and driver != value:
        a.assign(path, [(("spark_conf", _EXECUTOR_IMAGE), value)])
    elif not executor:
        a.record(path, MAPPED, "spec.image", "Same as spec.driver.image.")
    else:
        a.record(path, IGNORED, note="spec.driver.image and spec.executor.image both override it.")


def _pod_field(spec, role, field):
    pod = spec.get(role)
    return pod.get(field) if isinstance(pod, dict) and _text(pod.get(field)) else None


def _choice(target, choices):
    def handler(a, path, value, spec):
        if value not in choices:
            return a.invalid(path, f"one of {', '.join(sorted(choices))}")
        a.assign(path, [(target, value)])

    return handler


def _main_class(a, path, value, spec):
    if not _text(value):
        return a.invalid(path, "a string")
    a.assign(path, [(("class_name",), value)])


def _main_file(a, path, value, spec):
    if not _text(value):
        return a.invalid(path, "a string")
    if _value_secret(value):
        return a.secret(path)
    scheme = (_SCHEME.match(value) or [None, ""])[1].lower()
    if scheme in {"", "file"}:
        a.assign(
            path,
            [(("app_path",), value)],
            status=APPROXIMATED,
            note="spark-submit reads it inside the driver pod, so it must exist in the image; "
            "the operator would treat it as a file to upload from the submitter.",
        )
    else:
        a.assign(path, [(("app_path",), value)])


def _arguments(a, path, value, spec):
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return a.invalid(path, "a list of strings")
    if _arguments_secret(value):
        return a.secret(path)
    a.assign(path, [(("app_arguments",), value)])


def _spark_conf(a, path, value, spec):
    if not isinstance(value, dict):
        return a.invalid(path, "an object")
    for key, item in value.items():
        _spark_property(a, f"{path}[{key}]", key, item)


def _hadoop_conf(a, path, value, spec):
    if not isinstance(value, dict):
        return a.invalid(path, "an object")
    for key, item in value.items():
        _spark_property(a, f"{path}[{key}]", f"spark.hadoop.{key}", item)


def _spark_property(a: _Assessment, path: str, key: str, value: Any) -> None:
    # The CRD stores strings; unquoted YAML scalars are accepted as their string form.
    if not isinstance(value, (str, int, float)):
        return a.invalid(path, "a string")
    text = _conf_string(value)
    if _sensitive_key(key) or _value_secret(text):
        return a.unsupported(
            path,
            _SECRET_NOTE + " Spark properties appear in driver arguments and the Spark UI.",
            _SECRET_ALT,
        )
    if key in _RESOURCE_CONF:
        return _resource(a, path, _RESOURCE_CONF[key], text)
    if key in _ALLOCATION_CONF:
        return _allocation_value(a, path, _ALLOCATION_CONF[key], text)
    for prefix, heads in _DRIVER_MAP_CONF.items():
        if key.startswith(prefix) and len(key) > len(prefix):
            return a.assign(path, [((head, key[len(prefix) :]), text) for head in heads])
    if key == "spark.kubernetes.authenticate.driver.serviceAccountName":
        return a.assign(path, [(("service_account",), text)])
    if key == "spark.kubernetes.executor.podTemplateFile":
        return a.assign(
            path,
            [(("executor_pod_template_path",), text)],
            status=APPROXIMATED,
            note="Spark reads this path inside the driver pod; it must exist in the image or a volume.",
        )
    if key in _MANAGED_CONF:
        return a.unsupported(path, "spark-on-k8s sets this property itself.", _MANAGED_CONF[key])
    if key in _FACTOR_CONF:
        return a.unsupported(path, *_OVERHEAD_FACTOR)
    if key.startswith(_DRIVER_POD_CONF):
        return a.unsupported(path, *_DRIVER_POD)
    a.assign(path, [(("spark_conf", key), text)])


def _resource(a, path, target, value):
    if target[1] == "cpu":
        if not value.isdigit() or int(value) < 1:
            return a.invalid(path, "a whole number of cores")
        return a.assign(path, [(target, int(value))])
    size = _mebibytes(value)
    if size is None:
        return a.invalid(path, "a Spark memory size such as 512m or 2g")
    mebibytes, exact = size
    a.assign(
        path,
        [(target, mebibytes)],
        status=MAPPED if exact else APPROXIMATED,
        note=None if exact else "Rounded up to whole MiB.",
    )


def _allocation_value(a, path, name, text):
    if name == "enabled":
        if text.lower() not in {"true", "false"}:
            return a.invalid(path, "true or false")
        return a.allocate(name, text.lower() == "true", path)
    if not text.isdigit():
        return a.invalid(path, "a whole number of executors")
    a.allocate(name, int(text), path)


def _finish_allocation(a: _Assessment) -> None:
    def value(name):
        return a.allocation.get(name, (None, []))

    enabled, enabled_paths = value("enabled")
    instances, instance_paths = value("instances")
    target = ("executor_instances",)
    if enabled:
        bounds = {name: value(name)[0] for name in ("min", "max", "initial")}
        lower = bounds["min"] or 0
        result = {"min": lower, **({"max": bounds["max"]} if bounds["max"] is not None else {})}
        result["initial"] = max(lower, bounds["initial"] or 0, instances or 0)
        a.values[target] = (result, enabled_paths[0])
        for path in enabled_paths:
            a.record(
                path, MAPPED, "spec.executor_instances", "A minimum or maximum enables dynamic allocation."
            )
        for name in ("min", "max", "initial"):
            for path in value(name)[1]:
                a.record(path, MAPPED, f"spec.executor_instances.{name}")
        for path in instance_paths:
            a.record(
                path,
                APPROXIMATED,
                "spec.executor_instances.initial",
                "Spark starts with the largest of the minimum, initial and instance counts.",
            )
        return
    if instances is not None:
        a.values[target] = ({"initial": instances}, instance_paths[0])
    for path in instance_paths:
        a.record(path, MAPPED, "spec.executor_instances.initial")
    for path in enabled_paths:
        a.record(path, MAPPED, "spec.executor_instances", "Static allocation.")
    for name in ("min", "max", "initial"):
        for path in value(name)[1]:
            a.record(path, IGNORED, note="No effect while dynamic allocation is disabled.")


def _dynamic_allocation(a, path, value, spec):
    if not isinstance(value, dict):
        return a.invalid(path, "an object")
    names = {
        "enabled": "enabled",
        "minExecutors": "min",
        "maxExecutors": "max",
        "initialExecutors": "initial",
    }
    for key, item in value.items():
        item_path = f"{path}.{key}"
        if key == "enabled":
            if type(item) is not bool:
                a.invalid(item_path, "a boolean")
            else:
                a.allocate("enabled", item, item_path)
        elif key in names:
            if _count(item):
                a.allocate(names[key], item, item_path)
            else:
                a.invalid(item_path, "a whole number of executors")
        elif key == "shuffleTrackingTimeout":
            if _count(item):
                conf = "spark.dynamicAllocation.shuffleTracking.timeout"
                a.assign(item_path, [(("spark_conf", conf), f"{item}ms")])
            else:
                a.invalid(item_path, "milliseconds")
        elif key == "shuffleTrackingEnabled":
            if type(item) is bool:
                conf = "spark.dynamicAllocation.shuffleTracking.enabled"
                a.assign(item_path, [(("spark_conf", conf), _conf_string(item))])
            else:
                a.invalid(item_path, "a boolean")
        else:
            a.unknown(item_path)


_DEPENDENCY_CONF = {
    "jars": "spark.jars",
    "files": "spark.files",
    "pyFiles": "spark.submit.pyFiles",
    "archives": "spark.archives",
}


def _dependencies(a, path, value, spec):
    if not isinstance(value, dict):
        return a.invalid(path, "an object")
    for key, item in value.items():
        item_path = f"{path}.{key}"
        if not _text_list(item):
            a.invalid(item_path, "a list of strings")
        elif _value_secret(item):
            a.secret(item_path)
        elif key == "packages":
            a.assign(item_path, [(("packages",), item)])
        elif key in {"excludePackages", "repositories"}:
            conf = "spark.jars.excludes" if key == "excludePackages" else "spark.jars.repositories"
            a.assign(item_path, [(("spark_conf", conf), ",".join(item))])
        elif key in _DEPENDENCY_CONF:
            target = ("spark_conf", _DEPENDENCY_CONF[key])
            carried = [uri for uri in item if (_SCHEME.match(uri) or [None, "file"])[1].lower() != "file"]
            if carried and not a.assign(item_path, [(target, ",".join(carried))], record=False):
                continue
            for index, uri in enumerate(item):
                if uri in carried:
                    a.record(f"{item_path}[{index}]", MAPPED, _render(target))
                else:
                    a.unsupported(
                        f"{item_path}[{index}]",
                        "A local path is resolved inside the driver pod; the operator uploads "
                        "submitter-local files instead.",
                        "Bake the file into the image and reference it with local://, or host it at "
                        "a remote URI such as https:// or s3a://.",
                    )
        else:
            a.unknown(item_path)


def _restart_policy(a, path, value, spec):
    if not isinstance(value, dict):
        return a.invalid(path, "an object")
    if value.get("type") in {"OnFailure", "Always"}:
        return a.unsupported(path, *_RETRY)
    retries = {
        "onFailureRetries",
        "onFailureRetryInterval",
        "onSubmissionFailureRetries",
        "onSubmissionFailureRetryInterval",
    }
    for key, item in value.items():
        item_path = f"{path}.{key}"
        if key == "type" and item == "Never":
            a.record(item_path, MAPPED, None, "spark-on-k8s never resubmits an application.")
        elif key == "type":
            a.invalid(item_path, "Never, OnFailure or Always")
        elif key in retries:
            a.unsupported(item_path, *_RETRY)
        else:
            a.unknown(item_path)


def _node_selector(a, path, value, spec):
    if not _string_map(value):
        return a.invalid(path, "an object with string values")
    overridden = [role for role in ("driver", "executor") if _pod_field_present(spec, role, "nodeSelector")]
    for key, item in value.items():
        item_path = f"{path}[{key}]"
        applied = a.assign_default(
            item_path, [((f"{role}_node_selector", key), item) for role in ("driver", "executor")]
        )
        targets = ", ".join(_render(target) for target in applied) or None
        if overridden:
            a.record(
                item_path,
                APPROXIMATED,
                targets,
                "The operator treats spec.nodeSelector and pod-level nodeSelector as mutually exclusive; "
                "the draft merges them, and pod-level keys win.",
            )
        else:
            a.record(item_path, MAPPED, targets)


def _pod_field_present(spec, role, field):
    pod = spec.get(role)
    return isinstance(pod, dict) and bool(pod.get(field))


def _volumes(a, path, value, spec):
    if not isinstance(value, list):
        return a.invalid(path, "a list")
    seen = set()
    for index, volume in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(volume, dict) or not _text(volume.get("name")) or volume["name"] in seen:
            a.invalid(item_path, "a volume with a unique name")
            continue
        name = volume["name"]
        seen.add(name)
        if name in a.rejected_volumes:
            a.secret(item_path)
        elif name in a.volume_users:
            targets = ", ".join(a.pods[role].target("volumes") for role in sorted(a.volume_users[name]))
            a.record(item_path, MAPPED, targets)
        else:
            a.record(item_path, IGNORED, note="Not mounted by the driver or executor, so it is not added.")


def _unsupported(note, alternative):
    def handler(a, path, value, spec):
        a.unsupported(path, note, alternative)

    return handler


def _approximated(note):
    def handler(a, path, value, spec):
        if _text(value):
            a.record(path, APPROXIMATED, None, note)
        else:
            a.invalid(path, "a string")

    return handler


_APPLICATION_FIELDS = {
    "type": _approximated(
        "SparkJob has no type field: spark-submit infers it from app_path, and class_name selects the JVM "
        "main class."
    ),
    "pythonVersion": _approximated(
        "The image's Python interpreter runs the application; set spark.pyspark.python in spark_conf to "
        "choose another interpreter in the image."
    ),
    "mode": _approximated(
        "spark-on-k8s creates the driver pod itself and runs spark-submit in client mode inside it; "
        "Spark settings that shape a cluster-mode driver pod do not apply."
    ),
    "sparkVersion": _approximated(
        "SparkJob has no version field; the image determines the Spark runtime. "
        "Check it against the supported runtimes."
    ),
    "image": _image,
    "imagePullPolicy": _choice(("image_pull_policy",), {"Always", "Never", "IfNotPresent"}),
    "imagePullSecrets": lambda a, path, value, spec: _pull_secrets(a, path, value),
    "mainClass": _main_class,
    "mainApplicationFile": _main_file,
    "arguments": _arguments,
    "sparkConf": _spark_conf,
    "hadoopConf": _hadoop_conf,
    "volumes": _volumes,
    "deps": _dependencies,
    "restartPolicy": _restart_policy,
    "failureRetries": _unsupported(*_RETRY),
    "retryInterval": _unsupported(*_RETRY),
    "nodeSelector": _node_selector,
    "dynamicAllocation": _dynamic_allocation,
    "memoryOverheadFactor": _unsupported(*_OVERHEAD_FACTOR),
    "proxyUser": _unsupported(
        "spark-on-k8s does not pass --proxy-user to spark-submit.",
        "Configure impersonation in the application, or keep this workload on the operator.",
    ),
    "sparkConfigMap": _unsupported(
        "The operator mounts this ConfigMap as SPARK_CONF_DIR; the client does not.",
        "Copy its properties into spark_conf.",
    ),
    "hadoopConfigMap": _unsupported(
        "The operator mounts this ConfigMap as HADOOP_CONF_DIR; the client does not.",
        "Copy its properties into spark_conf as spark.hadoop.* entries, or mount the ConfigMap in both "
        "pod templates and set HADOOP_CONF_DIR.",
    ),
    "monitoring": _unsupported(
        "The operator's metrics exporter wiring has no SparkJob equivalent.",
        "Configure Spark metrics sinks with spark_conf (spark.metrics.conf.*) and an exporter included "
        "in the image, then scrape them with your monitoring stack.",
    ),
    "batchScheduler": _unsupported(
        "The operator integrates the batch scheduler, for example by creating Volcano PodGroups.",
        "Set schedulerName and the scheduler's labels or annotations in driver and executor pod "
        "templates; see the batch scheduler recipe.",
    ),
    "batchSchedulerOptions": _unsupported(
        "The operator passes these options to its batch scheduler integration.",
        "Express queue and priority with your scheduler's pod labels, annotations or priorityClassName "
        "in both pod templates; see the batch scheduler recipe.",
    ),
    "timeToLiveSeconds": _unsupported(
        "No controller deletes finished applications.",
        "Run `spark-on-k8s apps prune --delete --older-than SECONDS` on a schedule you own, or "
        "`spark-on-k8s app delete`; prune ages count from pod creation, not completion. Executors, "
        "the driver Service and client-created ConfigMaps are owned by the driver pod.",
    ),
    "sparkUIOptions": _unsupported(*_UI),
    "driverIngressOptions": _unsupported(*_UI),
}


def _pull_secrets(a: _Assessment, path: str, value: Any) -> None:
    if not _text_list(value):
        return a.invalid(path, "a list of Secret names")
    driver = a.pods["driver"]
    for name in value:
        driver.append("imagePullSecrets", {"name": name}, None)
    a.assign(
        path,
        [(("spark_conf", "spark.kubernetes.container.image.pullSecrets"), ",".join(value))],
        extra=[driver.target("imagePullSecrets")],
    )


# SparkPodSpec fields shared by the driver and executor.


def _pod_spec(a: _Assessment, prefix: str, context: _PodContext) -> None:
    fields = {**_POD_FIELDS, **(_DRIVER_FIELDS if context.role == "driver" else _EXECUTOR_FIELDS)}
    for key, value in context.source.items():
        path = f"{prefix}.{key}"
        if value is None:
            a.record(path, IGNORED, note="Null; treated as unset.")
        elif key in fields:
            fields[key](a, path, value, context)
        else:
            a.unknown(path)


def _pod_cores(a, path, value, context):
    if not _count(value, 1):
        return a.invalid(path, "a whole number of cores")
    a.assign(path, [((f"{context.role}_resources", "cpu"), value)])


def _pod_core_quantity(conf):
    def handler(a, path, value, context):
        quantity = _cores(value)
        if quantity is None:
            return a.invalid(path, "a CPU quantity such as 500m or 2")
        if context.role == "executor":
            return a.assign(path, [(("spark_conf", conf.format(role="executor")), _conf_string(value))])
        note = "The driver CPU request and limit both equal driver_resources.cpu, in whole cores."
        if "cores" in context.source or conf.endswith("limit.cores"):
            return a.record(path, APPROXIMATED, "spec.driver_resources.cpu", note)
        a.assign(
            path, [(("driver_resources", "cpu"), max(1, math.ceil(quantity)))], status=APPROXIMATED, note=note
        )

    return handler


def _pod_memory(field):
    def handler(a, path, value, context):
        _resource(a, path, (f"{context.role}_resources", field), value)

    return handler


def _pod_image(a, path, value, context):
    if not _text(value):
        return a.invalid(path, "a string")
    if context.role == "driver":
        return a.assign(path, [(("image",), value)])
    driver = _pod_field(context.application, "driver", "image") or context.application.get("image")
    if value == driver:
        return a.record(path, MAPPED, "spec.image", "Same as the driver image.")
    a.assign(path, [(("spark_conf", _EXECUTOR_IMAGE), value)])


def _mount(a, path, context, volume, mount, *, status=MAPPED, note=None):
    pod = context.pod
    conflict = pod.add_volume(volume) or pod.append("volumeMounts", mount, "mountPath", container=True)
    a.template(path, conflict, pod.target("volumeMounts", container=True), status=status, note=note)
    return conflict is None


def _pod_config_maps(a, path, value, context):
    if not isinstance(value, list):
        return a.invalid(path, "a list")
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(item, dict) or set(item) != {"name", "path"} or not _text_list(list(item.values())):
            a.invalid(item_path, "an object with name and path")
            continue
        volume_name = f"{item['name']}-vol"
        volume = {"name": volume_name, "configMap": {"name": item["name"]}}
        _mount(a, item_path, context, volume, {"name": volume_name, "mountPath": item["path"]})


def _pod_secrets(a, path, value, context):
    if not isinstance(value, list):
        return a.invalid(path, "a list")
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        kind = item.get("secretType", "Generic") if isinstance(item, dict) else None
        if (
            not isinstance(item, dict)
            or set(item) - {"name", "path", "secretType"}
            or not _text(item.get("name"))
            or not _text(item.get("path"))
            or kind not in {"Generic", "GCPServiceAccount", "HadoopDelegationToken"}
        ):
            a.invalid(item_path, "an object with name, path and an optional supported secretType")
            continue
        status, note = MAPPED, None
        if kind != "Generic":
            status = APPROXIMATED
            note = (
                "The Secret is mounted, but the environment variable the operator derives from "
                f"secretType {kind} is not set; add it to the pod template environment."
            )
        if context.role == "executor":
            conf = f"spark.kubernetes.executor.secrets.{item['name']}"
            a.assign(item_path, [(("spark_conf", conf), item["path"])], status=status, note=note)
            continue
        volume_name = f"{item['name']}-vol"
        volume = {"name": volume_name, "secret": {"secretName": item["name"]}}
        mount = {"name": volume_name, "mountPath": item["path"]}
        _mount(a, item_path, context, volume, mount, status=status, note=note)


def _secret_ref(reference: Any) -> dict | None:
    if isinstance(reference, dict) and set(reference) == {"secretKeyRef"}:
        ref = reference["secretKeyRef"]
        if isinstance(ref, dict) and set(ref) == {"name", "key"} and _text_list(list(ref.values())):
            return ref
    return None


def _env_entry(a, path, context, name, entry):
    """Add one environment variable: executor literals and Secret keys use Spark properties."""
    if context.role == "executor":
        if "valueFrom" not in entry:
            return a.assign(path, [(("spark_conf", f"spark.executorEnv.{name}"), entry.get("value", ""))])
        ref = _secret_ref(entry["valueFrom"])
        if ref is not None:
            conf = f"spark.kubernetes.executor.secretKeyRef.{name}"
            return a.assign(path, [(("spark_conf", conf), f"{ref['name']}:{ref['key']}")])
    conflict = context.pod.append("env", entry, "name", container=True)
    a.template(path, conflict, f"{context.pod.target('env', container=True)}[{name}]")


def _pod_env(a, path, value, context):
    if not isinstance(value, list):
        return a.invalid(path, "a list")
    for index, entry in enumerate(value):
        item_path = f"{path}[{index}]"
        if (
            not isinstance(entry, dict)
            or not _text(entry.get("name"))
            or set(entry) - {"name", "value", "valueFrom"}
            or {"value", "valueFrom"} <= set(entry)
            or "value" in entry
            and not isinstance(entry["value"], str)
        ):
            a.invalid(item_path, "an environment variable with a name and a string value or valueFrom")
        elif _sensitive_key(entry["name"]) and entry.get("value") or _value_secret(entry):
            a.secret(item_path)
        else:
            _env_entry(a, item_path, context, entry["name"], entry)


def _pod_env_vars(a, path, value, context):
    if not _string_map(value):
        return a.invalid(path, "an object with string values")
    for name, item in value.items():
        item_path = f"{path}[{name}]"
        if _sensitive_key(name) and item or _value_secret(item):
            a.secret(item_path)
        else:
            _env_entry(a, item_path, context, name, {"name": name, "value": item})


def _pod_env_secret_refs(a, path, value, context):
    if not isinstance(value, dict):
        return a.invalid(path, "an object")
    for name, item in value.items():
        item_path = f"{path}[{name}]"
        ref = _secret_ref({"secretKeyRef": item})
        if ref is None:
            a.invalid(item_path, "an object with name and key")
        else:
            _env_entry(a, item_path, context, name, {"name": name, "valueFrom": {"secretKeyRef": ref}})


def _pod_env_from(a, path, value, context):
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return a.invalid(path, "a list of objects")
    for index, item in enumerate(value):
        context.pod.append("envFrom", item, None, container=True)
        a.record(f"{path}[{index}]", MAPPED, context.pod.target("envFrom", container=True))


def _pod_map(field):
    def handler(a, path, value, context):
        if not _string_map(value):
            return a.invalid(path, "an object with string values")
        for key, item in value.items():
            a.assign(f"{path}[{key}]", [((f"{context.role}_{field}", key), item)])

    return handler


def _pod_volume_mounts(a, path, value, context):
    if not isinstance(value, list):
        return a.invalid(path, "a list")
    for index, mount in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(mount, dict) or not _text(mount.get("name")) or not _text(mount.get("mountPath")):
            a.invalid(item_path, "a volume mount with name and mountPath")
            continue
        volume = context.volumes.get(mount["name"])
        if volume is None:
            a.unsupported(
                item_path,
                "References a volume that spec.volumes does not define.",
                "Define the volume in the source manifest.",
            )
        elif _value_secret(volume) or _value_secret(mount):
            a.rejected_volumes.add(mount["name"])
            a.secret(item_path)
        elif _mount(a, item_path, context, volume, mount):
            a.volume_users.setdefault(mount["name"], set()).add(context.role)


_TYPE_NAMES = {dict: "an object", list: "a list", str: "a string", bool: "a boolean", int: "an integer"}


def _pod_template_field(field, expected, *, container=False):
    """Copy a Pod field the driver template merge preserves and Spark keeps in executor templates."""

    def handler(a, path, value, context):
        if not isinstance(value, expected) or isinstance(value, bool) and expected is not bool:
            return a.invalid(path, _TYPE_NAMES[expected])
        if _value_secret(value):
            return a.secret(path)
        context.pod.set(field, value, container=container)
        a.record(path, MAPPED, context.pod.target(field, container=container))

    return handler


def _pod_containers(sidecars: bool):
    def handler(a, path, value, context):
        if not isinstance(value, list):
            return a.invalid(path, "a list")
        for index, container in enumerate(value):
            item_path = f"{path}[{index}]"
            if not isinstance(container, dict) or not _text(container.get("name")):
                a.invalid(item_path, "a container with a name")
            elif _container_secret(container):
                a.secret(item_path)
            elif not sidecars:
                conflict = context.pod.append("initContainers", container, "name")
                a.template(item_path, conflict, context.pod.target(f"initContainers[{container['name']}]"))
            elif context.role == "driver":
                a.template(
                    item_path,
                    context.pod.add_sidecar(container),
                    context.pod.target(f"containers[{container['name']}]"),
                    status=APPROXIMATED,
                    note="A sidecar that keeps running keeps the driver pod, and waiting, running; prefer "
                    "a native sidecar (an init container with restartPolicy: Always).",
                )
            else:
                a.template(
                    item_path,
                    context.pod.add_sidecar(container),
                    context.pod.target(f"containers[{container['name']}]"),
                    note="The executor container stays first, which Spark uses as the executor container.",
                )

    return handler


def _pod_service_account(a, path, value, context):
    if not _text(value):
        return a.invalid(path, "a string")
    if context.role == "driver":
        return a.assign(path, [(("service_account",), value)])
    conf = "spark.kubernetes.authenticate.executor.serviceAccountName"
    a.assign(path, [(("spark_conf", conf), value)])


def _pod_java_options(a, path, value, context):
    if not _text(value):
        return a.invalid(path, "a string")
    if _value_secret(value):
        return a.secret(path)
    a.assign(path, [(("spark_conf", f"spark.{context.role}.extraJavaOptions"), value)])


def _executor_instances(a, path, value, context):
    if not _count(value):
        return a.invalid(path, "a whole number of executors")
    a.allocate("instances", value, path)


def _executor_delete(a, path, value, context):
    if type(value) is not bool:
        return a.invalid(path, "a boolean")
    conf = "spark.kubernetes.executor.deleteOnTermination"
    a.assign(path, [(("spark_conf", conf), _conf_string(value))])


def _pod_unsupported(note, alternative):
    def handler(a, path, value, context):
        a.unsupported(path, note, alternative)

    return handler


_POD_FIELDS = {
    "cores": _pod_cores,
    "coreRequest": _pod_core_quantity("spark.kubernetes.{role}.request.cores"),
    "coreLimit": _pod_core_quantity("spark.kubernetes.{role}.limit.cores"),
    "memory": _pod_memory("memory"),
    "memoryOverhead": _pod_memory("memory_overhead"),
    "gpu": _pod_unsupported(
        "GPU limits are not translated automatically.",
        "Add the resource limit to the container in driver_pod_template and executor_pod_template, then "
        "confirm that the running executor pods carry it.",
    ),
    "image": _pod_image,
    "configMaps": _pod_config_maps,
    "secrets": _pod_secrets,
    "env": _pod_env,
    "envVars": _pod_env_vars,
    "envSecretKeyRefs": _pod_env_secret_refs,
    "envFrom": _pod_env_from,
    "labels": _pod_map("labels"),
    "annotations": _pod_map("annotations"),
    "nodeSelector": _pod_map("node_selector"),
    "volumeMounts": _pod_volume_mounts,
    "affinity": _pod_template_field("affinity", dict),
    "tolerations": _pod_template_field("tolerations", list),
    "podSecurityContext": _pod_template_field("securityContext", dict),
    "securityContext": _pod_template_field("securityContext", dict, container=True),
    "schedulerName": _pod_template_field("schedulerName", str),
    "priorityClassName": _pod_template_field("priorityClassName", str),
    "dnsConfig": _pod_template_field("dnsConfig", dict),
    "hostAliases": _pod_template_field("hostAliases", list),
    "hostNetwork": _pod_unsupported(
        "Host networking changes driver and executor addresses and is not exercised with directly created "
        "drivers.",
        "Set hostNetwork in the pod templates only after verifying driver-executor connectivity.",
    ),
    "shareProcessNamespace": _pod_template_field("shareProcessNamespace", bool),
    "terminationGracePeriodSeconds": _pod_template_field("terminationGracePeriodSeconds", int),
    "lifecycle": _pod_template_field("lifecycle", dict, container=True),
    "ports": _pod_template_field("ports", list, container=True),
    "sidecars": _pod_containers(sidecars=True),
    "initContainers": _pod_containers(sidecars=False),
    "serviceAccount": _pod_service_account,
    "javaOptions": _pod_java_options,
    "template": _pod_unsupported(
        "Operator pod templates are not converted automatically.",
        "Move its fields into driver_pod_template or executor_pod_template and validate the result.",
    ),
}
_DRIVER_FIELDS = {
    "kubernetesMaster": _pod_unsupported(
        "The driver always uses the in-cluster Kubernetes API address.",
        "Submit the application to the cluster that should run its executors.",
    ),
    "serviceAnnotations": _pod_unsupported(
        "The client creates the driver Service without custom metadata.",
        "Add Service metadata with an admission policy if required.",
    ),
    "serviceLabels": _pod_unsupported(
        "The client creates the driver Service without custom metadata.",
        "Add Service metadata with an admission policy if required.",
    ),
    "podName": _pod_unsupported(
        "Driver pod names derive from the generated application ID.",
        "Find the driver from the submission result or a run receipt (--receipt-file).",
    ),
}
_EXECUTOR_FIELDS = {
    "instances": _executor_instances,
    "deleteOnTermination": _executor_delete,
}


# Documents.


def _metadata(a: _Assessment, metadata: dict, job_name: str) -> None:
    server_fields = {
        "uid",
        "resourceVersion",
        "generation",
        "creationTimestamp",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
        "managedFields",
        "selfLink",
    }
    for key, value in metadata.items():
        path = f"metadata.{key}"
        if key == "name":
            note = (
                "Application IDs add a generated timestamp suffix, so driver pods are not named "
                f"{value}-driver."
            )
            if job_name != value:
                note += f" Renamed to {job_name} to form a valid SparkJob name."
            a.record(path, APPROXIMATED, "metadata.name", note)
        elif key == "namespace":
            if _text(value):
                a.assign(path, [(("namespace",), value)])
            else:
                a.invalid(path, "a string")
        elif key == "labels":
            if not _string_map(value):
                a.invalid(path, "an object with string values")
                continue
            for label, item in value.items():
                item_path = f"{path}[{label}]"
                prefix, slash, _ = label.partition("/")
                if slash and prefix == "kueue.x-k8s.io":
                    a.unsupported(
                        item_path,
                        "The operator does not copy Kueue labels to pods, and no custom resource is created "
                        "for Kueue to admit.",
                        "See the batch scheduler recipe before adding Kueue labels to the pod templates.",
                    )
                    continue
                applied = a.assign_default(
                    item_path, [((f"{role}_labels", label), item) for role in ("driver", "executor")]
                )
                targets = ", ".join(_render(target) for target in applied)
                if targets:
                    a.record(item_path, MAPPED, targets, "Copied to the driver and executor pods.")
                else:
                    a.record(item_path, IGNORED, note="Both pods set this label themselves.")
        elif key == "annotations":
            if not _string_map(value):
                a.invalid(path, "an object with string values")
                continue
            for annotation in value:
                item_path = f"{path}[{annotation}]"
                if annotation == "kubectl.kubernetes.io/last-applied-configuration":
                    a.record(item_path, IGNORED, note="kubectl bookkeeping; its content is not read.")
                else:
                    a.unsupported(
                        item_path,
                        "Annotations describe the SparkApplication object; no custom resource is created.",
                        "Use driver_annotations or executor_annotations for pod annotations.",
                    )
        elif key in server_fields:
            a.record(path, IGNORED, note="Written by the API server.")
        elif key in {"generateName", "ownerReferences", "finalizers"}:
            a.unsupported(
                path,
                "Object lifecycle metadata has no target: no custom resource is created.",
                "Manage application lifecycle from the caller that submits the job.",
            )
        else:
            a.unknown(path)


def _scheduled(a: _Assessment, spec: dict) -> None:
    alternatives = {
        "schedule": _SCHEDULE,
        "timeZone": (_SCHEDULE[0], "Use CronJob spec.timeZone or the DAG's timezone."),
        "suspend": (_SCHEDULE[0], "Use CronJob spec.suspend or pause the DAG."),
        "concurrencyPolicy": (
            _SCHEDULE[0],
            "Use CronJob concurrencyPolicy; it only limits Spark runs while the submitting pod waits for "
            "them (app_waiter wait or log).",
        ),
        "successfulRunHistoryLimit": (
            "No controller retains or deletes finished runs.",
            "Use CronJob successfulJobsHistoryLimit for submitter pods and delete finished drivers with "
            "`spark-on-k8s app delete`.",
        ),
        "failedRunHistoryLimit": (
            "No controller retains or deletes finished runs.",
            "Use CronJob failedJobsHistoryLimit for submitter pods and delete finished drivers with "
            "`spark-on-k8s app delete`.",
        ),
    }
    for key, value in spec.items():
        path = f"spec.{key}"
        if key == "template":
            continue
        if value is None:
            a.record(path, IGNORED, note="Null; treated as unset.")
        elif key in alternatives:
            a.unsupported(path, *alternatives[key])
        else:
            a.unknown(path)


def _draft_spec(a: _Assessment) -> dict[str, Any]:
    spec: dict[str, Any] = {}
    for target, (value, _) in a.values.items():
        head, *rest = target
        if rest:
            spec.setdefault(head, {})[rest[0]] = copy.deepcopy(value)
        else:
            spec[head] = copy.deepcopy(value)
    for role, pod in a.pods.items():
        document = pod.document()
        if document is not None:
            spec[f"{role}_pod_template"] = document
    return {key: spec[key] for key in sorted(spec, key=lambda key: _DRAFT_ORDER.index(key))}


_UNACCOUNTED = "The assessor did not account for this field, so it was not translated."


def _leaves(value: Any, path: tuple[str, ...] = ()):
    if isinstance(value, dict) and value:
        for key, item in value.items():
            yield from _leaves(item, (*path, str(key)))
    elif isinstance(value, list) and value:
        for index, item in enumerate(value):
            yield from _leaves(item, (*path, str(index)))
    else:
        yield path, value


def _segments(path: str) -> tuple[str, ...]:
    return tuple(field or key for field, key in re.findall(r"([^.\[\]]+)|\[([^\]]*)\]", path))


def _account_for_every_leaf(a: _Assessment, document: dict) -> None:
    """Give empty objects, and anything a field handler missed, an explicit status."""
    covered = {_segments(finding.path) for finding in a.findings}
    for leaf, value in _leaves(document):
        if any(leaf[:length] in covered for length in range(1, len(leaf) + 1)):
            continue
        path = "".join(
            part if not index else f".{part}" if re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", part) else f"[{part}]"
            for index, part in enumerate(leaf)
        )
        if value in ({}, []):
            a.record(path, IGNORED, note="Empty; nothing to translate.")
        else:
            a.unsupported(path, _UNACCOUNTED, _UNKNOWN[1])
        covered.add(leaf)


def _notes(a: _Assessment, kind: str) -> list[str]:
    notes = [
        "Lifecycle ownership changes: spark-on-k8s creates the driver pod and returns. No controller "
        "retries, reschedules or deletes the application; the caller observes it (app_waiter, `app wait` "
        "or a run receipt) and owns retries and cleanup."
    ]
    if kind == "ScheduledSparkApplication":
        notes.append("The draft covers spec.template only; schedule it with a CronJob or an Airflow DAG.")
    if ("namespace",) not in a.values:
        notes.append(
            "The source has no namespace: job files submit to namespace default, while kubectl would have "
            "used the current context's namespace. Set spec.namespace."
        )
    if ("service_account",) not in a.values:
        notes.append(
            "The source sets no driver service account: the draft uses the SparkJob default spark. "
            "Set spec.service_account to the account the operator used."
        )
    for role in ("driver", "executor"):
        if (f"{role}_resources", "memory_overhead") not in a.values:
            notes.append(
                f"{role.capitalize()} memory overhead is unset: SparkJob uses 512 MiB, while Spark derives a "
                "default from a memory overhead factor. Set memory_overhead to keep pod memory unchanged."
            )
    return notes


def assess_document(document: dict, *, position: int = 1) -> dict[str, Any]:
    """Assess one SparkApplication or ScheduledSparkApplication mapping without contacting a cluster."""
    kind = document.get("kind")
    metadata = document.get("metadata")
    if not isinstance(metadata, dict) or not _text(metadata.get("name")):
        raise ManifestError(f"Document {position}: metadata.name is required.")
    spec = document.get("spec")
    if not isinstance(spec, dict):
        raise ManifestError(f"Document {position}: spec must be an object.")
    application = spec.get("template") if kind == "ScheduledSparkApplication" else spec
    if not isinstance(application, dict):
        raise ManifestError(f"Document {position}: spec.template must be an object.")
    a = _Assessment()
    job_name = _job_name(metadata["name"])
    prefix = "spec.template" if kind == "ScheduledSparkApplication" else "spec"
    if kind == "ScheduledSparkApplication":
        _scheduled(a, spec)
    _application(a, application, prefix)
    _metadata(a, metadata, job_name)
    for key in document:
        if key in {"apiVersion", "kind"}:
            a.record(key, MAPPED, key, f"The draft is a {API_VERSION} SparkJob document.")
        elif key == "status":
            a.record(key, IGNORED, note="Observed state written by the operator.")
        elif key not in {"metadata", "spec"}:
            a.unknown(key)
    _account_for_every_leaf(a, document)
    draft = {"apiVersion": API_VERSION, "kind": "SparkJob", "metadata": {"name": job_name}}
    draft["spec"] = _draft_spec(a)
    draft_error = None
    try:
        JobSpec.from_mapping(copy.deepcopy(draft)).plan()
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        draft_error = str(error) or type(error).__name__
    findings = sorted(a.findings, key=lambda finding: finding.path)
    counts = {status: sum(finding.status == status for finding in findings) for status in STATUSES}
    namespace = metadata.get("namespace")
    return {
        "document": position,
        "kind": kind,
        "name": metadata["name"],
        "namespace": namespace if isinstance(namespace, str) else None,
        "migration_ready": draft_error is None and counts[UNSUPPORTED] == 0,
        "counts": counts,
        "findings": [finding.to_dict() for finding in findings],
        "notes": _notes(a, kind),
        "draft": draft if draft_error is None else None,
        "draft_error": draft_error,
        "draft_file": None,
    }


def load_manifests(content: bytes | str) -> list[Any]:
    """Parse a YAML stream of at most 1 MiB, rejecting aliases and duplicate keys."""
    if len(content) > MAX_JOB_BYTES:
        raise ManifestError("Manifests must be no larger than 1 MiB.")
    try:
        return list(yaml.load_all(content, Loader=_ManifestLoader))
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark is not None else ""
        # Do not echo YAML snippets: the input can contain credentials.
        raise ManifestError(f"Invalid YAML{where}.") from error


def assess_manifests(content: bytes | str) -> dict[str, Any]:
    """Assess every Spark Operator application in a YAML stream; other documents are skipped."""
    applications, skipped = [], []
    for position, document in enumerate(load_manifests(content), start=1):
        if document is None:
            continue
        if not isinstance(document, dict):
            raise ManifestError(f"Document {position} is not a mapping.")
        api_version, kind = document.get("apiVersion"), document.get("kind")
        if api_version == OPERATOR_API_VERSION and kind in OPERATOR_KINDS:
            applications.append(assess_document(document, position=position))
        else:
            skipped.append(
                {
                    "document": position,
                    "api_version": api_version if isinstance(api_version, str) else None,
                    "kind": kind if isinstance(kind, str) else None,
                    "reason": f"Not an {OPERATOR_API_VERSION} SparkApplication or ScheduledSparkApplication.",
                }
            )
    if not applications:
        raise ManifestError(
            f"No {OPERATOR_API_VERSION} SparkApplication or ScheduledSparkApplication document was found."
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "operation": "assess_operator",
        "error": None,
        "migration_ready": all(item["migration_ready"] for item in applications),
        "applications": applications,
        "skipped": skipped,
    }


def assess_file(path: str | Path) -> dict[str, Any]:
    """Read and assess a manifest file. Nothing is submitted and no cluster is contacted."""
    with Path(path).expanduser().open("rb") as stream:
        return assess_manifests(stream.read(MAX_JOB_BYTES + 1))


def draft_yaml(application: dict[str, Any], source: str) -> str:
    """Render a validated draft with a header that marks it as a migration draft."""
    location = f"{application['namespace']}/" if application["namespace"] else ""
    header = (
        f"# DRAFT converted by `spark-on-k8s job assess-operator` from {application['kind']}\n"
        f"# {location}{application['name']} in {source}.\n"
        "# It contains only the settings reported as mapped or approximated. Review every\n"
        "# approximated and unsupported finding, then validate before use:\n"
        "#   spark-on-k8s job validate <this file>\n"
    )
    return header + yaml.safe_dump(application["draft"], sort_keys=False)
