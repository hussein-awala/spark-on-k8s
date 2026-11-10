from __future__ import annotations

import copy
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

if TYPE_CHECKING:
    from spark_on_k8s.planning import ApplicationPlan

API_VERSION = "spark-on-k8s/v1"
MAX_JOB_BYTES = 1024 * 1024
_TEXT_FIELDS = {
    "image",
    "app_path",
    "namespace",
    "service_account",
    "class_name",
    "driver_container_name",
    "executor_pod_template_path",
}
_MAP_FIELDS = {
    "spark_conf",
    "driver_node_selector",
    "executor_node_selector",
    "driver_labels",
    "executor_labels",
    "driver_annotations",
    "executor_annotations",
}
_LIST_FIELDS = {"packages", "app_arguments", "driver_env_vars_from_secrets"}
_RESOURCE_FIELDS = {"driver_resources", "executor_resources"}
_FIELDS = (
    _TEXT_FIELDS
    | _MAP_FIELDS
    | _LIST_FIELDS
    | _RESOURCE_FIELDS
    | {
        "executor_instances",
        "image_pull_policy",
        "ui_reverse_proxy",
        "startup_timeout",
        "app_waiter",
        "executor_pod_template",
        "driver_pod_template",
    }
)
_DNS_NAME = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")


class JobValidationError(ValueError):
    """A job definition has an invalid structure or unsupported setting."""


def validation_error(error: Exception) -> dict[str, str]:
    """Classify an offline validation failure for the versioned validate result."""
    # An unreadable job or template file is a read failure, not an invalid definition.
    return {"code": "read_error" if isinstance(error, OSError) else "invalid_job", "message": str(error)}


class _UniqueLoader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise JobValidationError("YAML aliases are not supported; use profiles for overrides.")
        return super().compose_node(parent, index)


def _mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str) or key in result:
            raise JobValidationError("Job keys must be unique strings; YAML merge keys are not supported.")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _object(value, allowed: set[str] | None, path: str) -> dict:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise JobValidationError(f"{path} must be an object with string keys.")
    if allowed is not None and set(value) - allowed:
        raise JobValidationError(f"Unsupported field in {path}: {', '.join(sorted(set(value) - allowed))}.")
    return value


def _text(value, path):
    if not isinstance(value, str) or not value.strip():
        raise JobValidationError(f"{path} must be a non-empty string.")


def _integer(value, path, minimum=0):
    if type(value) is not int or value < minimum:
        raise JobValidationError(f"{path} must be an integer >= {minimum}.")


def _merge(base: dict, overlay: dict) -> dict:
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        result[key] = (
            _merge(result[key], value)
            if isinstance(result.get(key), dict) and isinstance(value, dict)
            else copy.deepcopy(value)
        )
    return result


def _validate_spec(spec: dict):
    _object(spec, _FIELDS, "spec")
    for name in {"image", "app_path"}:
        if name not in spec:
            raise JobValidationError(f"spec.{name} is required.")
    for name in _TEXT_FIELDS & spec.keys():
        _text(spec[name], f"spec.{name}")
    for name in {"namespace", "service_account"} & spec.keys():
        if len(spec[name]) > 63 or not _DNS_NAME.fullmatch(spec[name]):
            raise JobValidationError(f"spec.{name} must be a Kubernetes DNS label of at most 63 characters.")
    for name in _MAP_FIELDS & spec.keys():
        values = _object(spec[name], None, f"spec.{name}")
        for key, value in values.items():
            if name == "spark_conf":
                if (
                    not isinstance(value, (str, bool, int, float))
                    or isinstance(value, float)
                    and not math.isfinite(value)
                ):
                    raise JobValidationError("Spark configuration values must be finite scalars.")
            elif not isinstance(value, str):
                raise JobValidationError(f"spec.{name} values must be strings.")
            _text(key, f"spec.{name} key")
    for name in _LIST_FIELDS & spec.keys():
        if not isinstance(spec[name], list) or not all(isinstance(value, str) for value in spec[name]):
            raise JobValidationError(f"spec.{name} must be a list of strings.")
    for name in _RESOURCE_FIELDS & spec.keys():
        for field, value in _object(spec[name], {"cpu", "memory", "memory_overhead"}, f"spec.{name}").items():
            _integer(value, f"spec.{name}.{field}", 0 if field == "memory_overhead" else 1)
    if "executor_instances" in spec:
        values = _object(
            spec["executor_instances"], set(ExecutorInstances.__dataclass_fields__), "spec.executor_instances"
        )
        for field in {"min", "max", "initial"} & values.keys():
            _integer(values[field], f"spec.executor_instances.{field}")
        lower, upper = values.get("min", 0), values.get("max")
        if upper is not None and upper < lower:
            raise JobValidationError("Executor maximum must be >= minimum.")
        if "initial" in values and (
            values["initial"] < lower or upper is not None and values["initial"] > upper
        ):
            raise JobValidationError("Initial executors must be within the allocation bounds.")
        ratio = values.get("executor_allocation_ratio", 1.0)
        if type(ratio) not in {int, float} or not 0 < ratio <= 1:
            raise JobValidationError("Executor allocation ratio must be greater than 0 and at most 1.")
        for field in {"scheduler_backlog_timeout", "sustained_scheduler_backlog_timeout"} & values.keys():
            _text(values[field], f"spec.executor_instances.{field}")
    for name, choices in {
        "image_pull_policy": {"Always", "Never", "IfNotPresent"},
        "app_waiter": {"no_wait", "wait", "log"},
    }.items():
        if name in spec and (not isinstance(spec[name], str) or spec[name] not in choices):
            raise JobValidationError(f"spec.{name} must be one of {', '.join(sorted(choices))}.")
    if "ui_reverse_proxy" in spec and type(spec["ui_reverse_proxy"]) is not bool:
        raise JobValidationError("spec.ui_reverse_proxy must be a boolean.")
    if "startup_timeout" in spec:
        _integer(spec["startup_timeout"], "spec.startup_timeout")
    for field in ("driver_pod_template", "executor_pod_template"):
        if field in spec and not isinstance(spec[field], (str, dict)):
            raise JobValidationError(f"spec.{field} must be YAML text or a Pod object.")
    if "executor_pod_template" in spec and "executor_pod_template_path" in spec:
        raise JobValidationError("Choose executor_pod_template or executor_pod_template_path, not both.")


@dataclass(frozen=True)
class JobSpec:
    """Validated effective job settings, independent of Airflow and the HTTP API."""

    name: str
    spec: dict[str, Any]
    kubernetes: dict[str, Any]
    profile: str | None = None

    @classmethod
    def from_mapping(cls, document: dict, *, profile: str | None = None) -> JobSpec:
        _object(document, {"apiVersion", "kind", "metadata", "spec", "kubernetes", "profiles"}, "job")
        if document.get("apiVersion") != API_VERSION or document.get("kind") != "SparkJob":
            raise JobValidationError(f"Expected apiVersion: {API_VERSION} and kind: SparkJob.")
        metadata = _object(document.get("metadata", {}), {"name"}, "metadata")
        name = metadata.get("name")
        _text(name, "metadata.name")
        if len(name) > 63 or not _DNS_NAME.fullmatch(name) or not name[0].isalpha():
            raise JobValidationError(
                "metadata.name must start with a letter and be a DNS label of at most 63 characters."
            )
        profiles = _object(document.get("profiles", {}), None, "profiles")
        for overlay in profiles.values():
            _object(overlay, {"spec", "kubernetes"}, "profile")
        resolved = copy.deepcopy(document)
        if profile is not None:
            if profile not in profiles:
                raise JobValidationError(f"Unknown profile: {profile}.")
            resolved = _merge(resolved, profiles[profile])
        spec = _object(resolved.get("spec", {}), _FIELDS, "spec")
        _validate_spec(spec)
        connection = _object(
            resolved.get("kubernetes", {}), {"context", "config_file", "in_cluster"}, "kubernetes"
        )
        for field in {"context", "config_file"} & connection.keys():
            _text(connection[field], f"kubernetes.{field}")
        if "in_cluster" in connection and type(connection["in_cluster"]) is not bool:
            raise JobValidationError("kubernetes.in_cluster must be a boolean.")
        if connection.get("in_cluster") and ("context" in connection or "config_file" in connection):
            raise JobValidationError(
                "In-cluster authentication cannot be combined with a kubeconfig/context."
            )
        return cls(name, copy.deepcopy(spec), copy.deepcopy(connection), profile)

    @classmethod
    def from_file(cls, path: str | Path, *, profile: str | None = None) -> JobSpec:
        path, document = cls._read_file(path)
        result = cls.from_mapping(document, profile=profile)
        for field in ("driver_pod_template", "executor_pod_template"):
            value = result.spec.get(field)
            if isinstance(value, str) and "\n" not in value and value.endswith((".yaml", ".yml")):
                template_path = Path(value).expanduser()
                if not template_path.is_absolute():
                    template_path = path.parent / template_path
                result.spec[field] = template_path.read_text(encoding="utf-8")
        if "config_file" in result.kubernetes:
            config_path = Path(result.kubernetes["config_file"]).expanduser()
            if not config_path.is_absolute():
                config_path = path.parent / config_path
            result.kubernetes["config_file"] = str(config_path)
        return result

    @staticmethod
    def _read_file(path):
        path = Path(path).expanduser().resolve()
        with path.open("rb") as stream:
            content = stream.read(MAX_JOB_BYTES + 1)
        if len(content) > MAX_JOB_BYTES:
            raise JobValidationError("Job files must be no larger than 1 MiB.")
        try:
            document = yaml.load(content, Loader=_UniqueLoader)
        except yaml.YAMLError as error:
            raise JobValidationError("Invalid job YAML.") from error
        return path, document

    @classmethod
    def validate_profiles(cls, path: str | Path) -> list[dict[str, Any]]:
        """Validate the base and every named profile offline, reporting every failure."""
        _, document = cls._read_file(path)
        _object(document, None, "job")
        profiles = _object(document.get("profiles", {}), None, "profiles")
        results = []
        for profile in [None, *sorted(profiles)]:
            try:
                cls.from_file(path, profile=profile).plan()
            except (ValueError, TypeError, OSError) as error:
                results.append({"profile": profile, "valid": False, "error": validation_error(error)})
            else:
                results.append({"profile": profile, "valid": True, "error": None})
        return results

    def to_submit_kwargs(self) -> dict[str, Any]:
        _validate_spec(self.spec)
        # Job files are portable: ambient CLI defaults must not change their meaning.
        options = {
            "namespace": "default",
            "service_account": "spark",
            "spark_conf": {},
            "class_name": None,
            "packages": [],
            "app_arguments": [],
            "app_waiter": "no_wait",
            "image_pull_policy": "IfNotPresent",
            "ui_reverse_proxy": False,
            "driver_resources": {},
            "executor_resources": {},
            "executor_instances": {"initial": 2},
            "executor_pod_template": "",
            "executor_pod_template_path": "",
            "secret_values": {},
            "driver_env_vars_from_secrets": [],
            "startup_timeout": 0,
            "driver_node_selector": {},
            "executor_node_selector": {},
            "driver_labels": {},
            "executor_labels": {},
            "driver_annotations": {},
            "executor_annotations": {},
            **copy.deepcopy(self.spec),
        }
        options["app_name"] = self.name
        for field in _RESOURCE_FIELDS & options.keys():
            options[field] = PodResources(**options[field])
        if "executor_instances" in options:
            instances = options["executor_instances"]
            instances.setdefault(
                "initial", instances.get("min", 0) if {"min", "max"} & instances.keys() else 2
            )
            options["executor_instances"] = ExecutorInstances(**instances)
        if isinstance(options.get("executor_pod_template"), dict):
            options["executor_pod_template"] = yaml.safe_dump(options["executor_pod_template"])
        return options

    def to_operator_kwargs(self) -> dict[str, Any]:
        """Use the same settings in Airflow; choose Kubernetes credentials through an Airflow connection."""
        if self.kubernetes:
            raise JobValidationError(
                "Use kubernetes_conn_id for Airflow credentials "
                "and a job profile without kubernetes settings."
            )
        return self.to_submit_kwargs()

    def client(self) -> SparkOnK8S:
        return SparkOnK8S(
            k8s_client_manager=KubernetesClientManager(
                **{
                    "context": None,
                    "config_file": None,
                    "in_cluster": False,
                    "client_configuration": None,
                    **self.kubernetes,
                }
            )
        )

    def plan(self) -> ApplicationPlan:
        return self.client().plan_app(**self.to_submit_kwargs())

    def submit(self) -> str:
        return self.client().submit_app(**self.to_submit_kwargs())
