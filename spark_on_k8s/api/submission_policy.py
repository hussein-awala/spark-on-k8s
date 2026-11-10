from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any

from spark_on_k8s.utils.run import validate_max_runtime

# These settings tune execution, but cannot select code, credentials, volumes or resource limits.
SAFE_SPARK_CONF = {
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.broadcastTimeout",
    "spark.sql.session.timeZone",
}
ALLOWED_SPEC = {
    "image",
    "app_path",
    "namespace",
    "service_account",
    "class_name",
    "app_arguments",
    "spark_conf",
    "driver_resources",
    "executor_resources",
    "executor_instances",
    "image_pull_policy",
    # A driver deadline can only shorten a run, so callers may always request one.
    "max_runtime",
}
LIMIT_FIELDS = {"driver_cpu", "driver_memory_mb", "executor_cpu", "executor_memory_mb", "executors"}
# Optional so existing policies stay valid; when present, every remote job must set a lower deadline.
OPTIONAL_LIMIT_FIELDS = {"max_runtime"}
DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
MAX_DOCUMENT_BYTES = 1024 * 1024


class AdmissionError(ValueError):
    """A job is outside the administrator's remote submission policy."""


def _object(value, fields, description):
    if not isinstance(value, dict) or set(value) - fields:
        raise ValueError(f"Invalid {description}: expected an object with supported fields.")
    return value


def _strings(value, description):
    if not isinstance(value, list) or not value or not all(isinstance(v, str) and v for v in value):
        raise ValueError(f"{description} must be a nonempty list of strings.")
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON keys are not supported.")
        result[key] = value
    return result


def parse_json(data: bytes | str):
    return json.loads(data, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)


def _invalid_constant(value):
    raise ValueError("Non-finite JSON numbers are not supported.")


@dataclass(frozen=True)
class SubmissionPolicy:
    namespaces: dict[str, list[str]]
    applications: list[dict[str, Any]]
    limits: dict[str, int]
    spark_conf: dict[str, list[Any]]

    @classmethod
    def from_mapping(cls, document):
        doc = _object(
            document, {"schema_version", "namespaces", "applications", "limits", "spark_conf"}, "policy"
        )
        if type(doc.get("schema_version")) is not int or doc["schema_version"] != 1:
            raise ValueError("Submission policy schema_version must be 1.")
        namespaces = doc.get("namespaces")
        if not isinstance(namespaces, dict) or not namespaces:
            raise ValueError("Policy namespaces must map allowed namespaces to service-account lists.")
        for namespace, accounts in namespaces.items():
            if not isinstance(namespace, str) or not DNS_LABEL.fullmatch(namespace):
                raise ValueError("Policy namespace must be a Kubernetes DNS label.")
            if not all(DNS_LABEL.fullmatch(v) for v in _strings(accounts, "Service accounts")):
                raise ValueError("Policy service accounts must be Kubernetes DNS labels.")
        applications = doc.get("applications")
        if not isinstance(applications, list) or not applications:
            raise ValueError("Policy applications must be a nonempty list.")
        for app in applications:
            _object(app, {"image", "app_path", "class_name", "arguments"}, "approved application")
            image, path = app.get("image"), app.get("app_path")
            if not isinstance(image, str) or not image or any(c.isspace() for c in image):
                raise ValueError("Approved image must be an exact image reference.")
            if not isinstance(path, str) or not path.startswith("local:///"):
                raise ValueError("Approved app_path must be a local:/// path inside the image.")
            if ".." in PurePosixPath(path[8:]).parts or any(c in path for c in "\x00\r\n?#"):
                raise ValueError("Approved app_path must be a plain absolute local path.")
            if app.get("class_name") is not None and (
                not isinstance(app["class_name"], str) or not app["class_name"].strip()
            ):
                raise ValueError("Approved class_name must be a nonempty string or null.")
            arguments = app.get("arguments", [[]])
            if (
                not isinstance(arguments, list)
                or not arguments
                or any(
                    not isinstance(argv, list) or not all(isinstance(arg, str) for arg in argv)
                    for argv in arguments
                )
            ):
                raise ValueError("Approved arguments must be a nonempty list of exact argument lists.")
        limits = _object(doc.get("limits"), LIMIT_FIELDS | OPTIONAL_LIMIT_FIELDS, "resource limits")
        if not LIMIT_FIELDS <= set(limits) or any(type(v) is not int or v < 1 for v in limits.values()):
            raise ValueError("Every policy resource limit must be a positive integer.")
        validate_max_runtime(limits.get("max_runtime"), "Policy max_runtime limit")
        conf = _object(doc.get("spark_conf", {}), SAFE_SPARK_CONF, "Spark configuration allowlist")
        for values in conf.values():
            if (
                not isinstance(values, list)
                or not values
                or any(type(v) not in {str, int, bool} for v in values)
            ):
                raise ValueError("Allowed Spark configuration values must be explicit scalar lists.")
        return cls(namespaces, applications, limits, conf)

    def admit(self, document):
        # Import only when enabled. The shared JobSpec owns schema/type validation.
        from spark_on_k8s.jobs import JobSpec

        if not isinstance(document, dict) or set(document) - {"apiVersion", "kind", "metadata", "spec"}:
            raise AdmissionError("Remote jobs cannot select profiles or Kubernetes credentials.")
        spec = document.get("spec")
        if not isinstance(spec, dict) or set(spec) - ALLOWED_SPEC:
            raise AdmissionError("Remote jobs contain a setting that is not admitted by the submission API.")
        job = JobSpec.from_mapping(document)
        options = job.to_submit_kwargs()
        namespace, account = options["namespace"], options["service_account"]
        if account not in self.namespaces.get(namespace, []):
            raise AdmissionError("The namespace and service-account pair is not approved.")
        if not any(
            options["image"] == app["image"]
            and options["app_path"] == app["app_path"]
            and options["class_name"] == app.get("class_name")
            and options["app_arguments"] in app.get("arguments", [[]])
            for app in self.applications
        ):
            raise AdmissionError(
                "The image, application entrypoint, class and argument list are not approved."
            )
        for key, value in options["spark_conf"].items():
            if key not in self.spark_conf or not any(
                type(value) is type(allowed) and value == allowed for allowed in self.spark_conf[key]
            ):
                raise AdmissionError("The Spark configuration key or value is not approved.")
        for role in ("driver", "executor"):
            resources = options[f"{role}_resources"]
            if (
                resources.cpu > self.limits[f"{role}_cpu"]
                or resources.memory + resources.memory_overhead > self.limits[f"{role}_memory_mb"]
            ):
                raise AdmissionError("Requested CPU or total memory exceeds the submission policy.")
        instances = options["executor_instances"]
        if instances.min is not None and instances.max is None:
            raise AdmissionError("Dynamic allocation requires an explicit maximum executor count.")
        if max(instances.initial or 0, instances.min or 0, instances.max or 0) > self.limits["executors"]:
            raise AdmissionError("Requested executor count exceeds the submission policy.")
        runtime_limit = self.limits.get("max_runtime")
        if runtime_limit is not None and (
            options.get("max_runtime") is None or options["max_runtime"] > runtime_limit
        ):
            raise AdmissionError("The submission policy requires max_runtime within its runtime limit.")
        # No operation in the API waits for a job. All defaults are supplied by JobSpec, not ambient CLI env.
        options.update(app_waiter="no_wait", should_print=False)
        return options


@dataclass(frozen=True)
class SubmissionSettings:
    enabled: bool = False
    token: bytes = field(default=b"", repr=False)
    policy: SubmissionPolicy | None = None

    @classmethod
    def from_env(cls):
        enabled = os.getenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "false").lower()
        if enabled not in {"true", "false", "1", "0"}:
            raise ValueError("SPARK_ON_K8S_SUBMISSION_ENABLED must be true or false.")
        if enabled in {"false", "0"}:
            return cls()
        try:
            token_file = Path(os.environ["SPARK_ON_K8S_SUBMISSION_TOKEN_FILE"])
            policy_file = Path(os.environ["SPARK_ON_K8S_SUBMISSION_POLICY_FILE"])
            with token_file.open("rb") as source:
                token = source.read(4097).strip()
            if not 32 <= len(token) <= 4096 or any(c < 33 or c > 126 for c in token):
                raise ValueError(
                    "Submission token must contain 32–4096 printable ASCII characters, without spaces."
                )
            with policy_file.open("rb") as source:
                data = source.read(MAX_DOCUMENT_BYTES + 1)
            if len(data) > MAX_DOCUMENT_BYTES:
                raise ValueError("Submission policy exceeds 1 MiB.")
            policy = SubmissionPolicy.from_mapping(parse_json(data))
        except (KeyError, OSError) as error:
            raise ValueError("Remote mode requires readable submission token and policy files.") from error
        return cls(True, token, policy)


@lru_cache(maxsize=1)
def get_submission_settings():
    return SubmissionSettings.from_env()
