from __future__ import annotations

import hmac
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
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
# Kubernetes label names and nonempty values share this syntax; a key may add a DNS-subdomain prefix.
LABEL_TEXT = re.compile(r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?")
DNS_SUBDOMAIN = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*")
# Spark, this project and Kubernetes own these labels; selectors and ownership checks rely on them.
RESERVED_LABEL_PREFIXES = ("spark-on-k8s.io", "kubernetes.io", "k8s.io")
# A rotation overlap needs two equivalent tokens; more would suggest per-client identities.
MAX_TOKENS = 2
MAX_TOKEN_FILE_BYTES = 64 * 1024
DEFAULT_RELOAD_INTERVAL = 10
logger = logging.getLogger(__name__)


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


def _validate_placement(app, namespaces):
    """An application's optional placement narrows the policy's namespace/service-account pairs."""
    allowed = namespaces
    if "namespaces" in app:
        if not set(_strings(app["namespaces"], "Approved application namespaces")) <= set(namespaces):
            raise ValueError("Approved application namespaces must be policy namespaces.")
        allowed = {namespace: namespaces[namespace] for namespace in app["namespaces"]}
    if "service_accounts" in app and not set(
        _strings(app["service_accounts"], "Approved application service accounts")
    ) <= {account for accounts in allowed.values() for account in accounts}:
        raise ValueError(
            "Approved application service accounts must be approved for one of its policy namespaces."
        )


def _label_key(key):
    prefix, slash, name = key.partition("/") if isinstance(key, str) else ("", "", "")
    if not slash:
        prefix, name = "", prefix
    if not LABEL_TEXT.fullmatch(name) or slash and (len(prefix) > 253 or not DNS_SUBDOMAIN.fullmatch(prefix)):
        raise ValueError(f"Policy label key {key!r} is not a Kubernetes label key.")
    if (not slash and name.startswith("spark-")) or any(
        prefix == reserved or prefix.endswith("." + reserved) for reserved in RESERVED_LABEL_PREFIXES
    ):
        raise ValueError(f"Policy label key {key} is reserved for Spark, spark-on-k8s or Kubernetes.")
    return key


def _label_policy(document):
    """Parse required label rules as (values, pattern) and fixed labels as exact values."""
    labels = _object(document, {"required", "fixed"}, "label policy")
    if not labels or not all(isinstance(section, dict) and section for section in labels.values()):
        raise ValueError("Policy label sections must be nonempty objects.")
    rules = {}
    for key, rule in labels.get("required", {}).items():
        rule = _object(rule, {"values", "pattern"}, "required label rule")
        if len(rule) > 1:
            raise ValueError("A required label rule may declare values or a pattern, not both.")
        values, pattern = rule.get("values"), rule.get("pattern")
        if values is not None and not all(LABEL_TEXT.fullmatch(v) for v in _strings(values, "Label values")):
            raise ValueError("Required label values must be nonempty Kubernetes label values.")
        if pattern is not None:
            try:
                pattern = re.compile(pattern) if isinstance(pattern, str) and pattern else None
            except re.error:
                pattern = None
            if pattern is None:
                raise ValueError("A required label pattern must be a valid regular expression.")
        rules[_label_key(key)] = (values, pattern)
    fixed = {}
    for key, value in labels.get("fixed", {}).items():
        if _label_key(key) in rules:
            raise ValueError("A label cannot be both required and fixed.")
        if not isinstance(value, str) or not LABEL_TEXT.fullmatch(value):
            raise ValueError("Fixed label values must be nonempty Kubernetes label values.")
        fixed[key] = value
    return rules, fixed


@dataclass(frozen=True)
class SubmissionPolicy:
    namespaces: dict[str, list[str]]
    applications: list[dict[str, Any]]
    limits: dict[str, int]
    spark_conf: dict[str, list[Any]]
    # Label key -> (allowed values or None, full-match pattern or None).
    required_labels: dict[str, tuple[list[str] | None, re.Pattern | None]] = field(default_factory=dict)
    fixed_labels: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, document):
        doc = _object(
            document,
            {"schema_version", "namespaces", "applications", "limits", "spark_conf", "labels"},
            "policy",
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
            _object(
                app,
                {"image", "app_path", "class_name", "arguments", "namespaces", "service_accounts"},
                "approved application",
            )
            _validate_placement(app, namespaces)
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
        required_labels, fixed_labels = _label_policy(doc["labels"]) if "labels" in doc else ({}, {})
        return cls(namespaces, applications, limits, conf, required_labels, fixed_labels)

    def admit(self, document):
        # Import only when enabled. The shared JobSpec owns schema/type validation.
        from spark_on_k8s.jobs import JobSpec

        if not isinstance(document, dict) or set(document) - {"apiVersion", "kind", "metadata", "spec"}:
            raise AdmissionError("Remote jobs cannot select profiles or Kubernetes credentials.")
        spec = document.get("spec")
        # Driver labels are admitted only to supply the policy's required labels.
        label_fields = {"driver_labels"} if self.required_labels else set()
        if not isinstance(spec, dict) or set(spec) - ALLOWED_SPEC - label_fields:
            raise AdmissionError("Remote jobs contain a setting that is not admitted by the submission API.")
        job = JobSpec.from_mapping(document)
        options = job.to_submit_kwargs()
        namespace, account = options["namespace"], options["service_account"]
        if account not in self.namespaces.get(namespace, []):
            raise AdmissionError("The namespace and service-account pair is not approved.")
        matches = [
            app
            for app in self.applications
            if options["image"] == app["image"]
            and options["app_path"] == app["app_path"]
            and options["class_name"] == app.get("class_name")
            and options["app_arguments"] in app.get("arguments", [[]])
        ]
        if not matches:
            raise AdmissionError(
                "The image, application entrypoint, class and argument list are not approved."
            )
        if not any(
            namespace in app.get("namespaces", [namespace])
            and account in app.get("service_accounts", [account])
            for app in matches
        ):
            raise AdmissionError(
                "The application is not approved for this namespace and service-account pair."
            )
        self._admit_labels(options)
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

    def _admit_labels(self, options):
        """Check required cost labels, then apply them with the fixed labels to driver and executors."""
        labels = options["driver_labels"]
        missing = sorted(set(self.required_labels) - set(labels))
        if missing:
            raise AdmissionError(f"The submission policy requires the driver labels: {', '.join(missing)}.")
        if set(labels) - set(self.required_labels):
            raise AdmissionError("Remote jobs can set only the driver labels the submission policy requires.")
        for key, (values, pattern) in self.required_labels.items():
            value = labels[key]
            if (
                not LABEL_TEXT.fullmatch(value)
                or (values is not None and value not in values)
                or (pattern is not None and not pattern.fullmatch(value))
            ):
                raise AdmissionError(
                    f"The submission policy does not approve the value of driver label {key}."
                )
        labels = {**labels, **self.fixed_labels}
        options.update(driver_labels=labels, executor_labels=dict(labels))


def _identity(status):
    return status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns, status.st_ctime_ns


def _read_file(path, limit):
    # Identify the file actually read: a Kubernetes Secret update swaps a symlink to new files.
    with path.open("rb") as source:
        return source.read(limit + 1), _identity(os.fstat(source.fileno()))


def _current_identities(paths):
    identities = []
    for path in paths:
        try:
            identities.append(_identity(os.stat(path)))
        except OSError:
            identities.append(None)
    return tuple(identities)


def _tokens(data):
    tokens = [line.strip() for line in data.splitlines() if line.strip()]
    if len(data) > MAX_TOKEN_FILE_BYTES or not 1 <= len(tokens) <= MAX_TOKENS:
        raise ValueError(
            "The submission token file must contain one token, or two on separate lines during a rotation."
        )
    for token in tokens:
        if not 32 <= len(token) <= 4096 or any(c < 33 or c > 126 for c in token):
            raise ValueError(
                "Submission token must contain 32–4096 printable ASCII characters, without spaces."
            )
    return tokens


@dataclass(frozen=True)
class SubmissionSettings:
    enabled: bool = False
    token: bytes = field(default=b"", repr=False)
    policy: SubmissionPolicy | None = None
    # Accepted like token during a rotation overlap: equivalent operator credentials, not identities.
    extra_tokens: tuple[bytes, ...] = field(default=(), repr=False)
    reload_interval: int = 0
    # The token and policy files, with the identity of each version read.
    sources: tuple[tuple[Path, tuple[int, ...]], ...] = field(default=(), repr=False)

    def accepts(self, credential: bytes) -> bool:
        # Compare with every token so the response time does not reveal which one matched.
        accepted = False
        for token in (self.token, *self.extra_tokens):
            accepted |= hmac.compare_digest(credential, token)
        return accepted

    @classmethod
    def from_env(cls):
        enabled = os.getenv("SPARK_ON_K8S_SUBMISSION_ENABLED", "false").lower()
        if enabled not in {"true", "false", "1", "0"}:
            raise ValueError("SPARK_ON_K8S_SUBMISSION_ENABLED must be true or false.")
        if enabled in {"false", "0"}:
            return cls()
        interval = os.getenv("SPARK_ON_K8S_SUBMISSION_RELOAD_INTERVAL", str(DEFAULT_RELOAD_INTERVAL))
        if not re.fullmatch(r"[0-9]{1,4}", interval.strip()) or int(interval) > 3600:
            raise ValueError("SPARK_ON_K8S_SUBMISSION_RELOAD_INTERVAL must be 0 (disabled) to 3600 seconds.")
        try:
            token_file = Path(os.environ["SPARK_ON_K8S_SUBMISSION_TOKEN_FILE"])
            policy_file = Path(os.environ["SPARK_ON_K8S_SUBMISSION_POLICY_FILE"])
        except KeyError as error:
            raise ValueError("Remote mode requires readable submission token and policy files.") from error
        return cls.read(token_file, policy_file, int(interval))

    @classmethod
    def read(cls, token_file: Path, policy_file: Path, reload_interval: int = 0):
        try:
            data, token_identity = _read_file(token_file, MAX_TOKEN_FILE_BYTES)
            tokens = _tokens(data)
            data, policy_identity = _read_file(policy_file, MAX_DOCUMENT_BYTES)
        except OSError as error:
            raise ValueError("Remote mode requires readable submission token and policy files.") from error
        if len(data) > MAX_DOCUMENT_BYTES:
            raise ValueError("Submission policy exceeds 1 MiB.")
        policy = SubmissionPolicy.from_mapping(parse_json(data))
        sources = ((token_file, token_identity), (policy_file, policy_identity))
        return cls(True, tokens[0], policy, tuple(tokens[1:]), reload_interval, sources)


class SettingsSource:
    """Serve the last valid settings, re-reading changed files at most once per reload interval."""

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._lock = threading.Lock()
        self._settings: SubmissionSettings | None = None
        self._next_check = 0.0
        self._rejected = None

    def get(self) -> SubmissionSettings:
        settings = self._settings
        if settings is None:
            with self._lock:
                if self._settings is None:
                    # Startup still fails closed on missing or invalid configuration.
                    self._settings = SubmissionSettings.from_env()
                    self._next_check = self._clock() + self._settings.reload_interval
                return self._settings
        # Requests never wait for another request's check; they use the settings already loaded.
        if settings.reload_interval and self._clock() >= self._next_check and self._lock.acquire(False):
            try:
                settings = self._settings = self._reload(settings)
                self._next_check = self._clock() + settings.reload_interval
            finally:
                self._lock.release()
        return settings

    def _reload(self, settings):
        paths = [path for path, _ in settings.sources]
        current = _current_identities(paths)
        if current == tuple(identity for _, identity in settings.sources) or current == self._rejected:
            return settings
        try:
            updated = SubmissionSettings.read(*paths, settings.reload_interval)
            # A changed policy must pass the same namespace-scope check as at startup.
            from spark_on_k8s.api.namespace_scope import namespace_scope

            namespace_scope().require_configured(updated.policy.namespaces, "The submission policy")
        except Exception as error:
            # Report a changed file once; never include token values in the message.
            self._rejected = current
            logger.error(
                "Kept the previous remote submission token and policy; the changed files are invalid: %s",
                error,
            )
            return settings
        self._rejected = None
        # Credential and policy changes are security relevant, so they are visible at the default level.
        logger.warning(
            "Reloaded the remote submission token and policy files; accepted tokens: %d.",
            1 + len(updated.extra_tokens),
        )
        return updated


_source = SettingsSource()


def get_submission_settings() -> SubmissionSettings:
    return _source.get()
