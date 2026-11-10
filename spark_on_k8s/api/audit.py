"""Structured audit events and operation counters for the API.

An audit event identifies a request, not a person: the API has no user identity, and the shared
operator bearer is not one. Events are ordinary log lines; shipping, retention and tamper evidence
belong to the deployment's log pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import threading
from collections import Counter
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from uuid import uuid4

from kubernetes.client.exceptions import ApiException
from kubernetes_asyncio.client import ApiException as AsyncApiException
from starlette.routing import compile_path

from spark_on_k8s.history.settings import boolean

AUDIT_VERSION = 1
REQUEST_ID_SCOPE_KEY = "spark_on_k8s.request_id"
logger = logging.getLogger("spark_on_k8s.audit")

# Every outcome of these routes is recorded; other requests only when authentication or the
# namespace scope refuses them.
OPERATIONS = {
    ("POST", "/jobs/v1/plan"): "job.plan",
    ("POST", "/jobs/v1/submit"): "job.submit",
    ("PATCH", "/app/{namespace}/{app_id}"): "app.kill",
    ("DELETE", "/app/{namespace}/{app_id}"): "app.delete",
}
# Authentication refuses requests before routing; these patterns still name refused operations.
_OPERATION_PATHS = {key: compile_path(key[1])[0] for key in OPERATIONS}
# A server or Kubernetes failure leaves the effect of these operations unknown, not absent.
_MUTATIONS = {"job.submit", "app.kill", "app.delete"}
_DENIALS = {"unauthorized", "namespace_forbidden"}
_TARGET_FIELDS = ("namespace", "app_id", "driver_pod", "driver_uid")
_CODE = re.compile(r"[a-z0-9_]{1,64}")
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9a-z-]+")
_CREDENTIAL_HEADERS = {"authorization", "proxy-authorization", "cookie", "set-cookie"}

_record: ContextVar[dict | None] = ContextVar("spark_on_k8s_audit_record", default=None)
_lock = threading.Lock()
_events: Counter[tuple[str, str, str]] = Counter()
_kubernetes_errors: Counter[str] = Counter()


@dataclass(frozen=True)
class AuditSettings:
    enabled: bool = True
    # A gateway-set header, recorded as an unverified claim; the API never authenticates it.
    identity_header: str | None = None

    @classmethod
    def from_env(cls):
        enabled = boolean("SPARK_ON_K8S_AUDIT_LOG_ENABLED", True)
        header = os.getenv("SPARK_ON_K8S_AUDIT_IDENTITY_HEADER", "").strip().lower()
        if header and (not _HEADER_NAME.fullmatch(header) or header in _CREDENTIAL_HEADERS):
            raise ValueError(
                "SPARK_ON_K8S_AUDIT_IDENTITY_HEADER must name a gateway identity header, "
                "not a credential header."
            )
        return cls(enabled, header or None)


@lru_cache(maxsize=1)
def get_audit_settings():
    return AuditSettings.from_env()


def configure_audit_log() -> None:
    """Write audit lines to stdout unless the deployment already routes this logger."""
    if not get_audit_settings().enabled:
        return
    if logger.level == logging.NOTSET:
        logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)


def note(**fields) -> None:
    """Add what the current request is known to target or return; a no-op outside a request."""
    record = _record.get()
    if record is not None:
        record.update(fields)


def count_kubernetes_error(error: BaseException) -> None:
    """Count a Kubernetes API error response by status class; other exceptions are ignored."""
    if isinstance(error, ApiException | AsyncApiException):
        status = error.status if isinstance(error.status, int) else 0
        with _lock:
            _kubernetes_errors[f"{status // 100}xx" if 400 <= status < 600 else "other"] += 1


def metric_lines() -> list[str]:
    """Prometheus text lines for this worker process's counters; families appear once counted."""
    with _lock:
        events, errors = sorted(_events.items()), sorted(_kubernetes_errors.items())
    lines = []
    if events:
        lines += [
            "# HELP spark_on_k8s_api_audit_events_total Audited API operations and access refusals "
            "by event, outcome and code, in this worker process.",
            "# TYPE spark_on_k8s_api_audit_events_total counter",
        ]
        lines += [
            f'spark_on_k8s_api_audit_events_total{{event="{event}",outcome="{outcome}",code="{code}"}} '
            f"{count}"
            for (event, outcome, code), count in events
        ]
    if errors:
        lines += [
            "# HELP spark_on_k8s_api_kubernetes_errors_total Kubernetes API error responses while "
            "serving API requests, by status class, in this worker process.",
            "# TYPE spark_on_k8s_api_kubernetes_errors_total counter",
        ]
        lines += [
            f'spark_on_k8s_api_kubernetes_errors_total{{status_class="{status_class}"}} {count}'
            for status_class, count in errors
        ]
    return lines


class AuditMiddleware:
    """Give every request an ID and record operations and refusals once their outcome is known."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            return await self.app(scope, receive, send)
        # Always generated here: a client-supplied ID could collide with or impersonate another.
        request_id = scope[REQUEST_ID_SCOPE_KEY] = uuid4().hex
        record: dict = {}
        response: dict = {}

        async def send_with_request_id(message):
            if message["type"] == "http.response.start":
                response.setdefault("status", message["status"])
                headers = [(k, v) for k, v in message.get("headers", []) if k.lower() != b"x-request-id"]
                message = {**message, "headers": [*headers, (b"x-request-id", request_id.encode())]}
            elif message["type"] == "websocket.accept":
                response.setdefault("status", 101)
            elif message["type"] == "websocket.close":
                # Closing before accepting rejects the handshake; servers answer it with HTTP 403.
                response.setdefault("status", 403)
            await send(message)

        token = _record.set(record)
        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception as error:
            count_kubernetes_error(error)
            response.setdefault("status", 500)
            raise
        finally:
            _record.reset(token)
            _finish(scope, request_id, record, response.get("status"))


def _route(scope, method: str) -> tuple[str | None, dict]:
    """The matched route template and path parameters; refused unrouted requests have none."""
    route = scope.get("route")
    if route is not None:
        return route.path, scope.get("path_params", {})
    path, root_path = scope["path"], scope.get("root_path", "").rstrip("/")
    if root_path and path.startswith(root_path + "/"):
        path = path[len(root_path) :]
    for (operation_method, template), pattern in _OPERATION_PATHS.items():
        if method == operation_method and (match := pattern.match(path)):
            return template, match.groupdict()
    return None, {}


def _outcome(event: str, status: int | None, code: str) -> str:
    """Classify the response; a submission receipt is not Spark success, nor a kill a stopped app."""
    if status is not None and status < 400:
        return "succeeded"
    if status in {401, 403}:
        return "denied"
    if event in _MUTATIONS and (status is None or status >= 500 or code == "submission_error"):
        return "unknown"
    return "failed" if status is None or status >= 500 else "rejected"


def _text(value, limit=253):
    return value[:limit] if isinstance(value, str) else None


def _code(record: dict, status: int | None) -> str:
    """The error code the client received, as noted by the handler that returned it."""
    code = record.get("code")
    if isinstance(code, str) and _CODE.fullmatch(code):
        return code
    if status is not None and status < 400:
        return "ok"
    # Unhandled exceptions and handle_exception() answer with the internal_error envelope.
    return "internal_error" if status in {None, 500} else "error"


def _finish(scope, request_id: str, record: dict, status: int | None) -> None:
    code = _code(record, status)
    method = scope["method"] if scope["type"] == "http" else "WEBSOCKET"
    template, path_params = _route(scope, method)
    event = OPERATIONS.get((method, template)) or ("access" if code in _DENIALS else None)
    if event is None:
        return
    outcome = _outcome(event, status, code)
    with _lock:
        _events[(event, outcome, code)] += 1
    settings = get_audit_settings()
    if not settings.enabled:
        return
    identity = None
    if settings.identity_header:
        name = settings.identity_header.encode("latin-1")
        values = [value for key, value in scope.get("headers", []) if key.lower() == name]
        # Several values mean the gateway did not replace a client-supplied header.
        identity = _text(values[0].decode("latin-1"), 256) if len(values) == 1 else None
    client = scope.get("client")
    line = {
        "audit_version": AUDIT_VERSION,
        "time": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "event": event,
        "outcome": outcome,
        "code": code,
        "status": status,
        "request_id": request_id,
        "method": method,
        "route": template,
        "client": client[0] if client else None,
        "unverified_identity": identity,
        **{field: _text(record.get(field) or path_params.get(field)) for field in _TARGET_FIELDS},
    }
    logger.info(json.dumps(line, separators=(",", ":")))
