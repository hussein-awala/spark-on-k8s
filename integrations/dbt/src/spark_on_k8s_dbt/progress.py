"""Human-readable managed-compute progress on stderr, never on dbt's stdout."""

from __future__ import annotations

import math
import sys
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

TRANSPORTS = {"connect": "Spark Connect", "thrift": "Thrift"}
_IMAGE_ERRORS = {"ErrImagePull", "ImagePullBackOff", "InvalidImageName", "ErrImageNeverPull"}


def dbt_allows_progress() -> bool:
    """Follow dbt's resolved --quiet and --log-level, including DBT_* variables and project flags."""
    try:
        from dbt.flags import get_flags
    except ImportError:
        return True
    flags = get_flags()
    if getattr(flags, "QUIET", False):
        return False
    return str(getattr(flags, "LOG_LEVEL", "info") or "info").lower() not in {"warn", "error", "none"}


class StartupProgress:
    """Write concise phase lines with elapsed time while managed compute starts or is deleted.

    dbt sends its console log (adapter events included) and machine-readable output
    to stdout, so these lines bypass dbt's event logger and go to stderr only. A new
    state prints immediately; a state already shown, whether persisting or
    recurring, repeats at most once per ``heartbeat`` seconds. ``allowed`` is
    consulted for every line. Lines contain no Spark configuration, container logs
    or Kubernetes event text.
    """

    def __init__(self, *, allowed: Callable[[], bool] = dbt_allows_progress, heartbeat: float = 30):
        if type(heartbeat) not in {int, float} or not math.isfinite(heartbeat) or heartbeat <= 0:
            raise ValueError("heartbeat must be positive and finite")
        self.allowed = allowed
        self.heartbeat = heartbeat
        self._lock = threading.Lock()
        self._started = time.monotonic()
        self._shown: dict[str, float] = {}

    @classmethod
    def silent(cls) -> StartupProgress:
        return cls(allowed=lambda: False)

    def start(self) -> None:
        """Measure elapsed time for a new operation: startup, a tunnel reopen or cleanup."""
        with self._lock:
            self._started = time.monotonic()
            self._shown.clear()

    def update(self, state: str, message: str) -> None:
        with self._lock:
            now = time.monotonic()
            if state in self._shown and now - self._shown[state] < self.heartbeat:
                return
            self._shown[state] = now
            if not self.allowed():
                return
            try:
                # Resolve the stream at write time: dbt runners and tests may replace it.
                sys.stderr.write(f"Managed Spark +{math.floor(now - self._started)}s: {message}\n")
                sys.stderr.flush()
            except (OSError, ValueError):
                pass  # Progress is advisory; a closed stream must not fail startup or cleanup.


def driver_state(pod) -> tuple[str, str]:
    """Summarize a starting driver as (state, message) from its Pod status only.

    Progress is advisory: absent status fields mean "no detail", never an error.
    """
    status = getattr(pod, "status", None)
    if getattr(status, "phase", None) == "Running":
        return "running", "driver container running"
    node = getattr(getattr(pod, "spec", None), "node_name", None)
    if not node:
        for condition in getattr(status, "conditions", None) or []:
            if condition.type == "PodScheduled" and condition.status == "False" and condition.reason:
                detail = condition.reason + (f": {condition.message}" if condition.message else "")
                if len(detail) > 160:
                    detail = detail[:157] + "..."
                return (
                    "unscheduled:" + condition.reason,
                    f"waiting for the driver pod to be scheduled ({detail})",
                )
        return "unscheduled", "waiting for the driver pod to be scheduled"
    containers = (getattr(status, "init_container_statuses", None) or []) + (
        getattr(status, "container_statuses", None) or []
    )
    for container in containers:
        waiting = getattr(container.state, "waiting", None)
        if waiting is not None and waiting.reason:
            hint = "; check compute.image and image pull access" if waiting.reason in _IMAGE_ERRORS else ""
            return (
                "waiting:" + waiting.reason,
                f"driver pod scheduled on {node}; container {container.name} is {waiting.reason}{hint}",
            )
    return "scheduled", f"driver pod scheduled on {node}; starting its containers"
