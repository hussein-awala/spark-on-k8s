from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass

# Kubernetes accepts any positive int64 deadline; a bound catches unit mistakes such as milliseconds.
MAX_RUNTIME_LIMIT = 30 * 24 * 60 * 60


def validate_max_runtime(value: int | None, name: str = "max_runtime") -> int | None:
    """Validate an optional driver lifetime in whole seconds; None means no limit."""
    if value is not None and (type(value) is not int or not 1 <= value <= MAX_RUNTIME_LIMIT):
        raise ValueError(
            f"{name} must be a whole number of seconds from 1 to {MAX_RUNTIME_LIMIT} (30 days), or unset"
        )
    return value


@dataclass(frozen=True, kw_only=True)
class RunHandle:
    """Stable identity of a submitted run, without credentials or live client state."""

    namespace: str
    app_id: str
    driver_pod: str
    driver_uid: str | None = None

    def to_dict(self) -> dict[str, str]:
        """Return JSON-serializable identity fields."""
        result = asdict(self)
        if self.driver_uid is None:
            result.pop("driver_uid")
        return result


class WaitDeadline:
    """A monotonic timeout shared by polling, requests, and log streaming."""

    def __init__(self, timeout: float | None = None):
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("Wait timeout must be a positive finite number of seconds")
        self.end = None if timeout is None else time.monotonic() + timeout

    def remaining(self) -> float | None:
        if self.end is None:
            return None
        remaining = self.end - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Application wait timeout exceeded")
        return remaining

    def request_options(self) -> dict[str, float]:
        remaining = self.remaining()
        return {} if remaining is None else {"_request_timeout": remaining}

    def sleep(self, seconds: float) -> None:
        remaining = self.remaining()
        time.sleep(seconds if remaining is None else min(seconds, remaining))
        self.remaining()
