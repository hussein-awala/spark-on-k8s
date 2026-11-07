from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass


@dataclass(frozen=True, kw_only=True)
class RunHandle:
    """Stable identity of a submitted run, without credentials or live client state."""

    namespace: str
    app_id: str
    driver_pod: str

    def to_dict(self) -> dict[str, str]:
        """Return JSON-serializable identity fields."""
        return asdict(self)


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
