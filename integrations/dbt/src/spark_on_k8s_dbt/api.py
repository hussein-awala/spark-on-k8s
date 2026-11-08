"""Per-client Kubernetes request budgets for managed dbt startup and cleanup."""

from __future__ import annotations

import inspect
import math
import time
from functools import wraps
from typing import TYPE_CHECKING

from spark_on_k8s.k8s.sync_client import KubernetesClientManager

if TYPE_CHECKING:
    from collections.abc import Callable


def _positive(value, name):
    if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return float(value)


class BoundedKubernetesClientManager(KubernetesClientManager):
    """Bound even submission calls that do not supply their own request timeout.

    ``deadline`` returns an absolute monotonic deadline or None. Each call gets a
    scalar ``_request_timeout`` so the Kubernetes client's transport uses a total
    budget, not independent connection/read budgets. An existing timeout pair is
    conservatively reduced to its shortest finite component; no caller limit is
    lengthened. Retry configuration changes only the newly created ApiClient.
    """

    def __init__(self, *, deadline: Callable[[], float | None], request_timeout=15, **kwargs):
        if not callable(deadline):
            raise ValueError("deadline must be callable")
        self.deadline = deadline
        self.request_timeout = _positive(request_timeout, "request_timeout")
        super().__init__(**kwargs)

    def _budget(self, requested):
        budget = self.request_timeout
        if requested is not None:
            limits = requested if isinstance(requested, tuple) and len(requested) == 2 else (requested,)
            for value in limits:
                if value is not None:
                    budget = min(budget, _positive(value, "_request_timeout"))
        deadline = self.deadline()
        if deadline is not None:
            if type(deadline) not in {int, float} or not math.isfinite(deadline):
                raise ValueError("deadline must return a finite monotonic timestamp or None")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Managed dbt Kubernetes request deadline expired")
            budget = min(budget, remaining)
        return budget

    def create_client(self):
        raw = super().create_client()
        try:
            # urllib3 includes retries in pool keys. Existing clients/configuration
            # and other clusters' pools remain unchanged.
            raw.rest_client.pool_manager.connection_pool_kw["retries"] = 0
            original = raw.call_api
            signature = inspect.signature(original)

            @wraps(original)
            def bounded_call(*args, **kwargs):
                # Generated clients normally use keywords; binding also handles
                # explicit positional _request_timeout without duplicate arguments.
                bound = signature.bind(*args, **kwargs)
                bound.arguments["_request_timeout"] = self._budget(bound.arguments.get("_request_timeout"))
                return original(*bound.args, **bound.kwargs)

            raw.call_api = bounded_call
            return raw
        except BaseException:
            raw.close()
            raise
