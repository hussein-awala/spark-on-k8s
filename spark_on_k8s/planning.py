from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field
from typing import Any

_SENSITIVE = re.compile(r"password|passwd|secret|token|credential|access[._-]?key|private[._-]?key", re.I)


def redact(value: Any, *, key: str = "") -> Any:
    """Return a display-only copy. Secret payloads and embedded file contents stay private."""
    if _SENSITIVE.search(key) and not key.endswith(("KeyRef", "secretRef")):
        return "<redacted>"
    if isinstance(value, dict):
        return {name: redact(item, key=name) for name, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        if "=" in value and _SENSITIVE.search(value.partition("=")[0]):
            return value.partition("=")[0] + "=<redacted>"
        value = re.sub(
            r"([?;&](?:password|passwd|secret|token|credential|access[._-]?key|private[._-]?key)=)[^&#;\s]*",
            r"\1<redacted>",
            value,
            flags=re.I,
        )
        return re.sub(r"([a-z][a-z0-9+.-]*://)[^/@\s]+:[^/@\s]+@", r"\1<redacted>@", value, flags=re.I)
    return value


@dataclass(frozen=True)
class ApplicationPlan:
    """An offline preview; generated identifiers are illustrative, not reserved runs."""

    namespace: str
    app_id: str
    driver_pod: str
    resources: tuple[dict[str, Any], ...] = field(repr=False)

    def to_dict(self) -> dict[str, Any]:
        resources = copy.deepcopy(self.resources)
        for resource in resources:
            if resource["kind"] in {"Secret", "ConfigMap"}:
                for field in ("data", "stringData", "binaryData"):
                    if field in resource:
                        resource[field] = dict.fromkeys(resource[field], "<redacted>")
            if resource["kind"] == "Pod":
                containers = resource["spec"].get("containers", [])
                for index, container in enumerate(containers + resource["spec"].get("initContainers", [])):
                    for variable in container.get("env", []):
                        if "value" in variable:
                            variable["value"] = "<redacted>"
                    # Positional application arguments can contain credentials with no identifying key.
                    args = container.get("args", [])
                    if "command" in container:
                        container["command"] = ["<redacted command>" for _ in container["command"]]
                    if index == 0 and containers and args and args[0] == "driver":
                        position = 1
                        while position < len(args) and args[position].startswith("--"):
                            position += 2
                        if position < len(args):
                            args[position + 1 :] = ["<redacted argument>" for _ in args[position + 1 :]]
                    elif args:
                        container["args"] = ["<redacted argument>" for _ in args]
        return {
            "schema_version": 1,
            "preview": True,
            "namespace": self.namespace,
            "app_id": self.app_id,
            "driver_pod": self.driver_pod,
            "resources": [redact(resource) for resource in resources],
        }
