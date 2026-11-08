from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

_DNS = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def boolean(name: str, default=False) -> bool:
    value = os.getenv(name, str(default)).lower()
    if value not in {"true", "false", "1", "0"}:
        raise ValueError(f"{name} must be true or false")
    return value in {"true", "1"}


def integer(name: str, default: int, lower: int, upper: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if not lower <= value <= upper:
        raise ValueError(f"{name} must be between {lower} and {upper}")
    return value


def namespaces(value: str) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(part.strip() for part in value.split(",")))
    if not 1 <= len(result) <= 20 or not all(_DNS.fullmatch(part) for part in result):
        raise ValueError(
            "Configure one to twenty explicit Kubernetes namespaces; wildcards are not supported"
        )
    return result


@dataclass(frozen=True)
class HistorySettings:
    enabled: bool = False
    path: Path | None = None
    namespaces: tuple[str, ...] = ("default",)
    interval: int = 30
    collection_timeout: int = 15
    request_timeout: int = 5
    page_size: int = 200
    max_pages: int = 20
    retention_days: int = 30
    max_rows: int = 10000
    metrics_enabled: bool = False

    @property
    def lease_seconds(self):
        return self.interval + self.collection_timeout + 10

    @property
    def freshness_seconds(self):
        return self.interval * 2 + self.collection_timeout + 10

    @classmethod
    def from_env(cls):
        enabled = boolean("SPARK_ON_K8S_RUN_HISTORY_ENABLED")
        metrics = boolean("SPARK_ON_K8S_METRICS_ENABLED")
        if not enabled:
            return cls(metrics_enabled=metrics)
        raw_path = os.getenv("SPARK_ON_K8S_RUN_HISTORY_PATH", "")
        path = Path(raw_path).expanduser()
        if not raw_path or not path.is_absolute() or path.name in {"", ":memory:"}:
            raise ValueError("SPARK_ON_K8S_RUN_HISTORY_PATH must be an absolute SQLite file path")
        return cls(
            enabled=True,
            path=path,
            namespaces=namespaces(os.getenv("SPARK_ON_K8S_RUN_HISTORY_NAMESPACES", "default")),
            interval=integer("SPARK_ON_K8S_RUN_HISTORY_INTERVAL", 30, 5, 3600),
            collection_timeout=integer("SPARK_ON_K8S_RUN_HISTORY_TIMEOUT", 15, 1, 300),
            request_timeout=integer("SPARK_ON_K8S_RUN_HISTORY_REQUEST_TIMEOUT", 5, 1, 30),
            page_size=integer("SPARK_ON_K8S_RUN_HISTORY_PAGE_SIZE", 200, 1, 500),
            max_pages=integer("SPARK_ON_K8S_RUN_HISTORY_MAX_PAGES", 20, 1, 100),
            retention_days=integer("SPARK_ON_K8S_RUN_HISTORY_RETENTION_DAYS", 30, 1, 3650),
            max_rows=integer("SPARK_ON_K8S_RUN_HISTORY_MAX_ROWS", 10000, 1, 1000000),
            metrics_enabled=metrics,
        )
