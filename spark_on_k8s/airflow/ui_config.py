from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from os import getenv
from urllib.parse import urlsplit, urlunsplit

CONSOLE_XCOM_KEY = "spark_on_k8s.console.v1"
PROVIDER_OPERATORS = ("SparkOnK8SOperator", "SparkSqlOnK8SOperator")
DEFAULT_SNAPSHOT_TTL = 5.0
MAX_SNAPSHOT_TTL = 60.0


def normalize_service_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "\\" in value
    ):
        raise ValueError(
            "The Airflow Spark service URL must be an HTTP(S) base URL without credentials or query."
        )
    # Accessing port also validates malformed values.
    _ = parsed.port
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def parse_operator_subclasses(value: str | None) -> tuple[str, ...]:
    names = tuple(dict.fromkeys(name.strip() for name in (value or "").split(",") if name.strip()))
    if not all(name.isidentifier() for name in names):
        raise ValueError("Airflow operator subclasses must be comma-separated Python class names.")
    return tuple(name for name in names if name not in PROVIDER_OPERATORS)


def parse_snapshot_ttl(value: str | None) -> float:
    if value is None or not value.strip():
        return DEFAULT_SNAPSHOT_TTL
    ttl = float(value)
    if not math.isfinite(ttl) or not 0 <= ttl <= MAX_SNAPSHOT_TTL:
        raise ValueError(
            f"The Airflow Spark snapshot TTL must be between 0 and {MAX_SNAPSHOT_TTL:g} seconds."
        )
    return ttl


@dataclass(frozen=True)
class UISettings:
    service_url: str
    allow_writes: bool = False
    public_url: str | None = None
    # Display scoping only: the backend verifies each task's operator lineage itself.
    operator_subclasses: tuple[str, ...] = ()
    snapshot_ttl: float = DEFAULT_SNAPSHOT_TTL

    @classmethod
    def from_environment(cls) -> UISettings:
        return cls(
            service_url=normalize_service_url(getenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", "")),
            public_url=normalize_service_url(getenv("SPARK_ON_K8S_AIRFLOW_PUBLIC_URL"))
            if getenv("SPARK_ON_K8S_AIRFLOW_PUBLIC_URL")
            else None,
            allow_writes=getenv("SPARK_ON_K8S_AIRFLOW_ALLOW_WRITES", "false").lower() == "true",
            operator_subclasses=parse_operator_subclasses(getenv("SPARK_ON_K8S_AIRFLOW_OPERATOR_SUBCLASSES")),
            snapshot_ttl=parse_snapshot_ttl(getenv("SPARK_ON_K8S_AIRFLOW_SNAPSHOT_TTL")),
        )

    @property
    def task_operators(self) -> list[str]:
        """Operator class names whose task instances show the native tab."""
        return [*PROVIDER_OPERATORS, *self.operator_subclasses]


def ui_plugin_enabled() -> bool:
    if not getenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL"):
        return False
    try:
        airflow_version = version("apache-airflow")
        major, minor = (int(part) for part in airflow_version.split(".")[:2])
        if major != 3 or minor < 4:
            return False
        UISettings.from_environment()
    except (PackageNotFoundError, ValueError):
        logging.getLogger(__name__).warning(
            "Spark task UI is disabled: unsupported Airflow or invalid configuration"
        )
        return False
    return True
