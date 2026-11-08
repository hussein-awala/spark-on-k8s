from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from os import getenv
from urllib.parse import urlsplit, urlunsplit

CONSOLE_XCOM_KEY = "spark_on_k8s.console.v1"


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


@dataclass(frozen=True)
class UISettings:
    service_url: str
    allow_writes: bool = False
    public_url: str | None = None

    @classmethod
    def from_environment(cls) -> UISettings:
        return cls(
            service_url=normalize_service_url(getenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", "")),
            public_url=normalize_service_url(getenv("SPARK_ON_K8S_AIRFLOW_PUBLIC_URL"))
            if getenv("SPARK_ON_K8S_AIRFLOW_PUBLIC_URL")
            else None,
            allow_writes=getenv("SPARK_ON_K8S_AIRFLOW_ALLOW_WRITES", "false").lower() == "true",
        )


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
