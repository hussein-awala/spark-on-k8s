from __future__ import annotations

from os import getenv


class APIConfiguration:
    """API configuration."""

    # API configuration
    SPARK_ON_K8S_API_DEFAULT_NAMESPACE = getenv("SPARK_ON_K8S_API_DEFAULT_NAMESPACE", "default")
    SPARK_ON_K8S_API_HOST = getenv("SPARK_ON_K8S_API_HOST", "127.0.0.1")
    SPARK_ON_K8S_API_PORT = int(getenv("SPARK_ON_K8S_API_PORT", "8000"))
    SPARK_ON_K8S_API_WORKERS = int(getenv("SPARK_ON_K8S_API_WORKERS", "4"))
    SPARK_ON_K8S_API_LOG_LEVEL = getenv("SPARK_ON_K8S_API_LOG_LEVEL", "info")
    SPARK_ON_K8S_API_LIMIT_CONCURRENCY = int(getenv("SPARK_ON_K8S_API_LIMIT_CONCURRENCY", "1000"))
