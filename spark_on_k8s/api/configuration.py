from __future__ import annotations

from os import getenv


class APIConfiguration:
    """API configuration."""

    # API configuration
    # Empty: the API's own namespace; "*": every namespace; otherwise comma-separated names.
    SPARK_ON_K8S_API_NAMESPACES = getenv("SPARK_ON_K8S_API_NAMESPACES", "")
    # Empty: the first namespace in scope, or the current namespace when every namespace is.
    SPARK_ON_K8S_API_DEFAULT_NAMESPACE = getenv("SPARK_ON_K8S_API_DEFAULT_NAMESPACE", "")
    SPARK_ON_K8S_API_HOST = getenv("SPARK_ON_K8S_API_HOST", "127.0.0.1")
    SPARK_ON_K8S_API_PORT = int(getenv("SPARK_ON_K8S_API_PORT", "8000"))
    SPARK_ON_K8S_API_WORKERS = int(getenv("SPARK_ON_K8S_API_WORKERS", "4"))
    SPARK_ON_K8S_API_LOG_LEVEL = getenv("SPARK_ON_K8S_API_LOG_LEVEL", "info")
    SPARK_ON_K8S_API_LIMIT_CONCURRENCY = int(getenv("SPARK_ON_K8S_API_LIMIT_CONCURRENCY", "1000"))
    # Public path prefix, such as /platform/spark, that a gateway removes before forwarding.
    SPARK_ON_K8S_API_ROOT_PATH = getenv("SPARK_ON_K8S_API_ROOT_PATH", "")
    SPARK_ON_K8S_API_SPARK_HISTORY_HOST = getenv("SPARK_ON_K8S_API_SPARK_HISTORY_HOST", None)
    SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON = (
        getenv("SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON", "true").lower() == "true"
    )
    SPARK_ON_K8S_API_WEBSERVER_APPS_DELETE_BUTTON = (
        getenv("SPARK_ON_K8S_API_WEBSERVER_APPS_DELETE_BUTTON", "true").lower() == "true"
    )
    SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL = int(
        getenv("SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL", "10")
    )

    # Exact public http(s) origins; empty means the direct request origin only.
    SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS = getenv("SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS", "")
