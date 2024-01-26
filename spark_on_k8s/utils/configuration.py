from __future__ import annotations

import json
from os import getenv

from kubernetes import client as k8s
from kubernetes_asyncio import client as async_k8s


class Configuration:
    """Spark on Kubernetes configuration."""

    SPARK_ON_K8S_DOCKER_IMAGE = getenv("SPARK_ON_K8S_DOCKER_IMAGE")
    SPARK_ON_K8S_APP_PATH = getenv("SPARK_ON_K8S_APP_PATH")
    SPARK_ON_K8S_NAMESPACE = getenv("SPARK_ON_K8S_NAMESPACE", "default")
    SPARK_ON_K8S_SERVICE_ACCOUNT = getenv("SPARK_ON_K8S_SERVICE_ACCOUNT", "spark")
    SPARK_ON_K8S_APP_NAME = getenv("SPARK_ON_K8S_APP_NAME")
    SPARK_ON_K8S_SPARK_CONF = json.loads(getenv("SPARK_ON_K8S_SPARK_CONF", "{}"))
    SPARK_ON_K8S_CLASS_NAME = getenv("SPARK_ON_K8S_CLASS_NAME")
    SPARK_ON_K8S_APP_ARGUMENTS = json.loads(getenv("SPARK_ON_K8S_APP_ARGUMENTS", "[]"))
    SPARK_ON_K8S_APP_WAITER = getenv("SPARK_ON_K8S_APP_WAITER", "no_wait")
    SPARK_ON_K8S_IMAGE_PULL_POLICY = getenv("SPARK_ON_K8S_IMAGE_PULL_POLICY", "IfNotPresent")
    SPARK_ON_K8S_UI_REVERSE_PROXY = getenv("SPARK_ON_K8S_UI_REVERSE_PROXY", "false").lower() == "true"
    SPARK_ON_K8S_DRIVER_CPU = int(getenv("SPARK_ON_K8S_DRIVER_CPU", 1))
    SPARK_ON_K8S_DRIVER_MEMORY = int(getenv("SPARK_ON_K8S_DRIVER_MEMORY", 1024))
    SPARK_ON_K8S_DRIVER_MEMORY_OVERHEAD = int(getenv("SPARK_ON_K8S_DRIVER_MEMORY_OVERHEAD", 512))
    SPARK_ON_K8S_EXECUTOR_CPU = int(getenv("SPARK_ON_K8S_EXECUTOR_CPU", 1))
    SPARK_ON_K8S_EXECUTOR_MEMORY = int(getenv("SPARK_ON_K8S_EXECUTOR_MEMORY", 1024))
    SPARK_ON_K8S_EXECUTOR_MEMORY_OVERHEAD = int(getenv("SPARK_ON_K8S_EXECUTOR_MEMORY_OVERHEAD", 512))
    SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES = (
        int(getenv("SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES"))
        if getenv("SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES")
        else None
    )
    SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES = (
        int(getenv("SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES"))
        if getenv("SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES")
        else None
    )
    SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES = (
        int(getenv("SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES"))
        if getenv("SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES")
        else None
    )

    # Kubernetes client configuration
    # K8S client configuration
    SPARK_ON_K8S_CONFIG_FILE = getenv("SPARK_ON_K8S_CONFIG_FILE", None)
    SPARK_ON_K8S_CONTEXT = getenv("SPARK_ON_K8S_CONTEXT", None)
    SPARK_ON_K8S_CLIENT_CONFIG = (
        k8s.Configuration(json.loads(getenv("SPARK_ON_K8S_CLIENT_CONFIG")))
        if getenv("SPARK_ON_K8S_CLIENT_CONFIG", None)
        else None
    )
    SPARK_ON_K8S_ASYNC_CLIENT_CONFIG = (
        async_k8s.Configuration(json.loads(getenv("SPARK_ON_K8S_ASYNC_CLIENT_CONFIG")))
        if getenv("SPARK_ON_K8S_ASYNC_CLIENT_CONFIG", None)
        else None
    )
    SPARK_ON_K8S_IN_CLUSTER = bool(getenv("SPARK_ON_K8S_IN_CLUSTER", False))
