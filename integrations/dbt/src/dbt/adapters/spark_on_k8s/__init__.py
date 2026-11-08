"""Experimental dbt adapter for invocation-owned Spark on Kubernetes."""

from __future__ import annotations

from dbt.adapters.base import AdapterPlugin
from dbt.adapters.spark_on_k8s.credentials import SparkOnK8SCredentials
from dbt.adapters.spark_on_k8s.impl import SparkOnK8SAdapter
from dbt.include.spark_on_k8s import PACKAGE_PATH

Plugin = AdapterPlugin(
    adapter=SparkOnK8SAdapter,
    credentials=SparkOnK8SCredentials,
    include_path=PACKAGE_PATH,
    dependencies=["spark"],
)
