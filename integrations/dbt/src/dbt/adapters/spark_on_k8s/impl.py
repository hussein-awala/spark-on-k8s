"""Spark macro and materialization inheritance with an explicit SQL-only scope."""

from __future__ import annotations

from dataclasses import replace

from dbt.adapters.spark import SparkAdapter
from dbt.adapters.spark_on_k8s.__version__ import version
from dbt.adapters.spark_on_k8s.connections import SparkOnK8SConnectionManager
from dbt_common.exceptions import DbtRuntimeError


class SparkOnK8SAdapter(SparkAdapter):
    ConnectionManager = SparkOnK8SConnectionManager

    @classmethod
    def get_adapter_run_info(cls, config):
        # dbt's default derives a module name from the class name, losing the
        # underscores in our registered type. This runs even with tracking off.
        parent = SparkAdapter.get_adapter_run_info(config)
        return replace(
            parent,
            adapter_name=cls.type(),
            adapter_version=version,
            model_adapter_details={**parent.model_adapter_details, "adapter_type": cls.type()},
        )

    def submit_python_job(self, parsed_model: dict, compiled_code: str):
        raise DbtRuntimeError(
            "The experimental spark_on_k8s adapter supports SQL models only; Python models are not supported"
        )

    @property
    def python_submission_helpers(self):
        return {}

    @property
    def default_python_submission_method(self) -> str:
        return "unsupported"
