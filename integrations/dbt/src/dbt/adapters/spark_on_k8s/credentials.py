"""Profiles for invocation-owned Spark compute, without a user-supplied endpoint."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from dbt.adapters.spark.connections import SparkCredentials
from dbt_common.exceptions import DbtRuntimeError


@dataclass
class SparkOnK8SCredentials(SparkCredentials):
    method: Literal["thrift", "connect"] = "thrift"
    host: str = "managed"
    port: int = 0
    user: str = "dbt"
    auth: str = "NONE"
    compute: dict[str, Any] = field(default_factory=dict)
    query_retries: int = 0

    def __post_init__(self) -> None:
        if self.method not in {"thrift", "connect"}:
            raise DbtRuntimeError("spark_on_k8s supports only method: thrift or method: connect")
        if not isinstance(self.schema, str) or not self.schema.strip():
            raise DbtRuntimeError("Specify a non-empty schema in the spark_on_k8s profile")
        if not isinstance(self.compute, dict) or not self.compute:
            raise DbtRuntimeError("Specify the managed server settings in the profile's compute mapping")
        if self.host != "managed" or self.port != 0:
            raise DbtRuntimeError("Do not set host or port: spark-on-k8s-dbt manages the endpoint")
        if (
            self.auth != "NONE"
            or self.use_ssl
            or any((self.password, self.token, self.kerberos_service_name, self.cluster, self.endpoint))
        ):
            raise DbtRuntimeError("Managed Spark currently supports private endpoints with auth: NONE only")
        if not isinstance(self.server_side_parameters, dict):
            raise DbtRuntimeError("server_side_parameters must be a mapping")
        static_sql = (
            "spark.sql.warehouse",
            "spark.sql.catalog",
            "spark.sql.defaultCatalog",
            "spark.sql.extensions",
            "spark.sql.globalTempDatabase",
            "spark.sql.hive.metastore",
            "spark.sql.hive.thriftServer",
        )
        for key in self.server_side_parameters:
            if not isinstance(key, str) or not key.startswith("spark.sql.") or key.startswith(static_sql):
                raise DbtRuntimeError(
                    "server_side_parameters accepts Spark SQL runtime settings only; "
                    "put catalog, warehouse and server configuration in compute.spark_conf"
                )
        if self.query_retries != 0:
            raise DbtRuntimeError("query_retries must be 0: failed statements must not replay writes")
        if self.method == "connect" and self.query_timeout is not None:
            raise DbtRuntimeError(
                "query_timeout is not supported for Connect; cancel the dbt invocation instead"
            )
        super().__post_init__()

    @property
    def type(self) -> str:
        return "spark_on_k8s"

    def _connection_keys(self) -> tuple[str, ...]:
        # Compute configuration can contain external catalog credentials.
        return "method", "schema", "user"
