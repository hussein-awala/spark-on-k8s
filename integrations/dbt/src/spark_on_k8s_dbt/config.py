from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from spark_on_k8s.client import ExecutorInstances, PodResources
from spark_on_k8s.connect_server import ConnectServerConfig


@dataclass(frozen=True)
class ComputeConfig:
    server: ConnectServerConfig
    kube_config_file: str | None
    kube_context: str | None
    in_cluster: bool
    startup_timeout: float
    cleanup_timeout: float
    poll_interval: float

    @classmethod
    def parse(cls, method: str, values: dict) -> ComputeConfig:
        if method not in {"connect", "thrift"}:
            raise ValueError("Experimental dbt method must be connect or thrift")
        if not isinstance(values, dict):
            raise ValueError("compute must be a mapping")
        values = deepcopy(values)
        runtime = {}
        for name, default in (("startup_timeout", 180), ("cleanup_timeout", 120), ("poll_interval", 1)):
            value = values.pop(name, default)
            if type(value) not in {int, float} or not math.isfinite(value) or not 0 < value <= 3600:
                raise ValueError(f"compute.{name} must be finite and in (0, 3600] seconds")
            runtime[name] = value
        in_cluster = values.pop("in_cluster", False)
        if type(in_cluster) is not bool:
            raise ValueError("compute.in_cluster must be a boolean")
        config_file = values.pop("kube_config_file", None)
        context = values.pop("kube_context", None)
        if in_cluster:
            if config_file is not None or context is not None:
                raise ValueError("In-cluster compute cannot also select a kubeconfig/context")
        else:
            if (
                not isinstance(config_file, str)
                or not config_file
                or not isinstance(context, str)
                or not context
            ):
                raise ValueError("Local compute requires explicit kube_config_file and kube_context")
            config_file = str(Path(config_file).expanduser().resolve(strict=True))
        values.setdefault("app_name", "dbt-" + method)
        values.setdefault("port", 10000 if method == "thrift" else 15002)
        if method == "thrift" and values["port"] != 10000:
            raise ValueError("Experimental Thrift requires port 10000")
        for key, factory in (
            ("driver_resources", PodResources),
            ("executor_resources", PodResources),
            ("executor_instances", ExecutorInstances),
        ):
            if key in values:
                if not isinstance(values[key], dict):
                    raise ValueError(f"compute.{key} must be a mapping")
                values[key] = factory(**values[key])
        server = ConnectServerConfig(**values)
        if method == "thrift":
            from spark_on_k8s_dbt.thrift_server import reject_unsupported_fields

            reject_unsupported_fields(server)
        if "spark.sql.warehouse.dir" not in server.spark_conf:
            raise ValueError("compute.spark_conf must specify a shared spark.sql.warehouse.dir")
        warehouse = server.spark_conf["spark.sql.warehouse.dir"]
        parsed = urlsplit(warehouse)
        if (
            not parsed.scheme
            or parsed.scheme.lower() in {"file", "local"}
            or not parsed.path
            or any(char.isspace() for char in warehouse)
        ):
            raise ValueError(
                "compute warehouse must be a nonempty remote filesystem URI, not driver-local storage"
            )
        # Static Hive configuration belongs on the server, never on a Connect session.
        if server.spark_conf.get("spark.sql.catalogImplementation") != "hive":
            raise ValueError("Experimental dbt requires spark.sql.catalogImplementation: hive")
        if not server.spark_conf.get("spark.hadoop.hive.metastore.uris"):
            raise ValueError("Experimental dbt requires an external spark.hadoop.hive.metastore.uris")
        return cls(server, config_file, context, in_cluster, **runtime)

    def cluster_identity(self) -> dict:
        return {
            "kube_config_file": self.kube_config_file,
            "kube_context": self.kube_context,
            "in_cluster": self.in_cluster,
        }
