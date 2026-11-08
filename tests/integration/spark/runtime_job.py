from __future__ import annotations

import json
import os
import platform
import socket
import sys

from pyspark.sql import SparkSession


def environment_snapshot(_=None):
    return {
        name: os.environ.get(name)
        for name in (
            "SPARK_COMPAT_ROLE",
            "SPARK_COMPAT_DRIVER_ONLY",
            "SPARK_COMPAT_EMPTY",
            "SPARK_COMPAT_NUMBER",
        )
    }


spark = SparkSession.builder.getOrCreate()
try:
    assert spark.version == sys.argv[1], (spark.version, sys.argv[1])
    # Execute Python workers as well as JVM DataFrame work. A driver-only import is insufficient.
    total = spark.sparkContext.parallelize(range(100), 2).map(lambda value: value * 2).sum()
    assert total == 9900, total
    assert spark.range(100).groupBy().sum("id").first()[0] == 4950
    workers = spark.sparkContext.parallelize(range(2), 2).map(lambda _: socket.gethostname()).collect()
    environment = None
    if spark.sparkContext.master.startswith("k8s://"):
        assert all(host != socket.gethostname() for host in workers), workers
        assert os.environ["SPARK_COMPAT_SENTINEL"] == "present"
        assert spark.sparkContext.parallelize([1], 1).map(
            lambda _: os.environ["SPARK_COMPAT_SENTINEL"]
        ).collect() == ["present"]
        driver_environment = environment_snapshot()
        assert driver_environment == {
            "SPARK_COMPAT_ROLE": "driver-property",
            "SPARK_COMPAT_DRIVER_ONLY": "driver-only",
            "SPARK_COMPAT_EMPTY": "",
            "SPARK_COMPAT_NUMBER": "42",
        }, driver_environment
        executor_environments = (
            spark.sparkContext.parallelize(range(2), 2).map(environment_snapshot).collect()
        )
        assert len(executor_environments) == 2, executor_environments
        assert all(
            worker
            == {
                "SPARK_COMPAT_ROLE": "executor-property",
                "SPARK_COMPAT_DRIVER_ONLY": None,
                "SPARK_COMPAT_EMPTY": None,
                "SPARK_COMPAT_NUMBER": None,
            }
            for worker in executor_environments
        ), executor_environments
        environment = {"driver": driver_environment, "executors": executor_environments}
    print(
        "SPARK_ON_K8S_PYTHON_OK "
        + json.dumps(
            {
                "spark": spark.version,
                "python": platform.python_version(),
                "workers": workers,
                "environment": environment,
            }
        ),
        flush=True,
    )
finally:
    spark.stop()
