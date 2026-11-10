from __future__ import annotations

import json
import os
import socket
import sys
import time

from pyspark.sql import SparkSession


def environment_snapshot(_=None):
    return {
        name: os.environ.get(name)
        for name in (
            "AIRFLOW_SPARK_ROLE",
            "AIRFLOW_SPARK_DRIVER_ONLY",
            "AIRFLOW_SPARK_EMPTY",
            "AIRFLOW_SPARK_NUMBER",
        )
    }


spark = SparkSession.builder.getOrCreate()
try:
    assert spark.version == sys.argv[1], spark.version
    workers = spark.sparkContext.parallelize(range(2), 2).map(lambda _: socket.gethostname()).collect()
    assert all(worker != socket.gethostname() for worker in workers), workers
    assert spark.range(10).groupBy().sum("id").first()[0] == 45
    print("AIRFLOW_SPARK_EXECUTORS_OK " + json.dumps(workers), flush=True)
    driver_environment = environment_snapshot()
    assert driver_environment == {
        "AIRFLOW_SPARK_ROLE": "driver-property",
        "AIRFLOW_SPARK_DRIVER_ONLY": "driver-only",
        "AIRFLOW_SPARK_EMPTY": "",
        "AIRFLOW_SPARK_NUMBER": "42",
    }, driver_environment
    executor_environments = spark.sparkContext.parallelize(range(2), 2).map(environment_snapshot).collect()
    assert len(executor_environments) == 2, executor_environments
    assert all(
        worker
        == {
            "AIRFLOW_SPARK_ROLE": "executor-property",
            "AIRFLOW_SPARK_DRIVER_ONLY": None,
            "AIRFLOW_SPARK_EMPTY": None,
            "AIRFLOW_SPARK_NUMBER": None,
        }
        for worker in executor_environments
    ), executor_environments
    print(
        "AIRFLOW_SPARK_ENVIRONMENT_OK "
        + json.dumps({"driver": driver_environment, "executors": executor_environments}),
        flush=True,
    )
    if sys.argv[2] == "failure":
        raise RuntimeError("AIRFLOW_SPARK_EXPECTED_FAILURE")
    if sys.argv[2] == "slow":
        # Keeps the driver running while a mapped task's retry looks for it.
        time.sleep(90)
    print("AIRFLOW_SPARK_SUCCESS", flush=True)
finally:
    spark.stop()
