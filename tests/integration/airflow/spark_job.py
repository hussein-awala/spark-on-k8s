from __future__ import annotations

import json
import socket
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
try:
    assert spark.version == sys.argv[1], spark.version
    workers = spark.sparkContext.parallelize(range(2), 2).map(lambda _: socket.gethostname()).collect()
    assert all(worker != socket.gethostname() for worker in workers), workers
    assert spark.range(10).groupBy().sum("id").first()[0] == 45
    print("AIRFLOW_SPARK_EXECUTORS_OK " + json.dumps(workers), flush=True)
    if sys.argv[2] == "failure":
        raise RuntimeError("AIRFLOW_SPARK_EXPECTED_FAILURE")
    print("AIRFLOW_SPARK_SUCCESS", flush=True)
finally:
    spark.stop()
