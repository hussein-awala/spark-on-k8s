from __future__ import annotations

import json
import os
import platform
import socket
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
try:
    assert spark.version == sys.argv[1], (spark.version, sys.argv[1])
    # Execute Python workers as well as JVM DataFrame work. A driver-only import is insufficient.
    total = spark.sparkContext.parallelize(range(100), 2).map(lambda value: value * 2).sum()
    assert total == 9900, total
    assert spark.range(100).groupBy().sum("id").first()[0] == 4950
    workers = spark.sparkContext.parallelize(range(2), 2).map(lambda _: socket.gethostname()).collect()
    if spark.sparkContext.master.startswith("k8s://"):
        assert all(host != socket.gethostname() for host in workers), workers
        assert os.environ["SPARK_COMPAT_SENTINEL"] == "present"
        assert spark.sparkContext.parallelize([1], 1).map(
            lambda _: os.environ["SPARK_COMPAT_SENTINEL"]
        ).collect() == ["present"]
    print(
        "SPARK_ON_K8S_PYTHON_OK "
        + json.dumps({"spark": spark.version, "python": platform.python_version(), "workers": workers}),
        flush=True,
    )
finally:
    spark.stop()
