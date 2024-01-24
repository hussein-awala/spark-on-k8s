from __future__ import annotations

import logging

from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark_client = SparkOnK8S()
    spark_client.submit_app(
        image="pyspark-job",
        app_path="local:///opt/spark/work-dir/job.py",
        app_arguments=["100000"],
        app_name="pyspark-job-example",
        namespace="spark",
        service_account="spark",
        app_waiter="log",
        # If you test this locally (minikube or kind) without pushing the image to a registry,
        # you need to set the image_pull_policy to Never.
        image_pull_policy="Never",
        ui_reverse_proxy=True,
        driver_resources=PodResources(cpu=1, memory=512, memory_overhead=128),
        executor_resources=PodResources(cpu=1, memory=512, memory_overhead=128),
        # Run with 5 executors
        executor_instances=ExecutorInstances(initial=5),
    )
