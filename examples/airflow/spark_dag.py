from __future__ import annotations

from datetime import datetime

from spark_on_k8s.airflow.operators import SparkOnK8SOperator
from spark_on_k8s.client import ExecutorInstances, PodResources

from airflow.models.dag import DAG

with DAG(
    dag_id="spark_on_k8s",
    schedule=None,
    start_date=datetime(2024, 1, 1),
) as dag:
    """
    This DAG submits a Spark application to Kubernetes using the SparkOnK8SOperator.
    """

    SparkOnK8SOperator(
        task_id="spark_application",
        namespace="spark",
        image="pyspark-job",
        image_pull_policy="Never",
        app_path="local:///opt/spark/work-dir/job.py",
        app_arguments=["100000"],
        app_name="pyspark-job-example",
        service_account="spark",
        app_waiter="log",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_instances=ExecutorInstances(min=0, max=5, initial=5),
        ui_reverse_proxy=True,
    )
