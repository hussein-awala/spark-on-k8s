from __future__ import annotations

from datetime import datetime

from spark_on_k8s.airflow.operators import SparkSqlOnK8SOperator
from spark_on_k8s.client import ExecutorInstances, PodResources

from airflow.models.dag import DAG

with DAG(
    dag_id="spark_sql_on_k8s",
    schedule=None,
    start_date=datetime(2024, 1, 1),
) as dag:
    """
    This DAG executes Spark SQL queries on Kubernetes using the SparkSqlOnK8SOperator.
    """

    SparkSqlOnK8SOperator(
        task_id="spark_sql_application",
        sql="CREATE TABLE test_table (key INT, value STRING) AS SELECT 1 AS key, 'value' AS value",
        namespace="spark",
        image="pyspark-job",
        image_pull_policy="Never",
        app_name="spark-sql-job-example",
        service_account="spark",
        app_waiter="log",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_instances=ExecutorInstances(min=0, max=5, initial=5),
        ui_reverse_proxy=True,
    )

    SparkSqlOnK8SOperator(
        task_id="spark_multiple_sql_application",
        sql="SELECT 1; SELECT 2; SELECT {{ ds }}",
        namespace="spark",
        image="pyspark-job",
        image_pull_policy="Never",
        app_name="spark-sql-job-example",
        service_account="spark",
        app_waiter="log",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_instances=ExecutorInstances(min=0, max=5, initial=5),
        ui_reverse_proxy=True,
    )

    SparkSqlOnK8SOperator(
        task_id="spark_sql_from_file_application",
        sql="query.sql",
        namespace="spark",
        image="pyspark-job",
        image_pull_policy="Never",
        app_name="spark-sql-job-example",
        service_account="spark",
        app_waiter="log",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_instances=ExecutorInstances(min=0, max=5, initial=5),
        ui_reverse_proxy=True,
    )

    SparkSqlOnK8SOperator(
        task_id="spark_iceberg_sql_application",
        sql="iceberg_query.sql",
        # it's better to use a custom image with the iceberg-spark-runtime package installed
        packages=["org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0"],
        spark_conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hive",
            "spark.sql.catalog.prod": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.prod.type": "rest",
            "spark.sql.catalog.prod.uri": "http://localhost:8080",
        },
        namespace="spark",
        image="pyspark-job",
        image_pull_policy="Never",
        app_name="spark-sql-job-example",
        service_account="spark",
        app_waiter="log",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_instances=ExecutorInstances(min=0, max=5, initial=5),
        ui_reverse_proxy=True,
    )
