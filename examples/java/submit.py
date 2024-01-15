from __future__ import annotations

import logging

from spark_on_k8s.client.generic import SparkOnK8S

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark_client = SparkOnK8S()
    spark_client.submit_job(
        image="java-spark-job",
        app_path="local:///java-job.jar",
        app_arguments=["100000"],
        app_name="spark-java-job-example",
        namespace="spark",
        service_account="spark",
        job_waiter="print",
        class_name="com.oss_tech.examples.TestJob",
        # If you test this locally (minikube or kind) without pushing the image to a registry,
        # you need to set the image_pull_policy to Never.
        image_pull_policy="Never",
    )
