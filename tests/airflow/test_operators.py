from __future__ import annotations

from unittest import mock

import pytest

from conftest import PYTHON_312


@pytest.mark.skipif(PYTHON_312, reason="Python 3.12 is not supported by Airflow")
class TestSparkOnK8SOperator:
    @mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app")
    def test_execute(self, mock_submit_app):
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator
        from spark_on_k8s.client import ExecutorInstances, PodResources

        spark_app_task = SparkOnK8SOperator(
            task_id="spark_application",
            namespace="spark",
            image="pyspark-job",
            image_pull_policy="Never",
            app_path="local:///opt/spark/work-dir/job.py",
            app_arguments=["100000"],
            app_name="pyspark-job-example",
            service_account="spark",
            app_waiter="no_wait",
            driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
            executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
            executor_instances=ExecutorInstances(min=0, max=5, initial=5),
            ui_reverse_proxy=True,
            driver_node_selector={"node-type": "driver"},
            executor_node_selector={"node-type": "executor"},
        )
        spark_app_task.execute(None)
        mock_submit_app.assert_called_once_with(
            namespace="spark",
            image="pyspark-job",
            image_pull_policy="Never",
            app_path="local:///opt/spark/work-dir/job.py",
            app_arguments=["100000"],
            app_name="pyspark-job-example",
            service_account="spark",
            app_waiter="no_wait",
            driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
            executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
            executor_instances=ExecutorInstances(min=0, max=5, initial=5),
            ui_reverse_proxy=True,
            spark_conf=None,
            class_name=None,
            secret_values=None,
            volumes=None,
            driver_volume_mounts=None,
            executor_volume_mounts=None,
            driver_node_selector={"node-type": "driver"},
            executor_node_selector={"node-type": "executor"},
        )

    @mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app")
    def test_rendering_templates(self, mock_submit_app):
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator
        from spark_on_k8s.client import ExecutorInstances, PodResources

        spark_app_task = SparkOnK8SOperator(
            task_id="spark_application",
            namespace="{{ template_namespace }}",
            image="{{ template_image }}",
            image_pull_policy="{{ template_image_pull_policy }}",
            app_path="{{ template_app_path }}",
            app_arguments=["{{ template_app_argument }}"],
            app_name="{{ template_app_name }}",
            service_account="{{ template_service_account }}",
            app_waiter="{{ template_app_waiter }}",
            driver_resources=PodResources(
                cpu="{{ template_driver_resources_cpu }}",
                memory="{{ template_driver_resources_memory }}",
                memory_overhead="{{ template_driver_resources_memory_overhead }}",
            ),
            executor_instances=ExecutorInstances(
                min="{{ template_executor_instances_min }}",
                max="{{ template_executor_instances_max }}",
                initial="{{ template_executor_instances_initial }}",
            ),
            ui_reverse_proxy=True,
            spark_conf={
                "spark.kubernetes.container.image": "{{ template_image }}",
                "spark.kubernetes.container.image.pullPolicy": "{{ template_image_pull_policy }}",
            },
            secret_values={
                "KEY1": "VALUE1",
                "KEY2": "{{ template_secret_value }}",
            },
        )
        spark_app_task.render_template_fields(
            context={
                "template_namespace": "spark",
                "template_image": "pyspark-job",
                "template_image_pull_policy": "Never",
                "template_app_path": "local:///opt/spark/work-dir/job.py",
                "template_app_argument": "100000",
                "template_app_name": "pyspark-job-example",
                "template_service_account": "spark",
                "template_app_waiter": "no_wait",
                "template_driver_resources_cpu": 1,
                "template_driver_resources_memory": 1024,
                "template_driver_resources_memory_overhead": 512,
                "template_executor_instances_min": 0,
                "template_executor_instances_max": 5,
                "template_executor_instances_initial": 5,
                "template_secret_value": "value from connection",
            },
        )
        spark_app_task.execute(None)
        mock_submit_app.assert_called_once_with(
            namespace="spark",
            image="pyspark-job",
            image_pull_policy="Never",
            app_path="local:///opt/spark/work-dir/job.py",
            app_arguments=["100000"],
            app_name="pyspark-job-example",
            service_account="spark",
            app_waiter="no_wait",
            driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
            executor_resources=None,
            executor_instances=ExecutorInstances(min=0, max=5, initial=5),
            ui_reverse_proxy=True,
            spark_conf={
                "spark.kubernetes.container.image": "pyspark-job",
                "spark.kubernetes.container.image.pullPolicy": "Never",
            },
            secret_values={
                "KEY1": "VALUE1",
                "KEY2": "value from connection",
            },
            class_name=None,
            volumes=None,
            driver_volume_mounts=None,
            executor_volume_mounts=None,
            driver_node_selector=None,
            executor_node_selector=None,
        )
