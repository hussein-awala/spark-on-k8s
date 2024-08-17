from __future__ import annotations

from unittest import mock

import pytest
from spark_on_k8s.utils.spark_app_status import SparkAppStatus

from conftest import PYTHON_312


@pytest.mark.skipif(PYTHON_312, reason="Python 3.12 is not supported by Airflow")
class TestSparkOnK8SOperator:
    @mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app")
    def test_execute(self, mock_submit_app):
        import kubernetes.client as k8s
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator
        from spark_on_k8s.client import ExecutorInstances, PodResources

        test_tolerations = [
            k8s.V1Toleration(key="key1", operator="Equal", value="value1", effect="NoSchedule"),
            k8s.V1Toleration(key="key2", operator="Equal", value="value2", effect="NoSchedule"),
        ]

        mock_submit_app.return_value = "test-app-id-driver"

        spark_app_task = SparkOnK8SOperator(
            task_id="spark_application",
            namespace="spark",
            image="pyspark-job",
            image_pull_policy="Never",
            app_path="local:///opt/spark/work-dir/job.py",
            app_arguments=["100000"],
            app_name="pyspark-job-example",
            service_account="spark",
            packages=["some-package"],
            app_waiter="no_wait",
            driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
            executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
            executor_instances=ExecutorInstances(min=0, max=5, initial=5),
            ui_reverse_proxy=True,
            driver_node_selector={"node-type": "driver"},
            executor_node_selector={"node-type": "executor"},
            driver_labels={"label1": "value1"},
            executor_labels={"label2": "value2"},
            driver_annotations={"annotation1": "value1"},
            executor_annotations={"annotation2": "value2"},
            driver_tolerations=test_tolerations,
            driver_ephemeral_configmaps_volumes=[
                {
                    "name": "configmap-volume",
                    "mount_path": "/etc/config",
                    "sources": [
                        {"name": "file1.txt", "text": "config1"},
                        {"name": "file2.txt", "text_path": "/path/to/some/file.txt"},
                    ],
                },
            ],
            executor_pod_template_path="s3a://bucket/executor.yml",
            spark_on_k8s_service_url="http://localhost:8000",
        )
        ti_mock = mock.MagicMock(
            xcom_pull=mock.MagicMock(return_value=None),
        )
        spark_app_task.execute({"ti": ti_mock})
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
            packages=["some-package"],
            secret_values=None,
            volumes=None,
            driver_volume_mounts=None,
            executor_volume_mounts=None,
            driver_node_selector={"node-type": "driver"},
            executor_node_selector={"node-type": "executor"},
            driver_labels={"label1": "value1"},
            executor_labels={"label2": "value2"},
            driver_annotations={"annotation1": "value1"},
            executor_annotations={"annotation2": "value2"},
            driver_tolerations=test_tolerations,
            driver_ephemeral_configmaps_volumes=[
                {
                    "name": "configmap-volume",
                    "mount_path": "/etc/config",
                    "sources": [
                        {"name": "file1.txt", "text": "config1"},
                        {"name": "file2.txt", "text_path": "/path/to/some/file.txt"},
                    ],
                },
            ],
            executor_pod_template_path="s3a://bucket/executor.yml",
        )
        assert ti_mock.xcom_push.call_count == 3
        xcom_push_calls = ti_mock.xcom_push.mock_calls
        assert xcom_push_calls[0].kwargs == {
            "key": "driver_pod_namespace",
            "value": "spark",
        }
        assert xcom_push_calls[1].kwargs == {
            "key": "driver_pod_name",
            "value": "test-app-id-driver",
        }
        assert xcom_push_calls[2].kwargs == {
            "key": "spark_ui_link",
            "value": "http://localhost:8000/webserver/ui/spark/test-app-id",
            "execution_date": None,
        }

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
            app_id_suffix="-{{ template_app_id_suffix }}",
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
                "template_app_id_suffix": "suffix",
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
        spark_app_task.execute(
            {
                "ti": mock.MagicMock(
                    xcom_pull=mock.MagicMock(return_value=None),
                )
            }
        )
        app_id_suffix_kwarg = mock_submit_app.call_args.kwargs.get("app_id_suffix")
        mock_submit_app.assert_called_once_with(
            namespace="spark",
            image="pyspark-job",
            image_pull_policy="Never",
            app_path="local:///opt/spark/work-dir/job.py",
            app_arguments=["100000"],
            app_name="pyspark-job-example",
            app_id_suffix=app_id_suffix_kwarg,
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
            packages=None,
            volumes=None,
            driver_volume_mounts=None,
            executor_volume_mounts=None,
            driver_node_selector=None,
            executor_node_selector=None,
            driver_labels=None,
            executor_labels=None,
            driver_annotations=None,
            executor_annotations=None,
            driver_tolerations=None,
            executor_pod_template_path=None,
            driver_ephemeral_configmaps_volumes=None,
        )
        assert app_id_suffix_kwarg() == "-suffix"

    @pytest.mark.parametrize(
        "job_status, should_submit",
        [
            (SparkAppStatus.Running, False),
            (SparkAppStatus.Succeeded, True),
            (SparkAppStatus.Failed, True),
        ],
    )
    @mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.wait_for_app")
    @mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.app_status")
    @mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app")
    def test_job_adoption(
        self, mock_submit_app, mock_app_status, mock_wait_for_app, job_status, should_submit
    ):
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        mock_app_status.side_effect = [job_status, SparkAppStatus.Succeeded]
        spark_app_task = SparkOnK8SOperator(
            task_id="spark_application",
            namespace="test-namespace",
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
        )
        spark_app_task.execute(
            {
                "ti": mock.MagicMock(
                    xcom_pull=mock.MagicMock(side_effect=["test-namespace", "existing-pod"]),
                )
            }
        )
        if should_submit:
            mock_submit_app.assert_called_once()
        else:
            mock_submit_app.assert_not_called()

    @pytest.mark.parametrize(
        "app_waiter",
        [
            pytest.param("log", id="log"),
            pytest.param("wait", id="wait"),
        ],
    )
    @mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.stream_logs")
    @mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.wait_for_app")
    @mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.app_status")
    @mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app")
    def test_startup_timeout(
        self,
        mock_submit_app,
        mock_app_status,
        mock_wait_for_app,
        mock_stream_logs,
        app_waiter,
    ):
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        mock_app_status.return_value = SparkAppStatus.Succeeded
        mock_submit_app.return_value = "test-pod-name"
        spark_app_task = SparkOnK8SOperator(
            task_id="spark_application",
            namespace="test-namespace",
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            startup_timeout=10,
            app_waiter=app_waiter,
        )
        spark_app_task.execute(
            {
                "ti": mock.MagicMock(
                    xcom_pull=mock.MagicMock(side_effect=["test-namespace", "existing-pod"]),
                )
            }
        )
        if app_waiter == "log":
            mock_stream_logs.assert_called_once_with(
                namespace="test-namespace",
                pod_name="test-pod-name",
                startup_timeout=10,
            )
        else:
            mock_wait_for_app.assert_called_once_with(
                namespace="test-namespace",
                pod_name="test-pod-name",
                poll_interval=10,
                startup_timeout=10,
            )
