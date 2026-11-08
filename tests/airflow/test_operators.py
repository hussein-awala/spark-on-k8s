from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest
from spark_on_k8s.utils.spark_app_status import SparkAppStatus

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


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
            driver_env_vars_from_secrets=None,
            executor_pod_template=None,
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
            driver_env_vars_from_secrets=None,
            executor_pod_template=None,
            executor_pod_template_path="s3a://bucket/executor.yml",
        )
        assert ti_mock.xcom_push.call_count == 4
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
            driver_annotations={},
            executor_annotations=None,
            driver_tolerations=None,
            driver_env_vars_from_secrets=None,
            executor_pod_template=None,
            executor_pod_template_path=None,
            driver_ephemeral_configmaps_volumes=None,
        )
        assert app_id_suffix_kwarg() == "-suffix"

    @pytest.mark.parametrize(
        "job_status, should_submit",
        [
            (SparkAppStatus.Pending, False),
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
            mock_wait_for_app.assert_called_once_with(
                namespace="test-namespace",
                pod_name="existing-pod",
                poll_interval=10,
                startup_timeout=0,
            )

    @pytest.mark.parametrize("status", [401, 403, 429, 500, 503, None])
    @pytest.mark.parametrize("map_index", [-1, 3])
    def test_adoption_lookup_error_never_submits(self, status, map_index):
        from airflow.exceptions import AirflowException
        from kubernetes.client import ApiException
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        task = SparkOnK8SOperator(task_id="spark", image="spark:test", app_path="local:///job.py")
        ti = mock.Mock(dag_id="dag", task_id="spark", map_index=map_index)
        ti.xcom_pull.side_effect = ["default", "existing-driver"]
        error = ApiException(status=status) if status is not None else ConnectionError("unreachable")
        with (
            mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.app_status", side_effect=error),
            mock.patch.object(task, "_submit_new_job") as submit,
            pytest.raises(AirflowException, match="refusing to submit another application") as caught,
        ):
            task.execute({"ti": ti})
        assert caught.value.__cause__ is error
        submit.assert_not_called()
        assert all(call.kwargs["map_indexes"] == map_index for call in ti.xcom_pull.call_args_list)
        ti.xcom_push.assert_not_called()

    def test_adoption_confirmed_absence_submits(self):
        from kubernetes.client import ApiException
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        task = SparkOnK8SOperator(
            task_id="spark", image="spark:test", app_path="local:///job.py", app_waiter="no_wait"
        )
        ti = mock.Mock(xcom_pull=mock.Mock(side_effect=["default", "missing-driver"]))
        with (
            mock.patch(
                "spark_on_k8s.utils.app_manager.SparkAppManager.app_status",
                side_effect=ApiException(status=404),
            ),
            mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="new-driver") as submit,
        ):
            task.execute({"ti": ti})
        submit.assert_called_once()
        ti.xcom_push.assert_any_call(key="driver_pod_name", value="new-driver")

    def test_adoption_unknown_status_never_submits(self):
        from airflow.exceptions import AirflowException
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        task = SparkOnK8SOperator(task_id="spark", image="spark:test", app_path="local:///job.py")
        ti = mock.Mock(xcom_pull=mock.Mock(side_effect=["default", "existing-driver"]))
        with (
            mock.patch(
                "spark_on_k8s.utils.app_manager.SparkAppManager.app_status",
                return_value=SparkAppStatus.Unknown,
            ),
            mock.patch.object(task, "_submit_new_job") as submit,
            pytest.raises(AirflowException, match="indeterminate status Unknown"),
        ):
            task.execute({"ti": ti})
        submit.assert_not_called()

    def test_package_and_pod_metadata_templates_preserve_task_identity(self):
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator
        from spark_on_k8s.utils.console_models import AIRFLOW_TASK_ANNOTATION

        packages = ["org.example:job:{{ version }}"]
        labels = {"dataset": "{{ dataset }}"}
        annotations = {"source": "{{ dataset }}", AIRFLOW_TASK_ANNOTATION: "forged"}
        for dataset in ("orders", "customers"):
            task = SparkOnK8SOperator(
                task_id="spark",
                image="spark:test",
                app_path="local:///job.py",
                app_waiter="no_wait",
                packages=packages,
                driver_labels=labels,
                executor_labels=labels,
                driver_annotations=annotations,
                executor_annotations={"source": "{{ dataset }}"},
            )
            task.render_template_fields({"version": "1.0", "dataset": dataset})
            ti = mock.Mock(
                dag_id="dag",
                task_id="spark",
                run_id="run",
                map_index=2,
                try_number=1,
                region_id=None,
                region_index=None,
                xcom_pull=mock.Mock(return_value=None),
            )
            with mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="driver") as submit:
                task.execute({"ti": ti})
            values = submit.call_args.kwargs
            assert values["packages"] == ["org.example:job:1.0"]
            assert values["driver_labels"] == values["executor_labels"] == {"dataset": dataset}
            assert values["driver_annotations"]["source"] == dataset
            assert values["executor_annotations"] == {"source": dataset}
            assert json.loads(values["driver_annotations"][AIRFLOW_TASK_ANNOTATION])["map_index"] == 2
        assert packages == ["org.example:job:{{ version }}"]
        assert labels == {"dataset": "{{ dataset }}"}
        assert annotations[AIRFLOW_TASK_ANNOTATION] == "forged"

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


class TestSparkSqlOnK8SOperator:
    @mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app")
    def test_rendering_templates(self, mock_submit_app):
        from spark_on_k8s import sql as sql_package
        from spark_on_k8s.airflow.operators import SparkSqlOnK8SOperator

        spark_app_task = SparkSqlOnK8SOperator(
            task_id="spark_sql_application",
            image="{{ template_image }}",
            sql="SELECT * FROM {{ template_table }}",
            app_waiter="no_wait",
        )
        spark_app_task.render_template_fields(
            context={
                "template_image": "pyspark-job",
                "template_table": "test_table",
            },
        )
        spark_app_task.execute(
            {
                "ti": mock.MagicMock(
                    xcom_pull=mock.MagicMock(return_value=None),
                )
            }
        )
        mock_submit_app.assert_called_once_with(
            image="pyspark-job",
            app_path="/configmap/spark_sql.py",
            driver_ephemeral_configmaps_volumes=[
                {
                    "mount_path": "/configmap",
                    "sources": [
                        {
                            "name": "spark_sql.py",
                            "text_path": str(Path(sql_package.__file__).with_name("runner.py")),
                        },
                        {
                            "name": "queries.json",
                            "text": json.dumps(["SELECT * FROM test_table"]),
                        },
                        {
                            "name": "locations.json",
                            "text": json.dumps(
                                {
                                    "schema_version": 1,
                                    "source": "<inline>",
                                    "statements": [{"statement": 1, "line": 1}],
                                }
                            ),
                        },
                    ],
                }
            ],
            namespace="default",
            service_account="spark",
            app_arguments=["/configmap/queries.json"],
            app_waiter="no_wait",
            image_pull_policy="IfNotPresent",
            app_name=None,
            spark_conf=None,
            class_name=None,
            packages=None,
            ui_reverse_proxy=False,
            driver_resources=None,
            executor_resources=None,
            executor_instances=None,
            secret_values=None,
            volumes=None,
            driver_volume_mounts=None,
            executor_volume_mounts=None,
            driver_node_selector=None,
            executor_node_selector=None,
            driver_labels=None,
            executor_labels=None,
            driver_annotations={},
            executor_annotations=None,
            driver_tolerations=None,
            driver_env_vars_from_secrets=None,
            executor_pod_template=None,
            executor_pod_template_path=None,
        )


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 'a;b'; SELECT 2;", ["SELECT 'a;b'", "SELECT 2"]),
        ('SELECT "a;b"; SELECT `c;d` FROM t;', ['SELECT "a;b"', "SELECT `c;d` FROM t"]),
        ("SELECT 'it''s;a'; SELECT 2;", ["SELECT 'it''s;a'", "SELECT 2"]),
        (
            "-- heading; note\nSELECT 1;\n/* block; note */ SELECT 2;",
            ["-- heading; note\nSELECT 1", "/* block; note */ SELECT 2"],
        ),
        ("/* only; comments */ -- more; comments\n", []),
        ("; ; SELECT 1; ;", ["SELECT 1"]),
    ],
)
@mock.patch("spark_on_k8s.airflow.operators.SparkOnK8SOperator.execute")
def test_sql_statement_boundaries_are_preserved(mock_execute, sql, expected):
    from spark_on_k8s.airflow.operators import SparkSqlOnK8SOperator

    task = SparkSqlOnK8SOperator(task_id="sql_boundaries", image="pyspark-job", sql=sql)
    task.render_template_fields(context={})
    task.execute({})

    sources = task.driver_ephemeral_configmaps_volumes[0]["sources"]
    statements_source = next(source for source in sources if source["name"] == "queries.json")
    assert json.loads(statements_source["text"]) == expected
    assert task.app_arguments == ["/configmap/queries.json"]
    mock_execute.assert_called_once_with({})


@mock.patch("spark_on_k8s.airflow.operators.SparkOnK8SOperator.execute")
def test_sql_file_is_rendered_before_splitting(mock_execute, tmp_path):
    from spark_on_k8s.airflow._compat import DAG
    from spark_on_k8s.airflow.operators import SparkSqlOnK8SOperator

    (tmp_path / "queries.sql").write_text("SELECT '{{ value }}'; SELECT 2;")
    dag = DAG(dag_id="sql_file", schedule=None, template_searchpath=[str(tmp_path)])
    task = SparkSqlOnK8SOperator(task_id="sql_file", image="pyspark-job", sql="queries.sql", dag=dag)
    task.render_template_fields(context={"value": "a;b"})
    task.execute({})

    sources = task.driver_ephemeral_configmaps_volumes[0]["sources"]
    statements_source = next(source for source in sources if source["name"] == "queries.json")
    assert json.loads(statements_source["text"]) == ["SELECT 'a;b'", "SELECT 2"]


def test_sql_operator_keeps_user_configmaps_beside_generated_queries():
    from spark_on_k8s.airflow.operators import SparkSqlOnK8SOperator

    # The SDK and CLI SQL paths append the generated statements; the operator must not drop
    # user-supplied driver ConfigMaps (for example, logging configuration).
    logging_configmap = {
        "name": "logging",
        "mount_path": "/opt/spark/conf/logging",
        "sources": [{"name": "log4j2.properties", "text": "rootLogger.level = warn"}],
    }
    supplied = [logging_configmap]
    task = SparkSqlOnK8SOperator(
        task_id="sql_with_configmaps",
        image="pyspark-job",
        sql="SELECT 1",
        app_waiter="no_wait",
        driver_ephemeral_configmaps_volumes=supplied,
    )
    task.render_template_fields(context={})
    with mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="sql-driver") as submit:
        task.execute({"ti": mock.MagicMock(xcom_pull=mock.MagicMock(return_value=None))})

    configmaps = submit.call_args.kwargs["driver_ephemeral_configmaps_volumes"]
    assert [configmap["mount_path"] for configmap in configmaps] == ["/opt/spark/conf/logging", "/configmap"]
    assert configmaps[0] == logging_configmap
    statements = next(source for source in configmaps[1]["sources"] if source["name"] == "queries.json")
    assert json.loads(statements["text"]) == ["SELECT 1"]
    assert supplied == [logging_configmap]


@pytest.mark.parametrize("mount_path", ["/configmap", "configmap/", "/configmap/extra", "/tmp/../configmap"])
def test_sql_operator_rejects_user_configmaps_on_the_reserved_mount(mount_path):
    from spark_on_k8s.airflow.operators import SparkSqlOnK8SOperator

    task = SparkSqlOnK8SOperator(
        task_id="sql_reserved_mount",
        image="pyspark-job",
        sql="SELECT 1",
        driver_ephemeral_configmaps_volumes=[
            {"name": "conflict", "mount_path": mount_path, "sources": [{"name": "a.txt", "text": "a"}]}
        ],
    )
    with (
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app") as submit,
        pytest.raises(ValueError, match="reserves the /configmap"),
    ):
        task.execute({"ti": mock.MagicMock(xcom_pull=mock.MagicMock(return_value=None))})
    submit.assert_not_called()


def test_job_spec_reuses_configuration_and_secret_references():
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator
    from spark_on_k8s.jobs import JobSpec

    job = JobSpec.from_mapping(
        {
            "apiVersion": "spark-on-k8s/v1",
            "kind": "SparkJob",
            "metadata": {"name": "portable"},
            "spec": {
                "image": "spark:4.0.5",
                "app_path": "local:///job.py",
                "driver_env_vars_from_secrets": ["existing-secret"],
                "executor_pod_template": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "spec": {"containers": [{"name": "executor"}]},
                },
            },
        }
    )
    operator = SparkOnK8SOperator(task_id="portable", **job.to_operator_kwargs())
    with mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="portable-driver") as submit:
        operator.execute({"ti": mock.MagicMock(xcom_pull=mock.MagicMock(return_value=None))})
    kwargs = submit.call_args.kwargs
    assert kwargs["driver_env_vars_from_secrets"] == ["existing-secret"]
    assert "kind: Pod" in kwargs["executor_pod_template"]
    assert kwargs["secret_values"] == {}
    assert kwargs["executor_instances"].initial == 2
