from __future__ import annotations

from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


class TestSparkOnK8SOperatorLink:
    def test_persist_spark_ui_link(self):
        from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink

        mock_spark_on_k8s_operator = mock.MagicMock()
        context = {"ti": mock.Mock()}
        SparkOnK8SOperatorLink.persist_spark_ui_link(
            context=context,
            task_instance=mock_spark_on_k8s_operator,
            spark_on_k8s_service_url="http://localhost:8000",
            namespace="spark",
            spark_app_id="spark-app-id",
        )

        context["ti"].xcom_push.assert_called_once_with(
            key="spark_ui_link",
            value="http://localhost:8000/webserver/ui/spark/spark-app-id",
        )

    def test_persist_spark_history_ui_link(self):
        from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink

        mock_spark_on_k8s_operator = mock.MagicMock()
        context = {"ti": mock.Mock()}
        SparkOnK8SOperatorLink.persist_spark_history_ui_link(
            context=context,
            task_instance=mock_spark_on_k8s_operator,
            spark_on_k8s_service_url="http://localhost:8000",
            spark_app_id="spark-app-id",
        )

        context["ti"].xcom_push.assert_called_once_with(
            key="spark_ui_link",
            value="http://localhost:8000/webserver/ui-history/history/spark-app-id",
        )

    @mock.patch("spark_on_k8s.airflow.operator_links.XCom.get_value")
    def test_get_link(self, mock_get_value):
        from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink

        mock_link = "http://localhost:8000/webserver/ui/spark/spark-app-id"
        mock_get_value.return_value = mock_link
        mock_spark_on_k8s_operator = mock.MagicMock()
        mock_ti_key = mock.MagicMock()
        spark_on_k8s_operator_link = SparkOnK8SOperatorLink()
        link = spark_on_k8s_operator_link.get_link(operator=mock_spark_on_k8s_operator, ti_key=mock_ti_key)

        assert link == mock_link


@pytest.mark.parametrize("history", [False, True])
def test_running_link_matches_the_serialized_lookup_key(history):
    from spark_on_k8s.airflow._compat import AIRFLOW_V_3_0_PLUS
    from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    from airflow.serialization.serialized_objects import BaseSerialization

    operator = SparkOnK8SOperator(task_id="spark_link", image="spark:test", app_path="local:///job.py")
    stored_xcoms = {}
    ti = mock.Mock()
    ti.xcom_push.side_effect = lambda *, key, value: stored_xcoms.update({key: value})
    arguments = {
        "context": {"ti": ti},
        "task_instance": operator,
        "spark_on_k8s_service_url": "https://spark.example",
        "spark_app_id": "spark-job",
    }
    if history:
        SparkOnK8SOperatorLink.persist_spark_history_ui_link(**arguments)
        expected = "https://spark.example/webserver/ui-history/history/spark-job"
    else:
        SparkOnK8SOperatorLink.persist_spark_ui_link(namespace="spark", **arguments)
        expected = "https://spark.example/webserver/ui/spark/spark-job"

    if AIRFLOW_V_3_0_PLUS:
        # Airflow 3 looks up the serialized XCom key before task finalization.
        serialized = BaseSerialization.serialize(operator)
        link_key = serialized["__var"]["_operator_extra_links"]["Spark Job UI"]
        assert stored_xcoms[link_key] == expected
    else:
        # Airflow 2 calls the custom link implementation directly.
        with mock.patch(
            "spark_on_k8s.airflow.operator_links.XCom.get_value",
            side_effect=lambda *, ti_key, key: stored_xcoms.get(key),
        ):
            assert SparkOnK8SOperatorLink().get_link(operator, ti_key=mock.Mock()) == expected
