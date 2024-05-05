from __future__ import annotations

from unittest import mock

import pytest

from conftest import PYTHON_312


@pytest.mark.skipif(PYTHON_312, reason="Python 3.12 is not supported by Airflow")
class TestSparkOnK8SOperatorLink:
    def test_persist_spark_ui_link(self):
        from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink

        mock_spark_on_k8s_operator = mock.MagicMock()
        context = {}
        SparkOnK8SOperatorLink.persist_spark_ui_link(
            context=context,
            task_instance=mock_spark_on_k8s_operator,
            spark_on_k8s_service_url="http://localhost:8000",
            namespace="spark",
            spark_app_id="spark-app-id",
        )

        mock_spark_on_k8s_operator.xcom_push.assert_called_once_with(
            context,
            key="spark_ui_link",
            value="http://localhost:8000/webserver/ui/spark/spark-app-id",
        )

    def test_persist_spark_history_ui_link(self):
        from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink

        mock_spark_on_k8s_operator = mock.MagicMock()
        context = {}
        SparkOnK8SOperatorLink.persist_spark_history_ui_link(
            context=context,
            task_instance=mock_spark_on_k8s_operator,
            spark_on_k8s_service_url="http://localhost:8000",
            spark_app_id="spark-app-id",
        )

        mock_spark_on_k8s_operator.xcom_push.assert_called_once_with(
            context,
            key="spark_ui_link",
            value="http://localhost:8000/webserver/ui-history/history/spark-app-id",
        )

    @mock.patch("airflow.models.xcom.BaseXCom.get_value")
    def test_get_link(self, mock_get_value):
        from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink

        mock_link = "http://localhost:8000/webserver/ui/spark/spark-app-id"
        mock_get_value.return_value = mock_link
        mock_spark_on_k8s_operator = mock.MagicMock()
        mock_ti_key = mock.MagicMock()
        spark_on_k8s_operator_link = SparkOnK8SOperatorLink()
        link = spark_on_k8s_operator_link.get_link(operator=mock_spark_on_k8s_operator, ti_key=mock_ti_key)

        assert link == mock_link
