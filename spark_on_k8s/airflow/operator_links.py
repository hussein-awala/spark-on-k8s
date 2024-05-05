from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class SparkOnK8SOperatorLink(BaseOperatorLink):
    """
    Operator link for SparkOnK8SOperator.

    It allows users to access Spark job UI and spark history UI using SparkOnK8SOperator.
    """

    name = "Spark Job UI"

    @staticmethod
    def persist_spark_ui_link(
        context: Context,
        task_instance: BaseOperator,
        spark_on_k8s_service_url: str,
        namespace: str,
        spark_app_id: str,
    ):
        """
        Persist Spark UI link to XCom.
        """
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        spark_ui_link = f"{spark_on_k8s_service_url}/webserver/ui/{namespace}/{spark_app_id}"
        task_instance.xcom_push(
            context,
            key=SparkOnK8SOperator.XCOM_SPARK_UI_LINK,
            value=spark_ui_link,
        )

    @staticmethod
    def persist_spark_history_ui_link(
        context: Context, task_instance: BaseOperator, spark_on_k8s_service_url: str, spark_app_id: str
    ):
        """
        Persist Spark history UI link to XCom.
        """
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        spark_history_ui_link = f"{spark_on_k8s_service_url}/webserver/ui-history/history/{spark_app_id}"
        task_instance.xcom_push(
            context,
            key=SparkOnK8SOperator.XCOM_SPARK_UI_LINK,
            value=spark_history_ui_link,
        )

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        """
        Get link to Spark job UI or Spark history UI.
        """
        from spark_on_k8s.airflow.operators import SparkOnK8SOperator

        spark_ui_link = XCom.get_value(ti_key=ti_key, key=SparkOnK8SOperator.XCOM_SPARK_UI_LINK)
        if spark_ui_link:
            return spark_ui_link
        else:
            return ""
