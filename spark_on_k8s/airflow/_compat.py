from __future__ import annotations

from airflow import __version__ as airflow_version

AIRFLOW_V_3_0_PLUS = int(airflow_version.split(".", 1)[0]) >= 3

__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "BaseOperator",
    "BaseOperatorLink",
    "Context",
    "DAG",
    "XCom",
    "get_current_context",
]

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, BaseOperator, BaseOperatorLink, Context, get_current_context
    from airflow.sdk.execution_time.xcom import XCom
else:
    from airflow.models import DAG, BaseOperator, BaseOperatorLink, XCom
    from airflow.operators.python import get_current_context
    from airflow.utils.context import Context
