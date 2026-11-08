from __future__ import annotations

from datetime import datetime  # noqa: TC003 - Pydantic resolves this type at runtime.
from typing import Literal
from uuid import UUID  # noqa: TC003 - Pydantic resolves this type at runtime.

from pydantic import BaseModel, Field

from spark_on_k8s.diagnostics import Diagnosis  # noqa: TC001 - Runtime model field.
from spark_on_k8s.utils.spark_app_status import SparkAppStatus  # noqa: TC001 - Runtime model field.

AIRFLOW_TASK_ANNOTATION = "spark-on-k8s.io/airflow-task"


class AirflowTaskContext(BaseModel):
    """Non-secret task identity recorded on an operator-created driver pod."""

    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    task_id: str = Field(min_length=1, max_length=250)
    map_index: int = Field(default=-1, ge=-1, strict=True)
    try_number: int = Field(ge=1, strict=True)
    region_id: UUID | None = None
    region_index: int | None = Field(default=None, ge=-1, strict=True)

    def owns_same_task(self, other: AirflowTaskContext) -> bool:
        # An Airflow retry may legitimately adopt its previous attempt's running pod.
        # Region identity disambiguates dynamic mappings that share a public map index.
        return (
            self.dag_id == other.dag_id
            and self.run_id == other.run_id
            and self.task_id == other.task_id
            and self.map_index == other.map_index
            and (self.region_id or UUID(int=0)) == (other.region_id or UUID(int=0))
            and (self.region_index if self.region_index is not None else self.map_index)
            == (other.region_index if other.region_index is not None else other.map_index)
        )


class ConsoleApplication(BaseModel):
    """Display metadata shared by the standalone console and embedded clients."""

    app_id: str
    name: str
    namespace: str | None = None
    status: SparkAppStatus
    created_at: datetime | None = None
    driver_pod: str | None = None
    image: str | None = None
    spark_version: str | None = None
    node_name: str | None = None
    driver_logs: bool = False
    spark_ui_proxy: bool = False
    spark_history_proxy: bool = False
    source: Literal["kubernetes", "history"]
    task_context: AirflowTaskContext | None = None


class ConsoleApplicationsPage(BaseModel):
    items: list[ConsoleApplication]
    next_cursor: str | None = None


class ConsoleHistory(BaseModel):
    items: list[ConsoleApplication]


class ConsoleDiagnostics(BaseModel):
    """Diagnosis and ownership metadata from the same inspected driver."""

    application: ConsoleApplication
    diagnosis: Diagnosis
