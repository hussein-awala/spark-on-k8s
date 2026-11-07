from __future__ import annotations

from datetime import datetime  # noqa: TC003 - Pydantic resolves this type at runtime.
from typing import Literal

from pydantic import BaseModel

from spark_on_k8s.utils.spark_app_status import SparkAppStatus  # noqa: TC001 - Runtime model field.


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


class ConsoleApplicationsPage(BaseModel):
    items: list[ConsoleApplication]
    next_cursor: str | None = None


class ConsoleHistory(BaseModel):
    items: list[ConsoleApplication]
