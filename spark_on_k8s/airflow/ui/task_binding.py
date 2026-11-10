from __future__ import annotations

import json
from typing import Annotated, Literal
from uuid import UUID  # noqa: TC003 -- Pydantic and FastAPI resolve runtime annotations.

from fastapi import HTTPException, Query
from pydantic import BaseModel, Field, ValidationError

from spark_on_k8s.airflow.ui_config import CONSOLE_XCOM_KEY, PROVIDER_OPERATORS
from spark_on_k8s.utils.console_models import AirflowTaskContext

KUBERNETES_NAME = r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"


class TaskReference(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1
    try_number: int
    region_id: UUID | None = None
    region_index: int | None = None


class TaskBinding(BaseModel):
    task_context: AirflowTaskContext | None = Field(default=None, exclude=True)
    version: Literal[1]
    namespace: str = Field(min_length=1, max_length=63, pattern=KUBERNETES_NAME)
    driver_pod: str = Field(min_length=1, max_length=63, pattern=KUBERNETES_NAME)
    try_number: int = Field(ge=1, strict=True)
    service_url: str | None = None
    app_id: str | None = Field(default=None, min_length=1, max_length=63, pattern=KUBERNETES_NAME)


def task_reference(
    dag_id: Annotated[str, Query(min_length=1, max_length=250)],
    run_id: Annotated[str, Query(min_length=1, max_length=250)],
    task_id: Annotated[str, Query(min_length=1, max_length=250)],
    try_number: Annotated[int, Query(ge=1)],
    map_index: Annotated[int, Query(ge=-1)] = -1,
    region_id: UUID | None = None,
    region_index: Annotated[int | None, Query(ge=-1)] = None,
) -> TaskReference:
    return TaskReference(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        map_index=map_index,
        region_id=region_id,
        region_index=region_index,
    )


def failure(code: str, message: str, status: int = 404) -> HTTPException:
    return HTTPException(status, detail={"code": code, "message": message})


def runs_provider_operator(task, resolver) -> bool:
    """Whether the task's operator is a provider operator or inherits from one."""
    if task.operator in PROVIDER_OPERATORS:
        return True
    from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink

    # Airflow serializes an operator's class name, not its base classes, and does not keep
    # custom class attributes on mapped tasks. Every provider operator declares the Spark job
    # link; subclasses inherit it and Airflow serializes it for plain and mapped tasks alike.
    operator = resolver.find_task(task)
    return any(
        getattr(link, "name", None) == SparkOnK8SOperatorLink.name
        and getattr(link, "xcom_key", None) == SparkOnK8SOperatorLink.xcom_key
        for link in getattr(operator, "operator_extra_links", None) or ()
    )


def load_task_binding(reference: TaskReference, dag_bag) -> TaskBinding:
    """Read only the current, authorized task attempt's small JSON binding."""
    from airflow.api_fastapi.core_api.services.public.task_coordinates import resolve_task_scope
    from airflow.models.task_coordinates import TaskCoordinateResolver
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.xcom import XComModel
    from airflow.utils.session import create_session
    from sqlalchemy import select

    with create_session() as session:
        resolver = TaskCoordinateResolver(dag_bag, session)
        scope = resolve_task_scope(
            dag_id=reference.dag_id,
            run_id=reference.run_id,
            task_id=reference.task_id,
            map_index=reference.map_index,
            region_id=reference.region_id,
            region_index=reference.region_index,
            resolver=resolver,
        )
        task = session.scalar(
            select(TaskInstance).where(
                TaskInstance.dag_id == reference.dag_id,
                TaskInstance.run_id == reference.run_id,
                TaskInstance.task_id == reference.task_id,
                TaskInstance.region_id == scope.region_id,
                TaskInstance.region_index == scope.region_index,
            )
        )
        if task is None or not runs_provider_operator(task, resolver):
            raise failure("task_not_found", "This task has no Spark application binding.")
        if task.try_number != reference.try_number:
            raise failure(
                "stale_attempt", "Select the current task attempt before managing its Spark application.", 409
            )
        producer = select(TaskInstance.id).where(TaskInstance.id == task.id)
        row = session.scalar(
            XComModel.get_many(run_id=reference.run_id, key=CONSOLE_XCOM_KEY, producer_ids=producer)
        )
        if row is None:
            raise failure(
                "binding_not_found", "The Spark application has not been submitted by this task attempt."
            )
        try:
            # Do not invoke custom XCom deserializers or fetch arbitrary remote XCom objects.
            value = json.loads(row.value) if isinstance(row.value, str) else row.value
            binding = TaskBinding.model_validate(value)
        except (ValueError, TypeError, ValidationError) as error:
            raise failure(
                "invalid_binding", "The task's Spark application binding is unavailable."
            ) from error
        if binding.try_number != reference.try_number:
            raise failure("stale_binding", "The Spark binding belongs to an earlier task attempt.", 409)
        # Ownership comes from the authorized database task, never editable XCom data.
        binding.task_context = AirflowTaskContext(
            dag_id=task.dag_id,
            run_id=task.run_id,
            task_id=task.task_id,
            map_index=resolver.public_map_index(task),
            try_number=task.try_number,
            region_id=task.region_id,
            region_index=task.region_index,
        )
        return binding
