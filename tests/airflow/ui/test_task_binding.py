from __future__ import annotations

import json
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from types import SimpleNamespace
from unittest import mock
from uuid import UUID

import pytest

try:
    SUPPORTED = tuple(int(x) for x in version("apache-airflow").split(".")[:2]) >= (3, 4)
except PackageNotFoundError:
    SUPPORTED = False
pytestmark = pytest.mark.skipif(not SUPPORTED, reason="Native task UI requires Airflow 3.4")


@pytest.fixture
def database(monkeypatch):
    from airflow.api_fastapi.core_api.services.public import task_coordinates
    from airflow.models import task_coordinates as model_coordinates
    from airflow.utils import session as sessions
    from spark_on_k8s.airflow.ui.task_binding import TaskReference

    reference = TaskReference(
        dag_id="dag", run_id="manual__date/with:punctuation", task_id="spark", try_number=3, map_index=7
    )
    task = SimpleNamespace(
        id=UUID(int=123),
        dag_id="dag",
        run_id=reference.run_id,
        task_id="spark",
        region_id=UUID(int=55),
        region_index=2,
        operator="SparkOnK8SOperator",
        try_number=3,
    )
    value = {
        "version": 1,
        "namespace": "spark",
        "driver_pod": "app-driver",
        "app_id": "app",
        "try_number": 3,
        "task_context": {"dag_id": "spoofed", "run_id": "x", "task_id": "y", "try_number": 3},
    }
    queries = []
    row = SimpleNamespace(value=value)
    session = mock.Mock()

    def scalar(statement):
        queries.append(statement)
        return task if len(queries) == 1 else row

    session.scalar.side_effect = scalar

    @contextmanager
    def create_session():
        yield session

    resolver = mock.Mock(public_map_index=mock.Mock(return_value=7))
    monkeypatch.setattr(sessions, "create_session", create_session)
    monkeypatch.setattr(model_coordinates, "TaskCoordinateResolver", mock.Mock(return_value=resolver))
    resolve = mock.Mock(return_value=SimpleNamespace(region_id=UUID(int=55), region_index=2))
    monkeypatch.setattr(task_coordinates, "resolve_task_scope", resolve)
    return SimpleNamespace(
        reference=reference, task=task, row=row, queries=queries, resolver=resolver, resolve=resolve
    )


def test_real_xcom_query_binds_run_and_exact_producer_and_overrides_editable_identity(database):
    from spark_on_k8s.airflow.ui.task_binding import load_task_binding

    binding = load_task_binding(database.reference, mock.sentinel.dag_bag)
    assert len(database.queries) == 2
    task_query, xcom_query = [statement.compile() for statement in database.queries]
    assert database.reference.run_id in task_query.params.values()
    assert UUID(int=55) in task_query.params.values()
    assert 2 in task_query.params.values()
    # Execute the real Airflow get_many query builder, including its required run_id argument.
    assert database.task.id in xcom_query.params.values()
    assert "spark_on_k8s.console.v1" in xcom_query.params.values()
    assert binding.task_context.dag_id == "dag"
    assert binding.task_context.map_index == 7
    assert binding.task_context.region_id == UUID(int=55)
    assert binding.task_context.region_index == 2
    assert "task_context" not in binding.model_dump()
    database.resolve.assert_called_once_with(
        dag_id="dag",
        run_id=database.reference.run_id,
        task_id="spark",
        map_index=7,
        region_id=None,
        region_index=None,
        resolver=database.resolver,
    )


def test_json_string_xcom_works_without_custom_deserialization(database):
    from spark_on_k8s.airflow.ui.task_binding import load_task_binding

    database.row.value = json.dumps(database.row.value)
    assert load_task_binding(database.reference, None).app_id == "app"


@pytest.mark.parametrize(
    "change,code",
    [
        ("task_attempt", "stale_attempt"),
        ("binding_attempt", "stale_binding"),
        ("operator", "task_not_found"),
        ("invalid_binding", "invalid_binding"),
    ],
)
def test_non_current_or_invalid_task_binding_is_rejected(database, change, code):
    from fastapi import HTTPException
    from spark_on_k8s.airflow.ui.task_binding import load_task_binding

    if change == "task_attempt":
        database.task.try_number = 4
    elif change == "binding_attempt":
        database.row.value["try_number"] = 2
    elif change == "operator":
        database.task.operator = "PythonOperator"
    else:
        database.row.value = "remote://not-an-inline-record"
    with pytest.raises(HTTPException) as error:
        load_task_binding(database.reference, None)
    assert error.value.detail["code"] == code
    if change in {"task_attempt", "operator"}:
        assert len(database.queries) == 1
