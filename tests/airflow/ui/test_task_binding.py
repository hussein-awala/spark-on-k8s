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

    resolver = mock.Mock(public_map_index=mock.Mock(return_value=7), find_task=mock.Mock(return_value=None))
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


@pytest.fixture(scope="module")
def serialized_tasks():
    """Operators as the Airflow API server sees them: deserialized from a serialized Dag."""
    from airflow.sdk import DAG, BaseOperator, BaseOperatorLink
    from airflow.serialization.serialized_objects import DagSerialization
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator, SparkSqlOnK8SOperator

    class LookalikeLink(BaseOperatorLink):
        name = "Spark Job UI"
        xcom_key = "another_link"

        def get_link(self, operator, *, ti_key):
            return ""

    class TeamLink(BaseOperatorLink):
        name = "Team dashboard"
        xcom_key = "team_dashboard"

        def get_link(self, operator, *, ti_key):
            return ""

    class TeamSparkOperator(SparkOnK8SOperator):
        pass

    class TeamSqlOperator(SparkSqlOnK8SOperator):
        pass

    class NestedTeamOperator(TeamSparkOperator):
        operator_extra_links = (*TeamSparkOperator.operator_extra_links, TeamLink())

    class LinklessSparkOperator(SparkOnK8SOperator):
        operator_extra_links = ()

    class LookalikeOperator(BaseOperator):
        operator_extra_links = (LookalikeLink(),)

    with DAG("dag", schedule=None) as dag:
        TeamSparkOperator(task_id="subclass", image="spark", app_path="local:///app.py")
        TeamSqlOperator(task_id="sql_subclass", image="spark", sql="SELECT 1")
        NestedTeamOperator(task_id="nested_subclass", image="spark", app_path="local:///app.py")
        TeamSparkOperator.partial(task_id="mapped_subclass", image="spark").expand(
            app_path=["local:///a.py", "local:///b.py"]
        )
        LinklessSparkOperator(task_id="linkless_subclass", image="spark", app_path="local:///app.py")
        LookalikeOperator(task_id="lookalike")
        BaseOperator(task_id="unrelated")
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    return {task.task_id: task for task in restored.tasks}


@pytest.mark.parametrize(
    "task_id,accepted",
    [
        ("subclass", True),
        ("sql_subclass", True),
        ("nested_subclass", True),
        ("mapped_subclass", True),
        # A subclass that drops the provider's inherited link no longer declares the lineage.
        ("linkless_subclass", False),
        ("lookalike", False),
        ("unrelated", False),
    ],
)
def test_serialized_operator_lineage_decides_subclass_binding(database, serialized_tasks, task_id, accepted):
    from fastapi import HTTPException
    from spark_on_k8s.airflow.ui.task_binding import load_task_binding

    operator = serialized_tasks[task_id]
    # Airflow records only the concrete class name on the task instance.
    database.task.operator = operator.task_type
    database.resolver.find_task.return_value = operator
    if accepted:
        assert load_task_binding(database.reference, None).app_id == "app"
        assert len(database.queries) == 2
    else:
        # An operator outside the provider's lineage is rejected before reading its XCom,
        # even though this task has a well-formed binding record.
        with pytest.raises(HTTPException) as error:
            load_task_binding(database.reference, None)
        assert error.value.detail["code"] == "task_not_found"
        assert len(database.queries) == 1
    database.resolver.find_task.assert_called_once_with(database.task)


@pytest.mark.parametrize("operator", ["SparkOnK8SOperator", "SparkSqlOnK8SOperator"])
def test_provider_operators_bind_without_loading_the_serialized_dag(database, operator):
    from spark_on_k8s.airflow.ui.task_binding import load_task_binding

    database.task.operator = operator
    assert load_task_binding(database.reference, None).app_id == "app"
    database.resolver.find_task.assert_not_called()


def test_task_missing_from_its_serialized_dag_is_not_bound(database):
    from fastapi import HTTPException
    from spark_on_k8s.airflow.ui.task_binding import load_task_binding

    database.task.operator = "TeamSparkOperator"
    with pytest.raises(HTTPException) as error:
        load_task_binding(database.reference, None)
    assert error.value.detail["code"] == "task_not_found"
    assert len(database.queries) == 1
