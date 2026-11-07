from __future__ import annotations

import json
import runpy
import sys
from importlib.resources import files
from types import ModuleType
from unittest import mock

import pytest

SCRIPT = files("spark_on_k8s").joinpath("sql/runner.py")


@pytest.fixture
def spark_session(monkeypatch):
    session = mock.Mock()
    pyspark_sql = ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = mock.Mock()
    pyspark_sql.SparkSession.builder.getOrCreate.return_value = session
    monkeypatch.setitem(sys.modules, "pyspark", ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    return session


def test_successful_commands_execute_in_order(spark_session):
    execute = runpy.run_path(str(SCRIPT))["execute_sql_commands"]
    execute(spark_session, ["SELECT 1", "SELECT 2"])
    assert spark_session.sql.call_args_list == [mock.call("SELECT 1"), mock.call("SELECT 2")]
    assert spark_session.sql.return_value.show.call_count == 2


@pytest.mark.parametrize("failure_stage", ["sql", "show"])
def test_failed_query_propagates_and_stops_later_commands(spark_session, failure_stage):
    execute = runpy.run_path(str(SCRIPT))["execute_sql_commands"]
    failing_method = spark_session.sql if failure_stage == "sql" else spark_session.sql.return_value.show
    failing_method.side_effect = RuntimeError("query failed")
    with pytest.raises(RuntimeError, match="SQL statement 1 at <inline> failed"):
        execute(spark_session, ["SELECT broken", "INSERT INTO output SELECT 1"])
    spark_session.sql.assert_called_once_with("SELECT broken")


def test_script_stops_session_when_query_fails(spark_session, monkeypatch, tmp_path):
    query_file = tmp_path / "queries.json"
    query_file.write_text(json.dumps(["SELECT broken"]))
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(query_file)])
    spark_session.sql.side_effect = RuntimeError("query failed")
    with pytest.raises(RuntimeError, match="SQL statement 1 at <inline> failed"):
        runpy.run_path(str(SCRIPT), run_name="__main__")
    spark_session.stop.assert_called_once_with()


def test_script_stops_session_when_input_cannot_be_read(spark_session, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(tmp_path / "missing.json")])
    with pytest.raises(FileNotFoundError):
        runpy.run_path(str(SCRIPT), run_name="__main__")
    spark_session.stop.assert_called_once_with()


def test_script_executes_json_statements_without_resplitting(spark_session, monkeypatch, tmp_path):
    statements = ["SELECT 'a;b'", "-- comment; text\nSELECT 'café'", "SELECT `c;d` FROM t"]
    query_file = tmp_path / "queries.json"
    query_file.write_text(json.dumps(statements), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(query_file)])
    # The Spark image only needs PySpark and the standard library.
    monkeypatch.setitem(sys.modules, "sqlparse", None)

    runpy.run_path(str(SCRIPT), run_name="__main__")

    assert spark_session.sql.call_args_list == [mock.call(statement) for statement in statements]
    spark_session.stop.assert_called_once_with()


@pytest.mark.parametrize("payload", [{"query": "SELECT 1"}, "SELECT 1", ["SELECT 1", 2], None])
def test_script_rejects_invalid_statement_payload(spark_session, monkeypatch, tmp_path, payload):
    query_file = tmp_path / "queries.json"
    query_file.write_text(json.dumps(payload))
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(query_file)])

    with pytest.raises(ValueError, match="JSON array of SQL strings"):
        runpy.run_path(str(SCRIPT), run_name="__main__")

    spark_session.sql.assert_not_called()
    spark_session.stop.assert_called_once_with()


@pytest.mark.parametrize("arguments", [[], ["one.json", "two.json"]])
def test_usage_error_does_not_start_spark(spark_session, monkeypatch, arguments):
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), *arguments])
    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert error.value.code == 1
    sys.modules["pyspark.sql"].SparkSession.builder.getOrCreate.assert_not_called()


@pytest.mark.parametrize(
    "metadata",
    [
        {"schema_version": True},
        {"schema_version": 1, "source": []},
        {"schema_version": 1, "statements": "private SQL text"},
        {"schema_version": 1, "statements": ["private SQL text"]},
        {"schema_version": 1, "statements": [{"line": "private SQL text"}]},
        {"schema_version": 1, "statements": [{"line": True}]},
        {"schema_version": 1, "statements": [{"line": 0}]},
    ],
)
def test_invalid_source_metadata_fails_before_sql_without_echoing_values(spark_session, metadata):
    execute = runpy.run_path(str(SCRIPT))["execute_sql_commands"]
    with pytest.raises(ValueError, match="SQL source") as error:
        execute(spark_session, ["SELECT 1"], metadata)
    assert "private SQL" not in str(error.value)
    spark_session.sql.assert_not_called()


@pytest.mark.parametrize("condition", ["TABLE_OR_VIEW_NOT_FOUND", "secret=private-sentinel", None])
def test_failure_locations_sanitize_source_and_bound_error_condition(spark_session, condition):
    class QueryError(Exception):
        def getCondition(self):
            return condition

        def getErrorClass(self):
            raise RuntimeError("private-sentinel")

    execute = runpy.run_path(str(SCRIPT))["execute_sql_commands"]
    spark_session.sql.side_effect = QueryError("SELECT private-sentinel")
    metadata = {
        "schema_version": 1,
        "source": "/private/customer/query\nname.sql",
        "statements": [{"line": 8}],
    }
    with pytest.raises(RuntimeError, match="SQL statement 1 at query_name.sql:8 failed") as error:
        execute(spark_session, ["SELECT private-sentinel"], metadata)
    assert "private" not in str(error.value)
    assert ("TABLE_OR_VIEW_NOT_FOUND" in str(error.value)) == (condition == "TABLE_OR_VIEW_NOT_FOUND")
