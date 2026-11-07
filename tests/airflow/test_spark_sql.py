from __future__ import annotations

import runpy
import sys
from pathlib import Path
from unittest import mock

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "spark_on_k8s/airflow/scripts/spark_sql.py"


@pytest.fixture
def spark_session(monkeypatch):
    session = mock.Mock()
    pyspark_sql = mock.Mock()
    pyspark_sql.SparkSession.builder.getOrCreate.return_value = session
    monkeypatch.setitem(sys.modules, "pyspark", mock.Mock())
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
    with pytest.raises(RuntimeError, match="query failed"):
        execute(spark_session, ["SELECT broken", "INSERT INTO output SELECT 1"])
    spark_session.sql.assert_called_once_with("SELECT broken")


def test_script_stops_session_when_query_fails(spark_session, monkeypatch, tmp_path):
    query_file = tmp_path / "queries.sql"
    query_file.write_text("SELECT broken;")
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(query_file)])
    spark_session.sql.side_effect = RuntimeError("query failed")
    with pytest.raises(RuntimeError, match="query failed"):
        runpy.run_path(str(SCRIPT), run_name="__main__")
    spark_session.stop.assert_called_once_with()


def test_script_stops_session_when_input_cannot_be_read(spark_session, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(tmp_path / "missing.sql")])
    with pytest.raises(FileNotFoundError):
        runpy.run_path(str(SCRIPT), run_name="__main__")
    spark_session.stop.assert_called_once_with()
