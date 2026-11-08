from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt.adapters.spark_on_k8s.connect_cursor import ConnectConnectionWrapper, bind_sql


class Profile:
    def __init__(self, rows=None):
        self.sessions = []
        self.released = []
        self.rows = rows or [(1,), (2,), (3,)]

    @contextmanager
    def session(self):
        data_type = SimpleNamespace(simpleString=lambda: "bigint")
        schema = SimpleNamespace(fields=[SimpleNamespace(name="id", dataType=data_type, nullable=False)])
        frame = SimpleNamespace(collect=Mock(return_value=self.rows), schema=schema)
        session = SimpleNamespace(sql=Mock(return_value=frame), conf=Mock(), interruptAll=Mock())
        self.sessions.append(session)
        try:
            yield session
        finally:
            self.released.append(session)


def test_parameters_use_native_bindings_preserving_types_and_not_interpolation():
    profile = Profile()
    wrapper = ConnectConnectionWrapper(profile, {"spark.sql.ansi.enabled": "false"})
    timestamp = datetime(2026, 1, 2, 3, 4, 5, 123456)
    values = [2**60 + 1, Decimal("1234567890123456.123456"), timestamp, "x'); DROP TABLE t;--", None]
    wrapper.execute("SELECT %s, %s, %s, %s, %s", values)
    profile.sessions[0].sql.assert_called_once_with("SELECT ?, ?, ?, ?, ?", args=values)
    profile.sessions[0].conf.set.assert_called_once_with("spark.sql.ansi.enabled", "false")
    assert wrapper.description == [("id", "bigint", None, None, None, None, False)]


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT '%s', \"%s\", `%s`, %s", "SELECT '%s', \"%s\", `%s`, ?"),
        ("-- %s\nSELECT %s /* %s */", "-- %s\nSELECT ? /* %s */"),
        ("SELECT 'it''s %s', %s", "SELECT 'it''s %s', ?"),
        (r"SELECT 'it\'s %s', %s", r"SELECT 'it\'s %s', ?"),
        ("/* nested /* %s */ %s */ SELECT %s", "/* nested /* %s */ %s */ SELECT ?"),
    ],
)
def test_placeholder_conversion_preserves_sql_literals_and_comments(sql, expected):
    assert bind_sql(sql, [7]) == (expected, [7])


@pytest.mark.parametrize("bindings", [[], [1, 2]])
def test_wrong_binding_count_fails_before_sql_execution(bindings):
    profile = Profile()
    wrapper = ConnectConnectionWrapper(profile)
    with pytest.raises(ValueError, match="parameter markers"):
        wrapper.execute("SELECT %s", bindings)
    profile.sessions[0].sql.assert_not_called()


@pytest.mark.parametrize("bindings", ["value", b"value", {"name": "value"}])
def test_named_and_string_bindings_rejected(bindings):
    with pytest.raises(TypeError, match="list or tuple"):
        bind_sql("SELECT %s", bindings)


def test_fetch_methods_consume_rows_and_new_execution_resets_results():
    profile = Profile()
    wrapper = ConnectConnectionWrapper(profile)
    wrapper.execute("SELECT id FROM source")
    assert wrapper.fetchone() == (1,)
    assert wrapper.fetchmany(0) == []
    assert wrapper.fetchmany() == [(2,)]
    assert wrapper.fetchall() == [(3,)]
    assert wrapper.fetchone() is None
    assert wrapper.fetchall() == []
    wrapper.execute("SELECT id FROM source")
    assert wrapper.fetchmany(100) == [(1,), (2,), (3,)]
    wrapper.cursor()
    assert wrapper.description is None
    assert wrapper.fetchall() == []


@pytest.mark.parametrize("size", [-1, 1.5, True])
def test_invalid_fetch_size(size):
    with pytest.raises(ValueError, match="non-negative integer"):
        ConnectConnectionWrapper(Profile()).fetchmany(size)


def test_query_failure_clears_previous_results_and_does_not_retry():
    profile = Profile()
    wrapper = ConnectConnectionWrapper(profile)
    wrapper.execute("SELECT id")
    profile.sessions[0].sql.side_effect = RuntimeError("SQL failed")
    with pytest.raises(RuntimeError, match="SQL failed"):
        wrapper.execute("INSERT INTO t SELECT bad")
    assert profile.sessions[0].sql.call_count == 2
    assert wrapper.description is None
    assert wrapper.fetchall() == []


def test_collect_failure_is_reported_before_execute_returns():
    profile = Profile()
    wrapper = ConnectConnectionWrapper(profile)
    profile.sessions[0].sql.return_value.collect.side_effect = RuntimeError("executor failed")
    with pytest.raises(RuntimeError, match="executor failed"):
        wrapper.execute("SELECT id")
    assert wrapper.description is None


def test_sessions_are_independent_and_close_is_idempotent(monkeypatch):
    monkeypatch.setenv("SPARK_REMOTE", "sc://unrelated-server:15002")
    profile = Profile()
    first = ConnectConnectionWrapper(profile)
    second = ConnectConnectionWrapper(profile)
    first.cancel()
    profile.sessions[0].interruptAll.assert_called_once_with()
    profile.sessions[1].interruptAll.assert_not_called()
    first.close()
    first.close()
    first.cancel()
    assert profile.released == [profile.sessions[0]]
    second.execute("SELECT 1")
    second.close()
    assert profile.released == profile.sessions
    import os

    assert os.environ["SPARK_REMOTE"] == "sc://unrelated-server:15002"
    with pytest.raises(RuntimeError, match="closed"):
        first.execute("SELECT 1")


def test_cancel_can_interrupt_an_executing_worker():
    profile = Profile()
    wrapper = ConnectConnectionWrapper(profile)
    executing, interrupted = Event(), Event()

    def collect():
        executing.set()
        assert interrupted.wait(5), "Cancellation was blocked behind the executing query"
        return []

    profile.sessions[0].sql.return_value.collect.side_effect = collect
    profile.sessions[0].interruptAll.side_effect = interrupted.set
    failures = []

    def execute():
        try:
            wrapper.execute("SELECT expensive")
        except BaseException as error:
            failures.append(error)

    worker = Thread(target=execute)
    worker.start()
    try:
        assert executing.wait(5)
        wrapper.cancel()
    finally:
        interrupted.set()
        worker.join(5)
        wrapper.close()
    assert not worker.is_alive()
    assert not failures


def test_failed_configuration_releases_the_new_session():
    released = []

    class BrokenProfile:
        @contextmanager
        def session(self):
            try:
                yield SimpleNamespace(conf=SimpleNamespace(set=Mock(side_effect=ValueError("static config"))))
            finally:
                released.append(True)

    with pytest.raises(ValueError, match="static config"):
        ConnectConnectionWrapper(BrokenProfile(), {"spark.static.setting": "bad"})
    assert released == [True]


def test_cancelled_connect_session_cannot_submit_another_statement():
    profile = Profile()
    wrapper = ConnectConnectionWrapper(profile)
    wrapper.cancel()
    with pytest.raises(RuntimeError, match="connection was cancelled"):
        wrapper.execute("INSERT INTO t VALUES (1)")
    profile.sessions[0].sql.assert_not_called()
