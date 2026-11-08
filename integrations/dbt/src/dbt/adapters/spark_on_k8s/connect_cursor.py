"""SQL-only dbt cursor backed by an independent Spark Connect session.

Results are buffered when execute returns so that dbt never reports a lazy query
as successful before Spark executes it. This is intended for dbt metadata,
seeds and materializations, not for streaming large SELECT results to Python.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from dbt.adapters.spark.connections import SparkConnectionWrapper

if TYPE_CHECKING:
    from spark_on_k8s.connect import ConnectProfile


def bind_sql(sql: str, bindings: Sequence[Any]) -> tuple[str, list[Any]]:
    """Translate dbt's format markers to native Spark SQL parameters.

    Values remain typed and are never interpolated into SQL. Markers in quoted
    strings, identifiers and comments are SQL text, not parameters.
    """
    if isinstance(bindings, (str, bytes)) or not isinstance(bindings, Sequence):
        raise TypeError("Connect SQL bindings must be a list or tuple")
    parts = []
    index = count = 0
    quote = None
    comment_depth = 0
    line_comment = False
    while index < len(sql):
        char = sql[index]
        pair = sql[index : index + 2]
        if line_comment:
            parts.append(char)
            line_comment = char != "\n"
        elif comment_depth:
            if pair == "/*":
                comment_depth += 1
                parts.append(pair)
                index += 1
            elif pair == "*/":
                comment_depth -= 1
                parts.append(pair)
                index += 1
            else:
                parts.append(char)
        elif quote:
            parts.append(char)
            if char == "\\" and index + 1 < len(sql):
                index += 1
                parts.append(sql[index])
            elif char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 1
                    parts.append(sql[index])
                else:
                    quote = None
        elif pair == "--":
            line_comment = True
            parts.append(pair)
            index += 1
        elif pair == "/*":
            comment_depth = 1
            parts.append(pair)
            index += 1
        elif char in {"'", '"', "`"}:
            quote = char
            parts.append(char)
        elif pair == "%s":
            parts.append("?")
            count += 1
            index += 1
        else:
            parts.append(char)
        index += 1
    if count != len(bindings):
        raise ValueError(f"SQL has {count} parameter markers but received {len(bindings)} bindings")
    return "".join(parts), list(bindings)


class ConnectConnectionWrapper(SparkConnectionWrapper):
    """One dbt connection owns one remote session; server ownership lives elsewhere."""

    arraysize = 1

    def __init__(self, profile: ConnectProfile, parameters: dict[str, str] | None = None):
        self._closed = False
        self._cancelled = False
        self._rows: list[tuple] = []
        self._offset = 0
        self._description = None
        self._context = profile.session()
        self._session = self._context.__enter__()
        try:
            for key, value in (parameters or {}).items():
                self._session.conf.set(key, value)
        except BaseException as error:
            self._closed = True
            self._context.__exit__(type(error), error, error.__traceback__)
            raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("The Spark Connect connection is closed")

    def cursor(self) -> ConnectConnectionWrapper:
        self._ensure_open()
        self._rows = []
        self._offset = 0
        self._description = None
        return self

    def execute(self, sql: str, bindings: Sequence[Any] | None = None) -> None:
        self._ensure_open()
        if self._cancelled:
            raise RuntimeError("The Spark Connect connection was cancelled")
        self.cursor()
        if bindings is None:
            frame = self._session.sql(sql)
        else:
            sql, values = bind_sql(sql, bindings)
            frame = self._session.sql(sql, args=values)
        rows = frame.collect()
        self._description = [
            (field.name, field.dataType.simpleString(), None, None, None, None, field.nullable)
            for field in frame.schema.fields
        ]
        self._rows = [tuple(row) for row in rows]

    @property
    def description(self):
        return self._description

    def fetchone(self):
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        self._ensure_open()
        size = self.arraysize if size is None else size
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("fetchmany size must be a non-negative integer")
        rows = self._rows[self._offset : self._offset + size]
        self._offset += len(rows)
        return rows

    def fetchall(self) -> list[tuple]:
        return self.fetchmany(len(self._rows) - self._offset)

    def cancel(self) -> None:
        # dbt cancels from its main thread while the worker is executing SQL.
        # Do not guard execution with a lock that would block this call.
        self._cancelled = True
        if not self._closed:
            self._session.interruptAll()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._rows = []
        self._description = None
        self._context.__exit__(None, None, None)

    def rollback(self, *args, **kwargs) -> None:
        # Spark SQL statements are not grouped into a database transaction.
        return None
