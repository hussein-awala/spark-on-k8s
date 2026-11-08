from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from pyspark.sql import SparkSession

logger = logging.getLogger("SparkSqlOnK8s")


def read_sql_file(file_path):
    """Read the JSON array of SQL statements prepared by the SQL submission helpers."""
    with open(file_path, encoding="utf-8") as f:
        commands = json.load(f)
    if not isinstance(commands, list) or not all(isinstance(command, str) for command in commands):
        raise ValueError("Expected a JSON array of SQL strings")
    return commands


def read_locations(query_file):
    """Read optional source coordinates; legacy arrays remain supported."""
    location_file = Path(query_file).with_name("locations.json")
    if not location_file.is_file():
        return None
    with location_file.open(encoding="utf-8") as stream:
        locations = json.load(stream)
    return validate_locations(locations)


def validate_locations(locations):
    """Reject malformed optional metadata before any SQL executes."""
    if locations is None:
        return None
    if (
        not isinstance(locations, dict)
        or type(locations.get("schema_version")) is not int
        or locations["schema_version"] != 1
    ):
        raise ValueError("Expected version 1 SQL source locations")
    source = locations.get("source", "<inline>")
    entries = locations.get("statements", [])
    if not isinstance(source, str) or not isinstance(entries, list):
        raise ValueError("Invalid SQL source locations")
    for entry in entries:
        if not isinstance(entry, dict) or (
            entry.get("line") is not None and (type(entry["line"]) is not int or entry["line"] < 1)
        ):
            raise ValueError("Invalid SQL source line")
    source = re.sub(r"[\x00-\x1f\x7f]", "_", source.replace("\\", "/").rsplit("/", 1)[-1])[:255] or "<inline>"
    return {**locations, "source": source}


def execute_sql_commands(spark_session: SparkSession, commands, locations=None):
    """Execute sequentially, reporting source coordinates without logging query text."""
    locations = validate_locations(locations)
    for index, command in enumerate(commands):
        source = "<inline>" if locations is None else locations.get("source", "<inline>")
        entries = [] if locations is None else locations.get("statements", [])
        line = entries[index].get("line") if index < len(entries) else None
        position = f"{source}:{line}" if line is not None else source
        label = f"SQL statement {index + 1} at {position}"
        try:
            logger.info("Executing %s", label)
            spark_session.sql(command).show(truncate=False)
        except Exception as error:
            # Do not reproduce query text or arbitrary exception messages here.
            condition = None
            for name in ("getCondition", "getErrorClass"):
                method = getattr(error, name, None)
                if callable(method):
                    try:
                        candidate = method()
                    except Exception:
                        continue
                    if isinstance(candidate, str) and re.fullmatch(r"[A-Z_][A-Z0-9_.]{0,127}", candidate):
                        condition = candidate
                        break
            detail = f" ({condition})" if condition else ""
            raise RuntimeError(f"{label} failed{detail}") from None


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        logger.error("Usage: spark-submit spark_sql.py <path_to_statements_json>")
        sys.exit(1)

    sql_file_path = sys.argv[1]

    _spark_session = SparkSession.builder.getOrCreate()

    try:
        sql_commands = read_sql_file(sql_file_path)
        execute_sql_commands(_spark_session, sql_commands, read_locations(sql_file_path))
    finally:
        _spark_session.stop()
