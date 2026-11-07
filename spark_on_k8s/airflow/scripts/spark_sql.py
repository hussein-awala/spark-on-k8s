from __future__ import annotations

import json
import logging

from pyspark.sql import SparkSession

logger = logging.getLogger("SparkSqlOnK8s")


def read_sql_file(file_path):
    """Read the JSON array of SQL statements prepared by SparkSqlOnK8SOperator."""
    with open(file_path, encoding="utf-8") as f:
        commands = json.load(f)
    if not isinstance(commands, list) or not all(isinstance(command, str) for command in commands):
        raise ValueError("Expected a JSON array of SQL strings")
    return commands


def execute_sql_commands(spark_session: SparkSession, commands):
    """
    Executes a list of SQL commands on a Spark session.
    """
    for command in commands:
        try:
            logger.info(f"Executing: {command}")
            spark_session.sql(command).show(truncate=False)
        except Exception:
            logger.exception(f"Error executing SQL command: {command}")
            raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        logger.error("Usage: spark-submit spark_sql.py <path_to_statements_json>")
        sys.exit(1)

    sql_file_path = sys.argv[1]

    _spark_session = SparkSession.builder.getOrCreate()

    try:
        sql_commands = read_sql_file(sql_file_path)
        execute_sql_commands(_spark_session, sql_commands)
    finally:
        _spark_session.stop()
