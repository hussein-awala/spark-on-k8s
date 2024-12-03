from __future__ import annotations

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger("SparkSqlOnK8s")


def read_sql_file(file_path):
    """
    Reads an SQL file and splits commands by semicolon ";".
    """
    with open(file_path) as f:
        file_content = f.read()
    # Split commands by semicolon while ignoring empty lines
    return [command.strip() for command in file_content.split(";") if command.strip()]


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


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        logger.error("Usage: spark-submit run_sql_commands.py <path_to_sql_file>")
        sys.exit(1)

    sql_file_path = sys.argv[1]

    _spark_session = SparkSession.builder.getOrCreate()

    # Read SQL commands from file
    sql_commands = read_sql_file(sql_file_path)

    # Execute each SQL command
    execute_sql_commands(_spark_session, sql_commands)

    # Stop SparkSession
    _spark_session.stop()
