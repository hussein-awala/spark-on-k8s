from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def run(command: list[str], log: Path, *, succeeds: bool = True, marker: str | None = None):
    with log.open("w") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=180)
    text = log.read_text()
    assert (result.returncode == 0) == succeeds, f"Unexpected exit {result.returncode}: {log}\n{text[-4000:]}"
    if marker:
        assert marker in text, f"Missing {marker}: {log}"
    if not succeeds:
        assert "UNRESOLVED_COLUMN" in text and "missing_column" in text, f"Unexpected failure: {log}"
        assert "SHOULD_NOT_RUN" not in text, f"SQL continued after failure: {log}"


def main():
    parser = argparse.ArgumentParser(description="Execute real Spark Python, SQL, and Java workloads locally")
    parser.add_argument("--spark-version", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    import pyspark

    assert pyspark.__version__ == args.spark_version
    assert sys.prefix != sys.base_prefix, "Use an isolated Spark runtime environment"
    spark_home = Path(pyspark.__file__).parent
    java_home = Path(os.environ["JAVA_HOME"])
    fixtures = Path(__file__).parent.resolve()
    repository = fixtures.parents[2]
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    os.environ.update(
        SPARK_LOCAL_IP="127.0.0.1", PYSPARK_PYTHON=sys.executable, PYSPARK_DRIVER_PYTHON=sys.executable
    )
    base = [
        str(spark_home / "bin/spark-submit"),
        "--master",
        "local[2]",
        "--conf",
        "spark.ui.enabled=false",
        "--conf",
        "spark.sql.shuffle.partitions=2",
        "--conf",
        "spark.driver.host=127.0.0.1",
    ]
    run(
        [*base, str(fixtures / "runtime_job.py"), args.spark_version],
        artifacts / "python.log",
        marker="SPARK_ON_K8S_PYTHON_OK",
    )
    runner = repository / "spark_on_k8s/airflow/scripts/spark_sql.py"
    run(
        [*base, str(runner), str(fixtures / "sql_success.json")],
        artifacts / "sql.log",
        marker="SPARK_ON_K8S_SQL_OK",
    )
    run(
        [*base, str(runner), str(fixtures / "sql_failure.json")],
        artifacts / "sql-failure.log",
        succeeds=False,
    )
    classes = artifacts / "classes"
    classes.mkdir(exist_ok=True)
    run(
        [
            str(java_home / "bin/javac"),
            "--release",
            "17",
            "-cp",
            str(spark_home / "jars/*"),
            "-d",
            str(classes),
            str(fixtures / "IntegrationJob.java"),
        ],
        artifacts / "javac.log",
    )
    jar = artifacts / "integration.jar"
    run([str(java_home / "bin/jar"), "cf", str(jar), "-C", str(classes), "."], artifacts / "jar.log")
    run(
        [*base, "--class", "IntegrationJob", str(jar), args.spark_version],
        artifacts / "java.log",
        marker="SPARK_ON_K8S_JAVA_OK",
    )
    result = {
        "spark": args.spark_version,
        "python": sys.version.split()[0],
        "java_home": str(java_home),
        "mode": "local[2]",
        "passed": ["python-workers", "sql", "sql-failure", "java"],
    }
    (artifacts / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
