"""Run isolated DAG tests in an in-cluster Airflow pod and retain each outcome."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from importlib.metadata import distribution, version
from pathlib import Path

CASES = (
    "sync-success",
    "sync-failure",
    "deferred-success",
    "deferred-failure",
    "sync-timeout",
    "deferred-timeout",
)
PREFIX = "AIRFLOW_K8S_CASE_RESULT "


def main():
    installed = distribution("spark-on-k8s")
    import spark_on_k8s

    assert "site-packages" in str(Path(spark_on_k8s.__file__).resolve()), spark_on_k8s.__file__
    direct_url = json.loads(installed.read_text("direct_url.json") or "{}")
    assert not direct_url.get("dir_info", {}).get("editable"), "Wheel installation required"
    print(
        json.dumps(
            {
                "airflow": version("apache-airflow"),
                "spark_on_k8s": installed.version,
                "installed_package": spark_on_k8s.__file__,
                "mode": "in-cluster dag.test",
            }
        ),
        flush=True,
    )
    subprocess.run(["airflow", "db", "migrate"], check=True, timeout=120)
    failed = False
    for scenario in CASES:
        env = os.environ | {"AIRFLOW_K8S_CASE": scenario}
        try:
            result = subprocess.run(
                [sys.executable, "-I", "/opt/airflow/tests/dags/lifecycle.py"],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=360,
            )
            print(result.stdout, flush=True)
            records = [
                json.loads(line.removeprefix(PREFIX))
                for line in result.stdout.splitlines()
                if line.startswith(PREFIX)
            ]
            assert result.returncode == 0 and len(records) == 1 and records[0]["passed"], (
                f"Case returned {result.returncode}; expected exactly one success record"
            )
            if scenario.endswith("timeout"):
                assert "Spark application startup timeout exceeded" in result.stdout, "Wrong failure reason"
            if scenario.startswith("deferred"):
                # Both supported Airflow versions emit this only after running the inline trigger.
                assert "Trigger completed" in result.stdout, "Airflow did not resume a real trigger"
        except Exception as error:
            if isinstance(error, subprocess.TimeoutExpired) and error.stdout:
                print(error.stdout.decode() if isinstance(error.stdout, bytes) else error.stdout, flush=True)
            print(
                PREFIX
                + json.dumps(
                    {
                        "scenario": scenario,
                        "passed": False,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    }
                ),
                flush=True,
            )
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
