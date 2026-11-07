from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from spark_on_k8s.connect import ConnectProfile


def main():
    parser = argparse.ArgumentParser(description="Test an isolated local Spark Connect server")
    parser.add_argument("--spark-home", type=Path, required=True)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="spark-connect-server-") as temporary:
        root = Path(temporary)
        (root / "conf").mkdir()
        with socket.socket() as available:
            available.bind(("127.0.0.1", 0))
            port = available.getsockname()[1]
        env = {key: value for key, value in os.environ.items() if not key.startswith(("SPARK_", "PYSPARK_"))}
        env.update(
            JAVA_HOME=str(args.java_home),
            SPARK_HOME=str(args.spark_home),
            SPARK_LOCAL_IP="127.0.0.1",
            SPARK_CONF_DIR=str(root / "conf"),
            SPARK_LOG_DIR=str(root),
            SPARK_PID_DIR=str(root),
        )
        command = [
            str(args.spark_home / "sbin/start-connect-server.sh"),
            "--wait",
            "--master",
            "local[2]",
            "--conf",
            "spark.ui.enabled=false",
            "--conf",
            "spark.driver.bindAddress=127.0.0.1",
            "--conf",
            "spark.connect.grpc.binding.address=127.0.0.1",
            "--conf",
            f"spark.connect.grpc.binding.port={port}",
        ]
        with (args.artifacts / "server.log").open("w") as log:
            process = subprocess.Popen(
                command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
            )
            try:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError("Connect server exited; inspect server.log")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                            break
                    except OSError:
                        time.sleep(0.25)
                else:
                    raise TimeoutError("Connect server did not start within 60 seconds")
                profile = ConnectProfile(endpoint=f"sc://127.0.0.1:{port}", tls=False)
                result = profile.probe(timeout=30)
                # Real DataFrame execution and independent session state.
                with profile.session() as first:
                    assert first.range(10).groupBy().sum("id").first()[0] == 45
                    assert first.range(3).toPandas()["id"].tolist() == [0, 1, 2]
                    first.range(1).createOrReplaceTempView("private_view")
                    with profile.session() as second:
                        assert not second.catalog.tableExists("private_view")
                        assert second.sql("SELECT 2 AS value").first().value == 2
                    assert first.range(3).count() == 3
                # Closing the client's sessions leaves the shared server usable.
                assert profile.probe(timeout=30)["status"] == "ok"
                with socket.socket() as unused:
                    unused.bind(("127.0.0.1", 0))
                    unused_port = unused.getsockname()[1]
                    started = time.monotonic()
                    try:
                        ConnectProfile(endpoint=f"sc://127.0.0.1:{unused_port}", tls=False).probe(timeout=1)
                    except TimeoutError:
                        assert time.monotonic() - started < 5
                    else:
                        raise AssertionError("Unreachable server did not respect the timeout")
                result.update(dataframe_sum=45, independent_sessions=True, timeout_enforced=True)
                (args.artifacts / "result.json").write_text(json.dumps(result, indent=2) + "\n")
                print(json.dumps(result), flush=True)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)


if __name__ == "__main__":
    main()
