"""Exercise the installed managed dbt adapter against an explicitly selected kind cluster."""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import spark_on_k8s
import yaml
from kubernetes import config
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("managed_dbt_fixture", HERE / "fixture.py")
fixture_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture_module)
Fixture, save = fixture_module.Fixture, fixture_module.save
REQUEST = (5, 20)


def installed_package():
    evidence = {}
    for name, module in (
        ("spark-on-k8s", spark_on_k8s),
        ("dbt-spark-on-k8s", importlib.import_module("spark_on_k8s_dbt")),
        ("dbt-spark-on-k8s-adapter", importlib.import_module("dbt.adapters.spark_on_k8s")),
    ):
        source = Path(module.__file__).resolve()
        if not source.is_relative_to(Path(sys.prefix).resolve()) or source.is_relative_to(
            ROOT / "spark_on_k8s"
        ):
            raise RuntimeError(f"Install and test noneditable wheels in this environment: {source}")
        distribution = importlib.metadata.distribution(name.removesuffix("-adapter"))
        direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
        if direct_url.get("dir_info", {}).get("editable"):
            raise RuntimeError("An editable installation is not installed-wheel evidence")
        evidence[name] = {"source": str(source), "version": distribution.version, "direct_url": direct_url}
    executable = Path(sys.executable).with_name("spark-on-k8s-dbt")
    if not executable.is_file():
        raise RuntimeError("Install the experimental dbt extra/wheel in the runner environment")
    return evidence, executable


def dbt_environment():
    environment = {
        **{key: value for key, value in os.environ.items() if not key.startswith("DBT_")},
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DBT_USE_COLORS": "false",
        "PYTHONNOUSERSITE": "1",
    }
    for key in ("SPARK_REMOTE", "PYTHONPATH", "DBT_PROFILES_DIR", "DBT_TARGET_PATH", "DBT_LOG_PATH"):
        environment.pop(key, None)
    return environment


def assert_tunnels_absent(namespace):
    processes = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True, timeout=10)
    remaining = [
        line.strip()
        for line in processes.splitlines()
        if namespace in line and "kubectl" in line and "port-forward" in line
    ]
    if remaining:
        raise AssertionError(f"Invocation-owned tunnel remains: {remaining}")
    return []


def write_profile(args, fixture, method, directory):
    directory.mkdir(parents=True)
    jar = "spark-connect" if method == "connect" else "spark-hive-thriftserver"
    compute = {
        "image": args.image,
        "server_jar": f"local:///opt/spark/jars/{jar}_2.13-4.3.0.jar",
        "namespace": fixture.namespace,
        "service_account": "spark",
        "app_name": "dbt-managed-" + method,
        "kube_config_file": str(args.kubeconfig),
        "kube_context": args.context,
        "in_cluster": False,
        "image_pull_policy": "Never",
        "driver_resources": {"cpu": 1, "memory": 768, "memory_overhead": 512},
        "executor_resources": {"cpu": 1, "memory": 512, "memory_overhead": 384},
        "executor_instances": {"initial": 1},
        "startup_timeout": 180,
        "cleanup_timeout": 120,
        "poll_interval": 0.5,
        "spark_conf": {
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": fixture.metastore,
            "spark.sql.warehouse.dir": fixture.warehouse,
            "spark.hadoop.fs.defaultFS": fixture.warehouse.rsplit("/", 1)[0],
            "spark.hadoop.dfs.client.use.datanode.hostname": "true",
            "spark.hadoop.fs.hdfs.impl.disable.cache": "true",
            "spark.kubernetes.executor.request.cores": "0.25",
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            "spark.sql.ansi.enabled": "false",
        },
    }
    document = {
        "spark_on_k8s_managed": {
            "target": method,
            "outputs": {
                method: {
                    "type": "spark_on_k8s",
                    "method": method,
                    "schema": "managed_" + method,
                    "threads": 2,
                    "compute": compute,
                }
            },
        }
    }
    (directory / "profiles.yml").write_text(yaml.safe_dump(document, sort_keys=False))


class Scenario:
    def __init__(self, args, fixture, executable, method):
        self.args, self.fixture, self.executable, self.method = args, fixture, executable, method
        self.destination = args.artifacts / method
        self.destination.mkdir()
        self.project = self.destination / "project"
        self.profiles = self.destination / "profiles"
        shutil.copytree(HERE / "project", self.project)
        self.python_project = self.destination / "python-project"
        shutil.copytree(HERE / "python_project", self.python_project)
        write_profile(args, fixture, method, self.profiles)
        self.results, self.driver_uids = [], set()

    def active_query(self, pods, destination):
        """Require the probe's Spark SQL job and an owned executor task to be active."""
        drivers = [pod for pod in pods if (pod.metadata.labels or {}).get("spark-role") == "driver"]
        if len(drivers) != 1:
            return None
        driver = drivers[0]
        app_id = driver.metadata.labels["spark-app-id"]
        probe = """import json,sys,urllib.request
base = 'http://127.0.0.1:4040/api/v1/applications/' + sys.argv[1]
def get(path):
    with urllib.request.urlopen(base + path, timeout=3) as response:
        return json.load(response)
queries = get('/sql')
matches = [q for q in queries if '1000000000000' in q.get('planDescription', '')]
ids = {str(j) for q in matches for j in q.get('runningJobIds', [])}
jobs = [get('/jobs/' + job_id) for job_id in ids]
print(json.dumps({'queries': matches, 'jobs': jobs, 'executors': get('/executors')}))
"""
        try:
            response = self.fixture.command(
                "exec",
                driver.metadata.name,
                "--",
                "python3",
                "-c",
                probe,
                app_id,
                timeout=20,
            )
            evidence = json.loads(response.stdout)
        except (subprocess.SubprocessError, ValueError):
            return None
        save(destination / "cancellation-probe-latest.json", evidence)
        hosts = {
            host
            for pod in pods
            if (pod.metadata.labels or {}).get("spark-role") == "executor"
            and any(owner.uid == driver.metadata.uid for owner in pod.metadata.owner_references or [])
            for host in (pod.status.pod_ip, pod.metadata.name)
        }
        active_executors = [
            executor
            for executor in evidence["executors"]
            if executor["id"] != "driver"
            and executor.get("activeTasks", 0) > 0
            and executor.get("hostPort", "").split(":")[0] in hosts
        ]
        if not evidence["queries"] or not active_executors:
            return None
        if not any(job.get("numActiveTasks", 0) > 0 for job in evidence["jobs"]):
            return None
        evidence["owned_active_executors"] = active_executors
        evidence["driver_uid"] = driver.metadata.uid
        return evidence

    def command(self, label, *arguments, expected=0, interrupt=False, nodes=None, project=None):
        directory = self.destination / label
        directory.mkdir()
        receipt_dir = directory / "receipt"
        target = directory / "target"
        command = [
            str(self.executable),
            "run",
            "--receipt-dir",
            str(receipt_dir),
            "--",
            *arguments,
            "--project-dir",
            str(project or self.project),
            "--profiles-dir",
            str(self.profiles),
            "--target",
            self.method,
            "--target-path",
            str(target),
            "--log-path",
            str(directory / "logs"),
        ]
        environment = dbt_environment()
        observed = {}
        interrupted = False
        started = time.monotonic()
        logfile = directory / "command.log"
        with logfile.open("w") as log:
            process = subprocess.Popen(
                command, stdout=log, stderr=subprocess.STDOUT, env=environment, start_new_session=True
            )
            try:
                deadline = started + args_timeout(self.args, interrupt)
                while process.poll() is None:
                    pods = self.fixture.api.list_namespaced_pod(
                        self.fixture.namespace, _request_timeout=REQUEST
                    ).items
                    for pod in pods:
                        if (pod.metadata.labels or {}).get("spark-role") in {"driver", "executor"}:
                            observed[pod.metadata.uid] = (
                                self.fixture.api.api_client.sanitize_for_serialization(pod)
                            )
                    if interrupt and not interrupted:
                        content = logfile.read_text()
                        executors = [
                            pod
                            for pod in pods
                            if (pod.metadata.labels or {}).get("spark-role") == "executor"
                            and pod.status.phase == "Running"
                        ]
                        active = None
                        if executors and "cancellation_probe" in content and "START" in content:
                            active = self.active_query(pods, directory)
                        if active:
                            save(directory / "active-query-before-interrupt.json", active)
                            self.fixture.snapshot(directory / "before-interrupt")
                            process.send_signal(signal.SIGINT)
                            interrupted = True
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"Managed dbt command exceeded its deadline: {label}")
                    time.sleep(0.5)
                code = process.wait()
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=10)
                save(directory / "observed-pods.json", list(observed.values()))
        if code != expected:
            raise AssertionError(f"{label}: expected exit {expected}, got {code}; inspect {logfile}")
        if interrupt and not interrupted:
            raise AssertionError("Cancellation did not reach an active SQL invocation")
        receipt = json.loads((receipt_dir / "receipt.json").read_text())
        if receipt["stage"] != "cleaned" or receipt["cleanup"]["status"] != "deleted":
            raise AssertionError(f"Command did not clean its owned compute: {receipt}")
        if receipt["dbt_exit_code"] != expected:
            raise AssertionError("Wrapper did not preserve the dbt result")
        run = receipt["server"]["run"]
        driver_uid = run["driver_uid"]
        if driver_uid in self.driver_uids:
            raise AssertionError("A new invocation reused the prior Spark driver")
        self.driver_uids.add(driver_uid)
        drivers = [
            pod
            for pod in observed.values()
            if pod["metadata"].get("labels", {}).get("spark-role") == "driver"
        ]
        if {pod["metadata"]["uid"] for pod in drivers} != {driver_uid}:
            raise AssertionError(
                "Invocation must provision exactly one observed driver despite concurrent dbt threads"
            )
        cleanup = self.fixture.assert_compute_absent()
        cleanup["task_tunnels_remaining"] = assert_tunnels_absent(self.fixture.namespace)
        save(directory / "independent-cleanup.json", cleanup)
        if label == "seed":
            transport = "Spark Connect" if self.method == "connect" else "Thrift"
            content = logfile.read_text()
            for line in (
                f"{transport} server ready (application {run['app_id']})",
                "managed Spark compute deleted",
            ):
                if line not in content:
                    raise AssertionError(f"Managed compute progress did not report: {line}")
            before = self.fixture.api.read_namespace(self.fixture.namespace, _request_timeout=REQUEST)
            repeated = subprocess.run(
                [str(self.executable), "cleanup", "--receipt", str(receipt_dir / "receipt.json")],
                capture_output=True,
                text=True,
                env=environment,
                timeout=150,
            )
            (directory / "repeated-cleanup.log").write_text(repeated.stdout + repeated.stderr)
            if repeated.returncode != 0:
                raise AssertionError("Recovery CLI failed on a completed receipt")
            after = self.fixture.api.read_namespace(self.fixture.namespace, _request_timeout=REQUEST)
            if before.metadata.uid != after.metadata.uid or after.metadata.uid != self.fixture.uid:
                raise AssertionError("Repeated cleanup changed the fixture namespace")
            if self.fixture.assert_compute_absent()["fixture_pods"] != cleanup["fixture_pods"]:
                raise AssertionError("Repeated cleanup changed warehouse or catalog identity")
            save(
                directory / "repeated-cleanup.json",
                {
                    "exit_code": 0,
                    "namespace_uid": after.metadata.uid,
                    "fixture_pods": cleanup["fixture_pods"],
                    "compute_resources_remaining": [],
                },
            )
        results_path = target / "run_results.json"
        worker_threads = []
        if nodes is not None:
            results = json.loads(results_path.read_text())["results"]
            if len(results) != nodes or any(
                result["status"] not in {"success", "pass"} for result in results
            ):
                raise AssertionError(f"Expected {nodes} successful dbt nodes")
            worker_threads = sorted(
                {
                    result["thread_id"]
                    for result in results
                    if result.get("thread_id") and result["thread_id"] not in {"MainThread", "main"}
                }
            )
            if nodes == 13 and len(worker_threads) < 2:
                raise AssertionError("The SQL build did not execute on at least two dbt worker threads")
        record = {
            "label": label,
            "exit_code": code,
            "stage": receipt["stage"],
            "driver_uid": driver_uid,
            "interrupted": interrupted,
            "elapsed_seconds": time.monotonic() - started,
            "compute_absence_observed": True,
            "fixture_pods": cleanup["fixture_pods"],
            "worker_thread_ids": worker_threads,
        }
        self.results.append(record)
        save(self.destination / "commands.json", self.results)
        print(json.dumps({"method": self.method, **record}), flush=True)
        return directory

    def preflight(self):
        """The read-only profile check must pass every check without creating compute."""
        directory = self.destination / "check"
        directory.mkdir()
        before = self.fixture.assert_compute_absent()
        result = subprocess.run(
            [
                str(self.executable),
                "check",
                "--project-dir",
                str(self.project),
                "--profiles-dir",
                str(self.profiles),
                "--target",
                self.method,
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            env=dbt_environment(),
            timeout=180,
        )
        (directory / "check.json").write_text(result.stdout)
        (directory / "check.log").write_text(result.stderr)
        report = json.loads(result.stdout)
        unexpected = [item for item in report["checks"] if item["status"] != "pass"]
        if result.returncode != 0 or report["status"] != "pass" or unexpected or report["error"]:
            problem = unexpected or report["error"]
            raise AssertionError(f"Profile check exited {result.returncode}: {problem}; inspect {directory}")
        after = self.fixture.assert_compute_absent()
        if after["fixture_pods"] != before["fixture_pods"]:
            raise AssertionError("The read-only profile check changed fixture identity")
        save(directory / "independent-cleanup.json", after)

    def run(self):
        self.preflight()
        self.command("seed", "seed", "--full-refresh", nodes=1)
        build = self.command("build", "build", nodes=13)
        executors = [
            pod["metadata"]["name"]
            for pod in json.loads((build / "observed-pods.json").read_text())
            if pod["metadata"].get("labels", {}).get("spark-role") == "executor"
        ]
        if not executors:
            raise AssertionError("No remote executor observed during the SQL build")
        persisted = self.command(
            "persisted-after-build",
            "run-operation",
            "assert_persisted",
            "--args",
            json.dumps({"expected_hosts": executors}),
        )
        if "PERSISTED_TABLE_VERIFIED" not in (persisted / "command.log").read_text():
            raise AssertionError("Fresh-server persisted table query did not run")
        docs = self.command("docs", "docs", "generate")
        catalog = json.loads((docs / "target/catalog.json").read_text())
        names = {entry["metadata"]["name"] for entry in catalog["nodes"].values()}
        if not {"orders", "orders_table", "orders_incremental"} <= names or catalog.get("errors"):
            raise AssertionError(f"dbt docs did not retrieve the persistent catalog: {names}")
        python_failure = self.command(
            "python-model-refused",
            "run",
            "--select",
            "python_probe",
            expected=1,
            project=self.python_project,
        )
        python_results = json.loads((python_failure / "target/run_results.json").read_text())["results"]
        expected_language_error = "only supports languages ['sql']; got \"python\""
        if (
            len(python_results) != 1
            or expected_language_error not in (python_results[0].get("message") or "").lower()
        ):
            raise AssertionError("Python model failed for an unrelated reason")
        self.command("table-preserved-after-python-refusal", "run-operation", "assert_persisted")
        sql_failure = self.command(
            "expected-sql-failure",
            "run",
            "--select",
            "expected_failure",
            "--vars",
            "{exercise_failure: true}",
            expected=1,
        )
        if "definitely_missing_column" not in (sql_failure / "command.log").read_text():
            raise AssertionError("The intentional SQL failure failed for an unrelated reason")
        self.command("recovery-build", "build", nodes=13)
        self.command(
            "interrupted",
            "run",
            "--select",
            "cancellation_probe",
            "--vars",
            "{exercise_cancel: true}",
            expected=130,
            interrupt=True,
        )
        self.command("persisted-after-cancellation", "run-operation", "assert_persisted")
        save(
            self.destination / "result.json",
            {
                "status": "passed",
                "method": self.method,
                "threads": 2,
                "fresh_driver_per_invocation": True,
                "external_catalog_survived_compute_deletion": True,
                "failed_sql_recovered": True,
                "interrupted_command_cleaned": True,
                "python_model_refused_without_table_loss": True,
                "commands": self.results,
            },
        )


def args_timeout(args, interrupt):
    return min(args.command_timeout, 300) if interrupt else args.command_timeout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True, type=Path)
    parser.add_argument("--context", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--hadoop-image", required=True)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--method", choices=["both", "thrift", "connect"], default="both")
    parser.add_argument("--command-timeout", type=int, default=600)
    args = parser.parse_args()
    if args.command_timeout <= 0:
        parser.error("command-timeout must be positive")
    args.artifacts = args.artifacts.resolve()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    args.kubeconfig = args.kubeconfig.resolve(strict=True)
    contexts, _ = config.list_kube_config_contexts(config_file=str(args.kubeconfig))
    selected = next(item for item in contexts if item["name"] == args.context)
    if not selected["context"]["cluster"].startswith("kind-"):
        raise ValueError("Select an explicit disposable kind cluster")
    package, executable = installed_package()
    save(args.artifacts / "installed-package.json", package)
    manager = KubernetesClientManager(
        config_file=str(args.kubeconfig), context=args.context, in_cluster=False, client_configuration=None
    )
    failure = None
    with manager.client() as connection:
        fixture = Fixture(connection, args)
        try:
            fixture.create()
            for method in ["thrift", "connect"] if args.method == "both" else [args.method]:
                Scenario(args, fixture, executable, method).run()
        except BaseException as error:
            failure = error
        finally:
            fixture.snapshot(args.artifacts / "final")
            try:
                fixture.cleanup()
            except BaseException as error:
                if failure is None:
                    failure = error
                else:
                    failure.add_note(f"Fixture cleanup also failed; retain it for inspection: {error}")
            save(
                args.artifacts / "result.json",
                {"status": "failed" if failure else "passed", "error": str(failure) if failure else None},
            )
    if failure:
        raise failure


if __name__ == "__main__":
    main()
