from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from kubernetes import client as k8s, config
from spark_on_k8s.cli.receipts import read_receipt, receipt_writer
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

REQUEST_TIMEOUT = (5, 20)


def read_logs(api, namespace, pod_name, **kwargs):
    # Read bytes directly: the Kubernetes client's str deserializer can stringify
    # a byte body as b'...\\n...', losing actual line boundaries in the receipt stream.
    response = api.read_namespaced_pod_log(
        pod_name, namespace, _preload_content=False, _request_timeout=REQUEST_TIMEOUT, **kwargs
    )
    try:
        return response.data.decode("utf-8", errors="replace")
    finally:
        response.close()
        response.release_conn()


def owned_executors(api, namespace: str, driver_uid: str):
    return [
        pod
        for pod in api.list_namespaced_pod(
            namespace, label_selector="spark-role=executor", _request_timeout=REQUEST_TIMEOUT
        ).items
        if any(owner.uid == driver_uid for owner in pod.metadata.owner_references or [])
    ]


def dump_diagnostics(api, namespace: str, destination: Path):
    destination.mkdir(parents=True, exist_ok=True)
    for resource, reader in (("pods", api.list_namespaced_pod), ("events", api.list_namespaced_event)):
        try:
            objects = reader(namespace, _request_timeout=REQUEST_TIMEOUT)
            (destination / f"{resource}.json").write_text(
                json.dumps(api.api_client.sanitize_for_serialization(objects), indent=2)
            )
            if resource == "pods":
                for pod in objects.items:
                    for container in pod.spec.containers:
                        try:
                            logs = read_logs(api, namespace, pod.metadata.name, container=container.name)
                        except k8s.ApiException as error:
                            logs = f"Logs unavailable: {error.status}"
                        (destination / f"{pod.metadata.name}-{container.name}.log").write_text(logs)
        except Exception as error:
            (destination / f"{resource}-error.txt").write_text(str(error))


def await_completion(api, namespace: str, pod_name: str, timeout: int):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pod = api.read_namespaced_pod(pod_name, namespace, _request_timeout=REQUEST_TIMEOUT)
        if pod.status.phase in {"Succeeded", "Failed"}:
            return pod
        time.sleep(2)
    raise TimeoutError(f"Driver {pod_name} did not finish within {timeout}s")


def await_cleanup(api, namespace: str, pod_name: str, app_id: str, owner_uids: set[str]):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        remaining = []
        for read in (
            api.list_namespaced_pod,
            api.list_namespaced_service,
            api.list_namespaced_secret,
            api.list_namespaced_config_map,
        ):
            remaining.extend(
                resource
                for resource in read(namespace, _request_timeout=REQUEST_TIMEOUT).items
                if (resource.metadata.labels or {}).get("spark-app-id") == app_id
                or any(owner.uid in owner_uids for owner in resource.metadata.owner_references or [])
            )
        if not remaining:
            return
        time.sleep(2)
    raise TimeoutError(f"Resources for {pod_name} were not garbage collected")


def run_case(api, submitter: SparkOnK8S, args, namespace: str, scenario: str):
    fixtures = Path(__file__).parent
    options = {}
    spark_conf = {
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.ui.enabled": "false",
        "spark.kubernetes.executor.request.cores": "0.5",
        # Retain finished executors until driver deletion so ownership/GC is observable.
        "spark.kubernetes.executor.deleteOnTermination": "false",
    }
    marker = f"SPARK_ON_K8S_{scenario.upper()}_OK"
    if scenario.startswith("sql"):
        queries = json.loads(
            (fixtures / f"{'sql_failure' if scenario == 'sql-failure' else 'sql_success'}.json").read_text()
        )
        if scenario == "sql":
            queries.insert(0, f"SELECT assert_true(version() LIKE '{args.spark_version}%')")
        options["sql"] = ";\n".join(queries) + ";"
    elif scenario == "java":
        options.update(
            app_path="local:///opt/spark/work-dir/integration.jar",
            class_name="IntegrationJob",
            app_arguments=[args.spark_version],
        )
    else:
        options.update(
            app_path="local:///opt/spark/work-dir/runtime_job.py",
            app_arguments=[args.spark_version],
            secret_values={"SPARK_COMPAT_SENTINEL": "present"},
        )
        spark_conf.update(
            {
                "spark.kubernetes.driverEnv.SPARK_COMPAT_ROLE": "driver-property",
                "spark.executorEnv.SPARK_COMPAT_ROLE": "executor-property",
                "spark.kubernetes.driverEnv.SPARK_COMPAT_DRIVER_ONLY": "driver-only",
                "spark.kubernetes.driverEnv.SPARK_COMPAT_EMPTY": "",
                "spark.kubernetes.driverEnv.SPARK_COMPAT_NUMBER": 42,
            }
        )
    submit = submitter.submit_sql if scenario.startswith("sql") else submitter.submit_app
    # The Java run uses a 63-character app ID, so its driver name is shortened with a hash.
    app_name = "compat-java-with-a-long-name-that-requires-a-hashed-driver" if scenario == "java" else None
    pod_name = submit(
        image=args.image,
        namespace=namespace,
        service_account="spark",
        app_name=app_name or f"compat-{scenario}",
        app_id_suffix=lambda: "-" + uuid4().hex[:8],
        image_pull_policy="Never",
        app_waiter="no_wait",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=384),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=384),
        executor_instances=ExecutorInstances(initial=1),
        spark_conf=spark_conf,
        **options,
    )
    run = submitter.last_submitted_run
    assert run is not None and run.driver_uid, "The installed client must return a UID-bearing receipt"
    if app_name:
        assert len(run.app_id) == 63 and run.driver_pod == pod_name, run
        assert len(pod_name) <= 63 and pod_name != f"{run.app_id}-driver", pod_name
    receipt_path = args.artifacts / f"{scenario}-receipt.json"
    with receipt_writer(receipt_path) as save:
        save(run)
    assert read_receipt(receipt_path) == run
    # A different process/client can resume observation using the durable identity.
    observer = SparkOnK8S(k8s_client_manager=submitter.k8s_client_manager)
    observer.app_manager.wait_for_app(
        namespace=run.namespace, pod_name=run.driver_pod, timeout=args.timeout, poll_interval=2
    )
    pod = api.read_namespaced_pod(pod_name, namespace, _request_timeout=REQUEST_TIMEOUT)
    assert pod.metadata.uid == run.driver_uid
    assert observer.app_manager.app_status(namespace=namespace, pod_name=pod_name).value == pod.status.phase
    logs = read_logs(api, namespace, pod_name, container="driver")
    (args.artifacts / f"{scenario}.log").write_text(logs)
    executors = owned_executors(api, namespace, pod.metadata.uid)
    if scenario == "sql-failure":
        assert pod.status.phase == "Failed", f"SQL failure incorrectly succeeded: {pod_name}"
        assert "UNRESOLVED_COLUMN" in logs and "SQL statement" in logs, f"Unexpected failure: {logs[-4000:]}"
        assert "SHOULD_NOT_RUN" not in logs, "SQL runner continued after the first failed statement"
    else:
        assert pod.status.phase == "Succeeded", f"{scenario} failed: {logs[-4000:]}"
        assert marker in logs, f"Expected actual execution marker {marker}"
        assert executors, f"{scenario} did not create an owned executor pod"
    diagnosis = submitter.diagnose_app(namespace=namespace, pod_name=pod_name)
    (args.artifacts / f"{scenario}-diagnosis.json").write_text(json.dumps(diagnosis.to_dict(), indent=2))
    assert diagnosis.app_id == submitter.last_submitted_app_id
    assert diagnosis.driver_pod == pod_name and diagnosis.namespace == namespace
    assert not diagnosis.incomplete, f"Incomplete real-cluster diagnosis: {diagnosis.to_dict()}"
    assert diagnosis.inspected_pods == 1 + len(executors), (
        "Diagnostic label selection must include every retained executor owned by the real driver"
    )
    if scenario == "sql-failure":
        assert any(
            finding.code == "container_failed" and finding.resource == pod_name
            for finding in diagnosis.findings
        ), f"Missing failed driver evidence: {diagnosis.to_dict()}"
    result = {
        "scenario": scenario,
        "phase": pod.status.phase,
        "driver": pod_name,
        "executors": len(executors),
        "diagnosed_pods": diagnosis.inspected_pods,
    }
    if scenario == "python":
        records = [
            json.loads(line.removeprefix(marker + " "))
            for line in logs.splitlines()
            if line.startswith(marker + " ")
        ]
        assert len(records) == 1 and records[0]["environment"], "Missing Python environment evidence"
        environment = records[0]["environment"]
        driver_container = next(container for container in pod.spec.containers if container.name == "driver")
        # The Kubernetes API may omit value="" while retaining the environment entry.
        driver_env = {item.name: item.value or "" for item in driver_container.env or []}
        assert {name: driver_env.get(name) for name in environment["driver"]} == environment["driver"]
        for executor in executors:
            executor_container = next(
                container
                for container in executor.spec.containers
                if container.name == "spark-kubernetes-executor"
            )
            executor_env = {item.name: item.value or "" for item in executor_container.env or []}
            assert {name: executor_env.get(name) for name in environment["driver"]} == environment[
                "executors"
            ][0]
        result["environment"] = environment
    dump_diagnostics(api, namespace, args.artifacts / scenario)
    try:
        observer.cancel_run(replace(run, driver_uid="stale-receipt-uid"))
    except ValueError:
        pass
    else:
        raise AssertionError("A stale receipt must not cancel a live driver")
    assert (
        api.read_namespaced_pod(pod_name, namespace, _request_timeout=REQUEST_TIMEOUT).metadata.uid
        == run.driver_uid
    )
    observer.cancel_run(read_receipt(receipt_path))
    await_cleanup(
        api,
        namespace,
        pod_name,
        submitter.last_submitted_app_id,
        {pod.metadata.uid, *(executor.metadata.uid for executor in executors)},
    )
    result["garbage_collected"] = True
    print(json.dumps(result), flush=True)
    return result


def run_pending_case(api, submitter, args, namespace):
    """A real unschedulable driver must time out without cancellation or a duplicate submission."""
    pod_name = submitter.submit_app(
        image=args.image,
        namespace=namespace,
        service_account="spark",
        app_name="pending",
        app_id_suffix=lambda: "-" + uuid4().hex[:8],
        image_pull_policy="Never",
        app_waiter="no_wait",
        app_path="local:///opt/spark/work-dir/runtime_job.py",
        app_arguments=[args.spark_version],
        driver_node_selector={"spark-on-k8s.test/unschedulable": uuid4().hex},
        executor_instances=ExecutorInstances(initial=1),
    )
    run = submitter.last_submitted_run
    start = time.monotonic()
    try:
        submitter.app_manager.wait_for_app(
            namespace=namespace, pod_name=pod_name, timeout=3, poll_interval=0.5
        )
    except TimeoutError:
        pass
    else:
        raise AssertionError("Unschedulable driver did not time out")
    elapsed = time.monotonic() - start
    assert elapsed < 15, f"Observation deadline exceeded its bounded tolerance: {elapsed}"
    pod = api.read_namespaced_pod(pod_name, namespace, _request_timeout=REQUEST_TIMEOUT)
    assert pod.metadata.uid == run.driver_uid and pod.status.phase == "Pending"
    assert (
        len(
            api.list_namespaced_pod(
                namespace,
                label_selector=f"spark-app-id={run.app_id},spark-role=driver",
                _request_timeout=REQUEST_TIMEOUT,
            ).items
        )
        == 1
    )
    report = submitter.diagnose_app(namespace=namespace, pod_name=pod_name)
    assert any(item.code in {"unschedulable", "cause_unknown"} for item in report.findings)
    dump_diagnostics(api, namespace, args.artifacts / "pending")
    submitter.cancel_run(run)
    await_cleanup(api, namespace, pod_name, run.app_id, {run.driver_uid})
    return {
        "scenario": "pending-timeout",
        "elapsed_seconds": elapsed,
        "driver_preserved_until_cancel": True,
        "garbage_collected": True,
    }


def run_rejected_case(api, submitter, args, namespace):
    """A definitively rejected driver must not leave the Secret and ConfigMap created before it."""
    suffix = "-" + uuid4().hex[:8]
    app_id = "rejected" + suffix
    status = None
    try:
        submitter.submit_app(
            image=args.image,
            namespace=namespace,
            service_account="spark",
            app_name="rejected",
            app_id_suffix=lambda: suffix,
            image_pull_policy="Never",
            app_waiter="no_wait",
            app_path="local:///opt/spark/work-dir/runtime_job.py",
            app_arguments=[args.spark_version],
            secret_values={"SPARK_COMPAT_SENTINEL": "present"},
            driver_ephemeral_configmaps_volumes=[
                {"mount_path": "/etc/spark-compat", "sources": [{"name": "marker", "text": "present"}]}
            ],
            # Priority admission rejects an unknown class after the Secret and ConfigMap exist.
            driver_pod_template={
                "apiVersion": "v1",
                "kind": "Pod",
                "spec": {
                    "priorityClassName": "spark-on-k8s-missing-" + uuid4().hex[:8],
                    "containers": [{"name": "driver"}],
                },
            },
            executor_instances=ExecutorInstances(initial=1),
        )
    except k8s.ApiException as error:
        status = error.status
    assert status is not None and 400 <= status < 500 and status != 408, f"Unexpected outcome: {status}"
    assert submitter.last_submitted_run is None
    assert submitter.last_submission_phase == "creating_resources"
    await_cleanup(api, namespace, app_id + "-driver", app_id, set())
    return {"scenario": "rejected-driver", "status": status, "unowned_resources_removed": True}


DEADLINE_JOB = """\
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# Keep the driver and one executor busy well past the driver's max_runtime.
spark.sparkContext.parallelize([1], 1).map(lambda _: time.sleep(3600)).collect()
"""


def run_deadline_case(api, submitter, args, namespace):
    """A driver past max_runtime must fail with DeadlineExceeded, release executors and be prunable."""
    max_runtime = 75
    submitter.submit_app(
        image=args.image,
        namespace=namespace,
        service_account="spark",
        app_name="deadline",
        app_id_suffix=lambda: "-" + uuid4().hex[:8],
        image_pull_policy="Never",
        app_waiter="no_wait",
        app_path="local:///opt/spark-deadline/job.py",
        driver_ephemeral_configmaps_volumes=[
            {"mount_path": "/opt/spark-deadline", "sources": [{"name": "job.py", "text": DEADLINE_JOB}]}
        ],
        secret_values={"SPARK_COMPAT_SENTINEL": "present"},
        spark_conf={"spark.ui.enabled": "false", "spark.kubernetes.executor.request.cores": "0.5"},
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=384),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=384),
        executor_instances=ExecutorInstances(initial=1),
        max_runtime=max_runtime,
    )
    run = submitter.last_submitted_run
    executors = set()
    limit = time.monotonic() + max_runtime + 120
    while True:
        pod = api.read_namespaced_pod(run.driver_pod, namespace, _request_timeout=REQUEST_TIMEOUT)
        executors.update(item.metadata.uid for item in owned_executors(api, namespace, run.driver_uid))
        if pod.status.phase in {"Succeeded", "Failed"}:
            break
        assert time.monotonic() < limit, f"Driver {run.driver_pod} outlived its deadline: {pod.status.phase}"
        time.sleep(2)
    dump_diagnostics(api, namespace, args.artifacts / "deadline-exceeded")
    assert pod.metadata.uid == run.driver_uid
    assert pod.spec.active_deadline_seconds == max_runtime
    assert (pod.status.phase, pod.status.reason) == ("Failed", "DeadlineExceeded"), pod.status
    assert executors, "The driver did not start an executor before its deadline"
    finished = [
        status.state.terminated.finished_at
        for status in pod.status.container_statuses or []
        if status.state and status.state.terminated and status.state.terminated.finished_at
    ]
    lifetime = (max(finished) - pod.status.start_time).total_seconds() if finished else None
    # Timestamps have one-second resolution; Kubernetes never stops the pod before the deadline.
    assert lifetime is None or lifetime >= max_runtime - 1, f"Stopped after {lifetime}s"
    report = submitter.diagnose_app(namespace=namespace, pod_name=run.driver_pod)
    assert any(finding.code == "deadline_exceeded" for finding in report.findings), report.to_dict()
    # The stopped driver no longer runs executors, although its own objects remain until deletion.
    release = time.monotonic() + 90
    active = {"Pending", "Running"}
    while any(item.status.phase in active for item in owned_executors(api, namespace, run.driver_uid)):
        assert time.monotonic() < release, "Executors kept running after the driver deadline"
        time.sleep(2)
    remaining_executors = len(owned_executors(api, namespace, run.driver_uid))
    # The installed CLI selects the terminal driver, deletes nothing without --delete, then deletes it.
    env = {
        **os.environ,
        "SPARK_ON_K8S_CONFIG_FILE": args.kubeconfig,
        "SPARK_ON_K8S_CONTEXT": args.context,
        "SPARK_ON_K8S_IN_CLUSTER": "false",
    }
    command = [str(Path(sys.executable).with_name("spark-on-k8s")), "apps", "prune", "-n", namespace]
    outcomes = {}
    for mode, extra in (("preview", []), ("delete", ["--delete"])):
        completed = subprocess.run(
            [*command, *extra, "--output", "json"], env=env, capture_output=True, text=True, timeout=120
        )
        (args.artifacts / f"deadline-prune-{mode}.json").write_text(completed.stdout)
        assert completed.returncode == 0, completed.stderr[-4000:]
        document = json.loads(completed.stdout)
        entry = next(item for item in document["items"] if item["driver_uid"] == run.driver_uid)
        assert entry["reason"] == "DeadlineExceeded", entry
        outcomes[mode] = entry["action"]
        if mode == "preview":
            current = api.read_namespaced_pod(run.driver_pod, namespace, _request_timeout=REQUEST_TIMEOUT)
            assert current.metadata.uid == run.driver_uid, "A prune preview must not delete the driver"
    assert outcomes == {"preview": "would_delete", "delete": "deletion_requested"}, outcomes
    await_cleanup(api, namespace, run.driver_pod, run.app_id, {run.driver_uid, *executors})
    return {
        "scenario": "deadline-exceeded",
        "max_runtime": max_runtime,
        "driver_lifetime_seconds": lifetime,
        "executors_started": len(executors),
        "executor_pods_retained_after_deadline": remaining_executors,
        "prune": outcomes,
        "garbage_collected": True,
    }


def main():
    parser = argparse.ArgumentParser(description="Run real Spark submissions on a disposable kind cluster")
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", default="kind-spark-compat")
    parser.add_argument("--image", required=True)
    parser.add_argument("--spark-version", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=240)
    args = parser.parse_args()
    assert re.fullmatch(r"\d+\.\d+\.\d+", args.spark_version), "Use an explicit Spark release"
    contexts, _ = config.list_kube_config_contexts(config_file=args.kubeconfig)
    selected = next(context for context in contexts if context["name"] == args.context)
    assert selected["context"]["cluster"].startswith("kind-"), (
        "This test only supports disposable kind clusters"
    )
    args.artifacts.mkdir(parents=True, exist_ok=True)
    manager = KubernetesClientManager(config_file=args.kubeconfig, context=args.context, in_cluster=False)
    namespace = "spark-compat-" + uuid4().hex[:8]
    with manager.client() as client:
        api = k8s.CoreV1Api(client)
        api.create_namespace(
            k8s.V1Namespace(metadata=k8s.V1ObjectMeta(name=namespace)), _request_timeout=REQUEST_TIMEOUT
        )
        try:
            api.create_namespaced_service_account(
                namespace,
                k8s.V1ServiceAccount(metadata=k8s.V1ObjectMeta(name="spark")),
                _request_timeout=REQUEST_TIMEOUT,
            )
            k8s.RbacAuthorizationV1Api(client).create_namespaced_role_binding(
                namespace,
                k8s.V1RoleBinding(
                    metadata=k8s.V1ObjectMeta(name="spark"),
                    role_ref=k8s.V1RoleRef(
                        api_group="rbac.authorization.k8s.io", kind="ClusterRole", name="edit"
                    ),
                    subjects=[k8s.RbacV1Subject(kind="ServiceAccount", name="spark", namespace=namespace)],
                ),
                _request_timeout=REQUEST_TIMEOUT,
            )
            submitter = SparkOnK8S(k8s_client_manager=manager)
            results = []
            special_cases = {
                "pending-timeout": run_pending_case,
                "rejected-driver": run_rejected_case,
                "deadline-exceeded": run_deadline_case,
            }
            for scenario in (
                "python",
                "sql",
                "sql-failure",
                "java",
                "pending-timeout",
                "rejected-driver",
                "deadline-exceeded",
            ):
                try:
                    result = (
                        special_cases[scenario](api, submitter, args, namespace)
                        if scenario in special_cases
                        else run_case(api, submitter, args, namespace, scenario)
                    )
                    results.append({**result, "test_status": "passed"})
                except Exception as error:
                    results.append(
                        {"scenario": scenario, "test_status": "failed", "error_type": type(error).__name__}
                    )
                    raise
                finally:
                    (args.artifacts / "result.json").write_text(
                        json.dumps(
                            {
                                "spark": args.spark_version,
                                "image": args.image,
                                "mode": "kubernetes",
                                "results": results,
                            },
                            indent=2,
                        )
                        + "\n"
                    )

        finally:
            dump_diagnostics(api, namespace, args.artifacts / "final")
            api.delete_namespace(namespace, _request_timeout=REQUEST_TIMEOUT)


if __name__ == "__main__":
    main()
