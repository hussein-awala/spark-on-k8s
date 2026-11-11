"""A real DAG.test run, including Airflow's inline trigger execution and resumption.

This deliberately does not claim scheduler, executor or separate triggerer coverage.
The module is also parsed by Airflow 3's local DAG bundle loader.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime, timedelta
from importlib.metadata import version

from kubernetes import client as k8s, config
from spark_on_k8s.airflow.operators import SparkOnK8SOperator
from spark_on_k8s.client import ExecutorInstances, PodResources
from spark_on_k8s.utils.console_models import AirflowTaskContext

from airflow import DAG

CASE = os.environ["AIRFLOW_K8S_CASE"]
NAMESPACE = os.environ["AIRFLOW_K8S_NAMESPACE"]
MODE, OUTCOME = CASE.split("-", 1)
TIMEOUT = 15
REQUEST_TIMEOUT = (5, 15)

with DAG(
    dag_id="spark_kubernetes_" + CASE.replace("-", "_"),
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    SparkOnK8SOperator(
        task_id="spark",
        namespace=NAMESPACE,
        image=os.environ["AIRFLOW_K8S_SPARK_IMAGE"],
        image_pull_policy="Never",
        app_path="local:///opt/spark/work-dir/airflow_job.py",
        app_name="airflow-" + CASE,
        app_id_suffix="-integration",
        app_arguments=[os.environ["AIRFLOW_K8S_SPARK_VERSION"], OUTCOME],
        service_account="spark",
        driver_resources=PodResources(cpu=1, memory=768, memory_overhead=384),
        executor_resources=PodResources(cpu=1, memory=768, memory_overhead=384),
        executor_instances=ExecutorInstances(initial=1),
        driver_labels={"airflow-integration-case": CASE},
        driver_node_selector={"spark-on-k8s.invalid/unschedulable": "true"} if OUTCOME == "timeout" else None,
        spark_conf={
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "2",
            "spark.kubernetes.executor.request.cores": "0.5",
            "spark.kubernetes.executor.deleteOnTermination": "false",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_ROLE": "driver-property",
            "spark.executorEnv.AIRFLOW_SPARK_ROLE": "executor-property",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_DRIVER_ONLY": "driver-only",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_EMPTY": "",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_NUMBER": 42,
        },
        app_waiter="wait",
        deferrable=MODE == "deferred",
        poll_interval=1,
        startup_timeout=TIMEOUT if OUTCOME == "timeout" else 120,
        execution_timeout=timedelta(seconds=300),
        on_finish_action="keep",
    )


def main():
    config.load_incluster_config()
    api = k8s.CoreV1Api()
    selector = "airflow-integration-case=" + CASE
    observed = {}
    stop = threading.Event()
    observer_errors = []

    def observe():
        while not stop.is_set():
            try:
                pods = api.list_namespaced_pod(
                    NAMESPACE, label_selector=selector, _request_timeout=REQUEST_TIMEOUT
                )
                for pod in pods.items:
                    observed[pod.metadata.uid] = pod
            except Exception as error:
                observer_errors.append(type(error).__name__)
            stop.wait(0.5)

    observer = threading.Thread(target=observe, daemon=True)
    observer.start()
    started = time.monotonic()
    try:
        run = dag.test()
    finally:
        elapsed = time.monotonic() - started
        stop.set()
        observer.join(timeout=20)
    expected = "success" if OUTCOME == "success" else "failed"
    assert run.state == expected, (CASE, run.state, expected)
    task_instances = run.get_task_instances()
    assert len(task_instances) == 1 and task_instances[0].state == expected, [
        ti.state for ti in task_instances
    ]
    assert observed, f"No real driver observed; observer errors: {observer_errors}"
    assert len(observed) == 1, f"Unexpected duplicate drivers: {list(observed)}"
    driver = next(iter(observed.values()))
    assert driver.spec.service_account_name == "spark"
    owner = driver.metadata.annotations.get("spark-on-k8s.io/airflow-task")
    assert owner, "Missing real task attribution"
    # Retries select earlier attempts' drivers by this label; the operator listed it before submitting.
    task_key = AirflowTaskContext.model_validate_json(owner).task_key()
    assert driver.metadata.labels.get("spark-on-k8s.io/airflow-task-key") == task_key, "Missing task key"
    driver_container = next(container for container in driver.spec.containers if container.name == "driver")
    # The Kubernetes API may omit value="" while retaining the environment entry.
    driver_env = {item.name: item.value or "" for item in driver_container.env or []}
    expected_driver_env = {
        "AIRFLOW_SPARK_ROLE": "driver-property",
        "AIRFLOW_SPARK_DRIVER_ONLY": "driver-only",
        "AIRFLOW_SPARK_EMPTY": "",
        "AIRFLOW_SPARK_NUMBER": "42",
    }
    assert {name: driver_env.get(name) for name in expected_driver_env} == expected_driver_env
    remaining = api.list_namespaced_pod(
        NAMESPACE, label_selector=selector, _request_timeout=REQUEST_TIMEOUT
    ).items
    executor_count = 0
    environment = None
    if OUTCOME == "timeout":
        assert elapsed < TIMEOUT + 60, f"Unbounded startup timeout: {elapsed}"
        assert driver.status.phase == "Pending", driver.status.phase
        assert any(condition.reason == "Unschedulable" for condition in driver.status.conditions or []), (
            "Driver did not reach the intended unschedulable condition"
        )
        # Timeout cleanup is part of the operator's public lifecycle, even with keep.
        deadline = time.monotonic() + 30
        while remaining and time.monotonic() < deadline:
            time.sleep(1)
            remaining = api.list_namespaced_pod(
                NAMESPACE, label_selector=selector, _request_timeout=REQUEST_TIMEOUT
            ).items
        assert not remaining, "Operator did not delete the startup-timeout driver"
    else:
        assert len(remaining) == 1
        driver = remaining[0]
        expected_phase = "Succeeded" if OUTCOME == "success" else "Failed"
        assert driver.status.phase == expected_phase, driver.status.phase
        response = api.read_namespaced_pod_log(
            driver.metadata.name,
            NAMESPACE,
            container="driver",
            _request_timeout=REQUEST_TIMEOUT,
            _preload_content=False,
        )
        try:
            logs = response.data.decode("utf-8", errors="replace")
        finally:
            response.close()
            response.release_conn()
        print("AIRFLOW_SPARK_LOG_BEGIN " + CASE, flush=True)
        print(logs, flush=True)
        print("AIRFLOW_SPARK_LOG_END " + CASE, flush=True)
        assert "AIRFLOW_SPARK_EXECUTORS_OK" in logs, "No actual executor work"
        environment_prefix = "AIRFLOW_SPARK_ENVIRONMENT_OK "
        environment_records = [
            json.loads(line.removeprefix(environment_prefix))
            for line in logs.splitlines()
            if line.startswith(environment_prefix)
        ]
        assert len(environment_records) == 1, "Missing actual driver/executor environment checks"
        environment = environment_records[0]
        assert environment["driver"] == expected_driver_env
        marker = "AIRFLOW_SPARK_SUCCESS" if OUTCOME == "success" else "AIRFLOW_SPARK_EXPECTED_FAILURE"
        assert marker in logs, f"Missing {marker}"
        if OUTCOME == "failure":
            assert "AIRFLOW_SPARK_SUCCESS" not in logs
        executors = [
            pod
            for pod in api.list_namespaced_pod(
                NAMESPACE, label_selector="spark-role=executor", _request_timeout=REQUEST_TIMEOUT
            ).items
            if any(owner.uid == driver.metadata.uid for owner in pod.metadata.owner_references or [])
        ]
        executor_count = len(executors)
        assert executor_count > 0
        for executor in executors:
            executor_container = next(
                container
                for container in executor.spec.containers
                if container.name == "spark-kubernetes-executor"
            )
            executor_env = {item.name: item.value or "" for item in executor_container.env or []}
            assert {name: executor_env.get(name) for name in expected_driver_env} == environment["executors"][
                0
            ]
    if remaining:
        api.delete_namespaced_pod(driver.metadata.name, NAMESPACE, _request_timeout=REQUEST_TIMEOUT)
    app_id = driver.metadata.labels["spark-app-id"]
    cleanup_deadline = time.monotonic() + 90
    while True:
        leftovers = []
        for reader in (
            api.list_namespaced_pod,
            api.list_namespaced_service,
            api.list_namespaced_secret,
            api.list_namespaced_config_map,
        ):
            leftovers.extend(
                item
                for item in reader(NAMESPACE, _request_timeout=REQUEST_TIMEOUT).items
                if (item.metadata.labels or {}).get("spark-app-id") == app_id
                or any(owner.uid == driver.metadata.uid for owner in item.metadata.owner_references or [])
            )
        if not leftovers:
            break
        if time.monotonic() >= cleanup_deadline:
            raise TimeoutError(f"Spark resources for {CASE} were not garbage collected")
        time.sleep(1)
    print(
        "AIRFLOW_K8S_CASE_RESULT "
        + json.dumps(
            {
                "scenario": CASE,
                "airflow": version("apache-airflow"),
                "state": run.state,
                "task_state": task_instances[0].state,
                "driver_uid": driver.metadata.uid,
                "driver_phase": driver.status.phase,
                "driver_conditions": [
                    {"type": condition.type, "status": condition.status, "reason": condition.reason}
                    for condition in driver.status.conditions or []
                ],
                "resources_garbage_collected": True,
                "executors": executor_count,
                "driver_pod_environment": expected_driver_env,
                "runtime_environment": environment,
                "elapsed_seconds": round(elapsed, 2),
                "passed": True,
                "execution": "dag.test in Kubernetes pod; real inline trigger, no scheduler",
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
