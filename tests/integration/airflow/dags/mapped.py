"""Dynamically mapped SparkOnK8SOperator tasks in a real DAG.test run.

`mapped-deferred` runs two deferrable mapped instances. `mapped-reattach` makes map index 1
lose its first try right after submitting, as a lost worker would; Airflow clears that try's
XCom, so the retry must find and adopt the running driver by its task-key label. Like
`lifecycle.py`, this does not claim scheduler, executor or separate triggerer coverage.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime, timedelta
from importlib.metadata import version

from airflow.exceptions import AirflowException
from kubernetes import client as k8s, config
from spark_on_k8s.airflow.operators import SparkOnK8SOperator
from spark_on_k8s.client import ExecutorInstances, PodResources
from spark_on_k8s.utils.console_models import AirflowTaskContext

from airflow import DAG

CASE = os.environ["AIRFLOW_K8S_CASE"]
NAMESPACE = os.environ["AIRFLOW_K8S_NAMESPACE"]
SPARK_VERSION = os.environ["AIRFLOW_K8S_SPARK_VERSION"]
REATTACH = CASE == "mapped-reattach"
REQUEST_TIMEOUT = (5, 15)
TASK_KEY_LABEL = "spark-on-k8s.io/airflow-task-key"


class LoseFirstTryAfterSubmission(SparkOnK8SOperator):
    """Map index 1's first try submits its driver and then fails, as if its worker were lost."""

    def execute(self, context):
        ti = context["ti"]
        if REATTACH and ti.map_index == 1 and ti.try_number == 1:
            self.app_waiter = "no_wait"
            super().execute(context)
            raise AirflowException("Simulated loss of the worker after submission")
        return super().execute(context)


with DAG(
    dag_id="spark_kubernetes_mapped_tasks_" + CASE.replace("-", "_"),
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    default_args={"retries": 1 if REATTACH else 0, "retry_delay": timedelta(seconds=0)},
) as dag:
    LoseFirstTryAfterSubmission.partial(
        task_id="spark",
        namespace=NAMESPACE,
        image=os.environ["AIRFLOW_K8S_SPARK_IMAGE"],
        image_pull_policy="Never",
        app_path="local:///opt/spark/work-dir/airflow_job.py",
        service_account="spark",
        driver_resources=PodResources(cpu=1, memory=768, memory_overhead=384),
        executor_resources=PodResources(cpu=1, memory=768, memory_overhead=384),
        executor_instances=ExecutorInstances(initial=1),
        driver_labels={"airflow-integration-case": CASE},
        spark_conf={
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "2",
            "spark.kubernetes.executor.request.cores": "0.5",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_ROLE": "driver-property",
            "spark.executorEnv.AIRFLOW_SPARK_ROLE": "executor-property",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_DRIVER_ONLY": "driver-only",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_EMPTY": "",
            "spark.kubernetes.driverEnv.AIRFLOW_SPARK_NUMBER": 42,
        },
        app_waiter="wait",
        deferrable=not REATTACH,
        poll_interval=1,
        startup_timeout=120,
        # Covers a retry that waits for the whole four-minute slow driver.
        execution_timeout=timedelta(seconds=540),
        on_finish_action="keep",
        # Map index 0 can run its whole application first, and on a small node it waits for CPU until
        # map index 1's driver ends, so that driver may already have succeeded when its retry looks for
        # it. Adopting a succeeded driver too keeps the case on the label path in either order.
        reattach_succeeded=REATTACH,
    ).expand_kwargs(
        [
            # Distinct names: mapped instances submitted within one second must not share an ID.
            {"app_name": f"airflow-{CASE}-0", "app_arguments": [SPARK_VERSION, "success"]},
            # The adopted driver must still be running when its retry starts.
            {
                "app_name": f"airflow-{CASE}-1",
                "app_arguments": [SPARK_VERSION, "slow" if REATTACH else "success"],
            },
        ]
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
    assert run.state == "success", (CASE, run.state)
    task_instances = sorted(run.get_task_instances(), key=lambda ti: ti.map_index)
    assert [(ti.map_index, ti.state) for ti in task_instances] == [(0, "success"), (1, "success")], [
        (ti.map_index, ti.state) for ti in task_instances
    ]
    if REATTACH:
        assert [ti.try_number for ti in task_instances] == [1, 2], [ti.try_number for ti in task_instances]
    drivers = [pod for pod in observed.values() if (pod.metadata.labels or {}).get("spark-role") == "driver"]
    # One driver per mapped instance: a retry that resubmitted would leave a second one.
    assert len(drivers) == 2, f"Expected one driver per mapped instance: {[d.metadata.name for d in drivers]}"
    owners = {}
    for driver in drivers:
        owner = AirflowTaskContext.model_validate_json(
            driver.metadata.annotations["spark-on-k8s.io/airflow-task"]
        )
        assert driver.metadata.labels.get(TASK_KEY_LABEL) == owner.task_key(), "Missing task key"
        owners[owner.map_index] = (owner, driver)
    assert sorted(owners) == [0, 1], sorted(owners)
    assert owners[0][1].metadata.labels[TASK_KEY_LABEL] != owners[1][1].metadata.labels[TASK_KEY_LABEL]
    if REATTACH:
        # The retry adopted the driver its lost first try submitted.
        assert owners[1][0].try_number == 1, owners[1][0]
    final = {
        pod.metadata.uid: pod
        for pod in api.list_namespaced_pod(
            NAMESPACE, label_selector=selector + ",spark-role=driver", _request_timeout=REQUEST_TIMEOUT
        ).items
    }
    assert set(final) == {driver.metadata.uid for driver in drivers}, sorted(final)
    phases = {
        owners[index][1].metadata.uid: final[owners[index][1].metadata.uid].status.phase for index in owners
    }
    assert set(phases.values()) == {"Succeeded"}, phases
    app_ids = {pod.metadata.labels["spark-app-id"] for pod in final.values()}
    for pod in final.values():
        api.delete_namespaced_pod(pod.metadata.name, NAMESPACE, _request_timeout=REQUEST_TIMEOUT)
    cleanup_deadline = time.monotonic() + 90
    while True:
        leftovers = [
            item.metadata.name
            for reader in (
                api.list_namespaced_pod,
                api.list_namespaced_service,
                api.list_namespaced_secret,
                api.list_namespaced_config_map,
            )
            for item in reader(NAMESPACE, _request_timeout=REQUEST_TIMEOUT).items
            if (item.metadata.labels or {}).get("spark-app-id") in app_ids
            or any(ref.uid in final for ref in item.metadata.owner_references or [])
        ]
        if not leftovers:
            break
        if time.monotonic() >= cleanup_deadline:
            raise TimeoutError(f"Spark resources for {CASE} were not garbage collected: {leftovers}")
        time.sleep(1)
    print(
        "AIRFLOW_K8S_CASE_RESULT "
        + json.dumps(
            {
                "scenario": CASE,
                "airflow": version("apache-airflow"),
                "state": run.state,
                "task_states": [ti.state for ti in task_instances],
                "try_numbers": [ti.try_number for ti in task_instances],
                "drivers": {str(index): owners[index][1].metadata.name for index in owners},
                "resources_garbage_collected": True,
                "elapsed_seconds": round(elapsed, 2),
                "passed": True,
                "execution": "dag.test in Kubernetes pod; mapped tasks, no scheduler",
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
