from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from uuid import uuid4

from kubernetes import client as k8s, config
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

REQUEST_TIMEOUT = (5, 20)


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
                            logs = api.read_namespaced_pod_log(
                                pod.metadata.name,
                                namespace,
                                container=container.name,
                                _request_timeout=REQUEST_TIMEOUT,
                            )
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
    submit = submitter.submit_sql if scenario.startswith("sql") else submitter.submit_app
    pod_name = submit(
        image=args.image,
        namespace=namespace,
        service_account="spark",
        app_name=f"compat-{scenario}",
        app_id_suffix=lambda: "-" + uuid4().hex[:8],
        image_pull_policy="Never",
        app_waiter="no_wait",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=384),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=384),
        executor_instances=ExecutorInstances(initial=1),
        spark_conf={
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            "spark.ui.enabled": "false",
            "spark.kubernetes.executor.request.cores": "0.5",
            # Retain finished executors until driver deletion so ownership/GC is observable.
            "spark.kubernetes.executor.deleteOnTermination": "false",
        },
        **options,
    )
    pod = await_completion(api, namespace, pod_name, args.timeout)
    logs = api.read_namespaced_pod_log(
        pod_name, namespace, container="driver", _request_timeout=REQUEST_TIMEOUT
    )
    (args.artifacts / f"{scenario}.log").write_text(logs)
    executors = owned_executors(api, namespace, pod.metadata.uid)
    if scenario == "sql-failure":
        assert pod.status.phase == "Failed", f"SQL failure incorrectly succeeded: {pod_name}"
        assert "UNRESOLVED_COLUMN" in logs and "missing_column" in logs, f"Unexpected failure: {logs[-4000:]}"
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
    dump_diagnostics(api, namespace, args.artifacts / scenario)
    submitter.app_manager.delete_app(namespace=namespace, pod_name=pod_name)
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
            results = [
                run_case(api, submitter, args, namespace, scenario)
                for scenario in ("python", "sql", "sql-failure", "java")
            ]
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
