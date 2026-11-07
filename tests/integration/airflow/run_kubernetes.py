"""Run Airflow's DAG test engine inside a disposable kind namespace."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from uuid import uuid4

from kubernetes import client as k8s, config

REQUEST_TIMEOUT = (5, 20)
PREFIX = "AIRFLOW_K8S_CASE_RESULT "
CASES = {
    "sync-success",
    "sync-failure",
    "deferred-success",
    "deferred-failure",
    "sync-timeout",
    "deferred-timeout",
}


def diagnostics(api, namespace, destination):
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", default="kind-airflow-compat")
    parser.add_argument("--airflow-image", required=True)
    parser.add_argument("--airflow-version", required=True, choices=["2.11.2", "3.4.0"])
    parser.add_argument("--spark-image", required=True)
    parser.add_argument("--spark-version", default="4.0.5")
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=1500)
    args = parser.parse_args()
    contexts, _ = config.list_kube_config_contexts(config_file=args.kubeconfig)
    selected = next(context for context in contexts if context["name"] == args.context)
    assert args.context.startswith("kind-") and selected["context"]["cluster"].startswith("kind-"), (
        "Only a disposable kind context is supported"
    )
    config.load_kube_config(config_file=args.kubeconfig, context=args.context)
    args.artifacts.mkdir(parents=True, exist_ok=True)
    summary = {
        "airflow": args.airflow_version,
        "spark": args.spark_version,
        "execution": "Airflow DAG.test in a Kubernetes pod with service account",
        "scheduler_coverage": False,
        "mocked_triggers": False,
        "results": [],
        "passed": False,
    }
    output = args.artifacts / "result.json"
    output.write_text(json.dumps(summary, indent=2) + "\n")
    namespace = "airflow-compat-" + uuid4().hex[:8]
    with k8s.ApiClient() as client:
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
            api.create_namespaced_pod(
                namespace,
                k8s.V1Pod(
                    metadata=k8s.V1ObjectMeta(name="airflow-runner"),
                    spec=k8s.V1PodSpec(
                        restart_policy="Never",
                        service_account_name="spark",
                        active_deadline_seconds=args.timeout,
                        containers=[
                            k8s.V1Container(
                                name="airflow",
                                image=args.airflow_image,
                                image_pull_policy="Never",
                                resources=k8s.V1ResourceRequirements(
                                    requests={"cpu": "250m", "memory": "512Mi"},
                                    limits={"cpu": "2", "memory": "1536Mi"},
                                ),
                                env=[
                                    k8s.V1EnvVar(name=name, value=value)
                                    for name, value in {
                                        "AIRFLOW_K8S_NAMESPACE": namespace,
                                        "AIRFLOW_K8S_SPARK_IMAGE": args.spark_image,
                                        "AIRFLOW_K8S_SPARK_VERSION": args.spark_version,
                                        "AIRFLOW_CONN_KUBERNETES_DEFAULT": json.dumps(
                                            {"conn_type": "kubernetes", "extra": {"in_cluster": True}}
                                        ),
                                    }.items()
                                ],
                            )
                        ],
                    ),
                ),
                _request_timeout=REQUEST_TIMEOUT,
            )
            deadline = time.monotonic() + args.timeout + 30
            while time.monotonic() < deadline:
                pod = api.read_namespaced_pod("airflow-runner", namespace, _request_timeout=REQUEST_TIMEOUT)
                if pod.status.phase in {"Succeeded", "Failed"}:
                    break
                time.sleep(3)
            else:
                raise TimeoutError("Airflow integration pod exceeded its deadline")
            logs = api.read_namespaced_pod_log("airflow-runner", namespace, _request_timeout=REQUEST_TIMEOUT)
            (args.artifacts / "airflow.log").write_text(logs)
            for line in logs.splitlines():
                if line.startswith(PREFIX):
                    summary["results"].append(json.loads(line.removeprefix(PREFIX)))
                    output.write_text(json.dumps(summary, indent=2) + "\n")
            assert pod.status.phase == "Succeeded", "Airflow runner failed; inspect airflow.log"
            results = summary["results"]
            assert len(results) == len(CASES) and {item["scenario"] for item in results} == CASES, results
            assert all(item["passed"] and item["airflow"] == args.airflow_version for item in results), (
                results
            )
            summary["passed"] = True
            print(json.dumps(summary), flush=True)
        except Exception as error:
            summary["error"] = {"type": type(error).__name__, "message": str(error)}
            raise
        finally:
            # Partial cases remain available in logs even when the runner deadline expires.
            diagnostics(api, namespace, args.artifacts / "cluster")
            runner_log = args.artifacts / "cluster" / "airflow-runner-airflow.log"
            if runner_log.exists() and not summary["results"]:
                summary["results"] = [
                    json.loads(line.removeprefix(PREFIX))
                    for line in runner_log.read_text().splitlines()
                    if line.startswith(PREFIX)
                ]
            output.write_text(json.dumps(summary, indent=2) + "\n")
            api.delete_namespace(namespace, _request_timeout=REQUEST_TIMEOUT)


if __name__ == "__main__":
    main()
