"""Exercise installed Connect provisioning against disposable Kubernetes executors."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from kubernetes import client as k8s, config
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.connect import ConnectProfile
from spark_on_k8s.connect_server import (
    ConnectServerConfig,
    ConnectServerError,
    ConnectServerHandle,
    ConnectServerOwnershipError,
    ConnectServerTimeout,
)
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

REQUEST_TIMEOUT = (5, 20)


def save(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def read_logs(api, namespace, pod):
    response = api.read_namespaced_pod_log(
        pod, namespace, _preload_content=False, _request_timeout=REQUEST_TIMEOUT, tail_lines=1500
    )
    try:
        return response.data.decode("utf-8", errors="replace")
    finally:
        response.close()
        response.release_conn()


def snapshot(api, namespace, destination):
    destination.mkdir(parents=True, exist_ok=True)
    for name, reader in (
        ("pods", api.list_namespaced_pod),
        ("services", api.list_namespaced_service),
        ("events", api.list_namespaced_event),
    ):
        try:
            objects = reader(namespace, _request_timeout=REQUEST_TIMEOUT)
            save(destination / f"{name}.json", api.api_client.sanitize_for_serialization(objects))
            if name == "pods":
                for pod in objects.items:
                    try:
                        text = read_logs(api, namespace, pod.metadata.name)
                    except k8s.ApiException as error:
                        text = f"Logs unavailable: Kubernetes status {error.status}\n"
                    (destination / f"{pod.metadata.name}.log").write_text(text)
        except Exception as error:
            (destination / f"{name}-error.txt").write_text(str(error))


def owned_executors(api, handle):
    return [
        pod
        for pod in api.list_namespaced_pod(
            handle.run.namespace, label_selector="spark-role=executor", _request_timeout=REQUEST_TIMEOUT
        ).items
        if any(owner.uid == handle.run.driver_uid for owner in pod.metadata.owner_references or [])
    ]


def identities(api, handle):
    run = handle.run
    pod = api.read_namespaced_pod(run.driver_pod, run.namespace, _request_timeout=REQUEST_TIMEOUT)
    driver_service = api.read_namespaced_service(run.app_id, run.namespace, _request_timeout=REQUEST_TIMEOUT)
    connect_service = api.read_namespaced_service(
        handle.service_name, run.namespace, _request_timeout=REQUEST_TIMEOUT
    )
    assert pod.metadata.uid == run.driver_uid
    assert all(item.metadata.deletion_timestamp is None for item in (pod, driver_service, connect_service))
    assert driver_service.metadata.uid == handle.driver_service_uid
    assert connect_service.metadata.uid == handle.service_uid
    assert connect_service.spec.type == "ClusterIP"
    assert connect_service.spec.cluster_ip not in {None, "None"}
    assert not connect_service.spec.external_ips and not connect_service.spec.external_name
    assert all(port.node_port is None for port in connect_service.spec.ports)
    assert any(owner.uid == run.driver_uid for owner in connect_service.metadata.owner_references or [])
    selected = api.list_namespaced_pod(
        run.namespace,
        label_selector=",".join(f"{key}={value}" for key, value in connect_service.spec.selector.items()),
        _request_timeout=REQUEST_TIMEOUT,
    ).items
    assert [item.metadata.uid for item in selected] == [run.driver_uid], "Service must select only its driver"
    return {
        "driver": api.api_client.sanitize_for_serialization(pod),
        "driver_service": api.api_client.sanitize_for_serialization(driver_service),
        "connect_service": api.api_client.sanitize_for_serialization(connect_service),
    }


def wait_for_cleanup(api, handle, executor_uids=()):
    owner_uids = {handle.run.driver_uid, *executor_uids}
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        remaining = []
        for reader in (
            api.list_namespaced_pod,
            api.list_namespaced_service,
            api.list_namespaced_config_map,
            api.list_namespaced_secret,
        ):
            remaining.extend(
                item.metadata.name
                for item in reader(handle.run.namespace, _request_timeout=REQUEST_TIMEOUT).items
                if (item.metadata.labels or {}).get("spark-app-id") == handle.run.app_id
                or any(owner.uid in owner_uids for owner in item.metadata.owner_references or [])
            )
        if not remaining:
            return
        time.sleep(1)
    raise TimeoutError(f"Owned Connect resources were not garbage collected: {remaining}")


@contextmanager
def forwarded(api, args, handle, label):
    """Own one loopback-only kubectl process; this is test infrastructure, not SDK behavior."""
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        pod = api.read_namespaced_pod(
            handle.run.driver_pod, handle.run.namespace, _request_timeout=REQUEST_TIMEOUT
        )
        if pod.status.phase == "Running":
            break
        if pod.status.phase in {"Succeeded", "Failed"}:
            raise RuntimeError("Connect driver terminated before test port-forward could start")
        time.sleep(0.2)
    else:
        raise TimeoutError("Connect driver did not run before test port-forward deadline")
    log_path = args.artifacts / f"{label}-port-forward.log"
    command = [
        "kubectl",
        "--kubeconfig",
        args.kubeconfig,
        "--context",
        args.context,
        "--namespace",
        handle.run.namespace,
        "port-forward",
        "--address",
        "127.0.0.1",
        "--pod-running-timeout=60s",
        f"service/{handle.service_name}",
        f":{handle.port}",
    ]
    # A local kubectl listener can open before the remote port is bound, then
    # exit permanently on the first refused connection. Wait only for transport
    # availability here; the SDK below must still execute and identify a query.
    transport_command = [
        "kubectl",
        "--kubeconfig",
        args.kubeconfig,
        "--context",
        args.context,
        "--namespace",
        handle.run.namespace,
        "exec",
        handle.run.driver_pod,
        "--",
        "/bin/bash",
        "-c",
        "for ((attempt=0; attempt<60; attempt++)); do "
        'if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then exit 0; fi; '
        "sleep 1; done; exit 1",
        "--",
        str(handle.port),
    ]
    with (args.artifacts / f"{label}-transport.log").open("w") as log:
        subprocess.run(transport_command, stdout=log, stderr=subprocess.STDOUT, timeout=65, check=True)
    with log_path.open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 70
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"Test port-forward exited; inspect {log_path.name}")
                match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+)", log_path.read_text())
                if match:
                    yield ConnectProfile(endpoint=f"sc://127.0.0.1:{match.group(1)}", tls=False)
                    return
                time.sleep(0.2)
            raise TimeoutError("Test port-forward did not become available")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)


def server_config(args, namespace, name, **updates):
    values = {
        "image": args.image,
        "server_jar": args.server_jar,
        "namespace": namespace,
        "service_account": "spark",
        "app_name": name,
        "image_pull_policy": "Never",
        "driver_resources": PodResources(cpu=1, memory=512, memory_overhead=384),
        "executor_resources": PodResources(cpu=1, memory=512, memory_overhead=384),
        "executor_instances": ExecutorInstances(initial=1),
        "spark_conf": {
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            "spark.kubernetes.executor.request.cores": "0.25",
        },
    }
    values.update(updates)
    return ConnectServerConfig(**values)


def cli(args, label, *command, expected=0):
    executable = str(Path(sys.executable).with_name("spark-on-k8s"))
    result = subprocess.run(
        [
            executable,
            "connect",
            "server",
            *command,
            "--kubeconfig",
            args.kubeconfig,
            "--context",
            args.context,
            "--output",
            "json",
        ],
        capture_output=True,
        text=True,
        # Explicit kubeconfig/context must override unrelated ambient defaults.
        env={**os.environ, "SPARK_ON_K8S_IN_CLUSTER": "true"},
        timeout=args.timeout + 30,
    )
    (args.artifacts / f"cli-{label}.stdout.json").write_text(result.stdout)
    (args.artifacts / f"cli-{label}.stderr.log").write_text(result.stderr)
    assert result.returncode == expected, f"CLI {label} exited {result.returncode}; inspect saved evidence"
    value = json.loads(result.stdout)
    assert value["schema_version"] == 1
    assert value["operation"].startswith("connect_server_")
    if not expected:
        assert value["error"] is None
    return value


def cli_server(args, namespace):
    receipt = args.artifacts / "cli-server-receipt.json"
    result = cli(
        args,
        "create",
        "create",
        "--image",
        args.image,
        "--server-jar",
        args.server_jar,
        "--namespace",
        namespace,
        "--service-account",
        "spark",
        "--name",
        "connect-cli",
        "--receipt-file",
        str(receipt),
        "--image-pull-policy",
        "Never",
        "--driver-memory",
        "512",
        "--driver-memory-overhead",
        "384",
        "--executor-memory",
        "512",
        "--executor-memory-overhead",
        "384",
        "--executors",
        "1",
        "--conf",
        "spark.kubernetes.executor.request.cores=0.25",
    )
    assert result["query_ready"] is False
    document = json.loads(receipt.read_text())
    assert document["schema_version"] == 1
    handle = ConnectServerHandle.from_dict(document["server"])
    assert result["server"] == handle.to_dict()
    return handle, receipt


def run_execution(api, client, args, namespace):
    handle = client.create_connect_server(server_config(args, namespace, "connect-sdk"))
    receipt = args.artifacts / "sdk-server-receipt.json"
    save(receipt, handle.to_dict())
    assert ConnectServerHandle.from_dict(json.loads(receipt.read_text())) == handle
    save(args.artifacts / "initial-status.json", client.connect_server_status(handle))
    with forwarded(api, args, handle, "sdk") as profile:
        ready = client.wait_connect_server_ready(handle, profile=profile, timeout=args.timeout)
        save(args.artifacts / "sdk-ready.json", ready)
        save(args.artifacts / "sdk-resources.json", identities(api, handle))
        with profile.session() as first:
            assert first.version == args.spark_version
            assert first.conf.get("spark.app.id") == handle.run.app_id
            dataframe_sum = first.range(0, 10000, 1, 4).groupBy().sum("id").first()[0]
            assert dataframe_sum == 49995000
            hosts = {
                row.executor_host
                for row in first.sql(
                    "SELECT DISTINCT reflect('java.lang.System', 'getenv', 'HOSTNAME') AS executor_host "
                    "FROM range(0, 10000, 1, 4)"
                ).collect()
            }
            executors = owned_executors(api, handle)
            assert executors and hosts
            assert hosts <= {pod.metadata.name for pod in executors}
            assert handle.run.driver_pod not in hosts, "The query must execute outside the driver pod"
            first.range(3).createOrReplaceTempView("private_connect_view")
            first.conf.set("spark.sql.shuffle.partitions", "3")
            with profile.session() as second:
                assert not second.catalog.tableExists("private_connect_view")
                assert second.conf.get("spark.sql.shuffle.partitions") == "2"
                assert second.sql("SELECT SUM(id) AS value FROM range(10000)").first().value == 49995000
            assert first.sql("SELECT COUNT(*) AS value FROM private_connect_view").first().value == 3
            try:
                first.sql("SELECT missing_connect_column FROM range(1)").collect()
            except Exception as error:
                assert "missing_connect_column" in str(error)
            else:
                raise AssertionError("Invalid SQL unexpectedly succeeded")
            assert first.range(7).count() == 7, "A query error must not terminate the shared server"
        assert profile.probe(timeout=30, expected_app_id=handle.run.app_id)["status"] == "ok"
        save(args.artifacts / "post-session-resources.json", identities(api, handle))
        # Exercise installed CLI receipt/status/wait/delete against a second real server.
        other, other_receipt = cli_server(args, namespace)
        cli(args, "status", "status", str(other_receipt))
        with forwarded(api, args, other, "cli") as other_profile:
            profile_file = args.artifacts / "cli-profile.toml"
            profile_file.write_text(
                f'[profiles.default]\nendpoint = "{other_profile.endpoint}"\ntls = false\n'
            )
            waited = cli(
                args,
                "wait",
                "wait",
                str(other_receipt),
                "--profile-file",
                str(profile_file),
                "--timeout",
                str(args.timeout),
            )
            assert waited["query_ready"] is True
            save(args.artifacts / "cli-resources.json", identities(api, other))
            assert client.connect_server_status(handle)["status"] == "running"
            assert client.connect_server_status(other)["status"] == "running"
            try:
                client.wait_connect_server_ready(handle, profile=other_profile, timeout=5, poll_interval=0.2)
            except ConnectServerTimeout:
                pass
            else:
                raise AssertionError("A different reachable Spark application was reported ready")
            identities(api, handle)
            identities(api, other)
            assert client.connect_server_status(handle)["status"] == "running"
            assert client.connect_server_status(other)["status"] == "running"
        other_executors = owned_executors(api, other)
        cli(args, "delete", "delete", str(other_receipt))
        wait_for_cleanup(api, other, [pod.metadata.uid for pod in other_executors])
        cli(args, "delete-again", "delete", str(other_receipt))
        # Every stale UID must fail before any deletion, including the headless Service.
        for name, stale in (
            ("driver", replace(handle, run=replace(handle.run, driver_uid=str(uuid4())))),
            ("connect-service", replace(handle, service_uid=str(uuid4()))),
            ("driver-service", replace(handle, driver_service_uid=str(uuid4()))),
        ):
            try:
                client.delete_connect_server(stale)
            except ConnectServerOwnershipError:
                pass
            else:
                raise AssertionError(f"Stale {name} UID was allowed to delete resources")
            identities(api, handle)
        snapshot(api, namespace, args.artifacts / "execution")
    # Re-create the same service name as an unrelated resource. No resource may be
    # partially deleted before all recorded identities are checked.
    api.delete_namespaced_service(
        handle.service_name,
        namespace,
        body=k8s.V1DeleteOptions(preconditions=k8s.V1Preconditions(uid=handle.service_uid)),
        _request_timeout=REQUEST_TIMEOUT,
    )
    replacement = api.create_namespaced_service(
        namespace,
        k8s.V1Service(
            metadata=k8s.V1ObjectMeta(name=handle.service_name, labels={"connect-test": "foreign"}),
            spec=k8s.V1ServiceSpec(
                type="ClusterIP",
                selector={"connect-test": "no-driver"},
                ports=[k8s.V1ServicePort(name="grpc", port=handle.port, target_port=handle.port)],
            ),
        ),
        _request_timeout=REQUEST_TIMEOUT,
    )
    assert replacement.metadata.uid != handle.service_uid
    try:
        client.delete_connect_server(handle)
    except ConnectServerOwnershipError:
        pass
    else:
        raise AssertionError("A same-name replacement service was adopted or deleted")
    for observed, uid in (
        (
            api.read_namespaced_pod(handle.run.driver_pod, namespace, _request_timeout=REQUEST_TIMEOUT),
            handle.run.driver_uid,
        ),
        (
            api.read_namespaced_service(handle.run.app_id, namespace, _request_timeout=REQUEST_TIMEOUT),
            handle.driver_service_uid,
        ),
        (
            api.read_namespaced_service(handle.service_name, namespace, _request_timeout=REQUEST_TIMEOUT),
            replacement.metadata.uid,
        ),
    ):
        assert observed.metadata.uid == uid and observed.metadata.deletion_timestamp is None
    save(args.artifacts / "foreign-replacement.json", api.api_client.sanitize_for_serialization(replacement))
    api.delete_namespaced_service(
        handle.service_name,
        namespace,
        body=k8s.V1DeleteOptions(preconditions=k8s.V1Preconditions(uid=replacement.metadata.uid)),
        _request_timeout=REQUEST_TIMEOUT,
    )
    deletion = client.delete_connect_server(handle)
    save(args.artifacts / "sdk-deletion.json", deletion)
    wait_for_cleanup(api, handle, [pod.metadata.uid for pod in executors])
    client.delete_connect_server(handle)
    return {
        "scenario": "distributed-execution-and-ownership",
        "spark_version": args.spark_version,
        "app_id": handle.run.app_id,
        "driver_uid": handle.run.driver_uid,
        "dataframe_sum": dataframe_sum,
        "sql_executor_hosts": sorted(hosts),
        "executors": [{"name": pod.metadata.name, "uid": pod.metadata.uid} for pod in executors],
        "independent_sessions": True,
        "session_release_preserved_server": True,
        "failed_query_recovered": True,
        "wrong_endpoint_rejected": True,
        "stale_uids_rejected": True,
        "same_name_replacement_preserved": True,
        "cli_lifecycle": True,
        "owned_resources_garbage_collected": True,
    }


def run_pending(api, client, args, namespace):
    handle = client.create_connect_server(
        server_config(
            args, namespace, "connect-pending", driver_node_selector={"connect-test/absent": uuid4().hex}
        )
    )
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        profile = ConnectProfile(endpoint=f"sc://127.0.0.1:{unused.getsockname()[1]}", tls=False)
        started = time.monotonic()
        try:
            client.wait_connect_server_ready(handle, profile=profile, timeout=3, poll_interval=0.2)
        except ConnectServerTimeout as error:
            assert error.handle == handle and error.cleanup["status"] == "not_attempted"
        else:
            raise AssertionError("Unschedulable server did not time out")
    elapsed = time.monotonic() - started
    assert elapsed < 8
    saved = identities(api, handle)
    assert saved["driver"]["status"]["phase"] == "Pending"
    save(args.artifacts / "pending-resources.json", saved)
    save(args.artifacts / "pending-status.json", client.connect_server_status(handle))
    snapshot(api, namespace, args.artifacts / "pending")
    client.delete_connect_server(handle)
    wait_for_cleanup(api, handle)
    return {
        "scenario": "pending-timeout",
        "elapsed_seconds": elapsed,
        "resources_preserved": True,
        "owned_resources_garbage_collected": True,
    }


def run_failed(api, client, args, namespace):
    handle = client.create_connect_server(
        server_config(
            args,
            namespace,
            "connect-failed",
            spark_conf={"spark.serializer": "missing.connect.integration.Serializer"},
        )
    )
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        profile = ConnectProfile(endpoint=f"sc://127.0.0.1:{unused.getsockname()[1]}", tls=False)
        try:
            client.wait_connect_server_ready(handle, profile=profile, timeout=args.timeout)
        except ConnectServerError:
            pass
        else:
            raise AssertionError("A failed server was reported ready")
    saved = identities(api, handle)
    assert saved["driver"]["status"]["phase"] == "Failed"
    logs = read_logs(api, namespace, handle.run.driver_pod)
    assert "missing.connect.integration.Serializer" in logs and "ClassNotFoundException" in logs
    save(args.artifacts / "failed-status.json", client.connect_server_status(handle))
    snapshot(api, namespace, args.artifacts / "failed")
    executors = owned_executors(api, handle)
    client.delete_connect_server(handle)
    wait_for_cleanup(api, handle, [pod.metadata.uid for pod in executors])
    return {
        "scenario": "startup-failure",
        "driver_phase": "Failed",
        "specific_error_verified": True,
        "owned_resources_garbage_collected": True,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Test installed Spark Connect provisioning on disposable kind"
    )
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--spark-version", required=True)
    parser.add_argument("--server-jar", required=True)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--case", choices=["all", "execution", "pending", "startup-failure"], default="all")
    args = parser.parse_args()
    assert re.fullmatch(r"4\.\d+\.\d+", args.spark_version), "Select an explicit supported Spark4 release"
    contexts, _ = config.list_kube_config_contexts(config_file=args.kubeconfig)
    selected = next(item for item in contexts if item["name"] == args.context)
    assert selected["context"]["cluster"].startswith("kind-"), "Only disposable kind clusters are supported"
    args.artifacts.mkdir(parents=True, exist_ok=True)
    namespace = "connect-compat-" + uuid4().hex[:8]
    manager = KubernetesClientManager(config_file=args.kubeconfig, context=args.context, in_cluster=False)
    results = []
    with manager.client() as connection:
        api = k8s.CoreV1Api(connection)
        created = api.create_namespace(
            k8s.V1Namespace(metadata=k8s.V1ObjectMeta(name=namespace)), _request_timeout=REQUEST_TIMEOUT
        )
        try:
            api.create_namespaced_service_account(
                namespace,
                k8s.V1ServiceAccount(metadata=k8s.V1ObjectMeta(name="spark")),
                _request_timeout=REQUEST_TIMEOUT,
            )
            rbac = k8s.RbacAuthorizationV1Api(connection)
            rbac.create_namespaced_role(
                namespace,
                k8s.V1Role(
                    metadata=k8s.V1ObjectMeta(name="spark"),
                    rules=[
                        k8s.V1PolicyRule(
                            api_groups=[""],
                            resources=["pods", "services", "configmaps"],
                            verbs=["get", "list", "watch", "create", "delete", "patch", "update"],
                        )
                    ],
                ),
                _request_timeout=REQUEST_TIMEOUT,
            )
            rbac.create_namespaced_role_binding(
                namespace,
                k8s.V1RoleBinding(
                    metadata=k8s.V1ObjectMeta(name="spark"),
                    role_ref=k8s.V1RoleRef(api_group="rbac.authorization.k8s.io", kind="Role", name="spark"),
                    subjects=[k8s.RbacV1Subject(kind="ServiceAccount", name="spark", namespace=namespace)],
                ),
                _request_timeout=REQUEST_TIMEOUT,
            )
            submitter = SparkOnK8S(k8s_client_manager=manager)
            scenarios = {"execution": run_execution, "pending": run_pending, "startup-failure": run_failed}
            selected_scenarios = scenarios.values() if args.case == "all" else [scenarios[args.case]]
            for scenario in selected_scenarios:
                try:
                    result = scenario(api, submitter, args, namespace)
                    results.append({**result, "test_status": "passed"})
                    print(json.dumps(results[-1]), flush=True)
                except Exception as error:
                    results.append(
                        {
                            "scenario": scenario.__name__,
                            "test_status": "failed",
                            "error_type": type(error).__name__,
                        }
                    )
                    raise
                finally:
                    save(
                        args.artifacts / "result.json",
                        {
                            "mode": "kubernetes",
                            "requested_case": args.case,
                            "spark": args.spark_version,
                            "image": args.image,
                            "server_jar": args.server_jar,
                            "context": args.context,
                            "namespace": namespace,
                            "results": results,
                        },
                    )
        finally:
            snapshot(api, namespace, args.artifacts / "final")
            api.delete_namespace(
                namespace,
                body=k8s.V1DeleteOptions(preconditions=k8s.V1Preconditions(uid=created.metadata.uid)),
                _request_timeout=REQUEST_TIMEOUT,
            )


if __name__ == "__main__":
    main()
