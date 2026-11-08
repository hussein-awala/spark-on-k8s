"""Execute the canonical notebook in real kernels against explicitly selected kind."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import shutil
import sys
import textwrap
import time
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import nbformat
import psutil
import spark_on_k8s
from kubernetes import client as k8s, config
from nbclient import NotebookClient
from spark_on_k8s.connect_server import ConnectServerHandle
from spark_on_k8s.k8s.sync_client import KubernetesClientManager

ROOT = Path(__file__).resolve().parents[3]
REQUEST_TIMEOUT = (5, 20)


def load_module(name, path):
    """Load a named helper without putting the repository on the SDK import path."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


connect = load_module("connect_integration", ROOT / "tests/integration/connect/run_kubernetes.py")


def best_effort_snapshot(api, namespace, destination):
    """Diagnostics must not prevent identity-checked teardown."""
    try:
        connect.snapshot(api, namespace, destination)
    except Exception as error:
        print(f"Diagnostic snapshot unavailable at {destination}: {error}", file=sys.stderr, flush=True)


def installed_package():
    location = Path(spark_on_k8s.__file__).resolve()
    assert location.is_relative_to(Path(sys.prefix).resolve()), f"SDK is outside this environment: {location}"
    assert not location.is_relative_to(ROOT / "spark_on_k8s"), (
        "The test must import the installed wheel, not repository source"
    )
    distribution = importlib.metadata.distribution("spark-on-k8s")
    direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
    assert not direct_url.get("dir_info", {}).get("editable"), "Install the noneditable wheel before testing"
    return {"location": str(location), "version": distribution.version, "direct_url": direct_url}


def configure_environment(args, namespace):
    state = args.artifacts / "jupyter-state"
    environment = {
        "SPARK_NOTEBOOK_KUBECONFIG": args.kubeconfig,
        "SPARK_NOTEBOOK_CONTEXT": args.context,
        "SPARK_NOTEBOOK_NAMESPACE": namespace,
        "SPARK_NOTEBOOK_SERVICE_ACCOUNT": args.service_account,
        "SPARK_NOTEBOOK_IMAGE": args.image,
        "SPARK_NOTEBOOK_SPARK_VERSION": args.spark_version,
        "SPARK_NOTEBOOK_SERVER_JAR": args.server_jar,
        "SPARK_NOTEBOOK_IMAGE_PULL_POLICY": "Never",
        "SPARK_NOTEBOOK_ARTIFACT_DIR": str(args.artifacts),
        "SPARK_NOTEBOOK_EXPECTED_PYTHON": sys.executable,
        "IPYTHONDIR": str(state / "ipython"),
        "JUPYTER_CONFIG_DIR": str(state / "config"),
        "JUPYTER_DATA_DIR": str(state / "data"),
        "JUPYTER_RUNTIME_DIR": str(state / "runtime"),
        "JUPYTER_PATH": str(state / "data"),
        "JUPYTER_NO_CONFIG": "1",
        "PYTHONNOUSERSITE": "1",
    }
    for name in ("IPYTHONDIR", "JUPYTER_CONFIG_DIR", "JUPYTER_DATA_DIR", "JUPYTER_RUNTIME_DIR"):
        Path(environment[name]).mkdir(parents=True)
    os.environ.update(environment)
    kernel_name = "spark-connect-" + uuid4().hex
    kernel = state / "data/kernels" / kernel_name
    kernel.mkdir(parents=True)
    connect.save(
        kernel / "kernel.json",
        {
            "argv": [sys.executable, "-I", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
            "display_name": "Spark Connect integration",
            "language": "python",
        },
    )
    return kernel_name


def create_service_account(connection, namespace, name):
    api = k8s.CoreV1Api(connection)
    api.create_namespaced_service_account(
        namespace,
        k8s.V1ServiceAccount(metadata=k8s.V1ObjectMeta(name=name)),
        _request_timeout=REQUEST_TIMEOUT,
    )
    rbac = k8s.RbacAuthorizationV1Api(connection)
    rbac.create_namespaced_role(
        namespace,
        k8s.V1Role(
            metadata=k8s.V1ObjectMeta(name=name),
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
            metadata=k8s.V1ObjectMeta(name=name),
            role_ref=k8s.V1RoleRef(api_group="rbac.authorization.k8s.io", kind="Role", name=name),
            subjects=[k8s.RbacV1Subject(kind="ServiceAccount", name=name, namespace=namespace)],
        ),
        _request_timeout=REQUEST_TIMEOUT,
    )


def identities(api, namespace):
    return {
        "drivers": sorted(
            p.metadata.uid
            for p in api.list_namespaced_pod(
                namespace, label_selector="spark-role=driver", _request_timeout=REQUEST_TIMEOUT
            ).items
        ),
        "services": sorted(
            service.metadata.uid
            for service in api.list_namespaced_service(namespace, _request_timeout=REQUEST_TIMEOUT).items
        ),
    }


class TunnelCleanup:
    """Track only exact notebook-owned process records, never general kubectl processes."""

    def __init__(self, args, namespace):
        self.args = args
        self.namespace = namespace
        self.started = time.time()
        self.records = {}

    def observe(self):
        for path in self.args.artifacts.glob("*-tunnel-process.json"):
            record = json.loads(path.read_text())
            pid = record["pid"]
            if pid in self.records:
                continue
            assert record["namespace"] == self.namespace
            argv = record["argv"]
            expected_prefix = [
                "kubectl",
                "--kubeconfig",
                self.args.kubeconfig,
                "--context",
                self.args.context,
                "--namespace",
                self.namespace,
                "port-forward",
                "--address=127.0.0.1",
            ]
            assert argv[: len(expected_prefix)] == expected_prefix
            try:
                process = psutil.Process(pid)
                actual = process.cmdline()
                assert Path(actual[0]).name == "kubectl" and actual[1:] == argv[1:]
                created = process.create_time()
                assert created >= self.started - 1
            except psutil.NoSuchProcess:
                self.records[pid] = {"pid": pid, "status": "already_closed"}
                continue
            self.records[pid] = {"pid": pid, "created": created, "argv": actual, "process": process}

    def close(self):
        errors = []
        try:
            self.observe()
        except Exception as error:
            errors.append(f"Tunnel identity could not be verified; preserved: {error}")
        results = []
        for record in self.records.values():
            process = record.get("process")
            result = {key: value for key, value in record.items() if key != "process"}
            try:
                if process is not None and process.is_running():
                    assert process.create_time() == record["created"]
                    assert process.cmdline() == record["argv"]
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except psutil.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                result["status"] = "closed"
            except psutil.NoSuchProcess:
                result["status"] = "closed"
            except Exception as error:
                result["status"] = "preserved_uncertain_identity"
                errors.append(str(error))
            results.append(result)
        connect.save(self.args.artifacts / "tunnel-cleanup.json", {"processes": results, "errors": errors})
        if errors:
            raise RuntimeError("; ".join(errors))


async def execute(notebook, runtime, args, api, namespace, kernel_name, tunnels):
    client = NotebookClient(
        notebook,
        timeout=args.cell_timeout,
        kernel_name=kernel_name,
        allow_errors=False,
        resources={"metadata": {"path": str(runtime)}},
        record_timing=True,
    )
    restarts = []
    try:
        async with client.async_setup_kernel():
            for index, cell in enumerate(notebook.cells):
                tags = cell.metadata.get("tags", [])
                if "restart-kernel-before" in tags:
                    best_effort_snapshot(api, namespace, args.artifacts / "before-restart")
                    before = client.km.provisioner.pid
                    await client.km.restart_kernel(now=True)
                    await client.kc.wait_for_ready(timeout=60)
                    after = client.km.provisioner.pid
                    assert before != after
                    restarts.append({"before_pid": before, "after_pid": after, "cell_index": index})
                if "delete-server" in tags:
                    best_effort_snapshot(api, namespace, args.artifacts / "before-deletion")
                await client.async_execute_cell(cell, index)
                tunnels.observe()
                if "create-server" in tags:
                    original = deepcopy(cell)
                    receipt_before = (args.artifacts / "server-receipt.json").read_bytes()
                    before = identities(api, namespace)
                    probe = nbformat.v4.new_code_cell(
                        "try:\n"
                        + textwrap.indent(cell.source, "    ")
                        + "\nexcept FileExistsError:\n    print('Creation rerun refused.')\n"
                        "else:\n    raise AssertionError('Creation rerun did not refuse submission')"
                    )
                    try:
                        await client.async_execute_cell(probe, index)
                        after = identities(api, namespace)
                        assert before == after
                        assert receipt_before == (args.artifacts / "server-receipt.json").read_bytes()
                        connect.save(
                            args.artifacts / "creation-rerun.json",
                            {
                                "status": "passed",
                                "before": before,
                                "after": after,
                                "receipt_unchanged": True,
                                "no_second_driver_created": True,
                            },
                        )
                    finally:
                        connect.save(args.artifacts / "creation-rerun-cell.json", dict(probe))
                        notebook.cells[index] = original
                print(json.dumps({"cell": index, "type": cell.cell_type, "status": "passed"}), flush=True)
                if args.interrupt_after_cell and args.interrupt_after_cell in tags:
                    raise KeyboardInterrupt(
                        f"Deliberate runner interruption after {args.interrupt_after_cell}"
                    )
    finally:
        nbformat.write(notebook, args.artifacts / "spark_connect.executed.ipynb")
        connect.save(args.artifacts / "kernel-restarts.json", restarts)


def cleanup(api, namespace, namespace_uid, args, helper, outcome):
    """Preserve a partial receipt or uncertain ownership instead of hiding it with namespace deletion."""
    current = api.read_namespace(namespace, _request_timeout=REQUEST_TIMEOUT)
    assert current.metadata.uid == namespace_uid, "Namespace was replaced; preserve its resources"
    best_effort_snapshot(api, namespace, args.artifacts / "final")
    receipt = args.artifacts / "server-receipt.json"
    if receipt.exists():
        document = json.loads(receipt.read_text())
        assert document["schema_version"] == 1
        handle = ConnectServerHandle.from_dict(document["server"])
        assert handle.run.namespace == namespace
        executors = connect.owned_executors(api, handle)
        submitter = helper.client_for(helper.settings())
        deletion = submitter.delete_connect_server(handle)
        connect.wait_for_cleanup(api, handle, [p.metadata.uid for p in executors])
        try:
            connect.save(
                args.artifacts / "fallback-server-cleanup.json",
                {
                    "deletion": deletion,
                    "owned_resources_garbage_collected": True,
                },
            )
        except Exception as error:
            print(f"Server cleanup evidence unavailable: {error}", file=sys.stderr, flush=True)
    elif receipt.with_suffix(".pending").exists():
        raise RuntimeError("Creation is uncertain and has no complete receipt; preserving namespace")
    assert not api.list_namespaced_pod(namespace, _request_timeout=REQUEST_TIMEOUT).items
    assert not api.list_namespaced_service(namespace, _request_timeout=REQUEST_TIMEOUT).items
    api.delete_namespace(
        namespace,
        body=k8s.V1DeleteOptions(preconditions=k8s.V1Preconditions(uid=namespace_uid)),
        _request_timeout=REQUEST_TIMEOUT,
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            existing = api.read_namespace(namespace, _request_timeout=REQUEST_TIMEOUT)
        except k8s.ApiException as error:
            if error.status == 404:
                connect.save(
                    args.artifacts / "fixture-cleanup.json",
                    {
                        "namespace": namespace,
                        "namespace_uid": namespace_uid,
                        "namespace_absence_observed": True,
                        "notebook_outcome": outcome,
                    },
                )
                return
            raise
        assert existing.metadata.uid == namespace_uid, "Namespace was replaced during cleanup"
        time.sleep(0.5)
    raise TimeoutError("Namespace deletion was not observed before the cleanup deadline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--spark-version", required=True, choices=["4.0.5", "4.3.0"])
    parser.add_argument("--server-jar", required=True)
    parser.add_argument("--service-account", default="spark")
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--cell-timeout", type=int, default=240)
    parser.add_argument("--interrupt-after-cell", choices=["create-server", "open-tunnel"])
    args = parser.parse_args()
    assert args.cell_timeout > 0
    args.kubeconfig = str(Path(args.kubeconfig).resolve(strict=True))
    contexts, _ = config.list_kube_config_contexts(config_file=args.kubeconfig)
    selected = next(item for item in contexts if item["name"] == args.context)
    assert selected["context"]["cluster"].startswith("kind-"), (
        "Only explicitly selected disposable kind is allowed"
    )
    package = installed_package()
    client_version = importlib.metadata.version("pyspark-client")
    if client_version != args.spark_version:
        parser.error(
            f"This notebook matrix requires pyspark-client=={args.spark_version}; found {client_version}"
        )
    args.artifacts = args.artifacts.resolve()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    namespace = "jupyter-connect-" + uuid4().hex[:8]
    kernel_name = configure_environment(args, namespace)
    runtime = args.artifacts / "example"
    shutil.copytree(ROOT / "examples/jupyter", runtime)
    helper = load_module("jupyter_notebook_support", runtime / "notebook_support.py")
    notebook_path = runtime / "spark_connect.ipynb"
    notebook = nbformat.read(notebook_path, as_version=4)
    assert all(not cell.get("outputs") for cell in notebook.cells), "Commit only a clean canonical notebook"
    tunnels = TunnelCleanup(args, namespace)
    manager = KubernetesClientManager(
        config_file=args.kubeconfig, context=args.context, in_cluster=False, client_configuration=None
    )
    outcome = "failed"
    failure = None
    result = {}
    started = time.monotonic()
    with manager.client() as connection:
        api = k8s.CoreV1Api(connection)
        created = api.create_namespace(
            k8s.V1Namespace(metadata=k8s.V1ObjectMeta(name=namespace)), _request_timeout=REQUEST_TIMEOUT
        )
        # Keep namespace UID in memory even if evidence publication fails.
        try:
            connect.save(args.artifacts / "namespace.json", {"name": namespace, "uid": created.metadata.uid})
            create_service_account(connection, namespace, args.service_account)
            asyncio.run(execute(notebook, runtime, args, api, namespace, kernel_name, tunnels))
            result = json.loads((args.artifacts / "notebook-result.json").read_text())
            assert result["status"] == "passed" and result["tunnels_closed"]
            assert result["spark_version"] == result["client_version"] == args.spark_version
            errors = [
                output
                for cell in notebook.cells
                for output in cell.get("outputs", [])
                if output.output_type == "error"
            ]
            tables = [
                output
                for cell in notebook.cells
                for output in cell.get("outputs", [])
                if "<table" in output.get("data", {}).get("text/html", "")
            ]
            assert not errors and tables
            restarts = json.loads((args.artifacts / "kernel-restarts.json").read_text())
            assert len(restarts) == 1
            assert restarts[0]["before_pid"] == result["first_kernel_pid"]
            assert restarts[0]["after_pid"] == result["second_kernel_pid"]
            result.update(
                {
                    "python": sys.version.split()[0],
                    "kernel_executable": sys.executable,
                    "notebook_code_cells": sum(cell.cell_type == "code" for cell in notebook.cells),
                    "html_table_outputs": len(tables),
                    "unhandled_notebook_errors": len(errors),
                    "elapsed_seconds": time.monotonic() - started,
                    "namespace": namespace,
                    "namespace_uid": created.metadata.uid,
                    "installed_package": package,
                    "notebook_sha256": hashlib.sha256(notebook_path.read_bytes()).hexdigest(),
                    "helper_sha256": hashlib.sha256(
                        (runtime / "notebook_support.py").read_bytes()
                    ).hexdigest(),
                    "jupyter_state_isolated": True,
                    "creation_cell_rerun_refused": True,
                }
            )
            outcome = "passed"
        except BaseException as error:
            failure = error
            injected = (
                isinstance(error, KeyboardInterrupt)
                and str(error) == f"Deliberate runner interruption after {args.interrupt_after_cell}"
            )
            try:
                connect.save(
                    args.artifacts / "failure.json",
                    {"type": type(error).__name__, "message": str(error), "injected_interruption": injected},
                )
            except Exception as evidence_error:
                failure.add_note(f"Could not write failure evidence: {evidence_error}")
        finally:
            cleanup_errors = []
            for operation in (
                tunnels.close,
                lambda: cleanup(api, namespace, created.metadata.uid, args, helper, outcome),
            ):
                try:
                    operation()
                except Exception as error:
                    cleanup_errors.append({"type": type(error).__name__, "message": str(error)})
            if cleanup_errors:
                outcome = "failed"
                message = f"Cleanup incomplete: {cleanup_errors}"
                if failure is not None:
                    failure.add_note(message)
                else:
                    failure = RuntimeError(message)
            try:
                connect.save(args.artifacts / "cleanup-errors.json", cleanup_errors)
                connect.save(
                    args.artifacts / "result.json",
                    {
                        **result,
                        "status": outcome,
                        "cleanup_errors": cleanup_errors,
                        "failure": None
                        if failure is None
                        else {"type": type(failure).__name__, "message": str(failure)},
                    },
                )
            except Exception as evidence_error:
                if failure is not None:
                    failure.add_note(f"Could not publish final evidence: {evidence_error}")
                else:
                    failure = evidence_error
    if failure is not None:
        raise failure
    print(json.dumps({"status": outcome, "artifacts": str(args.artifacts)}), flush=True)


if __name__ == "__main__":
    main()
