"""Small notebook utilities; Spark provisioning itself uses spark-on-k8s directly."""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from kubernetes import client as k8s
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.connect import ConnectProfile
from spark_on_k8s.k8s.sync_client import KubernetesClientManager


@dataclass(frozen=True)
class Settings:
    kubeconfig: str
    context: str
    namespace: str
    service_account: str
    image: str
    server_jar: str
    spark_version: str
    image_pull_policy: str
    artifact_dir: Path


def settings() -> Settings:
    """Require explicit connection settings; never switch the user's current context."""
    required = ["KUBECONFIG", "CONTEXT", "NAMESPACE", "IMAGE"]
    missing = [name for name in required if not os.environ.get("SPARK_NOTEBOOK_" + name)]
    if missing:
        raise ValueError("Set these SPARK_NOTEBOOK_ environment variables: " + ", ".join(missing))
    spark_version = os.environ.get("SPARK_NOTEBOOK_SPARK_VERSION", "4.3.0")
    if not re.fullmatch(r"4\.\d+\.\d+", spark_version):
        raise ValueError("Select an explicit Spark 4 release, for example 4.3.0")
    result = Settings(
        kubeconfig=str(Path(os.environ["SPARK_NOTEBOOK_KUBECONFIG"]).expanduser().resolve()),
        context=os.environ["SPARK_NOTEBOOK_CONTEXT"],
        namespace=os.environ["SPARK_NOTEBOOK_NAMESPACE"],
        service_account=os.environ.get("SPARK_NOTEBOOK_SERVICE_ACCOUNT", "spark"),
        image=os.environ["SPARK_NOTEBOOK_IMAGE"],
        server_jar=os.environ.get(
            "SPARK_NOTEBOOK_SERVER_JAR", f"local:///opt/spark/jars/spark-connect_2.13-{spark_version}.jar"
        ),
        spark_version=spark_version,
        image_pull_policy=os.environ.get("SPARK_NOTEBOOK_IMAGE_PULL_POLICY", "IfNotPresent"),
        artifact_dir=Path(os.environ.get("SPARK_NOTEBOOK_ARTIFACT_DIR", "notebook-results")).resolve(),
    )
    if not Path(result.kubeconfig).is_file():
        raise ValueError("The explicitly selected kubeconfig does not exist")
    result.artifact_dir.mkdir(parents=True, exist_ok=True)
    return result


def client_for(config: Settings) -> SparkOnK8S:
    return SparkOnK8S(
        k8s_client_manager=KubernetesClientManager(
            config_file=config.kubeconfig, context=config.context, in_cluster=False, client_configuration=None
        )
    )


def write_json(path: Path, value, *, exclusive=False):
    """Publish complete JSON on the same filesystem; never overwrite an exclusive receipt."""
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if exclusive:
            os.link(temporary, path)
        else:
            os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def reserve_receipt(path: Path):
    """Refuse duplicate creation, including after a crash before receipt publication.

    A pending marker is deliberately retained on failure. It is not proof that a
    server exists or is absent: inspect the failure and live resources before
    removing it. A complete receipt is still usable for status or deletion.
    """
    if path.exists():
        raise FileExistsError("A server receipt exists. Resume it; do not rerun creation.")
    pending = path.with_suffix(".pending")
    write_json(pending, {"status": "creation_started", "receipt": path.name}, exclusive=True)
    if path.exists():
        raise FileExistsError("A server receipt appeared. Inspect it before retrying creation.")
    yield
    pending.unlink()


def kubectl(config: Settings) -> list[str]:
    return [
        "kubectl",
        "--kubeconfig",
        config.kubeconfig,
        "--context",
        config.context,
        "--namespace",
        config.namespace,
    ]


class LoopbackTunnel:
    """One explicit notebook-owned tunnel, kept separate from the server lifecycle."""

    def __init__(self, config: Settings, server, label="session"):
        self.config = config
        self.server = server
        self.label = label
        self.process = None
        self.log = None
        self.profile = None

    def start(self):
        if self.process is not None and self.process.poll() is None:
            return self.profile
        command = kubectl(self.config)
        subprocess.run(
            command
            + [
                "wait",
                "--for=jsonpath={.status.phase}=Running",
                "--timeout=120s",
                "pod/" + self.server.run.driver_pod,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=125,
        )
        # kubectl can open its local listener before Spark's port is bound and
        # exit on the first refused request. This only prepares transport;
        # wait_connect_server_ready still performs the actual query/identity check.
        result = subprocess.run(
            command
            + [
                "exec",
                self.server.run.driver_pod,
                "--",
                "/bin/bash",
                "-c",
                'for ((i=0;i<90;i++)); do if (exec 3<>"/dev/tcp/127.0.0.1/$1") '
                "2>/dev/null; then exit 0; fi; sleep 1; done; exit 1",
                "--",
                str(self.server.port),
            ],
            capture_output=True,
            text=True,
            timeout=95,
        )
        (self.config.artifact_dir / f"{self.label}-listener.log").write_text(result.stdout + result.stderr)
        result.check_returncode()
        path = self.config.artifact_dir / f"{self.label}-port-forward.log"
        self.log = path.open("w", encoding="utf-8")
        command += [
            "port-forward",
            "--address=127.0.0.1",
            "service/" + self.server.service_name,
            ":" + str(self.server.port),
        ]
        self.process = subprocess.Popen(
            command,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            write_json(
                self.config.artifact_dir / f"{self.label}-tunnel-process.json",
                {
                    "pid": self.process.pid,
                    "argv": command,
                    "namespace": self.config.namespace,
                },
            )
            end = time.monotonic() + 30
            while time.monotonic() < end:
                if self.process.poll() is not None:
                    raise RuntimeError("The notebook tunnel exited; inspect " + str(path))
                match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+)", path.read_text())
                if match:
                    self.profile = ConnectProfile(endpoint="sc://127.0.0.1:" + match.group(1), tls=False)
                    return self.profile
                time.sleep(0.1)
            raise TimeoutError("Notebook tunnel did not open")
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process is not None and self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        if self.log is not None:
            self.log.close()


@contextmanager
def api_for(client):
    with client.k8s_client_manager.client() as raw:
        yield k8s.CoreV1Api(raw)


def executor_evidence(client, server, observed_hosts):
    with api_for(client) as api:
        pods = api.list_namespaced_pod(
            server.run.namespace, label_selector="spark-role=executor", _request_timeout=(5, 20)
        ).items
    owned = [
        p
        for p in pods
        if any(owner.uid == server.run.driver_uid for owner in p.metadata.owner_references or [])
    ]
    names = {p.metadata.name for p in owned}
    assert observed_hosts and observed_hosts <= names
    assert server.run.driver_pod not in observed_hosts
    return [{"name": p.metadata.name, "uid": p.metadata.uid} for p in owned]


def wait_for_cleanup(client, server, executor_uids=(), timeout=90):
    end = time.monotonic() + timeout
    owner_uids = {server.run.driver_uid, *executor_uids}
    with api_for(client) as api:
        while time.monotonic() < end:
            remaining = []
            for reader in (
                api.list_namespaced_pod,
                api.list_namespaced_service,
                api.list_namespaced_config_map,
            ):
                for item in reader(server.run.namespace, _request_timeout=(5, 20)).items:
                    if (item.metadata.labels or {}).get("spark-app-id") == server.run.app_id or any(
                        owner.uid in owner_uids for owner in item.metadata.owner_references or []
                    ):
                        remaining.append(item.metadata.name)
            if not remaining:
                return {"owned_resources_remaining": [], "garbage_collection_observed": True}
            time.sleep(0.5)
    raise TimeoutError("Owned server resources remain: " + ", ".join(remaining))
