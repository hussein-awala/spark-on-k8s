from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.connect import ConnectProfile
from spark_on_k8s.connect_server import ConnectServerHandle, ConnectServerTimeout

from spark_on_k8s_dbt.api import BoundedKubernetesClientManager
from spark_on_k8s_dbt.config import ComputeConfig
from spark_on_k8s_dbt.forward import PortForward
from spark_on_k8s_dbt.progress import TRANSPORTS, StartupProgress, driver_state

_active_owner = None
_scope_lock = threading.Lock()
_REQUEST = (5, 15)


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int
    connect_profile: ConnectProfile | None = None


def get_invocation_owner():
    return _active_owner


@contextmanager
def invocation_scope(owner):
    """Bind one invocation; adapters capture it before dbt starts worker threads."""
    global _active_owner
    if not _scope_lock.acquire(blocking=False):
        raise RuntimeError("Concurrent dbt invocations require separate processes")
    try:
        _active_owner = owner
        yield owner
    finally:
        _active_owner = None
        _scope_lock.release()


class RecordingSDK(SparkOnK8S):
    """Persist the planned identity before the first cluster mutation."""

    def __init__(self, manager, record):
        super().__init__(k8s_client_manager=manager)
        self.record = record

    def submit_run(self, **kwargs):
        suffix = kwargs["app_id_suffix"]()
        app_id = kwargs["app_name"] + suffix
        self.record(
            {
                "namespace": kwargs["namespace"],
                "app_id": app_id,
                "driver_pod": app_id + "-driver",
                "driver_uid": None,
            }
        )
        original = None
        try:
            return super().submit_run(**kwargs)
        except BaseException as error:
            original = error
            raise
        finally:
            if self.last_submitted_run is not None and self.last_submitted_run.app_id == app_id:
                try:
                    self.record(self.last_submitted_run.to_dict())
                except BaseException as error:
                    if original is None:
                        raise
                    original.add_note(f"Recovery identity publication also failed: {type(error).__name__}")


class InvocationOwner:
    """Own compute for one wrapper run of dbt commands, independent of connection cleanup.

    A run executes one dbt command or a sequence of them in this process; every
    command reuses the same server and it is deleted once, when the run ends.

    Receipts contain resource identities and cluster selection, never the compute
    configuration (which can include catalog credentials). Incomplete creation
    remains recoverable evidence and is never treated as permission to delete.
    ``progress`` reports startup and cleanup phases; it is silent by default.
    """

    def __init__(self, receipt_dir: str | Path | None = None, *, progress: StartupProgress | None = None):
        self.progress = progress or StartupProgress.silent()
        self.directory = Path(receipt_dir or Path.cwd() / ".spark-on-k8s" / "dbt" / uuid4().hex)
        self.directory = self.directory.expanduser().resolve()
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.state = {"format": 1, "stage": "reserved"}
        self.config = None
        self.sdk = None
        self.endpoint = None
        self.forward = None
        self.backend = None
        self.handle = None
        self._inputs = None
        self._lock = threading.RLock()
        self._cancelled = threading.Event()
        self._closed = False
        self._save()

    @property
    def receipt(self) -> Path:
        return self.directory / "receipt.json"

    def _save(self):
        payload = json.dumps(self.state, indent=2) + "\n"
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=self.directory, delete=False) as stream:
                temporary = Path(stream.name)
                os.chmod(temporary, 0o600)
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.receipt)
        except BaseException:
            print("Could not save the dbt receipt. Preserve this recovery identity:", file=sys.stderr)
            print(payload, file=sys.stderr)
            raise
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _record_run(self, run):
        self.state["run"] = deepcopy(run)
        self._save()

    def _record_server(self, server):
        self.state["server"] = deepcopy(server)
        self._save()

    def _manager(self):
        cluster = self.state["cluster"]
        return BoundedKubernetesClientManager(
            deadline=lambda: getattr(self, "_api_deadline", None),
            config_file=cluster["kube_config_file"],
            context=cluster["kube_context"],
            in_cluster=cluster["in_cluster"],
            client_configuration=None,
        )

    def _namespace(self):
        namespace = self.state["namespace"]
        with self.sdk.k8s_client_manager.client() as raw:
            try:
                obj = k8s.CoreV1Api(raw).read_namespace(namespace, _request_timeout=_REQUEST)
            except ApiException as error:
                if error.status == 404:
                    return False
                raise
        if obj.metadata.uid != self.state["namespace_uid"]:
            raise RuntimeError("Namespace identity changed; no compute deletion attempted")
        return True

    def ensure_endpoint(self, method: str, compute: dict) -> Endpoint:
        with self._lock:
            if self._closed:
                raise RuntimeError("The managed dbt invocation has already finished")
            if self._inputs is not None:
                if self._inputs != (method, compute):
                    raise ValueError("One dbt invocation cannot mix compute configurations or transports")
                if self.endpoint is None:
                    raise RuntimeError("Server creation already failed; inspect the receipt before retrying")
                if self.forward is not None and self.forward.process.poll() is not None:
                    self._reopen_tunnel()
                return self.endpoint
            config = ComputeConfig.parse(method, compute)
            self.config = config
            self._inputs = (method, deepcopy(compute))
            self.state.update(
                method=method,
                cluster=config.cluster_identity(),
                namespace=config.server.namespace,
                cleanup_timeout=config.cleanup_timeout,
                poll_interval=config.poll_interval,
                stage="creating",
            )
            self.progress.start()
            self.progress.update(
                "requesting",
                f"requesting a {TRANSPORTS[method]} server in namespace {config.server.namespace} "
                f"(startup timeout {config.startup_timeout:g}s)",
            )
            self._api_deadline = time.monotonic() + config.startup_timeout
            self.sdk = RecordingSDK(self._manager(), self._record_run)
            with self.sdk.k8s_client_manager.client() as raw:
                api = k8s.CoreV1Api(raw)
                namespace = api.read_namespace(config.server.namespace, _request_timeout=_REQUEST)
                api.read_namespaced_service_account(
                    config.server.service_account, config.server.namespace, _request_timeout=_REQUEST
                )
            self.state["namespace_uid"] = namespace.metadata.uid
            self._save()
            deadline = self._api_deadline
            try:
                if method == "connect":
                    self.handle = self.sdk.create_connect_server(config.server)
                    self._record_server(self.handle.to_dict())
                    run = self.handle.run
                    service = self.handle.service_name
                    port = self.handle.port
                else:
                    from spark_on_k8s_dbt.thrift_server import ThriftServer

                    self.backend = ThriftServer(self.sdk, config.server, self._record_server)
                    self.handle = self.backend.create()
                    self._record_server(self.handle)
                    run = self.handle["run"]
                    service = self.handle["service"]["name"]
                    port = self.handle["port"]
                pod = run.driver_pod if method == "connect" else run["driver_pod"]
                app_id = run.app_id if method == "connect" else run["app_id"]
                self.progress.update("created", f"driver pod {pod} created")
                self._wait_running(pod, deadline)
                host = f"{service}.{config.server.namespace}.svc"
                if not config.in_cluster:
                    self.progress.update("tunnel", "opening a kubectl port-forward tunnel to the driver")
                    self.forward = PortForward(config, config.server.namespace, pod, port)
                    port = self.forward.start(self._remaining(deadline))
                    host = "127.0.0.1"
                self.endpoint = self._verified_endpoint(host, port, deadline)
                self.state["stage"] = "ready"
                self._save()
                self.progress.update("ready", f"{TRANSPORTS[method]} server ready (application {app_id})")
                return self.endpoint
            except BaseException as error:
                if method == "connect":
                    handle = getattr(error, "handle", None)
                    run = getattr(error, "run", None)
                    if handle is not None:
                        self.handle = handle
                        self.state["server"] = handle.to_dict()
                    if run is not None:
                        self.state["run"] = run.to_dict()
                    if getattr(error, "cleanup", None):
                        self.state["creation_cleanup"] = error.cleanup
                else:
                    handle = getattr(error, "handle", None)
                    if handle is not None:
                        self.handle = handle
                        self.state["server"] = deepcopy(handle)
                        if handle.get("creation_cleanup"):
                            self.state["creation_cleanup"] = deepcopy(handle["creation_cleanup"])
                self.state["creation_error"] = type(error).__name__
                try:
                    self._save()
                except BaseException as publication_error:
                    error.add_note(
                        f"Recovery receipt publication also failed: {type(publication_error).__name__}"
                    )
                raise

    def prepare_next_command(self):
        """Before a later dbt command of the same run, confirm it can reuse the server.

        Nothing is checked before compute is requested; it is still created lazily
        by the first connection. A failed startup, or a driver that is gone, not
        running, being deleted or replaced, ends the run: a run never creates a
        second server. An exited tunnel is reopened to the same verified driver.
        """
        with self._lock:
            if self._closed or self._cancelled.is_set():
                raise RuntimeError("The managed dbt invocation has already finished")
            if self._inputs is None:
                return
            if self.endpoint is None:
                raise RuntimeError(
                    f"Managed Spark startup failed in an earlier command; inspect {self.receipt}"
                )
            run = self.state["server"]["run"]
            self._api_deadline = time.monotonic() + self.config.startup_timeout
            with self.sdk.k8s_client_manager.client() as raw:
                try:
                    observed = k8s.CoreV1Api(raw).read_namespaced_pod(
                        run["driver_pod"], self.config.server.namespace, _request_timeout=_REQUEST
                    )
                except ApiException as error:
                    if error.status != 404:
                        # Uncertain, not absent: stop without echoing the response body.
                        raise RuntimeError(
                            "Could not confirm that the managed Spark driver is still running "
                            f"(Kubernetes API status {error.status})"
                        ) from None
                    observed = None
            if observed is None:
                raise RuntimeError("The managed Spark driver no longer exists")
            if observed.metadata.uid != run["driver_uid"]:
                raise RuntimeError("Driver identity changed; the replacement is not adopted")
            if observed.metadata.deletion_timestamp or observed.status.phase != "Running":
                raise RuntimeError(
                    f"The managed Spark driver is no longer running (phase {observed.status.phase}"
                    f"{', deleting' if observed.metadata.deletion_timestamp else ''})"
                )
            if self.forward is not None and self.forward.process.poll() is not None:
                self._reopen_tunnel()

    def cancel(self):
        """Stop an in-progress startup at its next bounded wait; never deletes compute."""
        self._cancelled.set()

    @property
    def cancelled(self) -> bool:
        """True after an interrupt or cleanup began: no new dbt connection may start."""
        return self._cancelled.is_set()

    def _remaining(self, deadline):
        if self._cancelled.is_set():
            raise RuntimeError("Managed Spark startup was cancelled")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Managed Spark startup timed out")
        return remaining

    def _wait_running(self, pod, deadline):
        while True:
            self._remaining(deadline)
            with self.sdk.k8s_client_manager.client() as raw:
                observed = k8s.CoreV1Api(raw).read_namespaced_pod(
                    pod, self.config.server.namespace, _request_timeout=_REQUEST
                )
            run = self.state["server"]["run"]
            if observed.metadata.uid != run["driver_uid"]:
                raise RuntimeError("Driver identity changed during startup")
            if observed.status.phase in {"Failed", "Succeeded"} or observed.metadata.deletion_timestamp:
                raise RuntimeError("Managed Spark driver terminated before becoming ready")
            self.progress.update(*driver_state(observed))
            if observed.status.phase == "Running":
                return
            self._cancelled.wait(min(self.config.poll_interval, self._remaining(deadline)))

    def _verified_endpoint(self, host, port, deadline):
        """Query this application's identity through the endpoint before handing it out."""
        profile = None
        if self.state["method"] == "connect":
            while True:
                self._waiting_for_endpoint()
                port = self._refresh_tunnel(port, deadline)
                profile = ConnectProfile(
                    endpoint=f"sc://{host}:{port}", tls=False, allow_insecure=self.config.in_cluster
                )
                try:
                    self.sdk.wait_connect_server_ready(
                        self.handle,
                        profile=profile,
                        timeout=min(5, self._remaining(deadline)),
                        poll_interval=self.config.poll_interval,
                    )
                except ConnectServerTimeout:
                    self._remaining(deadline)
                else:
                    break
        else:
            port = self._wait_thrift(host, port, deadline)
            if self.backend.status(self.handle)["status"] != "running":
                raise RuntimeError("Thrift resources changed during readiness verification")
        return Endpoint(host, port, profile)

    def _waiting_for_endpoint(self):
        self.progress.update(
            "endpoint", f"waiting for the {TRANSPORTS[self.state['method']]} endpoint to answer a query"
        )

    def _reopen_tunnel(self):
        # kubectl can exit mid-run, for example when its API stream is lost.
        # Statements on the old tunnel have failed and are never replayed. Later
        # connections get a new tunnel only to the same verified driver UID.
        deadline = self._api_deadline = time.monotonic() + self.config.startup_timeout
        self.progress.start()
        self.progress.update(
            "reopening", "kubectl port-forward exited; reopening the tunnel to the same driver"
        )
        self._wait_running(self.forward.pod, deadline)
        self.forward.close()
        port = self.forward.start(self._remaining(deadline))
        try:
            self.endpoint = self._verified_endpoint("127.0.0.1", port, deadline)
        except BaseException:
            # A live but unverified tunnel would leave the stale port in use;
            # closing it makes the next open retry.
            self.forward.close()
            raise
        self.progress.update("ready", "tunnel reopened; the same driver answered a query")

    def _refresh_tunnel(self, port, deadline):
        # kubectl can exit after publishing its local listener if Spark has not
        # bound the remote socket yet. Recreate that invocation-owned tunnel.
        if self.forward is not None and self.forward.process.poll() is not None:
            self.progress.update("tunnel_reopening", "kubectl port-forward exited; reopening the tunnel")
            self.forward.close()
            return self.forward.start(self._remaining(deadline))
        return port

    def _wait_thrift(self, host, port, deadline):
        # A subprocess bounds PyHive's blocking socket operations and is always reaped.
        probe = """import sys
from pyhive import hive
connection = hive.Connection(host=sys.argv[1], port=int(sys.argv[2]), username='dbt', auth='NONE')
try:
    cursor = connection.cursor()
    cursor.execute('SELECT 1')
    assert cursor.fetchall() == [(1,)]
    cursor.execute('SET spark.app.id')
    assert cursor.fetchall() == [('spark.app.id', sys.argv[3])]
finally:
    connection.close()
"""
        while True:
            self._waiting_for_endpoint()
            port = self._refresh_tunnel(port, deadline)
            try:
                result = subprocess.run(
                    [
                        sys.executable,
                        # As the Connect check: no working directory on sys.path, but the caller's
                        # PYTHONPATH and user site, so the probe imports the client dbt imports.
                        "-P",
                        *(["-E"] if sys.flags.ignore_environment else []),
                        *(["-s"] if sys.flags.no_user_site else []),
                        "-c",
                        probe,
                        host,
                        str(port),
                        self.state["server"]["run"]["app_id"],
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=min(5, self._remaining(deadline)),
                    check=False,
                )
                if result.returncode == 0:
                    return port
            except subprocess.TimeoutExpired:
                pass
            self._cancelled.wait(min(self.config.poll_interval, self._remaining(deadline)))

    def _connect_remaining(self, handle):
        # Persist transitive ownership before deletion: executor ConfigMaps may
        # outlive the executor Pod that named their owner UID.
        known = set(self.state.get("owned_uids", []))
        known.update([handle.run.driver_uid, handle.driver_service_uid, handle.service_uid])
        objects = []
        with self.sdk.k8s_client_manager.client() as raw:
            api = k8s.CoreV1Api(raw)
            for kind in ("pod", "service", "config_map"):
                objects.extend(
                    getattr(api, f"list_namespaced_{kind}")(
                        handle.run.namespace, _request_timeout=_REQUEST
                    ).items
                )
        previous = None
        while previous != known:
            previous = set(known)
            for obj in objects:
                if (obj.metadata.labels or {}).get("spark-app-id") == handle.run.app_id or any(
                    owner.uid in known for owner in obj.metadata.owner_references or []
                ):
                    known.add(obj.metadata.uid)
        recorded = sorted(known)
        if self.state.get("owned_uids") != recorded:
            self.state["owned_uids"] = recorded
            self._save()
        return [obj.metadata.name for obj in objects if obj.metadata.uid in known]

    def _wait_connect_deleted(self, handle):
        deadline = time.monotonic() + self.state["cleanup_timeout"]
        while time.monotonic() < deadline:
            status = self.sdk.connect_server_status(
                handle, timeout=min(10, max(0.01, deadline - time.monotonic()))
            )
            remaining = self._connect_remaining(handle)
            if set(status["missing"]) == {"driver", "driver_service", "connect_service"} and not remaining:
                return
            time.sleep(min(self.state["poll_interval"], max(0, deadline - time.monotonic())))
        raise TimeoutError("Compute deletion was requested but resource disappearance was not observed")

    def _validate_identity(self):
        server = self.state.get("server")
        if not isinstance(server, dict) or not isinstance(server.get("run"), dict):
            raise ValueError("Receipt requires a complete server identity")
        if self.state.get("namespace") != server["run"].get("namespace"):
            raise ValueError("Receipt namespace differs from its server identity")
        if self.state["method"] == "connect":
            ConnectServerHandle.from_dict(server)
        elif self.state["method"] == "thrift":
            from spark_on_k8s_dbt.thrift_server import _validate_handle

            _validate_handle(server)
            if self.state.get("namespace_uid") != server["namespace_uid"]:
                raise ValueError("Receipt namespace UID differs from its server identity")
        else:
            raise ValueError("Unknown managed dbt transport")

    @contextmanager
    def _cleanup_lock(self, dbt_exit_code):
        timeout = self.state.get("cleanup_timeout", 120)
        if not self._lock.acquire(timeout=timeout):
            self._closed = True
            self.state.update(stage="cleanup_failed", cleanup={"status": "failed", "error": "owner_busy"})
            if dbt_exit_code is not None:
                self.state["dbt_exit_code"] = dbt_exit_code
            self._save()
            raise RuntimeError(f"Compute startup is still busy; retain and inspect {self.receipt}")
        try:
            yield
        finally:
            self._lock.release()

    def close(self, *, dbt_exit_code=None):
        # A worker can still hold the lock while waiting for a slow driver. Make it
        # stop at its next poll so cleanup can delete what it created.
        self.cancel()
        with self._cleanup_lock(dbt_exit_code):
            if self._closed:
                return
            self._closed = True
            self._api_deadline = time.monotonic() + self.state.get("cleanup_timeout", 120)
            if dbt_exit_code is not None:
                self.state["dbt_exit_code"] = dbt_exit_code
            errors = []
            try:
                if self.state.get("server"):
                    self._validate_identity()
                    self.progress.start()
                    self.progress.update(
                        "deleting", "deleting managed Spark compute and observing its removal"
                    )
                    if self._namespace():
                        if self.state["method"] == "connect":
                            handle = ConnectServerHandle.from_dict(self.state["server"])
                            self._connect_remaining(handle)
                            self.sdk.delete_connect_server(handle)
                            self._wait_connect_deleted(handle)
                        else:
                            from spark_on_k8s_dbt.thrift_server import ThriftServer

                            backend = self.backend or ThriftServer(self.sdk, None, self._record_server)
                            backend.delete(self.state["server"])
                            backend.wait_deleted(
                                self.state["server"],
                                timeout=self.state["cleanup_timeout"],
                                poll_interval=self.state["poll_interval"],
                            )
                    self.state.update(stage="cleaned", cleanup={"status": "deleted"})
                    self.progress.update("deleted", "managed Spark compute deleted")
                elif self.state.get("run"):
                    raise RuntimeError(
                        "Creation is incomplete; inspect the recorded identities before manual cleanup"
                    )
                else:
                    self.state.update(stage="not_started", cleanup={"status": "not_started"})
            except BaseException as error:
                errors.append(error)
                self.state.update(
                    stage="cleanup_failed", cleanup={"status": "failed", "error": type(error).__name__}
                )
            finally:
                if self.forward is not None:
                    try:
                        self.forward.close()
                    except BaseException as error:
                        errors.append(error)
                        self.state.update(stage="cleanup_failed", tunnel_cleanup="failed")
                try:
                    self._save()
                except BaseException as error:
                    errors.append(error)
            if errors:
                raise RuntimeError(f"Managed dbt cleanup failed; inspect {self.receipt}") from errors[0]

    @classmethod
    def recover(cls, receipt: str | Path):
        path = Path(receipt).expanduser().resolve(strict=True)
        if path.stat().st_size > 1024 * 1024:
            raise ValueError("Managed dbt receipt exceeds 1 MiB")
        state = json.loads(path.read_text())
        if not isinstance(state, dict) or state.get("format") != 1:
            raise ValueError("Unsupported managed dbt receipt format")
        if state.get("method") not in {"connect", "thrift"} or not state.get("server"):
            raise ValueError("Receipt has no complete server identity; inspect resources manually")
        if state.get("namespace") != state["server"].get("run", {}).get("namespace"):
            raise ValueError("Receipt namespace differs from its server identity")
        cluster = state.get("cluster")
        if not isinstance(cluster, dict) or set(cluster) != {
            "kube_config_file",
            "kube_context",
            "in_cluster",
        }:
            raise ValueError("Receipt requires explicit cluster identity")
        # Reuse validation for targeting and deadlines without trusting arbitrary objects.
        if type(cluster["in_cluster"]) is not bool:
            raise ValueError("Invalid receipt cluster mode")
        if not cluster["in_cluster"] and not all(
            isinstance(cluster[k], str) and cluster[k] for k in ("kube_config_file", "kube_context")
        ):
            raise ValueError("Receipt requires explicit kubeconfig/context")
        if not isinstance(state.get("namespace_uid"), str) or not state["namespace_uid"]:
            raise ValueError("Receipt requires namespace UID")
        owner = cls.__new__(cls)
        owner.directory, owner.state = path.parent, state
        if path.name != "receipt.json":
            raise ValueError("Recovery expects the original receipt.json filename")
        owner.config = owner.endpoint = owner.forward = owner.backend = owner.handle = None
        owner.progress = StartupProgress.silent()
        owner._inputs = None
        owner._lock = threading.RLock()
        owner._cancelled = threading.Event()
        owner._closed = False
        owner._validate_identity()
        if cluster["in_cluster"] and (
            cluster["kube_config_file"] is not None or cluster["kube_context"] is not None
        ):
            raise ValueError("In-cluster receipt cannot also select kubeconfig/context")
        owner.sdk = RecordingSDK(owner._manager(), owner._record_run)
        # Do not allow a modified receipt to disable bounded cleanup.
        for key, default in (("cleanup_timeout", 120), ("poll_interval", 1)):
            value = state.get(key, default)
            if type(value) not in {int, float} or not 0 < value <= 3600:
                raise ValueError("Invalid receipt cleanup deadline")
            state[key] = value
        return owner
