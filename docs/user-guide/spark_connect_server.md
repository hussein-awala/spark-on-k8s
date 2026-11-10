# Create a Spark Connect server on Kubernetes

Create a private Spark Connect server when you want notebooks or Python tools to
share a long-running Spark driver. The server launches real Kubernetes executors
using the existing submission machinery. The separate
[connection profiles guide](spark_connect.md) covers client sessions and existing
services. For the complete local notebook workflow, including an isolated kernel,
planned restart and cleanup, use [Jupyter notebooks on Kubernetes](jupyter.md).

This feature creates one driver and its private Services in an existing namespace.
It does not restart a failed driver or restore sessions after a driver replacement.
Use a new server receipt when replacing a server. Server pools, admission queues,
authentication gateways and the Helm chart are outside this lifecycle helper.

## Prerequisites

- Prepare the namespace and driver service account using the existing
  [namespace setup guide](getting_started.md#set-up-the-kubernetes-namespace).
  The driver needs permission to create and manage its executors. The caller needs
  permission to create/read/delete driver pods and Services in that namespace, and
  to list pods there for `connect server list`.
- Use a Spark 4 image containing the Connect server JAR. Specify its exact
  `local:///` path; the helper does not download a guessed Spark/Scala artifact.
  Kubernetes integration tests cover the pinned Spark 4.0.5 and 4.3.0 images in
  `.github/workflows/spark-connect.yml`.
- Creation, status and deletion use the base package and do not need local Java or
  PySpark. Install `spark-on-k8s[connect]` on Python 3.11–3.14 for query readiness
  checks and interactive sessions. A separately installed compatible Connect
  client can also be used; see the [client installation boundary](spark_connect.md).

The generated Connect Service is `ClusterIP`, with no public load balancer or
Ingress. It serves plaintext gRPC inside the cluster. Use it only within a trusted
network; anyone able to reach it has access to the server's execution environment
and data permissions. It is not a tenant-isolation boundary. An external TLS and
authentication gateway remains an administrator responsibility. No credentials are
stored in the server receipt.

## Create and connect from the CLI

Create a server and save its identity in a new file:

```bash
spark-on-k8s connect server create \
  --image apache/spark:4.3.0-scala2.13-java21-python3-ubuntu \
  --server-jar local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar \
  --namespace spark --service-account spark \
  --name notebook --executors 1 \
  --driver-memory 1024 --executor-memory 1024 \
  --receipt-file connect-server.json --output json
```

The result contains the private endpoint and a `server` handle. Creation means
that Kubernetes accepted the resources; `query_ready` is initially `false`.
The receipt is published atomically without replacing an existing file. Keep it
for status, readiness and deletion: it records the driver and Service UIDs, not
just their names.

Use `--kubeconfig FILE --context NAME` on each server command to select an explicit
cluster. Otherwise, the project's usual Kubernetes connection settings apply.
Server workload settings are taken from the server configuration, rather than
inheriting unrelated batch-submission defaults from environment variables.

For a notebook on your machine, keep a separate terminal running a tunnel. Replace
`SERVICE_NAME` with `server.service_name` from the receipt:

```bash
kubectl --namespace spark port-forward service/SERVICE_NAME 15002:15002
```

Use the same kubeconfig and context as the creation command. The tunnel is managed
by you; closing it does not stop the server. Spark can take time to start after
creation. If the tunnel exits with a connection-refused error during startup,
inspect the driver logs and restart the tunnel once the gRPC listener has started.
Create `connect.toml`:

```toml
[profiles.notebook]
endpoint = "sc://127.0.0.1:15002"
tls = false
```

Then verify this particular server and open a session:

```bash
spark-on-k8s connect server wait connect-server.json \
  --profile-file connect.toml --profile notebook --timeout 120 --output json
```

```python
from spark_on_k8s.connect import ConnectProfile

profile = ConnectProfile.from_file("connect.toml", profile="notebook")
with profile.session() as spark:
    assert spark.range(100).groupBy().sum("id").first()[0] == 4950
    spark.sql("SELECT 'connected' AS status").show()
```

Readiness checks a real remote action and verifies the remote `spark.app.id`
against the receipt. A successful query against a different server is rejected.
Resource identities are checked again before reporting readiness. A Kubernetes
`Running` phase alone does not establish query readiness.

The readiness result reports the `spark_version` returned by the server, the local
`client_version` that ran the query and `warnings`. A client and server that differ
in major or minor version produce a `version_skew` warning, as described in
[client and server versions](spark_connect.md#client-and-server-versions); readiness
still succeeds. `connect server wait` prints the warning on stderr in text mode and
copies it to a top-level `warnings` array in JSON.

Inside the cluster, use the returned private endpoint in a profile with
`tls = false` and an explicit `allow_insecure = true` only for your trusted network.
Files, connectors and data permissions belong to the server image and service
account; your local notebook's filesystem is not automatically available there.

## Python SDK

```python
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.connect import ConnectProfile
from spark_on_k8s.connect_server import ConnectServerConfig

client = SparkOnK8S()
server = client.create_connect_server(
    ConnectServerConfig(
        image="apache/spark:4.3.0-scala2.13-java21-python3-ubuntu",
        server_jar="local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar",
        namespace="spark",
        service_account="spark",
        app_name="notebook",
        driver_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_resources=PodResources(cpu=1, memory=1024, memory_overhead=512),
        executor_instances=ExecutorInstances(initial=1),
        spark_conf={"spark.sql.shuffle.partitions": "4"},
    )
)
print(server.endpoint)
print(server.to_dict())  # Persist this identity before waiting.

# After establishing a tunnel to server.service_name:
profile = ConnectProfile(endpoint="sc://127.0.0.1:15002", tls=False)
client.wait_connect_server_ready(server, profile=profile, timeout=120)
with profile.session() as spark:
    assert spark.range(10).count() == 10
```

`ConnectServerHandle.from_dict()` restores a saved identity. For the CLI receipt,
pass its inner `server` mapping: the file envelope is
`{"schema_version": 1, "server": {...}}`. Resource options use
the same `PodResources` and `ExecutorInstances` types as application submission.
Additional Spark settings and packages are explicit; settings that would change
the managed master, identity, driver networking, Connect port or resource contract
are rejected before creating resources. The explicit exception
`spark.kubernetes.executor.request.cores` accepts a positive Kubernetes CPU quantity
no greater than the configured executor cores, so fractional scheduling requests are
possible while executor cores remain managed by `PodResources`.

## Observe, stop and recover

```bash
spark-on-k8s connect server status connect-server.json --output json
spark-on-k8s connect server delete connect-server.json --output json
```

The SDK equivalents are `connect_server_status(server)` and
`delete_connect_server(server)`. Deletion requests shutdown of the recorded driver
and cleanup of its owned Services/executors. It reports a deletion request, not
instantaneous completion. Closing a client session or tunnel leaves the server
running. Keep the receipt until Kubernetes has removed the resources.

To bound a server that might be forgotten, or whose receipt or client is lost, add
`--max-runtime SECONDS` to `create` (`max_runtime` in `ConnectServerConfig`). Kubernetes
stops the driver that long after its pod starts, whether or not anyone deletes it.
Choose a value longer than any session you need: open sessions then fail and the
server is not restarted. Status reports `terminated` with `driver_reason:
"DeadlineExceeded"`. The stopped driver pod and its Services remain until `delete`
(or [`apps prune`](automation.md#prune-finished-or-orphaned-drivers)) removes them;
see [driver lifetime limits](automation.md#limit-how-long-a-driver-can-run).

Status reports observed resource presence, driver phase and the driver pod's
Kubernetes `driver_reason` (or `null`), including missing or terminating resources.
It also reports the driver's `created_at` time (UTC) and `age_seconds`, computed
with your local clock and never negative; both are `null` when the driver is
missing. Status does not run a query, so it cannot learn the server's Spark version:
`spark_version` is reported only from a `spark-version` label on the driver pod,
which this helper does not set, and is otherwise `null`. Use `connect server wait`
for the server-reported version and version warnings. Permission errors and
unreachable Kubernetes APIs remain errors, not evidence that a server has
disappeared. Destructive operations verify all recorded resource identities before
issuing UID-conditional deletion requests; a stale receipt cannot delete a same-name
replacement.

A readiness timeout exits with code 5 and leaves the server running. Inspect the
driver logs or diagnostics, repair connectivity or configuration, and retry the
check. Do not create another server simply because a readiness check timed out.
Without a compatible local client, the wait fails immediately with exit code 1 and
the client installation message instead of polling until its timeout.
Creation failures include their phase, available partial identity and cleanup
outcome. If creation confirmed ownership of a driver and later setup fails, the
helper attempts to remove only that owned driver. An uncertain create response is
not permission to adopt or delete a resource by name. Failed or uncertain cleanup
requires inspection of the reported resource identity before retrying.

Server CLI commands emit one versioned JSON document with `--output json`.
Runtime failures retain the available server or partial run identity. Exit codes
are 0 for an accepted operation, 1 for an operational error, 2 for CLI usage errors,
4 for a Kubernetes not-found error, 5 for a timeout and 130 for interruption.
Interruption retains any confirmed partial identity and does not initiate extra
cleanup. If it interrupts rollback or deletion already in progress, that request
may have reached Kubernetes; inspect the reported cleanup outcome.
Status can successfully report missing resources without treating that observation as a transport error.

## Find forgotten servers

A server keeps its driver and executors running after the notebook, tunnel or
script that created it has gone. To find servers in a namespace without a receipt:

```bash
spark-on-k8s connect server list --namespace spark
```

```text
APP ID                                     STATUS   AGE   CREATED               SPARK  IMAGE
notebook-6f0c3b8e2d4a4f1c9e7b5a3d1f2e4c6b  running  3d4h  2026-11-06T08:00:00Z  -      apache/spark:4.3.0-scala2.13-java21-python3-ubuntu
```

The command is read-only. It lists driver pods labeled `spark-role=driver` and
`spark-on-k8s.io/connect-server=true` with an application ID, oldest first. Servers
created through the CLI, SDK, notebook example or tools built on them carry these
labels. It does not verify receipts, Services or query readiness, and it never
deletes anything. Labels are not proof of ownership: anyone able to create pods in
the namespace can apply them. The helper records no creator identity, so choose a
recognizable `--name` (`name` in the list) when creating a server.

With `--output json`, the [versioned document](automation.md) has
`"operation": "connect_server_list"`, the namespace and `items` (`null` on error). Each item
contains `app_id`, `name`, `namespace`, `driver_pod`, `driver_uid`, `status`
(`starting`, `running`, `terminated`, `deleting` or `unknown`), `driver_phase`,
`spark_version` (from the optional `spark-version` label, usually `null`), the driver
`image`, `created_at` and `age_seconds`. Exit codes match the other server commands.
The SDK equivalent `client.list_connect_servers("spark")` returns the same items.
Each call inspects one namespace. A cluster administrator can find servers in every
namespace with
`kubectl get pods --all-namespaces -l spark-role=driver,spark-on-k8s.io/connect-server=true`.

To stop a server you no longer need:

1. Prefer its receipt, such as `connect-server.json` or the notebook example's
   `server-receipt.json`: `spark-on-k8s connect server delete RECEIPT` verifies
   every recorded identity before requesting deletion.
2. If the receipt is lost, confirm that nobody still uses the server: deletion ends
   every session on it. Then delete its driver by the listed application ID:
   `spark-on-k8s app delete --namespace spark --app-id APP_ID`. The command looks up
   that single driver and deletes it with its UID as a precondition. Kubernetes then
   garbage-collects the Services and executors it owns, asynchronously.
3. Run `connect server list` again. The server shows `deleting` until Kubernetes
   removes the driver, then disappears from the list. A `terminated` server has
   stopped Spark but its resources remain until deleted.

For upstream configuration details, see the
[Spark Connect server configuration](https://spark.apache.org/docs/4.3.0/configuration.html#spark-connect)
and [Kubernetes client-mode requirements](https://spark.apache.org/docs/4.3.0/running-on-kubernetes.html#client-mode).
