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

## Private images, credentials and node placement

A server for real data or ML work often needs an image from a private registry,
credentials such as object-storage keys, and nodes from a particular pool, for example
GPU nodes. Use the typed settings below instead of Spark properties: `spark_conf`
cannot set `spark.kubernetes.*` properties, and literal credentials in Spark
properties appear in driver arguments and diagnostics.

| Purpose | CLI option (repeatable) | `ConnectServerConfig` field | Value |
| --- | --- | --- | --- |
| Pull from a private registry | `--image-pull-secret NAME` | `image_pull_secrets` | Existing image pull Secret names, used by the driver and executors |
| Credentials as environment variables | `--driver-env-from-secret NAME=SECRET:KEY`, `--executor-env-from-secret NAME=SECRET:KEY` | `driver_env_from_secrets`, `executor_env_from_secrets` | Variable name mapped to a key of an existing Secret |
| Select nodes | `--driver-node-selector KEY=VALUE`, `--executor-node-selector KEY=VALUE` | `driver_node_selector`, `executor_node_selector` | Node label keys and values |
| Allow tainted nodes | `--driver-toleration`, `--executor-toleration` as `KEY[=VALUE][:EFFECT]` | `driver_tolerations`, `executor_tolerations` | Kubernetes toleration mappings |

The helper only references Secrets: it never creates, reads or copies them, and
their values never pass through the SDK, CLI, receipts, plans or logs. Kubernetes
resolves each `SECRET:KEY` reference when it starts a container. An administrator
creates the Secrets in the server's namespace, for example from files so that
values stay out of command lines and shell history:

```bash
kubectl --namespace spark create secret generic private-registry \
  --type=kubernetes.io/dockerconfigjson --from-file=.dockerconfigjson=./registry-config.json
kubectl --namespace spark create secret generic s3-credentials \
  --from-file=access-key=./access-key.txt --from-file=secret-key=./secret-key.txt
```

See Kubernetes' guide to
[pulling an image from a private registry](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/)
for the registry configuration file.

Every client of a server runs code in its driver and executors, so any client can
read these variables, for example with a query. Give a server only credentials that
all of its users may hold; a shared server is not a tenant-isolation boundary.

Create a server on labeled, tainted GPU nodes with a private image and storage
credentials:

```bash
spark-on-k8s connect server create \
  --image registry.example.com/data/spark-connect-ml:4.3.0 \
  --server-jar local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar \
  --namespace spark --service-account spark --name ml-notebook \
  --image-pull-secret private-registry \
  --driver-env-from-secret AWS_ACCESS_KEY_ID=s3-credentials:access-key \
  --driver-env-from-secret AWS_SECRET_ACCESS_KEY=s3-credentials:secret-key \
  --executor-env-from-secret AWS_ACCESS_KEY_ID=s3-credentials:access-key \
  --executor-env-from-secret AWS_SECRET_ACCESS_KEY=s3-credentials:secret-key \
  --executor-node-selector example.com/pool=gpu \
  --executor-toleration nvidia.com/gpu:NoSchedule \
  --receipt-file ml-server.json --output json
```

In the SDK, the same settings are fields of `ConnectServerConfig`:

```python
ConnectServerConfig(
    image="registry.example.com/data/spark-connect-ml:4.3.0",
    server_jar="local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar",
    namespace="spark",
    service_account="spark",
    app_name="ml-notebook",
    image_pull_secrets=["private-registry"],
    driver_env_from_secrets={"AWS_ACCESS_KEY_ID": "s3-credentials:access-key"},
    executor_env_from_secrets={"AWS_ACCESS_KEY_ID": "s3-credentials:access-key"},
    executor_node_selector={"example.com/pool": "gpu"},
    executor_tolerations=[{"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"}],
)
```

A node selector restricts pods to nodes with every listed label; a toleration only
permits scheduling on nodes with a matching taint. To dedicate a pool, use both.
In the CLI, `KEY=VALUE:EFFECT` creates an `Equal` toleration and `KEY:EFFECT` an
`Exists` toleration; omitting the effect tolerates every effect. Use the SDK for
`tolerationSeconds` or a key-less toleration. The mapping reuses existing
submission mechanisms:

- The driver gets `imagePullSecrets` and `valueFrom.secretKeyRef` variables through a
  generated [driver pod template](driver_pod_templates.md), and node selectors and
  tolerations through the submission options of the same name.
- Executors are created by Spark. They use Spark's
  `spark.kubernetes.container.image.pullSecrets`,
  `spark.kubernetes.executor.secretKeyRef.NAME` and
  `spark.kubernetes.executor.node.selector.KEY` properties. Spark has no property for
  tolerations, so executor tolerations use an executor pod template, stored in a
  ConfigMap owned by the driver. Creating it requires your permission to create and
  patch ConfigMaps in the namespace; Kubernetes deletes it with the driver.

Validation happens before any Kubernetes request: Secret and label syntax, toleration
fields and operators, and variable names. Variable names are identifiers
(`[A-Za-z_][A-Za-z0-9_]*`). Names starting with `SPARK_` are rejected because Spark
and this helper set such variables. An executor variable cannot also be set with
`spark.executorEnv.NAME` in `spark_conf`. These settings map individual Secret keys;
the application option `driver_env_vars_from_secrets` instead imports every key of
a Secret into the driver.

Kubernetes, not this helper, checks that the referenced Secrets, keys, labels and
taints exist. A missing pull Secret leaves the driver in `ErrImagePull` or
`ImagePullBackOff`; a missing variable Secret or key stops the container from starting
(`CreateContainerConfigError`); unmatched node selectors leave pods `Pending` as
unschedulable. Readiness then times out and keeps the server. Run
`spark-on-k8s app diagnose --namespace spark --app-id APP_ID` for the driver, or
inspect executor pod events, then delete the server before creating a corrected one.

### Preview a server offline

`connect server plan` accepts the same server options as `create`, without a receipt
or cluster selection. It opens no Kubernetes connection and prints a redacted preview
of the driver Pod, both Services and, with executor tolerations, the executor template
ConfigMap that creation would request, as YAML or, with `--output json`, as a
[versioned document](automation.md) with `"operation": "connect_server_plan"`:

```bash
spark-on-k8s connect server plan \
  --image registry.example.com/data/spark-connect-ml:4.3.0 \
  --server-jar local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar \
  --namespace spark --service-account spark --name ml-notebook \
  --image-pull-secret private-registry \
  --executor-env-from-secret AWS_ACCESS_KEY_ID=s3-credentials:access-key \
  --executor-toleration nvidia.com/gpu:NoSchedule --output json
```

The document contains `plan` and `pod_settings`, which lists the typed settings
above: Secret names and keys, never values. Plan redaction hides fields whose names
suggest credentials, such as the driver's `imagePullSecrets` and Spark properties
naming Secrets, and the contents of ConfigMaps, so `pod_settings` is where to review
those references. Identifiers in the plan, such as the `-preview` application ID,
are illustrative. The SDK equivalent `client.plan_connect_server(config)` returns
an `ApplicationPlan`; its raw `resources` are unredacted, which for these settings
still contain only references.

For an image with Python ML libraries and GPU executors, see
[Build an ML image for Spark Connect](connect_ml_image.md).

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

Status reports observed resource presence and driver phase, including missing or
terminating resources. It also reports the driver's `created_at` time (UTC) and
`age_seconds`, computed with your local clock and never negative; both are `null`
when the driver is missing. Status does not run a query, so it cannot learn the
server's Spark version: `spark_version` is reported only from a `spark-version`
label on the driver pod, which this helper does not set, and is otherwise `null`.
Use `connect server wait` for the server-reported version and version warnings.
Permission errors and unreachable Kubernetes APIs remain errors, not evidence that
a server has disappeared. Destructive operations verify
all recorded resource identities before issuing UID-conditional deletion requests;
a stale receipt cannot delete a same-name replacement.

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
