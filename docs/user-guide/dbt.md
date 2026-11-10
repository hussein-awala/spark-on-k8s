# Experimental managed dbt

Run a dbt Core command against temporary Spark compute on an existing Kubernetes
cluster. The wrapper creates one server when dbt first opens a connection, shares
it across dbt worker threads, and deletes its driver, executors and private
Services when the command ends. Choose **Thrift** or **Spark Connect** in the
profile; you do not create the server yourself.

!!! warning "Experimental, separately installed integration"
    This feature lives in `integrations/dbt` and is not part of a normal
    `spark-on-k8s` installation. Its profile and Python interfaces may change.
    The initial compatibility target is dbt Core **1.12.6**, dbt-spark **1.11.0**,
    Spark/client **4.3.0**, PyHive **0.7.0**, and Thrift **0.25.0**. It is a Python
    dbt Core adapter, not a dbt Fusion, dbt Cloud or dbt v2 integration. SQL models, seeds and data tests are supported;
    Python models are explicitly rejected. `dbt clone` is explicitly rejected in this experiment.

## Prepare the platform

The platform owner supplies:

- An existing namespace and driver service account with the permissions described
  in [server setup](spark_connect_server.md). This integration does not create or
  delete the Kubernetes cluster, namespace or RBAC.
- A compatible Spark image with the bundled Connect or HiveThriftServer2 JAR.
- A **shared warehouse** reachable by the driver and executors and an **external
  Hive metastore** reachable by each new driver. The profile requires both. An
  embedded driver-local metastore would disappear when the driver is deleted.
  Driver-local warehouse paths, including `file://`, are rejected because this
  experiment does not mount persistent driver volumes. Supply a remote filesystem
  URI and its Spark connector; the integration fixture tests HDFS.
- A trusted network for the private, plaintext server and catalog endpoints.
  The experiment uses Thrift `auth: NONE`; it is not a multi-tenant gateway.

For a local client, install `kubectl` and provide an explicit kubeconfig path and
context. The wrapper opens a loopback-only tunnel to its driver, using an
OS-assigned local port. If `kubectl` exits mid-run, statements on that tunnel fail
and are not replayed; the next dbt connection gets a new tunnel after the driver
UID and application identity are verified again. For a client already inside
Kubernetes, set `in_cluster: true` and omit both kubeconfig fields; it connects to the private
Service directly. The client's identity needs permission to create/read/delete
its driver and Services, read the namespace/service account and observe executor
pods, Services and ConfigMaps (including list permission for garbage-collection
checks); local access also needs pod port-forward permission.
[`spark-on-k8s-dbt check`](#check-a-profile-before-running) verifies these
permissions without creating anything.

## Install from the repository

Until the experimental distribution is separately published, build both wheels
from the same checkout. These commands use an isolated Python 3.12 environment:

```bash
# From the spark-on-k8s repository root:
uv build --wheel --out-dir dist/dbt-experiment
uv build --project integrations/dbt --wheel --out-dir dist/dbt-experiment
uv venv --python 3.12 .venv-dbt
uv pip install --python .venv-dbt/bin/python dist/dbt-experiment/*.whl
uv pip check --python .venv-dbt/bin/python
```

For reproducible development/testing, use the separate lockfile instead:

```bash
UV_PROJECT_ENVIRONMENT="$PWD/.venv-dbt" uv sync \
  --project integrations/dbt --locked --python 3.12 --group test
```

The integration declares PyHive and patched Thrift directly instead of selecting
`dbt-spark[pyhive]`, whose current extra caps Thrift below the patched releases.
Do not add that extra to this environment: let the normal dependency resolver
keep the transport compatible. Thrift's protocol size/recursion protections remain
enabled; unusually large individual result fields can be rejected by those limits.

The package accepts Python 3.11–3.13. Kubernetes execution is tested with Python
3.12. The independent lockfile prevents dbt's dependencies from changing the
normal SDK, API or Airflow environments.

## Configure a managed profile

Use this profile in your dbt project's `profiles.yml`, replacing the namespace,
service account, kubeconfig, metastore and warehouse with your platform values:

```yaml
analytics:
  target: connect
  outputs:
    connect:
      type: spark_on_k8s
      method: connect
      schema: analytics
      threads: 2
      compute:
        kube_config_file: /absolute/path/to/kubeconfig
        kube_context: my-context
        namespace: spark
        service_account: spark
        image: apache/spark:4.3.0-scala2.13-java21-python3-ubuntu
        server_jar: local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar
        startup_timeout: 180
        cleanup_timeout: 120
        driver_resources: {cpu: 1, memory: 1024, memory_overhead: 512}
        executor_resources: {cpu: 1, memory: 1024, memory_overhead: 512}
        executor_instances: {initial: 2}
        spark_conf:
          spark.sql.catalogImplementation: hive
          spark.sql.warehouse.dir: hdfs://namenode.data.svc:8020/warehouse
          spark.hadoop.hive.metastore.uris: thrift://metastore.data.svc:9083
          spark.sql.shuffle.partitions: '4'
```

Your `dbt_project.yml` must select `profile: analytics`. To use Thrift, change
`target` and the output name to `thrift`, set `method: thrift`, and replace
`server_jar` with:

```yaml
server_jar: local:///opt/spark/jars/spark-hive-thriftserver_2.13-4.3.0.jar
```

The adapter selects the host and port. Do not supply `host`, `port`, `SPARK_REMOTE`
or the upstream `method: session` workaround. Connect uses a separate explicit
remote session for each dbt connection. Thrift uses PyHive's HiveServer2 protocol,
not a JDBC client.

`compute` accepts the explicit image, namespace, service account, resource,
executor-allocation, package, node-selector and `max_runtime` fields of
[`ConnectServerConfig`](spark_connect_server.md), plus the cluster-selection
fields above, `startup_timeout`, `cleanup_timeout` and `poll_interval` (positive seconds, at
most 3600). `max_runtime` is unset by default; see
[receipts, failure and recovery](#receipts-failure-and-recovery).
Thrift fixes its managed port to 10000. Managed identity, ports and routing cannot
be overridden through `spark_conf`. Put static Hive/catalog configuration in
`compute.spark_conf`; `server_side_parameters` accepts runtime `spark.sql.*` settings; catalog, warehouse,
Hive metastore and extension settings are excluded.

## Check a profile before running

`spark-on-k8s-dbt check` validates the selected output and its cluster access
without creating a Spark server or any other Kubernetes resource, so configuration
errors appear in seconds instead of after compute has been requested. It accepts
dbt's profile-selection options with dbt's defaults and environment variables:
`--project-dir` (`DBT_PROJECT_DIR`), `--profiles-dir` (`DBT_PROFILES_DIR`),
`--profile` (`DBT_PROFILE`), `--target`/`-t` (`DBT_TARGET`) and `--vars`. With
`--profile`, a dbt project is not required.

```bash
/path/to/spark-on-k8s/.venv-dbt/bin/spark-on-k8s-dbt check \
  --project-dir /path/to/analytics --profiles-dir /path/to/analytics --target connect
```

Each check reports `pass`, `fail` or `skip`; a failure includes a next step:

| Check | Verifies |
| --- | --- |
| `profile` | dbt renders and selects the output (including `env_var()` and `--vars`) and the adapter accepts its credentials, through the same code dbt uses. |
| `compute` | The adapter's `compute` validation, including the kubeconfig path and the Thrift server's managed settings. |
| `client_libraries` | The transport client imports: the Spark Connect client, or PyHive with its Thrift SASL and HiveServer2 modules. |
| `kubectl` | `kubectl` is on `PATH` for a local client. Skipped with `in_cluster: true`. |
| `kubernetes_config` | The selected kubeconfig and context, or the in-cluster configuration, loads. |
| `namespace` | The namespace exists, is not terminating, and the invoking identity can read it. |
| `service_account` | The driver service account exists. Its own permissions are not verified. |
| `permission.<verb>.<resource>` | A SelfSubjectAccessReview for each request the wrapper makes as the invoking identity. |

The permission checks cover `get` on the namespace and driver service account;
`create`, `get`, `list` and `delete` on Pods and Services; `list` on ConfigMaps for
cleanup observation; and, for a local client, `create` on `pods/portforward`. The
wrapper itself creates no Secrets or ConfigMaps; executor resources are created by
the driver's service account. The API server evaluates access reviews without
storing them. A review that cannot be performed fails rather than being assumed
allowed. A missing namespace, rejected credentials or an unreachable API server
skip the dependent checks. Kubernetes requests share a total `--timeout` (default
30 seconds).

`--output json` prints one versioned document on stdout; dbt's own warnings and
other messages go to stderr. Every check ID appears once, in a fixed order:

```json
{
  "schema_version": 1,
  "operation": "dbt_check",
  "status": "fail",
  "profile": "analytics",
  "target": "connect",
  "method": "connect",
  "namespace": "spark",
  "checks": [
    {"id": "profile", "status": "pass", "message": "...", "remediation": null},
    {"id": "permission.delete.pods", "status": "fail", "message": "Denied: delete pods in namespace spark.",
     "remediation": "Grant the invoking identity delete on pods in namespace spark."}
  ],
  "error": null
}
```

`status` is `fail` when any check fails. `error` is `null` unless the check was
interrupted (`interrupted`) or stopped unexpectedly (`operation_error`). The command
exits 0 when no check failed, 1 when any check failed, 2 for invalid options and
130 when interrupted.

!!! note "A passing check is not a successful run"
    The check does not pull the image, schedule a driver, start Spark or contact
    the warehouse and metastore. Image pull failures, quotas, admission policies,
    node capacity, the driver service account's permissions, network policy and
    Spark configuration errors still surface only when `spark-on-k8s-dbt run`
    starts compute.

## Run dbt with automatic compute cleanup

Use the installed executable's absolute path if your current directory is a
separate dbt project:

```bash
/path/to/spark-on-k8s/.venv-dbt/bin/spark-on-k8s-dbt run -- \
  build --project-dir /path/to/analytics --profiles-dir /path/to/analytics
```

Every command is an independent invocation. For example, `seed`, `build`,
`run-operation`, `test` and `docs generate` can each use the same wrapper:

```bash
/path/to/spark-on-k8s/.venv-dbt/bin/spark-on-k8s-dbt run -- \
  docs generate --project-dir /path/to/analytics --profiles-dir /path/to/analytics
```

The server survives dbt's intermediate connection cleanup, including the
compilation phase of `docs generate`. It is removed only when the entire command
finishes. Tables remain in the external warehouse/catalog for the next invocation.
Commands that never open a database connection do not provision compute.
Using plain `dbt` with this managed adapter reports that the wrapper is required;
there is no unbounded background server hidden behind a connection-close hook.

While the server starts, and while it is deleted at the end, the wrapper writes
short progress lines with the elapsed time to **stderr**:

```text
Managed Spark +0s: requesting a Spark Connect server in namespace spark (startup timeout 180s)
Managed Spark +1s: driver pod dbt-connect-0f3c...-driver created
Managed Spark +1s: waiting for the driver pod to be scheduled (Unschedulable: 0/3 nodes are available: 3 Insufficient cpu.)
Managed Spark +12s: driver pod scheduled on worker-2; container spark-kubernetes-driver is ContainerCreating
Managed Spark +40s: driver container running
Managed Spark +40s: opening a kubectl port-forward tunnel to the driver
Managed Spark +41s: waiting for the Spark Connect endpoint to answer a query
Managed Spark +55s: Spark Connect server ready (application dbt-connect-0f3c...)
```

A new phase prints immediately; an unchanged phase repeats at most every 30
seconds. Image pull errors point at `compute.image`, so you can interrupt instead of
waiting for `startup_timeout`. The lines never go to stdout, so dbt's stdout,
including `--log-format json` events, is unchanged; they are also not written to
dbt's log file. They follow dbt's resolved console settings: `--quiet`
(`DBT_QUIET`) or `--log-level warn`, `error` or `none` (`DBT_LOG_LEVEL`, or the
project's `flags`) suppresses them. The receipt path is always printed.

Statements are not automatically replayed (`query_retries` must be 0). Spark SQL
is not transactional: failure and cancellation can leave successfully written
tables or rows. Review dbt's normal incremental/idempotency behavior before retrying.
Connect supports cancellation but does not accept `query_timeout` in this initial
version; Thrift retains dbt-spark's query timeout setting. Thrift serializes
requests on each connection and limits acquisition of the request lock and
individual blocking socket operations to 15 seconds. These transport limits do not cap the full SQL
execution time: queries run asynchronously and the adapter polls for completion.
A failed transport is closed rather than reused, and statements are not replayed.
Connect buffers SQL results for dbt metadata/seed/materialization operations; it
is not a streaming query-results interface.

## Receipts, failure and recovery

The wrapper prints the recovery receipt path to stderr. By default each invocation
gets a unique directory under `.spark-on-k8s/dbt/` in the working directory. Set
`--receipt-dir /new/private/directory` before `--` to choose it explicitly; the
directory must not already exist. Receipts have private file permissions and retain
resource UIDs and cluster selection, not the original Spark configuration or its
catalog credentials. Protect the dbt profile and normal dbt logs separately.

With `--fail-fast`, dbt cancels running statements and then still runs `on-run-end`
hooks on the same compute. After Ctrl-C/SIGTERM the adapter opens no new connection.
Normal success, dbt failures and a graceful Ctrl-C/SIGTERM all attempt cleanup.
Exit codes are 0 for success, 1 for a dbt execution failure, 2 for invocation or
cleanup errors, and 130/143 for interruption. A cleanup failure preserves an
already failing dbt exit code and records the separate cleanup failure. A successful
command becomes nonzero if cleanup fails. Cleanup verifies the recorded namespace,
driver and Service identities and observes disappearance before reporting success.
It never deletes the warehouse or metastore.

A hard kill, machine loss, unavailable Kubernetes API, or ambiguous creation can
leave compute running. If creation fails after the driver's UID is confirmed, the
wrapper requests deletion of that driver by UID (its Services are owned by it) and
records the outcome as `creation_cleanup`. The receipt stays incomplete because
disappearance was not observed; confirm the driver is gone. After inspecting a
**complete** receipt, retry safe cleanup:

```bash
/path/to/spark-on-k8s/.venv-dbt/bin/spark-on-k8s-dbt cleanup \
  --receipt /path/to/invocation/receipt.json
```

Deletion refuses changed identities and incomplete receipts. In those cases,
inspect the recorded application and Kubernetes UIDs manually; do not delete by
name alone or discard the receipt. No external orphan-reaper controller is
installed. Avoid concurrent invocations targeting the same dbt output tables;
separate process ownership protects compute, not conflicting SQL writes.

To bound what a hard kill can leave running, set `compute.max_runtime` (whole seconds)
above the longest command you expect, including startup and `on-run-end` hooks.
Kubernetes stops the server's driver that long after its pod starts, whether or not
the wrapper survives; a command still running at that point fails like any lost
compute. The stopped driver pod and its Services remain until cleanup or
[`apps prune`](automation.md#prune-finished-or-orphaned-drivers) deletes them. See
[driver lifetime limits](automation.md#limit-how-long-a-driver-can-run).

## What the tests exercise

The [integration harness](../development.md#experimental-dbt-integration) runs
installed wheels on disposable Kubernetes with a shared HDFS warehouse and an
external metastore fixture. It first runs `spark-on-k8s-dbt check` against each
generated profile and requires every check to pass without creating compute. It
checks both transports, parallel dbt workers, seeds/models/data tests, fresh
compute across commands, startup and cleanup progress, catalog persistence,
documentation generation, failed SQL and graceful cancellation. Fixture storage
is temporary and is not a production storage deployment recipe.
