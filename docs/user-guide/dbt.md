# Experimental managed dbt

Run dbt Core commands against temporary Spark compute on an existing Kubernetes
cluster. The wrapper creates one server when dbt first opens a connection, shares
it across dbt worker threads and across the commands of one invocation, and
deletes its driver, executors and private Services when the invocation ends.
Choose **Thrift** or **Spark Connect** in the profile; you do not create the
server yourself.

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

`compute` accepts `app_name` (default `dbt-connect` or `dbt-thrift`; set one per project or
pipeline so its servers are recognizable), the explicit image, namespace, service account, resource,
executor-allocation, package, node-selector and `max_runtime` fields of
[`ConnectServerConfig`](spark_connect_server.md), plus the cluster-selection
fields above, `startup_timeout`, `cleanup_timeout` and `poll_interval` (positive seconds, at
most 3600). `max_runtime` is unset by default; see
[receipts, failure and recovery](#receipts-failure-and-recovery).
With `method: connect`, it also accepts the
[image pull Secret, Secret-backed environment and toleration fields](spark_connect_server.md#private-images-credentials-and-node-placement)
and `executor_node_selector`, for example `image_pull_secrets: [private-registry]`
or `executor_env_from_secrets: {AWS_ACCESS_KEY_ID: "s3-credentials:access-key"}`.
`method: thrift` supports only `driver_node_selector` of these and rejects the
others before contacting the cluster.
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

Each wrapper invocation creates its own server. For example, `seed`, `build`,
`run-operation`, `test` and `docs generate` can each use the same wrapper; to pay
the server startup once for several of them, see
[Run several commands on one server](#run-several-commands-on-one-server):

```bash
/path/to/spark-on-k8s/.venv-dbt/bin/spark-on-k8s-dbt run -- \
  docs generate --project-dir /path/to/analytics --profiles-dir /path/to/analytics
```

The server survives dbt's intermediate connection cleanup, including the
compilation phase of `docs generate`. It is removed only when the entire command
finishes. Tables remain in the external warehouse/catalog for the next invocation.
Commands that never open a database connection, such as `parse` and `ls`, do not
provision compute. `compile` opens one to fill its relation cache unless you pass
`--no-populate-cache`.
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

## Run several commands on one server

Every invocation pays one server startup: scheduling the driver, pulling its image
and starting Spark often take minutes. A job that runs `seed`, `run` and `test`, or
`build` and then `docs generate`, pays it once when the commands share one
invocation. Separate them with further `--`:

```bash
export DBT_PROJECT_DIR=/path/to/analytics DBT_PROFILES_DIR=/path/to/analytics
/path/to/spark-on-k8s/.venv-dbt/bin/spark-on-k8s-dbt run -- \
  deps -- seed --full-refresh -- build -- docs generate
```

Each group after a `--` is passed to dbt exactly as written, as `dbt <group>`
would receive it; the wrapper does not re-split or re-quote it. dbt reads
`DBT_PROJECT_DIR`, `DBT_PROFILES_DIR` and `DBT_TARGET`, so shared options need not
be repeated per command. Put wrapper options (`--receipt-dir`, `--keep-going`,
`--deadline`) before the first dbt argument: everything after it belongs to dbt,
and a standalone `--` always starts the next command rather than being passed to
dbt.

The commands run one after another in the wrapper's process:

| Topic | Behavior |
| --- | --- |
| Server | Created by the first command that opens a connection (`deps` does not), reused by every later command and deleted once, when the sequence ends, with the cleanup guarantees of a single command. |
| Receipt | One receipt covers the server for the whole sequence. Its `dbt_exit_code` is the sequence's exit code. |
| Failure | The sequence stops after the first command that fails. With `--keep-going`, the remaining commands still start. |
| Exit code | 130/143 when interrupted, even after an earlier failure. Otherwise the first failed command's code (1 or 2), also with `--keep-going`; then 5 when `--deadline` left a command unstarted; 0 when every command succeeded. A cleanup failure turns 0 into 2. |
| Between commands | The wrapper confirms that the same driver (by UID) is still running, and reopens an exited `kubectl` tunnel to it, before the next command starts. A failed startup, or a driver that stopped, is being deleted or was replaced, ends the sequence with exit 2 unless a command already failed. A sequence never creates a second server. |
| Interruption | Ctrl-C or SIGTERM during a command stops it as for a single command, and no later command starts. Between commands, nothing further starts. Compute is deleted in both cases. |
| `--deadline SECONDS` | No command starts once this many seconds have passed since the wrapper started. A running command is not interrupted or shortened: the deadline is not a cancellation. Owned compute is still deleted when the sequence ends. |
| dbt semantics | Each command is a separate dbt invocation: dbt parses the project again and applies its own `--fail-fast`, hooks, artifacts and result. Each command opens its own SQL sessions, so session settings and temporary views do not carry over. |

Every command must select the same managed output. A command whose rendered
profile selects a different `method` or `compute` mapping fails its first
connection rather than creating another server; schema, threads and `--vars`
that do not change `compute` may differ.

With several commands, the wrapper also reports each command's result and a
final summary on stderr:

```text
Managed dbt 1/4 (dbt deps) succeeded in 4s
Managed Spark +0s: requesting a Spark Connect server in namespace spark (startup timeout 180s)
...
Managed dbt 2/4 (dbt seed) succeeded in 71s
Managed dbt 3/4 (dbt build) failed with exit 1 after 240s
Managed Spark +0s: deleting managed Spark compute and observing its removal
Managed Spark +6s: managed Spark compute deleted
Managed dbt run finished with exit 1: 2 succeeded, 1 failed, 1 not run (stopped after a failed command; --keep-going runs the rest)
```

Commands are named by their leading words only; option values such as `--vars`
are never echoed. Success lines follow dbt's console settings, as startup progress
does. A failed or interrupted command, and the summary of a sequence that failed
or left a command unstarted, are always printed. A single command prints none of
these lines.

The server belongs to this one process for the duration of the sequence. It is
not a shared or warm pool: other processes, including concurrent CI jobs, cannot
attach to it.

## Receipts, failure and recovery

The wrapper prints the recovery receipt path to stderr. By default each invocation
gets a unique directory under `.spark-on-k8s/dbt/` in the working directory. Set
`--receipt-dir /new/private/directory` before `--` to choose it explicitly; the
directory must not already exist. Add `.spark-on-k8s/` to the project's
`.gitignore`; every invocation, including one that starts no server, creates a
directory there. Receipts have private file permissions and retain
resource UIDs and cluster selection, not the original Spark configuration or its
catalog credentials. Protect the dbt profile and normal dbt logs separately.

With `--fail-fast`, dbt cancels running statements and then still runs `on-run-end`
hooks on the same compute. After Ctrl-C/SIGTERM the adapter opens no new connection.
Normal success, dbt failures and a graceful Ctrl-C/SIGTERM all attempt cleanup.
Exit codes are 0 for success, 1 for a dbt execution failure, 2 for invocation or
cleanup errors, 5 when a sequence's `--deadline` left a command unstarted, and
130/143 for interruption. A cleanup failure preserves an
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
above the longest invocation you expect, including startup and `on-run-end` hooks. All
commands of a [sequence](#run-several-commands-on-one-server) share one server, so the
limit must cover the whole sequence. Kubernetes stops the server's driver that long
after its pod starts, whether or not the wrapper survives; a command still running at
that point fails like any lost compute. The stopped driver pod and its Services remain
until cleanup or [`apps prune`](automation.md#prune-finished-or-orphaned-drivers)
deletes them. See [driver lifetime
limits](automation.md#limit-how-long-a-driver-can-run).

## Run in CI

This recipe uses plain shell steps, so it fits GitHub Actions, GitLab CI or any
runner that can reach the Kubernetes API. Install the wheels as in
[Install from the repository](#install-from-the-repository) and put the
environment's `bin` directory on `PATH`.

### Keep credentials out of the repository

Commit a CI output that reads everything secret or job-specific from the
environment, and let the CI system inject the values from its secret store:

```yaml
analytics:
  target: ci
  outputs:
    ci:
      type: spark_on_k8s
      method: connect
      schema: "{{ env_var('DBT_CI_SCHEMA') }}"
      threads: 4
      compute:
        kube_config_file: "{{ env_var('SPARK_KUBECONFIG') }}"
        kube_context: "{{ env_var('SPARK_KUBE_CONTEXT') }}"
        namespace: spark-ci
        service_account: spark
        image: registry.example.com/analytics/spark-dbt:4.3.0
        server_jar: local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar
        startup_timeout: 600
        executor_instances: {initial: 2}
        spark_conf:
          spark.sql.catalogImplementation: hive
          spark.sql.warehouse.dir: hdfs://namenode.data.svc:8020/warehouse
          spark.hadoop.hive.metastore.uris: thrift://metastore.data.svc:9083
```

Write the kubeconfig secret to a private temporary file, or use your CI's
file-type secret, and pass its path in `SPARK_KUBECONFIG`; never commit a
kubeconfig, token or literal credential. Give CI an identity limited to the
permissions that [`spark-on-k8s-dbt check`](#check-a-profile-before-running)
verifies. For catalog
or storage credentials in `compute.spark_conf`, read variables whose names start
with `DBT_ENV_SECRET`, which dbt scrubs from its logs. Receipts never contain the
compute configuration.

### Give each pull request its own schema

Derive the schema from the pull request number so concurrent pull requests never
write the same tables:

```bash
export DBT_CI_SCHEMA="analytics_pr_${PR_NUMBER:?}"  # from your CI's pull request variable
```

dbt's default `generate_schema_name` prefixes custom schemas with the target
schema (`analytics_pr_123_marts`), so models with `+schema` stay separate too. If
your project overrides `generate_schema_name`, or a snapshot sets a fixed
`target_schema`, check that the CI target still produces pull-request names. The
wrapper never deletes warehouse or catalog data: drop a pull request's schemas when
it closes, for example with a `run-operation` macro that refuses any schema
without the `analytics_pr_` prefix.

Different pull requests run separate invocations with separate servers and
receipts. Each running job adds a driver and its executors to the namespace, so
cap CI concurrency or the namespace quota: a driver that cannot be scheduled waits
until `startup_timeout`. Two jobs for the same pull request write the same schema;
let your CI cancel or serialize the older one (for example a GitHub Actions
`concurrency` group with `cancel-in-progress`, or GitLab `interruptible` jobs).

### Steps

```bash
# Step 1: fail in seconds on configuration or permission errors; creates nothing.
spark-on-k8s-dbt check

# Step 2: one server for the whole job. exec makes the wrapper the step's main
# process, so a cancellation signal sent only to that process reaches it.
exec spark-on-k8s-dbt run --receipt-dir "$JOB_TMP/dbt-receipt" --deadline 2400 -- \
  deps -- seed -- build -- docs generate
```

`JOB_TMP` is a private per-job directory, such as `$RUNNER_TEMP` on GitHub
Actions; the receipt directory must not exist yet. Choose `--deadline` below the
job timeout so that the last command and cleanup can finish: with a 60-minute
limit, `--deadline 2400` starts no command after 40 minutes.

CI systems usually signal a cancelled or timed-out job and kill it after a grace
period that can be shorter than `cleanup_timeout`. Add a step that your CI runs
even after failure or cancellation (for example `if: always()` on GitHub Actions)
and keep the receipt as a job artifact. It contains resource identities and
cluster selection, not credentials:

```bash
# Step 3: always run. Retry cleanup unless the receipt shows nothing to delete.
receipt="$JOB_TMP/dbt-receipt/receipt.json"
if [ -f "$receipt" ]; then
  stage=$(python -c 'import json, sys; print(json.load(open(sys.argv[1]))["stage"])' "$receipt")
  case "$stage" in
    cleaned | not_started | reserved) echo "No managed compute to delete ($stage)" ;;
    *) spark-on-k8s-dbt cleanup --receipt "$receipt" ;;
  esac
fi
```

`reserved` means the wrapper stopped before it requested any compute.
`cleanup` refuses a receipt without a complete server identity: that step then
fails, and the receipt tells you what to inspect manually. Nothing runs if the
runner itself disappears before this step; compute then keeps running until you
run `cleanup` with the preserved receipt, or until `compute.max_runtime`, if set,
stops the driver. Set it above the job timeout.

## Size the managed server

`compute` sizes one Spark application: a driver that runs the Thrift or Connect
server and plans every query, and executors that run the tasks. dbt's `threads`
sets how many models run at once; each is a concurrent Spark query on the same
executors. Resources and executor allocation come only from these fields;
`spark_conf` cannot override them.

| `compute` field | Spark setting | Kubernetes Pod |
| --- | --- | --- |
| `driver_resources.cpu` | none | Driver CPU request and limit |
| `driver_resources.memory` (MiB) | `spark.driver.memory` (JVM heap) | Driver memory request and limit: `memory` + `memory_overhead` |
| `driver_resources.memory_overhead` (MiB) | none: headroom outside the heap | Included in the driver memory above |
| `executor_resources.cpu` | `spark.executor.cores`: concurrent tasks per executor | Executor CPU request, unless `spark_conf` sets a smaller `spark.kubernetes.executor.request.cores` |
| `executor_resources.memory` (MiB) | `spark.executor.memory` | Executor memory request and limit: `memory` + `memory_overhead` |
| `executor_resources.memory_overhead` (MiB) | `spark.executor.memoryOverhead` | Included in the executor memory above |
| `executor_instances.initial` alone | `spark.executor.instances` (2 when nothing is set) | A fixed number of executor Pods |
| `executor_instances.min` or `max` | Dynamic allocation with shuffle tracking, starting from `initial` (at least `min`) | Executors added while tasks queue and removed after 60 idle seconds by default, later while they hold shuffle data still in use |

Each `cpu` and `memory` value applies to every executor; executors × `cpu` is the
number of tasks that run at once across all models. More `threads` than that
rarely helps, and Spark's default FIFO scheduling can let one large model's tasks
delay smaller ones. Set `spark.sql.shuffle.partitions` in `compute.spark_conf` to
a small multiple of the total executor cores for small data; Spark's default of
200 creates many tiny tasks.

**Memory overhead.** A container that exceeds its Pod memory limit is killed by
Kubernetes. For an executor, Spark retries the lost tasks elsewhere; for the
driver, the server and the running dbt command fail. The heap is `memory`;
everything else (JVM metadata, threads, network and shuffle buffers, native
libraries) must fit in `memory_overhead`. Spark's own default rule is 10% of
memory with a 384 MiB minimum, but spark-on-k8s always sets the overhead (512 MiB
when omitted), so raise it yourself above about 5 GiB of memory. `0` is accepted
and leaves no headroom. The driver also holds query plans and broadcast tables, and
every result returned to dbt passes through it: large seeds and `docs generate` on
big catalogs need driver memory, not more executors.

**Startup time.** These mostly decide how long `startup_timeout` must be:

- **Image size and pull policy.** Spark images are large. The default
  `image_pull_policy: IfNotPresent` with an immutable tag or digest lets nodes
  reuse a cached image; `Always` contacts the registry on every start. Pre-pulling
  the image onto nodes removes the pull from startup. Executors pull it too,
  possibly on other nodes.
- **Node capacity and autoscaling.** A driver that fits on no node stays
  `Unschedulable` (shown in the startup progress) until a node autoscaler adds a
  node, which can take minutes. A request larger than any node never schedules.
- **`packages`** downloads Maven dependencies on every start; build them into the
  image instead.
- **Executors.** Readiness does not wait for them, so the first queries may wait
  for executor Pods to be scheduled and their image pulled.

A [sequence](#run-several-commands-on-one-server) pays this once instead of once
per command.

**Starting points.** The values below are illustrative starting points, not
measurements or recommendations for your data. Adjust them from the Spark UI
(task time, spill, garbage collection, skew) and from Pod memory usage. While a
server runs, `kubectl --namespace <namespace> port-forward pod/<driver pod> 4040`
opens its UI; the driver Pod name is in the startup progress and the receipt.

| Workload | `threads` | `driver_resources` | `executor_resources` | `executor_instances` | `spark.sql.shuffle.partitions` |
| --- | --- | --- | --- | --- | --- |
| Small: CI checks, sample data, models up to a few GB | 2–4 | `{cpu: 1, memory: 2048, memory_overhead: 512}` | `{cpu: 2, memory: 4096, memory_overhead: 512}` | `{initial: 2}` | `'8'` |
| Medium: models of tens of GB | 4–8 | `{cpu: 2, memory: 8192, memory_overhead: 1024}` | `{cpu: 4, memory: 16384, memory_overhead: 2048}` | `{min: 2, initial: 2, max: 10}` | `'64'` |

A fixed executor count starts every executor at once and suits short CI jobs.
Dynamic allocation releases idle executors between models and commands, at the
cost of waiting for new executors when work arrives. The integration tests run
their small fixture with a 768 MiB driver and one 512 MiB executor on a single
kind node; that shows the lifecycle works, not that those sizes perform.

## What the tests exercise

The [integration harness](../development.md#experimental-dbt-integration) runs
installed wheels on disposable Kubernetes with a shared HDFS warehouse and an
external metastore fixture. It first runs `spark-on-k8s-dbt check` against each
generated profile and requires every check to pass without creating compute. It
checks both transports, parallel dbt workers, seeds/models/data tests, fresh
compute across invocations, one driver shared by the commands of a sequence
(including a command after a failure with `--keep-going`, and no further command
after an interrupt), startup and cleanup progress, catalog persistence,
documentation generation, failed SQL and graceful cancellation. Fixture storage
is temporary and is not a production storage deployment recipe. The CI recipe and
sizing values above are not exercised by these tests.
