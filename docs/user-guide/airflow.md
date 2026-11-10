# Airflow

The source version supports Airflow 3.4 on Python 3.11–3.14 and Airflow 2.11.2 on
Python 3.11–3.12. Follow the [installation instructions](installation.md) for the
`airflow` extra; published releases can differ from the current source. Install it
in the Airflow components that load and execute your DAGs, including the triggerer
when using deferrable tasks. The package registers its provider automatically.
If your DAGs run with a 0.12.x release, review the
[Airflow upgrade checklist](airflow-upgrade.md) first: some tasks that succeeded
before now fail.

## Connect to Kubernetes

Configure an Airflow connection of type **Kubernetes**, with ID
`kubernetes_default`. To use another connection, set `kubernetes_conn_id` on the
operator. Choose one authentication method. For Airflow running inside Kubernetes,
this environment variable uses the Airflow pod's service account:

```bash
export AIRFLOW_CONN_KUBERNETES_DEFAULT='{"conn_type":"kubernetes","extra":{"in_cluster":true}}'
```

For an external cluster, use the connection's `kube_config_path` and, optionally,
`cluster_context` fields instead. The kubeconfig must be available wherever the
task or trigger executes. See the provider's
[Kubernetes connection reference](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/connections/kubernetes.html).

The connection authenticates Airflow to Kubernetes. The operator's `namespace`
and `service_account` choose where Spark runs and which service account its driver
uses; they do not inherit the connection's namespace. Follow
[Getting Started](getting_started.md) to prepare these resources and permissions.
The examples use namespace `spark` and service account `spark`.

## Separate execution and task-tab permissions

| Component/action | Credential and authorization boundary |
| --- | --- |
| Worker submits, waits or performs configured cleanup | The selected Airflow Kubernetes connection; grant the required pod/service and optional Secret/ConfigMap operations only in approved namespaces |
| Triggerer observes a deferred application | The same connection ID resolved in the triggerer's own environment; it needs observation access, not permission to submit another job |
| Spark driver creates executors | The submission's `service_account` in its application namespace, independently of the worker's identity |
| Native tab reads details/logs | Airflow task/task-log access, verified task ownership, and the Spark API's own permissions |
| Native tab kills/deletes | The corresponding Airflow write permission, verified ownership, `SPARK_ON_K8S_AIRFLOW_ALLOW_WRITES=true`, and the Spark API's permissions |

Setting the tab's allow-writes flag to `false` only disables browser actions. It
does not prevent a DAG from submitting through its Kubernetes connection or from
performing configured cleanup. Install the same integration version in workers
and triggerers, and restrict their resolved credentials to the intended scope.
A connection that works on a worker may still be unavailable to the triggerer.

## Load the example DAGs

The [Airflow examples](https://github.com/hussein-awala/spark-on-k8s/tree/main/examples/airflow)
include `spark_dag.py` for a packaged PySpark job and `spark_sql_dag.py` for inline
SQL and SQL files. Build the `pyspark-job` image using the
[Python example](examples.md#python), then make it available on the cluster.
`image_pull_policy="Never"` assumes the image is already loaded on the nodes.
For a registry image, change `image` and use `IfNotPresent` or `Always`.

Deploy the DAG Python files to your Airflow DAG directory or DAG bundle. Keep
`query.sql` and `iceberg_query.sql` beside `spark_sql_dag.py`, or set the DAG's
`template_searchpath` to a directory containing them. That directory must also be
available to task workers. The SQL templates use `ds` and `ts`; supply a logical
date when triggering a manual run, or adapt those templates to your input values.
Both example DAGs have `schedule=None`, so they run when triggered.

The SQL DAG creates four independent tasks with distinct application names. The
inline example uses `CREATE TABLE ... USING PARQUET AS SELECT`; the multiple-query
example renders its date as a SQL date literal. `SparkSqlOnK8SOperator` renders SQL
before splitting it into statements and sends a JSON statement list to the Spark
runner. Install the extra in Airflow; the Spark image does not need `sqlparse`.

## Prepare the Iceberg example

Before running `spark_iceberg_sql_application`, replace
`spark.sql.catalog.prod.uri` with a REST catalog endpoint reachable from Spark's
pods. The sample `localhost` address refers to the driver pod itself. Configure
any catalog credentials and warehouse access your deployment requires.

The example runtime package targets Spark 3.5 with Scala 2.12; use an image and
Iceberg runtime that match each other. Create the `prod.db` namespace and the
source table `prod.db.another_sample` with `id`, `data`, and `category` columns.
The query creates the target `prod.db.sample` if needed, then merges from that
source table. It does not provision a catalog, warehouse, or source dataset.

## Spark UI links and waiting

To enable the **Spark Job UI** operator link, set `spark_on_k8s_service_url` to the
base URL of a deployed spark-on-k8s API that the browser can reach. Set
`ui_reverse_proxy=True` for access to the running driver's UI. Enabling the reverse
proxy alone, as in the examples, does not set an Airflow link URL.

The operator stores the live UI URL in XCom after submission or adoption. For tasks
that wait for completion, it then replaces that URL with the history URL. The
history destination requires Spark event logging, a Spark History Server, and the
API's `SPARK_ON_K8S_API_SPARK_HISTORY_HOST` configuration. The link does not create
that infrastructure. With `app_waiter="no_wait"`, the operator returns after
submission and leaves the live link in place.

With `deferrable=True`, waiting runs in the Airflow triggerer. If
`app_waiter="log"` is also set, logs are read when the task resumes after completion.

## Lifecycle, retries and cleanup

The table applies to `app_waiter="wait"` or `"log"` unless stated otherwise.
`startup_timeout` bounds the driver's Pending phase; `0` disables that deadline.
It is separate from Airflow's task execution timeout.

`max_runtime` (seconds, unset by default) sets the driver pod's Kubernetes
`activeDeadlineSeconds`: Kubernetes stops the driver that long after its pod starts
and marks it `Failed` with reason `DeadlineExceeded`, even when the worker, triggerer
or `on_kill` never runs. A waiting task then follows the "Spark fails" row. Airflow's
`execution_timeout` still needs a live worker to call `on_kill`. The deadline covers
the driver's whole life, including time a retry spends adopting it; a retry that finds
the driver stopped submits a new application with a new deadline. See
[driver lifetime limits](automation.md#limit-how-long-a-driver-can-run).

| Outcome or event | Task outcome and application action |
| --- | --- |
| Spark succeeds | Task succeeds; `keep` retains the application, `delete` and `keep_if_failed` delete it |
| Spark fails | Task fails; `keep` and `keep_if_failed` retain it, `delete` deletes it |
| Driver startup deadline expires | Task fails and deletes the application, including with `on_finish_action="keep"`; applies to synchronous and deferred waiting |
| Deferred trigger reports an observation error | Task fails with the trigger error; `keep` retains the application, while `delete` and `keep_if_failed` delete it because an observation error is not a confirmed Spark `Failed` state |
| Synchronous observation raises a non-timeout API/transport error | Error propagates before finish cleanup; no new submission or automatic cleanup is performed by that wait path |
| `app_waiter="no_wait"` | Returns after submission/adoption; no finish cleanup or completion assertion |
| Airflow invokes `on_kill` with a known driver | `on_kill_action="keep"` retains, `"delete"` requests deletion, and `"kill"` requests process termination; default is `delete` |

Do not assume every scheduler state change, worker loss or manual task-state
edit invokes `on_kill`. A termination request also does not prove that application
output was rolled back. The SDK/CLI retain applications after observation timeout
or interruption; the Airflow startup-timeout deletion policy is intentionally
different. See the [automation contract](automation.md).

When task-local XCom still contains the recorded namespace and driver name, a
retry adopts a Pending or Running application. Confirmed absence (Kubernetes
404), or a confirmed Succeeded/Failed application, permits the existing new-job
path. Authentication failures, unavailable status lookups and Unknown status now
fail the task before submission. Restoring access lets a later attempt inspect
the same recorded identity; uncertainty is not treated as absence.

This depends on retained XCom. Airflow may clear XCom during a retry, and a crash
between pod creation and receipt persistence remains outside this guarantee.
It is not submission deduplication or an exactly-once data-write promise.
Applications must define their own handling of a repeated logical date.

For a daily DAG, choose the startup deadline and retention behavior explicitly.
Replace the example image with your approved application artifact:

```python
from datetime import UTC, datetime, timedelta
from airflow import DAG
from spark_on_k8s.airflow.operators import SparkOnK8SOperator

with DAG(
    "daily_transform", schedule="@daily", catchup=False,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
) as dag:
    SparkOnK8SOperator(
        task_id="transform",
        image="registry.example.com/team/daily-job:approved",
        app_path="local:///opt/spark/work-dir/job.py",
        namespace="analytics", service_account="spark-driver",
        app_arguments=["--run-date", "{{ ds }}"],
        driver_labels={"logical-date": "{{ ds_nodash }}"},
        executor_labels={"logical-date": "{{ ds_nodash }}"},
        deferrable=True, startup_timeout=180, poll_interval=10,
        on_finish_action="keep_if_failed", on_kill_action="delete",
        retries=2, retry_delay=timedelta(minutes=2),
    )
```

`packages` and driver/executor labels and annotations support Airflow templates,
alongside the existing image, arguments and resource fields. Use valid Kubernetes
label values after rendering. The reserved `spark-on-k8s.io/airflow-task`
annotation is generated from task identity and cannot be overridden by a user
annotation. Previously literal Jinja expressions in these newly templated fields
will now be rendered; review existing DAGs when upgrading.

## Verify the integration on Kubernetes

The source workflow `.github/workflows/airflow-kubernetes.yml` builds the wheel
and runs Airflow 2.11.2 and 3.4.0 in service-account-bearing pods against the
representative Spark 4.0.5 image. It checks successful work, deliberate Spark
failure after real executor work, and an unschedulable startup timeout, each in
synchronous and deferred mode. Deferred cases run and resume the real trigger.

This uses Airflow's `DAG.test()` engine, metadata database and XCom. It does not
qualify a separately deployed scheduler, executor or triggerer process, or
worker-crash recovery. The broader Spark runtime matrix remains in the Spark
compatibility workflow. Follow the Airflow workflow's image-building and kind
setup, then run its exact host command from the repository root:

```bash
.venv/bin/python -I tests/integration/airflow/run_kubernetes.py \
  --kubeconfig "$RUNNER_TEMP/airflow-kubeconfig" \
  --airflow-image "airflow-compat:$AIRFLOW_VERSION" \
  --airflow-version "$AIRFLOW_VERSION" \
  --spark-image "airflow-spark:$SPARK_VERSION" --spark-version "$SPARK_VERSION" \
  --artifacts "$RUNNER_TEMP/airflow-kubernetes-results"
```

The default context is `kind-airflow-compat`; the runner refuses non-kind
contexts. Inspect all six cases in `result.json`, the Airflow log, cluster
pods/events/logs and kind logs. A passing unit suite or an unexecuted workflow is
not real-cluster evidence. See the [development guide](../development.md) for
installed-wheel checks and the complete validation policy.

## Native task tab in Airflow 3

The optional **Spark on Kubernetes** tab is available for `SparkOnK8SOperator` and
`SparkSqlOnK8SOperator` task instances, and for
[listed subclasses](#operator-subclasses), on Airflow 3.4 or newer, below Airflow 4.
It reuses the standalone console's React components for application details,
logs, actions and history links. Airflow 2.11.2 continues to support the operators
and **Spark Job UI** link, but does not load this tab.

This feature describes the current source, not an earlier PyPI release. Install
from the checkout as described in [Installation](installation.md#current-source):

```bash
uv sync --python 3.12 --locked --no-default-groups --extra airflow-ui
# Or, in an activated virtual environment:
python -m pip install ".[airflow-ui]"
```

The `airflow-ui` extra includes the Airflow operator dependencies and the plugin's
HTTP and WebSocket dependencies; it does not require the standalone `api` extra
in the Airflow process. Install the same package version in the Airflow API server
and task workers. Deploy the spark-on-k8s API from that version separately with
its `api` extra, so its console endpoints can return the driver's task ownership
metadata. Built JavaScript and CSS are included in the Python package.

Set these variables before starting the Airflow API server, then restart it:

```bash
export SPARK_ON_K8S_AIRFLOW_SERVICE_URL='http://spark-on-k8s.spark.svc.cluster.local:8000'
export SPARK_ON_K8S_AIRFLOW_PUBLIC_URL='https://spark.example.com'
export SPARK_ON_K8S_AIRFLOW_ALLOW_WRITES='false'
```

| Variable | Meaning | Default |
| --- | --- | --- |
| `SPARK_ON_K8S_AIRFLOW_SERVICE_URL` | Administrator-configured Spark API base URL reachable from the Airflow API server. Required to register the tab. | unset; tab disabled |
| `SPARK_ON_K8S_AIRFLOW_PUBLIC_URL` | Browser-reachable base URL for external Spark UI and history links. | service URL |
| `SPARK_ON_K8S_AIRFLOW_ALLOW_WRITES` | Set to `true` to permit kill/delete requests after task ownership and Airflow permissions are checked. | `false` |
| `SPARK_ON_K8S_AIRFLOW_OPERATOR_SUBCLASSES` | Comma-separated class names of operator subclasses that also show the tab, for example `TeamSparkOperator,TeamSqlOperator`. | unset |
| `SPARK_ON_K8S_AIRFLOW_SNAPSHOT_TTL` | Seconds, from 0 to 60, for which viewers of one task attempt share an application snapshot. `0` disables sharing. | `5` |

An invalid URL, subclass list or snapshot TTL disables the tab and logs a warning
in the Airflow API server.

Both URLs may include a deployment path prefix and must be HTTP(S) URLs without
embedded credentials, query parameters or fragments. The service URL is chosen
by the administrator, not by browser input. The plugin does not forward the
browser's Airflow credentials to that service. Keep the Spark API behind
appropriate network access controls; Airflow authentication does not protect a
separately exposed Spark API or its external links.

If `spark_on_k8s_service_url` is set on an operator, it must match the configured
service or public URL. Use the public URL to keep the existing **Spark Job UI**
link browser-accessible. Set `ui_reverse_proxy=True` on the operator to offer the
live Spark UI link. Airflow deployments under a URL prefix are supported; no
root-relative plugin URL needs to be configured.

The tab uses the selected DAG, run, task, mapped index or region, and current
attempt to resolve a small metadata record in XCom. Live data is checked against
the `spark-on-k8s.io/airflow-task` annotation written on the driver by the operator.
A mismatched or missing ownership annotation is rejected. A retry may adopt its
own earlier attempt's driver; another task's driver cannot be selected by editing
XCom. Older task submissions without the new metadata retain their existing
operator links but cannot use the task tab's live features.

Read requests require Airflow access to the task; log streaming additionally
requires task-log access. Kill and delete also require the corresponding Airflow
write permission and `SPARK_ON_K8S_AIRFLOW_ALLOW_WRITES=true`. Log connections and
write requests must come from the Airflow page's origin. The Spark API's own
permissions still apply.

After the driver pod is deleted, the tab shows only the saved task-local
application ID with unknown status. It offers an external history link only when
the Spark API reports a configured History Server. It disables live
logs and actions, and does not fetch history records using XCom alone. The
external history link requires the event logging and History Server setup
[described above](#spark-ui-links-and-waiting). While a driver exists and ownership
is verified, the tab can fetch that application's history metadata through the
configured Spark API.

The native tab checks the Spark API configuration before enabling history. An
unconfigured History Server does not produce a warning; a configured server that
cannot be reached still does. Older API services without the capability endpoint
retain live application features but omit optional history links until upgraded.

The native task log proxy accepts individual text messages up to 16 MiB, including
long driver log lines. A larger message interrupts the stream rather than being
silently truncated. The proxy uses a small receive queue to keep buffering bounded;
the console's separate line-count limit still applies.

### Operator subclasses

Airflow decides where to show a plugin tab by matching the task instance's exact
operator class name; it does not see base classes. To show the tab for a platform
wrapper, list the wrapper's class name and restart the Airflow API server:

```python
from spark_on_k8s.airflow.operators import SparkOnK8SOperator


class TeamSparkOperator(SparkOnK8SOperator):
    """Applies the platform team's defaults."""
```

```bash
export SPARK_ON_K8S_AIRFLOW_OPERATOR_SUBCLASSES='TeamSparkOperator'
```

The list only decides where the tab appears. Airflow's serialized task definition
records the operator's class name but not its base classes, so for any other
operator the tab's backend looks in that definition for the provider's
**Spark Job UI** operator link (`SparkOnK8SOperatorLink`) before it reads the
task's XCom record. Both provider operators declare this link, and subclasses,
including mapped tasks, inherit it. A subclass that replaces
`operator_extra_links` must keep `SparkOnK8SOperatorLink()` among its links.
Otherwise, or for a listed operator that does not inherit from a provider
operator, the tab reports that the task has no Spark application binding and
does not read a record that the task pushed to XCom.

### Refresh and shared snapshots

By default, the tab refreshes every 10 seconds while the application is pending,
running or in an unknown state. Once the application has succeeded or failed,
live refresh ends and the tab says so; **Refresh** still loads the current state
on demand. A driver log stream ends when the driver terminates and shows
**Stream ended**; **Reconnect** requests the available logs again.

Viewers of the same task attempt share the application snapshot that the Spark API
reads from Kubernetes for `SPARK_ON_K8S_AIRFLOW_SNAPSHOT_TTL` seconds, and
concurrent requests share one lookup. Each request is still authorized by Airflow
and resolves its own task binding before it can receive a shared snapshot, and
different task attempts never share one. Kill and delete verify the driver's
current application directly, then discard the shared snapshot. A failed lookup
is not reused after it completes. Each Airflow API server process keeps its own
bounded in-memory snapshots, so a deployment with several API server workers or
replicas can still perform one lookup per process for each interval. History
records, the History Server capability check and diagnostics are requested for
each viewer.
