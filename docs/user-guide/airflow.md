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

| Outcome or event | Task outcome and application action |
| --- | --- |
| Spark succeeds | Task succeeds; `keep` retains the application, `delete` and `keep_if_failed` delete it |
| Spark fails | Task fails; `keep` and `keep_if_failed` retain it, `delete` deletes it |
| Driver startup deadline expires | Task fails and deletes the application, including with `on_finish_action="keep"`; applies to synchronous and deferred waiting |
| Deferred trigger reports an observation error | Task fails with the trigger error; `keep` retains the application, while `delete` and `keep_if_failed` delete it because an observation error is not a confirmed Spark `Failed` state |
| Synchronous observation raises a non-timeout API/transport error | Error propagates before finish cleanup; no new submission or automatic cleanup is performed by that wait path |
| `app_waiter="no_wait"` | Returns after submission/adoption; no finish cleanup or completion assertion |
| A retried or cleared attempt finds an earlier driver of the same task instance | One Pending or Running driver is adopted instead of submitting; a Succeeded one only with `reattach_succeeded=True`; ambiguous or uncertain findings fail the attempt. See [retries and reattachment](#retries-and-reattachment) |
| Airflow invokes `on_kill` with a known driver | `on_kill_action="keep"` retains, `"delete"` requests deletion, and `"kill"` requests process termination; default is `delete` |

Do not assume every scheduler state change, worker loss or manual task-state
edit invokes `on_kill`. A termination request also does not prove that application
output was rolled back. The SDK/CLI retain applications after observation timeout
or interruption; the Airflow startup-timeout deletion policy is intentionally
different. See the [automation contract](automation.md).

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
annotation and the task labels described below are generated from task identity
and cannot be overridden by user annotations or labels. Previously literal Jinja
expressions in these newly templated fields will now be rendered; review existing
DAGs when upgrading.

### Retries and reattachment

Each driver the operator submits carries the reserved `spark-on-k8s.io/airflow-task`
ownership annotation and these labels:

| Label | Value |
| --- | --- |
| `spark-on-k8s.io/airflow-task-key` | 32-character digest of the DAG ID, run ID, task ID, map index and, on Airflow 3, region; the same for every attempt of one task instance |
| `spark-on-k8s.io/airflow-dag-id`, `spark-on-k8s.io/airflow-task-id` | Readable IDs for people and dashboards. Characters outside the label alphabet become `_` and values are cut to 63 characters, so they can collide; never select one task instance by them |

When an attempt starts, before submitting, the operator looks for an earlier
driver of the same task instance in its `namespace`:

1. If task XCom still records a driver, that driver is checked first. A Pending or
   Running driver is adopted. Status lookup failures and Unknown status fail the
   attempt. A confirmed absent (404) or terminal driver continues to step 2.
2. With `reattach_by_label=True`, the default, it lists the drivers carrying the
   attempt's task key and acts as in the table below. Otherwise it submits a new
   application.

Airflow 2.11 and 3.4 delete a task instance's XCom when a new attempt starts; they
keep it only when a deferred task resumes. On an ordinary retry, or after clearing
the task, step 2 therefore finds the earlier driver; step 1 applies only where XCom
survives into the next attempt. The labels are written at pod creation, so step 2
also covers a worker lost between creating the driver and recording it in XCom.

| Drivers found by task key | Attempt's action |
| --- | --- |
| None, or only Failed drivers | Submits a new application; Failed drivers are logged and kept or deleted as `on_finish_action` already decided |
| One Pending or Running driver | Adopts it and waits as for a new submission, synchronously or deferred, with a new startup deadline; terminal drivers beside it are ignored |
| More than one Pending or Running driver | Fails without submitting or choosing; delete the extra drivers before retrying |
| Succeeded drivers and no active one, `reattach_succeeded=False` (default) | Logs a warning naming them, then submits a new application |
| One Succeeded driver and no active one, `reattach_succeeded=True` | Adopts it as the attempt's result without rerunning Spark, then applies `on_finish_action` |
| More than one Succeeded driver and no active one, `reattach_succeeded=True` | Fails without submitting or choosing |
| Any driver with Unknown or missing phase | Fails without submitting; an indeterminate driver may still be running |
| A driver whose deletion was already requested | Ignored, for example after a startup-timeout deletion |
| The listing fails (permission, API, network or timeout error) | Fails without submitting; restore access before the next attempt |

`reattach_succeeded=True` also adopts a Succeeded driver found in retained XCom.
It suits a job whose earlier success for the same DAG run is an acceptable result,
such as a partition overwrite that should not run twice. The Succeeded driver must
still exist: `on_finish_action="keep"` retains it, while `"delete"` and
`"keep_if_failed"` delete it after success. With the option enabled, clearing a
successful task does not rerun Spark while that driver exists; delete the driver
first to force a rerun.

```python
from spark_on_k8s.airflow.operators import SparkOnK8SOperator

SparkOnK8SOperator(
    task_id="publish_partition",
    image="registry.example.com/team/publish-job:approved",
    app_path="local:///opt/spark/work-dir/publish.py",
    namespace="analytics", service_account="spark-driver",
    app_arguments=["--partition", "{{ ds }}"],
    on_finish_action="keep",  # retain the Succeeded driver that a retry may adopt
    reattach_succeeded=True,
    retries=2,
)
```

To inspect a DAG's drivers and their task keys, list them with the readable labels:

```bash
kubectl get pods -n analytics -L spark-on-k8s.io/airflow-task-key \
  -l spark-role=driver,spark-on-k8s.io/airflow-dag-id=daily_transform
```

The worker's Kubernetes connection needs permission to list pods in the namespace;
the `edit` role used in [Getting Started](getting_started.md) includes it. Without
that permission, every attempt fails before submitting. Set `reattach_by_label=False`
to keep only the XCom check; drivers are still labelled.

Upgrading changes retries of existing DAGs. An attempt whose earlier driver of the
same task instance is still Pending or Running now adopts that driver instead of
starting a concurrent second application. This includes drivers retained by
`on_kill_action="keep"` or left running when `on_kill` was not invoked. The adopted
driver keeps the image, arguments and configuration it was submitted with; to run
changed DAG code instead, delete or kill the earlier driver before clearing or
retrying the task. Drivers submitted by earlier versions have no task-key label and
are not found.

This is best-effort reattachment, not submission deduplication or an exactly-once
data-write promise. Drivers that were deleted, are in another namespace, or lost
their labels are not found, and two concurrent attempts are not serialized.
Applications must define their own handling of a repeated logical date.

## Verify the integration on Kubernetes

The source workflow `.github/workflows/airflow-kubernetes.yml` builds the wheel
and runs Airflow 2.11.2 and 3.4.0 in service-account-bearing pods against the
representative Spark 4.0.5 image. It checks successful work, deliberate Spark
failure after real executor work, and an unschedulable startup timeout, each in
synchronous and deferred mode. Deferred cases run and resume the real trigger.
Each case also checks that the driver carries its task-key label.

This uses Airflow's `DAG.test()` engine, metadata database and XCom. It does not
qualify a separately deployed scheduler, executor or triggerer process,
worker-crash recovery, or a retry reattaching by label. The broader Spark runtime matrix remains in the Spark
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
`SparkSqlOnK8SOperator` task instances on Airflow 3.4 or newer, below Airflow 4.
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
