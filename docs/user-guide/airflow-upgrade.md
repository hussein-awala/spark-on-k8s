# Upgrade Airflow DAGs from 0.12

Use this checklist before moving DAGs that run `SparkOnK8SOperator` or
`SparkSqlOnK8SOperator` with a published 0.12.x release to the version these docs
describe. Several fixes surface problems that 0.12.x hid, so a task that finished
green before can now fail. Each item states what changed, how a DAG shows it and
what to change. The [release notes](https://hussein.awala.fr/spark-on-k8s/release-notes/)
list every change; the [Airflow guide](airflow.md) describes the resulting behavior.

These parts are unchanged: the `spark_on_k8s.airflow.operators` and
`spark_on_k8s.airflow.triggers` import paths, the operator arguments and their
defaults (including `on_finish_action="keep"`, `on_kill_action="delete"`,
`startup_timeout=0` and `deferrable=False`), the Kubernetes connection lookup,
the existing XCom keys and how the **Spark Job UI** link is configured. No operator
argument was removed or renamed.

## Checklist at a glance

| Review | DAGs affected | Section |
| --- | --- | --- |
| Python, Airflow, provider and client versions | All | [Runtime](#upgrade-the-runtime-first) |
| Rollout order of triggerers and workers | `deferrable=True` | [Rollout](#upgrade-triggerers-with-workers) |
| SQL statements that failed silently | `SparkSqlOnK8SOperator` | [SQL failures](#sql-statement-failures-fail-the-task) |
| Kubernetes API errors while waiting | Tasks that wait (`app_waiter` is not `no_wait`) | [API errors](#kubernetes-api-errors-end-the-wait) |
| Startup deadline in deferred mode | `deferrable=True` with nonzero `startup_timeout` | [Startup timeout](#deferrable-tasks-enforce-startup_timeout) |
| Retries of a task with a recorded driver | `retries` above 0 | [Retries](#retries-reattach-only-to-a-verifiable-driver) |
| Values that now go through Jinja | `{{` or `{%` in labels, annotations or `packages` | [Templates](#more-arguments-are-templated) |
| Dynamic allocation tuning | `executor_instances` tuning fields | [Allocation](#executor-allocation-tuning-is-applied) |
| ConfigMaps on SQL tasks | SQL tasks with `driver_ephemeral_configmaps_volumes` | [ConfigMaps](#sql-operator-configmaps-are-kept) |
| Application IDs and driver Pod names | Custom `app_id_suffix` or long `app_name` | [Names](#application-ids-and-driver-pod-names) |
| Connections, environment variables and XCom | Worker `SPARK_ON_K8S_*` variables, XCom consumers | [Connections](#connections-configuration-and-xcom) |
| Timeout and cleanup expectations | `execution_timeout`, `on_finish_action`, `on_kill_action` | [Timeouts](#timeouts-and-cleanup) |
| Native task tab | Optional, Airflow 3.4+ | [Task tab](#optional-native-task-tab) |

## Upgrade the runtime first

- **What changed:** The package requires Python 3.11 or newer and the `kubernetes`
  client 36.x. The `airflow` extra requires Airflow 2.11.2 or newer (below 4) and
  `apache-airflow-providers-cncf-kubernetes` 10.25 or newer (below 11). It also
  installs `sqlparse`, which the SQL operator now uses on the worker. The `airflow`
  and `airflow-ui` extras require Python below 3.15. The 0.12.x releases accepted
  Python 3.9, Airflow 2.6 and provider 7.
- **How you notice:** The installer refuses the upgrade on Python 3.9 or 3.10, or
  when Airflow, the provider or `kubernetes` is pinned to an older version, for
  example by a constraints file. A worker without `sqlparse` fails SQL tasks with
  `SQL submission requires the 'sql' extra`. Airflow 3.4.0's constraints files satisfy
  both extras. Airflow 2.11.2's constraints files pin older `kubernetes`,
  `kubernetes-asyncio`, provider and `sqlparse` versions than this release requires:
  on Airflow 2.11.2, override those pins or install without `--constraint`, then
  run `pip check`.
- **What to do:** Move Airflow to 3.4 (tested on Python 3.11–3.14) or 2.11.2 (tested
  on Python 3.11–3.12) before upgrading this package. Airflow 2.11 is a
  [legacy compatibility path](support.md); use Airflow 3 for new deployments.
  Install the same `spark-on-k8s[airflow]` version, or `airflow-ui` for the
  [task tab](#optional-native-task-tab), in every Airflow component that parses DAG
  files, runs tasks or runs triggers, then run `pip check` in each environment.

Spark images are configured per task and are not upgraded by this package. The
tested runtimes are listed in [Spark compatibility](spark_compatibility.md). Change
a task's Spark image in a separate step, so that a new failure can be attributed;
Spark 4 also changes SQL behavior, including ANSI mode by default.

## Upgrade triggerers with workers

- **What changed:** `SparkOnK8STrigger` now serializes a `startup_deadline`
  argument, which the 0.12.x trigger class does not accept.
- **How you notice:** A triggerer still running 0.12.x cannot create the trigger of
  a newly deferred task. Airflow reports a trigger failure and fails the task
  without running the operator's completion cleanup, so the Spark application
  keeps running.
- **What to do:** Upgrade triggerers before, or together with, workers. An upgraded
  triggerer resumes triggers created by 0.12.x workers; those run without a
  startup deadline, as before. Let running synchronous Spark tasks finish, or pause
  their DAGs, before restarting workers: a retry might not
  [reattach to its earlier driver](#retries-reattach-only-to-a-verifiable-driver),
  and Airflow may call `on_kill`, whose default deletes the application.

## SQL statement failures fail the task

- **What changed:** The 0.12.x SQL runner logged a failed statement, continued with
  the next one, and the Spark application succeeded. Now the first failed
  statement stops the run, the application ends `Failed` and the task fails;
  later statements do not run. SQL is also split on the Airflow worker with
  `sqlparse` instead of at every `;` inside the driver. Semicolons in quoted strings
  and comments no longer split a statement, and comment-only fragments are skipped.
- **How you notice:** A `SparkSqlOnK8SOperator` task that was green fails with
  `The job finished with status: Failed`. The driver log identifies the statement
  by position, as in `SQL statement 2 at <inline>:3 failed`, followed by Spark's
  error condition when one is available, such as `UNRESOLVED_COLUMN`. The runner no
  longer logs query text or Spark's full error message. Line numbers refer to the
  rendered SQL, and Airflow tasks report the source as `<inline>`, including SQL
  loaded from a `.sql` template file.
- **What to do:** Run each SQL task against staging data and fix statements that
  were failing unnoticed. Make an intentionally optional statement succeed in SQL,
  for example with `DROP TABLE IF EXISTS`, or move it to its own task. Update log
  searches and alerts that matched the old `Executing: <query>` or
  `Error executing SQL command` lines.

## Kubernetes API errors end the wait

- **What changed:** While a task waits, a Kubernetes API error other than 404 now
  ends the wait. A definitive error, such as 401 or 403, ends it at once. Throttling
  (429), server errors (5xx) and lost connections end it after five consecutive
  failed status reads; between them the wait keeps polling at `poll_interval`. Once
  0.12.x had read a status, it ignored every such error and kept polling.
- **How you notice:**
    - Synchronous waiting: the task fails with the Kubernetes `ApiException`. No
      finish cleanup runs, so the application keeps running.
    - Deferred waiting: the trigger ends with an error event and the task fails with
      `SparkOnK8STrigger failed: with error: ...`. Finish cleanup then runs: `keep`
      retains the application, while `delete` and `keep_if_failed` delete it even if
      it was still running, because an observation error is not a Spark `Failed`
      state. With `app_waiter="log"`, driver logs are no longer replayed before this
      error is reported.
- **What to do:** Confirm that the worker and the triggerer each resolve the
  connection and can read driver Pods in the application namespace; see
  [permissions](airflow.md#separate-execution-and-task-tab-permissions). Set
  Airflow `retries` for API outages longer than about four poll intervals. If a deferred task must never delete
  a possibly running application after an observation error, use
  `on_finish_action="keep"` and clean up separately.

## Deferrable tasks enforce `startup_timeout`

- **What changed:** In 0.12.x, `startup_timeout` applied only to synchronous
  waiting; deferred tasks ignored it. The triggerer now enforces it too. The
  deadline is fixed when the task defers, after submission or reattachment, and a
  triggerer restart does not extend it. The default `0` still disables it.
- **How you notice:** A deferred task whose driver stays Pending past the deadline,
  for example while waiting for capacity, quota or an image pull, fails with
  `Spark application startup timeout exceeded`. The application is deleted, even
  with `on_finish_action="keep"`.
- **What to do:** For deferrable tasks that set `startup_timeout`, cover the longest
  normal queueing time, or set it to `0`. Add `retries` if a new submission is the
  recovery you want.

```python
SparkOnK8SOperator(
    task_id="transform",
    image="registry.example.com/team/daily-job:approved",
    app_path="local:///opt/spark/work-dir/job.py",
    namespace="analytics",
    service_account="spark-driver",
    deferrable=True,
    # Now enforced while deferred: a driver still Pending after 15 minutes is deleted.
    startup_timeout=900,
    retries=2,
)
```

## Retries reattach only to a verifiable driver

- **What changed:** When the task's XCom records a driver from an earlier attempt,
  0.12.x reattached only to a Running driver and submitted a new application after
  any lookup error. Now a Pending or Running driver is reattached. A driver that is
  confirmed absent (404), Succeeded or Failed allows a new submission. Any other
  lookup error, or `Unknown` status, fails the task before it submits.
- **How you notice:** A retry fails with
  `Cannot verify previously recorded Spark driver <namespace>/<pod>; refusing to submit another application`
  or `Previously recorded Spark driver <namespace>/<pod> has indeterminate status Unknown`.
- **What to do:** Restore the connection's access to the namespace and retry. For
  `Unknown`, inspect the driver Pod named in the message, for example with
  `kubectl describe pod`, or with `spark-on-k8s app diagnose` and the Pod's
  `spark-app-id` label value. Once it is terminal or deleted, the next attempt can
  submit. Reattachment needs the earlier attempt's XCom, which Airflow may clear
  when a retry starts, so it is not duplicate protection; see
  [lifecycle, retries and cleanup](airflow.md#lifecycle-retries-and-cleanup).

## More arguments are templated

- **What changed:** `packages`, `driver_labels`, `executor_labels`,
  `driver_annotations` and `executor_annotations` are now Airflow template fields.
  The new optional arguments `driver_pod_template`, `driver_container_name`,
  `executor_pod_template` and `driver_env_vars_from_secrets` are templated too. The
  operator also writes the reserved `spark-on-k8s.io/airflow-task` annotation on the
  driver from the task identity and ignores a user value for that key.
- **How you notice:** Values that contained literal `{{ ... }}` or `{% ... %}` are
  now rendered. A rendering error fails the task before submission, and Kubernetes
  rejects rendered label values that are not valid.
- **What to do:** Search these arguments for `{{` and `{%`, and wrap intended
  literals in `{% raw %}...{% endraw %}`.

## Executor allocation tuning is applied

- **What changed:** The 0.12.x operator rebuilt `executor_instances` from `min`,
  `max` and `initial` only, so `executor_allocation_ratio`,
  `scheduler_backlog_timeout` and `sustained_scheduler_backlog_timeout` fell back to
  their defaults. They are now passed to Spark whenever `min` or `max` enables
  dynamic allocation.
- **How you notice:** Dynamic allocation scales at a different pace for tasks that
  set these fields.
- **What to do:** Review those values now that they take effect.

## SQL operator ConfigMaps are kept

- **What changed:** On `SparkSqlOnK8SOperator`, 0.12.x replaced
  `driver_ephemeral_configmaps_volumes` with the generated SQL ConfigMap, dropping
  yours without a warning. They are now mounted beside it, and `/configmap` and its
  subdirectories are reserved for the generated files.
- **How you notice:** Files from those ConfigMaps appear in the driver. A ConfigMap
  mounted at `/configmap` or below it fails the task before submission with
  `SQL submission reserves the /configmap mount path and its descendants`.
- **What to do:** Check ConfigMaps that 0.12.x ignored, and move reserved mounts to
  another path:

```python
SparkSqlOnK8SOperator(
    task_id="report",
    sql="report.sql",
    image="registry.example.com/team/pyspark:approved",
    namespace="analytics",
    service_account="spark-driver",
    driver_ephemeral_configmaps_volumes=[
        {"mount_path": "/etc/report", "sources": [{"name": "settings.json", "text": "{}"}]},
    ],
)
```

## Application IDs and driver Pod names

- **What changed:** The application ID, made of the sanitized `app_name` and the
  `app_id_suffix`, must be a DNS-1035 label of at most 63 characters: lowercase
  letters, digits and `-`, starting with a letter. It is checked before any
  resource is created. Driver Pods for IDs longer than 56 characters are named with
  a hash of the ID instead of its first 56 characters, and generated ConfigMap names
  follow the same rule.
- **How you notice:** A suffix such as `-{{ ts_nodash }}`, which renders an
  uppercase `T`, fails with `Application ID ... must be a DNS-1035 label`; 0.12.x
  failed later, while creating the Kubernetes resources. Long application names
  produce different driver Pod names, and reruns no longer collide.
- **What to do:** Lowercase the suffix, for example
  `app_id_suffix="-{{ ts_nodash | lower }}"`. Find drivers from the
  `driver_pod_name` XCom value or the `spark-app-id=<app-id>,spark-role=driver`
  labels instead of deriving Pod names from the application ID.

## Connections, configuration and XCom

The connection is resolved as before: a **Kubernetes** connection, selected by
`kubernetes_conn_id` (default `kubernetes_default`), separately in the worker and
the triggerer. Only the minimum provider version changed.

The configuration fixes in the release notes, such as `SPARK_ON_K8S_IN_CLUSTER` and
`SPARK_ON_K8S_APP_WAITER`, do not change operator tasks: the operator authenticates
through the Airflow connection and submits its own arguments. Worker environment
variables still fill `secret_values` (`SPARK_ON_K8S_SECRET_ENV_VAR`) and executor
templates (`SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH` and the new
`SPARK_ON_K8S_EXECUTOR_TEMPLATE`) when the operator leaves them unset. Submission
fails if both an executor template and a template path resolve.

The XCom keys `driver_pod_namespace`, `driver_pod_name` and `spark_ui_link` and the
task's return value are unchanged. Each task also pushes `spark_on_k8s.console.v1`,
a small record that the task tab reads. The **Spark Job UI** link now uses the
recorded application ID instead of deriving it from the driver Pod name, which
differs for long application IDs.

## Timeouts and cleanup

Within the operator, only `startup_timeout` deletes an application because a
deadline passed. Airflow's `execution_timeout` reaches the application only through
`on_kill`, and other timeouts never cancel Spark by themselves.

| Event | 0.12.x | Current |
| --- | --- | --- |
| `startup_timeout` expires, synchronous wait | Application deleted, task fails | Unchanged |
| `startup_timeout` expires, deferred wait | Not enforced | Application deleted, task fails, even with `keep` |
| Non-404 Kubernetes API error, synchronous wait | Ignored once polling had started | Task fails; no finish cleanup |
| Non-404 Kubernetes API error, deferred wait | Ignored once polling had started | Task fails; `keep` retains, `delete` and `keep_if_failed` delete |
| Airflow `execution_timeout` interrupts a synchronous wait | Airflow calls `on_kill`; `on_kill_action` applies | Unchanged |
| Airflow fails a deferred task without resuming it, such as a trigger failure | No operator cleanup; the application keeps running | Unchanged |
| Cleanup fails after a deferred completion | The cleanup error could replace the Spark outcome | The task reports the trigger error, startup timeout or Spark status, with the cleanup error as its cause |

The operator does not handle `execution_timeout` itself. Airflow 2.11.2 and 3.4.0
call `on_kill` when it interrupts synchronous waiting on a worker; the default
`on_kill_action="delete"` then requests deletion. While a task is deferred, no
worker holds its driver, so a failure that does not resume `execute_complete`
performs no cleanup. If a deferred task needs a hard runtime limit, enforce it in
the application or with a separate cleanup step rather than relying on
`execution_timeout`. The SDK and CLI never cancel on timeout; see the
[automation contract](automation.md).

## Optional: native task tab

- **What changed:** Next to its provider entry, which registers the **Spark Job UI**
  link, the package declares an Airflow plugin, `spark_on_k8s_ui`. The plugin
  registers nothing unless `SPARK_ON_K8S_AIRFLOW_SERVICE_URL` is set for the Airflow
  API server and Airflow is 3.4 or newer, below 4. Invalid settings disable it with
  a `Spark task UI is disabled` warning. Airflow 2.11 never loads the tab; the
  operators and link work as before.
- **How you notice:** Nothing changes in the Airflow UI until you enable it. Tasks
  submitted by 0.12.x have neither the console record nor the driver ownership
  annotation, so the tab cannot show their live data.
- **What to do:** Follow [Native task tab in Airflow 3](airflow.md#native-task-tab-in-airflow-3):
  install `spark-on-k8s[airflow-ui]` of the same version in the API server and
  workers, deploy the spark-on-k8s API from that version, set the variables and
  restart the API server. If an operator sets `spark_on_k8s_service_url`, it must
  match the configured service or public URL.

## Test before upgrading production

1. List affected DAGs. Search DAG files for both operators and record
   `deferrable`, `startup_timeout`, `on_finish_action`, `on_kill_action`, `retries`,
   `execution_timeout`, `app_id_suffix`, `executor_instances`, SQL task ConfigMaps
   and `{{` or `{%` in labels, annotations and `packages`.
2. Prepare a staging Airflow with the target Airflow version and this package in
   every component, including a separately running triggerer. Configure the same
   connection IDs for a non-production cluster, and give the tasks' namespaces
   production-equivalent RBAC.
3. Confirm that DAGs parse with `airflow dags list-import-errors`.
4. Run representative DAGs with `airflow dags test <dag-id> <logical-date>` for a
   quick check. It runs triggers inside the same process, so then trigger the same
   DAGs through the scheduler to exercise the deployed triggerer. Cover every SQL
   task, synchronous and deferred tasks, and tasks with retries.
5. Compare outcomes with recent 0.12.x runs of the same DAGs, and match each new
   failure to a section above.
6. Exercise the failure paths you rely on: a SQL task with a failing statement, a
   deferred task with a short `startup_timeout` whose driver cannot be scheduled
   (for example, a CPU request larger than any node), and an `execution_timeout`
   that expires during synchronous waiting.
7. After each run, list drivers and confirm that retention matches
   `on_finish_action` and `on_kill_action` and that no application is orphaned:

    ```bash
    kubectl get pods -n <namespace> -l spark-role=driver --show-labels
    ```

8. In production, upgrade triggerers no later than workers, then watch the first
   scheduled runs of each affected DAG.

The repository's own Kubernetes workflow covers success, Spark failure and startup
timeout on Airflow 2.11.2 and 3.4.0; see
[Verify the integration on Kubernetes](airflow.md#verify-the-integration-on-kubernetes).
It does not replace testing your DAGs.
