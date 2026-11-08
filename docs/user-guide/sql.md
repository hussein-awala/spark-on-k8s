# SQL jobs without Airflow

The SQL client submits a local `.sql` file or inline statements using a PySpark
image. Airflow is optional. Install the SQL extra from this source checkout:

```bash
uv sync --locked --extra sql
```

After a release containing this feature is published, install that release with
`python -m pip install 'spark-on-k8s[sql]'`. The current source documentation may
include features absent from the latest PyPI release.

Configure Kubernetes access, namespace `spark`, and service account `spark` as in
[Getting Started](getting_started.md). Then submit a local file:

```bash
uv run --no-sync spark-on-k8s sql submit \
  --image apache/spark:4.3.0-scala2.13-java21-python3-ubuntu \
  --namespace spark --service-account spark --name daily-summary \
  --file queries.sql --logs
```

Or submit inline SQL:

```bash
uv run --no-sync spark-on-k8s sql submit \
  --image apache/spark:4.3.0-scala2.13-java21-python3-ubuntu \
  --namespace spark --service-account spark \
  --sql "SELECT 'quoted;semicolon' AS value; SELECT sum(id) FROM range(100);" \
  --executor-initial-instances 1 --conf spark.sql.shuffle.partitions=2 --wait
```

Use exactly one of `--file` and `--sql`. `--logs` streams output and waits;
`--wait` waits without streaming. Both return a nonzero CLI exit status when the
application fails (exit code `3`; timeouts use `5`). Add `--output json` for the
shared versioned submission receipt, or `--timeout 300` to bound observation after
submission. A timeout does not delete or stop the application. Without either
waiter flag, submission returns before the
job finishes. Use the regular `app` commands to inspect its status or logs.

## Python

`SparkOnK8S.submit_sql` accepts the same submission options as `submit_app`, except
`app_path`, `app_arguments`, and `class_name`, which the SQL runner manages:

```python
from spark_on_k8s.client import ExecutorInstances, SparkOnK8S

client = SparkOnK8S()
pod_name = client.submit_sql(
    sql_file="queries.sql",  # Or sql="SELECT 1; SELECT 2;"
    image="apache/spark:4.3.0-scala2.13-java21-python3-ubuntu",
    namespace="spark",
    service_account="spark",
    executor_instances=ExecutorInstances(initial=1),
    app_waiter="log",
)
print(client.app_manager.app_status(namespace="spark", pod_name=pod_name))
```

The return value is the driver Pod name, as with `submit_app`. Set `dry_run=True`
to return an offline `ApplicationPlan`; its `to_dict()` preview redacts SQL
contents and other ConfigMap data. Generated SQL mounts take precedence over
matching mounts in a driver template. Explicit conflicting SDK mounts and nested
driver-template mounts that could shadow the SQL files are rejected. SQL runs in a
single Spark session and preserves temporary views between statements. The first
failed statement stops execution and fails the application. Statements already
completed are not rolled back: this is not a multi-statement transaction.

The client splits SQL before submission, preserving quoted semicolons and
comments. It mounts the runner and a JSON statement list in a managed ConfigMap
at `/configmap`; do not mount another ConfigMap there. The image needs PySpark,
but does not need Airflow, `spark-on-k8s`, or `sqlparse`. Deleting the driver Pod
removes the managed ConfigMap through Kubernetes ownership. Empty or comment-only
SQL is rejected by the SDK and CLI.

SQL is plaintext in the ConfigMap and may appear in application logs. Use
Kubernetes Secrets for credentials, and configure storage/catalog credentials
through Spark settings or the SDK's secret options. Input and output data must
be accessible to the driver and executors; a path on the submitting machine is
not automatically uploaded. The SQL file itself is uploaded, subject to the
Kubernetes ConfigMap size limit.

For scheduled and templated SQL, the [Airflow operator](airflow.md) uses the same
runner. Jinja templating belongs to Airflow; the standalone SDK and CLI send SQL
as provided. Review [Spark compatibility](spark_compatibility.md) before changing
runtime images or connector versions.

## Locate a failed statement

The SQL runner records the local file basename (or `<inline>`), statement number
and original starting line in a companion metadata file. It stops at the first
failure and reports that location plus the Spark error condition when available.
The runner no longer prints the complete query in its own progress or failure
messages. Spark's own logs and displayed result rows can still contain data;
apply your normal log-access and retention controls.

Earlier successful statements are not rolled back. For quality gates, run
explicit Spark SQL `assert_true(...)` statements before writes where possible.
A successful pod only establishes program completion, not business correctness.
