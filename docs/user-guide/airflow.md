# Airflow

The source version supports Airflow 3.4 on Python 3.11–3.14 and Airflow 2.11.2 on
Python 3.11–3.12. Follow the [installation instructions](installation.md) for the
`airflow` extra; published releases can differ from the current source. Install it
in the Airflow components that load and execute your DAGs, including the triggerer
when using deferrable tasks. The package registers its provider automatically.

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
