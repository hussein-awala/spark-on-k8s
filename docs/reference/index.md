# Python reference

Start with the documented imports below. Each reference page shows signatures,
parameters and return types from this documentation revision. Choose the docs
version that matches your installed package; `develop` can describe unreleased
changes.

## Client and results

| Task | Entry point or result | Usage guide |
| --- | --- | --- |
| Submit and observe Spark applications | [`SparkOnK8S`](spark_on_k8s/client.md#spark_on_k8s.client.SparkOnK8S) from `spark_on_k8s.client` | [Getting started](../user-guide/getting_started.md) |
| Set driver and executor resources | [`PodResources`](spark_on_k8s/client.md#spark_on_k8s.client.PodResources) and [`ExecutorInstances`](spark_on_k8s/client.md#spark_on_k8s.client.ExecutorInstances) from `spark_on_k8s.client` | [Examples](../user-guide/examples.md) |
| Retain a submitted run's identity | [`RunHandle`](spark_on_k8s/utils/run.md#spark_on_k8s.utils.run.RunHandle), returned by `SparkOnK8S.last_submitted_run` | [Automation and recovery](../user-guide/automation.md) |
| Inspect startup or runtime problems | [`Diagnosis`](spark_on_k8s/diagnostics.md#spark_on_k8s.diagnostics.Diagnosis), returned by `SparkOnK8S.diagnose_app()` | [Diagnostics](../user-guide/diagnostics.md) |
| Provision namespace access during setup | [`SparkOnK8SNamespaceSetup`](spark_on_k8s/utils/setup_namespace.md#spark_on_k8s.utils.setup_namespace.SparkOnK8SNamespaceSetup) from `spark_on_k8s.utils.setup_namespace` | [Namespace setup](../user-guide/getting_started.md#set-up-the-kubernetes-namespace) |

A run handle identifies a submission; it does not certify data correctness.
Observation timeouts and cancellation have different effects. Read the lifecycle
rules in the linked guides before automating retries or cleanup.

## Job definitions and integrations

| Task | Entry point or result | Usage guide |
| --- | --- | --- |
| Load a job document and select a profile | [`JobSpec`](spark_on_k8s/jobs.md#spark_on_k8s.jobs.JobSpec) from `spark_on_k8s.jobs` | [Job files](../user-guide/jobs.md) |
| Inspect an offline submission plan | [`ApplicationPlan`](spark_on_k8s/planning.md#spark_on_k8s.planning.ApplicationPlan), returned by planning methods | [Planning a job](../user-guide/jobs.md) |
| Run a Spark task through Airflow | [`SparkOnK8SOperator`](spark_on_k8s/airflow/operators.md#spark_on_k8s.airflow.operators.SparkOnK8SOperator) from `spark_on_k8s.airflow.operators` | [Airflow lifecycle and permissions](../user-guide/airflow.md) |
| Connect to an existing Spark Connect endpoint | [`ConnectProfile`](spark_on_k8s/connect.md#spark_on_k8s.connect.ConnectProfile) from `spark_on_k8s.connect` | [Spark Connect](../user-guide/spark_connect.md) |
| Create a private Spark Connect server | [`ConnectServerConfig`](spark_on_k8s/connect_server.md#spark_on_k8s.connect_server.ConnectServerConfig) and [`ConnectServerHandle`](spark_on_k8s/connect_server.md#spark_on_k8s.connect_server.ConnectServerHandle) from `spark_on_k8s.connect_server` | [Server lifecycle](../user-guide/spark_connect_server.md) |

SQL submission uses the client's `submit_sql()` method or the
Airflow operator's `sql` / `sql_file` options. See [SQL jobs](../user-guide/sql.md)
for the required extra and execution behavior.

## Implementation reference

The **Implementation details** navigation retains module pages for contributors
and existing links, including private helpers and optional integration internals.
It also includes functions defined in package `__init__.py` files, such as the
[SQL preparation helpers](spark_on_k8s/sql/index.md) and
[API client holders](spark_on_k8s/api/index.md). Empty package markers are omitted.

A generated signature is not a support promise for every importable object.
The [support and compatibility policy](../user-guide/support.md) defines which
documented contracts receive migration guidance. For CLI commands, HTTP routes,
configuration and deployment behavior, use the corresponding user guide rather
than importing their implementation functions.
