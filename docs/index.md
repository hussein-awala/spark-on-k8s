# Spark on Kubernetes

Submit and observe Spark applications from Python, the CLI or Airflow using your
existing Kubernetes cluster. Optional API and shared web-console components make
applications, logs and diagnostics accessible to operators. Spark creates the
executors; your cluster and applications retain their infrastructure and data
responsibilities.

[Start here](user-guide/start-here.md) selects a route by outcome and names its
prerequisites, installation extra and first success checkpoint:

- Run the [ETL fixture locally](user-guide/starters.md#order-events-to-parquet)
  before configuring a cluster.
- Submit an existing application with a [portable job file](user-guide/jobs.md)
  or the [Python/CLI guide](user-guide/getting_started.md).
- Use an existing [Spark Connect endpoint](user-guide/spark_connect.md).
- Inspect applications through [diagnostics](user-guide/diagnostics.md), the
  optional console or the [Airflow task tab](user-guide/airflow.md).

Choose the [documentation and installation matching your release](user-guide/installation.md).
The current source can include features not yet on PyPI; chart and package releases
are independent. Read [support boundaries](user-guide/support.md) before planning
production operation: this project does not provide a multi-tenant scheduler,
a highly available metadata service or an embedded identity provider.

Contributors can use the [repository map and check routes](development.md).
Maintainers use the [release runbook](releases.md) to qualify and publish artifacts.
