# Start here

Choose the outcome you need. The API and Airflow are optional: neither is required
to submit a job from Python or the CLI. These routes describe the current source;
first choose [a published release or a source checkout](installation.md).
To compare spark-on-k8s with a Spark operator, a managed Spark service or plain
`spark-submit`, read [when to use it](when-to-use.md) first.

If you already have a Spark application image and Kubernetes access, follow
[Installation](installation.md), then [Getting started](getting_started.md).
If you want to try the included data example before arranging cluster access,
start with the local ETL route below. It runs Spark and Java inside Docker.

| Your goal | Prerequisites and owner | Install/profile | First observable success | Next guide |
| --- | --- | --- | --- | --- |
| See a real ETL result locally | Application author: source checkout, Docker and capacity for the documented 2 GiB container; no cluster | Starter application image; no API/Airflow | `SPARK_STARTER_OK` after Parquet output and replay checks | [ETL starter](starters.md#order-events-to-parquet) |
| Submit an existing Python/JVM application | Administrator supplies namespace, scoped service account and cluster access; author supplies a compatible image and reachable data | Core package; `sql` extra for SQL | Recorded driver identity, then terminal success and application output | [Getting started](getting_started.md), [job files](jobs.md), [automation](automation.md) |
| Explore an existing Spark Connect service | Platform owner supplies endpoint, trust/credentials and permitted data; author uses a compatible client | `connect` extra on its supported Python versions | Bounded check succeeds against that endpoint | [Spark Connect](spark_connect.md) |
| Explore data from a Jupyter notebook | Platform owner prepares namespace, driver permissions and trusted cluster access; author supplies a compatible image | Locked `notebook` group plus `connect`; optional JupyterLab frontend | Distributed query and rendered table, then reconnection to the same server after a planned kernel restart | [Jupyter guide and notebook](jupyter.md) |
| Run dbt with temporary Spark compute | Platform owner supplies Kubernetes access, shared warehouse and external Hive metastore | Separate experimental `dbt-spark-on-k8s` package | dbt completes and owned compute is observed absent | [Managed dbt](dbt.md) |
| Create an interactive Spark Connect server | Platform owner prepares a namespace, driver permissions and trusted network access | Base package for lifecycle; `connect` extra for queries | Receipt identifies private resources; a real query verifies readiness | [Server setup](spark_connect_server.md) |
| Observe jobs in a browser | Operator supplies API deployment, scoped Kubernetes access and access-controlled networking | `api` extra or a selected API image/chart | Console lists the expected namespace and opens a known run | [Local console](getting_started.md#rest-api), [chart deployment](chart-deployments.md) |
| Run a scheduled Airflow task | Airflow owner supplies a supported installation, connection and worker/triggerer access; author supplies the job image | `airflow`; `airflow-ui` for native Airflow 3 task tab | Task runs an application and reaches its documented outcome | [Airflow](airflow.md) |

The Python version used to submit a job is separate from the runtimes inside its
application image. Use the [Spark compatibility guide](spark_compatibility.md) for
Spark, Java, Scala and application-Python choices; the base client needs no local JVM.

A local ETL check proves computation on its fixture, not cluster networking or
storage permissions. For shared clusters, ask the administrator for namespace-scoped
permissions before using the namespace setup helper: that helper's cluster-wide
grant is intentionally broader. Driver and executor data access must both work.

## When the first attempt fails

| What you observe | Next inspection | What it cannot establish |
| --- | --- | --- |
| No confirmed driver identity | Check the submission error and [doctor guidance](diagnostics.md#check-cluster-prerequisites); an ambiguous response may still have created resources | Caller checks do not prove the driver's runtime access or that nothing was created |
| Driver stays Pending | Diagnose the known application using [diagnostics](diagnostics.md); inspect scheduling, image and storage evidence | An empty or incomplete report does not establish health |
| Driver terminated or logs stopped | Inspect its final status and diagnostic evidence; logs ending alone are not success | A termination request does not prove a completed graceful drain |
| Process succeeded but data is wrong | Check application output assertions and input/replay assumptions; see [starters](starters.md) | Kubernetes success does not validate business results |

Keep the generated run identity. Share only sanitized evidence with the relevant
application or cluster owner. For project fit, compatibility and operating limits,
read [when to use it](when-to-use.md) and [support boundaries](support.md).
