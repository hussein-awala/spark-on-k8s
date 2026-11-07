# Start here

Choose the outcome you need. The API and Airflow are optional: neither is required
to submit a job from Python or the CLI. These routes describe the current source;
first choose [a published release or a source checkout](installation.md).

| Your goal | Prerequisites and owner | Install/profile | First observable success | Next guide |
| --- | --- | --- | --- | --- |
| See a real ETL result locally | Application author: source checkout, Docker and capacity for the documented 2 GiB container; no cluster | Starter application image; no API/Airflow | `SPARK_STARTER_OK` after Parquet output and replay checks | [ETL starter](starters.md#order-events-to-parquet) |
| Submit an existing Python/JVM application | Administrator supplies namespace, scoped service account and cluster access; author supplies a compatible image and reachable data | Core package; `sql` extra for SQL | Recorded driver identity, then terminal success and application output | [Getting started](getting_started.md), [job files](jobs.md), [automation](automation.md) |
| Explore an existing Spark Connect service | Platform owner supplies endpoint, trust/credentials and permitted data; author uses a compatible client | `connect` extra on its supported Python versions | Bounded check succeeds against that endpoint | [Spark Connect](spark_connect.md) |
| Observe jobs in a browser | Operator supplies API deployment, scoped Kubernetes access and access-controlled networking | `api` extra or a selected API image/chart | Console lists the expected namespace and opens a known run | [Getting started](getting_started.md), [diagnostics](diagnostics.md) |
| Run a scheduled Airflow task | Airflow owner supplies a supported installation, connection and worker/triggerer access; author supplies the job image | `airflow`; `airflow-ui` for native Airflow 3 task tab | Task runs an application and reaches its documented outcome | [Airflow](airflow.md) |

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
read [support boundaries](support.md).
