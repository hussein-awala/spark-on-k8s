# Airflow lifecycle tests on Kubernetes

This suite installs the built wheel in a dedicated Airflow image and runs that
image as a Kubernetes pod with an in-cluster service account. It uses Airflow's
`DAG.test()` engine and its real metadata database, XCom, task execution, inline
trigger runner and deferred-task resumption. Spark drivers and executors run as
separate real Kubernetes pods. No Kubernetes or trigger calls are mocked.

This is **not** a deployed scheduler/executor/triggerer-process test. It does not
establish worker-crash recovery, scheduler retry behavior, or a retry reattaching
after Airflow clears XCom. Each case checks that the real driver carries the
task-key label matching its ownership annotation, after the operator's label
lookup has run against the cluster. Reattachment decisions by label and by retained
XCom are covered by the operator regression tests. Production scheduler coverage
needs a separate, explicitly scoped deployment fixture.

The workflow runs Airflow 2.11.2 and 3.4.0 on Python 3.12 against one representative
Spark 4.0.5 image. Existing Spark compatibility CI owns the broader Spark matrix.
Each Airflow version runs six cases: successful Spark execution, deliberate
application failure after actual executor work, and an unschedulable startup
timeout, each with synchronous and deferrable execution. The startup-timeout
cases require a real Pending/Unschedulable pod and operator cleanup even when
`on_finish_action="keep"`. Deferred cases must execute and resume the real trigger.
Failure and timeout cases must log the operator's failure postmortem: the failures
a driver log tail, the timeouts an `unschedulable` finding read before deletion.
Successful cases must not log one.

The successful and deliberately failing applications also check environment
variables inside the driver and executor Python workers. `spark.kubernetes.driverEnv.*`
sets driver-only values, including an empty string and a numeric value converted to
text. `spark.executorEnv.*` sets a different executor value for the same name, and
driver-only values must remain absent from executors. Pod specifications are checked
against the runtime evidence; startup-timeout cases check only the driver Pod
specification because their application never starts. These results are retained
in each case's evidence alongside the execution and cleanup checks.

`.github/workflows/airflow-kubernetes.yml` contains the complete setup. The host
runner accepts only a disposable kind context, creates a unique namespace, saves
per-case results plus logs and Kubernetes evidence, and removes its namespace.
The workflow always deletes its kind cluster. It never connects to a production
cluster. On a workstation, start a Docker daemon before running the same steps.

The example namespace grants the runner and Spark driver the standard `edit`
role for this disposable test only. It is not a production RBAC recommendation.
