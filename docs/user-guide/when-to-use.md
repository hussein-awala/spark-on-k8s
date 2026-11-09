# When to use spark-on-k8s

Use this page to decide whether spark-on-k8s fits your team before planning a
pilot. It describes the current source; consult the documentation matching your
installed release for its available features. Other products appear here only
with their general, long-standing architecture. Check their own documentation
for current features, support terms and pricing.

## What it is

Spark-on-k8s is a Python package that submits Spark applications to a Kubernetes
cluster you already operate. The client creates the driver pod, and the resources
that belong to it, such as its Service and any Secret or ConfigMaps, directly
through the Kubernetes API with the caller's credentials. The Apache Spark image's
`driver` entrypoint runs `spark-submit` in client mode inside that pod; Spark then
creates and owns the executors. The submitting machine needs no Spark or Java.

Nothing is installed in the cluster to submit a job: there is no controller,
custom resource definition or admission webhook. The cluster needs a namespace,
a driver service account and permission for the submitter to create the
application's resources. The other pieces are optional and reuse the same core:

| Component | What it adds | Guide |
| --- | --- | --- |
| Python client and CLI | Submit, wait, stream logs, check status and cancel a recorded run; versioned JSON results, receipts and exit codes; job files with offline plans; SQL jobs; diagnostics | [Getting started](getting_started.md), [automation](automation.md), [job files](jobs.md) |
| API and web console | Application list, driver logs, Spark UI and History Server proxies, and opt-in policy-gated remote submission; deployed with the Helm chart | [Chart deployment](chart-deployments.md), [remote submission](remote-submission.md) |
| Run history | Bounded driver metadata that remains after pods disappear, in a single-replica SQLite store | [Run history](run-history.md) |
| Airflow provider | Application and SQL operators, deferrable waiting, adoption of a recorded driver on retry, and an Airflow 3 task tab | [Airflow](airflow.md) |
| Spark Connect | Profiles for existing endpoints and a lifecycle helper for private single-driver servers | [Spark Connect](spark_connect.md), [server setup](spark_connect_server.md), [Jupyter](jupyter.md) |
| Experimental dbt adapter | Temporary Spark compute for one dbt Core command, installed separately | [Managed dbt](dbt.md) |

## What it is not

These boundaries are deliberate; the [support boundaries](support.md) and
[operational boundaries](operations.md) describe them in more detail.

- **Not a controller.** A `SparkJob` file is loaded by a client; it is not a
  Kubernetes custom resource, and nothing reconciles desired state after the
  client exits. Driver pods use `restartPolicy: Never`, and a failed driver is
  not restarted or replaced automatically.
- **Not a scheduler.** There is no built-in schedule or queue. Use Airflow, a
  workflow engine or CI to decide when to submit and whether to retry.
- **Not a multi-tenant service.** The API has no built-in user authentication.
  Remote submission uses a shared operator token and an admission policy, which
  is not per-user authorization or tenant isolation.
- **Not exactly-once submission.** An ambiguous creation response is reported as
  unknown and never blindly retried. There is no deduplication key, and a
  receipt identifies a run; it does not make data writes idempotent.
- **Not an infrastructure manager.** It does not create clusters, node pools,
  cloud identities, storage or catalogs, and it does not autoscale nodes.
  Executors can scale with Spark dynamic allocation within the capacity your
  cluster provides.
- **Not a log or audit store.** Driver logs live with their pods, Spark event
  logs need a Spark History Server, and run history keeps operational metadata
  only.
- **Not a managed Spark distribution.** You supply the application image. The
  [Spark compatibility matrix](spark_compatibility.md) lists the tested Apache
  Spark images. There is no vendor support contract or response-time SLA.

## Compare the alternatives

| Approach | What you install and operate | Who owns a run's lifecycle | Usually the better fit when |
| --- | --- | --- | --- |
| Spark-on-k8s | The Python package wherever you submit; optionally the API chart, Airflow provider or dbt adapter | The caller: a script, CI job, notebook or Airflow task submits, then decides whether to wait, retry or cancel | You operate Kubernetes and want Python-native submission without cluster-scoped components |
| A Spark operator (`SparkApplication` resources) | A controller, its custom resource definitions and RBAC, and their upgrades | The controller, which reconciles each custom resource | You want Spark applications declared and managed as Kubernetes objects |
| Airflow `KubernetesPodOperator` running `spark-submit` | Airflow's Kubernetes provider and an image that can run `spark-submit` | The Airflow task pod; your script and Spark settings define how it relates to the driver | Your Airflow deployment standardizes on generic container tasks |
| `spark-submit` against Kubernetes | A Spark distribution and Java on each submitting machine | The person or script running `spark-submit` | Spark users who already maintain a local Spark installation |
| Amazon EMR on EKS | Amazon EKS plus the EMR service configuration and AWS IAM roles | The AWS service, through its job-run API | Your organization standardizes on AWS-managed Spark runtimes and support |
| Databricks | A Databricks account and workspace; no Spark infrastructure in your Kubernetes cluster | The Databricks platform | You prefer a managed data platform to operating Spark on Kubernetes |

These approaches can coexist. A team can keep a Spark operator or a managed
platform for some workloads and use spark-on-k8s where direct submission fits.

### A Spark operator and `SparkApplication` resources

Kubernetes operators for Spark, such as the Kubeflow Spark Operator or the Apache
Spark Kubernetes Operator, add custom resource definitions to the cluster and run
a controller that reconciles them.
You declare an application as a custom resource, for example `SparkApplication`,
and the controller submits it and records its state on that resource. Depending
on the operator and its version, the controller can also apply restart policies
or schedules declared in the resource.

**Prefer an operator when** you want Spark applications managed with `kubectl`
and GitOps tools like other Kubernetes objects, you need the cluster rather than
the caller to own restarts or schedules, or your platform already runs one.
Airflow's Kubernetes provider also includes an operator that creates
`SparkApplication` resources; those require a compatible Spark operator in the
cluster.

**Prefer spark-on-k8s when** you cannot or do not want to install cluster-scoped
resources, or the submitting process should own each run. It needs namespace
permissions rather than custom resource definitions, and it returns a run
identity to the caller instead of a resource to reconcile.

**Operational cost.** An operator adds a controller deployment, CRD upgrades and
the cluster-scoped privileges needed to install them. Spark-on-k8s adds nothing
to the cluster for submission, but restart and scheduling policy move to whoever
submits. If both submit drivers to one namespace, drivers started by the operator
can appear in the spark-on-k8s console's application list; use separate
namespaces when each tool should show only its own runs.

### Airflow `KubernetesPodOperator` running `spark-submit`

`KubernetesPodOperator`, from Airflow's Kubernetes provider, runs a container as
a pod for each task. Teams often use it to run `spark-submit` from a Spark image.
In client mode the task pod is the driver; in cluster mode it asks Kubernetes to
create a separate driver pod.

**Prefer `KubernetesPodOperator` when** your Airflow deployment standardizes on
generic container tasks, the task does more than submit Spark, or you do not
want another Python package in Airflow workers.

**Prefer the spark-on-k8s operators when** you want the Airflow task to track the
Spark driver directly. `SparkOnK8SOperator` and `SparkSqlOnK8SOperator` create the
driver from the worker without a separate submitter pod, wait for it in the
triggerer when deferrable, adopt a recorded Pending or Running driver on retry
while its task XCom record remains, and apply documented finish, kill and
startup-timeout actions. See the [lifecycle table](airflow.md#lifecycle-retries-and-cleanup).

**Operational cost.** Both approaches use an Airflow Kubernetes connection. The
spark-on-k8s provider must be installed in the Airflow components that run your
DAGs, including triggerers for deferrable tasks. The optional task tab also needs
a separately deployed spark-on-k8s API. With `KubernetesPodOperator`, retry and
kill behavior for the Spark driver depends on your wrapper script and
`spark-submit` configuration.

### `spark-submit` against Kubernetes

Apache Spark's `spark-submit` supports a Kubernetes master. In cluster mode, it
creates the driver pod from a machine that has a Spark distribution and Java.
Spark-on-k8s still runs `spark-submit`, but inside the driver pod; it replaces
the submitting side.

**Prefer `spark-submit` when** your users already maintain a Spark installation
and rely on Spark's own submission options, such as uploading local dependencies
at submission time. Spark-on-k8s expects the application and its dependencies to
be available to the driver, normally inside the image; it does not upload files
from the submitting machine.

**Prefer spark-on-k8s when** submitters, CI jobs or services should not need a
JVM, or you want run receipts, JSON results with stable exit codes, cancellation
by recorded identity, portable job files and diagnostics.

### Amazon EMR on EKS

Amazon EMR on EKS is an AWS-managed service. You register a namespace in an
Amazon EKS cluster as an EMR virtual cluster and submit job runs through AWS APIs.
Jobs use Amazon EMR release images, access is governed by AWS IAM, and the
service has its own AWS pricing in addition to the underlying cluster and compute.

**Prefer EMR on EKS when** your organization runs on Amazon EKS and wants
AWS-built Spark runtimes, job runs managed through AWS APIs and IAM, and support
through AWS.

**Prefer spark-on-k8s when** you need the same tool on any conformant Kubernetes
cluster, including local, on-premises or other clouds, or you want to run Apache
Spark images or images you build yourself. It is open-source software under the
Apache License 2.0; your costs are your own cluster and its operation.

**Operational cost.** With EMR on EKS, AWS provides the job submission service
and runtime images. With spark-on-k8s, you own image builds, Spark upgrades,
cloud identity setup, log and history retention, and troubleshooting.

### Databricks

Databricks is a managed data and AI platform. It provisions and operates Spark
compute outside your Kubernetes cluster, and provides its own workspace,
notebooks, job scheduling, access controls and pricing.

**Prefer Databricks when** you want a managed platform rather than operating
Spark: managed compute and autoscaling, collaborative notebooks, built-in
scheduling, workspace user and permission management, and vendor support.

**Prefer spark-on-k8s when** Spark must run inside Kubernetes clusters you already
operate, for example to reuse their capacity, namespaces and network policies,
and your team is prepared to own the images, storage access and operations.

**Operational cost.** Spark-on-k8s has no platform subscription, but you take on
the work a managed platform otherwise performs: capacity, autoscaling, security
boundaries, catalog and storage integration, upgrades and support.

## Do not use spark-on-k8s if

- You need a controller to reconcile declared Spark resources, or to restart
  failed drivers automatically, including those of long-running streaming jobs.
- You need per-user or per-tenant authorization inside the tool. Local SDK and
  CLI users act with their own Kubernetes credentials, but the API acts as one
  service account. Use separate deployments and namespaces for separate trust
  boundaries.
- You need exactly-once submission or a deduplication guarantee across retries.
- You need the tool to provision or autoscale clusters, node pools, storage,
  catalogs or cloud identities.
- You need a durable audit trail, retained driver logs or a highly available
  metadata service from the tool itself.
- You need a pool of shared, warm SQL or Spark Connect servers. The Connect
  helper creates private single-driver servers without authentication gateways.
- You need a vendor support contract, response-time SLA or certified runtime.
- You do not have, or do not want to operate, a Kubernetes cluster.

## Moving from another approach

There is no importer or converter. Spark-on-k8s does not read `SparkApplication`
resources, `spark-submit` scripts or managed-service job definitions. Plan a
side-by-side trial in a staging namespace instead:

- **From a Spark operator:** map each resource field to a [job file](jobs.md)
  setting, Spark property or [driver pod template](driver_pod_templates.md).
  List the lifecycle features without an equivalent, such as restart policies,
  schedules and cleanup handled by the controller, and decide who will own them,
  or keep those workloads on the operator. Read
  [lifecycle ownership](automation.md#contract-coverage-and-lifecycle-ownership)
  first, and record every field you cannot map instead of dropping it; see
  [qualifying an integration](operations.md#qualify-an-additional-integration).
- **From `KubernetesPodOperator` and `spark-submit`:** move the submission to
  [`SparkOnK8SOperator`](airflow.md), keep the application in its image, and
  compare your current retry and kill handling with the
  [Airflow lifecycle table](airflow.md#lifecycle-retries-and-cleanup). Grant the
  worker and triggerer connections only the permissions in the
  [Airflow permission table](airflow.md#separate-execution-and-task-tab-permissions).
- **From `spark-submit`:** map its arguments to
  [`spark-on-k8s app submit`](getting_started.md#cli) options, which accept Spark
  properties as repeated `--conf key=value`, or to a [job file](jobs.md)'s
  `spark_conf`. Use the [automation contract](automation.md) instead of parsing
  log output.
- **From EMR on EKS or Databricks:** the repository has no migration guidance.
  Evaluate runtime differences between the vendor's Spark build and the
  [Apache Spark images you would use](spark_compatibility.md), connector and
  catalog availability, how cloud identity maps to Kubernetes service accounts,
  where scheduling will live, and how logs and history will be retained.

For any pilot, record the versions and staging evidence described in
[Evaluate and report](support.md#evaluate-and-report): at least one real job and
its diagnostics, plus the recovery procedure if you enable persistent history.
Include a controlled failure so that you observe how each tool reports it.
