# Ecosystem recipes

These recipes combine [job files](jobs.md) with common Kubernetes and Spark
ecosystem tools. They reuse existing SparkJob settings and well-established
Kubernetes or Spark configuration; nothing here adds a controller or a new
integration package. They are configuration starting points, **not** part of the
tested support matrix: this project's CI does not run a CronJob, a batch
scheduler, an OpenLineage backend or an object store. Pin versions, run the
recipe in a staging namespace and check the actual output before relying on it.
See [support boundaries](support.md) for what the project owns.

Never write credentials into job files, ConfigMaps or `spark_conf`: Spark
properties appear in driver arguments and the Spark UI. Use Kubernetes Secrets
or workload identity as shown below.

## Schedule with a Kubernetes CronJob

Use this when a schedule does not justify an Airflow deployment, for example to
replace a `ScheduledSparkApplication`. A CronJob starts a small submitter pod that
runs `spark-on-k8s job submit`, waits for the application and exits with the
[automation exit code](automation.md#exit-codes). For dependencies between jobs,
backfills or retries with history, prefer the [Airflow integration](airflow.md).

### Submitter image and identity

Any image with the spark-on-k8s package works; the submitter does not need Spark or
Java. Pin the version you validated and publish the image by digest:

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir "spark-on-k8s==VERSION"
USER 10001
ENTRYPOINT ["spark-on-k8s"]
```

The submitter's service account is separate from the driver's service account
(`spec.service_account` in the job file), which creates executors. Grant the
submitter only what submission and waiting use, in the job's namespace:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-submitter
  namespace: reporting
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-submitter
  namespace: reporting
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["create"]
  # ConfigMaps hold pod templates and are owned by the driver after creation.
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-submitter
  namespace: reporting
subjects:
  - kind: ServiceAccount
    name: spark-submitter
    namespace: reporting
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-submitter
```

These rules follow the API calls the client makes for a job file without
`secret_values`; confirm them with your cluster's audit logs. Add `delete` on
pods only if the same identity also runs `app delete` for cleanup.

### Job file and CronJob

Make the submission wait so that the CronJob's concurrency policy applies to Spark
runs, and select in-cluster credentials with a profile:

```yaml
apiVersion: spark-on-k8s/v1
kind: SparkJob
metadata:
  name: nightly-report
spec:
  image: registry.example.com/team/report:1.0
  app_path: local:///opt/spark/work-dir/report.py
  namespace: reporting
  service_account: spark
  app_waiter: wait
  startup_timeout: 600
profiles:
  in-cluster:
    kubernetes:
      in_cluster: true
```

```bash
kubectl -n reporting create configmap nightly-report-job --from-file=job.yaml
```

Put pod-template files referenced by the job into the same ConfigMap so that
relative paths still resolve next to `job.yaml`.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-report
  namespace: reporting
spec:
  schedule: "0 2 * * *"
  timeZone: Europe/Paris
  concurrencyPolicy: Forbid
  startingDeadlineSeconds: 600
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 5
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          serviceAccountName: spark-submitter
          restartPolicy: Never
          containers:
            - name: submit
              image: registry.example.com/platform/spark-on-k8s-cli@sha256:DIGEST
              args:
                - job
                - submit
                - /jobs/job.yaml
                - --profile
                - in-cluster
                - --output
                - json
                - --receipt-file
                - /receipts/$(POD_NAME).json
              env:
                - name: POD_NAME
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
              volumeMounts:
                - name: job
                  mountPath: /jobs
                  readOnly: true
                - name: receipts
                  mountPath: /receipts
          volumes:
            - name: job
              configMap:
                name: nightly-report-job
            - name: receipts
              persistentVolumeClaim:
                claimName: spark-receipts
```

The receipt is written before waiting starts, to a new file named after the
submitter pod, so a crashed or deleted submitter still leaves the run's namespace,
application ID and driver UID. Use a volume the container user can write; the JSON
result on stdout (kept in the pod log while Kubernetes retains the Job) carries the
same identity. Resume observation or cancel from any machine with access to the
cluster:

```bash
spark-on-k8s app wait --namespace reporting --app-id "$APP_ID" --output json
spark-on-k8s app cancel --receipt nightly-report-29000000-abcde.json --output json
```

### Failure and concurrency semantics

- **A receipt is not data success.** Exit code `0` with `app_waiter: wait` means
  the application reached `Succeeded`; your application still owns output
  correctness. Exit `3` means it failed; `5` and `6` mean observation timed out or
  the outcome is unknown, and the application may still be running.
- **Retries resubmit.** Kubernetes retries a failed Job pod by starting a new
  pod, which submits a new application. `backoffLimit: 0` avoids duplicate runs
  after an ambiguous failure. To retry only confirmed Spark failures, add a
  [pod failure policy](https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-failure-policy)
  that fails the Job for every exit code except `3`, and raise `backoffLimit`.
  Make outputs idempotent before allowing any retry.
- **Timeouts do not cancel.** `--timeout`, Job `activeDeadlineSeconds` and pod
  deletion stop the submitter only. The driver pod is not owned by the Job, so
  the application keeps running and a later schedule can overlap it despite
  `concurrencyPolicy: Forbid`. Cancel explicitly with `app cancel --receipt`.
- **Forbid covers waiting submitters.** With `app_waiter: no_wait`, the submitter
  exits after submission and `concurrencyPolicy` no longer limits Spark runs.
  `Replace` deletes the waiting submitter, not the running application.
- **Schedules are best effort.** Kubernetes can occasionally start a scheduled Job
  twice or skip one, for example after controller downtime past
  `startingDeadlineSeconds`. Design the job to tolerate a repeated or missed run.
- **History limits apply to submitters.** CronJob history limits delete Job and
  submitter pods, not Spark drivers. Remove finished drivers with
  `spark-on-k8s app delete`; executors and client-created objects are owned by the
  driver pod.

## Run under a batch scheduler

Batch schedulers such as Volcano, Apache YuniKorn and Kueue queue pods by reading
pod fields, labels and annotations. spark-on-k8s creates the **driver** pod
directly, and the running driver creates **executor** pods through Spark. Both
pods must therefore carry the scheduler's settings: the driver through
`driver_pod_template` (or the driver label and annotation maps), executors
through `executor_pod_template` (or the executor maps).

```yaml
apiVersion: spark-on-k8s/v1
kind: SparkJob
metadata:
  name: nightly-features
spec:
  image: registry.example.com/ml/features:3.2
  app_path: local:///opt/app/build_features.py
  namespace: ml
  startup_timeout: 1800
  driver_pod_template:
    apiVersion: v1
    kind: Pod
    spec:
      schedulerName: SCHEDULER_NAME
      priorityClassName: batch-low
      containers:
        - name: driver
  executor_pod_template:
    apiVersion: v1
    kind: Pod
    spec:
      schedulerName: SCHEDULER_NAME
      priorityClassName: batch-low
      containers:
        - name: executor
  driver_labels: {}       # the queue or group labels your scheduler documents
  executor_labels: {}
  driver_annotations: {}  # the queue or group annotations your scheduler documents
  executor_annotations: {}
```

`schedulerName` and `priorityClassName` are standard Kubernetes Pod fields.
`spark.kubernetes.executor.scheduler.name` in `spark_conf` is an alternative for
executors; Spark's driver-side scheduler settings do not apply because the client
creates the driver pod. Scheduler names, queue and group label or annotation keys,
and whether a scheduler needs a separate group object are scheduler-specific:
**check your scheduler's documentation** for the version you run. Kueue, for
example, does not select pods by `schedulerName`; check how your Kueue version
admits plain pods.

Consider these properties of direct submission when designing queues:

- Executors do not exist when the driver is admitted. They are created later, in
  batches, and dynamic allocation can add or remove them during the run. Gang
  scheduling that expects every member up front does not see the executors when
  it admits the driver; check how your scheduler handles members created later.
- Spark's built-in Volcano integration creates its pod group while `spark-submit`
  creates the driver in cluster mode. That step does not run here. Do not enable
  Spark's Volcano feature steps unless you have verified that the pod group they
  reference exists.
- A queued driver stays `Pending`. `startup_timeout` bounds that wait for an
  observing CLI or SDK caller without deleting the application; the Airflow
  operator's startup timeout deletes it. Choose a value larger than expected
  queue time.
- `job plan --output json` reports driver and executor CPU and memory requests,
  which helps size queue quotas. It does not predict admission or placement.

## Emit lineage with OpenLineage

The [OpenLineage](https://openlineage.io/docs/integrations/spark/) Spark
integration is a Spark listener that sends run and dataset events from the
driver. Configure it through `packages` and `spark_conf`, for example in a profile
so that uninstrumented runs remain unchanged:

```yaml
profiles:
  lineage:
    spec:
      packages:
        - io.openlineage:openlineage-spark_2.13:VERSION
      spark_conf:
        spark.extraListeners: io.openlineage.spark.agent.OpenLineageSparkListener
        spark.openlineage.transport.type: http
        spark.openlineage.transport.url: http://marquez.lineage.svc:5000
        spark.openlineage.namespace: analytics
```

```bash
spark-on-k8s job submit job.yaml --profile lineage --output json
```

- Choose the artifact matching your image's Scala version (`_2.12` or `_2.13`)
  and a release that supports your Spark version; check the OpenLineage
  documentation. A profile replaces the base `packages` list, so repeat any base
  packages in it.
- `packages` downloads from Maven repositories when the driver starts. For
  reproducible or offline clusters, bake the jar into the image and add it with
  `spark.jars: local:///...`, as the [lakehouse starter](starters.md) does with
  Iceberg.
- The driver needs network access to the endpoint. Do not put API keys in
  `spark_conf`; use the credential options your OpenLineage version documents
  together with a Kubernetes Secret.
- Lineage events are optional evidence of what Spark reported reading and
  writing. They are not proof that a write committed, and spark-on-k8s
  [run history](run-history.md) is not lineage.

## Read and write S3 or MinIO

Spark reads `s3a://` paths through Hadoop's S3A connector from the `hadoop-aws`
module. Most Spark images do not include it. Use the `hadoop-aws` version that
matches the Hadoop client bundled with your image (check the `hadoop-client-*`
jars under `/opt/spark/jars`); it brings the matching AWS SDK bundle.

```yaml
apiVersion: spark-on-k8s/v1
kind: SparkJob
metadata:
  name: orders-to-minio
spec:
  image: registry.example.com/team/orders:2.1.0
  app_path: local:///opt/spark/work-dir/orders.py
  app_arguments: ["--output", "s3a://warehouse/orders/"]
  namespace: spark
  service_account: spark
  packages:
    - org.apache.hadoop:hadoop-aws:HADOOP_VERSION
  driver_env_vars_from_secrets:
    - minio-credentials
  spark_conf:
    spark.hadoop.fs.s3a.endpoint: https://minio.storage.svc:9000
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID: minio-credentials:AWS_ACCESS_KEY_ID
    spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY: minio-credentials:AWS_SECRET_ACCESS_KEY
```

The Secret `minio-credentials` must exist in the job's namespace with the keys
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`; create it with your secret
management tooling rather than literal values in shell history.
`driver_env_vars_from_secrets` exposes every key of the Secret to the driver, and
the `secretKeyRef` properties expose the two keys to each executor. S3A's default
credential chain reads these standard environment variables. Both driver and
executors read and write data, so both need the credentials.

For AWS S3 with workload identity (for example EKS IAM roles for service
accounts), omit the Secret and endpoint settings. Annotate the driver service
account as your platform documents, and set the S3A credential provider for the
Hadoop version in your image:

```yaml
spec:
  service_account: spark-s3
  spark_conf:
    # Hadoop 3.4 (AWS SDK v2); Hadoop 3.3 uses com.amazonaws.auth.WebIdentityTokenCredentialsProvider.
    spark.hadoop.fs.s3a.aws.credentials.provider: software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider
```

Executors use the driver's service account unless
`spark.kubernetes.authenticate.executor.serviceAccountName` selects another, so
the identity applies to both. Check the Hadoop S3A documentation for your version
before changing committers, encryption or endpoint settings.

If the image's user has no home directory, set `spark.jars.ivy` to a writable path
such as `/tmp/.ivy2` so that `packages` can be resolved, or bake the jars into the
image. `job plan` redacts sensitive configuration keys and Secret values, but it
cannot detect credentials written under unrecognized names.
