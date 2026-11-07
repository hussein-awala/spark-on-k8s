# Portable job files and offline plans

Use one versioned job definition from your laptop, CI, or an Airflow DAG. Job files
need only the base package: no Airflow or API installation is required.

```yaml
apiVersion: spark-on-k8s/v1
kind: SparkJob
metadata:
  name: daily-etl
spec:
  image: registry.example.com/team/spark-etl:1.0
  app_path: local:///opt/spark/jobs/etl.py
  namespace: spark
  service_account: spark
  executor_instances:
    initial: 2
  spark_conf:
    spark.sql.shuffle.partitions: 8
profiles:
  production:
    spec:
      namespace: production
      executor_instances:
        initial: 4
      spark_conf:
        spark.sql.shuffle.partitions: 32
```

```bash
spark-on-k8s job validate job.yaml --profile production
spark-on-k8s job plan job.yaml --profile production --output json
spark-on-k8s job submit job.yaml --profile production --output json
```

Validation and planning do not open a Kubernetes connection. Plans use the same
resource builder as submission, with deterministic preview identifiers. They are
illustrations, not reserved runs or manifests to apply with `kubectl`: owner
references require the real driver pod UID and are attached during submission.

The display preview redacts Secret and ConfigMap contents, literal environment
values, sensitive configuration keys, and application arguments. Review any custom
metadata before sharing it; arbitrary user text can contain credentials in places
no automatic redactor can identify. `ApplicationPlan.resources` is the raw in-memory
representation and can contain secrets; use `to_dict()` for display.

## Profiles and settings

Profiles recursively merge objects and replace lists and scalar values. A selected
profile is validated after merging. Unknown settings, duplicate YAML keys, aliases,
invalid resource quantities, and incompatible executor allocation bounds are errors.
Files are limited to 1 MiB. Resource memory and overhead are integer MiB; CPU values
are positive whole cores. For dynamic allocation, an omitted initial count defaults
to the minimum (or zero when only a maximum is given).

Settings under `spec` match the Python SDK names: `image`, `app_path`, `namespace`,
`service_account`, `class_name`, `packages`, `app_arguments`, `spark_conf`,
`driver_resources`, `executor_resources`, `executor_instances`, `image_pull_policy`,
`app_waiter`, `startup_timeout`, `ui_reverse_proxy`, driver/executor node selectors,
labels and annotations, `driver_env_vars_from_secrets`, and `executor_pod_template`
or `executor_pod_template_path`, plus `driver_pod_template` and `driver_container_name`. Secrets are referenced by name; literal secret
payloads are deliberately not a job-file field.

Job submission defaults to `no_wait`, namespace `default`, service account `spark`,
two executors, and 1 CPU/1024 MiB memory/512 MiB overhead per pod. Job files do not
inherit `SPARK_ON_K8S_*` submission defaults. Set `spec.app_waiter: wait` or `log` to
wait, and optionally bound CLI waiting with `--timeout SECONDS`.

A driver or executor template may be a Pod mapping, inline YAML, or a `.yaml`/`.yml` filename.
Local filenames resolve relative to the job file, not the current directory.
`executor_pod_template_path` is a path visible to Spark, not a local file to upload.
Application paths are also interpreted by Spark; local Python files are not bundled.

Choose cluster credentials explicitly when needed:

```yaml
kubernetes:
  config_file: ./kubeconfig
  context: development
```

Or use `kubernetes: {in_cluster: true}` inside Kubernetes. The default uses the
standard kubeconfig/current context. A custom kubeconfig filename resolves relative
to the job file. Never commit kubeconfig credentials to source control.

## Python and Airflow

```python
from spark_on_k8s.jobs import JobSpec

job = JobSpec.from_file("job.yaml", profile="production")
preview = job.plan().to_dict()
driver_pod = job.submit()
```

The same definition works with both supported Airflow versions:

```python
from spark_on_k8s.airflow.operators import SparkOnK8SOperator
from spark_on_k8s.jobs import JobSpec

job = JobSpec.from_file("job.yaml", profile="production")
run = SparkOnK8SOperator(
    task_id="daily_etl",
    kubernetes_conn_id="spark_cluster",
    **job.to_operator_kwargs(),
)
```

For Airflow, use a job without a `kubernetes` section: the operator connection owns
credentials. Set `app_waiter` to `wait` or `log` when downstream tasks depend on
completion, and use the operator's existing deferrable option when appropriate.

## Validate every environment

```bash
spark-on-k8s job validate job.yaml --all-profiles --output json
```

This validates the base definition and every named profile, including relative
pod-template files, without connecting to Kubernetes. The result lists each
profile and returns a nonzero exit code if any fails. `--profile` and
`--all-profiles` are mutually exclusive. Run this in your configuration CI;
validating only a development profile does not validate production overrides.

## Review CPU and memory requests

Plans include a `capacity` summary derived from the generated driver requests
and effective Spark configuration. It shows executor heap, explicit overhead,
off-heap and PySpark allocations, executor counts and minimum/initial/maximum
CPU and memory requests. Memory is reported in MiB and CPU in cores.

```bash
spark-on-k8s job plan job.yaml --profile production --output json \
  --max-cpu 12 --max-memory-mib 32768
```

Optional budgets fail when a requested maximum exceeds the limit **or cannot be
calculated**. Executor templates, driver sidecars/init containers, invalid
resource settings and unbounded dynamic allocation are called out instead of
being silently estimated. The manifest remains the source for detailed review.
Admission mutation, quota availability, scheduling placement, storage, GPUs and
runtime-created resource profiles are outside this estimate. This is a request
budget check, not a pricing estimate or an admission controller.

For promotion, select the cluster as well as the namespace:

```yaml
profiles:
  production:
    kubernetes:
      context: production-cluster
    spec:
      namespace: analytics-production
```

The context name is a reference to externally managed credentials, not a secret
payload. Namespace selection alone does not select a cluster. For Airflow, omit
this Kubernetes section and use the reviewed Airflow connection instead.
