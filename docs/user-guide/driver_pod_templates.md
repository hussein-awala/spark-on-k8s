# Driver pod templates

The source version supports a Kubernetes Pod template for the driver through the
Python client, CLI and both Airflow operators. This feature does not require a Spark
operator or a custom resource. Install from source until a release includes it; see
[installation](installation.md).

Use a template for settings such as pod/container security contexts, affinity,
image pull secrets, sidecars, init containers and additional volumes. The client
merges the template into the same driver pod it normally submits.

## Submit a template

Save this as `driver.yaml`. The application image still comes from the submission
arguments, so the selected container does not need an image in the template.

```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    team: analytics
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
    fsGroup: 185
  containers:
    - name: driver
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: [ALL]
      volumeMounts:
        - name: scratch
          mountPath: /scratch
  volumes:
    - name: scratch
      emptyDir: {}
```

Select a user ID supported by your application image and ensure its working
directories are writable. A template configures the driver only; use an
[executor template](examples.md#executor-pod-template) for executor pod settings.

```python
from spark_on_k8s.client import SparkOnK8S

SparkOnK8S().submit_app(
    image="registry.example.com/team/spark-job:1.0",
    app_path="local:///opt/spark/work-dir/job.py",
    namespace="spark",
    service_account="spark",
    driver_pod_template="./driver.yaml",
    app_waiter="log",
)
```

```bash
spark-on-k8s app submit \
  --image registry.example.com/team/spark-job:1.0 \
  --path local:///opt/spark/work-dir/job.py \
  --namespace spark \
  --service-account spark \
  --driver-pod-template ./driver.yaml \
  --logs
```

`driver_pod_template` accepts YAML text, a local path string, a `pathlib.Path`, or a
Python mapping. The CLI accepts YAML text or a local path. Files are read on the
machine submitting the application; this option does not fetch remote URLs.

## Select the driver container

A template can contain additional containers. The client selects the container
named `driver`, or the sole container if there is only one. Otherwise, specify
`driver_container_name="spark-main"` or `--driver-container-name spark-main`.
An explicit name must exist. An ambiguous or missing selection is an error.

The selected container becomes the first container in the submitted pod, retaining
its name. Other containers keep their relative order and settings. Logs and kill
operations target this selected container; the client also sets
`kubectl.kubernetes.io/default-container` for interactive `kubectl` use.

Ordinary sidecars must exit for the pod to finish. A sidecar that keeps running
also keeps pod-based waiting and Airflow tasks running. Where supported by your
cluster, [Kubernetes native sidecars](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/) in `initContainers` with `restartPolicy: Always`
provide a lifecycle that ends with the application containers. Sidecar lifecycle
is controlled by Kubernetes, not by this client.

## Merge rules

The template is the starting point. Submission arguments and their resolved
configuration defaults supply the generated fields below. In particular, omitting
an argument does **not** make a template image, service account or CPU/memory
setting override the client's configured default. Input mappings are not modified.

| Field | Result |
| --- | --- |
| Pod name and namespace | Generated application pod name and submission namespace. |
| `spark-app-id`, `spark-app-name`, `spark-role`, `spark-ui-proxy` labels | Managed application identity and proxy settings; template values are discarded. |
| Other labels and annotations | Template entries are retained; explicit driver entries override matching keys. |
| Service account and restart policy | Submission service account and `Never`; the deprecated `serviceAccount` alias is removed. |
| Selected container image, image pull policy and arguments | Generated from submission options and Spark configuration. |
| Selected container command | Removed, so the application image's Spark entrypoint runs the generated driver arguments. |
| Selected container CPU and memory requests/limits | Generated from `driver_resources` or configured defaults. Other resources, such as ephemeral storage or GPU limits, are retained. |
| Environment variables | Merged by name; generated values win, including `SPARK_DRIVER_BIND_ADDRESS`. |
| Environment sources (`envFrom`) | Template sources first, then generated secret references; identical entries are not duplicated. |
| Volumes, init containers | Merged by name; an explicit matching entry replaces the entire template entry. |
| Driver volume mounts | Merged by mount path; an explicit matching entry replaces the entire template entry. |
| Container ports | Merged by name, or by port/protocol for unnamed entries; generated driver/UI ports win on matching keys. |
| Node selector | Merged by key, with explicit driver selectors winning. |
| Host aliases | Merged by IP, with explicit driver aliases winning. |
| Tolerations | Template entries followed by explicit driver entries, without exact duplicates. |
| Other pod/container fields | Preserved, including security contexts, affinity, image pull secrets and sidecar settings. |

Lists with duplicate merge keys are rejected rather than resolved by position.
Server-assigned metadata, owner references, `generateName`, and `status` are not
accepted. Templates cannot supply annotations in the reserved `spark-on-k8s.io/`
namespace. Keep that namespace available for project-managed ownership metadata;
use your own annotation prefix for application metadata.

Loading, container selection and merge validation finish before any Kubernetes
Secret, ConfigMap, Pod or Service is created. This is not a complete Kubernetes
schema validator: the API server still validates other fields and enforces cluster
admission policies. Template settings do not bypass those policies.

## Airflow

Both `SparkOnK8SOperator` and `SparkSqlOnK8SOperator` accept the same template and
container-selection arguments. These fields support Airflow templating and DAG
serialization on Airflow 2.11 and Airflow 3.

```python
from spark_on_k8s.airflow.operators import SparkOnK8SOperator

application = SparkOnK8SOperator(
    task_id="application",
    image="registry.example.com/team/spark-job:1.0",
    app_path="local:///opt/spark/work-dir/job.py",
    namespace="spark",
    service_account="spark",
    driver_pod_template={
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"labels": {"run-date": "{{ ds }}"}},
        "spec": {"containers": [{"name": "driver"}]},
    },
)
```

For a file path, Airflow renders the path string, then the SDK reads that file on
the worker. File contents are not separately rendered by Airflow. Pass inline YAML
or a mapping when the template contents themselves contain Jinja expressions.
See the [Airflow guide](airflow.md) for connection and deployment setup.
