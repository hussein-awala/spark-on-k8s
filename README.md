[![PyPI - Package Version](https://img.shields.io/pypi/v/spark-on-k8s)](https://pypi.org/project/spark-on-k8s)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/spark-on-k8s)](https://pypi.org/project/spark-on-k8s)
[![PyPI - Python Version](https://img.shields.io/pypi/l/spark-on-k8s)](https://github.com/hussein-awala/spark-on-k8s/blob/main/LICENSE)
[![CI workflow](https://github.com/hussein-awala/spark-on-k8s/actions/workflows/ci.yml/badge.svg)](https://github.com/hussein-awala/spark-on-k8s/actions/workflows/ci.yml)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8883/badge)](https://www.bestpractices.dev/projects/8883)
[![codecov](https://codecov.io/gh/hussein-awala/spark-on-k8s/branch/main/graph/badge.svg?token=Z9YN7L7VMH)](https://codecov.io/gh/hussein-awala/spark-on-k8s)

# Spark On Kubernetes

Spark on Kubernetes is a python package that makes it easy to submit and manage spark apps on Kubernetes.
It provides a Python client that can be used to submit apps in your API or scheduler of choice, and a CLI
that can be used to submit apps from the command line, instead of  using spark-submit.

It also provides an optional REST API with a web UI that can be used to list and manage apps, and access the
spark UI through the reverse proxy.

## Installation

### Published releases

The following commands install a published package from PyPI. Choose the
extra for the functionality you need, inside a virtual environment:

```bash
python -m pip install spark-on-k8s # Python client, CLI and helpers
python -m pip install "spark-on-k8s[api]" # Also install the REST API and web UI
python -m pip install "spark-on-k8s[airflow]" # Also install the Airflow integration
```

Published releases can have different Python requirements and features from the
unreleased `main` branch. Check the selected release's metadata on
[PyPI](https://pypi.org/project/spark-on-k8s/) and its version of the documentation.
The source requirements and examples below describe `main`; installing from PyPI
does not install unreleased changes.

### Current source

The current source requires Python 3.11 or newer. The core client and API are
tested on Python 3.11–3.15. The Airflow integration is tested with Airflow 3.4 on
Python 3.11–3.14 and Airflow 2.11.2 on Python 3.11–3.12. Airflow 2 is end-of-life
and retains dependency versions with known security advisories; use Airflow 3 for
new deployments. The `airflow` extra is not supported on Python 3.15.

Clone the repository to use the source instructions and example files in this guide:

```bash
git clone https://github.com/hussein-awala/spark-on-k8s.git
cd spark-on-k8s
```

With [uv](https://docs.astral.sh/uv/), choose one environment profile. These commands
use the committed lockfile and create a project-local `.venv`:

```bash
uv sync --locked --no-default-groups # Core package
uv sync --locked --no-default-groups --extra api # API and web UI
uv sync --python 3.12 --locked --no-default-groups --extra airflow # Airflow 3 integration
```

Run commands from the selected environment with `uv run --no-sync spark-on-k8s`,
or activate it with `source .venv/bin/activate` before using the commands below.
To install both integrations, select `--all-extras` on a supported Airflow Python
version. A later `uv sync` selects a new profile and can remove unselected extras.

Alternatively, install from the checkout with pip in an activated virtual environment:

```bash
python -m pip install . # Core package
python -m pip install ".[api]" # API and web UI
python -m pip install ".[airflow]" # Airflow integration
```

See [CONTRIBUTING.md](https://github.com/hussein-awala/spark-on-k8s/blob/main/CONTRIBUTING.md)
for development dependencies and the separate Airflow 2.11 compatibility environment.
See the [Airflow guide](docs/user-guide/airflow.md) for provider installation, connections and DAG examples.

## Usage

### Set up the Kubernetes namespace

The client creates the Spark driver pod. The driver uses its Kubernetes service
account to create and manage executor pods. The following examples use namespace
`spark` and service account `spark`; the submitting client also needs permission
to create the application resources in that namespace.

The setup helper creates the namespace if needed, a service account named `spark`,
and a **ClusterRoleBinding to the built-in `edit` ClusterRole**. This grants that
service account edit access across all namespaces. It is broader than a
namespace-scoped Spark driver role. On a shared cluster, ask the cluster
administrator to provision a suitably scoped service account instead.

Running the helper requires credentials that can list namespaces, service accounts
and ClusterRoleBindings, create missing resources, and bind the `edit` role.
Use it only when that cluster-wide grant is intended. See the
[Kubernetes RBAC documentation](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#rolebinding-and-clusterrolebinding)
for the difference between RoleBindings and ClusterRoleBindings.

With Python:

```python
from spark_on_k8s.utils.setup_namespace import SparkOnK8SNamespaceSetup

namespace_setup = SparkOnK8SNamespaceSetup()
namespace_setup.setup_namespace(namespace="spark")
```

With the CLI:

```bash
spark-on-k8s namespace setup -n spark
```

### Python client

Replace the image and application path with an image accessible to your cluster
and an application file included in that image:

```python
from spark_on_k8s.client import SparkOnK8S

client = SparkOnK8S()
driver_pod_name = client.submit_app(
    image="my-registry/my-image:latest",
    app_path="local:///opt/spark/work-dir/my-app.py",
    app_arguments=["arg1", "arg2"],
    app_name="my-app",
    namespace="spark",
    service_account="spark",
    app_waiter="log",
    image_pull_policy="IfNotPresent",
    ui_reverse_proxy=True,
)
```

`submit_app` returns the driver pod name. The application ID is generated from the
application name with a timestamp suffix and is available in the application list.
For a private registry, configure image-pull credentials for the pods in your cluster.

### CLI

The CLI can submit applications and manage applications created by either the CLI
or the Python client. Submit an application and stream its logs:

```bash
spark-on-k8s app submit \
  --image my-registry/my-image:latest \
  --path local:///opt/spark/work-dir/my-app.py \
  --namespace spark \
  --service-account spark \
  --name my-app \
  --image-pull-policy IfNotPresent \
  --ui-reverse-proxy \
  --logs \
  param1 param2
```

List applications to find their generated IDs:

```bash
spark-on-k8s apps list -n spark
```

To kill a running application, copy its actual ID from that output. The following
ID is illustrative; `my-app` alone is the application name, not the generated ID:

```bash
APP_ID="my-app-20261107120000" # Replace with the ID from the application list
spark-on-k8s app kill -n spark --app-id "$APP_ID"
```

Check the help for further commands and options:

```bash
spark-on-k8s --help
spark-on-k8s app --help
spark-on-k8s apps --help
```

Use `--output json` for versioned submission receipts and machine-readable list/status results.
Waiting commands report failed workloads with nonzero exit codes; `--timeout` bounds observation.
See the [automation guide](docs/user-guide/automation.md) for exit codes and Python run handles.

### REST API

Install the `api` extra before starting the REST API and web UI. For local access:

```bash
spark-on-k8s api start \
    --host "127.0.0.1" \
    --port 8080 \
    --workers 4 \
    --log-level error \
    --limit-concurrency 100
```

List applications in the `spark` namespace:

```bash
curl 'http://127.0.0.1:8080/apps/list_apps/spark' \
  -H 'accept: application/json'
```

Open `http://127.0.0.1:8080/webserver/apps?namespace=spark` and use **Open Spark UI**
for an application submitted with UI reverse proxy enabled. Its direct URL is
`http://127.0.0.1:8080/webserver/ui/spark/APP_ID`; replace `APP_ID` with the actual
application ID. The API process must be able to reach the driver's service.

Use `--host 0.0.0.0` to listen on all network interfaces when deploying behind an
appropriate service or proxy. Client URLs should use the server's reachable host
or address, not the wildcard bind address `0.0.0.0`.

### Deploy the API with Helm

The chart deploys the API, its Service and RBAC resources. Ingress and the optional
Spark History Server are controlled by chart values. The chart's `env` list sets
environment variables for the API and its Kubernetes client.

The **published chart** is released independently from this source checkout.
Inspect the selected published chart's values before deploying it; unreleased
chart fixes and defaults described below may not be present:

```bash
helm repo add spark-on-k8s https://hussein.awala.fr/spark-on-k8s-chart
helm repo update
helm show values spark-on-k8s/spark-on-k8s
helm install spark-on-k8s-release spark-on-k8s/spark-on-k8s \
  --values examples/helm/values.yaml
```

To use the **chart from this checkout**, render it for review first, then install
that local chart:

```bash
helm template spark-on-k8s-release ./chart --values examples/helm/values.yaml
helm install spark-on-k8s-release ./chart --values examples/helm/values.yaml
```

Choose one chart source for a release. The commands above use example values from
this checkout; adapt them to your cluster and selected chart version.
The source chart defaults to `readOnly: true`, `namespaces: "*"`, a ClusterIP Service
on port `8000`, and disabled ingress. Set `namespaces` to a comma-separated string
such as `"spark"` or `"spark,analytics"` to scope the chart-created Kubernetes
permissions to those namespaces.
Enable `readOnly: false` only if the API should be able to kill or
delete applications. The separate web UI button settings control visibility,
not Kubernetes authorization. The API has no built-in user authentication, and
`namespaces` does not restrict the HTTP reverse proxy. Expose the API through
access-controlled networking or an authenticating proxy.

The source chart supplies in-cluster Kubernetes configuration and the API bind
address and port. Its default API image tag is `latest`; using a local chart does
not build or install the Python source into that image. Set `image.repository`
and `image.tag` to the API image version you intend to deploy, independently of
the Spark application images passed to `submit_app`.

## Configuration

Set environment variables before starting the CLI/API process or importing the
Python client: configuration values are read at module import time. The table
below lists defaults in the current source, before chart or caller overrides.
`unset` means no environment default (`None`); `{}`, `[]` and an empty string are
distinct empty defaults. Use lowercase `true` or `false` for boolean settings.

Most client defaults also apply to the CLI. The six node-selector, label and
annotation variables marked **CLI** are CLI defaults; Python callers should pass
the corresponding `driver_*` or `executor_*` dictionaries to `submit_app`.
If all three executor-count variables are unset, applications use two executors
with dynamic allocation disabled. The two executor-template settings are mutually
exclusive, including when supplied through environment variables.

| Environment variable | Description | Default |
| --- | --- | --- |
| `SPARK_ON_K8S_DOCKER_IMAGE` | Spark driver and executor image; required unless passed explicitly | unset |
| `SPARK_ON_K8S_APP_PATH` | Application file URI; required unless passed explicitly | unset |
| `SPARK_ON_K8S_NAMESPACE` | Application namespace | `default` |
| `SPARK_ON_K8S_SERVICE_ACCOUNT` | Driver service account | `spark` |
| `SPARK_ON_K8S_APP_NAME` | Application name; generated when unset | unset |
| `SPARK_ON_K8S_SPARK_CONF` | Spark properties as a JSON object | `{}` |
| `SPARK_ON_K8S_CLASS_NAME` | Main class for a JVM application | unset |
| `SPARK_ON_K8S_PACKAGES` | Maven coordinates separated by commas | empty string |
| `SPARK_ON_K8S_APP_ARGUMENTS` | Application arguments as a JSON array of strings | `[]` |
| `SPARK_ON_K8S_APP_WAITER` | Wait mode: `no_wait`, `wait` or `log` | `no_wait` |
| `SPARK_ON_K8S_IMAGE_PULL_POLICY` | Pod image pull policy: `Always`, `IfNotPresent` or `Never` | `IfNotPresent` |
| `SPARK_ON_K8S_UI_REVERSE_PROXY` | Configure the application for Spark UI reverse proxy access | `false` |
| `SPARK_ON_K8S_DRIVER_CPU` | Driver CPU cores (integer) | `1` |
| `SPARK_ON_K8S_DRIVER_MEMORY` | Driver memory in MiB (integer) | `1024` |
| `SPARK_ON_K8S_DRIVER_MEMORY_OVERHEAD` | Driver memory overhead in MiB (integer) | `512` |
| `SPARK_ON_K8S_EXECUTOR_CPU` | Executor CPU cores (integer) | `1` |
| `SPARK_ON_K8S_EXECUTOR_MEMORY` | Executor memory in MiB (integer) | `1024` |
| `SPARK_ON_K8S_EXECUTOR_MEMORY_OVERHEAD` | Executor memory overhead in MiB (integer) | `512` |
| `SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES` | Minimum executor count; enables dynamic allocation when set | unset |
| `SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES` | Maximum executor count; enables dynamic allocation when set | unset |
| `SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES` | Initial executor count; fixed count when minimum and maximum are unset | unset |
| `SPARK_ON_K8S_EXECUTOR_ALLOCATION_RATIO` | Dynamic allocation ratio (number) | `1.0` |
| `SPARK_ON_K8S_SCHEDULER_BACKLOG_TIMEOUT` | Backlog duration before requesting executors | `1s` |
| `SPARK_ON_K8S_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT` | Backlog duration for subsequent executor requests | value of `SPARK_ON_K8S_SCHEDULER_BACKLOG_TIMEOUT` |
| `SPARK_ON_K8S_SECRET_ENV_VAR` | Driver and executor environment values as a JSON object; stored in an application-owned Secret | `{}` |
| `SPARK_ON_K8S_DRIVER_ENV_VARS_FROM_SECRET` | Existing Secret names separated by commas; import their keys as driver environment variables | empty list |
| `SPARK_ON_K8S_STARTUP_TIMEOUT` | Driver startup timeout in seconds when waiting or streaming logs (integer); `0` disables it | `0` |
| `SPARK_ON_K8S_CONFIG_FILE` | Kubeconfig file path | unset |
| `SPARK_ON_K8S_CONTEXT` | Kubeconfig context | unset |
| `SPARK_ON_K8S_CLIENT_CONFIG` | JSON object of synchronous `kubernetes.client.Configuration` constructor arguments | unset |
| `SPARK_ON_K8S_ASYNC_CLIENT_CONFIG` | JSON object of `kubernetes_asyncio.client.Configuration` constructor arguments; requires the `api` or `airflow` extra | unset |
| `SPARK_ON_K8S_IN_CLUSTER` | Load Kubernetes credentials from the running pod | `false` |
| `SPARK_ON_K8S_SPARK_DRIVER_NODE_SELECTOR` | CLI driver node selector as a JSON object | `{}` |
| `SPARK_ON_K8S_SPARK_EXECUTOR_NODE_SELECTOR` | CLI executor node selector as a JSON object | `{}` |
| `SPARK_ON_K8S_SPARK_DRIVER_LABELS` | CLI driver labels as a JSON object | `{}` |
| `SPARK_ON_K8S_SPARK_EXECUTOR_LABELS` | CLI executor labels as a JSON object | `{}` |
| `SPARK_ON_K8S_SPARK_DRIVER_ANNOTATIONS` | CLI driver annotations as a JSON object | `{}` |
| `SPARK_ON_K8S_SPARK_EXECUTOR_ANNOTATIONS` | CLI executor annotations as a JSON object | `{}` |
| `SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH` | Executor template path already available to the driver | unset |
| `SPARK_ON_K8S_EXECUTOR_TEMPLATE` | Executor template YAML or local file path; creates and mounts a ConfigMap | unset |
| `SPARK_ON_K8S_API_DEFAULT_NAMESPACE` | Default namespace displayed by the API/web UI | `default` |
| `SPARK_ON_K8S_API_HOST` | API bind address | `127.0.0.1` |
| `SPARK_ON_K8S_API_PORT` | API listen port (integer) | `8000` |
| `SPARK_ON_K8S_API_WORKERS` | API worker count (integer) | `4` |
| `SPARK_ON_K8S_API_LOG_LEVEL` | API server log level | `info` |
| `SPARK_ON_K8S_API_LIMIT_CONCURRENCY` | Maximum concurrent API connections/tasks (integer) | `1000` |
| `SPARK_ON_K8S_API_SPARK_HISTORY_HOST` | Spark History Server host and port, optionally with an HTTP(S) scheme | unset |
| `SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON` | Show the kill button in the applications page | `true` |
| `SPARK_ON_K8S_API_WEBSERVER_APPS_DELETE_BUTTON` | Show the delete button in the applications page | `true` |
| `SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL` | Applications-page refresh interval in whole seconds; `0` or a negative value disables it | `10` |

JSON settings use JSON syntax, including double-quoted keys and strings. Packages
and existing Secret names use comma-separated strings instead of JSON arrays:

```bash
export SPARK_ON_K8S_SPARK_CONF='{"spark.sql.shuffle.partitions": "8"}'
export SPARK_ON_K8S_APP_ARGUMENTS='["--input", "s3a://example/input"]'
export SPARK_ON_K8S_DRIVER_ENV_VARS_FROM_SECRET='spark-storage,spark-catalog'
export SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL='10'
```

The named Secrets must exist in the application's namespace. Refresh values such
as `10s` are invalid because the setting is parsed as an integer.

## Examples

Run the following commands from a source checkout. The examples use namespace
`spark` and service account `spark`, which must already be configured as described
above. See the [examples guide](docs/user-guide/examples.md) for the application
code, driver configuration and executor templates, and the
[Airflow guide](docs/user-guide/airflow.md) for DAG examples.

### Python

Build the image from the repository root:

```bash
docker build -t pyspark-job examples/python
```

For a local cluster, load the image into **one** of the following cluster runtimes:

```bash
# Minikube
minikube image load pyspark-job
# Or kind
kind load docker-image pyspark-job
```

The supplied submission scripts use `image_pull_policy="Never"` (CLI:
`--image-pull-policy Never`) for these preloaded images. For a remote cluster,
replace the example registry below, push the tagged image, and update the script's
image to that same name and its pull policy to `IfNotPresent`:

```bash
docker tag pyspark-job registry.example.com/team/pyspark-job:example
docker push registry.example.com/team/pyspark-job:example
```

Submit with the Python client or the CLI script:

```bash
python examples/python/submit.py
# Or
bash examples/python/submit.sh
```

### Java

Build the image from the repository root:

```bash
docker build -t java-spark-job examples/java
```

For a local cluster, load the image into **one** of the following cluster runtimes:

```bash
# Minikube
minikube image load java-spark-job
# Or kind
kind load docker-image java-spark-job
```

The supplied submission scripts use `image_pull_policy="Never"` (CLI:
`--image-pull-policy Never`) for these preloaded images. For a remote cluster,
replace the example registry below, push the tagged image, and update the script's
image to that same name and its pull policy to `IfNotPresent`:

```bash
docker tag java-spark-job registry.example.com/team/java-spark-job:example
docker push registry.example.com/team/java-spark-job:example
```

Submit with the Python client or the CLI script:

```bash
python examples/java/submit.py
# Or
bash examples/java/submit.sh
```

## What's next

See the [TODO list](https://github.com/hussein-awala/spark-on-k8s/blob/main/TODO.md)
for future work. Contributions are welcome!
