# Getting started

This guide describes the current source. Follow the [installation guide](installation.md)
to choose between a published release and a source checkout. Example file paths
below are relative to the repository root.

## Before submitting

Install the core package using the [installation guide](installation.md), and use
that same Python environment for these commands. Choose either the Python or CLI
example below; they submit the same kind of application.

Have these inputs ready:

- Kubernetes credentials for the intended cluster and namespace. By default the
  client loads your kubeconfig's current context. Use `SPARK_ON_K8S_CONFIG_FILE`
  and `SPARK_ON_K8S_CONTEXT` to select them explicitly, or `SPARK_ON_K8S_IN_CLUSTER=true`
  when running with an in-cluster service account.
- A namespace and driver service account with the permissions described below.
  The submitting client's credentials and the driver's service account are separate.
- A Spark application image accessible to every node that may run a driver or
  executor, with the application file and its dependencies included. A `local:///`
  application path names a file **inside that image**, not on your laptop.
- Data and storage access for both driver and executors.

The submitter does not need Spark or Java installed locally. The application image
must have compatible Spark, Java and application runtimes; use the
[Spark compatibility guide](spark_compatibility.md) when choosing it. If you do not
yet have an image, follow the [Python or Java examples](examples.md) first.

## Set up the Kubernetes namespace

The client creates the Spark driver pod. The driver uses its Kubernetes service
account to create and manage executor pods. The following examples use namespace
`spark` and service account `spark`; the submitting client also needs permission
to create the application resources in that namespace.

If your administrator has already provided that namespace and service account,
skip the setup helper and use their names in the submission examples.

The optional setup helper creates the namespace if needed, a service account named `spark`,
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

## Python client

Replace the image and application path with an image accessible to your cluster
and an application file included in that image. The values below are placeholders:

```python
from spark_on_k8s.client import SparkOnK8S

client = SparkOnK8S()
driver_pod_name = client.submit_app(
    image="registry.example.com/team/my-app:1.0",
    app_path="local:///opt/spark/work-dir/my-app.py",
    app_arguments=["arg1", "arg2"],
    app_name="my-app",
    namespace="spark",
    service_account="spark",
    app_waiter="log",
    image_pull_policy="IfNotPresent",
)
```

`submit_app` returns the driver pod name. The application ID is generated from the
application name with a timestamp suffix and is available in the application list.
The driver pod is usually named `<application ID>-driver`; an ID longer than 56
characters instead keeps a readable prefix followed by a hash of the full ID, so
use the returned name or run receipt rather than deriving it.
For a private registry, configure image-pull credentials for the pods in your cluster.

## CLI

The CLI can submit applications and manage applications created by either the CLI
or the Python client. Submit an application and stream its logs:

```bash
spark-on-k8s app submit \
  --image registry.example.com/team/my-app:1.0 \
  --path local:///opt/spark/work-dir/my-app.py \
  --namespace spark \
  --service-account spark \
  --name my-app \
  --image-pull-policy IfNotPresent \
  --logs --timeout 300 \
  param1 param2
```

The CLI example waits up to 300 seconds after submission and reports the final
status. A timeout stops observation; it does not cancel the application. Confirm
`Succeeded` and check the application's expected output before treating the run
as successful. For JSON output, saved receipts and safe cancellation, follow
[Automation and run identities](automation.md).

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

## REST API

Install the `api` extra before starting the REST API and web UI. The API serves
only its current namespace unless `SPARK_ON_K8S_API_NAMESPACES` names others; outside
a cluster, that is the namespace of the kubeconfig context. For local access to the
`spark` namespace:

```bash
export SPARK_ON_K8S_API_NAMESPACES=spark
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

The console provides a searchable application list, status filters, namespace selection,
application details, and confirmed stop/delete actions. It refreshes in the background
without resetting your filters. The last successful fetch age and polling state help
you recognize stale data. Counts and filters describe the applications loaded so far;
use **Load more** when additional pages are available. Completed applications from the
configured history server appear as archive entries without live-driver actions.

The driver-log view supports search, a configurable tail and frozen reading while
new lines arrive. Its live buffer retains the most recent 10,000 lines. Download
exports that buffered text; it is not a complete historical log archive. The excerpt
preview lets you review and edit bounded text before copying or sharing it.
Connection failures appear separately from log text, and a history-server outage
leaves live applications available. JavaScript and CSS are packaged with the Python
distribution; no CDN is needed.

Open `http://127.0.0.1:8080/webserver/apps?namespace=spark` and use **Open Spark UI**
for an application submitted with `ui_reverse_proxy=True` in Python or
`--ui-reverse-proxy` in the CLI. Its direct URL is
`http://127.0.0.1:8080/webserver/ui/spark/APP_ID`; replace `APP_ID` with the actual
application ID. The API process must be able to reach the driver's service.
When placing the console behind a gateway, configure the
[browser and proxy boundaries](../security-boundaries.md), including the public
WebSocket origin when needed.

Use `--host 0.0.0.0` to listen on all network interfaces when deploying behind an
appropriate service or proxy. Client URLs should use the server's reachable host
or address, not the wildcard bind address `0.0.0.0`.

When a gateway serves the API below a path such as `/platform/spark` and removes that
prefix before forwarding, pass `--root-path /platform/spark` or set
`SPARK_ON_K8S_API_ROOT_PATH=/platform/spark`. See the
[mounted path guidance](../security-boundaries.md#mounted-spark-ui-paths).

## Deploy the API with Helm

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
that local chart. Set `API_IMAGE_TAG` to a compatible, tested image from the
current source; the old `0.12.1` API does not implement the readiness endpoint:

```bash
helm template spark-on-k8s-release ./chart --values examples/helm/values.yaml \
  --set-string image.tag="${API_IMAGE_TAG:?Set a qualified API image tag}"
helm install spark-on-k8s-release ./chart --values examples/helm/values.yaml \
  --set-string image.tag="$API_IMAGE_TAG"
```

Choose one chart source for a release. The commands above use example values from
this checkout; adapt them to your cluster and selected chart version.
The source chart defaults to `readOnly: true`, `namespaces: ""` (the release
namespace), a ClusterIP Service on port `8000`, and disabled ingress. Set `namespaces`
to a comma-separated string such as `"spark"` or `"spark,analytics"` to serve those
namespaces, or to `"*"` to serve every namespace through a cluster-wide binding. The
chart-created Kubernetes permissions follow the same value, and the API refuses
requests, Spark UI proxy connections and log streams for other namespaces.
Enable `readOnly: false` only if the API should be able to kill or
delete applications. The separate web UI button settings control visibility,
not Kubernetes authorization. The API has no built-in user authentication; its
namespace scope limits what it can reach, not who can use it. Expose the API through
access-controlled networking or an authenticating proxy.

The source chart supplies in-cluster Kubernetes configuration and the API bind
address and port. It requires an explicit API image tag or digest; a published
chart defaults to its `appVersion` tag. Using a local chart does not build or
install the Python source into an image. Set `image.repository`
and `image.tag` to the API image version you intend to deploy, or use
`image.digest` to select verified image contents; the digest takes precedence over
the tag. This is independent of the Spark application image passed to `submit_app`.
Follow [Chart deployment boundaries](chart-deployments.md) for scoped access,
image selection and existing-release upgrades.

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
| `SPARK_ON_K8S_API_NAMESPACES` | Namespaces the API serves: empty for its current namespace (the pod's, or the kubeconfig context's), `*` for every namespace, or a comma-separated list | empty |
| `SPARK_ON_K8S_API_DEFAULT_NAMESPACE` | Namespace the API/web UI uses when a request names none; must be in the scope | the first namespace in scope; with `*`, the current namespace |
| `SPARK_ON_K8S_API_HOST` | API bind address | `127.0.0.1` |
| `SPARK_ON_K8S_API_PORT` | API listen port (integer) | `8000` |
| `SPARK_ON_K8S_API_WORKERS` | API worker count (integer) | `4` |
| `SPARK_ON_K8S_API_LOG_LEVEL` | API server log level | `info` |
| `SPARK_ON_K8S_API_LIMIT_CONCURRENCY` | Maximum concurrent API connections/tasks (integer) | `1000` |
| `SPARK_ON_K8S_API_ROOT_PATH` | Public path prefix, such as `/platform/spark`, that a gateway removes before forwarding to the API; `--root-path` default | empty string |
| `SPARK_ON_K8S_API_SPARK_HISTORY_HOST` | Spark History Server host and port, optionally with an HTTP(S) scheme | unset |
| `SPARK_ON_K8S_API_WEBSERVER_APPS_KILL_BUTTON` | Show the kill button in the applications page | `true` |
| `SPARK_ON_K8S_API_WEBSERVER_APPS_DELETE_BUTTON` | Show the delete button in the applications page | `true` |
| `SPARK_ON_K8S_API_WEBSERVER_APPS_REFRESH_INTERVAL` | Applications-page refresh interval in whole seconds; `0` or a negative value disables it | `10` |

Boolean settings in this table accept `true` or `false` in any letter case; any other
value is treated as `false`.

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
