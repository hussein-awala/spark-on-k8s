# Spark On Kubernetes

Spark on Kubernetes is a python package that makes it easy to submit and manage spark apps on Kubernetes.
It provides a Python client that can be used to submit apps in your API or scheduler of choice, and a CLI
that can be used to submit apps from the command line, instead of  using spark-submit.

It also provides an optional REST API with a web UI that can be used to list and manage apps, and access the
spark UI through the reverse proxy.

## Installation
To install the core python package (only the Python client and the helpers), run:
```bash
pip install spark-on-k8s
```
If you want to use the REST API and the web UI, you will also need to install the api package:
```bash
pip install spark-on-k8s[api]
```

You can also install the package from source with pip or poetry:
```bash
# With pip
pip install . # For the core package
pip install ".[api]" # For the API package

# With poetry
poetry install # For the core package
poetry install -E api # For the API package
```

## Usage

### Setup the Kubernetes namespace

When submitting a Spark application to Kubernetes, we only create the driver pod, which is responsible for creating and
managing the executors pods. To give the driver pod the permissions to create the executors pods, we can give it a
service account with the required permissions. To simplify this process, we provide a helper function that creates
a namespace if needed, and a service account with the required permissions:

With Python:
```python
from spark_on_k8s.utils.setup_namespace import SparkOnK8SNamespaceSetup

spark_on_k8s_setuper = SparkOnK8SNamespaceSetup()
spark_on_k8s_setuper.setup_namespace(namespace="<namespace name>")
```

With the CLI:
```bash
spark-on-k8s namespace setup -n <namespace name>
```

### Python Client
The Python client can be used to submit apps from your Python code, instead of using spark-submit:

```python
from spark_on_k8s.client import SparkOnK8S

client = SparkOnK8S()
client.submit_app(
    image="my-registry/my-image:latest",
    app_path="local:///opt/spark/work-dir/my-app.py",
    app_arguments=["arg1", "arg2"],
    app_name="my-app",
    namespace="spark-namespace",
    service_account="spark-service-account",
    app_waiter="log",
    image_pull_policy="Never",
    ui_reverse_proxy=True,
)
```

### CLI
The CLI can be used to submit apps from the command line, instead of using spark-submit, it can also be used to
manage apps submitted with the Python client (list, get, delete, logs, etc.):

Submit a app:
```bash
spark-on-k8s app submit \
  --image my-registry/my-image:latest \
  --path local:///opt/spark/work-dir/my-app.py \
  -n spark \
  --name my-app \
  --image-pull-policy Never \
  --ui-reverse-proxy \
  --log \
  param1 param2
```
Kill a app:
```bash
spark-on-k8s app kill -n spark-namespace --app-id my-app
```

List apps:
```bash
spark-on-k8s apps list -n spark-namespace
```

You can check the help for more information:
```bash
spark-on-k8s --help
spark-on-k8s app --help
spark-on-k8s apps --help
```

### REST API

The REST API implements some of the same functionality as the CLI but in async way, and also provides a web UI
that can be used to list the apps in the cluster and access the spark UI through a reverse proxy. The UI will be
improved in the future and more functionality will be added to both UI and API.

To run the API, you can use the CLI:
```bash
spark-on-k8s api start \
    --host "0.0.0.0" \
    --port 8080 \
    --workers 4 \
    --log-level error \
    --limit-concurrency 100
```

To list the apps, you can use the API:
```bash
curl -X 'GET' \
  'http://0.0.0.0:8080/apps/list_apps/spark-namespace' \
  -H 'accept: application/json'
```

To access the spark UI of the app APP_ID, in the namespace NAMESPACE, you can use the web UI link:
`http://0.0.0.0:8080/webserver/ui/NAMESPACE/APP_ID`, or getting all the application and then clicking
on the button `Open Spark UI` from the link `http://0.0.0.0:8080/webserver/apps?namespace=NAMESPACE`.

### API in production

To deploy the API in production, you can use the project helm chart, that setups all the required resources in the
cluster, including the API deployment, the service, the ingress and the RBAC resources. The API has a configuration
class that loads the configuration from environment variables, so you can use the helm chart `env` values to configure
the API and its Kubernetes client.

To install the helm chart, you can run:
```bash
helm install spark-on-k8s chart --values examples/helm/values.yaml
```

## Configuration

The Python client and the CLI can be configured with environment variables to avoid passing the same arguments every
time if you have a common configuration for all your apps. The environment variables are the same for both the client
and the CLI. Here is a list of the available environment variables:

| Environment Variable                       | Description                                           | Default      |
|--------------------------------------------|-------------------------------------------------------|--------------|
| SPARK_ON_K8S_DOCKER_IMAGE                  | The docker image to use for the spark pods            |              |
| SPARK_ON_K8S_APP_PATH                      | The path to the app file                              |              |
| SPARK_ON_K8S_NAMESPACE                     | The namespace to use                                  | default      |
| SPARK_ON_K8S_SERVICE_ACCOUNT               | The service account to use                            | spark        |
| SPARK_ON_K8S_SPARK_CONF                    | The spark configuration to use                        | {}           |
| SPARK_ON_K8S_CLASS_NAME                    | The class name to use                                 |              |
| SPARK_ON_K8S_APP_ARGUMENTS                 | The arguments to pass to the app                      | []           |
| SPARK_ON_K8S_APP_WAITER                    | The waiter to use to wait for the app to finish       | no_wait      |
| SPARK_ON_K8S_IMAGE_PULL_POLICY             | The image pull policy to use                          | IfNotPresent |
| SPARK_ON_K8S_UI_REVERSE_PROXY              | Whether to use a reverse proxy to access the spark UI | false        |
| SPARK_ON_K8S_DRIVER_CPU                    | The driver CPU                                        | 1            |
| SPARK_ON_K8S_DRIVER_MEMORY                 | The driver memory                                     | 1024         |
| SPARK_ON_K8S_DRIVER_MEMORY_OVERHEAD        | The driver memory overhead                            | 512          |
| SPARK_ON_K8S_EXECUTOR_CPU                  | The executor CPU                                      | 1            |
| SPARK_ON_K8S_EXECUTOR_MEMORY               | The executor memory                                   | 1024         |
| SPARK_ON_K8S_EXECUTOR_MEMORY_OVERHEAD      | The executor memory overhead                          | 512          |
| SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES        | The minimum number of executor instances              |              |
| SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES        | The maximum number of executor instances              |              |
| SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES    | The initial number of executor instances              |              |
| SPARK_ON_K8S_CONFIG_FILE                   | The path to the config file                           |              |
| SPARK_ON_K8S_CONTEXT                       | The context to use                                    |              |
| SPARK_ON_K8S_CLIENT_CONFIG                 | The sync Kubernetes client configuration to use       |              |
| SPARK_ON_K8S_ASYNC_CLIENT_CONFIG           | The async Kubernetes client configuration to use      |              |
| SPARK_ON_K8S_IN_CLUSTER                    | Whether to use the in cluster Kubernetes config       | false        |
| SPARK_ON_K8S_API_DEFAULT_NAMESPACE         | The default namespace to use for the API              | default      |
| SPARK_ON_K8S_API_HOST                      | The host to use for the API                           | 127.0.0.1    |
| SPARK_ON_K8S_API_PORT                      | The port to use for the API                           | 8000         |
| SPARK_ON_K8S_API_WORKERS                   | The number of workers to use for the API              | 4            |
| SPARK_ON_K8S_API_LOG_LEVEL                 | The log level to use for the API                      | info         |
| SPARK_ON_K8S_API_LIMIT_CONCURRENCY         | The limit concurrency to use for the API              | 1000         |
| SPARK_ON_K8S_SPARK_DRIVER_NODE_SELECTOR    | The node selector to use for the driver pod           | {}           |
| SPARK_ON_K8S_SPARK_EXECUTOR_NODE_SELECTOR  | The node selector to use for the executor pods        | {}           |
| SPARK_ON_K8S_SPARK_DRIVER_LABELS           | The labels to use for the driver pod                  | {}           |
| SPARK_ON_K8S_SPARK_EXECUTOR_LABELS         | The labels to use for the executor pods               | {}           |
| SPARK_ON_K8S_SPARK_DRIVER_ANNOTATIONS      | The annotations to use for the driver pod             | {}           |
| SPARK_ON_K8S_SPARK_EXECUTOR_ANNOTATIONS    | The annotations to use for the executor pods          | {}           |
| SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH    | The path to the executor pod template                 |              |


## Examples

Here are some examples of how to package and submit spark apps with this package. In the examples, the base image is
built with the spark image tool, as described in the
[spark documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html#docker-images).

### Python
First, build the docker image and push it to a registry accessible by your cluster,
or load it into your cluster's local registry if you're using minikube or kind:
```bash
docker build -t pyspark-job examples/python

# For minikube
minikube image load pyspark-job
# For kind
kind load docker-image pyspark-job
# For remote clusters, you will need to change the image name to match your registry,
# and then push it to that registry
docker push pyspark-job
```
Then, submit the job:
```bash
python examples/python/submit.py
```
Or via the bash script:
```bash
./examples/python/submit.sh
```

### Java
Same as above, but with the java example:
```bash
docker build -t java-spark-job examples/java

# For minikube
minikube image load java-spark-job
# For kind
kind load docker-image java-spark-job
# For remote clusters, you will need to change the image name to match your registry,
# and then push it to that registry
docker push java-spark-job
```
Then, submit the job:
```bash
python examples/java/submit.py
```

Or via the bash script:
```bash
./examples/java/submit.sh
```

## What's next

You can check the [TODO](https://github.com/hussein-awala/spark-on-k8s/blob/main/TODO.md) list for the things that we
will work on in the future. All contributions are welcome!