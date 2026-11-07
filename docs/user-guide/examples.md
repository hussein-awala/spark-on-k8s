These examples describe the current source checkout. Run build and submission
commands from the repository root, with namespace `spark` and service account
`spark` configured as described in [Getting Started](getting_started.md).
The example Dockerfiles default to the Apache Spark 3.5.9 image. To build them
with Spark 4, set the image and matching Java dependency versions as described in
[Spark compatibility](spark_compatibility.md).
For scheduled jobs, see the [Airflow guide](airflow.md).

### Python

In this example, we use a small PySpark application that takes a parameter `num_points` to calculate the value of Pi:
```python
--8<-- "examples/python/job.py"
```
and a Dockerfile to package the application with the spark image:
```dockerfile
--8<-- "examples/python/Dockerfile"
```

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
Then, you can submit the job using the python client:
```python
--8<-- "examples/python/submit.py"
```
or using the CLI:
```bash
--8<-- "examples/python/submit.sh"
```

### Java
This example is similar to the previous one, but it's implemented in java (with maven).

pom.xml:
```xml
--8<-- "examples/java/job/pom.xml"
```
the job class (`src/main/java/com/oss_tech/examples/TestJob.java`):
```java
--8<-- "examples/java/job/src/main/java/com/oss_tech/examples/TestJob.java"
```

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
Then, submit the job using the python client:
```python
--8<-- "examples/java/submit.py"
```
or using the CLI:
```bash
--8<-- "examples/java/submit.sh"
```

### Driver configuration

This package manages Spark applications in Kubernetes by directly creating the driver pod and submitting the job in client mode. This approach bypasses some of Spark's built-in driver configuration mechanisms, causing certain settings to be ignored (e.g., `spark.kubernetes.driver.podTemplateFile`, `spark.kubernetes.driver.label.[LabelName]`, ...).

To address this, the package provides internal implementations for some key driver configurations, ensuring greater control and flexibility when deploying Spark applications in Kubernetes.

#### Driver service account
Instead of providing `spark.kubernetes.authenticate.driver.serviceAccountName=<some-service-account>`, you can use:
```python
spark_client.submit_app(
    ...,
    service_account="<some-service-account>",
)
```

#### Driver resources
Instead of providing `spark.driver.memory`, `spark.driver.cores`, `spark.driver.memoryOverhead`, you can use:
```python
from spark_on_k8s.client import PodResources

spark_client.submit_app(
    ...,
    driver_resources=PodResources(cpu=1, memory=512, memory_overhead=128),
)
```

#### Driver environment variables
Use `secret_values` to create an application-owned Kubernetes Secret. Its keys
become environment variables in both the driver and executors; executor delivery
uses Spark's `spark.kubernetes.executor.secretKeyRef` configuration. To import
existing Secrets into the driver only, use `driver_env_vars_from_secrets`.
```python
spark_client.submit_app(
    ...,
    secret_values={"EnvironmentVariableName": "some-value"},
)
```

#### Driver volume mounts
Pass Kubernetes volume and mount objects to `volumes` and `driver_volume_mounts`.
The Secret `secret1`, ConfigMap `configmap1` and persistent volume claim `pvc` in
this example must already exist in the application's namespace. Replace the host
and NFS paths with resources available in your cluster:

```python
from kubernetes import client as k8s

spark_client.submit_app(
    ...,
    volumes=[
        k8s.V1Volume(
            name="volume1",
            host_path=k8s.V1HostPathVolumeSource(path="/mnt/volume1"),
        ),
        k8s.V1Volume(
            name="volume2",
            empty_dir=k8s.V1EmptyDirVolumeSource(medium="Memory", size_limit="1Gi"),
        ),
        k8s.V1Volume(
            name="volume3",
            secret=k8s.V1SecretVolumeSource(
                secret_name="secret1", items=[k8s.V1KeyToPath(key="key1", path="path1")]
            ),
        ),
        k8s.V1Volume(
            name="volume4",
            config_map=k8s.V1ConfigMapVolumeSource(
                name="configmap1", items=[k8s.V1KeyToPath(key="key1", path="path1")]
            ),
        ),
        k8s.V1Volume(
            name="volume5",
            projected=k8s.V1ProjectedVolumeSource(
                sources=[
                    k8s.V1VolumeProjection(
                        secret=k8s.V1SecretProjection(
                            name="secret1",
                            items=[k8s.V1KeyToPath(key="key1", path="secret/key1")],
                        )
                    ),
                    k8s.V1VolumeProjection(
                        config_map=k8s.V1ConfigMapProjection(
                            name="configmap1",
                            items=[k8s.V1KeyToPath(key="key1", path="config/key1")],
                        )
                    ),
                ]
            ),
        ),
        k8s.V1Volume(
            name="volume6",
            nfs=k8s.V1NFSVolumeSource(server="nfs-server", path="/mnt/volume6", read_only=True),
        ),
        k8s.V1Volume(
            name="volume7",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="pvc", read_only=True
            ),
        ),
    ],
    driver_volume_mounts=[
        k8s.V1VolumeMount(name="volume1", mount_path="/mnt/volume1"),
        k8s.V1VolumeMount(name="volume2", mount_path="/mnt/volume2", sub_path="sub-path"),
        k8s.V1VolumeMount(name="volume3", mount_path="/mnt/volume3"),
        k8s.V1VolumeMount(name="volume4", mount_path="/mnt/volume4"),
        k8s.V1VolumeMount(name="volume5", mount_path="/mnt/volume5"),
        k8s.V1VolumeMount(name="volume6", mount_path="/mnt/volume6"),
        k8s.V1VolumeMount(name="volume7", mount_path="/mnt/volume7", read_only=True),
    ],
)
```

#### Driver node selector
Instead of providing `spark.kubernetes.driver.node.selector.[labelKey]=<labelValue>`, you can use:
```python
spark_client.submit_app(
    ...,
    driver_node_selector={"labelKey": "labelValue"},
)
```

#### Driver tolerations
Instead of configuring tolerations for the driver pod via a pod template file, you can use:
```python
from kubernetes import client as k8s

spark_client.submit_app(
    ...,
    driver_tolerations=[
        k8s.V1Toleration(
            key="key1",
            operator="Equal",
            value="value1",
            effect="NoSchedule",
        ),
        k8s.V1Toleration(
            key="key2",
            operator="Exists",
            effect="NoExecute",
        ),
    ],
)
```

#### Driver init containers
Instead of adding init containers to the driver pod via a pod template file, you can use:
```python
from kubernetes import client as k8s

spark_client.submit_app(
    ...,
    driver_init_containers=[
        k8s.V1Container(
            name="init-container1",
            image="init-container1-image",
            command=["init-command1"],
            args=["init-arg1"],
        ),
        k8s.V1Container(
            name="init-container2",
            image="init-container2-image",
            command=["init-command2"],
            args=["init-arg2"],
        ),
    ],
)
```
#### Driver host aliases
Instead of providing host aliases (entries added to pod's /etc/hosts file) for the driver pod via a pod template file, you can use:
```python
from kubernetes import client as k8s

spark_client.submit_app(
    ...,
    driver_host_aliases=[
        k8s.V1HostAlias(
            hostnames=["foo.local", "bar.local"],
            ip="127.0.0.1",
        )
    ],
)
```

### Executor configuration

#### Executor pod template

The package provides an enhanced way to configure executor pod templates compared to Spark's built-in `spark.kubernetes.executor.podTemplateFile` configuration.

Instead of manually creating ConfigMaps and managing volume mounts, you can use the `executor_pod_template` parameter that accepts either:

- Template content as a string (YAML)
- Path to a local template file

The package automatically:

1. Creates a ConfigMap with the template content
2. Mounts it to the driver pod (so the driver can validate the template)
3. Configures Spark to use the template for executor pods
4. Sets the driver pod as the ConfigMap owner, so Kubernetes cleans up the ConfigMap when that pod is deleted

These submissions use the locally preloaded `pyspark-job` image and the `spark`
namespace/service account prepared earlier. For a registry image, change the image
name and use `IfNotPresent` as described in the Python example above.

**Using template content as string:**
```python
executor_template_yaml = """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: executor
    resources:
      requests:
        memory: "4Gi"
        cpu: "2"
      limits:
        memory: "4Gi"
        cpu: "2"
    env:
    - name: CUSTOM_ENV_VAR
      value: "custom-value"
  nodeSelector:
    node-type: "compute-optimized"
  tolerations:
  - key: "spark-executor"
    operator: "Equal"
    value: "true"
    effect: "NoSchedule"
"""

spark_client.submit_app(
    image="pyspark-job",
    app_path="local:///opt/spark/work-dir/job.py",
    namespace="spark",
    service_account="spark",
    image_pull_policy="Never",
    executor_pod_template=executor_template_yaml,
)
```

**Using local template file:**
```python
# Save your template to a file, e.g., executor-template.yaml
spark_client.submit_app(
    image="pyspark-job",
    app_path="local:///opt/spark/work-dir/job.py",
    namespace="spark",
    service_account="spark",
    image_pull_policy="Never",
    executor_pod_template="./executor-template.yaml",
)
```

**Via CLI:**
```bash
# Using template file
spark-on-k8s app submit --image pyspark-job --path local:///opt/spark/work-dir/job.py \
    --namespace spark --service-account spark --image-pull-policy Never \
    --executor-template ./executor-template.yaml

# You can also set it via environment variable
export SPARK_ON_K8S_EXECUTOR_TEMPLATE="./executor-template.yaml"
spark-on-k8s app submit --image pyspark-job --path local:///opt/spark/work-dir/job.py \
    --namespace spark --service-account spark --image-pull-policy Never
```

**Note:** `executor_pod_template` and `executor_pod_template_path` are mutually exclusive. Use `executor_pod_template_path` (or `--executor-pod-template-path` in the CLI) for a template path already available to the Spark driver. Do not configure both options, including through environment defaults.
