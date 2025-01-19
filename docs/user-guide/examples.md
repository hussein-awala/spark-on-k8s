Here are some examples of how to package and submit spark apps with this package. In the examples, the base image is
built with the spark image tool, as described in the
[spark documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html#docker-images).

### Python

In this example, we use a small PySpark application that takes a parameter `num_points` to calculate the value of Pi:
```python
--8<-- "examples/python/job.py"
```
and a Dockerfile to package the application with the spark image:
```dockerfile
--8<-- "examples/python/Dockerfile"
```

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

Similar to the PySpark application, you need to build the docker image and push it to a registry accessible by
your cluster, or load it into your cluster's local registry if you're using minikube or kind:
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
Instead of providing `spark.kubernetes.driverEnv.[EnvironmentVariableName]=<some-value>`, the package provides a safer way by creating an ephemeral secret and mounting it as an environment variable in the driver pod:
```python
spark_client.submit_app(
    ...,
    secret_values={"EnvironmentVariableName": "some-value"},
)
```

#### Driver volume mounts
Instead of providing `spark.kubernetes.driver.volumes`, `spark.kubernetes.driver.volumeMounts`, you can use:
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
                        secret=k8s.V1SecretProjection(items=[k8s.V1KeyToPath(key="key1", path="path1")])
                    ),
                    k8s.V1VolumeProjection(
                        config_map=k8s.V1ConfigMapProjection(
                            items=[k8s.V1KeyToPath(key="key1", path="path1")]
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
