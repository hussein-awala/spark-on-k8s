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
