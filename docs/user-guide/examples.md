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