# Build an ML image for Spark Connect

A notebook connected to a [Spark Connect server](spark_connect_server.md) sends plans,
not Python processes. Python UDFs, pandas UDFs and other Python code that Spark runs
for a query execute in the server's driver and executor containers, using the
packages installed in the server image. Packages in your notebook kernel are not
copied there. This recipe extends the Apache Spark image with pinned Python
libraries, then places the server on a private registry and an ML node pool with
the typed server settings.

## Write a pinned requirements file

Pin every package, including transitive dependencies, so that a rebuild cannot
pick up different code. Save this list as `requirements-ml.txt`. It has binary
wheels for Python 3.10–3.13 on Linux x86_64 and arm64, and satisfies the pandas,
PyArrow and NumPy minimums of PySpark 4.3 (2.2.0, 18.0.0 and 1.23.2). The image
installs it with `--no-deps`, so an unlisted dependency would only appear later as
an import error on the server. Change versions deliberately.

```text
numpy==2.2.6
pandas==2.3.3
python-dateutil==2.9.0.post0
pytz==2025.2
six==1.17.0
tzdata==2025.2
pyarrow==21.0.0
scikit-learn==1.7.2
scipy==1.15.3
joblib==1.5.2
threadpoolctl==3.6.0
```

To check a changed list before building, resolve it for the image's Python version
and platform without installing anything. The output must list exactly your pins,
with nothing added:

```bash
uv pip compile requirements-ml.txt --python-version 3.12 \
  --python-platform x86_64-manylinux_2_28 --only-binary :all: --no-annotate
```

## Extend the Spark image

Use the Spark release and Python variant that your server runs, preferably by
digest. The official images run as the `spark` user and contain Python 3 and pip in
their `-python3-` variants. Save this `Dockerfile` beside the requirements file:

```dockerfile
ARG SPARK_IMAGE=apache/spark:4.3.0-scala2.13-java21-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root
COPY requirements-ml.txt /opt/spark-ml/requirements-ml.txt
# Install into a separate directory: newer Ubuntu releases protect the system Python
# (PEP 668). --no-deps and --only-binary install exactly the pinned wheels.
RUN python3 -m pip install --no-cache-dir --no-deps --only-binary=:all: \
      --target /opt/spark-ml/python --requirement /opt/spark-ml/requirements-ml.txt
# Spark adds the container's PYTHONPATH to its Python workers on the driver and executors.
ENV PYTHONPATH="/opt/spark-ml/python${PYTHONPATH:+:${PYTHONPATH}}"
USER spark
```

Build, check the libraries and the Connect server JAR, then push to your registry:

```bash
docker build --tag registry.example.com/data/spark-connect-ml:4.3.0 .
docker run --rm --entrypoint python3 registry.example.com/data/spark-connect-ml:4.3.0 \
  -c 'import sys, sklearn, pandas, pyarrow; print(sys.version, sklearn.__version__, pandas.__version__, pyarrow.__version__)'
docker run --rm --entrypoint ls registry.example.com/data/spark-connect-ml:4.3.0 \
  /opt/spark/jars/spark-connect_2.13-4.3.0.jar
docker push registry.example.com/data/spark-connect-ml:4.3.0
```

The same image runs the driver and executors, so every executor has the same
libraries. GPU libraries such as CUDA builds of deep-learning frameworks also need
CUDA user-space libraries compatible with your nodes' drivers; follow their
vendors' instructions and pin them the same way.

## Match the client to the server

Two version pairs matter:

- **Spark.** Install the `pyspark-client` matching the server's Spark major.minor
  version in your notebook environment. A mismatched client can pass readiness and
  fail later; readiness checks report a `version_skew` warning, as described in
  [client and server versions](spark_connect.md#client-and-server-versions). The
  [Jupyter guide](jupyter.md#start-an-isolated-jupyter-kernel) shows how to pin the
  client in an isolated kernel.
- **Python.** A Python UDF is serialized by the notebook's Python and run by the
  image's Python. PySpark refuses to run it when their minor versions differ
  (`PYTHON_VERSION_MISMATCH`). Compare the `sys.version` printed above with your
  kernel's Python, and use the same minor version, or build the image from a base
  with your kernel's Python. DataFrame and SQL operations without Python UDFs do
  not depend on the image's Python version.

## Run on a private registry and a GPU node pool

An administrator creates the image pull Secret and installs the GPU device plugin,
such as NVIDIA's, which advertises a resource like `nvidia.com/gpu` on GPU nodes.
Clusters commonly label GPU nodes and taint them so that only pods tolerating the
taint run there; use your cluster's actual label and taint.

```bash
spark-on-k8s connect server create \
  --image registry.example.com/data/spark-connect-ml:4.3.0 \
  --server-jar local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar \
  --namespace spark --service-account spark --name ml-notebook \
  --image-pull-secret private-registry \
  --executor-node-selector example.com/pool=gpu \
  --executor-toleration nvidia.com/gpu:NoSchedule \
  --receipt-file ml-server.json --output json
```

This places executors on the pool; it does not request GPUs. Spark requests them
through its resource scheduling properties, which become Kubernetes container
limits for executors when the vendor is set. To use one GPU per executor and per
task, add these options to the command:

```bash
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.executor.resource.gpu.vendor=nvidia.com \
  --conf spark.executor.resource.gpu.discoveryScript=/opt/spark-ml/getGpusResources.sh \
  --conf spark.task.resource.gpu.amount=1
```

Kubernetes does not tell Spark which devices it assigned, so the image must contain
an executable discovery script, for example added with
`COPY --chmod=0755 getGpusResources.sh /opt/spark-ml/` before `USER spark`. The
Spark distribution includes an example script at
`examples/src/main/scripts/getGpusResources.sh`. See Spark's
[resource scheduling configuration](https://spark.apache.org/docs/4.3.0/configuration.html#custom-resource-scheduling-and-configuration-overview)
and [Kubernetes resource allocation](https://spark.apache.org/docs/4.3.0/running-on-kubernetes.html#resource-allocation-and-configuration-overview)
for the requirements and for fractional task amounts. The server's driver Pod is
created by this helper with CPU and memory from its resource settings only; do not
set `spark.driver.resource.*`, which would not add device limits to that Pod.

Credentials for object storage or model registries belong in Kubernetes Secrets,
referenced with `--driver-env-from-secret` and `--executor-env-from-secret`, as in
[Private images, credentials and node placement](spark_connect_server.md#private-images-credentials-and-node-placement).
For a runnable notebook that uses a pull Secret, a Secret-backed variable and a node
selector, see the [private server quickstart](jupyter.md#private-server-quickstart).
