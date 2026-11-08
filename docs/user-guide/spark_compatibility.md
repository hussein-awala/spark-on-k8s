# Spark compatibility

The client supports the Apache Spark 3.5 and Spark 4 container entrypoint. It
creates a driver Pod and starts `spark-submit` in client mode inside that Pod;
Spark creates the executor Pods. No Spark installation or Java runtime is needed
on the machine running the Python client, CLI, or API.

The execution matrix covers the maintained 3.5 line, the first Spark 4 minor
line, and the current Spark 4 release:

| Spark | Apache image tag | Scala binary version | Runtime Java | Upstream Python minimum |
| --- | --- | --- | --- | --- |
| 3.5.9 | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` | 2.12 | 17 | 3.8 |
| 4.0.5 | `apache/spark:4.0.5-scala2.13-java17-python3-ubuntu` | 2.13 | 17 | 3.9 |
| 4.3.0 | `apache/spark:4.3.0-scala2.13-java21-python3-ubuntu` | 2.13 | 21 | 3.11 |

The workflow pins these images by digest. These are selected test combinations,
not the complete set of runtimes supported by Apache Spark. See the upstream
requirements for [3.5.9](https://spark.apache.org/docs/3.5.9/),
[4.0.5](https://spark.apache.org/docs/4.0.5/), and
[4.3.0](https://spark.apache.org/docs/4.3.0/). Other Spark releases and custom
images need their own validation. Spark Connect, custom schedulers, GPU workloads,
and third-party connectors are outside this matrix. Connect has a separate
[server lifecycle matrix](spark_connect_server.md#prerequisites) for Spark 4.0.5
and 4.3.0.

## Client and application runtimes

The `spark-on-k8s` Python package requires Python 3.11 or newer. That is separate
from the Python and Java runtimes in the application image. The driver and its
Python workers must use compatible Python versions and the PySpark version that
matches the image's Spark distribution. The integration workflow runs the client
on Python 3.11 and uses the Python runtime supplied by each Apache image.

When upgrading a Java or Scala application, match the Spark dependencies and
Scala artifact suffix to the image. For example, Spark 4 uses
`spark-sql_2.13`, whereas the selected Spark 3.5 image uses `spark-sql_2.12`.
Use Maven's `provided` scope for Spark dependencies; the image supplies them at
runtime. Match third-party connectors, such as Iceberg, to both the Spark minor
version and Scala binary version supported by that connector.

Spark 4 also changes SQL behavior, including enabling ANSI mode by default.
The client does not disable these checks. Validate application queries against
the [Spark SQL migration guide](https://spark.apache.org/docs/4.3.0/sql-migration-guide.html)
when upgrading.

## Build the examples with Spark 4

The [example Dockerfiles](examples.md) default to Spark 3.5.9. From the repository
root, build the Python example with Spark 4.3.0:

```bash
docker build -t pyspark-job \
  --build-arg SPARK_IMAGE=apache/spark:4.3.0-scala2.13-java21-python3-ubuntu \
  examples/python
```

For Java, change the compile-time dependencies as well as the runtime image:

```bash
docker build -t java-spark-job \
  --build-arg SPARK_IMAGE=apache/spark:4.3.0-scala2.13-java21-python3-ubuntu \
  --build-arg SPARK_VERSION=4.3.0 \
  --build-arg SCALA_BINARY_VERSION=2.13 \
  examples/java
```

The Java builder uses JDK 17, which can compile against both selected Spark 3 and
Spark 4 distributions. The resulting application runs on the image's JDK. Load
or push the resulting image and submit it using the [example instructions](examples.md).
Do not replace the Apache image's entrypoint: the client relies on its `driver`
command and Spark's executor command.

## Execution tests

The [Spark runtime compatibility workflow](https://github.com/hussein-awala/spark-on-k8s/actions/workflows/spark-compatibility.yml)
creates an isolated kind cluster for each of the three versions. It installs the
built client wheel, compiles a Java job against the image's exact Spark jars, and
submits four small applications through `SparkOnK8S`:

- Python worker computation and a DataFrame aggregation, including Secret-backed
  environment variables in both driver and executor.
- Multiple SQL statements mounted in a managed ConfigMap, including a quoted
  semicolon and a checked aggregation.
- A SQL error that must fail the driver and prevent the next statement from running.
- A Java class entrypoint that performs and checks a distributed aggregation.

Successful jobs must create executor Pods owned by the driver. After every job,
the test deletes the driver and checks that its Pods, Services, Secrets, and
ConfigMaps are garbage collected. The workflow uploads logs, Pod status, events,
and the execution result, including on failure. Each job uses one executor,
a four-minute application deadline, and a 25-minute overall deadline. At most
two matrix jobs run concurrently. The harness accepts only a kind context and
creates and deletes its own uniquely named namespace.

Local Spark computation can also be checked without Docker. With `JAVA_HOME`
pointing to JDK 21, run from a checkout:

```bash
REPOSITORY="$PWD"
RUNTIME_DIR="$(mktemp -d)"
uv venv --python 3.11 "$RUNTIME_DIR/runtime"
uv pip install --python "$RUNTIME_DIR/runtime/bin/python" 'pyspark==4.3.0'
cd "$RUNTIME_DIR"
"$RUNTIME_DIR/runtime/bin/python" -I "$REPOSITORY/tests/integration/spark/run_local.py" \
  --spark-version 4.3.0 --artifacts "$RUNTIME_DIR/results"
```

Repeat with PySpark 3.5.9 or 4.0.5 and JDK 17 for the other matrix rows. This
executes Python, SQL, SQL failure, and Java workloads with `local[2]`; it does
**not** validate Kubernetes submission, networking, RBAC, or resource cleanup.
The Kubernetes workflow requires a working Docker daemon. Its run results are
the evidence for cluster compatibility; local-mode results are separate evidence.

The JVM matrix compiles a Java class against each runtime image's Spark jars.
This proves that selected workload, not compatibility of an existing Scala
assembly or arbitrary third-party connector. Rebuild and test your actual
application with the intended Java, Scala binary version, Spark jars and
connector set before upgrading it.
