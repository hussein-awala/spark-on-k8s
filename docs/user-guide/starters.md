# ETL and lakehouse starters

The source checkout includes two small projects with data, application images,
and [portable job definitions](jobs.md). Each has real Spark execution checks;
they are starting points for batch pipelines, not production storage or catalog
installations. Run the following commands from the repository root.

## Order events to Parquet

`examples/starters/etl/` reads JSON events with an explicit schema. It normalizes
country codes, quarantines invalid amounts, countries, and timestamps, chooses
the latest event per order, and writes three Parquet datasets:

| Dataset | Fixture result |
| --- | --- |
| `clean_orders` | 4 orders; `o1` keeps its latest amount of 1,250 cents |
| `quarantine` | 4 invalid events with rejection reasons |
| `daily_revenue` | 1 January: France, 2 paid orders, 1,750 cents; 2 January: US, 1 paid order, 2,000 cents |

Cancelled orders remain in the clean dataset but are excluded from revenue.
Money uses integer cents and dates use UTC. Revenue stays on the original order
date even when a newer event arrives on a later day; the fixture checks that
case. Equal update timestamps use a deterministic
record hash as a tie-break. Replace that rule with an authoritative event sequence
if your source provides one. Replays overwrite only the three named datasets
under the selected output prefix, so use a dedicated prefix for this job. This
Parquet replacement is not an atomic multi-table transaction: do not run concurrent
writers against the same output prefix.

Build and check the example without a Kubernetes cluster:

```bash
docker build -f examples/starters/etl/Dockerfile -t starter-etl:4.3.0 .
docker run --rm --network none --memory 2g --cpus 2 \
  -e SPARK_LOCAL_IP=127.0.0.1 \
  --mount "type=bind,source=$PWD/tests/integration/starters/check.py,target=/opt/spark/work-dir/check.py,readonly" \
  --entrypoint /opt/spark/bin/spark-submit starter-etl:4.3.0 \
  --master 'local[2]' --conf spark.ui.enabled=false --conf spark.driver.host=127.0.0.1 \
  /opt/spark/work-dir/check.py --case etl --project /opt/spark/work-dir/starters \
  --work-dir /tmp/starter-check --spark-version 4.3.0
```

The check runs the pipeline twice and reads the written Parquet data to assert
those results. `SPARK_STARTER_OK` indicates success; output data in this disposable
container is removed when it exits. The ETL check covers Spark 3.5.9, 4.0.5, and
4.3.0 in the [runtime workflow](https://github.com/hussein-awala/spark-on-k8s/actions/workflows/spark-compatibility.yml).
Use the matching `SPARK_IMAGE` build argument to test another supported image.

## Replayable Iceberg updates

`examples/starters/lakehouse/` bootstraps two fixture orders and merges a batch
containing a new order and two versions of an existing order. It deduplicates the
incoming batch and updates a stored order only when the incoming timestamp is
newer. Equal incoming timestamps use amount and country as deterministic
tie-breaks; equal timestamps already stored in the table are not overwritten.
The expected final rows are `o1=1250`, `o2=500`, and `o3=2000` cents. Replaying the
batch must leave those values and the row count unchanged.

This example uses **Spark 4.0.5, Scala 2.13, and Iceberg 1.12.0**. Apache Iceberg
1.12.0 publishes Spark 3.5, 4.0, and 4.1 runtime artifacts; it does not publish a
Spark 4.3 runtime artifact. Do not use this jar with Spark 4.3. See the
[official Iceberg release artifacts](https://iceberg.apache.org/releases/).

Download the pinned jar, verify its published checksum, and build the image:

```bash
mkdir -p examples/starters/target
ICEBERG_JAR=examples/starters/target/iceberg-spark-runtime-4.0_2.13-1.12.0.jar
curl --fail --location --retry 3 \
  https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.12.0/iceberg-spark-runtime-4.0_2.13-1.12.0.jar \
  -o "$ICEBERG_JAR"
printf '%s  %s\n' \
  '03b923f05fb7c9224dbf36a0a443726915a4b81fd9ff171e7c90395e992220294197918c3725076485f6e72bfdd4838d46599a37416ccdd77abc757330b02aff' \
  "$ICEBERG_JAR" | shasum -a 512 --check
docker build -f examples/starters/lakehouse/Dockerfile -t starter-lakehouse:4.0.5 .
```

The jar is bundled into the image; Spark does not need to download Maven packages
at runtime. Check merges, replay safety, and reading the pre-merge snapshot:

```bash
docker run --rm --network none --memory 2g --cpus 2 \
  -e SPARK_LOCAL_IP=127.0.0.1 \
  --mount "type=bind,source=$PWD/tests/integration/starters/check.py,target=/opt/spark/work-dir/check.py,readonly" \
  --entrypoint /opt/spark/bin/spark-submit starter-lakehouse:4.0.5 \
  --master 'local[2]' --conf spark.ui.enabled=false --conf spark.driver.host=127.0.0.1 \
  /opt/spark/work-dir/check.py --case iceberg --project /opt/spark/work-dir/starters \
  --work-dir /tmp/starter-check --spark-version 4.0.5
```

The check verifies both the original snapshot and the current table contents.
This is a single-writer demonstration. The bootstrap step is not a concurrent
initialization protocol, and snapshot cleanup or retention policies are not
configured. Set those policies before growing a real table indefinitely.

## Submit on Kubernetes

Configure namespace `spark` and service account `spark` as described in
[Getting Started](getting_started.md). The job definitions mount the same PVC at
`/data` in **both** driver and executor templates. This is required: a driver's
local filesystem is not shared with executor Pods.

`examples/starters/storage.yaml` requests `ReadWriteMany`. Select an RWX-capable
StorageClass for your cluster and ensure it grants write access to group `185`.
The templates set `fsGroup: 185`, but storage provisioners differ in how they apply
ownership. For a single-node demo only, `ReadWriteOnce` can be used instead.

```bash
kubectl apply -f examples/starters/storage.yaml
kubectl -n spark get pvc spark-starter-data
```

Continue only when the claim is `Bound`. For a local kind cluster, preload the
images; the job files use `image_pull_policy: Never`:

```bash
kind load docker-image starter-etl:4.3.0 starter-lakehouse:4.0.5
uv run --no-sync spark-on-k8s job validate examples/starters/etl/job.yaml
uv run --no-sync spark-on-k8s job plan examples/starters/etl/job.yaml --output json
uv run --no-sync spark-on-k8s job submit examples/starters/etl/job.yaml --timeout 300
uv run --no-sync spark-on-k8s job submit examples/starters/lakehouse/job.yaml --timeout 300
```

For another cluster, push the images to a registry it can access, then update the
job files' image names and use `IfNotPresent`. Change the input and output paths
for your own data. Data paths must be accessible to every Pod; the client does not
upload local datasets. The same job files can be loaded with `JobSpec` and passed
to an [Airflow operator](jobs.md#python-and-airflow).

The lakehouse job uses a Hadoop catalog on the shared filesystem at
`file:///data/lakehouse`. It requires a filesystem with appropriate atomic rename
semantics. For object storage, configure a suitable catalog (for example REST)
and storage connector instead of changing this local path to an S3 URI. Follow
[Iceberg catalog configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
and keep credentials in Kubernetes Secrets or workload identity.

Deleting a driver does not delete these output datasets or the PVC. The local
checks and CI container checks validate Spark computation and data results;
shared-storage provisioning and Kubernetes execution of these particular starter
jobs still depend on the target cluster. The separate compatibility workflow
also exercises actual Kubernetes submission and driver-owned resource cleanup.
