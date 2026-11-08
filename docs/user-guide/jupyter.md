# Jupyter notebooks on Kubernetes

Create a Spark Connect server from a local Jupyter notebook, run DataFrame and SQL
queries on Kubernetes executors, reconnect after a planned kernel restart, and
delete the recorded server. The notebook uses the existing
[server lifecycle SDK](spark_connect_server.md) and
[independent Connect sessions](spark_connect.md); Jupyter runs on your machine.

Use the canonical example at `examples/jupyter/spark_connect.ipynb` with
`notebook_support.py` beside it. You can also download the
[notebook bundle](../downloads/jupyter/jupyter-connect.zip),
[notebook](../downloads/jupyter/spark_connect.ipynb), or
[helper](../downloads/jupyter/notebook_support.py).
These downloads are built from the same source as the selected documentation
version. The bundle contains both required files; it does not include credentials
or an environment.

## Prepare the cluster and client

Server provisioning is available in the current source and may not be in your
installed PyPI release. Follow [current-source installation](installation.md#current-source)
and use a checkout matching the documentation version. Run the following commands
from that checkout's root. The example does not create a cluster, namespace,
service account, catalog or storage.

- An administrator must prepare an existing namespace and Spark driver service
  account with permission to create and manage executors. See the
  [namespace setup guidance](getting_started.md#set-up-the-kubernetes-namespace).
- Your Kubernetes identity needs access to create, read and delete the driver and
  Services, inspect executor resources and logs, and use pod `exec` and
  `port-forward`. The example's startup helper uses `exec` to wait for the Spark
  listener before opening a tunnel.
- Install `uv`, Git and `kubectl` locally. Use Python 3.12 for this example; the
  optional Connect client supports Python 3.11–3.14. Local Spark and Java are not
  required by the thin `pyspark-client`.
- Choose a Spark 4 image available to the cluster, for its architecture, containing
  the exact Connect server JAR. The commands below select Spark 4.3.0; the
  integration workflow also selects 4.0.5. Use the client/server pair selected
  below and consult the [compatibility guidance](spark_compatibility.md).

The server exposes plaintext gRPC through a private `ClusterIP` Service. The
example opens a notebook-owned tunnel bound only to `127.0.0.1`; it does not expose
a public endpoint. Use a trusted development cluster. Server authentication, TLS,
data authorization and network policy remain administrator responsibilities.
Kubernetes access does not provide isolation between clients of a shared server.

## Start an isolated Jupyter kernel

On macOS or Linux, create a separate environment using the repository's locked
notebook dependencies and Connect extra. The optional JupyterLab frontend is
installed into that same environment; it is not needed by the automated notebook runner.

This notebook uses these client/server pairs:

| Spark server | Thin client |
| --- | --- |
| 4.3.0 (default) | `pyspark-client==4.3.0` |
| 4.0.5 | `pyspark-client==4.0.5` |

A newer client API can require a newer server even when a simple query works. In
validation, client 4.3.0 passed readiness and range queries against server 4.0.5,
but `createDataFrame` from local rows failed because that server lacked
`spark.sql.session.localRelationSizeLimit`. This specific failure is why the
notebook checks matching versions; it does not establish that every operation
across different versions is incompatible.

```bash
UV_PROJECT_ENVIRONMENT=.venv-notebook uv sync --python 3.12 --locked \
  --no-default-groups --group notebook --extra connect
uv pip install --python .venv-notebook/bin/python 'jupyterlab==4.6.6'
uv pip install --python .venv-notebook/bin/python --no-deps 'pyspark-client==4.3.0'
uv pip check --python .venv-notebook/bin/python
.venv-notebook/bin/python -c 'from importlib.metadata import version; client = version("pyspark-client"); assert client == "4.3.0", client; print("Connect client:", client)'

export IPYTHONDIR="$PWD/.venv-notebook/jupyter-state/ipython"
export JUPYTER_CONFIG_DIR="$PWD/.venv-notebook/jupyter-state/config"
export JUPYTER_DATA_DIR="$PWD/.venv-notebook/jupyter-state/data"
export JUPYTER_RUNTIME_DIR="$PWD/.venv-notebook/jupyter-state/runtime"
export JUPYTER_PATH="$PWD/.venv-notebook/share/jupyter"
mkdir -p "$IPYTHONDIR" "$JUPYTER_CONFIG_DIR" "$JUPYTER_DATA_DIR" "$JUPYTER_RUNTIME_DIR"
.venv-notebook/bin/python -m ipykernel install --prefix "$PWD/.venv-notebook" \
  --name spark-connect-notebook --display-name 'Spark Connect (Python 3.12)'
```

The kernel registration and notebook state stay in this environment instead of a
user-wide kernel installation. Jupyter can still discover other configured kernels;
select **Spark Connect (Python 3.12)** in the notebook. An existing Jupyter frontend
can also use this kernel when configured with the same kernel search path.
See the upstream [kernel installation guide](https://ipython.readthedocs.io/en/stable/install/kernel_install.html)
and [Jupyter directory settings](https://docs.jupyter.org/en/latest/use/jupyter-directories.html).

Do not install full `pyspark` alongside `pyspark-client` in this environment: both
provide the `pyspark` Python import namespace. A later `uv sync` can restore the
locked client version and remove the separately installed JupyterLab frontend.
Both version choices explicitly select the client after the locked sync, matching
the CI matrix even if a future lockfile selects a newer client. After resyncing,
reapply the selected client version, reinstall the frontend, and
check versions before launching a new kernel. Use the environment's executables
directly so an implicit sync does not undo that choice.
Normal source use is editable. CI installs the built wheel and checks the actual
kernel interpreter and package location separately.

Set explicit cluster settings before launching Jupyter:

```bash
export SPARK_NOTEBOOK_KUBECONFIG=/absolute/path/to/kubeconfig
export SPARK_NOTEBOOK_CONTEXT=your-kubernetes-context
export SPARK_NOTEBOOK_NAMESPACE=your-prepared-namespace
export SPARK_NOTEBOOK_SERVICE_ACCOUNT=spark
export SPARK_NOTEBOOK_SPARK_VERSION=4.3.0
export SPARK_NOTEBOOK_IMAGE=apache/spark:4.3.0-scala2.13-java21-python3-ubuntu
export SPARK_NOTEBOOK_SERVER_JAR=local:///opt/spark/jars/spark-connect_2.13-4.3.0.jar
export SPARK_NOTEBOOK_ARTIFACT_DIR="$PWD/downloads/jupyter-results"
```

To choose **Spark 4.0.5 instead**, override the thin client in this same isolated
environment after the locked sync and before starting Jupyter:

```bash
uv pip install --python .venv-notebook/bin/python --no-deps 'pyspark-client==4.0.5'
uv pip check --python .venv-notebook/bin/python
.venv-notebook/bin/python -c 'from importlib.metadata import version; client = version("pyspark-client"); assert client == "4.0.5", client; print("Connect client:", client)'
export SPARK_NOTEBOOK_SPARK_VERSION=4.0.5
export SPARK_NOTEBOOK_IMAGE=apache/spark:4.0.5-scala2.13-java17-python3-ubuntu
export SPARK_NOTEBOOK_SERVER_JAR=local:///opt/spark/jars/spark-connect_2.13-4.0.5.jar
```

If changing an already running example, finish its server cleanup and stop its
kernel first. Use a new artifact directory for the new run after confirming that
cleanup; do not overwrite or discard the previous server receipt. Changing a
Python package on disk does not replace a client already imported into a kernel.

Launch the notebook with the selected pair:

```bash
.venv-notebook/bin/jupyter lab examples/jupyter/spark_connect.ipynb
```

The helper reads these settings without changing your current Kubernetes context.
When `SPARK_NOTEBOOK_SERVER_JAR` is unset, the helper derives
`local:///opt/spark/jars/spark-connect_2.13-<version>.jar` from the selected Spark
version; override it for a different image layout.
`SPARK_NOTEBOOK_IMAGE_PULL_POLICY` defaults to `IfNotPresent`; use `Never`
only when the image is already loaded on the cluster's nodes. The notebook requests
one small executor; adjust capacity for your workload rather than treating the
example's resource settings as production sizing.

For the download, extract both files into a writable directory and open its
`spark_connect.ipynb` using the same environment. Keep the helper beside the
notebook. Keep the artifact directory until resource cleanup is confirmed; it
contains the server receipt needed to reconnect or delete safely.

## Run, restart and reconnect

Run the numbered notebook sections in order:

1. **Sections 1–3:** inspect the kernel and settings, create the server, save its
   receipt, and open the loopback tunnel. Query readiness performs a real action
   and verifies the remote application against the receipt. A `Running` pod alone
   is not query readiness.
2. **Section 4:** run distributed DataFrame and SQL queries and display a bounded
   pandas result. The range sum is **49,995,000**. The result table has `books` and
   `games`, each with quantity `3` and revenue `45.0`. The notebook compares the
   reported executor hostnames with actual owned Kubernetes executor pods.
3. **Section 5:** open an independent session, verify that temporary views and SQL
   settings do not leak into it, and recover from an intentionally invalid query.
4. **Section 6:** close the client sessions and tunnel, save restart evidence and
   leave the server running. Use **Kernel → Restart Kernel**. **Do not use Run All
   across this boundary.**
5. **Section 7:** resume in the new kernel. It restores the recorded server,
   establishes a new tunnel and session, and checks the same application and driver
   UID. The query returns the same sum. The previous session's temporary view is
   absent and must be recreated.
6. **Section 8:** explicitly delete the server and wait for owned resources to
   disappear. The final `notebook-result.json` records `status: passed`,
   `kernel_restart_verified: true`, and `owned_resources_remaining: []`.

The creation cell refuses to create a second server when its receipt or pending
creation guard exists. Do not delete these files to bypass the guard. Reconnect
to the known server or complete its cleanup first. If receipt publication fails
after creation, retain the full identity printed by the cell and the available
`server` handle; resources can still exist.

The notebook's helper manages the tunnel separately from the SDK. Closing a
session, tunnel or notebook does not delete the server. A readiness timeout also
leaves the server running. Keep the receipt and inspect the existing resources
before attempting another creation.

## Data and session boundaries

| Where state lives | What survives a notebook kernel restart |
| --- | --- |
| Notebook variables and local Python objects | Nothing in the old kernel; rerun imports and recreate variables |
| Connect session temporary views and session settings | A new session does not inherit them |
| The Kubernetes driver and executor resources | They continue until explicitly deleted or failed; the receipt identifies them |
| Shared catalog and durable data | Only what your separately configured catalog and storage preserve |

Paths passed to Spark refer to the server/executor environment. Local notebook
files, installed Python packages and credentials are not automatically copied to
executor pods. Use shared storage reachable by both driver and executors, and
install required connectors and worker dependencies in the appropriate image.
Collect only bounded results with `toPandas()` or `collect()`: those results are
transferred into the notebook process.

Spark Connect exposes DataFrame and SQL operations; classic SparkContext/RDD APIs
are unavailable. The example proves these generated-data operations and notebook
rendering, not every Python UDF, extension, storage connector or production catalog.
See the upstream [Spark Connect quickstart](https://spark.apache.org/docs/4.3.0/api/python/getting_started/quickstart_connect.html).

## Recover without notebook variables

The restart exercise deliberately closes sessions and the tunnel before restarting
the kernel. It uses `phase-one.json` to prove that the kernel changed and the server
did not. This is a **planned restart test**, not abrupt-crash recovery coverage.

If the kernel exits before that evidence file is written, server status and
deletion still need only `server-receipt.json` and your cluster credentials. From
the checkout, use the same environment and explicit cluster settings:

```bash
.venv-notebook/bin/spark-on-k8s connect server status \
  "$SPARK_NOTEBOOK_ARTIFACT_DIR/server-receipt.json" \
  --kubeconfig "$SPARK_NOTEBOOK_KUBECONFIG" --context "$SPARK_NOTEBOOK_CONTEXT" --output json
.venv-notebook/bin/spark-on-k8s connect server delete \
  "$SPARK_NOTEBOOK_ARTIFACT_DIR/server-receipt.json" \
  --kubeconfig "$SPARK_NOTEBOOK_KUBECONFIG" --context "$SPARK_NOTEBOOK_CONTEXT" --output json
```

Deletion requests cleanup; keep the receipt until the resources are observed
absent. To continue work instead, use the receipt-only recovery instructions at
the end of the notebook: restore `ConnectServerHandle` from the receipt's inner
`server` mapping, open a new tunnel, verify readiness, and create a new session.
That workflow does not require the planned-restart evidence file.

An abruptly terminated kernel can leave a tunnel process running. Inspect the
example's tunnel process records and stop only the process you confirm belongs to
that notebook; do not assume a recorded PID still identifies the same process.
The SDK does not own tunnels. A failed driver also is not automatically replaced:
inspect its recorded status and follow [server recovery](spark_connect_server.md#observe-stop-and-recover)
before creating a replacement with a new receipt.

| Problem | Next action |
| --- | --- |
| Wrong kernel or import failure | Select the registered kernel and check its printed interpreter and package versions |
| Pending driver or listener timeout | Inspect scheduling, image/JAR, service-account permissions and driver logs; retain the receipt |
| Lost tunnel or readiness timeout | Repair connectivity and retry readiness for the recorded server; do not create a duplicate |
| Receipt or pending creation guard already exists | Inspect the saved/partial identity and reconnect or finish cleanup; an uncertain response is not absence |
| Same-name resource has a different UID | Stop; inspect the ownership conflict instead of deleting by name |

## Automated coverage

The Spark Connect Kubernetes workflow executes this exact notebook through a real
IPython kernel, restarts that kernel at the marked boundary, and checks distributed
execution, rendered table output, session separation, failure recovery and cleanup.
It uses a built wheel on disposable kind clusters with matching client/server
pairs 4.0.5/4.0.5 and 4.3.0/4.3.0. Its evidence includes the executed notebook and JSON results.

This automation covers notebook kernel execution and output. It does not click
through JupyterLab's browser UI, test arbitrary extensions, or simulate an abrupt
kernel/driver crash. Follow [development validation](../development.md#real-runtime-checks)
for the runner and workflow; a failed or unexecuted matrix row is not passing
runtime evidence.
