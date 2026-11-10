# Jupyter on Kubernetes integration

The canonical runnable example is
[`examples/jupyter/spark_connect.ipynb`](../../../examples/jupyter/spark_connect.ipynb).
Its setup and recovery instructions live in the [Jupyter guide](../../../docs/user-guide/jupyter.md).
The companion `notebook_support.py` is copied with it; no separate test notebook or
substitute cell implementation is maintained.

`--notebook private_connect_quickstart.ipynb` runs the
[private server quickstart](../../../examples/jupyter/private_connect_quickstart.ipynb)
instead, through the same kernel, receipt, rerun-refusal, tunnel and cleanup
handling. Before execution the runner creates a placeholder
`kubernetes.io/dockerconfigjson` Secret and a credential Secret holding a random
token, and sets the notebook's Secret-reference and node-selector variables. It
then requires `quickstart-result.json` to report executor pods with the expected
node selector, pull Secret name and Secret-key reference, the variable present in
every executor task, and no remaining owned resources. The run fails if the token
appears in any file in the artifact directory. The image is preloaded with pull
policy `Never`, so registry authentication itself is not exercised; the quickstart
has no kernel restart.

The runner requires an installed, noneditable `spark-on-k8s[connect]` wheel, the
locked `notebook` dependency group, `kubectl`, and an explicitly selected disposable
kind cluster containing the chosen Spark image. The existing Spark Connect workflow
runs it sequentially with the provisioning harness for Spark 4.0.5 and 4.3.0. Use
Python 3.12 for the notebook CI environment. Each row explicitly installs
`pyspark-client==SPARK_VERSION` after the locked profile; the runner and notebook
preflight require this matching pair for the local-data DataFrame workflow. A
JupyterLab server is not needed in CI.

For a prepared disposable cluster and wheel environment:

```bash
.venv/bin/python -I tests/integration/jupyter/run_kubernetes.py \
  --kubeconfig /path/to/disposable-kubeconfig \
  --context kind-spark-connect-ci \
  --image spark-connect-test:4.0.5 \
  --spark-version 4.0.5 \
  --server-jar local:///opt/spark/jars/spark-connect_2.13-4.0.5.jar \
  --artifacts /tmp/jupyter-kubernetes-results
```

Use a fresh artifact directory for each run. The runner creates its own unique
namespace and service account (`--service-account` defaults to `spark`). It copies
the canonical example unchanged into the artifact directory, hashes both files,
and runs every code cell in actual IPython kernels using the same interpreter as
the installed-wheel runner. Kernel/config/data/history state stays under the
artifact directory, and `-I` plus SDK import assertions prevent source shadowing.
No user kernel registration or default kubeconfig change occurs.

The notebook covers the Jupyter-specific path through the existing lifecycle:

- Provision a server from a notebook cell and persist a UID-bearing receipt.
- Deliberately rerun the exact creation cell; driver/Service UIDs and receipt bytes
  must stay unchanged.
- Run DataFrame and SQL actions remotely, collect a bounded Pandas table, and
  assert actual HTML table output. Executor hostnames must match owned Kubernetes
  executor Pods and exclude the driver.
- Check session isolation and successful work after an intentionally invalid SQL
  query.
- Close the first client/tunnel, restart the actual kernel process, restore the
  receipt, and reconnect to the same application and driver UID.
- Request UID-protected deletion, observe owned resource garbage collection before
  namespace cleanup, repeat deletion, and verify tunnel shutdown.

The runner follows the notebook's restart tag. Interactive users must instead run
sections 1–6, use **Kernel → Restart Kernel**, and continue at section 7; ordinary
Run All intentionally does not satisfy the restart assertion. An abrupt kernel
crash is a different scenario from this planned restart.

Cell execution, API calls, transport checks, and cleanup waits have deadlines.
Evidence includes the exact copied notebook/helper, executed notebook, restart
PIDs, receipt, Kubernetes snapshots/logs, rerun identities, tunnel process records,
and separate server/namespace cleanup results. The runner reuses the Connect
harness's diagnostics and garbage-collection helpers. It does not repeat that
harness's stale UID, foreign Service, pending driver, and startup-failure suite.

On failure, cleanup only uses a validated receipt and the recorded namespace UID.
A malformed/partial receipt, unresolved creation marker, replaced namespace, or
ownership mismatch causes preservation and a failing cleanup report. Namespace
deletion cannot turn a failed resource-cleanup assertion into a pass. Tunnel
cleanup considers only recorded processes with this test's exact command,
namespace, context, PID, and observed start time; uncertain identity is preserved.

To verify failure cleanup deliberately, use a new artifact directory and add
`--interrupt-after-cell open-tunnel` (or `create-server`). This raises a deliberate
runner interruption and must exit nonzero. Inspect `failure.json`,
`fallback-server-cleanup.json`, `tunnel-cleanup.json`, `fixture-cleanup.json`, and
`cleanup-errors.json`; the last file must be empty when cleanup succeeds. This is
an interrupted runner test, not a claim that arbitrary machine crashes are safe.

The Spark 4.3.0 CI row runs the interruption scenario automatically. It requires
the exact injected failure and independently checks server garbage collection,
closed tunnel records, namespace absence, and an empty cleanup-error report.
