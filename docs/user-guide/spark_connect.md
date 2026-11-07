# Spark Connect companion

Use an existing Spark Connect endpoint from notebooks or Python tools without
creating a Kubernetes job for every interactive query. This companion creates
independent client sessions; it does not deploy or operate a Connect server.

Install the optional thin client in a Python 3.11–3.14 environment:

```bash
uv pip install 'spark-on-k8s[connect]'
```

The extra targets Spark 4 clients and keeps pandas below 3, as required for full
upstream compatibility. pandas 2 has no Python 3.15 wheels, so this optional extra
is limited to Python 3.11–3.14; the base SDK and CLI retain Python 3.15 support. For Spark 3.5 servers, install the base package
and a matching `pyspark[connect]` version instead. Match client and server versions
when possible. The base Spark-on-K8s CLI/SDK does not import PySpark or need Java.
The Spark 4 thin client also needs no local JVM.

## Named connection profiles

Create `connect.toml` with endpoint addresses and references to credentials:

```toml
[profiles.production]
endpoint = "sc://spark.example.com:443"
tls = true
token_env = "SPARK_CONNECT_TOKEN"

[profiles.local]
endpoint = "sc://127.0.0.1:15002"
tls = false
```

Set the referenced token through your normal secret manager or shell environment.
Profile files reject embedded credentials, connection-string parameters, duplicate
keys, and unknown settings. TLS is enabled by default. Non-loopback plaintext
connections require an explicit `allow_insecure = true`; use this only with a trusted
network or an independently protected tunnel. TLS verifies server certificates using
the upstream gRPC client. There is no disable-certificate-verification option.

```bash
spark-on-k8s connect check connect.toml --profile production --timeout 15 --output json
```

The check runs a small remote DataFrame action and reports the server's Spark
version. It runs in a separate process with a total timeout, so an unavailable server
cannot leave a stuck CLI. Exit codes are 0 for success, 1 for configuration/connection
failure, 2 for CLI usage errors, and 5 for timeout. Error output omits gRPC details that
could contain credentials. A timed-out session may remain until the server's session
expiry policy cleans it up; the check never stops the shared server.

## Notebook and application sessions

```python
from spark_on_k8s.connect import ConnectProfile

profile = ConnectProfile.from_file("connect.toml", profile="production")
with profile.session() as spark:
    assert spark.range(100).count() == 100
    spark.range(5).show()
```

Every context opens a separate Connect session and attempts to release it on exit,
even when user code raises an exception. If both application code and release fail,
the original application exception is preserved with a cleanup note. A release-only
failure raises `ConnectError`; server-side session expiry remains the fallback. It does not reuse or stop another notebook session.
After the generated-data example works, try your warehouse data (for example
`spark.read.parquet("s3a://warehouse/events/")`) only after verifying the server
has the required connector, storage permissions and data. Files and data paths
are interpreted on the server. Cluster-wide state and static
Spark configuration belong to the server; classic RDD/SparkContext APIs are not
available through Connect. Authenticate and authorize the server with your existing
gRPC gateway and identity system: profiles do not provide server-side isolation.

For local access to a private Kubernetes service, use your normal `kubectl port-forward`
tunnel and the loopback profile. The helper never reads or changes your kubeconfig.
`connection_string()` is available for advanced integration but can return credentials;
do not log it. Profile representations contain only the environment-variable name.

See the upstream [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
and [client installation guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html).
