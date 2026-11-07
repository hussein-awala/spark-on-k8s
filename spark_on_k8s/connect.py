from __future__ import annotations

import ipaddress
import json
import math
import os
import re
import subprocess
import sys
import tomllib
from contextlib import contextmanager, redirect_stdout
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit

_CHECK_ERROR = "Spark Connect check failed; verify the client, endpoint, TLS, and credentials."


class ConnectError(ValueError):
    """Invalid connection settings or a failed bounded connection check."""


@dataclass(frozen=True, kw_only=True)
class ConnectProfile:
    """Connection to an existing Spark Connect service; no cluster is created."""

    endpoint: str
    tls: bool = True
    token_env: str | None = None
    user_id: str | None = None
    allow_insecure: bool = False

    def __post_init__(self):
        if not isinstance(self.endpoint, str) or any(c.isspace() for c in self.endpoint):
            raise ConnectError("endpoint must be a Spark Connect URL without whitespace.")
        try:
            url = urlsplit(self.endpoint)
            port = url.port
        except ValueError:
            raise ConnectError("Invalid Spark Connect endpoint.") from None
        if (
            url.scheme != "sc"
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
            or ";" in self.endpoint
            or port == 0
        ):
            raise ConnectError("Use sc://host:port without credentials, parameters, query, or path.")
        if type(self.tls) is not bool or type(self.allow_insecure) is not bool:
            raise ConnectError("tls and allow_insecure must be booleans.")
        try:
            loopback = ipaddress.ip_address(url.hostname).is_loopback
        except ValueError:
            loopback = url.hostname.lower() == "localhost"
        if not self.tls and not loopback and not self.allow_insecure:
            raise ConnectError("Remote endpoints require TLS; use allow_insecure only on a trusted network.")
        if self.token_env is not None and (
            not isinstance(self.token_env, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.token_env)
        ):
            raise ConnectError("token_env must name an environment variable.")
        if self.user_id is not None and (not isinstance(self.user_id, str) or not self.user_id):
            raise ConnectError("user_id must be a non-empty string.")

    @classmethod
    def from_file(cls, path: str | Path, *, profile: str = "default") -> ConnectProfile:
        with Path(path).expanduser().open("rb") as stream:
            content = stream.read(1024 * 1024 + 1)
        if len(content) > 1024 * 1024:
            raise ConnectError("Connect profile files must be no larger than 1 MiB.")
        try:
            document = tomllib.loads(content.decode("utf-8"))
        except (UnicodeError, tomllib.TOMLDecodeError):
            raise ConnectError("Invalid Connect profile TOML.") from None
        if set(document) != {"profiles"} or not isinstance(document["profiles"], dict):
            raise ConnectError("Expected a TOML [profiles.NAME] table.")
        values = document["profiles"].get(profile)
        if not isinstance(values, dict):
            raise ConnectError("The selected Connect profile does not exist.")
        if set(values) - set(cls.__dataclass_fields__) or "endpoint" not in values:
            raise ConnectError("Connect profile has missing or unsupported fields.")
        return cls(**values)

    def connection_string(self) -> str:
        """Build a private URI for PySpark. This return value may contain credentials."""
        url = self.endpoint.rstrip("/") + "/;use_ssl=" + str(self.tls).lower()
        if self.token_env:
            token = os.environ.get(self.token_env)
            if not token:
                raise ConnectError("The configured token environment variable is empty or missing.")
            url += ";token=" + quote(token, safe="")
        if self.user_id:
            url += ";user_id=" + quote(self.user_id, safe="")
        return url

    @contextmanager
    def session(self):
        """Create and close an independent remote session, including when user code fails."""
        connection = self.connection_string()
        try:
            from pyspark.sql.connect.session import SparkSession
        except ImportError:
            raise ConnectError(
                "Install spark-on-k8s[connect] on Python 3.11–3.14, or a matching pyspark[connect] client."
            ) from None
        try:
            session = SparkSession.builder.remote(connection).create()
        except Exception:
            raise ConnectError(
                "Could not open a Spark Connect session; check endpoint and credentials."
            ) from None
        try:
            yield session
        finally:
            session.stop()

    def probe(self, *, timeout: float = 15) -> dict:
        """Execute a tiny remote query in a child process with a hard total timeout."""
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
            raise ConnectError("timeout must be a positive finite number of seconds.")
        # Credentials remain in the environment/stdin, never process arguments or a temporary file.
        try:
            result = subprocess.run(
                [sys.executable, "-I", "-m", "spark_on_k8s.connect"],
                input=json.dumps(asdict(self)),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise TimeoutError("Spark Connect check timed out; the server was not stopped.") from None
        if result.returncode:
            try:
                error = json.loads(result.stdout)["error"]
            except (ValueError, KeyError, TypeError):
                error = _CHECK_ERROR
            raise ConnectError(error)
        try:
            payload = json.loads(result.stdout)
            if payload.get("schema_version") != 1 or payload.get("status") != "ok":
                raise ValueError
            return payload
        except (ValueError, AttributeError):
            raise ConnectError("Spark Connect check returned an invalid response.") from None


def _probe_worker():
    try:
        profile = ConnectProfile(**json.loads(sys.stdin.read(1024 * 1024)))
        # PySpark diagnostics must not corrupt the machine-readable result.
        with redirect_stdout(sys.stderr), profile.session() as session:
            assert session.range(1).count() == 1
            version = session.version
        payload = {"schema_version": 1, "status": "ok", "spark_version": version}
        code = 0
    except Exception:
        payload = {
            "schema_version": 1,
            "status": "error",
            "error": _CHECK_ERROR,
        }
        code = 1
    print(json.dumps(payload))
    return code


if __name__ == "__main__":
    raise SystemExit(_probe_worker())
