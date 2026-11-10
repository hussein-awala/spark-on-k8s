"""The failure postmortem's driver log tail redacts common secret spellings before Airflow logs it."""

from __future__ import annotations

from unittest import mock

import pytest

pytest.importorskip("airflow")

SECRET = "wJalrEXAMPLEKEY"


class Response:
    def __init__(self, data: bytes):
        self.data = data

    def read(self, amount):
        chunk, self.data = self.data[:amount], self.data[amount:]
        return chunk

    def close(self):
        pass


def tail_of(line: str) -> str:
    from spark_on_k8s.airflow.postmortem import _read_tail
    from spark_on_k8s.utils.run import WaitDeadline

    api = mock.Mock()
    api.read_namespaced_pod_log.return_value = Response(line.encode() + b"\n")
    lines, _ = _read_tail(api, "application-driver", "spark", "driver", 100, WaitDeadline(5))
    return "\n".join(lines)


def test_control_the_access_key_and_session_token_are_redacted():
    tail = tail_of(
        "+ exec /usr/bin/tini -s -- /opt/spark/bin/spark-submit --deploy-mode client "
        "--conf spark.hadoop.fs.s3a.access.key=AKIAEXAMPLE "
        "--conf spark.hadoop.fs.s3a.session.token=FQoGEXAMPLE"
    )
    assert "AKIAEXAMPLE" not in tail and "FQoGEXAMPLE" not in tail


@pytest.mark.parametrize(
    "line",
    [
        # The usual static S3A credential, as the driver receives it from spark_conf.
        f"+ exec /usr/bin/tini -s -- /opt/spark/bin/spark-submit "
        f"--conf spark.hadoop.fs.s3a.secret.key={SECRET} job.py",
        f"secret_key={SECRET}",
        f"secretKey: {SECRET}",
        # A printed mapping or a JSON log line.
        f'{{"password": "{SECRET}", "user": "etl"}}',
        f"{{'client_secret': '{SECRET}'}}",
    ],
    ids=["s3a-secret-key", "secret_key", "secretKey", "json", "python-dict"],
)
def test_secret_like_key_with_a_suffix_or_quotes_is_redacted(line):
    assert SECRET not in tail_of(line)
