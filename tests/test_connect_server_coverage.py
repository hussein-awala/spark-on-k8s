from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from spark_on_k8s.cli.receipts import receipt_writer
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.connect_server import ConnectServerConfig, ConnectServerCreateError, ConnectServerHandle
from spark_on_k8s.utils.run import RunHandle


def test_failed_new_submission_cannot_roll_back_a_previous_run(monkeypatch):
    previous = RunHandle(
        namespace="previous", app_id="old-app", driver_pod="old-app-driver", driver_uid="old-driver-uid"
    )
    manager = MagicMock()
    client = SparkOnK8S(k8s_client_manager=manager)
    client.last_submitted_run = previous
    # An adapter can fail before delegating to submit_app and resetting the client's convenience state.
    failure = ConnectionError("submission adapter disconnected")
    monkeypatch.setattr(client, "submit_run", MagicMock(side_effect=failure))
    cancel = MagicMock()
    monkeypatch.setattr(client, "cancel_run", cancel)
    config = ConnectServerConfig(
        image="spark:test",
        server_jar="local:///opt/spark/jars/connect.jar",
        namespace="new-server",
        service_account="spark",
    )
    with pytest.raises(ConnectServerCreateError) as raised:
        client.create_connect_server(config)
    error = raised.value
    assert error.__cause__ is failure
    assert error.run.namespace == "new-server"
    assert error.run.driver_pod != previous.driver_pod
    assert error.run.driver_uid is None
    assert error.cleanup["status"] == "not_attempted"
    assert client.last_submitted_run == previous
    cancel.assert_not_called()
    manager.client.assert_not_called()


@pytest.mark.parametrize("run", [None, {"namespace": "spark", "driver_uid": "uid"}])
def test_server_handle_requires_typed_run_before_becoming_a_deletion_identity(run):
    with pytest.raises(ValueError, match="run must be a RunHandle"):
        ConnectServerHandle(
            run=run,
            service_name="server-connect",
            service_uid="service-uid",
            driver_service_uid="driver-service-uid",
            port=15002,
        )


def test_unsupported_receipt_schema_cannot_create_a_file_or_enter_submission(tmp_path):
    destination = tmp_path / "receipt.json"
    with pytest.raises(ValueError, match="Unsupported receipt field"):
        with receipt_writer(destination, field="unrecognized-server-schema"):
            pytest.fail("An unsupported receipt schema must fail before submission can start")
    assert not list(tmp_path.iterdir())
