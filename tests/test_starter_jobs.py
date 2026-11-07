from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest
import yaml
from spark_on_k8s.jobs import JobSpec


@pytest.mark.parametrize("starter", ["etl", "lakehouse"])
def test_starter_job_plans_share_the_same_storage(root_path, starter, monkeypatch, tmp_path):
    root = Path(root_path) / "examples/starters"
    # Paths to templates belong to the job file, independent of the caller's cwd.
    monkeypatch.chdir(tmp_path)
    job = JobSpec.from_file(root / starter / "job.yaml")
    with mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        plan = job.plan()
    connect.assert_not_called()
    assert plan.namespace == "spark"
    assert job.spec["executor_instances"] == {"initial": 1}
    assert job.spec["app_waiter"] == "log"
    driver = next(resource for resource in plan.resources if resource["kind"] == "Pod")
    assert driver["spec"]["securityContext"]["fsGroup"] == 185
    assert driver["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] == "spark-starter-data"
    assert {mount["mountPath"] for mount in driver["spec"]["containers"][0]["volumeMounts"]} >= {"/data"}
    template = next(resource for resource in plan.resources if resource["kind"] == "ConfigMap")
    executor = yaml.safe_load(template["data"]["executor-template.yaml"])
    assert executor["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] == "spark-starter-data"
    assert executor["spec"]["containers"][0]["volumeMounts"][0]["mountPath"] == "/data"
    if starter == "lakehouse":
        assert job.spec["image"] == "starter-lakehouse:4.0.5"
        assert job.spec["spark_conf"]["spark.sql.catalog.lakehouse.warehouse"] == "file:///data/lakehouse"


def test_starter_storage_claim_requires_shared_filesystem(root_path):
    storage = yaml.safe_load((Path(root_path) / "examples/starters/storage.yaml").read_text())
    assert storage["metadata"] == {"name": "spark-starter-data", "namespace": "spark"}
    assert storage["spec"]["accessModes"] == ["ReadWriteMany"]
