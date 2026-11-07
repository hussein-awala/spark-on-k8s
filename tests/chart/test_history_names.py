from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HELM = shutil.which("helm")
CHART = Path(__file__).resolve().parents[2] / "chart"
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is required to render the chart")


@pytest.mark.parametrize(
    "settings",
    [[], ["fullnameOverride=" + "a" * 63], ["nameOverride=" + "a" * 63]],
    ids=["default", "long-fullname", "long-chart-name"],
)
def test_history_resources_have_distinct_valid_names_and_selectors(settings):
    command = [HELM, "template", "test", str(CHART), "--set", "sparkHistory.enabled=true"]
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    resources = list(yaml.safe_load_all(result.stdout))
    identities = [(item["kind"], item["metadata"]["name"]) for item in resources]
    assert len(identities) == len(set(identities)), "API and history resource names must not collide"
    deployments = [item for item in resources if item["kind"] == "Deployment"]
    services = [item for item in resources if item["kind"] == "Service"]
    assert len(deployments) == len(services) == 2
    for deployment in deployments:
        assert len(deployment["metadata"]["name"]) <= 63
        labels = deployment["spec"]["template"]["metadata"]["labels"]
        assert all(len(str(value)) <= 63 for value in labels.values())
        assert deployment["spec"]["selector"]["matchLabels"].items() <= labels.items()
        matched_services = [
            service for service in services if service["spec"]["selector"].items() <= labels.items()
        ]
        assert len(matched_services) == 1
        assert len(matched_services[0]["metadata"]["name"]) <= 63
    history = next(
        item
        for item in deployments
        if item["spec"]["template"]["spec"]["containers"][0]["name"] == "spark-history"
    )
    assert history["metadata"]["name"].endswith("-history")
    if not settings:
        assert history["metadata"]["name"] == "test-spark-on-k8s-history"
