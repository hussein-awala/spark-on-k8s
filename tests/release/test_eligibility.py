from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

COMMIT = "a" * 40
REPOSITORY = "example/spark-on-k8s"


def run(run_id=1, **overrides):
    return {
        "id": run_id,
        "head_sha": COMMIT,
        "event": "push",
        "head_repository": {"full_name": REPOSITORY},
        "status": "completed",
        "conclusion": "success",
        "html_url": f"https://github.com/{REPOSITORY}/actions/runs/{run_id}",
        **overrides,
    }


def api(runs, jobs=None):
    return SimpleNamespace(
        repository=REPOSITORY,
        runs=Mock(return_value=runs),
        jobs=Mock(return_value=jobs if jobs is not None else [{"name": "runtime", "conclusion": "success"}]),
    )


@pytest.mark.parametrize("version", ["0.13.0", "1.0.1", "2.30.400"])
def test_stable_release_tags(release_modules, version):
    assert release_modules["eligibility"].validate_version(version) is False


@pytest.mark.parametrize("version", ["0.13.0a1", "0.13.0b2", "0.13.0rc1"])
def test_prerelease_tags(release_modules, version):
    assert release_modules["eligibility"].validate_version(version) is True


@pytest.mark.parametrize(
    "version", ["v1.0.0", "01.0.0", "1.0", "1.0.0-dev", "1.0.0\n", "latest", "1.0.0+local"]
)
def test_reject_ambiguous_or_nonrelease_tags(release_modules, version):
    with pytest.raises(ValueError, match="bare"):
        release_modules["eligibility"].validate_version(version)


def test_accept_exact_commit_and_executed_jobs(release_modules):
    client = api([run()])
    result = release_modules["eligibility"].inspect_workflow(client, "ci.yml", COMMIT)
    assert result["state"] == "passed"
    assert result["jobs"] == ["runtime"]
    client.runs.assert_called_once_with("ci.yml", COMMIT)
    client.jobs.assert_called_once_with(1)


def test_other_commit_fork_and_pull_request_cannot_qualify(release_modules):
    client = api(
        [
            run(1, head_sha="b" * 40),
            run(2, event="pull_request"),
            run(3, head_repository={"full_name": "fork/spark-on-k8s"}),
        ]
    )
    result = release_modules["eligibility"].inspect_workflow(client, "ci.yml", COMMIT)
    assert result["state"] == "missing"
    client.jobs.assert_not_called()


def test_manual_qualification_is_allowed(release_modules):
    client = api([run(event="workflow_dispatch")])
    assert release_modules["eligibility"].inspect_workflow(client, "ci.yml", COMMIT)["state"] == "passed"


def test_latest_failed_attempt_cannot_reuse_older_success(release_modules):
    client = api([run(1), run(2, conclusion="failure")])
    with pytest.raises(ValueError, match="concluded failure"):
        release_modules["eligibility"].inspect_workflow(client, "ci.yml", COMMIT)


@pytest.mark.parametrize(
    "jobs", [[], [{"name": "core", "conclusion": "skipped"}], [{"name": "core", "conclusion": "cancelled"}]]
)
def test_skipped_empty_or_cancelled_execution_fails_closed(release_modules, jobs):
    with pytest.raises(ValueError):
        release_modules["eligibility"].inspect_workflow(api([run()], jobs), "ci.yml", COMMIT)


def test_pending_waits_then_passes_all_four_workflows(release_modules, monkeypatch):
    module = release_modules["eligibility"]
    client = api([run()])
    client.runs.side_effect = [[run(status="in_progress")], [run()], [run()], [run()], *[[run()]] * 4]
    clock = {"now": 0.0}
    monkeypatch.setattr(module.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: clock.update(now=clock["now"] + seconds))
    results = module.wait_for_checks(client, COMMIT, 10, 2)
    assert len(results) == 4
    assert clock["now"] == 2


def test_missing_checks_have_finite_deadline(release_modules, monkeypatch):
    module = release_modules["eligibility"]
    clock = {"now": 0.0}
    monkeypatch.setattr(module.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: clock.update(now=clock["now"] + seconds))
    with pytest.raises(TimeoutError, match="missing or unfinished"):
        module.wait_for_checks(api([]), COMMIT, 3, 20)
    assert clock["now"] == 3


@pytest.mark.parametrize(
    ("version", "latest", "minor"),
    [("0.13.1", True, True), ("0.12.2", False, True), ("0.12.0", False, False), ("0.14.0rc1", False, False)],
)
def test_release_channels_never_regress(release_modules, version, latest, minor):
    tags = [{"name": name} for name in ("0.12.1", "0.13.0", "0.14.0rc1", "unrelated")]
    assert release_modules["eligibility"].channel_promotions(version, tags) == {
        "promote_latest": latest,
        "promote_minor": minor,
    }


def test_first_stable_release_can_promote(release_modules):
    assert release_modules["eligibility"].channel_promotions("0.1.0", []) == {
        "promote_latest": True,
        "promote_minor": True,
    }
