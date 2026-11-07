"""Require successful, executed workflow jobs for one exact release commit."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

REQUIRED_WORKFLOWS = (
    "ci.yml",
    "spark-compatibility.yml",
    "spark-connect.yml",
    "airflow-kubernetes.yml",
)
VERSION_PATTERN = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:(a|b|rc)(0|[1-9]\d*))?")


def validate_version(value: str) -> bool:
    """Return whether the bare numeric release version is a prerelease."""
    match = VERSION_PATTERN.fullmatch(value)
    if not match:
        raise ValueError("Use a bare X.Y.Z tag, optionally followed by aN, bN or rcN")
    return match.group(4) is not None


class GitHub:
    def __init__(self, repository: str, token: str, deadline: float):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("Expected an owner/repository name")
        if not token:
            raise ValueError("GH_TOKEN is required for release eligibility")
        self.repository = repository
        self.token = token
        self.deadline = deadline

    def get_pages(self, endpoint: str, key: str | None, **query) -> list[dict]:
        results = []
        for page in range(1, 101):
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Release qualification deadline expired")
            url = f"https://api.github.com/repos/{self.repository}/{endpoint}?"
            url += urlencode({**query, "per_page": 100, "page": page})
            request = Request(
                url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            with urlopen(request, timeout=min(30, remaining)) as response:
                payload = json.load(response)
            items = payload[key] if key else payload
            if not isinstance(items, list):
                raise ValueError(f"Unexpected GitHub {key} response")
            results.extend(items)
            if len(items) < 100:
                return results
        raise ValueError("GitHub pagination exceeded the release check bound")

    def runs(self, workflow: str, commit: str) -> list[dict]:
        return self.get_pages(f"actions/workflows/{workflow}/runs", "workflow_runs", head_sha=commit)

    def jobs(self, run_id: int) -> list[dict]:
        return self.get_pages(f"actions/runs/{run_id}/jobs", "jobs", filter="latest")

    def tags(self) -> list[dict]:
        return self.get_pages("tags", None)


def channel_promotions(version: str, tags: list[dict]) -> dict[str, bool]:
    if validate_version(version):
        return {"promote_latest": False, "promote_minor": False}
    candidate = tuple(map(int, version.split(".")))
    stable = []
    for tag in tags:
        match = VERSION_PATTERN.fullmatch(tag["name"])
        if match and match.group(4) is None:
            stable.append(tuple(map(int, match.group(1, 2, 3))))
    return {
        "promote_latest": all(candidate >= published for published in stable),
        "promote_minor": all(
            candidate >= published for published in stable if published[:2] == candidate[:2]
        ),
    }


def inspect_workflow(api, workflow: str, commit: str) -> dict:
    candidates = [
        run
        for run in api.runs(workflow, commit)
        if run.get("head_sha") == commit
        and run.get("event") in {"push", "workflow_dispatch"}
        and (run.get("head_repository") or {}).get("full_name") == api.repository
    ]
    if not candidates:
        return {"workflow": workflow, "state": "missing"}
    run = max(candidates, key=lambda candidate: candidate["id"])
    evidence = {"workflow": workflow, "run_id": run["id"], "url": run["html_url"]}
    if run["status"] != "completed":
        return {**evidence, "state": "pending"}
    if run.get("conclusion") != "success":
        raise ValueError(f"{workflow}: latest qualifying run concluded {run.get('conclusion')}")
    jobs = api.jobs(run["id"])
    if not jobs:
        raise ValueError(f"{workflow}: successful run contains no jobs")
    invalid = [job["name"] for job in jobs if job.get("conclusion") != "success"]
    if invalid:
        raise ValueError(f"{workflow}: required execution is not successful for {', '.join(invalid)}")
    return {**evidence, "state": "passed", "jobs": [job["name"] for job in jobs]}


def wait_for_checks(api, commit: str, deadline: float, interval: float) -> list[dict]:
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("Required exact-commit workflow evidence is missing or unfinished")
        results = [inspect_workflow(api, workflow, commit) for workflow in REQUIRED_WORKFLOWS]
        if all(result["state"] == "passed" for result in results):
            return results
        pending = ", ".join(f"{result['workflow']}={result['state']}" for result in results)
        print(f"Waiting for release eligibility: {pending}", flush=True)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Required exact-commit workflow evidence is missing or unfinished")
        time.sleep(min(interval, remaining))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    try:
        prerelease = validate_version(args.version)
        if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
            raise ValueError("Release commit must be a full lowercase Git SHA")
        if not 1 <= args.timeout_seconds <= 3600 or not 1 <= args.poll_seconds <= 60:
            raise ValueError("Timeout must be 1–3600 seconds and polling 1–60 seconds")
        deadline = time.monotonic() + args.timeout_seconds
        api = GitHub(args.repository, os.environ.get("GH_TOKEN", ""), deadline)
        workflows = wait_for_checks(api, args.commit, deadline, args.poll_seconds)
        channels = channel_promotions(args.version, api.tags())
        payload = {
            "schema_version": 1,
            "repository": args.repository,
            "commit": args.commit,
            "version": args.version,
            "prerelease": prerelease,
            **channels,
            "workflows": workflows,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n")
        if args.github_output:
            with args.github_output.open("a") as output:
                output.write(f"commit={args.commit}\nversion={args.version}\n")
                output.write(f"prerelease={str(prerelease).lower()}\n")
                for name, enabled in channels.items():
                    output.write(f"{name}={str(enabled).lower()}\n")
        return 0
    except (ValueError, KeyError, TimeoutError, HTTPError, URLError, OSError) as error:
        print(f"Release is not eligible: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
