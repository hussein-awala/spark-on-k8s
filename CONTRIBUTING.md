# Contributing Guidelines

We welcome contributions from the community and are happy to have them.
Please follow this guide when logging issues or making changes.

## Installation

To install the package, you can check [this documentation](README.md#installation).

## Testing

Use Python 3.11+ and Poetry 2.5.1. The lockfile separates test, lint, and documentation
dependencies so contributors and CI can install the tools they need.

For the core client and API tests:

```bash
poetry sync --only main,test -E api
poetry run pytest --ignore=tests/airflow
```

For the complete suite, use Python 3.11–3.14 and install both extras:

```bash
poetry sync --only main,test --all-extras
poetry run pytest
```

Install Helm and Node.js to run the chart-rendering and browser-log regression tests.
Airflow-specific CI jobs reject skipped tests so missing dependencies cannot
silently reduce coverage.

To run code checks and build documentation:

```bash
poetry sync --with lint,docs --all-extras
poetry run ruff check .
poetry run ruff format --check .
poetry run helm-mkdocs chart -o docs/helm-chart
poetry run mkdocs build --strict
poetry build
```

The Airflow compatibility workflow runs separate checks for Airflow 3.4.0 and
Airflow 2.11.2. Both checks require collected tests and reject skipped tests.

## Reporting a bug

If you find a bug in this project, you can report it by opening an issue via this
[link](https://github.com/hussein-awala/spark-on-k8s/issues/new) and providing all the
information can help reproduce it.
If you already have a fix for the bug, you can skip creating an issue and submit a PR to fix it.

**Warning**: if the bug is a security vulnerability, please follow
[this documentation](SECURITY.md#reporting-a-vulnerability) to report it.

## Questions

If you have any question, a new idea, or anything to discuss, you can start a new discussion
via this [link](https://github.com/hussein-awala/spark-on-k8s/discussions/new/choose).
