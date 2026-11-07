# Installation

Choose the package release before following examples. **`develop` documents the
current `main` branch**, which can include features that are not yet on PyPI.
For a published package, choose its matching documentation in the version selector.
The Python package, API container and Helm chart have separate release versions.

## Published releases

Use a virtual environment with a Python version accepted by your selected release.
Check its metadata on [PyPI](https://pypi.org/project/spark-on-k8s/). For example,
create and activate an environment on macOS/Linux using that Python interpreter:

```bash
python -m venv .venv
source .venv/bin/activate
```

Then choose one of these installations:

```bash
python -m pip install spark-on-k8s # Python client, CLI and helpers
python -m pip install "spark-on-k8s[api]" # Also install the REST API and web UI
python -m pip install "spark-on-k8s[airflow]" # Also install the Airflow integration
```

These commands install from PyPI. They do not install this development branch or
the repository's example files. Availability of other extras and features depends
on the selected release. For an existing Airflow deployment, follow the
[Airflow guide](airflow.md) in the matching docs.

## Current source

The current source requires Python 3.11 or newer. The core client and API are
tested on Python 3.11–3.15. Airflow 3.4 is tested on Python 3.11–3.14; the legacy
Airflow 2.11.2 CI profile uses Python 3.12. Use Airflow 3 for new deployments.
The `airflow`, `airflow-ui` and `connect` extras require Python below 3.15.
Installing one of those extras on Python 3.15 does not provide its integration
dependencies.

The submitting machine does not need Spark or Java. The application container
supplies its own Spark, Java and, for Python jobs, Python runtime. Check the
[Spark compatibility guide](spark_compatibility.md) when selecting that image.
The optional [Spark Connect client](spark_connect.md) uses an existing server.

Clone the repository to obtain the source and example files:

```bash
git clone https://github.com/hussein-awala/spark-on-k8s.git
cd spark-on-k8s
```

With [uv](https://docs.astral.sh/uv/), **choose one row** below. Each command uses
the committed lockfile and creates `.venv` in the checkout. Python 3.12 is used
here so every listed optional integration has a supported interpreter.

| Functionality | Installation command |
| --- | --- |
| Python client, CLI and job files | `uv sync --python 3.12 --locked --no-default-groups` |
| SQL submission | `uv sync --python 3.12 --locked --no-default-groups --extra sql` |
| REST API and web console | `uv sync --python 3.12 --locked --no-default-groups --extra api` |
| Spark Connect companion | `uv sync --python 3.12 --locked --no-default-groups --extra connect` |
| Airflow integration | `uv sync --python 3.12 --locked --no-default-groups --extra airflow` |
| Airflow 3.4+ integration with its native task tab | `uv sync --python 3.12 --locked --no-default-groups --extra airflow-ui` |

Select multiple `--extra` options when needed, such as `--extra api --extra sql`.
`airflow-ui` already includes the Airflow integration. A later `uv sync` selects
a new profile and can remove unselected extras.

Run commands through that environment:

```bash
uv run --no-sync spark-on-k8s --help
```

To use the bare `spark-on-k8s` and `python` commands in the other guides, activate
`.venv` in your shell first. On macOS/Linux:

```bash
source .venv/bin/activate
```

Alternatively, use pip in an activated virtual environment with a supported
Python version. From the checkout, install `.` for core or select extras, for example:

```bash
python -m pip install ".[api,sql]"
```

This pip path resolves dependencies from the package metadata; it does not use
`uv.lock`. Use the uv profile above when you need the checkout's locked environment.

## Check the environment and continue

Using the selected environment, confirm the installed package and CLI are visible:

```bash
python -c 'from importlib.metadata import version; print(version("spark-on-k8s"))'
spark-on-k8s --help
```

These checks do not contact Kubernetes.

Continue with [Getting started](getting_started.md) to submit an existing
application, or choose another route in [Start here](start-here.md). Commands that
reference `examples/` run from the repository root. Contributor dependencies and
the separate Airflow 2.11 environment are in [Development and validation](../development.md).
