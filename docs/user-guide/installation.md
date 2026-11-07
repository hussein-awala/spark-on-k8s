## Installation

### Published releases

The following commands install a published package from PyPI. Choose the
extra for the functionality you need, inside a virtual environment:

```bash
python -m pip install spark-on-k8s # Python client, CLI and helpers
python -m pip install "spark-on-k8s[api]" # Also install the REST API and web UI
python -m pip install "spark-on-k8s[airflow]" # Also install the Airflow integration
```

Published releases can have different Python requirements and features from the
unreleased `main` branch. Check the selected release's metadata on
[PyPI](https://pypi.org/project/spark-on-k8s/) and its version of the documentation.
The source requirements and examples below describe `main`; installing from PyPI
does not install unreleased changes.

### Current source

The current source requires Python 3.11 or newer. The core client and API are
tested on Python 3.11–3.15. The Airflow integration is tested with Airflow 3.4 on
Python 3.11–3.14 and Airflow 2.11.2 on Python 3.11–3.12. Airflow 2 is end-of-life
and retains dependency versions with known security advisories; use Airflow 3 for
new deployments. The optional `airflow-ui` extra adds a native task tab for
Airflow 3.4 or newer, below Airflow 4. The `airflow` and `airflow-ui` extras are
not supported on Python 3.15.

Clone the repository to use the source instructions and example files in this guide:

```bash
git clone https://github.com/hussein-awala/spark-on-k8s.git
cd spark-on-k8s
```

With [uv](https://docs.astral.sh/uv/), choose one environment profile. These commands
use the committed lockfile and create a project-local `.venv`:

```bash
uv sync --locked --no-default-groups # Core package
uv sync --locked --no-default-groups --extra api # API and web UI
uv sync --python 3.12 --locked --no-default-groups --extra airflow # Airflow 3 integration
uv sync --python 3.12 --locked --no-default-groups --extra airflow-ui # Also install the native task tab
```

Run commands from the selected environment with `uv run --no-sync spark-on-k8s`,
or activate it with `source .venv/bin/activate` before using the commands below.
To install the API and Airflow integration together, select `--extra api --extra airflow`,
or `--extra api --extra airflow-ui` to include the task tab. A later `uv sync`
selects a new profile and can remove unselected extras.

Alternatively, install from the checkout with pip in an activated virtual environment:

```bash
python -m pip install . # Core package
python -m pip install ".[api]" # API and web UI
python -m pip install ".[airflow]" # Airflow integration
python -m pip install ".[airflow-ui]" # Airflow 3.4+ integration and native task tab
```

See [CONTRIBUTING.md](https://github.com/hussein-awala/spark-on-k8s/blob/main/CONTRIBUTING.md)
for development dependencies and the separate Airflow 2.11 compatibility environment.
See the [Airflow guide](airflow.md) for provider installation, connections and DAG examples.
