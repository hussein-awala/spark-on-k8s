This development version requires Python 3.11 or newer. The core client and API
are tested on Python 3.11–3.15. The Airflow integration supports Airflow 3.4 on Python
3.11–3.14 and Airflow 2.11.2 on Python 3.11–3.12.
Airflow 2 is end-of-life and retains dependency versions with known security
advisories; new deployments should use Airflow 3. The `airflow` extra
is not supported on Python 3.15 because Airflow does not support that version yet.

To install the core python package (only the Python client, the CLI and the helpers), run:
```bash
pip install spark-on-k8s
```
If you want to use the REST API and the web UI, you will also need to install the api package:
```bash
pip install "spark-on-k8s[api]"
```

For Airflow, install the extra in an environment with a supported Python version:
```bash
pip install "spark-on-k8s[airflow]"
```

From a source checkout, use pip or [uv](https://docs.astral.sh/uv/):
```bash
# With pip, inside a virtual environment
python -m pip install . # Core package
python -m pip install ".[api]" # API and web UI
python -m pip install ".[airflow]" # Airflow integration on a supported Python version

# With uv, using the committed lockfile and a project-local virtual environment
uv sync --locked --no-default-groups # Core package
uv sync --locked --no-default-groups --extra api # API and web UI
uv sync --python 3.12 --locked --no-default-groups --extra airflow # Airflow 3 integration
```
Run commands from the selected uv environment with `uv run --no-sync spark-on-k8s`.
See [CONTRIBUTING.md](https://github.com/hussein-awala/spark-on-k8s/blob/main/CONTRIBUTING.md)
for development dependencies and the separate Airflow 2.11 compatibility environment.
