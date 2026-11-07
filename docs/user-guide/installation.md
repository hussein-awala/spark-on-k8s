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

You can also install the package from source with pip or Poetry 2.5.1:
```bash
# With pip
pip install . # For the core package
pip install ".[api]" # For the API package

# With poetry
poetry sync --only main # For the core package
poetry sync --only main -E api # For the API package
```
