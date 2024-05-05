To install the core python package (only the Python client, the CLI and the helpers), run:
```bash
pip install spark-on-k8s
```
If you want to use the REST API and the web UI, you will also need to install the api package:
```bash
pip install spark-on-k8s[api]
```

You can also install the package from source with pip or poetry:
```bash
# With pip
pip install . # For the core package
pip install ".[api]" # For the API package

# With poetry
poetry install # For the core package
poetry install -E api # For the API package
```
