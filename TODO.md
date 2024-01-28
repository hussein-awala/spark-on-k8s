# Spark On Kubernetes - todo list

This is a list of things that we will work on in the future. If you want to contribute, please open an issue or a PR.

## Python Client

- [ ] Support more Spark configuration options
- [ ] Simplify setup to work with cloud provider services (e.g. S3, GCS, Glue, etc.)
- [ ] Support creating of K8S ephemeral secrets to store and use credentials, configmaps for env vars, etc.
- [ ] Support configuring Kubernetes client from cloud provider credentials
(e.g. AWS access/secret keys, GCP service account, etc.)

## API / Web UI

- [ ] Improve the UI to be more user-friendly and add more features
- [ ] Support managing the Spark application from the UI and the API (e.g. kill, delete, submit, etc.)
- [ ] Support authentication and RBAC for the API and the UI (e.g. only allow certain users to submit apps, etc.)

## CLI

- [ ] Support all the added features in the Python client
- [ ] Support the REST API as a backend for the CLI (instead of using the Kubernetes client directly)

## Airflow Operator

- [ ] Support all the added features in the Python client
- [ ] Adopting the existing Spark application after a failure (e.g. when the Airflow worker pod is restarted)
- [ ] Find a way to read the logs from the trigger instead of waiting for the app to finish
- [ ] Create an operator extra link for the Spark UI when the reverse proxy is enabled
(and maybe the spark history server)

## Other

- [ ] Improve the documentation
- [ ] Improve the test coverage
- [ ] Add integration tests
- [ ] Add more examples
