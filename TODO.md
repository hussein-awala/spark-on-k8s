# Spark On Kubernetes - todo list

This is a list of things that we will work on in the future. If you want to contribute, please open an issue or a PR.

## Python Client

- [ ] Support more Spark configuration options
- [ ] Simplify setup to work with cloud provider services (e.g. S3, GCS, Glue, etc.)
- [x] Create ephemeral Kubernetes Secrets for application environment variables.
- [ ] Support ConfigMaps as sources of environment variables.
- [ ] Support configuring Kubernetes client from cloud provider credentials
(e.g. AWS access/secret keys, GCP service account, etc.)

## API / Web UI

- [ ] Improve the UI to be more user-friendly and add more features
- [x] Kill and delete applications through the API and web UI.
- [ ] Submit applications through the API and web UI.
- [ ] Support authentication and RBAC for the API and the UI (e.g. only allow certain users to submit apps, etc.)

## CLI

- [ ] Support all the added features in the Python client
- [ ] Support the REST API as a backend for the CLI (instead of using the Kubernetes client directly)

## Airflow Operator

- [ ] Support all the added features in the Python client
- [x] Adopt Pending or Running applications using the namespace and driver pod stored in XCom.
- [ ] Find a way to read the logs from the trigger instead of waiting for the app to finish
- [x] Provide operator extra links to the Spark UI and Spark History Server through the API.

## CI compatibility follow-up

- [x] Replace temporary runner and tooling pins, refresh dependencies, and validate
  Python 3.11–3.15 on Linux and macOS ARM64 with separate Airflow 3 and 2.11 checks.
- [x] Replace the package manager and build backend with uv, preserving installed
  Airflow provider discovery and Spark UI links.
- [ ] Migrate the external Snyk dependency project from Poetry to a scan that
  includes uv extras and dependency groups.

## Other

- [ ] Improve the documentation
- [ ] Improve the test coverage
- [ ] Add integration tests
- [ ] Add more examples
