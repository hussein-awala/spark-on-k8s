from __future__ import annotations

import json
import math
import sys
from contextlib import contextmanager, nullcontext, redirect_stdout

import click
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import MaxRetryError, NewConnectionError, TimeoutError as HTTPTimeoutError

from spark_on_k8s.utils.spark_app_status import SparkAppStatus

output_option = click.option(
    "--output", type=click.Choice(["text", "json"]), default="text", show_default=True
)


def validate_timeout(ctx, param, value):
    if value is not None and not math.isfinite(value):
        raise click.BadParameter("Timeout must be a positive finite number of seconds")
    return value


timeout_option = click.option(
    "--timeout",
    type=click.FloatRange(min=0, min_open=True),
    callback=validate_timeout,
    default=None,
    help="Maximum seconds to wait after submission (does not stop or delete the application).",
)


def is_transport_timeout(error: BaseException) -> bool:
    """Kubernetes request timeouts arrive wrapped in MaxRetryError; refused connections are not timeouts."""
    reason = error.reason if isinstance(error, MaxRetryError) else error
    return isinstance(reason, HTTPTimeoutError) and not isinstance(reason, NewConnectionError)


def outcome_code(status: SparkAppStatus) -> int:
    if status == SparkAppStatus.Succeeded:
        return 0
    return 3 if status == SparkAppStatus.Failed else 6


@contextmanager
def execution(operation: str, namespace: str, output: str):
    """Keep JSON stdout to one versioned document, including runtime failures."""
    result = {"schema_version": 1, "operation": operation, "namespace": namespace, "error": None}
    code = 0
    try:
        with redirect_stdout(sys.stderr) if output == "json" else nullcontext():
            yield result
    except KeyboardInterrupt:
        code = 130
        result["error"] = {
            "code": "interrupted",
            "message": (
                "Interrupted; inspect the reported resource identity and cleanup outcome."
                if operation.startswith("connect_server_")
                else "Interrupted; the application was not stopped."
            ),
        }
    except (TimeoutError, HTTPTimeoutError, MaxRetryError) as error:
        timed_out = isinstance(error, TimeoutError) or is_transport_timeout(error)
        code = 5 if timed_out else 1
        result["error"] = {"code": "timeout" if timed_out else "operation_error", "message": str(error)}
    except ApiException as error:
        code = 4 if error.status == 404 else 1
        result["error"] = {
            "code": "not_found" if code == 4 else "api_error",
            # Do not serialize Kubernetes response bodies, which can contain submitted secrets.
            "message": f"Kubernetes API {error.status}: {error.reason or 'request failed'}",
        }
    except Exception as error:
        code = 1
        result["error"] = {"code": "operation_error", "message": str(error)}
    else:
        if "status" in result and operation in {"wait", "submit"} and result["status"] is not None:
            code = outcome_code(result["status"])
    if output == "json":
        click.echo(json.dumps(result, ensure_ascii=False))
    elif result["error"]:
        click.echo(f"Error: {result['error']['message']}", err=True)
    if code:
        raise click.exceptions.Exit(code)


def execute_submission(
    spark_client, kwargs: dict, output: str = "text", timeout: float | None = None, receipt_file=None
):
    """Submit and observe a run using the CLI v1 output and exit-code contract.

    kwargs accepts submit_app arguments, including app_waiter. The mapping is copied.
    A timeout limits observation after submission, never deletes the application.
    """
    from spark_on_k8s.cli.receipts import receipt_writer
    from spark_on_k8s.utils.configuration import Configuration
    from spark_on_k8s.utils.run import WaitDeadline

    options = dict(kwargs)
    waiter = options.pop("app_waiter", Configuration.SPARK_ON_K8S_APP_WAITER)
    namespace = options.get("namespace", Configuration.SPARK_ON_K8S_NAMESPACE)
    if timeout is not None and waiter not in {"wait", "log"}:
        raise click.UsageError("--timeout requires a wait or log waiter")
    try:
        WaitDeadline(timeout)
    except ValueError as error:
        raise click.BadParameter(str(error), param_hint="--timeout") from error
    options["app_waiter"] = "no_wait"
    options["should_print"] = True
    with execution("submit", namespace, output) as result:
        result["run"] = None
        result["status"] = None
        with receipt_writer(receipt_file) as save_receipt:
            try:
                spark_client.submit_app(**options)
            finally:
                submission_failed = sys.exc_info()[0] is not None
                run = spark_client.last_submitted_run
                phase = getattr(spark_client, "last_submission_phase", None)
                if isinstance(phase, str):
                    result["submission_phase"] = phase
                if run is not None:
                    result["run"] = run.to_dict()
                    if submission_failed and output == "text":
                        click.echo(
                            f"Driver created: {run.driver_pod} in {run.namespace} "
                            f"(application {run.app_id}); submission did not complete.",
                            err=True,
                        )
                    try:
                        # A partial submission with a confirmed driver is also recoverable evidence.
                        save_receipt(run)
                    except OSError as error:
                        result["receipt_error"] = {"code": "receipt_write_error", "message": str(error)}
                        if output == "text":
                            click.echo(
                                f"Could not save receipt for {run.driver_pod} in {run.namespace} "
                                f"(application {run.app_id}).",
                                err=True,
                            )
                        if not submission_failed:
                            raise
            if run is None:
                raise RuntimeError("Submission completed without a run identity")
        if output == "text" and run is not None:
            click.echo(f"Submitted {run.app_id} in {run.namespace} (driver {run.driver_pod})")
        if waiter in {"wait", "log"}:
            deadline = WaitDeadline(timeout)
            target = {"namespace": namespace, "pod_name": run.driver_pod}
            startup_timeout = options.get("startup_timeout", Configuration.SPARK_ON_K8S_STARTUP_TIMEOUT)
            startup = {"startup_timeout": startup_timeout} if startup_timeout else {}
            if waiter == "log":
                spark_client.app_manager.stream_logs(
                    **target,
                    **startup,
                    should_print=True,
                    **({"timeout": deadline.remaining()} if timeout is not None else {}),
                )
            spark_client.app_manager.wait_for_app(
                **target,
                **startup,
                should_print=True,
                **({"timeout": deadline.remaining()} if timeout is not None else {}),
            )
            result["status"] = spark_client.app_manager.app_status(
                **target,
                **({"request_timeout": deadline.remaining()} if timeout is not None else {}),
            )
            deadline.remaining()
            if output == "text":
                click.echo(f"App {run.app_id} has terminated with status {result['status'].value}")
