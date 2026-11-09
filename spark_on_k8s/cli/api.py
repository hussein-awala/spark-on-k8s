from __future__ import annotations

import click
from click.core import ParameterSource

from spark_on_k8s.api.configuration import APIConfiguration


def _validate_root_path(ctx: click.Context, param: click.Parameter, value: str) -> str:
    # Uvicorn prepends this prefix to every request path and encodes it as ASCII, so accept
    # only a clean absolute path rather than starting a server whose routes cannot match.
    if value and not (
        value.startswith("/")
        and not value.endswith("/")
        and "//" not in value
        and value.isascii()
        and value.isprintable()
        and not any(character in value for character in " ?#")
    ):
        origin = (
            " (from SPARK_ON_K8S_API_ROOT_PATH)"
            if ctx.get_parameter_source(param.name) is ParameterSource.DEFAULT
            else ""
        )
        raise click.BadParameter(
            f"{value!r}{origin} must be empty or a URL path such as /platform/spark: "
            "it starts with '/' and has no trailing '/', empty segment, spaces, query or fragment.",
            ctx=ctx,
            param=param,
        )
    return value


@click.group(name="api", help="Manage the spark-on-k8s API.")
def api_cli():
    pass


@api_cli.command(
    help="Start the spark-on-k8s API.",
)
@click.option(
    "--host",
    default=APIConfiguration.SPARK_ON_K8S_API_HOST,
    type=str,
    show_default=True,
    help="The host to bind to.",
)
@click.option(
    "--port",
    default=APIConfiguration.SPARK_ON_K8S_API_PORT,
    type=int,
    show_default=True,
    help="The port to bind to.",
)
@click.option(
    "--workers",
    default=APIConfiguration.SPARK_ON_K8S_API_WORKERS,
    type=int,
    show_default=True,
    help="The number of workers.",
)
@click.option(
    "--log-level",
    default=APIConfiguration.SPARK_ON_K8S_API_LOG_LEVEL,
    show_default=True,
    type=click.Choice(["critical", "error", "warning", "info", "debug"]),
    help="The log level.",
)
@click.option(
    "--limit-concurrency",
    default=APIConfiguration.SPARK_ON_K8S_API_LIMIT_CONCURRENCY,
    type=int,
    show_default=True,
    help="The maximum number of concurrent connections.",
)
@click.option(
    "--root-path",
    default=APIConfiguration.SPARK_ON_K8S_API_ROOT_PATH,
    type=str,
    callback=_validate_root_path,
    help=(
        "The public URL path prefix, such as /platform/spark, when a gateway serves the API "
        "below a path and removes that prefix before forwarding requests. "
        "Defaults to SPARK_ON_K8S_API_ROOT_PATH; empty serves the API at /."
    ),
)
def start(
    host: str,
    port: int,
    workers: int,
    log_level: str,
    limit_concurrency: int,
    root_path: str,
):
    try:
        import uvicorn
    except ImportError:
        raise click.ClickException(
            "The API requires the 'api' extra: pip install 'spark-on-k8s[api]'"
        ) from None

    uvicorn.run(
        "spark_on_k8s.api.main:app",
        host=host,
        port=port,
        log_level=log_level,
        workers=workers,
        limit_concurrency=limit_concurrency,
        root_path=root_path,
    )
