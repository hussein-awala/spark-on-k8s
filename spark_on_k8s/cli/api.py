from __future__ import annotations

import click

from spark_on_k8s.api.configuration import APIConfiguration


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
def start(
    host: str,
    port: int,
    workers: int,
    log_level: str,
    limit_concurrency: int,
):
    try:
        import uvicorn
    except ImportError:
        raise ImportError("Please install API dependencies with `pip install spark-on-k8s[api]`.") from None

    uvicorn.run(
        "spark_on_k8s.api.main:app",
        host=host,
        port=port,
        log_level=log_level,
        workers=workers,
        limit_concurrency=limit_concurrency,
    )
