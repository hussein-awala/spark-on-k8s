from __future__ import annotations

import click

from spark_on_k8s.cli.options import namespace_option


@click.group(name="namespace", help="Manage Spark applications namespace.")
def namespace_cli():
    pass


@namespace_cli.command(params=[namespace_option], help="Setup a namespace for Spark applications.")
def setup(namespace: str):
    from spark_on_k8s.utils.setup_namespace import SparkOnK8SNamespaceSetup

    spark_on_k8s_setuper = SparkOnK8SNamespaceSetup()
    spark_on_k8s_setuper.setup_namespace(namespace=namespace, should_print=True)
