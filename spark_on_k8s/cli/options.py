from __future__ import annotations

import click

namespace_option = click.Option(
    ("-n", "--namespace"),
    type=str,
    default="default",
    show_default=True,
    help="The namespace to operate on.",
)

app_id_option = click.Option(("--app-id",), type=str, required=True, help="The ID of the app to operate on.")

force_option = click.Option(
    ("-f", "--force"), is_flag=True, default=False, show_default=True, help="Force the operation."
)


# Submit options
def validate_spark_conf(ctx, param, value):
    spark_conf = {}
    for conf in value:
        try:
            key, val = conf.split("=")
            spark_conf[key] = val
        except ValueError:
            raise click.BadParameter("Spark conf parameter must be in the form key=value.") from None


docker_image_option = click.Option(
    ("--image",),
    type=str,
    required=True,
    help="The docker image to use for the app.",
)
app_path_option = click.Option(
    ("--path",),
    type=str,
    required=True,
    help="The path to the app to submit.",
)
service_account_option = click.Option(
    ("--service-account",),
    type=str,
    default="spark",
    show_default=True,
    help="The service account to use for the app.",
)
app_name_option = click.Option(
    ("--name",),
    type=str,
    required=None,
    help="The name of the app.",
)
spark_conf_option = click.Option(
    ("--conf", "spark_conf"),
    type=str,
    multiple=True,
    callback=validate_spark_conf,
    help="Spark configuration property in key=value format. Can be repeated.",
)
class_name_option = click.Option(
    ("--class", "class_name"),
    type=str,
    default=None,
    show_default=True,
    help="The main class for the app.",
)
wait_option = click.Option(
    ("--wait",),
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="Wait for the app to finish.",
)
logs_option = click.Option(
    ("--logs",),
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="Print the app logs.",
)
image_pull_policy_option = click.Option(
    ("--image-pull-policy",),
    type=click.Choice(["Always", "IfNotPresent", "Never"]),
    default="IfNotPresent",
    show_default=True,
    help="The image pull policy.",
)
ui_reverse_proxy_option = click.Option(
    ("--ui-reverse-proxy",),
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="Whether to enable UI reverse proxy.",
)
