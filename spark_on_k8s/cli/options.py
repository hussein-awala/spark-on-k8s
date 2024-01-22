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
driver_cpu_option = click.Option(
    ("--driver-cpu",),
    type=int,
    default=1,
    show_default=True,
    help="The driver CPU.",
)
driver_memory_option = click.Option(
    ("--driver-memory",),
    type=int,
    default=1024,
    show_default=True,
    help="The driver memory (in MB).",
)
driver_memory_overhead_option = click.Option(
    ("--driver-memory-overhead",),
    type=int,
    default=512,
    show_default=True,
    help="The driver memory overhead (in MB).",
)
executor_cpu_option = click.Option(
    ("--executor-cpu",),
    type=int,
    default=1,
    show_default=True,
    help="The executor CPU.",
)
executor_memory_option = click.Option(
    ("--executor-memory",),
    type=int,
    default=1024,
    show_default=True,
    help="The executor memory (in MB).",
)
executor_memory_overhead_option = click.Option(
    ("--executor-memory-overhead",),
    type=int,
    default=512,
    show_default=True,
    help="The executor memory overhead (in MB).",
)
executor_min_instances_option = click.Option(
    ("--executor-min-instances",),
    type=int,
    default=None,
    show_default=True,
    help="The minimum number of executor instances. If provided, dynamic allocation is enabled.",
)
executor_max_instances_option = click.Option(
    ("--executor-max-instances",),
    type=int,
    default=None,
    show_default=True,
    help="The maximum number of executor instances. If provided, dynamic allocation is enabled.",
)
executor_initial_instances_option = click.Option(
    ("--executor-initial-instances",),
    type=int,
    default=None,
    show_default=True,
    help=(
        "The initial number of executor instances. If max and min are not provided, dynamic "
        "allocation will be disabled and the number of executors will be fixed to this or 2 if "
        "this is not provided. If max or min or both are provided, dynamic allocation will be "
        "enabled and the number of executors will be between min and max (inclusive), and this "
        "will be the initial number of executors with a default of 0."
    ),
)
