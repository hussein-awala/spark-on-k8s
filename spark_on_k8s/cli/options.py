from __future__ import annotations

import click

from spark_on_k8s.utils.configuration import Configuration

namespace_option = click.Option(
    ("-n", "--namespace"),
    type=str,
    default=Configuration.SPARK_ON_K8S_NAMESPACE,
    show_default=True,
    help="The namespace to operate on.",
)

app_id_option = click.Option(("--app-id",), type=str, required=True, help="The ID of the app to operate on.")

force_option = click.Option(
    ("-f", "--force"), is_flag=True, default=False, show_default=True, help="Force the operation."
)


# Submit options
def validate_dictionary_option(ctx, param, value):
    dict_values = {}
    for conf in value:
        try:
            key, val = conf.split("=", 1)
            dict_values[key] = val
        except ValueError:
            raise click.BadParameter(f"{param.name} parameter must be in the form key=value.") from None
    return dict_values


def validate_list_option(ctx, param, value):
    list_values = value.split(",") if value else []
    return list_values


docker_image_option = click.Option(
    ("--image",),
    type=str,
    default=Configuration.SPARK_ON_K8S_DOCKER_IMAGE,
    show_default=True,
    required=Configuration.SPARK_ON_K8S_DOCKER_IMAGE is None,
    help="The docker image to use for the app.",
)
app_path_option = click.Option(
    ("--path",),
    type=str,
    default=Configuration.SPARK_ON_K8S_APP_PATH,
    show_default=True,
    required=Configuration.SPARK_ON_K8S_APP_PATH is None,
    help="The path to the app to submit.",
)
service_account_option = click.Option(
    ("--service-account",),
    type=str,
    default=Configuration.SPARK_ON_K8S_SERVICE_ACCOUNT,
    show_default=True,
    help="The service account to use for the app.",
)
app_name_option = click.Option(
    ("--name",),
    type=str,
    default=Configuration.SPARK_ON_K8S_APP_NAME,
    show_default=True,
    required=False,
    help="The name of the app.",
)
spark_conf_option = click.Option(
    ("--conf", "spark_conf"),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SPARK_CONF,
    show_default=True,
    help="Spark configuration property in key=value format. Can be repeated.",
)
class_name_option = click.Option(
    ("--class", "class_name"),
    type=str,
    default=Configuration.SPARK_ON_K8S_CLASS_NAME,
    show_default=True,
    help="The main class for the app.",
)
wait_option = click.Option(
    ("--wait",),
    type=bool,
    is_flag=True,
    default=Configuration.SPARK_ON_K8S_APP_WAITER == "wait",
    show_default=True,
    help="Wait for the app to finish.",
)
logs_option = click.Option(
    ("--logs",),
    type=bool,
    is_flag=True,
    default=Configuration.SPARK_ON_K8S_APP_WAITER == "logs",
    show_default=True,
    help="Print the app logs.",
)
image_pull_policy_option = click.Option(
    ("--image-pull-policy",),
    type=click.Choice(["Always", "IfNotPresent", "Never"]),
    default=Configuration.SPARK_ON_K8S_IMAGE_PULL_POLICY,
    show_default=True,
    help="The image pull policy.",
)
ui_reverse_proxy_option = click.Option(
    ("--ui-reverse-proxy",),
    type=bool,
    is_flag=True,
    default=Configuration.SPARK_ON_K8S_UI_REVERSE_PROXY,
    show_default=True,
    help="Whether to enable UI reverse proxy.",
)
driver_cpu_option = click.Option(
    ("--driver-cpu",),
    type=int,
    default=Configuration.SPARK_ON_K8S_DRIVER_CPU,
    show_default=True,
    help="The driver CPU.",
)
driver_memory_option = click.Option(
    ("--driver-memory",),
    type=int,
    default=Configuration.SPARK_ON_K8S_DRIVER_MEMORY,
    show_default=True,
    help="The driver memory (in MB).",
)
driver_memory_overhead_option = click.Option(
    ("--driver-memory-overhead",),
    type=int,
    default=Configuration.SPARK_ON_K8S_DRIVER_MEMORY_OVERHEAD,
    show_default=True,
    help="The driver memory overhead (in MB).",
)
executor_cpu_option = click.Option(
    ("--executor-cpu",),
    type=int,
    default=Configuration.SPARK_ON_K8S_EXECUTOR_CPU,
    show_default=True,
    help="The executor CPU.",
)
executor_memory_option = click.Option(
    ("--executor-memory",),
    type=int,
    default=Configuration.SPARK_ON_K8S_EXECUTOR_MEMORY,
    show_default=True,
    help="The executor memory (in MB).",
)
executor_memory_overhead_option = click.Option(
    ("--executor-memory-overhead",),
    type=int,
    default=Configuration.SPARK_ON_K8S_EXECUTOR_MEMORY_OVERHEAD,
    show_default=True,
    help="The executor memory overhead (in MB).",
)
executor_min_instances_option = click.Option(
    ("--executor-min-instances",),
    type=int,
    default=Configuration.SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES,
    show_default=True,
    help="The minimum number of executor instances. If provided, dynamic allocation is enabled.",
)
executor_max_instances_option = click.Option(
    ("--executor-max-instances",),
    type=int,
    default=Configuration.SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES,
    show_default=True,
    help="The maximum number of executor instances. If provided, dynamic allocation is enabled.",
)
executor_initial_instances_option = click.Option(
    ("--executor-initial-instances",),
    type=int,
    default=Configuration.SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES,
    show_default=True,
    help=(
        "The initial number of executor instances. If max and min are not provided, dynamic "
        "allocation will be disabled and the number of executors will be fixed to this or 2 if "
        "this is not provided. If max or min or both are provided, dynamic allocation will be "
        "enabled and the number of executors will be between min and max (inclusive), and this "
        "will be the initial number of executors with a default of 0."
    ),
)
secret_env_var_option = click.Option(
    ("--secret-env-var",),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SECRET_ENV_VAR,
    show_default=True,
    help="Secret environment variable in key=value format. Can be repeated.",
)
driver_env_vars_from_secrets_option = click.Option(
    ("--driver-env-vars-from-secrets",),
    type=str,
    callback=validate_list_option,
    default=",".join(Configuration.SPARK_ON_K8S_DRIVER_ENV_VARS_FROM_SECRET),
    show_default=True,
    help="Secrets to load environment variables from for the driver. Comma-separated list of secret names.",
)
driver_node_selector_option = click.Option(
    ("--driver-node-selector", "driver_node_selector"),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SPARK_DRIVER_NODE_SELECTOR,
    show_default=True,
    help="Node selector for the driver in key=value format. Can be repeated.",
)
executor_node_selector_option = click.Option(
    ("--executor-node-selector", "executor_node_selector"),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SPARK_EXECUTOR_NODE_SELECTOR,
    show_default=True,
    help="Node selector for the executor in key=value format. Can be repeated.",
)
driver_labels_option = click.Option(
    ("--driver-label", "driver_labels"),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SPARK_DRIVER_LABELS,
    show_default=True,
    help="Labels for the driver in key=value format. Can be repeated.",
)
executor_labels_option = click.Option(
    ("--executor-label", "executor_labels"),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SPARK_EXECUTOR_LABELS,
    show_default=True,
    help="Labels for the executor in key=value format. Can be repeated.",
)
driver_annotations_option = click.Option(
    ("--driver-annotation", "driver_annotations"),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SPARK_DRIVER_ANNOTATIONS,
    show_default=True,
    help="Annotations for the driver in key=value format. Can be repeated.",
)
executor_annotations_option = click.Option(
    ("--executor-annotation", "executor_annotations"),
    type=str,
    multiple=True,
    callback=validate_dictionary_option,
    default=Configuration.SPARK_ON_K8S_SPARK_EXECUTOR_ANNOTATIONS,
    show_default=True,
    help="Annotations for the executor in key=value format. Can be repeated.",
)
executor_pod_template_path_option = click.Option(
    ("--executor-pod-template-path", "executor_pod_template_path"),
    type=str,
    default=Configuration.SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH,
    show_default=True,
    help="The path to the executor pod template file.",
)
