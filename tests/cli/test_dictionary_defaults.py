from __future__ import annotations

import runpy

import click
import pytest
from click.testing import CliRunner
from spark_on_k8s.cli import options
from spark_on_k8s.utils import configuration


@pytest.mark.parametrize(
    "option_name, configuration_name",
    [
        ("spark_conf_option", "SPARK_ON_K8S_SPARK_CONF"),
        ("secret_env_var_option", "SPARK_ON_K8S_SECRET_ENV_VAR"),
        ("driver_node_selector_option", "SPARK_ON_K8S_SPARK_DRIVER_NODE_SELECTOR"),
        ("executor_node_selector_option", "SPARK_ON_K8S_SPARK_EXECUTOR_NODE_SELECTOR"),
        ("driver_labels_option", "SPARK_ON_K8S_SPARK_DRIVER_LABELS"),
        ("executor_labels_option", "SPARK_ON_K8S_SPARK_EXECUTOR_LABELS"),
        ("driver_annotations_option", "SPARK_ON_K8S_SPARK_DRIVER_ANNOTATIONS"),
        ("executor_annotations_option", "SPARK_ON_K8S_SPARK_EXECUTOR_ANNOTATIONS"),
    ],
)
@pytest.mark.parametrize("override", [False, True])
def test_dictionary_configuration_defaults(monkeypatch, option_name, configuration_name, override):
    configured_values = {"first": "value=with=equals", "second": ""}
    monkeypatch.setattr(configuration.Configuration, configuration_name, configured_values)
    # Recreate the option exactly as a fresh CLI import would, without changing
    # other tests' imported command objects.
    option = runpy.run_path(options.__file__)[option_name]
    captured = {}
    command = click.Command("capture", params=[option], callback=lambda **kwargs: captured.update(kwargs))
    arguments = [option.opts[0], "explicit=override"] if override else []

    result = CliRunner().invoke(command, arguments)

    assert result.exit_code == 0, result.output
    assert captured[option.name] == ({"explicit": "override"} if override else configured_values)


def test_secret_defaults_are_not_printed_in_help(monkeypatch):
    monkeypatch.setattr(
        configuration.Configuration, "SPARK_ON_K8S_SECRET_ENV_VAR", {"TOKEN": "private-example-value"}
    )
    option = runpy.run_path(options.__file__)["secret_env_var_option"]
    command = click.Command("capture", params=[option])

    result = CliRunner().invoke(command, ["--help"])

    assert result.exit_code == 0
    assert "private-example-value" not in result.output
