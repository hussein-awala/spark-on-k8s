from __future__ import annotations

import pytest
from click.core import Argument, BadParameter, Command, Context
from click.testing import CliRunner
from spark_on_k8s.cli.options import validate_dictionary_option, validate_list_option


@pytest.mark.parametrize(
    "passed_value, expected_result",
    [
        pytest.param([], {}, id="empty value"),
        pytest.param(["p1=v1"], {"p1": "v1"}, id="a single value"),
        pytest.param(["p1=v1", "p2=v2"], {"p1": "v1", "p2": "v2"}, id="multiple values"),
        pytest.param(["p1:v1"], BadParameter, id="a bad value"),
        pytest.param(["p1=v1", "p2"], BadParameter, id="multiple value with a bad one"),
    ],
)
def test_validate_dictionary_option(
    passed_value: list[str], expected_result: dict[str, str] | type[Exception]
):
    ctx = Context(Command("fake-command"))
    param = Argument(["--test-param"])
    if isinstance(expected_result, type) and issubclass(expected_result, Exception):
        with pytest.raises(expected_result):
            validate_dictionary_option(ctx, param, passed_value)
    else:
        actual_result = validate_dictionary_option(ctx, param, passed_value)
        assert expected_result == actual_result


@pytest.mark.parametrize(
    "passed_value, expected_result",
    [
        pytest.param("", [], id="empty value"),
        pytest.param("p1", ["p1"], id="a single value"),
        pytest.param("p1,p2,p3", ["p1", "p2", "p3"], id="multiple values"),
    ],
)
def test_validate_list_option(passed_value: str, expected_result: list[str]):
    ctx = Context(Command("fake-command"))
    param = Argument(["--test-param"])
    actual_result = validate_list_option(ctx, param, passed_value)
    assert expected_result == actual_result


@pytest.mark.parametrize(
    "command, arguments, missing",
    [
        pytest.param(
            "app", ["submit", "--path", "local:///app.py"], "--image", id="app submit without --image"
        ),
        pytest.param("app", ["submit", "--image", "spark:test"], "--path", id="app submit without --path"),
        pytest.param("sql", ["submit", "--sql", "SELECT 1"], "--image", id="sql submit without --image"),
    ],
)
def test_required_options_without_an_environment_default_are_usage_errors(
    command, arguments, missing, monkeypatch
):
    from spark_on_k8s.cli.app import app_cli
    from spark_on_k8s.cli.sql import sql_cli
    from spark_on_k8s.utils.configuration import Configuration

    # The options read these at import; the test suite runs without them.
    assert Configuration.SPARK_ON_K8S_DOCKER_IMAGE is None
    assert Configuration.SPARK_ON_K8S_APP_PATH is None
    group = {"app": app_cli, "sql": sql_cli}[command]

    result = CliRunner().invoke(group, arguments)

    assert result.exit_code == 2, result.output
    assert f"Missing option '{missing}'" in result.output
