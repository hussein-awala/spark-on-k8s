from __future__ import annotations

import pytest
from kubernetes import client as k8s
from spark_on_k8s.utils.env import (
    ConfigurationError,
    env_client_config,
    env_float,
    env_int,
    env_json,
    env_optional_int,
)

NAME = "SPARK_ON_K8S_TEST_VALUE"


def test_unset_variables_use_their_defaults(monkeypatch):
    monkeypatch.delenv(NAME, raising=False)
    assert env_int(NAME, 3) == 3
    assert env_optional_int(NAME) is None
    assert env_float(NAME, 1.5) == 1.5
    assert env_json(NAME, {}, dict) == {}
    assert env_client_config(NAME, k8s.Configuration) is None


def test_values_are_parsed(monkeypatch):
    monkeypatch.setenv(NAME, "4")
    assert env_int(NAME, 3) == 4
    assert env_optional_int(NAME) == 4
    assert env_float(NAME, 1.5) == 4.0
    monkeypatch.setenv(NAME, '{"host": "https://k8s.example"}')
    assert env_json(NAME, {}, dict) == {"host": "https://k8s.example"}
    assert env_client_config(NAME, k8s.Configuration).host == "https://k8s.example"


def test_an_empty_optional_integer_is_unset(monkeypatch):
    monkeypatch.setenv(NAME, "")
    assert env_optional_int(NAME) is None


@pytest.mark.parametrize(
    "value,parse,message",
    [
        ("two", lambda: env_int(NAME, 1), f"{NAME} must be an integer; got 'two'"),
        ("", lambda: env_int(NAME, 1), f"{NAME} must be an integer; got ''"),
        ("1.5", lambda: env_optional_int(NAME), f"{NAME} must be an integer; got '1.5'"),
        ("fast", lambda: env_float(NAME, 1.0), f"{NAME} must be a number; got 'fast'"),
        ("token=abc", lambda: env_json(NAME, {}, dict), f"{NAME} must be a JSON object"),
        ('["a"]', lambda: env_json(NAME, {}, dict), f"{NAME} must be a JSON object"),
        ('{"a": 1}', lambda: env_json(NAME, [], list), f"{NAME} must be a JSON array"),
        (
            '{"no_such_option": 1}',
            lambda: env_client_config(NAME, k8s.Configuration),
            f"{NAME} names an argument the Kubernetes client configuration does not accept",
        ),
    ],
)
def test_malformed_values_name_the_variable_without_echoing_json(monkeypatch, value, parse, message):
    monkeypatch.setenv(NAME, value)
    with pytest.raises(ConfigurationError) as raised:
        parse()
    assert str(raised.value) == message
    assert isinstance(raised.value, ValueError)
