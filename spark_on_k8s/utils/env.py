"""Parse SPARK_ON_K8S_* environment variables with errors that name the variable."""

from __future__ import annotations

import json
from os import getenv
from typing import Any


class ConfigurationError(ValueError):
    """An environment variable holds a value that cannot be used."""


def env_int(name: str, default: int | None) -> int | None:
    raw = getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be an integer; got {raw!r}") from None


def env_optional_int(name: str) -> int | None:
    """An integer, or None when the variable is unset or empty."""
    return env_int(name, None) if getenv(name) else None


def env_float(name: str, default: float) -> float:
    raw = getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be a number; got {raw!r}") from None


def env_json(name: str, default: Any, kind: type) -> Any:
    """A JSON object or array. The value is never echoed: some of these variables hold secrets."""
    raw = getenv(name)
    if raw is None:
        return default
    expected = "object" if kind is dict else "array"
    try:
        value = json.loads(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be a JSON {expected}") from None
    if not isinstance(value, kind):
        raise ConfigurationError(f"{name} must be a JSON {expected}")
    return value


def env_client_config(name: str, configuration_type: type) -> Any:
    """A Kubernetes client configuration from a JSON object of its constructor arguments."""
    if not getenv(name):
        return None
    try:
        return configuration_type(**env_json(name, {}, dict))
    except TypeError:
        raise ConfigurationError(
            f"{name} names an argument the Kubernetes client configuration does not accept"
        ) from None
