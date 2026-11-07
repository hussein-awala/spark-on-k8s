from __future__ import annotations

from typing import NotRequired, Required, TypedDict


class ArgNotSet:
    """A type used to indicate that an argument was not set."""


NOTSET = ArgNotSet()


class _TextConfigMapSource(TypedDict):
    name: Required[str]
    text: Required[str]


class _TextFileConfigMapSource(TypedDict):
    name: Required[str]
    text_path: Required[str]


ConfigMapSource = _TextConfigMapSource | _TextFileConfigMapSource


class ConfigMap(TypedDict):
    name: NotRequired[str]
    mount_path: Required[str]
    sources: Required[list[ConfigMapSource]]
