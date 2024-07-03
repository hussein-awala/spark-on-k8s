from __future__ import annotations

from typing import TypedDict, Union

try:
    # Python 3.11+
    from typing import NotRequired, Required
except ImportError:
    from typing_extensions import NotRequired, Required


class ArgNotSet:
    """A type used to indicate that an argument was not set."""


NOTSET = ArgNotSet()


class _TextConfigMapSource(TypedDict):
    name: Required[str]
    text: Required[str]


class _TextFileConfigMapSource(TypedDict):
    name: Required[str]
    text_path: Required[str]


ConfigMapSource = Union[_TextConfigMapSource, _TextFileConfigMapSource]


class ConfigMap(TypedDict):
    name: NotRequired[str]
    mount_path: Required[str]
    sources: Required[list[ConfigMapSource]]
