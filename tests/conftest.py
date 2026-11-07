from __future__ import annotations

import subprocess

import pytest


@pytest.fixture(scope="session")
def root_path() -> str:
    return (
        subprocess.Popen(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE)
        .communicate()[0]
        .rstrip()
        .decode("utf-8")
    )
