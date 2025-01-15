from __future__ import annotations

import subprocess
import sys

import pytest

PYTHON_313_OR_ABOVE = sys.version_info.major == 3 and sys.version_info.minor >= 13


@pytest.fixture(scope="session")
def root_path() -> str:
    return (
        subprocess.Popen(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE)
        .communicate()[0]
        .rstrip()
        .decode("utf-8")
    )
