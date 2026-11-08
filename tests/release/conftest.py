from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.fixture
def release_modules():
    modules = {}
    for name in ("eligibility", "artifacts", "notes"):
        path = Path(__file__).resolve().parents[2] / "scripts" / f"release_{name}.py"
        spec = importlib.util.spec_from_file_location(f"release_{name}", path)
        module = importlib.util.module_from_spec(spec)
        # Dataclasses resolve their defining module while the script executes.
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        modules[name] = module
    return modules
