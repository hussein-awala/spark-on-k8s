"""Run pytest against a noneditable installation: python -I tests/run_installed_tests.py [args]."""

from __future__ import annotations

import json
import sys
import sysconfig
from importlib.metadata import distribution
from pathlib import Path


def _check_installed_modules() -> None:
    environment = Path(sys.prefix).resolve()
    roots = {Path(sysconfig.get_path(name)).resolve() for name in ("purelib", "platlib")}
    if sys.prefix == sys.base_prefix or not all(root.is_relative_to(environment) for root in roots):
        raise RuntimeError("Installed-wheel tests require an isolated virtual environment")
    for name, module in tuple(sys.modules.items()):
        if name != "spark_on_k8s" and not name.startswith("spark_on_k8s."):
            continue
        paths = [getattr(module, "__file__", None), *getattr(module, "__path__", [])]
        if not paths or any(
            path is None or not any(Path(path).resolve().is_relative_to(root) for root in roots)
            for path in paths
        ):
            raise RuntimeError(f"{name} was not loaded from environment site-packages: {paths}")


def main() -> int:
    if not sys.flags.isolated:
        raise RuntimeError(
            "Use python -I to exclude the checkout and PYTHONPATH before importing the package"
        )
    installed = distribution("spark-on-k8s")
    direct_url = json.loads(installed.read_text("direct_url.json") or "{}")
    if direct_url.get("dir_info", {}).get("editable"):
        raise RuntimeError("Installed-wheel tests cannot use an editable installation")

    # Anchor the package path before pytest adds test directories to sys.path.
    import spark_on_k8s

    _check_installed_modules()
    print(f"Testing installed spark-on-k8s {installed.version} at {spark_on_k8s.__file__}")
    import pytest

    result = pytest.main(sys.argv[1:] or ["tests"])
    _check_installed_modules()
    return int(result)


if __name__ == "__main__":
    raise SystemExit(main())
