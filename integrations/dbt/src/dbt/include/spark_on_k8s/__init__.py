"""Include package; SQL macros are inherited from the dbt-spark dependency."""

from __future__ import annotations

from pathlib import Path

PACKAGE_PATH = str(Path(__file__).parent)
