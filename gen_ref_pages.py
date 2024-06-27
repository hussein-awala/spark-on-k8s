"""Generate the code reference pages."""
from __future__ import annotations

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

for path in sorted(Path("spark_on_k8s").rglob("*.py")):
    if path.name == "__init__.py":
        continue
    module_path = path.relative_to("spark_on_k8s").with_suffix("")
    doc_path = Path("spark_on_k8s", path.relative_to("spark_on_k8s").with_suffix(".md"))
    full_doc_path = Path("reference", doc_path)
    nav[module_path.parts] = str(doc_path)

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = "spark_on_k8s." + ".".join(module_path.parts)
        print("::: " + identifier, file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, path)
with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
