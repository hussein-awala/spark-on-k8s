"""Check relative links, anchors and assets in the built documentation without network access."""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Page(HTMLParser):
    def __init__(self, text: str):
        super().__init__(convert_charrefs=True)
        self.anchors: set[str] = set()
        self.links: list[str] = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        for name in ("id", "name") if tag == "a" else ("id",):
            if attributes.get(name):
                self.anchors.add(attributes[name])
        attribute = {
            "a": "href",
            "link": "href",
            "img": "src",
            "script": "src",
            "iframe": "src",
            "source": "src",
        }.get(tag)
        if attribute and attributes.get(attribute):
            self.links.append(attributes[attribute])


def check_site(directory: Path) -> list[str]:
    root = directory.resolve()
    if not (root / "index.html").is_file():
        return ["index.html: missing documentation homepage"]
    pages = {path: Page(path.read_text(encoding="utf-8")) for path in sorted(root.rglob("*.html"))}
    errors = []
    for source, page in pages.items():
        for href in dict.fromkeys(page.links):
            url = urlsplit(href)
            # Version selectors and external references require a published site.
            # This check intentionally validates only paths within this build.
            if url.scheme or url.netloc or url.path.startswith("/"):
                continue
            target = (source.parent / unquote(url.path)).resolve() if url.path else source
            if not target.is_relative_to(root):
                reason = "relative path leaves the documentation build"
            else:
                if target.is_dir() or url.path.endswith("/"):
                    target /= "index.html"
                if not target.is_file():
                    reason = "target does not exist"
                elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].anchors:
                    reason = "target anchor does not exist"
                else:
                    continue
            errors.append(f"{source.relative_to(root)}: {href!r}: {reason}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path, help="Built site directory, usually site")
    args = parser.parse_args()
    errors = check_site(args.site)
    if errors:
        for error in errors[:50]:
            print(error)
        print(f"Documentation link check failed: {len(errors)} broken references")
        return 1
    print("Documentation relative links, anchors and assets passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
