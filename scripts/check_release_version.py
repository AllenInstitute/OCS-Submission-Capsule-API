#!/usr/bin/env python3
"""Release gate: keep the git tag, pyproject version, and CHANGELOG in sync.

The Release workflow (.github/workflows/release.yml) runs this against the tag
that triggered it (e.g. ``v0.1.1``). Two modes:

    check_release_version.py v0.1.1            # verify tag == pyproject == CHANGELOG
    check_release_version.py v0.1.1 --notes    # print that version's CHANGELOG body

Verification fails (non-zero exit) if the tag version does not match
``pyproject.toml`` or if CHANGELOG.md has no matching ``## [x.y.z]`` section, so a
mistagged or undocumented release never reaches ``gh release create``.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
CHANGELOG = REPO_ROOT / "CHANGELOG.md"


def fail(message: str) -> "NoReturn":  # type: ignore[name-defined]
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def version_from_tag(tag: str) -> str:
    """``v0.1.1`` -> ``0.1.1``. The leading ``v`` is required by the tag_format."""
    if not tag.startswith("v"):
        fail(f"tag {tag!r} must start with 'v' (expected form v0.1.1)")
    version = tag[1:]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        fail(f"tag {tag!r} is not a vMAJOR.MINOR.PATCH release tag")
    return version


def pyproject_version() -> str:
    with PYPROJECT.open("rb") as fh:
        data = tomllib.load(fh)
    try:
        return data["project"]["version"]
    except KeyError:
        fail("no [project].version found in pyproject.toml")


def changelog_notes(version: str) -> str:
    """Return the body under ``## [version] ...`` up to the next ``## `` heading.

    Matches Keep a Changelog headings like ``## [0.1.1] - 2026-07-06``.
    """
    if not CHANGELOG.exists():
        fail("CHANGELOG.md not found")

    text = CHANGELOG.read_text(encoding="utf-8")
    # Heading for this exact version; tolerate an optional date suffix.
    heading = re.compile(
        rf"^## \[{re.escape(version)}\](?: - \d{{4}}-\d{{2}}-\d{{2}})?\s*$",
        re.MULTILINE,
    )
    match = heading.search(text)
    if match is None:
        fail(
            f"CHANGELOG.md has no '## [{version}]' section. "
            "Add release notes before tagging."
        )

    rest = text[match.end():]
    next_heading = re.search(r"^## ", rest, re.MULTILINE)
    body = rest[: next_heading.start()] if next_heading else rest
    body = body.strip()
    if not body:
        fail(f"CHANGELOG.md section for {version} is empty.")
    return body


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="release tag, e.g. v0.1.1")
    parser.add_argument(
        "--notes",
        action="store_true",
        help="print the CHANGELOG body for this version (for gh release --notes-file)",
    )
    args = parser.parse_args(argv)

    version = version_from_tag(args.tag)

    project_version = pyproject_version()
    if project_version != version:
        fail(
            f"tag {args.tag} implies version {version}, but pyproject.toml is "
            f"{project_version}. Bump the version and re-tag."
        )

    notes = changelog_notes(version)  # also validates the section exists

    if args.notes:
        print(notes)
    else:
        print(f"ok: {args.tag} matches pyproject.toml and CHANGELOG.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
