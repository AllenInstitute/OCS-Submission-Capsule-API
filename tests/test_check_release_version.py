from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def load_release_script() -> ModuleType:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "check_release_version.py"
    spec = importlib.util.spec_from_file_location("check_release_version", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def release_script(tmp_path, monkeypatch) -> ModuleType:
    module = load_release_script()
    pyproject = tmp_path / "pyproject.toml"
    changelog = tmp_path / "CHANGELOG.md"
    pyproject.write_text('[project]\nversion = "1.2.3"\n', encoding="utf-8")
    changelog.write_text(
        "# Changelog\n\n## [Unreleased]\n\n## [1.2.3] - 2026-07-06\n\n### Added\n\n- Release notes\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "PYPROJECT", pyproject)
    monkeypatch.setattr(module, "CHANGELOG", changelog)
    return module


def test_main_accepts_matching_tag_version_and_changelog(release_script, capsys):
    assert release_script.main(["v1.2.3"]) == 0

    assert capsys.readouterr().out.strip() == "ok: v1.2.3 matches pyproject.toml and CHANGELOG.md"


def test_main_rejects_tag_that_is_not_on_required_ref(release_script, monkeypatch):
    def fake_git_output(args: list[str]) -> str:
        if args == ["rev-list", "-n", "1", "v1.2.3"]:
            return "tag-commit"
        if args == ["rev-parse", "origin/main"]:
            return "main-commit"
        raise AssertionError(f"unexpected git args: {args}")

    monkeypatch.setattr(release_script, "git_output", fake_git_output)
    monkeypatch.setattr(release_script, "is_ancestor", lambda _ancestor, _ref: False)

    with pytest.raises(SystemExit) as exc_info:
        release_script.main(["v1.2.3", "--require-ancestor-ref", "origin/main"])

    assert exc_info.value.code == 1


def test_main_accepts_tag_on_required_ref(release_script, monkeypatch):
    def fake_git_output(args: list[str]) -> str:
        if args == ["rev-list", "-n", "1", "v1.2.3"]:
            return "tag-commit"
        if args == ["rev-parse", "origin/main"]:
            return "main-commit"
        raise AssertionError(f"unexpected git args: {args}")

    monkeypatch.setattr(release_script, "git_output", fake_git_output)
    monkeypatch.setattr(
        release_script, "is_ancestor", lambda ancestor, ref: ancestor == "tag-commit" and ref == "origin/main"
    )

    assert release_script.main(["v1.2.3", "--require-ancestor-ref", "origin/main"]) == 0
