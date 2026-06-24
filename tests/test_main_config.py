from __future__ import annotations

from pathlib import Path

from ocs_submission.main import load_jsonc_config


def _write(tmp_path: Path, text: str) -> str:
    config_path = tmp_path / "config.jsonc"
    config_path.write_text(text)
    return str(config_path)


def test_load_jsonc_config_strips_comments(tmp_path):
    config_path = _write(
        tmp_path,
        """
        {
            // a line comment
            "references": {
                "mouse": { "all": "mouse_ref" } /* a block comment */
            },
            "job_settings": { "limit": 5 }
        }
        """,
    )
    config = load_jsonc_config(config_path)
    assert config["references"]["mouse"] == {"all": "mouse_ref"}
    assert config["job_settings"]["limit"] == 5


def test_load_jsonc_config_expands_pipe_delimited_reference_keys(tmp_path):
    config_path = _write(
        tmp_path,
        """
        {
            "references": {
                "macaque | macaque_nemestrina | macaque_fascicularis": { "RTX": "macaque_ref" }
            }
        }
        """,
    )
    config = load_jsonc_config(config_path)
    assert config["references"]["macaque"] == {"RTX": "macaque_ref"}
    assert config["references"]["macaque_nemestrina"] == {"RTX": "macaque_ref"}
    assert config["references"]["macaque_fascicularis"] == {"RTX": "macaque_ref"}


def test_single_organism_key_is_preserved(tmp_path):
    config_path = _write(
        tmp_path,
        """
        {
            "references": {
                "human": { "all": "human_ref" }
            }
        }
        """,
    )
    config = load_jsonc_config(config_path)
    assert set(config["references"]) == {"human"}
