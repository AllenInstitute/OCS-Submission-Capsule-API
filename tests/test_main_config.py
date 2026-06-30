from __future__ import annotations

from pathlib import Path

from ocs_submission.main import CONFIG_PATH, load_jsonc_config
from ocs_submission.ocs_command_builder import COMMAND_CONFIG_BY_STAGE


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


def test_default_config_matches_command_config_schema():
    config = load_jsonc_config(CONFIG_PATH)

    for modality, workflow in config["workflows"].items():
        assert "post_alignment" not in workflow

        for command_config_field, _ in COMMAND_CONFIG_BY_STAGE.values():
            assert command_config_field in workflow

            for command_config in workflow[command_config_field]:
                match = command_config["match"]
                assert match["library_preps"]
                assert "*" not in match["library_preps"]
                assert match.get("organisms") != ["*"]
