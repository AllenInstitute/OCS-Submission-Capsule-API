from pathlib import Path
from string import Formatter

import pytest

from ocs_submission.commands.builder import COMMAND_CONFIG_BY_STAGE, build_ocs_command_args
from ocs_submission.main import CONFIG_PATH, load_jsonc_config

EMAIL = "test@example.org"
# Command tests load a fresh config so mutations cannot affect collected cases.
_DEFAULT_CONFIG = load_jsonc_config(CONFIG_PATH)


def _write(tmp_path: Path, text: str) -> str:
    config_path = tmp_path / "config.jsonc"
    config_path.write_text(text)
    return str(config_path)


def _argument_placeholders(command_config: dict) -> set[str]:
    placeholders = set()
    for argument in command_config["arguments"]:
        value_template = argument.get("value", "")
        placeholders.update(field_name for _, field_name, _, _ in Formatter().parse(value_template) if field_name)
    return placeholders


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


def _command_config_params():
    params = []
    for modality, workflow in _DEFAULT_CONFIG["workflows"].items():
        for stage, (command_config_field, _) in COMMAND_CONFIG_BY_STAGE.items():
            for command_config in workflow[command_config_field]:
                params.append(
                    pytest.param(
                        modality,
                        stage,
                        command_config,
                        id=f"{modality}-{stage.name}-{command_config.get('name', 'unnamed')}",
                    )
                )
    assert params, "Default config produced no command configs to validate"
    return params


@pytest.mark.parametrize("_modality, _stage, command_config", _command_config_params())
def test_default_command_config_match_is_well_formed(_modality, _stage, command_config):
    match = command_config["match"]
    assert command_config["command"]
    assert match["library_preps"]
    assert "*" not in match["library_preps"]
    assert match.get("organisms") != ["*"]


@pytest.mark.parametrize("modality, _stage, command_config", _command_config_params())
def test_default_command_config_renders_without_unresolved_placeholders(
    modality, _stage, command_config, make_fastq_record
):
    config = load_jsonc_config(CONFIG_PATH)
    library_prep_method_name = command_config["match"]["library_preps"][0]

    placeholders = _argument_placeholders(command_config)
    if "execution_vcpus" in placeholders:
        assert command_config.get("execution_vcpus")

    command_args, spacing = build_ocs_command_args(
        config=config,
        fastq_record=make_fastq_record(library_prep_method_name=library_prep_method_name),
        modality=modality,
        email=EMAIL,
        command_template=command_config,
    )

    assert spacing > 0
    assert all("{" not in argument and "}" not in argument for argument in command_args)
